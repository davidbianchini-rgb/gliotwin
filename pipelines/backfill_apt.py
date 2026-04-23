#!/usr/bin/env python3
"""Backfill APT sequences for patients that already have FeTS segmentation done.

For each session:
  1. Find the APT DICOM series in the raw study folder.
  2. Convert the APTW subset to NIfTI (native DICOM space).
  3. Register APT native → FeTS canonical space using:
       T1ce_native (DICOM, skull)  →  T1ce_prepared (FeTS canonical, skull)
     ANTs affine MI registration — same modality + same skull content → reliable.
  4. Save the transform and register APT.
  5. Insert / update the APT sequence in the DB without touching the FeTS pipeline state.

Usage:
    python pipelines/backfill_apt.py [--dry-run] [--subject-id SUBJECT_ID] [--force]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import db
from app.services.import_scan import _image_type_values, _read_anchor

DICOM_ROOT     = Path("/mnt/dati/irst_data/irst_dicom_raw/DICOM GBM")
PROJECT_ROOT   = Path(__file__).resolve().parents[1]
APT_PROCESSED_ROOT = PROJECT_ROOT / "processed" / "imported_sequences"
FETS_ENV_PYTHON    = Path("/home/irst/miniconda3/envs/fets-env/bin/python")
APT_CONVERTER_SCRIPT = PROJECT_ROOT / "pipelines" / "convert_apt_dicom.py"
APT_REGISTER_SCRIPT  = PROJECT_ROOT / "pipelines" / "register_apt_to_reference.py"
REGISTRATION_TIMEOUT = 300


# ---------------------------------------------------------------------------
# DICOM scanning helpers
# ---------------------------------------------------------------------------

def _find_apt_series_in_study(study_dir: Path) -> list[Path]:
    """Return series sub-directories that contain ≥2 APTW DICOM instances."""
    apt_dirs = []
    if not study_dir.is_dir():
        return apt_dirs
    for series_dir in sorted(study_dir.iterdir()):
        if not series_dir.is_dir():
            continue
        aptw_count = 0
        for f in series_dir.iterdir():
            if not f.is_file():
                continue
            ds = _read_anchor(f)
            if ds is None:
                continue
            if any("APTW" in str(v).upper() for v in _image_type_values(ds)):
                aptw_count += 1
            if aptw_count >= 2:
                break
        if aptw_count >= 2:
            apt_dirs.append(series_dir)
    return apt_dirs


def _extract_series_uid(series_dir: Path) -> str | None:
    for f in series_dir.iterdir():
        if not f.is_file():
            continue
        ds = _read_anchor(f)
        if ds is None:
            continue
        uid = getattr(ds, "SeriesInstanceUID", None)
        return str(uid) if uid else None
    return None


# ---------------------------------------------------------------------------
# Conversion / registration helpers
# ---------------------------------------------------------------------------

def _convert_aptw(series_dir: Path, output_path: Path) -> str | None:
    """Convert APTW subset to NIfTI.  Returns error string or None on success."""
    aptw_files = [
        f for f in sorted(series_dir.iterdir())
        if f.is_file()
        and (ds := _read_anchor(f)) is not None
        and any("APTW" in str(v).upper() for v in _image_type_values(ds))
    ]
    if not aptw_files:
        return "No APTW files found"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gliotwin_apt_backfill_", dir="/tmp") as tmp:
        subset = Path(tmp) / "aptw"
        subset.mkdir()
        for f in aptw_files:
            (subset / f.name).symlink_to(f)
        proc = subprocess.run(
            [str(FETS_ENV_PYTHON), str(APT_CONVERTER_SCRIPT),
             "--input-dir", str(subset), "--output-img", str(output_path)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
    if proc.returncode != 0:
        return (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()
    return None


def _find_t1ce_prepared(run_root: Path, subject_id: str, session_label: str) -> Path | None:
    """Return the non-skull-stripped T1ce in FeTS canonical space."""
    candidates = [
        run_root / "output" / "prepared" / "DataForQC" / subject_id / f".{session_label}" / "reoriented" / f"{subject_id}_{session_label}_t1c.nii.gz",
        run_root / "output" / "prepared" / "DataForQC" / subject_id / session_label       / "reoriented" / f"{subject_id}_{session_label}_t1c.nii.gz",
        run_root / "output" / "brain_extracted" / "DataForQC" / subject_id / session_label / "reoriented" / f"{subject_id}_{session_label}_t1c.nii.gz",
    ]
    return next((c for c in candidates if c.exists()), None)


def _run_root_from_processed_dir(processed_dir: str) -> Path | None:
    parts = Path(processed_dir).parts
    for i, p in enumerate(parts):
        if p == "runs" and i + 1 < len(parts):
            return Path(*parts[: i + 2])
    return None


def _register_apt(native_path: Path, t1ce_dicom_dir: str, t1ce_prepared: Path,
                   output_path: Path, transform_path: Path) -> str | None:
    """Register APT native → FeTS canonical.  Returns error string or None."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [
            str(FETS_ENV_PYTHON), str(APT_REGISTER_SCRIPT),
            "--t1ce-dicom",     t1ce_dicom_dir,
            "--t1ce-prepared",  str(t1ce_prepared),
            "--apt-nifti",      str(native_path),
            "--output",         str(output_path),
            "--save-transform", str(transform_path),
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        timeout=REGISTRATION_TIMEOUT,
    )
    if proc.returncode != 0:
        return (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()
    return None


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------

def _upsert_apt_sequence(conn, session_id: int, subject_id: str, session_label: str,
                          series_dir: Path, series_uid: str | None,
                          native_path: str, registered_path: str | None,
                          transform_path: str | None, error: str | None) -> str:
    metadata = {
        "series_description": series_dir.name,
        "apt_native_path": native_path,
        "apt_registered_path": registered_path,
        "apt_resample_strategy": "ants_affine_native_to_canonical" if registered_path else "pending",
        "apt_transform_path": transform_path,
        "apt_conversion_error": None,
        "apt_registration_error": error,
        "apt_conversion_mode": "image_type_contains_aptw",
    }
    processed_path = registered_path or native_path
    existing = conn.execute(
        "SELECT id FROM sequences WHERE session_id = ? AND sequence_type = 'APT'",
        (session_id,),
    ).fetchone()
    if existing:
        conn.execute(
            """UPDATE sequences
               SET raw_path = ?, processed_path = ?, source_series_uid = ?, metadata_json = ?
               WHERE session_id = ? AND sequence_type = 'APT'""",
            (str(series_dir), processed_path, series_uid,
             json.dumps(metadata, ensure_ascii=True), session_id),
        )
        return "updated"
    conn.execute(
        """INSERT INTO sequences
               (session_id, sequence_type, contrast_agent, raw_path, processed_path,
                display_label, import_class, source_series_uid, metadata_json)
           VALUES (?, 'APT', 0, ?, ?, ?, 'apt', ?, ?)""",
        (session_id, str(series_dir), processed_path,
         series_dir.name, series_uid, json.dumps(metadata, ensure_ascii=True)),
    )
    return "inserted"


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def backfill(dry_run: bool = False, filter_subject_id: str | None = None,
             force: bool = False) -> None:
    with db() as conn:
        query = """
            SELECT se.id AS session_id, sub.subject_id, se.session_label,
                   se.raw_dir, se.processed_dir,
                   t1ce.raw_path  AS t1ce_raw_dir,
                   apt.id         AS apt_seq_id,
                   apt.metadata_json AS apt_meta
            FROM sessions se
            JOIN subjects sub  ON sub.id = se.subject_id
            JOIN sequences t1ce ON t1ce.session_id = se.id AND t1ce.sequence_type = 'T1ce'
            LEFT JOIN sequences apt ON apt.session_id = se.id AND apt.sequence_type = 'APT'
            WHERE se.processed_dir IS NOT NULL
        """
        params: list = []
        if filter_subject_id:
            query += " AND sub.subject_id = ?"
            params.append(filter_subject_id)
        query += " ORDER BY sub.subject_id, se.session_label"
        rows = conn.execute(query, params).fetchall()

    print(f"Found {len(rows)} sessions with FeTS done.")

    for row in rows:
        subject_id    = row["subject_id"]
        session_label = row["session_label"]
        session_id    = row["session_id"]
        t1ce_raw_dir  = row["t1ce_raw_dir"]
        processed_dir = row["processed_dir"]
        prefix = f"[{subject_id}/{session_label}]"

        # Skip if already done (unless --force)
        if not force and row["apt_seq_id"]:
            apt_meta = json.loads(row["apt_meta"] or "{}")
            registered = apt_meta.get("apt_registered_path")
            if registered and Path(registered).exists():
                print(f"{prefix} APT already registered → skip")
                continue

        # Locate run root and T1ce prepared
        run_root = _run_root_from_processed_dir(processed_dir)
        if run_root is None:
            print(f"{prefix} WARNING: cannot parse run root from {processed_dir}")
            continue

        t1ce_prepared = _find_t1ce_prepared(run_root, subject_id, session_label)
        if t1ce_prepared is None:
            print(f"{prefix} WARNING: T1ce prepared not found in {run_root}")
            continue

        # Find APT DICOM series
        study_dir = DICOM_ROOT / row["raw_dir"]
        if not study_dir.is_dir():
            print(f"{prefix} WARNING: study dir not found: {study_dir}")
            continue

        apt_series_dirs = _find_apt_series_in_study(study_dir)
        if not apt_series_dirs:
            print(f"{prefix} No APT series found")
            continue
        if len(apt_series_dirs) > 1:
            print(f"{prefix} Multiple APT series, using first: {[d.name for d in apt_series_dirs]}")
        series_dir = apt_series_dirs[0]
        n_files = sum(1 for f in series_dir.iterdir() if f.is_file())
        print(f"{prefix} APT series: {series_dir.name} ({n_files} files), prepared: {t1ce_prepared.name}")

        if dry_run:
            print(f"{prefix} [DRY RUN] would convert + register")
            continue

        series_uid      = _extract_series_uid(series_dir)
        series_uid_safe = (series_uid or "apt").replace(".", "_")

        native_path     = APT_PROCESSED_ROOT / subject_id / session_label / "apt" / f"{subject_id}_{session_label}_{series_uid_safe}_aptw.nii.gz"
        registered_path = APT_PROCESSED_ROOT / subject_id / session_label / "apt_registered" / f"{subject_id}_{session_label}_{series_uid_safe}_aptw_reg.nii.gz"
        transform_path  = APT_PROCESSED_ROOT / subject_id / session_label / "apt_registered" / f"{subject_id}_{session_label}_{series_uid_safe}_native_to_canonical.mat"

        # Convert APTW → NIfTI
        if native_path.exists() and not force:
            print(f"{prefix} Native NIfTI exists, skip conversion")
        else:
            print(f"{prefix} Converting APTW → NIfTI ...", end=" ", flush=True)
            err = _convert_aptw(series_dir, native_path)
            if err:
                print(f"ERROR: {err}")
                continue
            print("OK")

        # Register APT native → FeTS canonical
        if registered_path.exists() and not force:
            print(f"{prefix} Registered NIfTI exists, skip registration")
        else:
            print(f"{prefix} Registering (ANTs affine MI, T1ce skull vs T1ce skull) ...", end=" ", flush=True)
            try:
                err = _register_apt(native_path, t1ce_raw_dir, t1ce_prepared,
                                    registered_path, transform_path)
            except subprocess.TimeoutExpired:
                err = f"timeout after {REGISTRATION_TIMEOUT}s"
            if err:
                print(f"ERROR: {err}")
                with db() as conn:
                    _upsert_apt_sequence(conn, session_id, subject_id, session_label,
                                          series_dir, series_uid, str(native_path),
                                          None, None, err)
                continue
            print("OK")

        with db() as conn:
            action = _upsert_apt_sequence(conn, session_id, subject_id, session_label,
                                           series_dir, series_uid, str(native_path),
                                           str(registered_path), str(transform_path), None)
        print(f"{prefix} DB {action}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run",    action="store_true", help="Scan only, make no changes.")
    parser.add_argument("--subject-id", help="Process only this subject_id.")
    parser.add_argument("--force",      action="store_true", help="Re-register even if output already exists.")
    args = parser.parse_args()
    backfill(dry_run=args.dry_run, filter_subject_id=args.subject_id, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
