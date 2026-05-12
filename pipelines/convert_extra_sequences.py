#!/usr/bin/env python3
"""
Converte le sequenze extra (DWI, DSC/perf, CBV/ktrans) in formato NIfTI canonico.

Per ogni sessione che ha T1ce (o T1) disponibile come raw DICOM:
  1. Converte il riferimento (T1ce, o T1 se manca T1ce) DICOM → NIfTI
  2. Converte ogni sequenza extra DICOM → NIfTI
  3. Ricampiona sulla griglia del riferimento (le serie sono già co-registrate nativamente)
  4. Aggiorna sequences.processed_path nel DB

Utilizzo:
  python pipelines/convert_extra_sequences.py
  python pipelines/convert_extra_sequences.py --subject-id GT-000042
  python pipelines/convert_extra_sequences.py --dry-run
  python pipelines/convert_extra_sequences.py --force
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import SimpleITK as sitk

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import db

PROCESSED_ROOT = PROJECT_ROOT / "processed" / "imported_sequences"

EXTRA_TYPES = {"DWI", "ADC", "DSC", "CBV", "CBF", "MTT", "SWAN"}
REFERENCE_TYPES = ["T1ce", "T1"]


def _read_dicom_series(dicom_dir: str) -> sitk.Image:
    reader = sitk.ImageSeriesReader()
    files = reader.GetGDCMSeriesFileNames(dicom_dir)
    if not files:
        raise RuntimeError(f"Nessuna serie DICOM in {dicom_dir}")
    reader.SetFileNames(files)
    return reader.Execute()


def _resample_to_reference(moving: sitk.Image, reference: sitk.Image) -> sitk.Image:
    resampler = sitk.ResampleImageFilter()
    resampler.SetReferenceImage(reference)
    resampler.SetInterpolator(sitk.sitkLinear)
    resampler.SetDefaultPixelValue(0.0)
    resampler.SetTransform(sitk.Transform())
    return resampler.Execute(moving)


def _t1ce_ref_path(subject_id: str, session_label: str) -> Path:
    return (
        PROCESSED_ROOT
        / subject_id
        / session_label
        / "t1ce_ref"
        / f"{subject_id}_{session_label}_t1ce_ref.nii.gz"
    )


def _output_path(subject_id: str, session_label: str, seq_type: str) -> Path:
    return (
        PROCESSED_ROOT
        / subject_id
        / session_label
        / seq_type.lower()
        / f"{subject_id}_{session_label}_{seq_type.lower()}.nii.gz"
    )


def _convert_session(
    conn,
    session_id: int,
    subject_id: str,
    session_label: str,
    dry_run: bool,
    force: bool,
) -> None:
    prefix = f"[{subject_id}/{session_label}]"

    # Sequenza di riferimento: T1ce se disponibile, altrimenti T1
    ref_seq = None
    ref_type_used = None
    for ref_type in REFERENCE_TYPES:
        row = conn.execute(
            "SELECT id, raw_path FROM sequences WHERE session_id = ? AND sequence_type = ?",
            (session_id, ref_type),
        ).fetchone()
        if row and row["raw_path"] and Path(row["raw_path"]).exists():
            ref_seq = dict(row)
            ref_type_used = ref_type
            break

    if ref_seq is None:
        print(f"{prefix} SKIP: nessuna T1ce/T1 con raw_path accessibile")
        return

    # Sequenze extra da processare
    placeholders = ",".join("?" * len(EXTRA_TYPES))
    extra_rows = conn.execute(
        f"SELECT id, sequence_type, raw_path, processed_path "
        f"FROM sequences WHERE session_id = ? AND sequence_type IN ({placeholders})",
        (session_id, *EXTRA_TYPES),
    ).fetchall()

    to_process = [
        dict(r) for r in extra_rows
        if r["raw_path"]
        and Path(r["raw_path"]).exists()
        and (force or not r["processed_path"])
    ]

    if not to_process:
        already = [r["sequence_type"] for r in extra_rows if r["processed_path"]]
        if already:
            print(f"{prefix} già processato: {already}  (usa --force per rielaborare)")
        else:
            print(f"{prefix} SKIP: nessuna sequenza extra con raw_path")
        return

    print(
        f"{prefix} ref={ref_type_used}  "
        f"da processare: {[r['sequence_type'] for r in to_process]}"
    )
    if dry_run:
        return

    # Converti il riferimento DICOM → NIfTI (una volta sola per sessione)
    ref_nifti_path = _t1ce_ref_path(subject_id, session_label)
    if ref_nifti_path.exists() and not force:
        ref_img = sitk.ReadImage(str(ref_nifti_path))
        print(f"{prefix}   riferimento già presente: {ref_nifti_path.name}")
    else:
        print(f"{prefix}   {ref_type_used} DICOM → NIfTI ...", end=" ", flush=True)
        try:
            ref_img = _read_dicom_series(ref_seq["raw_path"])
            ref_nifti_path.parent.mkdir(parents=True, exist_ok=True)
            sitk.WriteImage(ref_img, str(ref_nifti_path))
            print("OK")
        except Exception as exc:
            print(f"ERRORE: {exc}")
            return

    # Converti e ricampiona ogni sequenza extra
    for seq in to_process:
        seq_type = seq["sequence_type"]
        out_path = _output_path(subject_id, session_label, seq_type)

        print(f"{prefix}   {seq_type}: DICOM → NIfTI + resample ...", end=" ", flush=True)
        try:
            moving_img = _read_dicom_series(seq["raw_path"])
        except Exception as exc:
            print(f"ERRORE lettura DICOM: {exc}")
            continue

        try:
            resampled = _resample_to_reference(moving_img, ref_img)
        except Exception as exc:
            print(f"ERRORE resample: {exc}")
            continue

        out_path.parent.mkdir(parents=True, exist_ok=True)
        sitk.WriteImage(resampled, str(out_path))
        conn.execute(
            "UPDATE sequences SET processed_path = ? WHERE id = ?",
            (str(out_path), seq["id"]),
        )
        print(f"OK → {out_path.relative_to(PROJECT_ROOT)}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--subject-id", help="Processa solo questo subject_id")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Mostra cosa verrebbe fatto senza scrivere nulla"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Rielabora anche se processed_path è già impostato"
    )
    args = parser.parse_args()

    query = """
        SELECT ses.id AS session_id, ses.session_label, sub.subject_id
        FROM sessions ses
        JOIN subjects sub ON sub.id = ses.subject_id
    """
    params: list = []
    if args.subject_id:
        query += " WHERE sub.subject_id = ?"
        params.append(args.subject_id)
    query += " ORDER BY sub.subject_id, ses.session_label"

    with db() as conn:
        sessions = conn.execute(query, params).fetchall()

    print(f"Sessioni trovate: {len(sessions)}")
    for row in sessions:
        with db() as conn:
            _convert_session(
                conn,
                row["session_id"],
                row["subject_id"],
                row["session_label"],
                dry_run=args.dry_run,
                force=args.force,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
