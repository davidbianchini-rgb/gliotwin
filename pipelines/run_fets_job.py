#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import SimpleITK as sitk

from app.services.fets_finalize import sync_fets_brain_outputs, FINAL_ROOT
from app.services.processing_jobs import dispatch_next_job
from app.services.pipeline_state import ensure_state, update_step
from app.services.rtstruct_import import scan_session_for_rtstruct

INPUT_ROOT = Path("/mnt/dati/irst_data/processing_jobs")
WRAPPER = Path(__file__).resolve().with_name("run_fets_patient.sh")
USE_GPU = os.environ.get("GLIOTWIN_FETS_USE_GPU", "1").strip().lower() in {"1", "true", "yes"}
GPU_ID = os.environ.get("GLIOTWIN_FETS_GPU_ID", "0").strip() or "0"
SQLITE_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30_000
SQLITE_RETRY_ATTEMPTS = 5
SQLITE_RETRY_DELAY_SECONDS = 1.0
MIN_FREE_BYTES = 5 * 1024 * 1024 * 1024

CORE_IMPORT_CLASS = {
    "T1": "t1n",
    "T1ce": "t1c",
    "T2": "t2w",
    "FLAIR": "t2f",
}

SITK_SUFFIX = {
    "T1":    "_t1n.nii.gz",
    "T1ce":  "_t1c.nii.gz",
    "T2":    "_t2w.nii.gz",
    "FLAIR": "_t2f.nii.gz",
    "APT":    "_apt.nii.gz",
    "DWI":    "_dwi.nii.gz",
    "Ktrans": "_ktrans.nii.gz",
    "nrCBV":  "_nrcbv.nii.gz",
}


def update_job(conn: sqlite3.Connection, job_id: int, **fields) -> None:
    if not fields:
        return
    pairs = ", ".join(f"{key} = ?" for key in fields)
    values = list(fields.values())
    values.append(job_id)
    execute_with_retry(
        conn,
        f"UPDATE processing_jobs SET {pairs}, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?",
        values,
        commit=True,
    )


def execute_with_retry(
    conn: sqlite3.Connection,
    sql: str,
    params=(),
    *,
    commit: bool = False,
):
    last_error = None
    for attempt in range(SQLITE_RETRY_ATTEMPTS):
        try:
            cursor = conn.execute(sql, params)
            if commit:
                conn.commit()
            return cursor
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            conn.rollback()
            last_error = exc
            if attempt == SQLITE_RETRY_ATTEMPTS - 1:
                break
            time.sleep(SQLITE_RETRY_DELAY_SECONDS * (attempt + 1))
    raise last_error


def session_payload(conn: sqlite3.Connection, job_id: int) -> dict:
    row = conn.execute(
        """
        SELECT
            pj.id AS job_id,
            ses.id AS session_id,
            ses.session_label,
            sub.subject_id
        FROM processing_jobs pj
        JOIN sessions ses ON ses.id = pj.session_id
        JOIN subjects sub ON sub.id = ses.subject_id
        WHERE pj.id = ?
        """,
        (job_id,),
    ).fetchone()
    if not row:
        raise RuntimeError(f"Job not found: {job_id}")
    sequences = conn.execute(
        """
        SELECT sequence_type, raw_path, processed_path
        FROM sequences
        WHERE session_id = ?
        """,
        (row["session_id"],),
    ).fetchall()
    seq_map = {item["sequence_type"]: dict(item) for item in sequences}
    return {
        "job_id": row["job_id"],
        "session_id": row["session_id"],
        "subject_id": row["subject_id"],
        "session_label": row["session_label"],
        "sequences": seq_map,
    }


def ensure_free_space(path: Path, minimum_bytes: int = MIN_FREE_BYTES) -> None:
    usage = shutil.disk_usage(path)
    if usage.free < minimum_bytes:
        raise RuntimeError(
            f"Insufficient free disk space on {path}: {usage.free / (1024**3):.2f} GB available, "
            f"{minimum_bytes / (1024**3):.0f} GB required"
        )


def prepare_input(case: dict, job_root: Path) -> Path:
    case_root = job_root / "input" / case["subject_id"] / case["session_label"]
    case_root.mkdir(parents=True, exist_ok=True)
    for sequence_type, folder_name in CORE_IMPORT_CLASS.items():
        src_dir = case["sequences"].get(sequence_type, {}).get("raw_path")
        if not src_dir:
            raise RuntimeError(f"Missing raw_path for {sequence_type}")
        src = Path(src_dir)
        if not src.exists():
            raise RuntimeError(f"Raw DICOM directory not found: {src}")
        dst = case_root / folder_name
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
    return job_root / "input"


def _read_dicom_series(dicom_dir: str) -> sitk.Image:
    reader = sitk.ImageSeriesReader()
    files = reader.GetGDCMSeriesFileNames(dicom_dir)
    if not files:
        raise RuntimeError(f"No DICOM files found in {dicom_dir}")
    reader.SetFileNames(files)
    return reader.Execute()


def _read_dicom_filtered(dicom_dir: str, imagetype_keyword: str) -> sitk.Image:
    """Legge una serie DICOM tenendo solo i file il cui ImageType contiene la keyword.

    Ordina per ImagePositionPatient.z (come GetGDCMSeriesFileNames), non per
    nome file, per garantire una ricostruzione 3D corretta.
    """
    import pydicom
    candidates = []
    for fname in os.listdir(dicom_dir):
        fpath = str(Path(dicom_dir) / fname)
        try:
            ds = pydicom.dcmread(fpath, stop_before_pixels=True)
            it = [x.upper() for x in getattr(ds, "ImageType", [])]
            if imagetype_keyword.upper() not in it:
                continue
            pos = getattr(ds, "ImagePositionPatient", None)
            z = float(pos[2]) if pos and len(pos) >= 3 else float(getattr(ds, "InstanceNumber", 0))
            candidates.append((z, fpath))
        except Exception:
            continue
    if not candidates:
        raise RuntimeError(
            f"Nessun file con ImageType '{imagetype_keyword}' in {dicom_dir}"
        )
    candidates.sort(key=lambda x: x[0])
    reader = sitk.ImageSeriesReader()
    reader.SetFileNames([fpath for _, fpath in candidates])
    return reader.Execute()


def _resample_to_1mm(img: sitk.Image) -> sitk.Image:
    orig_spacing = img.GetSpacing()
    orig_size = img.GetSize()
    new_spacing = [1.0, 1.0, 1.0]
    new_size = [int(round(orig_size[i] * orig_spacing[i])) for i in range(3)]
    resampler = sitk.ResampleImageFilter()
    resampler.SetOutputSpacing(new_spacing)
    resampler.SetSize(new_size)
    resampler.SetOutputDirection(img.GetDirection())
    resampler.SetOutputOrigin(img.GetOrigin())
    resampler.SetInterpolator(sitk.sitkLinear)
    resampler.SetDefaultPixelValue(0.0)
    resampler.SetTransform(sitk.Transform())
    return sitk.Cast(resampler.Execute(img), sitk.sitkFloat32)


def _outputs_are_ready(expected: dict[str, str]) -> bool:
    """True se tutti i file attesi esistono su disco e sono già a 1mm iso."""
    import nibabel as nib
    for path in expected.values():
        p = Path(path)
        if not p.exists():
            return False
        try:
            zooms = nib.load(str(p)).header.get_zooms()[:3]
            if not all(abs(z - 1.0) < 0.15 for z in zooms):
                return False
        except Exception:
            return False
    return True


def run_sitk_preprocessing(
    case: dict,
    log,
    conn: sqlite3.Connection,
) -> str:
    subject_id = case["subject_id"]
    session_label = case["session_label"]
    session_id = case["session_id"]
    sequences = case["sequences"]

    out_dir = FINAL_ROOT / subject_id / session_label
    out_dir.mkdir(parents=True, exist_ok=True)

    expected_outputs = {
        seq_type: str(out_dir / f"{subject_id}_{session_label}{suffix}")
        for seq_type, suffix in SITK_SUFFIX.items()
        if sequences.get(seq_type, {}).get("raw_path")
    }

    if _outputs_are_ready(expected_outputs):
        log.write(f"[sitk] output 1mm già presenti, skip conversione\n")
        for seq_type, out_path in expected_outputs.items():
            execute_with_retry(
                conn,
                "UPDATE sequences SET processed_path = ? WHERE session_id = ? AND sequence_type = ?",
                (out_path, session_id, seq_type),
                commit=True,
            )
        return str(out_dir)

    ref_type = next(
        (k for k in ("T1ce", "T1") if sequences.get(k, {}).get("raw_path")),
        None,
    )
    if not ref_type:
        raise RuntimeError("No reference sequence (T1ce or T1) available")

    log.write(f"[sitk] reference={ref_type}\n")
    log.flush()

    ref_img_native = _read_dicom_series(sequences[ref_type]["raw_path"])
    ref_img = _resample_to_1mm(ref_img_native)
    ref_out = out_dir / f"{subject_id}_{session_label}{SITK_SUFFIX[ref_type]}"
    sitk.WriteImage(ref_img, str(ref_out))
    log.write(f"[sitk] {ref_type} → {ref_out.name} shape={ref_img.GetSize()} spacing={ref_img.GetSpacing()}\n")
    execute_with_retry(
        conn,
        "UPDATE sequences SET processed_path = ? WHERE session_id = ? AND sequence_type = ?",
        (str(ref_out), session_id, ref_type),
        commit=True,
    )

    for seq_type, suffix in SITK_SUFFIX.items():
        if seq_type == ref_type:
            continue
        raw_path = sequences.get(seq_type, {}).get("raw_path")
        if not raw_path:
            continue
        log.write(f"[sitk] {seq_type}: DICOM → NIfTI 1mm float32 ...\n")
        log.flush()
        if seq_type == "APT":
            moving = _read_dicom_filtered(raw_path, "APTW")
        else:
            moving = _read_dicom_series(raw_path)
        resampler = sitk.ResampleImageFilter()
        resampler.SetReferenceImage(ref_img)
        resampler.SetInterpolator(sitk.sitkLinear)
        resampler.SetDefaultPixelValue(0.0)
        resampler.SetTransform(sitk.Transform())
        resampled = sitk.Cast(resampler.Execute(moving), sitk.sitkFloat32)
        out_path = out_dir / f"{subject_id}_{session_label}{suffix}"
        sitk.WriteImage(resampled, str(out_path))
        log.write(f"[sitk] {seq_type} → {out_path.name}\n")
        execute_with_retry(
            conn,
            "UPDATE sequences SET processed_path = ? WHERE session_id = ? AND sequence_type = ?",
            (str(out_path), session_id, seq_type),
            commit=True,
        )

    log.write(f"[sitk] done → {out_dir}\n")
    return str(out_dir)


def discover_run_dir(run_root: Path, subject_id: str) -> Path:
    matches = sorted(run_root.glob(f"{subject_id}_*"))
    if not matches:
        raise RuntimeError(f"No FeTS run directory created under {run_root}")
    return matches[-1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", type=int, required=True)
    parser.add_argument("--db-path", type=Path, required=True)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")

    job_id = args.job_id
    job_root = INPUT_ROOT / f"job_{job_id:05d}"
    if job_root.exists():
        shutil.rmtree(job_root)
    job_root.mkdir(parents=True, exist_ok=True)
    worker_log = job_root / "worker.log"
    worker_log.write_text("", encoding="utf-8")

    try:
        case = session_payload(conn, job_id)
        ensure_free_space(INPUT_ROOT)
        ensure_state(
            case["subject_id"],
            case["session_label"],
            session_id=case["session_id"],
            dataset="irst_dicom_raw",
        )

        fets_capable = all(
            case["sequences"].get(k, {}).get("raw_path")
            for k in ("T1", "T1ce", "T2", "FLAIR")
        )

        update_job(
            conn,
            job_id,
            progress_stage="preparing_input",
            input_dir=str(job_root / "input"),
            run_dir=str(job_root / "runs"),
            log_path=str(worker_log),
            error_message=None,
        )
        with worker_log.open("a", encoding="utf-8") as log:
            if fets_capable:
                log.write(f"[job {job_id}] mode=fets\n")
                input_root = prepare_input(case, job_root)
                run_root = job_root / "runs"
                run_root.mkdir(parents=True, exist_ok=True)
                update_job(conn, job_id, progress_stage="running_fets")
                cmd = [
                    str(WRAPPER),
                    "--paziente",
                    case["subject_id"],
                    "--input",
                    str(input_root),
                    "--output",
                    str(run_root),
                    "--mode",
                    "brain",
                ]
                if USE_GPU:
                    cmd.extend(["--gpu", GPU_ID])
                else:
                    cmd.append("--no-gpu")
                log.write("[job {}] {}\n".format(job_id, " ".join(cmd)))
                log.flush()
                proc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, text=True)
                if proc.returncode != 0:
                    raise RuntimeError(f"FeTS wrapper failed with exit code {proc.returncode}")

                update_job(conn, job_id, progress_stage="completed")
                run_dir = discover_run_dir(run_root, case["subject_id"])
                update_step(
                    case["subject_id"], case["session_label"], "initial_validation",
                    status="done", output_path=str(job_root / "input"),
                    error_message=None, started=True, finished=True,
                    session_id=case["session_id"], dataset="irst_dicom_raw",
                )
                update_step(
                    case["subject_id"], case["session_label"], "nifti_conversion",
                    status="done", output_path=str(run_dir / "output"),
                    error_message=None, started=True, finished=True,
                    session_id=case["session_id"], dataset="irst_dicom_raw",
                )
                update_step(
                    case["subject_id"], case["session_label"], "brain_extraction",
                    status="done", output_path=str(run_dir / "output"),
                    error_message=None, started=True, finished=True,
                    session_id=case["session_id"], dataset="irst_dicom_raw",
                )
                sync_result = sync_fets_brain_outputs(case["session_id"], str(run_dir))
                update_job(
                    conn, job_id,
                    status="completed", progress_stage="completed",
                    run_dir=str(run_dir), final_dir=sync_result["processed_dir"],
                    return_code=0,
                )
                log.write(f"FETS_OK {run_dir}\n")
                execute_with_retry(
                    conn,
                    """
                    UPDATE processing_jobs
                    SET status='completed', progress_stage='completed',
                        run_dir=?, final_dir=?, return_code=0,
                        finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                        updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    WHERE id=?
                    """,
                    (str(run_dir), sync_result["processed_dir"], job_id),
                    commit=True,
                )
                try:
                    rts = scan_session_for_rtstruct(case["session_id"], case["subject_id"], case["session_label"])
                    if rts:
                        log.write(f"[rtstruct] importati {sum(r.get('inserted',0)+r.get('updated',0) for r in rts)} ROI\n")
                except Exception as _rts_exc:
                    log.write(f"[rtstruct] warning: {_rts_exc}\n")
            else:
                log.write(f"[job {job_id}] mode=sitk (T1/T2 not available)\n")
                update_job(conn, job_id, progress_stage="nifti_conversion")
                out_dir = run_sitk_preprocessing(case, log, conn)
                update_step(
                    case["subject_id"], case["session_label"], "initial_validation",
                    status="done", output_path=out_dir,
                    error_message=None, started=True, finished=True,
                    session_id=case["session_id"], dataset="irst_dicom_raw",
                )
                update_step(
                    case["subject_id"], case["session_label"], "nifti_conversion",
                    status="done", output_path=out_dir,
                    error_message=None, started=True, finished=True,
                    session_id=case["session_id"], dataset="irst_dicom_raw",
                )
                log.write(f"SITK_OK {out_dir}\n")
                execute_with_retry(
                    conn,
                    """
                    UPDATE processing_jobs
                    SET status='completed', progress_stage='completed',
                        run_dir=?, final_dir=?, return_code=0,
                        finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                        updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    WHERE id=?
                    """,
                    (out_dir, out_dir, job_id),
                    commit=True,
                )
                try:
                    rts = scan_session_for_rtstruct(case["session_id"], case["subject_id"], case["session_label"])
                    if rts:
                        log.write(f"[rtstruct] importati {sum(r.get('inserted',0)+r.get('updated',0) for r in rts)} ROI\n")
                except Exception as _rts_exc:
                    log.write(f"[rtstruct] warning: {_rts_exc}\n")
        return 0
    except Exception as exc:
        case_ref = locals().get("case")
        if case_ref:
            update_step(
                case_ref["subject_id"],
                case_ref["session_label"],
                "brain_extraction",
                status="failed",
                output_path=str(job_root),
                error_message=str(exc),
                started=True,
                finished=True,
                session_id=case_ref["session_id"],
                dataset="irst_dicom_raw",
            )
        with worker_log.open("a", encoding="utf-8") as log:
            log.write(f"ERROR {exc}\n")
        execute_with_retry(
            conn,
            """
            UPDATE processing_jobs
            SET status='failed',
                progress_stage='failed',
                error_message=?,
                finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id=?
            """,
            (str(exc), job_id),
            commit=True,
        )
        return 1
    finally:
        conn.close()
        try:
            dispatch_next_job()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
