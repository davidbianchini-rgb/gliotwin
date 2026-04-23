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

from app.services.fets_finalize import sync_fets_brain_outputs
from app.services.processing_jobs import dispatch_next_job
from app.services.pipeline_state import ensure_state, update_step

INPUT_ROOT = Path("/mnt/dati/irst_data/processing_jobs")
WRAPPER = Path(__file__).resolve().with_name("run_fets_patient.sh")
USE_GPU = os.environ.get("GLIOTWIN_FETS_USE_GPU", "1").strip().lower() in {"1", "true", "yes"}
GPU_ID = os.environ.get("GLIOTWIN_FETS_GPU_ID", "0").strip() or "0"
SQLITE_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30_000
SQLITE_RETRY_ATTEMPTS = 5
SQLITE_RETRY_DELAY_SECONDS = 1.0

CORE_IMPORT_CLASS = {
    "T1": "t1n",
    "T1ce": "t1c",
    "T2": "t2w",
    "FLAIR": "t2f",
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
    job_root.mkdir(parents=True, exist_ok=True)
    worker_log = job_root / "worker.log"
    worker_log.write_text("", encoding="utf-8")

    try:
        case = session_payload(conn, job_id)
        ensure_state(
            case["subject_id"],
            case["session_label"],
            session_id=case["session_id"],
            dataset="irst_dicom_raw",
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
            log.write(f"[job {job_id}] prepare_input\n")
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
                case["subject_id"],
                case["session_label"],
                "initial_validation",
                status="done",
                output_path=str(job_root / "input"),
                error_message=None,
                started=True,
                finished=True,
                session_id=case["session_id"],
                dataset="irst_dicom_raw",
            )
            update_step(
                case["subject_id"],
                case["session_label"],
                "nifti_conversion",
                status="done",
                output_path=str(run_dir / "output"),
                error_message=None,
                started=True,
                finished=True,
                session_id=case["session_id"],
                dataset="irst_dicom_raw",
            )
            update_step(
                case["subject_id"],
                case["session_label"],
                "brain_extraction",
                status="done",
                output_path=str(run_dir / "output"),
                error_message=None,
                started=True,
                finished=True,
                session_id=case["session_id"],
                dataset="irst_dicom_raw",
            )
            sync_result = sync_fets_brain_outputs(case["session_id"], str(run_dir))
            update_job(
                conn,
                job_id,
                status="completed",
                progress_stage="completed",
                run_dir=str(run_dir),
                final_dir=sync_result["processed_dir"],
                return_code=0,
            )
            log.write(f"FETS_OK {run_dir}\n")
        execute_with_retry(
            conn,
            """
            UPDATE processing_jobs
            SET status='completed',
                progress_stage='completed',
                run_dir=?,
                final_dir=?,
                return_code=0,
                finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id=?
            """,
            (str(run_dir), sync_result["processed_dir"], job_id),
            commit=True,
        )
        return 0
    except Exception as exc:
        case_ref = locals().get("case")
        if case_ref:
            update_step(
                case_ref["subject_id"],
                case_ref["session_label"],
                "tumor_segmentation",
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
