from __future__ import annotations

import os
import signal
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException

from app.db import DB_PATH, db, rows_as_dicts
from app.services.fets_finalize import sync_fets_brain_outputs, sync_fets_outputs
from app.services.pipeline_state import ensure_state, load_state, pipeline_state_summary, reset_downstream_steps, update_step

RUNNER_PATH = Path(__file__).resolve().parents[2] / "pipelines" / "run_fets_job.py"
PYTHON_BIN = Path("/home/irst/miniconda3/bin/python")
TARGET_DATASET = "irst_dicom_raw"
CORE_SEQUENCE_TYPES = ("T1", "T1ce", "T2", "FLAIR")

STEP_KEYS = [
    "import_dicom",
    "select_core_sequences",
    "initial_validation",
    "nifti_conversion",
    "brain_extraction",
    "tumor_segmentation",
]


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _elapsed_seconds(started_at: str | None) -> int | None:
    start = _parse_ts(started_at)
    if not start:
        return None
    return max(int((datetime.now(timezone.utc) - start.astimezone(timezone.utc)).total_seconds()), 0)


def _session_row(conn, session_id: int):
    row = conn.execute(
        """
        SELECT
            ses.*,
            sub.subject_id AS patient_code,
            sub.patient_name,
            sub.patient_given_name,
            sub.patient_family_name,
            sub.patient_birth_date,
            sub.dataset,
            sub.id AS patient_pk
        FROM sessions ses
        JOIN subjects sub ON sub.id = ses.subject_id
        WHERE ses.id = ?
        """,
        (session_id,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "Session not found")
    return row


def _job_row(conn, job_id: int):
    row = conn.execute("SELECT * FROM processing_jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Processing job not found")
    return row


def _pid_is_running(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _pid_cmdline(pid: int | None) -> str:
    if not pid:
        return ""
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except Exception:
        return ""
    return raw.replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()


def _pipeline_log_path(job: dict) -> Path | None:
    run_dir = job.get("run_dir")
    patient_code = job.get("patient_code")
    if not run_dir or not patient_code:
        return None
    run_root = Path(run_dir)
    if run_root.is_file():
        return run_root
    if run_root.name.startswith(f"{patient_code}_"):
        candidate = run_root / "pipeline.log"
        return candidate if candidate.exists() else None
    matches = sorted(run_root.glob(f"{patient_code}_*/pipeline.log"))
    return matches[-1] if matches else None


def _resolved_run_dir(job: dict) -> Path | None:
    run_dir = job.get("run_dir")
    patient_code = job.get("patient_code")
    if not run_dir or not patient_code:
        return None
    run_root = Path(run_dir)
    if not run_root.exists():
        return None
    if run_root.name.startswith(f"{patient_code}_"):
        return run_root
    matches = sorted(run_root.glob(f"{patient_code}_*"))
    return matches[-1] if matches else None


def _segmentation_output_exists(job: dict) -> bool:
    run_dir = _resolved_run_dir(job)
    patient_code = job.get("patient_code")
    session_label = job.get("session_label")
    if not run_dir or not patient_code or not session_label:
        return False
    case_id = f"{patient_code}-{session_label}"
    candidates = [
        run_dir / "output_labels" / f"{case_id}.nii.gz",
        run_dir / "output_labels" / f"{case_id}_tumorMask.nii.gz",
        run_dir / "output" / "tumor_extracted" / "tmp-out" / f"{case_id}.nii.gz",
        run_dir / "output" / "tumor_extracted" / "tmp-out" / f"{case_id}_tumorMask.nii.gz",
        run_dir / "output" / "tumor_extracted" / "DataForQC" / patient_code / session_label / "TumorMasksForQC" / f"{patient_code}_{session_label}_tumorMask.nii.gz",
        run_dir / "output" / "tumor_extracted" / "DataForQC" / patient_code / session_label / "TumorMasksForQC" / f"{patient_code}_{session_label}_tumorMask_model_0.nii.gz",
    ]
    return any(path.exists() for path in candidates)


def _report_snapshot(job: dict) -> dict | None:
    run_dir = _resolved_run_dir(job)
    if not run_dir:
        return None
    report_path = run_dir / "report.yaml"
    if not report_path.exists():
        return None
    try:
        lines = report_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return None

    status_map: dict[str, str] = {}
    in_status = False
    for raw_line in lines:
        if not raw_line.strip():
            continue
        if not raw_line.startswith(" "):
            in_status = raw_line.strip() == "status:"
            continue
        if not in_status:
            continue
        line = raw_line.strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        status_map[key.strip()] = value.strip()

    case_key = f"{job.get('patient_code')}|{job.get('session_label')}"
    raw_status = status_map.get(case_key)
    if raw_status is None and status_map:
        raw_status = next(iter(status_map.values()))
    try:
        status_code = int(float(raw_status))
    except Exception:
        status_code = None
    report_ts = datetime.fromtimestamp(report_path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    return {"status_code": status_code, "reported_at": report_ts, "path": str(report_path)}


def _derive_failure_message(job: dict, default_message: str | None = None) -> str:
    text = _combined_job_text(job)
    stage = job.get("progress_stage")
    fallback = default_message or "Worker exited without final status"

    if "ERROR " in text:
      errors = [line.strip() for line in text.splitlines() if line.strip().startswith("ERROR ")]
      if errors:
          return errors[-1][6:].strip() or fallback

    if "Traceback" in text:
        return "Pipeline crashed during execution; check the log traceback."

    if stage in {"tumor_extraction", "tumor_preprocessing", "tumor_prediction"} and not _segmentation_output_exists(job):
        return "Tumor segmentation stopped before producing the final mask output."

    if "This worker has ended successfully, no errors to report" in text:
        return "FeTS worker exited without publishing a final completion marker or final segmentation output."

    return fallback


def _job_process_alive(job: dict) -> bool:
    pid = job.get("pid")
    if not _pid_is_running(pid):
        return False

    cmdline = _pid_cmdline(pid)
    if "run_fets_job.py" not in cmdline:
        return False

    text = _combined_job_text(job)
    if "COMPLETATO con successo" in text or "FINALIZE_OK" in text:
        return False

    if (
        "This worker has ended successfully, no errors to report" in text
        and not _segmentation_output_exists(job)
    ):
        return False

    return True


def _can_finalize_completed_job(job: dict) -> bool:
    if not (job.get("session_id") and job.get("run_dir") and _segmentation_output_exists(job)):
        return False
    if job.get("status") == "completed" and job.get("final_dir"):
        return False
    return True


def _finalize_completed_job(conn, job: dict) -> dict | None:
    if not _can_finalize_completed_job(job):
        return None
    try:
        sync_result = sync_fets_outputs(int(job["session_id"]), str(job["run_dir"]))
    except Exception:
        return None

    conn.execute(
        """
        UPDATE processing_jobs
        SET status = 'completed',
            progress_stage = 'completed',
            final_dir = ?,
            return_code = 0,
            error_message = NULL,
            finished_at = COALESCE(finished_at, strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ?
        """,
        (sync_result.get("processed_dir"), job["id"]),
    )
    return dict(conn.execute("SELECT * FROM processing_jobs WHERE id = ?", (job["id"],)).fetchone())


def _case_identity(job: dict) -> tuple[str | None, str | None]:
    return job.get("patient_code"), job.get("session_label")


def _needs_brain_output_sync(conn, job: dict) -> bool:
    session_id = job.get("session_id")
    if not session_id:
        return False
    rows = conn.execute(
        """
        SELECT sequence_type, processed_path
        FROM sequences
        WHERE session_id = ?
        """,
        (session_id,),
    ).fetchall()
    core_rows = {row["sequence_type"]: row["processed_path"] for row in rows if row["sequence_type"] in CORE_SEQUENCE_TYPES}
    return any(not core_rows.get(sequence_type) for sequence_type in CORE_SEQUENCE_TYPES)


def _sync_pipeline_state(conn, job: dict) -> dict | None:
    subject_id, session_label = _case_identity(job)
    if not subject_id or not session_label:
        return None

    state = ensure_state(
        subject_id,
        session_label,
        session_id=job.get("session_id"),
        dataset=TARGET_DATASET,
        conn=conn,
    )
    status = job.get("status")
    stage = job.get("progress_stage")
    input_dir = job.get("input_dir")
    run_dir = job.get("run_dir")
    final_dir = job.get("final_dir")
    error_message = job.get("error_message") or "Pipeline step failed"
    report = _report_snapshot(job)

    def mark_done(step_key: str, output_path: str | None = None, *, started_at_value: str | None = None, finished_at_value: str | None = None) -> None:
        update_step(
            subject_id,
            session_label,
            step_key,
            status="done",
            output_path=output_path,
            error_message=None,
            started=True,
            finished=True,
            started_at_value=started_at_value,
            finished_at_value=finished_at_value,
            session_id=job.get("session_id"),
            dataset=TARGET_DATASET,
            conn=conn,
        )

    def mark_running(step_key: str, output_path: str | None = None, *, started_at_value: str | None = None) -> None:
        update_step(
            subject_id,
            session_label,
            step_key,
            status="running",
            output_path=output_path,
            error_message=None,
            started=True,
            finished=False,
            started_at_value=started_at_value,
            session_id=job.get("session_id"),
            dataset=TARGET_DATASET,
            conn=conn,
        )

    def mark_failed(step_key: str, output_path: str | None = None) -> None:
        update_step(
            subject_id,
            session_label,
            step_key,
            status="failed",
            output_path=output_path,
            error_message=error_message,
            started=True,
            finished=True,
            session_id=job.get("session_id"),
            dataset=TARGET_DATASET,
            conn=conn,
        )

    if status == "queued":
        return load_state(subject_id, session_label, conn=conn)

    if report and status == "running" and report.get("status_code") is not None:
        report_code = report["status_code"]
        report_ts = report.get("reported_at")
        job_start = job.get("started_at")
        if report_code <= 0:
            mark_running("initial_validation", input_dir, started_at_value=job_start)
            return load_state(subject_id, session_label, conn=conn)
        if report_code == 1:
            mark_done("initial_validation", input_dir, started_at_value=job_start, finished_at_value=report_ts)
            mark_running("nifti_conversion", run_dir or input_dir, started_at_value=report_ts)
            return load_state(subject_id, session_label, conn=conn)
        if report_code == 2:
            mark_done("initial_validation", input_dir, started_at_value=job_start, finished_at_value=report_ts)
            mark_done("nifti_conversion", run_dir or input_dir, started_at_value=report_ts, finished_at_value=report_ts)
            mark_running("brain_extraction", run_dir, started_at_value=report_ts)
            return load_state(subject_id, session_label, conn=conn)
        if report_code >= 3:
            mark_done("initial_validation", input_dir, started_at_value=job_start, finished_at_value=report_ts)
            mark_done("nifti_conversion", run_dir or input_dir, started_at_value=report_ts, finished_at_value=report_ts)
            mark_done("brain_extraction", run_dir, started_at_value=report_ts, finished_at_value=report_ts)
            return load_state(subject_id, session_label, conn=conn)

    if stage in {"preparing_input", "queued", "running_fets"} and status == "running":
        return load_state(subject_id, session_label, conn=conn)

    if stage == "initial_validation":
        mark_running("initial_validation", run_dir or input_dir)
        return load_state(subject_id, session_label, conn=conn)

    if stage == "nifti_conversion":
        mark_done("initial_validation", input_dir)
        mark_running("nifti_conversion", run_dir or input_dir)
        return load_state(subject_id, session_label, conn=conn)

    if stage == "brain_extraction":
        mark_done("initial_validation", input_dir)
        mark_done("nifti_conversion", run_dir or input_dir)
        mark_running("brain_extraction", run_dir)
        return load_state(subject_id, session_label, conn=conn)

    if stage in {"brain_queue", "brain_inference"}:
        mark_done("initial_validation", input_dir)
        mark_done("nifti_conversion", run_dir or input_dir)
        mark_running("brain_extraction", run_dir)
        return load_state(subject_id, session_label, conn=conn)

    if stage in {"tumor_extraction", "tumor_preprocessing", "tumor_prediction"}:
        mark_done("initial_validation", input_dir)
        mark_done("nifti_conversion", run_dir)
        mark_done("brain_extraction", run_dir)
        mark_running("tumor_segmentation", run_dir)
        return load_state(subject_id, session_label, conn=conn)

    if status == "completed":
        mark_done("initial_validation", input_dir)
        mark_done("nifti_conversion", run_dir)
        mark_done("brain_extraction", run_dir)
        return load_state(subject_id, session_label, conn=conn)

    if status in {"failed", "cancelled"}:
        failed_step = "tumor_segmentation"
        output_path = final_dir or run_dir or input_dir
        if stage in {"preparing_input", "queued", "initial_validation"}:
            failed_step = "initial_validation"
            output_path = input_dir
        elif stage == "nifti_conversion":
            mark_done("initial_validation", input_dir)
            failed_step = "nifti_conversion"
            output_path = run_dir or input_dir
        elif stage == "brain_extraction":
            mark_done("initial_validation", input_dir)
            mark_done("nifti_conversion", run_dir or input_dir)
            failed_step = "brain_extraction"
            output_path = run_dir
        elif stage in {"brain_queue", "brain_inference"}:
            mark_done("initial_validation", input_dir)
            mark_done("nifti_conversion", run_dir or input_dir)
            failed_step = "brain_extraction"
            output_path = run_dir
        elif stage in {"tumor_extraction", "tumor_preprocessing", "tumor_prediction"}:
            mark_done("initial_validation", input_dir)
            mark_done("nifti_conversion", run_dir)
            mark_done("brain_extraction", run_dir)
            failed_step = "tumor_segmentation"
            output_path = run_dir
        mark_failed(failed_step, output_path)
        return load_state(subject_id, session_label, conn=conn)

    return state


def _combined_job_text(job: dict) -> str:
    parts = []
    log_path = job.get("log_path")
    if log_path:
        worker_path = Path(log_path)
        if worker_path.exists():
            parts.append(worker_path.read_text(encoding="utf-8", errors="replace"))
    pipeline_path = _pipeline_log_path(job)
    if pipeline_path and pipeline_path.exists():
        parts.append(pipeline_path.read_text(encoding="utf-8", errors="replace"))
    return "\n".join(parts)


def _infer_progress_stage(job: dict) -> str | None:
    report = _report_snapshot(job)
    if job.get("status") == "running" and report and report.get("status_code") is not None:
        status_code = report["status_code"]
        if status_code <= 0:
            return "initial_validation"
        if status_code == 1:
            return "nifti_conversion"
        if status_code == 2:
            return "brain_extraction"
        if status_code >= 3:
            return "completed"

    text = _combined_job_text(job)
    if text:
        if "FINALIZE_OK" in text:
            return "finalizing"
        if "predicting /output/" in text and "nnUNet Tumor Extraction" in text:
            return "tumor_prediction"
        if "starting prediction..." in text and "nnUNet Tumor Extraction" in text:
            return "tumor_preprocessing"
        if "nnUNet Tumor Extraction" in text:
            return "tumor_extraction"
        if "Looping over inference data:" in text and "Brain Extraction:" in text:
            return "brain_inference"
        if "Constructing queue for testing data:" in text and "Brain Extraction:" in text:
            return "brain_queue"
        if "Brain Extraction:" in text or "Extract brain" in text:
            return "brain_extraction"
        if "Running BraTSPipeline:" in text or "Saving screenshot:" in text or "Processing " in text:
            return "nifti_conversion"
        if "Processing " in text and "NiFTI Conversion" in text:
            return "nifti_conversion"
        if "NiFTI Conversion" in text:
            return "nifti_conversion"
        if "Initial Validation" in text:
            return "initial_validation"
    return job.get("progress_stage")


def _stage_label(stage: str | None) -> str | None:
    mapping = {
        "queued": "Queued",
        "preparing_input": "Preparing Input",
        "running_fets": "Preprocessing",
        "initial_validation": "Initial Validation",
        "nifti_conversion": "DICOM to NIfTI",
        "brain_extraction": "Brain Extraction",
        "brain_queue": "Brain Extraction Queue",
        "brain_inference": "Brain Extraction Inference",
        "tumor_extraction": "Tumor Segmentation",
        "tumor_preprocessing": "Tumor Segmentation Preprocessing",
        "tumor_prediction": "Tumor Segmentation Prediction",
        "finalizing": "Finalizing",
        "completed": "Completed",
        "cancelled": "Cancelled",
        "failed": "Failed",
    }
    return mapping.get(stage, stage)


def _running_job_exists(conn) -> bool:
    rows = conn.execute("SELECT * FROM processing_jobs WHERE status = 'running'").fetchall()
    active = False
    for row in rows:
        synced = _sync_job_state(conn, row)
        if synced["status"] == "running":
            active = True
    return active


def _start_job_record(conn, job_id: int) -> dict:
    job_row = _job_row(conn, job_id)
    job = _sync_job_state(conn, job_row)
    if job["status"] == "running":
        return job
    if job["status"] == "completed":
        return job
    if _running_job_exists(conn):
        return job

    cmd = [str(PYTHON_BIN), str(RUNNER_PATH), "--job-id", str(job_id), "--db-path", str(DB_PATH)]
    proc = subprocess.Popen(
        cmd,
        cwd=str(RUNNER_PATH.parent.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    conn.execute(
        """
        UPDATE processing_jobs
        SET status = 'running',
            progress_stage = 'queued',
            pid = ?,
            started_at = COALESCE(started_at, strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ?
        """,
        (proc.pid, job_id),
    )
    return dict(conn.execute("SELECT * FROM processing_jobs WHERE id = ?", (job_id,)).fetchone())


def dispatch_next_job() -> dict:
    with db() as conn:
        if _running_job_exists(conn):
            rows = conn.execute(
                """
                SELECT
                    pj.*,
                    ses.session_label,
                    ses.study_date,
                    ses.study_time,
                    sub.subject_id AS patient_code,
                    sub.patient_name,
                    sub.patient_given_name,
                    sub.patient_family_name,
                    sub.patient_birth_date
                FROM processing_jobs pj
                JOIN sessions ses ON ses.id = pj.session_id
                JOIN subjects sub ON sub.id = ses.subject_id
                WHERE pj.status = 'running'
                ORDER BY pj.id ASC
                LIMIT 1
                """
            ).fetchall()
            return {"started": False, "job": dict(rows[0]) if rows else None}

        next_job = conn.execute(
            """
            SELECT * FROM processing_jobs
            WHERE status = 'queued'
            ORDER BY id ASC
            LIMIT 1
            """
        ).fetchone()
        if not next_job:
            return {"started": False, "job": None}
        started = _start_job_record(conn, int(next_job["id"]))
        return {"started": True, "job": started}


def _sync_job_state(conn, job_row) -> dict:
    job = dict(job_row)
    inferred_stage = _infer_progress_stage(job)
    lock_terminal_stage = job.get("status") in {"failed", "completed", "cancelled"} and job.get("progress_stage") in {"failed", "completed", "cancelled"}
    if inferred_stage and inferred_stage != job.get("progress_stage") and not lock_terminal_stage:
        conn.execute(
            """
            UPDATE processing_jobs
            SET progress_stage = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (inferred_stage, job["id"]),
        )
        job["progress_stage"] = inferred_stage

    if (
        job.get("status") == "running"
        and job.get("session_id")
        and job.get("run_dir")
        and job.get("progress_stage") in {
        "tumor_extraction",
        "tumor_preprocessing",
        "tumor_prediction",
        "completed",
    }
        and _needs_brain_output_sync(conn, job)
    ):
        try:
            sync_fets_brain_outputs(int(job["session_id"]), str(job["run_dir"]))
        except Exception:
            pass

    is_alive = _job_process_alive(job)
    if is_alive:
        if job.get("status") != "running":
            conn.execute(
                """
                UPDATE processing_jobs
                SET status = 'running',
                    error_message = NULL,
                    finished_at = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE id = ?
                """,
                (job["id"],),
            )
            job["status"] = "running"
            job["error_message"] = None
            job["finished_at"] = None
        job["progress_label"] = _stage_label(job.get("progress_stage"))
        pipeline_state = _sync_pipeline_state(conn, job)
        if pipeline_state:
            job["pipeline_state"] = pipeline_state
            job["pipeline_summary"] = pipeline_state_summary(pipeline_state)
        return job

    finalized = _finalize_completed_job(conn, job)
    if finalized is not None:
        finalized["progress_label"] = _stage_label(finalized.get("progress_stage"))
        pipeline_state = _sync_pipeline_state(conn, finalized)
        if pipeline_state:
            finalized["pipeline_state"] = pipeline_state
            finalized["pipeline_summary"] = pipeline_state_summary(pipeline_state)
        return finalized

    if job["status"] != "running":
        job["progress_label"] = _stage_label(job.get("progress_stage"))
        pipeline_state = _sync_pipeline_state(conn, job)
        if pipeline_state:
            job["pipeline_state"] = pipeline_state
            job["pipeline_summary"] = pipeline_state_summary(pipeline_state)
        return job

    log_path = job.get("log_path")
    error_message = job.get("error_message")
    status = "failed"
    return_code = job.get("return_code")
    if log_path and Path(log_path).exists():
        try:
            tail = Path(log_path).read_text(encoding="utf-8", errors="replace")[-4000:]
            if "COMPLETATO con successo" in tail or "FINALIZE_OK" in tail:
                status = "completed"
                return_code = 0 if return_code is None else return_code
                error_message = None
            elif not error_message:
                error_message = tail.strip().splitlines()[-1] if tail.strip() else None
        except Exception:
            pass

    if status != "completed":
        error_message = _derive_failure_message(job, error_message)

    conn.execute(
        """
        UPDATE processing_jobs
        SET status = ?, progress_stage = CASE WHEN ? = 'completed' THEN progress_stage ELSE 'failed' END,
            return_code = COALESCE(return_code, ?),
            error_message = ?, finished_at = COALESCE(finished_at, strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ?
        """,
        (status, status, return_code, error_message, job["id"]),
    )

    updated = dict(conn.execute("SELECT * FROM processing_jobs WHERE id = ?", (job["id"],)).fetchone())
    updated["progress_label"] = _stage_label(updated.get("progress_stage"))
    pipeline_state = _sync_pipeline_state(conn, updated)
    if pipeline_state:
        updated["pipeline_state"] = pipeline_state
        updated["pipeline_summary"] = pipeline_state_summary(pipeline_state)
    return updated


def _core_status_for_session(session_row: dict, sequences: list[dict], jobs: list[dict], computed_count: int) -> dict:
    core_map = {seq["sequence_type"]: seq for seq in sequences if seq["sequence_type"] in CORE_SEQUENCE_TYPES}
    has_all_core = all(k in core_map and core_map[k].get("raw_path") for k in CORE_SEQUENCE_TYPES)
    has_processed = all(k in core_map and core_map[k].get("processed_path") for k in CORE_SEQUENCE_TYPES)
    latest_job = jobs[0] if jobs else None

    if latest_job and latest_job["status"] == "running":
        operational_status = "running"
    elif latest_job and latest_job["status"] == "queued":
        operational_status = "queued"
    elif has_processed and computed_count > 0:
        operational_status = "completed"
    elif latest_job and latest_job["status"] == "failed":
        operational_status = "failed"
    elif latest_job and latest_job["status"] == "cancelled":
        operational_status = "cancelled"
    elif has_all_core:
        operational_status = "ready"
    else:
        operational_status = "incomplete"

    return {
        "has_all_core": has_all_core,
        "has_processed": has_processed,
        "operational_status": operational_status,
        "latest_job": latest_job,
    }


def create_processing_job(session_id: int) -> dict:
    with db() as conn:
        session = _session_row(conn, session_id)
        active = conn.execute(
            """
            SELECT * FROM processing_jobs
            WHERE session_id = ? AND status IN ('queued', 'running')
            ORDER BY id DESC
            LIMIT 1
            """,
            (session_id,),
        ).fetchone()
        if active:
            active = _sync_job_state(conn, active)
            if active["status"] in ("queued", "running"):
                return active

        reset_downstream_steps(
            session["patient_code"],
            session["session_label"],
            ["initial_validation", "nifti_conversion", "brain_extraction", "tumor_segmentation"],
            conn=conn,
        )

        conn.execute(
            """
            INSERT INTO processing_jobs (session_id, job_type, status, progress_stage)
            VALUES (?, 'fets_postop', 'queued', 'queued')
            """,
            (session_id,),
        )
        job = conn.execute("SELECT * FROM processing_jobs WHERE id = last_insert_rowid()").fetchone()
        return {
            **dict(job),
            "session_label": session["session_label"],
            "study_date": session["study_date"],
            "study_time": session["study_time"],
            "patient_code": session["patient_code"],
            "patient_name": session["patient_name"],
            "patient_given_name": session["patient_given_name"],
            "patient_family_name": session["patient_family_name"],
            "patient_birth_date": session["patient_birth_date"],
            "pipeline_state": load_state(session["patient_code"], session["session_label"], conn=conn),
        }


def queue_processing_jobs(session_ids: list[int]) -> dict:
    queued = []
    for session_id in session_ids:
        queued.append(create_processing_job(session_id))
    dispatch = dispatch_next_job()
    return {"queued_jobs": queued, "dispatch": dispatch}


def queue_all_unprocessed_jobs(dataset: str = TARGET_DATASET) -> dict:
    candidate_ids: list[int] = []
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
                ses.id AS session_id,
                ses.session_label,
                ses.study_date,
                ses.study_time,
                ses.processed_dir,
                ses.quality_flag,
                sub.id AS patient_id,
                sub.subject_id AS patient_code,
                sub.patient_name,
                sub.patient_given_name,
                sub.patient_family_name,
                sub.patient_birth_date,
                sub.dataset
            FROM sessions ses
            JOIN subjects sub ON sub.id = ses.subject_id
            WHERE sub.dataset = ?
            ORDER BY sub.subject_id, ses.session_label
            """,
            (dataset,),
        ).fetchall()

        for row in rows:
            seq_rows = conn.execute(
                """
                SELECT sequence_type, raw_path, processed_path, display_label, import_class
                FROM sequences
                WHERE session_id = ?
                ORDER BY sequence_type
                """,
                (row["session_id"],),
            ).fetchall()
            sequences = rows_as_dicts(seq_rows)
            computed_count = conn.execute(
                "SELECT COUNT(*) FROM computed_structures WHERE session_id = ?",
                (row["session_id"],),
            ).fetchone()[0]
            job_rows = conn.execute(
                "SELECT * FROM processing_jobs WHERE session_id = ? ORDER BY id DESC",
                (row["session_id"],),
            ).fetchall()
            jobs = [_sync_job_state(conn, job_row) for job_row in job_rows]
            status_info = _core_status_for_session(dict(row), sequences, jobs, computed_count)
            if status_info["operational_status"] in {"ready", "failed", "cancelled"}:
                candidate_ids.append(int(row["session_id"]))

    return {"candidate_session_ids": candidate_ids, **queue_processing_jobs(candidate_ids)}


def start_processing_job(job_id: int) -> dict:
    with db() as conn:
        job = _start_job_record(conn, job_id)
        return dict(job)


def get_job(job_id: int) -> dict:
    with db() as conn:
        return _sync_job_state(conn, _job_row(conn, job_id))


def list_jobs(session_id: int | None = None) -> list[dict]:
    dispatch_next_job()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
                pj.*,
                ses.session_label,
                ses.study_date,
                ses.study_time,
                sub.subject_id AS patient_code,
                sub.patient_name,
                sub.patient_given_name,
                sub.patient_family_name,
                sub.patient_birth_date
            FROM processing_jobs pj
            JOIN sessions ses ON ses.id = pj.session_id
            JOIN subjects sub ON sub.id = ses.subject_id
            WHERE (? IS NULL OR pj.session_id = ?)
            ORDER BY pj.id DESC
            """,
            (session_id, session_id),
        ).fetchall()
        return [_sync_job_state(conn, row) for row in rows]


def read_job_log(job_id: int, tail: int = 4000) -> dict:
    with db() as conn:
        job = _sync_job_state(conn, _job_row(conn, job_id))
    path = job.get("log_path")
    worker_text = ""
    worker_path = Path(path) if path else None
    if worker_path and worker_path.exists():
        worker_text = worker_path.read_text(encoding="utf-8", errors="replace")
    pipeline_path = _pipeline_log_path(job)
    pipeline_text = ""
    if pipeline_path and pipeline_path.exists():
        pipeline_text = pipeline_path.read_text(encoding="utf-8", errors="replace")

    combined = ""
    if worker_text:
        combined += "[worker.log]\n" + worker_text.strip() + "\n\n"
    if pipeline_text:
        combined += f"[pipeline.log: {pipeline_path}]\n" + pipeline_text.strip()

    return {
        "job_id": job_id,
        "log_path": str(worker_path) if worker_path else None,
        "pipeline_log_path": str(pipeline_path) if pipeline_path else None,
        "text": combined[-tail:],
    }


def cancel_processing_job(job_id: int) -> dict:
    with db() as conn:
        job = _sync_job_state(conn, _job_row(conn, job_id))
        pid = job.get("pid")
        if job["status"] == "running" and pid:
            try:
                os.killpg(pid, signal.SIGTERM)
            except OSError:
                pass
        conn.execute(
            """
            UPDATE processing_jobs
            SET status = 'cancelled',
                progress_stage = 'cancelled',
                finished_at = COALESCE(finished_at, strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (job_id,),
        )
    dispatch_next_job()
    return get_job(job_id)


def stop_all_processing() -> dict:
    cancelled = []
    cleaned_sessions = []
    removed_job_dirs = []
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
                pj.*,
                ses.session_label,
                sub.subject_id AS patient_code
            FROM processing_jobs pj
            JOIN sessions ses ON ses.id = pj.session_id
            JOIN subjects sub ON sub.id = ses.subject_id
            WHERE pj.status IN ('running','queued')
            """
        ).fetchall()
        for row in rows:
            job = dict(row)
            pid = job.get("pid")
            if job["status"] == "running" and pid:
                try:
                    os.killpg(pid, signal.SIGTERM)
                except OSError:
                    pass
            conn.execute(
                """
                UPDATE processing_jobs
                SET status = 'cancelled',
                    progress_stage = 'cancelled',
                    finished_at = COALESCE(finished_at, strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE id = ?
                """,
                (job["id"],),
            )
            cancelled.append(job["id"])
            run_dir = job.get("run_dir")
            input_dir = job.get("input_dir")
            log_path = job.get("log_path")
            for path_str in {run_dir, input_dir, log_path and str(Path(log_path).parent)}:
                if not path_str:
                    continue
                path = Path(path_str)
                try:
                    if path.is_file():
                        path.unlink(missing_ok=True)
                    elif path.exists():
                        shutil.rmtree(path, ignore_errors=True)
                    removed_job_dirs.append(str(path))
                except Exception:
                    pass
            reset_downstream_steps(
                job["patient_code"],
                job["session_label"],
                ["initial_validation", "nifti_conversion", "brain_extraction", "tumor_segmentation"],
                conn=conn,
            )
            cleaned_sessions.append(f"{job['patient_code']}:{job['session_label']}")
        if cancelled:
            conn.execute("DELETE FROM processing_jobs WHERE status = 'cancelled'")

        # Stop All must also clear stale preprocessing state left on disk,
        # even after server restarts or when no active DB jobs remain.
        session_rows = conn.execute(
            """
            SELECT
                ses.id AS session_id,
                ses.session_label,
                sub.subject_id AS patient_code
            FROM sessions ses
            JOIN subjects sub ON sub.id = ses.subject_id
            """
        ).fetchall()
        for row in session_rows:
            active = conn.execute(
                "SELECT COUNT(*) FROM processing_jobs WHERE session_id = ? AND status IN ('queued','running')",
                (row["session_id"],),
            ).fetchone()[0]
            if active:
                continue
            reset_downstream_steps(
                row["patient_code"],
                row["session_label"],
                ["initial_validation", "nifti_conversion", "brain_extraction", "tumor_segmentation"],
                conn=conn,
            )
            cleaned_sessions.append(f"{row['patient_code']}:{row['session_label']}")
    return {
        "cancelled_job_ids": cancelled,
        "cleaned_sessions": cleaned_sessions,
        "removed_paths": sorted(set(removed_job_dirs)),
    }


def remove_processing_job(job_id: int) -> dict:
    with db() as conn:
        job = _sync_job_state(conn, _job_row(conn, job_id))
        if job["status"] == "running":
            raise HTTPException(400, "Cannot remove a running job; cancel it first")
        conn.execute("DELETE FROM processing_jobs WHERE id = ?", (job_id,))
        return {"removed": True, "job_id": job_id}


def processing_sessions(
    dataset: str = TARGET_DATASET,
    only_unprocessed: bool = True,
) -> list[dict]:
    jobs = list_jobs()
    jobs_by_session: dict[int, list[dict]] = {}
    for job in jobs:
        jobs_by_session.setdefault(int(job["session_id"]), []).append(job)

    with db() as conn:
        rows = conn.execute(
            """
            SELECT
                ses.id AS session_id,
                ses.session_label,
                ses.study_date,
                ses.study_time,
                ses.processed_dir,
                ses.quality_flag,
                sub.id AS patient_id,
                sub.subject_id AS patient_code,
                sub.patient_name,
                sub.patient_given_name,
                sub.patient_family_name,
                sub.patient_birth_date,
                sub.dataset
            FROM sessions ses
            JOIN subjects sub ON sub.id = ses.subject_id
            WHERE sub.dataset = ?
            ORDER BY sub.subject_id, ses.session_label
            """,
            (dataset,),
        ).fetchall()

        sessions = []
        for row in rows:
            seq_rows = conn.execute(
                """
                SELECT sequence_type, raw_path, processed_path, display_label, import_class
                FROM sequences
                WHERE session_id = ?
                ORDER BY sequence_type
                """,
                (row["session_id"],),
            ).fetchall()
            sequences = rows_as_dicts(seq_rows)
            computed_count = conn.execute(
                "SELECT COUNT(*) FROM computed_structures WHERE session_id = ?",
                (row["session_id"],),
            ).fetchone()[0]
            session_jobs = jobs_by_session.get(int(row["session_id"]), [])
            status_info = _core_status_for_session(dict(row), sequences, session_jobs, computed_count)
            if only_unprocessed and status_info["operational_status"] == "completed":
                continue
            sessions.append({
                "session_id": row["session_id"],
                "session_label": row["session_label"],
                "study_date": row["study_date"],
                "study_time": row["study_time"],
                "patient_id": row["patient_id"],
                "patient_code": row["patient_code"],
                "patient_name": row["patient_name"],
                "patient_given_name": row["patient_given_name"],
                "patient_family_name": row["patient_family_name"],
                "patient_birth_date": row["patient_birth_date"],
                "dataset": row["dataset"],
                "processed_dir": row["processed_dir"],
                "quality_flag": row["quality_flag"],
                "sequences": sequences,
                "computed_count": computed_count,
                "pipeline_state": load_state(row["patient_code"], row["session_label"]),
                **status_info,
            })
        return sessions


def workspace_case(session_id: int) -> dict:
    with db() as conn:
        session = _session_row(conn, session_id)
        sequences = conn.execute(
            "SELECT * FROM sequences WHERE session_id = ? ORDER BY sequence_type",
            (session_id,),
        ).fetchall()
        computed = conn.execute(
            "SELECT * FROM computed_structures WHERE session_id = ? ORDER BY label_code, label",
            (session_id,),
        ).fetchall()
        jobs = list_jobs(session_id=session_id)

    pipeline_state = load_state(session["patient_code"], session["session_label"])
    seqs = rows_as_dicts(sequences)
    steps = [
        {
            "key": step["key"],
            "label": step["step_name"],
            "status": "missing" if step["status"] == "pending" else step["status"],
        }
        for step in pipeline_state["steps"]
    ]

    return {
        "session": dict(session),
        "subject": {
            "id": session["patient_pk"],
            "subject_id": session["patient_code"],
            "dataset": session["dataset"],
            "patient_name": session["patient_name"],
            "patient_given_name": session["patient_given_name"],
            "patient_family_name": session["patient_family_name"],
            "patient_birth_date": session["patient_birth_date"],
        },
        "pipeline_state": pipeline_state,
        "sequences": seqs,
        "computed_structures": rows_as_dicts(computed),
        "jobs": jobs,
        "steps": steps,
    }


def workspace_overview() -> dict:
    jobs = list_jobs()
    running = [job for job in jobs if job["status"] == "running"]
    queued = [job for job in jobs if job["status"] == "queued"]
    recent = jobs[:20]
    sessions = processing_sessions()
    current_job = None
    if running:
        current_job = dict(running[0])
        current_job["elapsed_seconds"] = _elapsed_seconds(current_job.get("started_at"))
    return {
        "current_job": current_job,
        "running_jobs": running,
        "queued_jobs": queued,
        "recent_jobs": recent,
        "sessions": sessions,
    }
