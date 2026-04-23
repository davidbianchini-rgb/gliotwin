"""fetsseg.py — API endpoints for FeTS tumor segmentation on preprocessed cases."""

from __future__ import annotations

import os
import subprocess
import threading
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from app.db import db, rows_as_dicts
from app.services.fets_finalize import sync_fets_outputs

router = APIRouter(tags=["fetsseg"])

WRAPPER = Path("/home/irst/gliotwin/pipelines/run_fets_patient.sh")
USE_GPU = os.environ.get("GLIOTWIN_FETS_USE_GPU", "1").strip().lower() in {"1", "true", "yes"}
GPU_ID = os.environ.get("GLIOTWIN_FETS_GPU_ID", "0").strip() or "0"

_job = {
    "running": False,
    "current": 0,
    "total": 0,
    "last_msg": "",
    "result": None,
    "error": None,
}
_job_lock = threading.Lock()


class FeTSRequest(BaseModel):
    session_ids: list[int] | None = None
    force: bool = False


def _latest_preproc_run(session_id: int) -> tuple[dict, Path]:
    with db() as conn:
        row = conn.execute(
            """
            SELECT
                pj.id AS job_id,
                pj.run_dir,
                ses.session_label,
                sub.subject_id,
                sub.id AS patient_id
            FROM processing_jobs pj
            JOIN sessions ses ON ses.id = pj.session_id
            JOIN subjects sub ON sub.id = ses.subject_id
            WHERE pj.session_id = ?
              AND pj.status = 'completed'
              AND pj.run_dir IS NOT NULL
            ORDER BY pj.id DESC
            LIMIT 1
            """,
            (session_id,),
        ).fetchone()
    if not row:
        raise HTTPException(400, f"No completed preprocessing run found for session {session_id}")
    run_root = Path(row["run_dir"]).expanduser().resolve()
    if not run_root.exists():
        raise HTTPException(400, f"Preprocessing run directory not found: {run_root}")
    return dict(row), run_root


def _session_input_root(subject_id: str, session_label: str, run_root: Path) -> Path:
    job_root = run_root.parent.parent
    input_root = job_root / "input"
    expected = input_root / subject_id / session_label
    if not expected.exists():
        raise HTTPException(400, f"Preprocessing input directory not found: {expected}")
    return input_root


@router.get("/fetsseg/status")
def fetsseg_status():
    with db() as conn:
        done = conn.execute(
            "SELECT COUNT(DISTINCT session_id) FROM computed_structures WHERE model_name = 'fets_postop'"
        ).fetchone()[0]
        total_sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]

    with _job_lock:
        state = dict(_job)

    return {
        "running": state["running"],
        "current": state["current"],
        "total": state["total"],
        "last_msg": state["last_msg"],
        "result": state["result"],
        "error": state["error"],
        "sessions_done": done,
        "sessions_total": total_sessions,
    }


@router.get("/fetsseg/results")
def fetsseg_results(patient_id: int | None = None):
    with db() as conn:
        query = """
            SELECT sub.subject_id, se.id AS session_id, se.session_label,
                   se.days_from_baseline, se.study_date,
                   cs.label, cs.label_code, cs.volume_ml, cs.mask_path,
                   cs.reference_space, cs.created_at
            FROM computed_structures cs
            JOIN sessions se  ON se.id  = cs.session_id
            JOIN subjects sub ON sub.id = se.subject_id
            WHERE cs.model_name = 'fets_postop'
        """
        params = []
        if patient_id:
            query += " AND sub.id = ?"
            params.append(patient_id)
        query += " ORDER BY sub.subject_id, se.days_from_baseline, cs.label"
        rows = conn.execute(query, params).fetchall()
    return {"rows": rows_as_dicts(rows)}


@router.post("/fetsseg/run")
def fetsseg_run(payload: FeTSRequest, background_tasks: BackgroundTasks):
    with _job_lock:
        if _job["running"]:
            return {"status": "already_running", "current": _job["current"], "total": _job["total"]}
        _job.update({"running": True, "current": 0, "total": 0, "last_msg": "Starting…", "result": None, "error": None})

    background_tasks.add_task(_run_job, payload.session_ids or [], payload.force)
    return {"status": "started"}


def _run_job(session_ids: list[int], force: bool):
    processed = 0
    failed = 0
    errors: list[dict] = []
    total = len(session_ids)

    try:
        with _job_lock:
            _job["total"] = total

        for index, session_id in enumerate(session_ids, start=1):
            with _job_lock:
                _job["current"] = index
                _job["last_msg"] = f"FeTS tumor on session {session_id}"

            with db() as conn:
                if not force:
                    existing = conn.execute(
                        "SELECT COUNT(*) FROM computed_structures WHERE session_id = ? AND model_name = 'fets_postop'",
                        (session_id,),
                    ).fetchone()[0]
                    if existing:
                        continue

            row, run_root = _latest_preproc_run(session_id)
            input_root = _session_input_root(row["subject_id"], row["session_label"], run_root)

            cmd = [
                str(WRAPPER),
                "--paziente", row["subject_id"],
                "--input", str(input_root),
                "--output", str(run_root),
                "--mode", "tumor",
            ]
            if USE_GPU:
                cmd.extend(["--gpu", GPU_ID])
            else:
                cmd.append("--no-gpu")

            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            if proc.returncode != 0:
                failed += 1
                errors.append({"session_id": session_id, "error": proc.stdout[-1000:] if proc.stdout else f"exit {proc.returncode}"})
                continue

            sync_fets_outputs(session_id, str(run_root))
            processed += 1

        with _job_lock:
            _job["running"] = False
            _job["result"] = {"processed": processed, "failed": failed, "errors": errors}
            _job["last_msg"] = f"Completato: {processed} segmentazioni FeTS, {failed} errori"
    except Exception as exc:
        with _job_lock:
            _job["running"] = False
            _job["error"] = str(exc)
            _job["last_msg"] = f"Errore: {exc}"
