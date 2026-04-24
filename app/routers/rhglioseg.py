"""rhglioseg.py — API endpoints for rh-glioseg-v3 segmentation jobs."""

import threading
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from app.db import db, rows_as_dicts

router = APIRouter(tags=["rhglioseg"])

# In-memory job state (single job at a time, server-local)
_job = {
    "running": False,
    "current": 0,
    "total": 0,
    "last_msg": "",
    "result": None,
    "error": None,
}
_job_lock = threading.Lock()


class RhGlioSegRequest(BaseModel):
    session_ids: list[int] | None = None
    force: bool = False


@router.get("/rhglioseg/status")
def rhglioseg_status():
    """Job status + per-session summary from DB."""
    with db() as conn:
        done = conn.execute("""
            SELECT COUNT(DISTINCT session_id) FROM computed_structures
            WHERE model_name = 'rh-glioseg-v3'
        """).fetchone()[0]
        total_sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]

    with _job_lock:
        state = dict(_job)

    return {
        "running":         state["running"],
        "current":         state["current"],
        "total":           state["total"],
        "last_msg":        state["last_msg"],
        "result":          state["result"],
        "error":           state["error"],
        "sessions_done":   done,
        "sessions_total":  total_sessions,
    }


@router.get("/rhglioseg/results")
def rhglioseg_results(patient_id: int | None = None):
    """All rh-glioseg computed_structures, optionally filtered by patient."""
    with db() as conn:
        query = """
            SELECT sub.subject_id, se.id AS session_id, se.session_label,
                   se.days_from_baseline, se.study_date,
                   cs.label, cs.label_code, cs.volume_ml, cs.mask_path,
                   cs.reference_space, cs.created_at
            FROM computed_structures cs
            JOIN sessions se  ON se.id  = cs.session_id
            JOIN subjects sub ON sub.id = se.subject_id
            WHERE cs.model_name = 'rh-glioseg-v3'
        """
        params = []
        if patient_id:
            query += " AND sub.id = ?"
            params.append(patient_id)
        query += " ORDER BY sub.subject_id, se.days_from_baseline, cs.label"
        rows = conn.execute(query, params).fetchall()
    return {"rows": rows_as_dicts(rows)}


@router.post("/rhglioseg/run")
def rhglioseg_run(payload: RhGlioSegRequest, background_tasks: BackgroundTasks):
    """Start rh-glioseg segmentation job in background."""
    with _job_lock:
        if _job["running"]:
            return {"status": "already_running", "current": _job["current"], "total": _job["total"]}
        _job.update({"running": True, "current": 0, "total": 0,
                     "last_msg": "Starting…", "result": None, "error": None})

    background_tasks.add_task(_run_job, payload.session_ids, payload.force)
    return {"status": "started"}


@router.get("/segmentation/sessions")
def segmentation_sessions():
    """All sessions with per-session segmentation status (which models ran, preprocessing ready)."""
    with db() as conn:
        rows = conn.execute("""
            SELECT
                sub.id                  AS patient_id,
                sub.subject_id,
                sub.dataset,
                sub.patient_name,
                sub.patient_given_name,
                sub.patient_family_name,
                sub.patient_birth_date,
                se.id                   AS session_id,
                se.session_label,
                se.timepoint_type,
                se.days_from_baseline,
                se.study_date,
                GROUP_CONCAT(DISTINCT cs.model_name) AS segmented_models
            FROM subjects sub
            JOIN sessions se ON se.subject_id = sub.id
            LEFT JOIN computed_structures cs ON cs.session_id = se.id
            GROUP BY se.id
            ORDER BY sub.subject_id, se.days_from_baseline
        """).fetchall()

        # sessions with enough preprocessed NIfTI for rh-GlioSeg (shape_x=240)
        ready_rows = conn.execute("""
            SELECT session_id, COUNT(DISTINCT sequence_type) AS n
            FROM sequences
            WHERE sequence_type IN ('FLAIR','T1','T1ce','T2')
              AND processed_path IS NOT NULL
              AND shape_x = 240
            GROUP BY session_id
        """).fetchall()

    ready_set = {r["session_id"] for r in ready_rows if r["n"] >= 4}
    result = []
    for r in rows_as_dicts(rows):
        models = [m for m in (r["segmented_models"] or "").split(",") if m]
        result.append({
            **r,
            "segmented_models": models,
            "preprocessing_ready": r["session_id"] in ready_set,
        })
    return {"sessions": result}


class DeleteStructuresRequest(BaseModel):
    session_id: int
    model_name: str


@router.delete("/segmentation/structures")
def delete_segmentation_structures(payload: DeleteStructuresRequest):
    """Delete all computed_structures for a given session + model."""
    with db() as conn:
        deleted = conn.execute(
            "DELETE FROM computed_structures WHERE session_id = ? AND model_name = ?",
            (payload.session_id, payload.model_name),
        ).rowcount
        conn.commit()
    return {"deleted": deleted, "session_id": payload.session_id, "model_name": payload.model_name}


def _run_job(session_ids, force):
    import sys, os
    sys.path.insert(0, "/home/irst/gliotwin")
    os.chdir("/home/irst/gliotwin")
    try:
        from pipelines.run_rhglioseg import run_all

        def _progress(current, total, msg):
            with _job_lock:
                _job["current"] = current
                _job["total"]   = total
                _job["last_msg"] = msg

        result = run_all(session_ids=session_ids, force=force, progress_callback=_progress)

        with _job_lock:
            _job["running"] = False
            _job["result"]  = result
            _job["last_msg"] = f"Completato: {result['processed']} segmentazioni, {result['failed']} errori"
    except Exception as e:
        with _job_lock:
            _job["running"] = False
            _job["error"]   = str(e)
            _job["last_msg"] = f"Errore: {e}"
