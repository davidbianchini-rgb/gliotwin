from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.db import db, rows_as_dicts
from app.services.pipeline_state import sessions_ready_for_phase
from app.services.structure_metrics import (
    get_signal_metric_job,
    latest_signal_metric_job,
    patient_signal_timeline,
    queue_signal_metric_job,
    signal_metric_status,
)

router = APIRouter(tags=["sessions"])


class SignalMetricJobRequest(BaseModel):
    patient_id: int | None = None
    force: bool = False


@router.get("/patients/{patient_id}/sessions")
def list_sessions(patient_id: int):
    with db() as conn:
        subj = conn.execute(
            "SELECT id FROM subjects WHERE id = ?", (patient_id,)
        ).fetchone()
        if not subj:
            raise HTTPException(404, "Patient not found")

        sessions = conn.execute("""
            SELECT
                ses.*,
                COUNT(DISTINCT seq.id) AS n_sequences
            FROM sessions ses
            LEFT JOIN sequences seq ON seq.session_id = ses.id
            WHERE ses.subject_id = ?
            GROUP BY ses.id
            ORDER BY ses.days_from_baseline
        """, (patient_id,)).fetchall()

    return rows_as_dicts(sessions)


@router.get("/patients/{patient_id}/volumes")
def patient_volume_timeline(patient_id: int):
    """Aggregated volume data for the longitudinal chart (computed + radiological)."""
    with db() as conn:
        subj = conn.execute(
            "SELECT id FROM subjects WHERE id = ?", (patient_id,)
        ).fetchone()
        if not subj:
            raise HTTPException(404, "Patient not found")

        computed = conn.execute("""
            SELECT
                ses.id              AS session_id,
                ses.session_label,
                ses.days_from_baseline,
                ses.timepoint_type,
                cs.label,
                cs.volume_ml,
                cs.dice_vs_gt,
                cs.model_name,
                'computed'          AS source
            FROM computed_structures cs
            JOIN sessions ses ON ses.id = cs.session_id
            WHERE ses.subject_id = ?
            ORDER BY ses.days_from_baseline, cs.label
        """, (patient_id,)).fetchall()

        radiological = conn.execute("""
            SELECT
                ses.id              AS session_id,
                ses.session_label,
                ses.days_from_baseline,
                ses.timepoint_type,
                rs.label,
                rs.volume_ml,
                NULL                AS dice_vs_gt,
                rs.annotator        AS model_name,
                'radiological'      AS source
            FROM radiological_structures rs
            JOIN sessions ses ON ses.id = rs.session_id
            WHERE ses.subject_id = ?
            ORDER BY ses.days_from_baseline, rs.label
        """, (patient_id,)).fetchall()

    return {
        "computed":     rows_as_dicts(computed),
        "radiological": rows_as_dicts(radiological),
    }


@router.get("/patients/{patient_id}/signal-timeline")
def patient_structure_signal_timeline(
    patient_id: int,
    label: str | None = None,
    sequence_type: str | None = None,
    structure_source: str = "preferred",
):
    return patient_signal_timeline(patient_id, label, sequence_type, structure_source)


@router.get("/signal-metrics/status")
def get_signal_metrics_status(patient_id: int | None = None):
    return signal_metric_status(patient_id=patient_id)


@router.get("/signal-metrics/jobs/latest")
def get_latest_signal_metrics_job(patient_id: int | None = None):
    return {"job": latest_signal_metric_job(patient_id=patient_id)}


@router.get("/signal-metrics/jobs/{job_id}")
def get_signal_metrics_job(job_id: int):
    return get_signal_metric_job(job_id)


@router.post("/signal-metrics/jobs/queue-missing")
def queue_missing_signal_metrics(payload: SignalMetricJobRequest):
    return queue_signal_metric_job(patient_id=payload.patient_id, force=payload.force)


@router.get("/global-metrics")
def get_global_metrics():
    """All signal metric cache rows joined with patient/session info, for cross-patient charts."""
    with db() as conn:
        rows = conn.execute("""
            SELECT
                sub.id          AS patient_id,
                sub.subject_id  AS subject_id,
                se.id           AS session_id,
                se.session_label,
                se.study_date,
                se.days_from_baseline,
                smc.label,
                smc.label_code,
                smc.sequence_type,
                smc.structure_source,
                smc.volume_ml,
                smc.n_voxels,
                smc.median,
                smc.q1,
                smc.q3,
                smc.signal_error
            FROM signal_metric_cache smc
            JOIN sessions se  ON se.id  = smc.session_id
            JOIN subjects sub ON sub.id = se.subject_id
            ORDER BY sub.subject_id, se.days_from_baseline
        """).fetchall()
    return {"rows": rows_as_dicts(rows)}


@router.get("/sessions/ready-for/{phase}")
def sessions_ready_for(phase: str):
    """
    Returns sessions ready to advance to the given phase.
    Previous phase must have at least one done step; current phase must be fully pending.
    Valid phases: import, preprocessing, segmentation, analysis, export.
    """
    try:
        return {"phase": phase, "sessions": sessions_ready_for_phase(phase)}
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.get("/sessions/{session_id}")
def get_session(session_id: int):
    with db() as conn:
        session = conn.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if not session:
            raise HTTPException(404, "Session not found")

        sequences = conn.execute("""
            SELECT * FROM sequences
            WHERE session_id = ?
            ORDER BY sequence_type
        """, (session_id,)).fetchall()

    return {**dict(session), "sequences": rows_as_dicts(sequences)}


@router.get("/sessions/{session_id}/structures")
def get_session_structures(session_id: int):
    with db() as conn:
        session = conn.execute(
            "SELECT id FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if not session:
            raise HTTPException(404, "Session not found")

        computed = conn.execute("""
            SELECT * FROM computed_structures
            WHERE session_id = ?
            ORDER BY label
        """, (session_id,)).fetchall()

        radiological = conn.execute("""
            SELECT * FROM radiological_structures
            WHERE session_id = ?
            ORDER BY label
        """, (session_id,)).fetchall()

    return {
        "computed":     rows_as_dicts(computed),
        "radiological": rows_as_dicts(radiological),
    }
