from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import db, rows_as_dicts

router = APIRouter(tags=["patients"])


class PatientUpdateRequest(BaseModel):
    patient_name: str | None = None
    patient_given_name: str | None = None
    patient_family_name: str | None = None
    patient_birth_date: str | None = None
    sex: str | None = None
    diagnosis: str | None = None
    age_at_diagnosis: float | None = None
    idh_status: str | None = None
    mgmt_status: str | None = None
    os_days: int | None = None
    vital_status: str | None = None
    notes: str | None = None
    ida: str | None = None
    tax_code: str | None = None
    diagnosis_date: str | None = None
    radiotherapy_start_date: str | None = None
    death_date: str | None = None
    fractions_count: int | None = None


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_iso_date(value: str | None) -> str | None:
    text = _clean_string(value)
    if not text:
        return None
    if re.fullmatch(r"\d{8}", text):
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _upsert_subject_external_ref(conn, patient_id: int, ref_type: str, ref_value: str | None) -> None:
    row = conn.execute(
        """
        SELECT id
        FROM subject_external_refs
        WHERE subject_id = ? AND source_system = 'mosaiq_rt' AND ref_type = ?
        """,
        (patient_id, ref_type),
    ).fetchone()
    if ref_value:
        if row:
            conn.execute(
                """
                UPDATE subject_external_refs
                SET ref_value = ?, raw_value = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE id = ?
                """,
                (ref_value, ref_value, row["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO subject_external_refs (
                    subject_id, source_system, ref_type, ref_value, raw_value
                ) VALUES (?, 'mosaiq_rt', ?, ?, ?)
                """,
                (patient_id, ref_type, ref_value, ref_value),
            )
    elif row:
        conn.execute("DELETE FROM subject_external_refs WHERE id = ?", (row["id"],))


def _upsert_clinical_event(conn, patient_id: int, event_type: str, event_date: str | None, description: str) -> None:
    row = conn.execute(
        """
        SELECT id
        FROM clinical_events
        WHERE subject_id = ? AND event_type = ?
        ORDER BY COALESCE(event_date, ''), created_at
        LIMIT 1
        """,
        (patient_id, event_type),
    ).fetchone()
    if event_date:
        if row:
            conn.execute(
                """
                UPDATE clinical_events
                SET event_date = ?, description = ?
                WHERE id = ?
                """,
                (event_date, description, row["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO clinical_events (
                    subject_id, event_type, event_date, description
                ) VALUES (?, ?, ?, ?)
                """,
                (patient_id, event_type, event_date, description),
            )
    elif row:
        conn.execute("DELETE FROM clinical_events WHERE id = ?", (row["id"],))


def _upsert_radiotherapy_course(
    conn,
    patient_id: int,
    ida: str | None,
    tax_code: str | None,
    fractions_count: int | None,
    diagnosis_date: str | None,
    start_date: str | None,
) -> None:
    row = conn.execute(
        """
        SELECT id
        FROM radiotherapy_courses
        WHERE subject_id = ? AND source_system = 'mosaiq_rt'
        ORDER BY COALESCE(start_date, ''), COALESCE(diagnosis_date, ''), updated_at DESC
        LIMIT 1
        """,
        (patient_id,),
    ).fetchone()
    has_payload = any(value is not None for value in (ida, tax_code, fractions_count, diagnosis_date, start_date))
    if not has_payload:
        if row:
            conn.execute("DELETE FROM radiotherapy_courses WHERE id = ?", (row["id"],))
        return
    if row:
        conn.execute(
            """
            UPDATE radiotherapy_courses
            SET external_course_id = ?, tax_code = ?, fractions_count = ?,
                diagnosis_date = ?, start_date = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (ida, tax_code, fractions_count, diagnosis_date, start_date, row["id"]),
        )
    else:
        conn.execute(
            """
            INSERT INTO radiotherapy_courses (
                subject_id, source_system, external_course_id, tax_code,
                fractions_count, diagnosis_date, start_date
            ) VALUES (?, 'mosaiq_rt', ?, ?, ?, ?, ?)
            """,
            (patient_id, ida, tax_code, fractions_count, diagnosis_date, start_date),
        )


def _patient_detail(conn, patient_id: int) -> dict[str, Any]:
    subj = conn.execute(
        "SELECT * FROM subjects WHERE id = ?",
        (patient_id,),
    ).fetchone()
    if not subj:
        raise HTTPException(404, "Patient not found")

    events = rows_as_dicts(
        conn.execute(
            """
            SELECT *
            FROM clinical_events
            WHERE subject_id = ?
            ORDER BY COALESCE(event_date, ''), COALESCE(days_from_baseline, 999999), created_at
            """,
            (patient_id,),
        ).fetchall()
    )
    refs = rows_as_dicts(
        conn.execute(
            """
            SELECT *
            FROM subject_external_refs
            WHERE subject_id = ?
            ORDER BY source_system, ref_type
            """,
            (patient_id,),
        ).fetchall()
    )
    rt_courses = rows_as_dicts(
        conn.execute(
            """
            SELECT *
            FROM radiotherapy_courses
            WHERE subject_id = ?
            ORDER BY COALESCE(start_date, ''), COALESCE(diagnosis_date, ''), updated_at DESC
            """,
            (patient_id,),
        ).fetchall()
    )
    ref_map = {row["ref_type"]: row["ref_value"] for row in refs if row.get("ref_type")}
    latest_rt = rt_courses[-1] if rt_courses else None

    return {
        **dict(subj),
        "clinical_events": events,
        "external_refs": refs,
        "external_ref_map": ref_map,
        "radiotherapy_courses": rt_courses,
        "latest_radiotherapy_course": latest_rt,
        "diagnosis_date": next((item.get("event_date") for item in events if item.get("event_type") == "diagnosis" and item.get("event_date")), None),
        "radiotherapy_start_date": next((item.get("event_date") for item in events if item.get("event_type") == "radiotherapy_start" and item.get("event_date")), None),
        "death_date": next((item.get("event_date") for item in events if item.get("event_type") == "death" and item.get("event_date")), None),
    }


@router.get("/patients")
def list_patients():
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
                s.id,
                s.subject_id,
                s.dataset,
                s.patient_name,
                s.patient_given_name,
                s.patient_family_name,
                s.patient_birth_date,
                s.sex,
                s.age_at_diagnosis,
                s.diagnosis,
                s.idh_status,
                s.mgmt_status,
                s.os_days,
                s.vital_status,
                s.notes,
                COUNT(DISTINCT ses.id)  AS n_sessions,
                COUNT(DISTINCT seq.id)  AS n_sequences
            FROM subjects s
            LEFT JOIN sessions ses ON ses.subject_id = s.id
            LEFT JOIN sequences seq ON seq.session_id = ses.id
            GROUP BY s.id
            ORDER BY s.dataset, s.subject_id
            """
        ).fetchall()
    return rows_as_dicts(rows)


@router.get("/patients/{patient_id}")
def get_patient(patient_id: int):
    with db() as conn:
        return _patient_detail(conn, patient_id)


@router.put("/patients/{patient_id}")
def update_patient(patient_id: int, payload: PatientUpdateRequest):
    with db() as conn:
        existing = conn.execute("SELECT id FROM subjects WHERE id = ?", (patient_id,)).fetchone()
        if not existing:
            raise HTTPException(404, "Patient not found")

        subject_updates = {
            "patient_name": _clean_string(payload.patient_name),
            "patient_given_name": _clean_string(payload.patient_given_name),
            "patient_family_name": _clean_string(payload.patient_family_name),
            "patient_birth_date": _clean_string(payload.patient_birth_date),
            "sex": _clean_string(payload.sex),
            "diagnosis": _clean_string(payload.diagnosis),
            "age_at_diagnosis": payload.age_at_diagnosis,
            "idh_status": _clean_string(payload.idh_status),
            "mgmt_status": _clean_string(payload.mgmt_status),
            "os_days": payload.os_days,
            "vital_status": _clean_string(payload.vital_status),
            "notes": _clean_string(payload.notes),
        }
        assignments = ", ".join(f"{column} = ?" for column in subject_updates)
        values = list(subject_updates.values()) + [patient_id]
        conn.execute(
            f"""
            UPDATE subjects
            SET {assignments},
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            values,
        )

        ida = _clean_string(payload.ida)
        tax_code = _clean_string(payload.tax_code)
        diagnosis_date = _clean_iso_date(payload.diagnosis_date)
        rt_start = _clean_iso_date(payload.radiotherapy_start_date)
        death_date = _clean_iso_date(payload.death_date)
        fractions = payload.fractions_count

        _upsert_subject_external_ref(conn, patient_id, "ida", ida)
        _upsert_subject_external_ref(conn, patient_id, "tax_code", tax_code)
        _upsert_radiotherapy_course(conn, patient_id, ida, tax_code, fractions, diagnosis_date, rt_start)
        _upsert_clinical_event(conn, patient_id, "diagnosis", diagnosis_date, "Manual update from patient editor")
        _upsert_clinical_event(conn, patient_id, "radiotherapy_start", rt_start, "Manual update from patient editor")
        _upsert_clinical_event(conn, patient_id, "death", death_date, "Manual update from patient editor")

        return _patient_detail(conn, patient_id)
