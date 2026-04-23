from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import db, rows_as_dicts
from app.services.pipeline_state import state_path

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


class PatientMergeRequest(BaseModel):
    source_patient_id: int
    target_patient_id: int
    delete_source: bool = True


def _identity_key(item: dict[str, Any]) -> str:
    family = _clean_string(item.get("patient_family_name")) or ""
    given = _clean_string(item.get("patient_given_name")) or ""
    birth = _clean_iso_date(item.get("patient_birth_date")) or ""
    if family or given:
        return "|".join([family.upper(), given.upper(), birth])

    display = _clean_string(item.get("patient_name")) or ""
    if display:
        return "|".join([display.replace("^", " ").upper(), birth])

    return ""


def _subject_merge_guard(conn, patient_id: int) -> list[str]:
    issues: list[str] = []
    subject = conn.execute(
        "SELECT subject_id FROM subjects WHERE id = ?",
        (patient_id,),
    ).fetchone()
    if not subject:
        return ["patient not found"]

    sessions = rows_as_dicts(
        conn.execute("SELECT id, session_label FROM sessions WHERE subject_id = ?", (patient_id,)).fetchall()
    )
    session_ids = [row["id"] for row in sessions]

    if session_ids:
        placeholders = ",".join("?" for _ in session_ids)
        processed = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM sequences
            WHERE session_id IN ({placeholders})
              AND processed_path IS NOT NULL
            """,
            session_ids,
        ).fetchone()[0]
        jobs = conn.execute(
            f"SELECT COUNT(*) FROM processing_jobs WHERE session_id IN ({placeholders})",
            session_ids,
        ).fetchone()[0]
        computed = conn.execute(
            f"SELECT COUNT(*) FROM computed_structures WHERE session_id IN ({placeholders})",
            session_ids,
        ).fetchone()[0]
        if processed:
            issues.append("processed sequences already exist")
        if jobs:
            issues.append("processing jobs already exist")
        if computed:
            issues.append("computed structures already exist")

    subject_code = str(subject["subject_id"])
    for sess in sessions:
        if state_path(subject_code, str(sess["session_label"])).exists():
            issues.append(f"pipeline state exists for {sess['session_label']}")
            break
    return issues


def _merge_subject_rows(conn, table: str, source_patient_id: int, target_patient_id: int, key_columns: list[str]) -> None:
    rows = rows_as_dicts(conn.execute(f"SELECT * FROM {table} WHERE subject_id = ?", (source_patient_id,)).fetchall())
    for row in rows:
        filters = " AND ".join(f"{col} = ?" for col in key_columns)
        filter_values = [row[col] for col in key_columns]
        existing = conn.execute(
            f"SELECT id FROM {table} WHERE subject_id = ? AND {filters}",
            [target_patient_id, *filter_values],
        ).fetchone()
        if existing:
            conn.execute(f"DELETE FROM {table} WHERE id = ?", (row["id"],))
        else:
            conn.execute(f"UPDATE {table} SET subject_id = ? WHERE id = ?", (target_patient_id, row["id"]))


def _merge_session_children(conn, source_session_id: int, target_session_id: int) -> None:
    sequence_rows = rows_as_dicts(conn.execute("SELECT * FROM sequences WHERE session_id = ?", (source_session_id,)).fetchall())
    for row in sequence_rows:
        existing = conn.execute(
            "SELECT id FROM sequences WHERE session_id = ? AND sequence_type = ?",
            (target_session_id, row["sequence_type"]),
        ).fetchone()
        if existing:
            conn.execute("DELETE FROM sequences WHERE id = ?", (row["id"],))
        else:
            conn.execute("UPDATE sequences SET session_id = ? WHERE id = ?", (target_session_id, row["id"]))

    radio_rows = rows_as_dicts(conn.execute("SELECT * FROM radiological_structures WHERE session_id = ?", (source_session_id,)).fetchall())
    for row in radio_rows:
        existing = conn.execute(
            """
            SELECT id FROM radiological_structures
            WHERE session_id = ? AND label = ? AND COALESCE(annotator, '') = COALESCE(?, '')
            """,
            (target_session_id, row["label"], row["annotator"]),
        ).fetchone()
        if existing:
            conn.execute("DELETE FROM radiological_structures WHERE id = ?", (row["id"],))
        else:
            conn.execute("UPDATE radiological_structures SET session_id = ? WHERE id = ?", (target_session_id, row["id"]))

    computed_rows = rows_as_dicts(conn.execute("SELECT * FROM computed_structures WHERE session_id = ?", (source_session_id,)).fetchall())
    for row in computed_rows:
        existing = conn.execute(
            """
            SELECT id FROM computed_structures
            WHERE session_id = ? AND COALESCE(model_name, '') = COALESCE(?, '') AND label = ?
            """,
            (target_session_id, row["model_name"], row["label"]),
        ).fetchone()
        if existing:
            conn.execute("DELETE FROM computed_structures WHERE id = ?", (row["id"],))
        else:
            conn.execute("UPDATE computed_structures SET session_id = ? WHERE id = ?", (target_session_id, row["id"]))

    cache_rows = rows_as_dicts(conn.execute("SELECT * FROM signal_metric_cache WHERE session_id = ?", (source_session_id,)).fetchall())
    for row in cache_rows:
        existing = conn.execute(
            """
            SELECT id FROM signal_metric_cache
            WHERE session_id = ? AND structure_source = ? AND label = ? AND sequence_type = ?
            """,
            (target_session_id, row["structure_source"], row["label"], row["sequence_type"]),
        ).fetchone()
        if existing:
            conn.execute("DELETE FROM signal_metric_cache WHERE id = ?", (row["id"],))
        else:
            conn.execute("UPDATE signal_metric_cache SET session_id = ? WHERE id = ?", (target_session_id, row["id"]))

    conn.execute("UPDATE processing_jobs SET session_id = ? WHERE session_id = ?", (target_session_id, source_session_id))
    conn.execute("UPDATE clinical_events SET session_id = ? WHERE session_id = ?", (target_session_id, source_session_id))


def _merge_patients(conn, source_patient_id: int, target_patient_id: int, delete_source: bool = True) -> dict[str, Any]:
    source = conn.execute("SELECT * FROM subjects WHERE id = ?", (source_patient_id,)).fetchone()
    target = conn.execute("SELECT * FROM subjects WHERE id = ?", (target_patient_id,)).fetchone()
    if not source or not target:
        raise HTTPException(404, "Patient not found")
    if source_patient_id == target_patient_id:
        raise HTTPException(400, "Source and target patient must be different")
    if source["dataset"] != target["dataset"]:
        raise HTTPException(400, "Patients must belong to the same dataset")

    source_issues = _subject_merge_guard(conn, source_patient_id)
    target_issues = _subject_merge_guard(conn, target_patient_id)
    if source_issues or target_issues:
        raise HTTPException(
            400,
            {
                "detail": "Merge allowed only before preprocessing/segmentation",
                "source_issues": source_issues,
                "target_issues": target_issues,
            },
        )

    conn.execute(
        """
        UPDATE subjects
        SET patient_name = COALESCE(NULLIF(patient_name, ''), ?),
            patient_given_name = COALESCE(NULLIF(patient_given_name, ''), ?),
            patient_family_name = COALESCE(NULLIF(patient_family_name, ''), ?),
            patient_birth_date = COALESCE(NULLIF(patient_birth_date, ''), ?),
            sex = COALESCE(NULLIF(sex, ''), ?),
            diagnosis = COALESCE(NULLIF(diagnosis, ''), ?),
            idh_status = COALESCE(NULLIF(idh_status, ''), ?),
            mgmt_status = COALESCE(NULLIF(mgmt_status, ''), ?),
            notes = CASE
                WHEN COALESCE(notes, '') = '' THEN ?
                WHEN COALESCE(?, '') = '' THEN notes
                ELSE notes || ' | merged-from ' || ?
            END,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ?
        """,
        (
            source["patient_name"],
            source["patient_given_name"],
            source["patient_family_name"],
            source["patient_birth_date"],
            source["sex"],
            source["diagnosis"],
            source["idh_status"],
            source["mgmt_status"],
            source["notes"],
            source["notes"],
            source["subject_id"],
            target_patient_id,
        ),
    )

    target_sessions = {
        row["session_label"]: row["id"]
        for row in rows_as_dicts(conn.execute("SELECT id, session_label FROM sessions WHERE subject_id = ?", (target_patient_id,)).fetchall())
    }
    source_sessions = rows_as_dicts(
        conn.execute("SELECT id, session_label FROM sessions WHERE subject_id = ?", (source_patient_id,)).fetchall()
    )

    merged_session_labels: list[str] = []
    moved_session_labels: list[str] = []

    for source_session in source_sessions:
        target_session_id = target_sessions.get(source_session["session_label"])
        if target_session_id:
            _merge_session_children(conn, source_session["id"], target_session_id)
            conn.execute("DELETE FROM sessions WHERE id = ?", (source_session["id"],))
            merged_session_labels.append(str(source_session["session_label"]))
        else:
            conn.execute(
                "UPDATE sessions SET subject_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?",
                (target_patient_id, source_session["id"]),
            )
            moved_session_labels.append(str(source_session["session_label"]))

    _merge_subject_rows(conn, "subject_external_refs", source_patient_id, target_patient_id, ["source_system", "ref_type"])
    _merge_subject_rows(conn, "radiotherapy_courses", source_patient_id, target_patient_id, ["source_system", "external_course_id"])
    _merge_subject_rows(conn, "clinical_events", source_patient_id, target_patient_id, ["event_type", "days_from_baseline"])
    conn.execute("UPDATE signal_metric_jobs SET patient_id = ? WHERE patient_id = ?", (target_patient_id, source_patient_id))

    if delete_source:
        conn.execute("DELETE FROM subjects WHERE id = ?", (source_patient_id,))

    return {
        "source_patient_id": source_patient_id,
        "target_patient_id": target_patient_id,
        "source_subject_id": source["subject_id"],
        "target_subject_id": target["subject_id"],
        "moved_sessions": moved_session_labels,
        "merged_sessions": merged_session_labels,
    }


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


@router.get("/patient-merge-candidates")
def list_merge_candidates():
    with db() as conn:
        rows = rows_as_dicts(
            conn.execute(
                """
                SELECT
                    id, subject_id, dataset, patient_name,
                    patient_given_name, patient_family_name, patient_birth_date,
                    sex, notes
                FROM subjects
                ORDER BY dataset, subject_id
                """
            ).fetchall()
        )

    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = _identity_key(row)
        if not key:
            continue
        groups.setdefault((row["dataset"], key), []).append(row)

    candidates = []
    for (dataset, key), items in groups.items():
        if len(items) < 2:
            continue
        candidates.append(
            {
                "dataset": dataset,
                "identity_key": key,
                "patients": items,
            }
        )

    candidates.sort(key=lambda item: (item["dataset"], item["identity_key"]))
    return {"rows": candidates}


@router.post("/patient-merge")
def merge_patients(payload: PatientMergeRequest):
    with db() as conn:
        result = _merge_patients(
            conn,
            source_patient_id=payload.source_patient_id,
            target_patient_id=payload.target_patient_id,
            delete_source=payload.delete_source,
        )
        result["target_patient"] = _patient_detail(conn, payload.target_patient_id)
        return result
