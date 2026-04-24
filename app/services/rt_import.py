from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import HTTPException

from app.db import db, rows_as_dicts
from app.services.clinical_timeline import sync_subject_timeline_offsets
from app.services.subject_identity import (
    add_subject_alias,
    create_subject,
    find_subject_by_alias,
    normalize_person_key,
    update_subject_demographics,
)

ALLOWED_IMPORT_ROOTS = [
    Path("/mnt/dati"),
    Path("/home/irst"),
]
DEFAULT_RT_DATASET = "irst_dicom_raw"
DEFAULT_RT_SOURCE = "mosaiq_rt"


@dataclass(frozen=True)
class RtRow:
    row_index: int
    ida: str | None
    patient_name_raw: str
    patient_family_name: str
    patient_given_name: str
    tax_code: str | None
    fractions_count: int | None
    start_date: str | None
    diagnosis_date: str | None


def _resolve_excel_path(file_path: str) -> Path:
    if not file_path:
        raise HTTPException(400, "file_path is required")
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.exists():
        raise HTTPException(404, f"Excel file not found: {file_path}")
    if not resolved.is_file():
        raise HTTPException(400, f"Path is not a file: {file_path}")
    if resolved.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
        raise HTTPException(400, "Only Excel files are supported")
    if not any(str(resolved).startswith(str(root)) for root in ALLOWED_IMPORT_ROOTS):
        raise HTTPException(403, "File path outside allowed roots")
    return resolved


def _normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper().strip()
    text = text.replace(",", " ").replace("'", " ").replace("-", " ")
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_header(value: Any) -> str:
    return _normalize_text(value).replace(" ", "").replace("_", "")


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    return text or None


def _clean_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        return int(round(float(value)))
    except Exception:
        return None


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date().isoformat()
        except ValueError:
            continue
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _parse_rt_name(raw_name: Any) -> tuple[str, str]:
    raw = _clean_string(raw_name) or ""
    if "," in raw:
        family_raw, given_raw = raw.split(",", 1)
        return _normalize_text(family_raw), _normalize_text(given_raw)
    normalized = _normalize_text(raw)
    parts = normalized.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return "", normalized


def _canonical_name(family_name: str, given_name: str) -> str:
    return _normalize_text(f"{family_name} {given_name}".strip())


def _read_excel_rows(file_path: Path) -> list[RtRow]:
    df = pd.read_excel(file_path)
    if df.empty:
        return []

    original_columns = list(df.columns)
    header_map = {_normalize_header(name): name for name in original_columns}

    def col(*names: str) -> str:
        for name in names:
            hit = header_map.get(_normalize_header(name))
            if hit:
                return hit
        raise HTTPException(400, f"Missing required column in Excel: {names[0]}")

    ida_col = col("IDA")
    name_col = col("PAT_NAME", "Pat_Name")
    tax_col = col("IDB")
    fractions_col = col("numeroSedute", "Numero sedute")
    start_col = col("dataStart", "DataStart")
    dx_col = col("Dx_Partial_DtTm")

    rows: list[RtRow] = []
    for idx, row in df.iterrows():
        patient_name_raw = _clean_string(row.get(name_col))
        if not patient_name_raw:
            continue
        family_name, given_name = _parse_rt_name(patient_name_raw)
        rows.append(
            RtRow(
                row_index=int(idx) + 2,
                ida=_clean_string(row.get(ida_col)),
                patient_name_raw=patient_name_raw,
                patient_family_name=family_name,
                patient_given_name=given_name,
                tax_code=_clean_string(row.get(tax_col)),
                fractions_count=_clean_int(row.get(fractions_col)),
                start_date=_iso_date(row.get(start_col)),
                diagnosis_date=_iso_date(row.get(dx_col)),
            )
        )
    return rows


def _subject_name_keys(subject: dict[str, Any]) -> set[str]:
    patient_name = _normalize_text(subject.get("patient_name"))
    family_name = _normalize_text(subject.get("patient_family_name"))
    given_name = _normalize_text(subject.get("patient_given_name"))
    keys = set()
    if patient_name:
        keys.add(patient_name)
    if family_name or given_name:
        keys.add(_canonical_name(family_name, given_name))
        keys.add(_canonical_name(given_name, family_name))
    return {item for item in keys if item}


def _candidate_subjects(dataset: str | None = None) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[tuple[str, str], list[dict[str, Any]]]]:
    with db() as conn:
        if dataset:
            rows = conn.execute(
                """
                SELECT *
                FROM subjects
                WHERE dataset = ?
                ORDER BY patient_family_name, patient_given_name, subject_id
                """,
                (dataset,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM subjects
                ORDER BY dataset, patient_family_name, patient_given_name, subject_id
                """
            ).fetchall()
        alias_rows = conn.execute(
            f"""
            SELECT sa.*, s.dataset, s.subject_id AS internal_subject_id
            FROM subject_aliases sa
            JOIN subjects s ON s.id = sa.subject_id
            {"WHERE s.dataset = ?" if dataset else ""}
            ORDER BY sa.subject_id
            """,
            ((dataset,) if dataset else ()),
        ).fetchall()
    subjects = rows_as_dicts(rows)
    name_index: dict[str, list[dict[str, Any]]] = {}
    alias_index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for subject in subjects:
        for key in _subject_name_keys(subject):
            name_index.setdefault(key, []).append(subject)
    for alias in rows_as_dicts(alias_rows):
        key = (alias["alias_type"], _clean_string(alias.get("alias_value")) or _clean_string(alias.get("alias_norm")) or "")
        if not key[1]:
            continue
        alias_index.setdefault(key, []).append(alias)
    return subjects, name_index, alias_index


def _match_row(
    row: RtRow,
    name_index: dict[str, list[dict[str, Any]]],
    alias_index: dict[tuple[str, str], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    seen_ids: set[int] = set()

    alias_keys = []
    if row.ida:
        alias_keys.append(("rt_ida", row.ida))
    if row.tax_code:
        alias_keys.append(("tax_code", row.tax_code))
    person_key = normalize_person_key(row.patient_family_name, row.patient_given_name)
    if person_key:
        alias_keys.append(("person_key", person_key))

    for alias_type, alias_value in alias_keys:
        for alias in alias_index.get((alias_type, alias_value), []):
            subject_id = int(alias["subject_id"])
            if subject_id in seen_ids:
                continue
            subject = {
                "id": alias["subject_id"],
                "subject_id": alias["internal_subject_id"],
                "dataset": alias["dataset"],
            }
            matches.append(subject)
            seen_ids.add(subject_id)

    if matches:
        return matches

    keys = []
    if row.patient_family_name or row.patient_given_name:
        keys.append(_canonical_name(row.patient_family_name, row.patient_given_name))
        keys.append(_canonical_name(row.patient_given_name, row.patient_family_name))
    keys.append(_normalize_text(row.patient_name_raw))
    for key in keys:
        for subject in name_index.get(key, []):
            subject_id = int(subject["id"])
            if subject_id in seen_ids:
                continue
            matches.append(subject)
            seen_ids.add(subject_id)
    return matches


def analyze_rt_excel(file_path: str, dataset: str = DEFAULT_RT_DATASET) -> dict[str, Any]:
    resolved = _resolve_excel_path(file_path)
    rows = _read_excel_rows(resolved)
    subjects, name_index, alias_index = _candidate_subjects(dataset=dataset)

    preview_rows: list[dict[str, Any]] = []
    matched = 0
    ambiguous = 0
    unmatched = 0

    for row in rows:
        candidates = _match_row(row, name_index, alias_index)
        if len(candidates) == 1:
            status = "matched"
            matched += 1
        elif len(candidates) > 1:
            status = "ambiguous"
            ambiguous += 1
        else:
            status = "unmatched"
            unmatched += 1

        preview_rows.append(
            {
                "row_index": row.row_index,
                "status": status,
                "ida": row.ida,
                "patient_name_raw": row.patient_name_raw,
                "patient_family_name": row.patient_family_name,
                "patient_given_name": row.patient_given_name,
                "tax_code": row.tax_code,
                "fractions_count": row.fractions_count,
                "start_date": row.start_date,
                "diagnosis_date": row.diagnosis_date,
                "candidate_count": len(candidates),
                "candidates": [
                    {
                        "id": candidate["id"],
                        "subject_id": candidate["subject_id"],
                        "dataset": candidate["dataset"],
                        "patient_name": candidate.get("patient_name"),
                        "patient_birth_date": candidate.get("patient_birth_date"),
                        "sex": candidate.get("sex"),
                    }
                    for candidate in candidates
                ],
            }
        )

    return {
        "file_path": str(resolved),
        "dataset": dataset,
        "source_system": DEFAULT_RT_SOURCE,
        "summary": {
            "rows_total": len(rows),
            "subjects_in_dataset": len(subjects),
            "matched_rows": matched,
            "ambiguous_rows": ambiguous,
            "unmatched_rows": unmatched,
        },
        "rows": preview_rows,
    }


def _subject_anchor_date(conn, subject_pk: int) -> date | None:
    row = conn.execute(
        """
        SELECT study_date
        FROM sessions
        WHERE subject_id = ? AND study_date IS NOT NULL AND study_date != ''
        ORDER BY study_date
        LIMIT 1
        """,
        (subject_pk,),
    ).fetchone()
    if not row or not row["study_date"]:
        return None
    try:
        return datetime.strptime(str(row["study_date"]), "%Y-%m-%d").date()
    except ValueError:
        return None


def _days_from_anchor(anchor_date: date | None, event_date_iso: str | None) -> int | None:
    if anchor_date is None or not event_date_iso:
        return None
    try:
        event_dt = datetime.strptime(event_date_iso, "%Y-%m-%d").date()
    except ValueError:
        return None
    return (event_dt - anchor_date).days


def _update_subject_from_rt(conn, subject: dict[str, Any], row: RtRow) -> None:
    updates: dict[str, Any] = {}
    if not subject.get("patient_name") and row.patient_name_raw:
        updates["patient_name"] = f"{row.patient_given_name} {row.patient_family_name}".strip()
    if not subject.get("patient_family_name") and row.patient_family_name:
        updates["patient_family_name"] = row.patient_family_name
    if not subject.get("patient_given_name") and row.patient_given_name:
        updates["patient_given_name"] = row.patient_given_name
    if row.diagnosis_date:
        birth_raw = subject.get("patient_birth_date")
        birth_date = None
        if birth_raw and re.fullmatch(r"\d{8}", str(birth_raw)):
            birth_date = datetime.strptime(str(birth_raw), "%Y%m%d").date()
        if birth_date is not None:
            dx_date = datetime.strptime(row.diagnosis_date, "%Y-%m-%d").date()
            age_years = round((dx_date - birth_date).days / 365.25, 2)
            updates["age_at_diagnosis"] = age_years
    if updates:
        assignments = ", ".join(f"{column} = ?" for column in updates)
        values = list(updates.values()) + [subject["id"]]
        conn.execute(
            f"UPDATE subjects SET {assignments}, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?",
            values,
        )


def _resolve_or_create_subject_from_rt(conn, row: RtRow, dataset: str, source_file: str) -> dict[str, Any]:
    person_key = normalize_person_key(row.patient_family_name, row.patient_given_name)
    subject_row = None
    if row.ida:
        subject_row = find_subject_by_alias(
            conn,
            source_system="mosaiq_rt",
            alias_type="rt_ida",
            alias_value=row.ida,
            dataset=dataset,
        )
    if subject_row is None and row.tax_code:
        subject_row = find_subject_by_alias(
            conn,
            source_system="identity",
            alias_type="tax_code",
            alias_value=row.tax_code,
            dataset=dataset,
        )
    if subject_row is None and person_key:
        subject_row = find_subject_by_alias(
            conn,
            source_system="identity",
            alias_type="person_key",
            alias_norm=person_key,
            dataset=dataset,
        )
    if subject_row is None:
        subject_pk = create_subject(
            conn,
            dataset,
            patient_name=f"{row.patient_given_name} {row.patient_family_name}".strip() or row.patient_name_raw,
            patient_given_name=row.patient_given_name or None,
            patient_family_name=row.patient_family_name or None,
            notes=f"Imported from RT file {Path(source_file).name}",
        )
        subject_row = conn.execute("SELECT * FROM subjects WHERE id = ?", (subject_pk,)).fetchone()

    subject_dict = dict(subject_row)
    update_subject_demographics(
        conn,
        int(subject_dict["id"]),
        patient_name=f"{row.patient_given_name} {row.patient_family_name}".strip() or row.patient_name_raw,
        patient_given_name=row.patient_given_name or None,
        patient_family_name=row.patient_family_name or None,
        notes=f"Imported from RT file {Path(source_file).name}",
    )
    add_subject_alias(
        conn,
        int(subject_dict["id"]),
        source_system="mosaiq_rt",
        alias_type="rt_patient_name",
        alias_value=_canonical_name(row.patient_family_name, row.patient_given_name),
        alias_norm=person_key,
        raw_value=row.patient_name_raw,
    )
    if person_key:
        add_subject_alias(
            conn,
            int(subject_dict["id"]),
            source_system="identity",
            alias_type="person_key",
            alias_value=person_key,
            alias_norm=person_key,
            raw_value=row.patient_name_raw,
        )
    if row.ida:
        add_subject_alias(
            conn,
            int(subject_dict["id"]),
            source_system="mosaiq_rt",
            alias_type="rt_ida",
            alias_value=row.ida,
            raw_value=row.ida,
        )
    if row.tax_code:
        add_subject_alias(
            conn,
            int(subject_dict["id"]),
            source_system="identity",
            alias_type="tax_code",
            alias_value=row.tax_code,
            raw_value=row.tax_code,
        )
    refreshed = conn.execute("SELECT * FROM subjects WHERE id = ?", (int(subject_dict["id"]),)).fetchone()
    return dict(refreshed)


def _upsert_subject_ref(conn, subject_id: int, ref_type: str, ref_value: str | None, raw_value: str | None) -> None:
    if not ref_value:
        return
    existing = conn.execute(
        """
        SELECT id
        FROM subject_external_refs
        WHERE subject_id = ? AND source_system = ? AND ref_type = ?
        """,
        (subject_id, DEFAULT_RT_SOURCE, ref_type),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE subject_external_refs
            SET ref_value = ?, raw_value = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (ref_value, raw_value, existing["id"]),
        )
        return
    conn.execute(
        """
        INSERT INTO subject_external_refs (
            subject_id, source_system, ref_type, ref_value, raw_value
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (subject_id, DEFAULT_RT_SOURCE, ref_type, ref_value, raw_value),
    )


def _upsert_radiotherapy_course(conn, subject_id: int, source_file: str, row: RtRow) -> None:
    if row.ida is None:
        existing = conn.execute(
            """
            SELECT id
            FROM radiotherapy_courses
            WHERE subject_id = ? AND source_system = ? AND external_course_id IS NULL
            """,
            (subject_id, DEFAULT_RT_SOURCE),
        ).fetchone()
    else:
        existing = conn.execute(
            """
            SELECT id
            FROM radiotherapy_courses
            WHERE subject_id = ? AND source_system = ? AND external_course_id = ?
            """,
            (subject_id, DEFAULT_RT_SOURCE, row.ida),
        ).fetchone()
    payload = (
        row.ida,
        row.patient_name_raw,
        row.tax_code,
        row.fractions_count,
        row.start_date,
        row.diagnosis_date,
        source_file,
    )
    if existing:
        conn.execute(
            """
            UPDATE radiotherapy_courses
            SET external_course_id = ?, raw_patient_name = ?, tax_code = ?,
                fractions_count = ?, start_date = ?, diagnosis_date = ?,
                source_file = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            payload + (existing["id"],),
        )
        return
    conn.execute(
        """
        INSERT INTO radiotherapy_courses (
            subject_id, source_system, external_course_id, raw_patient_name,
            tax_code, fractions_count, start_date, diagnosis_date, source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (subject_id, DEFAULT_RT_SOURCE) + payload,
    )


def _cleanup_nat_values(conn) -> None:
    conn.execute(
        """
        UPDATE radiotherapy_courses
        SET start_date = NULL
        WHERE start_date IN ('NaT', '')
        """
    )
    conn.execute(
        """
        UPDATE radiotherapy_courses
        SET diagnosis_date = NULL
        WHERE diagnosis_date IN ('NaT', '')
        """
    )
    conn.execute(
        """
        UPDATE clinical_events
        SET event_date = NULL
        WHERE event_date IN ('NaT', '')
        """
    )


def _upsert_event(
    conn,
    subject_id: int,
    event_type: str,
    event_date: str | None,
    days_from_baseline: int | None,
    description: str | None,
) -> None:
    if not event_date and days_from_baseline is None:
        return
    existing = None
    if event_date:
        existing = conn.execute(
            """
            SELECT id
            FROM clinical_events
            WHERE subject_id = ? AND event_type = ? AND event_date = ?
            """,
            (subject_id, event_type, event_date),
        ).fetchone()
    if existing is None and days_from_baseline is not None:
        existing = conn.execute(
            """
            SELECT id
            FROM clinical_events
            WHERE subject_id = ? AND event_type = ? AND days_from_baseline = ?
            """,
            (subject_id, event_type, days_from_baseline),
        ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE clinical_events
            SET event_date = ?, days_from_baseline = ?, description = ?
            WHERE id = ?
            """,
            (event_date, days_from_baseline, description, existing["id"]),
        )
        return
    conn.execute(
        """
        INSERT INTO clinical_events (
            subject_id, event_type, event_date, days_from_baseline, description
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (subject_id, event_type, event_date, days_from_baseline, description),
    )


def commit_rt_excel(file_path: str, dataset: str = DEFAULT_RT_DATASET) -> dict[str, Any]:
    analysis = analyze_rt_excel(file_path=file_path, dataset=dataset)
    resolved = analysis["file_path"]

    imported = 0
    skipped_ambiguous = 0
    skipped_unmatched = 0
    imported_subjects: list[dict[str, Any]] = []

    source_rows = {row.row_index: row for row in _read_excel_rows(Path(resolved))}

    with db() as conn:
        _cleanup_nat_values(conn)
        for preview in analysis["rows"]:
            status = preview["status"]
            if status == "ambiguous":
                skipped_ambiguous += 1
                continue

            source_row = source_rows[preview["row_index"]]
            if status == "matched":
                subject = preview["candidates"][0]
                subject_row = conn.execute(
                    "SELECT * FROM subjects WHERE id = ?",
                    (subject["id"],),
                ).fetchone()
                if not subject_row:
                    skipped_unmatched += 1
                    continue
                subject_dict = dict(subject_row)
            else:
                skipped_unmatched += 1
                continue

            _update_subject_from_rt(conn, subject_dict, source_row)
            _upsert_subject_ref(conn, subject_dict["id"], "ida", source_row.ida, source_row.ida)
            _upsert_subject_ref(conn, subject_dict["id"], "tax_code", source_row.tax_code, source_row.tax_code)
            _upsert_subject_ref(
                conn,
                subject_dict["id"],
                "rt_patient_name",
                _canonical_name(source_row.patient_family_name, source_row.patient_given_name),
                source_row.patient_name_raw,
            )
            _upsert_radiotherapy_course(conn, subject_dict["id"], resolved, source_row)

            anchor = _subject_anchor_date(conn, subject_dict["id"])
            dx_days = _days_from_anchor(anchor, source_row.diagnosis_date)
            rt_days = _days_from_anchor(anchor, source_row.start_date)
            _upsert_event(
                conn,
                subject_dict["id"],
                "diagnosis",
                source_row.diagnosis_date,
                dx_days,
                f"Imported from {Path(resolved).name}",
            )
            rt_description = f"Imported from {Path(resolved).name}"
            if source_row.fractions_count is not None:
                rt_description += f" · fractions={source_row.fractions_count}"
            _upsert_event(
                conn,
                subject_dict["id"],
                "radiotherapy_start",
                source_row.start_date,
                rt_days,
                rt_description,
            )
            sync_subject_timeline_offsets(conn, subject_dict["id"])

            imported += 1
            imported_subjects.append(
                {
                    "subject_pk": subject_dict["id"],
                    "subject_id": subject_dict["subject_id"],
                    "patient_name": subject_dict.get("patient_name"),
                    "ida": source_row.ida,
                    "start_date": source_row.start_date,
                    "diagnosis_date": source_row.diagnosis_date,
                    "fractions_count": source_row.fractions_count,
                }
            )

    return {
        "file_path": resolved,
        "dataset": dataset,
        "source_system": DEFAULT_RT_SOURCE,
        "summary": {
            **analysis["summary"],
            "imported_rows": imported,
            "skipped_ambiguous": skipped_ambiguous,
            "skipped_unmatched": skipped_unmatched,
        },
        "imported_subjects": imported_subjects,
        "rows": analysis["rows"],
    }
