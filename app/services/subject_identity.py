from __future__ import annotations

import re
import unicodedata
from typing import Any


def normalize_alias(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def normalize_person_key(family_name: Any, given_name: Any, birth_date: Any = None) -> str | None:
    def _norm_part(value: Any) -> str:
        text = "" if value is None else str(value)
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.upper().strip()
        text = text.replace(",", " ").replace("'", " ").replace("-", " ")
        text = re.sub(r"[^A-Z0-9 ]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    family = _norm_part(family_name)
    given = _norm_part(given_name)
    birth = _norm_part(birth_date)
    if not family and not given:
      return None
    return "|".join([family, given, birth])


def _next_internal_subject_code(conn) -> str:
    row = conn.execute(
        """
        SELECT subject_id
        FROM subjects
        WHERE subject_id GLOB 'GT-[0-9][0-9][0-9][0-9][0-9][0-9]'
        ORDER BY subject_id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row or not row["subject_id"]:
        return "GT-000001"
    current = int(str(row["subject_id"]).split("-")[-1])
    return f"GT-{current + 1:06d}"


def create_subject(
    conn,
    dataset: str,
    *,
    patient_name: str | None = None,
    patient_given_name: str | None = None,
    patient_family_name: str | None = None,
    patient_birth_date: str | None = None,
    sex: str | None = None,
    notes: str | None = None,
) -> int:
    internal_code = _next_internal_subject_code(conn)
    conn.execute(
        """
        INSERT INTO subjects (
            subject_id, dataset, patient_name, patient_given_name,
            patient_family_name, patient_birth_date, sex, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            internal_code,
            dataset,
            patient_name,
            patient_given_name,
            patient_family_name,
            patient_birth_date,
            sex,
            notes,
        ),
    )
    row = conn.execute(
        "SELECT id FROM subjects WHERE subject_id = ? AND dataset = ?",
        (internal_code, dataset),
    ).fetchone()
    return int(row["id"])


def add_subject_alias(
    conn,
    subject_pk: int,
    *,
    source_system: str,
    alias_type: str,
    alias_value: str | None,
    alias_norm: str | None = None,
    raw_value: str | None = None,
) -> None:
    alias_value = normalize_alias(alias_value)
    alias_norm = normalize_alias(alias_norm)
    if not alias_value and not alias_norm:
        return

    existing = conn.execute(
        """
        SELECT id, subject_id
        FROM subject_aliases
        WHERE source_system = ?
          AND alias_type = ?
          AND (
            alias_value = ?
            OR (? IS NOT NULL AND alias_norm = ?)
          )
        LIMIT 1
        """,
        (source_system, alias_type, alias_value, alias_norm, alias_norm),
    ).fetchone()
    if existing:
        if int(existing["subject_id"]) != subject_pk:
            raise ValueError(f"Alias already linked to subject {existing['subject_id']}")
        conn.execute(
            """
            UPDATE subject_aliases
            SET alias_value = COALESCE(?, alias_value),
                alias_norm = COALESCE(?, alias_norm),
                raw_value = COALESCE(?, raw_value),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (alias_value, alias_norm, raw_value, existing["id"]),
        )
        return

    conn.execute(
        """
        INSERT INTO subject_aliases (
            subject_id, source_system, alias_type, alias_value, alias_norm, raw_value
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (subject_pk, source_system, alias_type, alias_value or alias_norm, alias_norm, raw_value),
    )


def find_subject_by_alias(
    conn,
    *,
    source_system: str,
    alias_type: str,
    alias_value: str | None = None,
    alias_norm: str | None = None,
    dataset: str | None = None,
):
    alias_value = normalize_alias(alias_value)
    alias_norm = normalize_alias(alias_norm)
    if not alias_value and not alias_norm:
        return None
    row = conn.execute(
        f"""
        SELECT s.*
        FROM subject_aliases sa
        JOIN subjects s ON s.id = sa.subject_id
        WHERE sa.source_system = ?
          AND sa.alias_type = ?
          AND (
            sa.alias_value = ?
            OR (? IS NOT NULL AND sa.alias_norm = ?)
          )
          {"AND s.dataset = ?" if dataset else ""}
        ORDER BY s.id
        LIMIT 1
        """,
        tuple(
            [source_system, alias_type, alias_value, alias_norm, alias_norm]
            + ([dataset] if dataset else [])
        ),
    ).fetchone()
    return row


def update_subject_demographics(
    conn,
    subject_pk: int,
    *,
    patient_name: str | None = None,
    patient_given_name: str | None = None,
    patient_family_name: str | None = None,
    patient_birth_date: str | None = None,
    sex: str | None = None,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE subjects
        SET patient_name = COALESCE(NULLIF(patient_name, ''), ?),
            patient_given_name = COALESCE(NULLIF(patient_given_name, ''), ?),
            patient_family_name = COALESCE(NULLIF(patient_family_name, ''), ?),
            patient_birth_date = COALESCE(NULLIF(patient_birth_date, ''), ?),
            sex = COALESCE(NULLIF(sex, ''), ?),
            notes = CASE
                WHEN COALESCE(notes, '') = '' THEN ?
                ELSE notes
            END,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ?
        """,
        (
            patient_name,
            patient_given_name,
            patient_family_name,
            patient_birth_date,
            sex,
            notes,
            subject_pk,
        ),
    )
