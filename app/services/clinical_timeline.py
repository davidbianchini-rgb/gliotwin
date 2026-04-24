from __future__ import annotations

import sqlite3
from datetime import date


def _parse_iso(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _days_between(anchor: str | None, target: str | None) -> int | None:
    anchor_dt = _parse_iso(anchor)
    target_dt = _parse_iso(target)
    if not anchor_dt or not target_dt:
        return None
    return (target_dt - anchor_dt).days


def subject_diagnosis_date(conn: sqlite3.Connection, subject_id: int) -> str | None:
    row = conn.execute(
        """
        SELECT event_date
        FROM clinical_events
        WHERE subject_id = ? AND event_type = 'diagnosis' AND COALESCE(event_date, '') <> ''
        ORDER BY event_date, id
        LIMIT 1
        """,
        (subject_id,),
    ).fetchone()
    if row and row[0]:
        return row[0]
    row = conn.execute(
        """
        SELECT diagnosis_date
        FROM radiotherapy_courses
        WHERE subject_id = ? AND COALESCE(diagnosis_date, '') <> ''
        ORDER BY diagnosis_date, updated_at DESC
        LIMIT 1
        """,
        (subject_id,),
    ).fetchone()
    return row[0] if row and row[0] else None


def sync_subject_timeline_offsets(conn: sqlite3.Connection, subject_id: int) -> dict[str, int]:
    diagnosis_date = subject_diagnosis_date(conn, subject_id)
    stats = {
        "clinical_events_updated": 0,
        "sessions_updated": 0,
    }
    if not diagnosis_date:
        return stats

    diagnosis_row = conn.execute(
        """
        SELECT id, event_date, days_from_baseline
        FROM clinical_events
        WHERE subject_id = ? AND event_type = 'diagnosis'
        ORDER BY COALESCE(event_date, ''), id
        LIMIT 1
        """,
        (subject_id,),
    ).fetchone()
    if diagnosis_row is None:
        conn.execute(
            """
            INSERT INTO clinical_events (
                subject_id, event_type, event_date, days_from_baseline, description
            ) VALUES (?, 'diagnosis', ?, 0, 'Backfilled from diagnosis baseline')
            """,
            (subject_id, diagnosis_date),
        )
        stats["clinical_events_updated"] += 1
    elif diagnosis_row[1] != diagnosis_date or diagnosis_row[2] != 0:
        conn.execute(
            "UPDATE clinical_events SET event_date = ?, days_from_baseline = 0 WHERE id = ?",
            (diagnosis_date, diagnosis_row[0]),
        )
        stats["clinical_events_updated"] += 1

    event_rows = conn.execute(
        """
        SELECT id, event_date, days_from_baseline
        FROM clinical_events
        WHERE subject_id = ? AND event_type <> 'diagnosis'
        """,
        (subject_id,),
    ).fetchall()
    for row in event_rows:
        calc_days = _days_between(diagnosis_date, row[1])
        if calc_days is None or row[2] == calc_days:
            continue
        conn.execute("UPDATE clinical_events SET days_from_baseline = ? WHERE id = ?", (calc_days, row[0]))
        stats["clinical_events_updated"] += 1

    session_rows = conn.execute(
        """
        SELECT id, study_date, days_from_baseline
        FROM sessions
        WHERE subject_id = ?
        """,
        (subject_id,),
    ).fetchall()
    for row in session_rows:
        calc_days = _days_between(diagnosis_date, row[1])
        if calc_days is None or row[2] == calc_days:
            continue
        conn.execute("UPDATE sessions SET days_from_baseline = ? WHERE id = ?", (calc_days, row[0]))
        stats["sessions_updated"] += 1
    return stats
