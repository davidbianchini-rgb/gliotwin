from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "db" / "gliotwin.db"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.clinical_timeline import sync_subject_timeline_offsets


def _subjects_with_rt_diagnosis(conn: sqlite3.Connection, dataset: str | None) -> list[sqlite3.Row]:
    sql = """
        SELECT
            s.id,
            s.subject_id,
            s.dataset,
            (
                SELECT rt.diagnosis_date
                FROM radiotherapy_courses rt
                WHERE rt.subject_id = s.id
                  AND COALESCE(rt.diagnosis_date, '') <> ''
                ORDER BY COALESCE(rt.diagnosis_date, ''), rt.updated_at DESC
                LIMIT 1
            ) AS diagnosis_date
        FROM subjects s
        WHERE EXISTS (
            SELECT 1
            FROM radiotherapy_courses rt
            WHERE rt.subject_id = s.id
              AND COALESCE(rt.diagnosis_date, '') <> ''
        )
    """
    params: list[str] = []
    if dataset:
        sql += " AND s.dataset = ?"
        params.append(dataset)
    sql += " ORDER BY s.id"
    return conn.execute(sql, params).fetchall()


def run(dataset: str | None = None, dry_run: bool = False) -> dict[str, int]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    stats = {
        "subjects_seen": 0,
        "diagnosis_events_inserted": 0,
        "diagnosis_events_updated": 0,
        "clinical_events_updated": 0,
        "sessions_updated": 0,
    }

    try:
        subjects = _subjects_with_rt_diagnosis(conn, dataset)
        stats["subjects_seen"] = len(subjects)
        for subj in subjects:
            subject_id = subj["id"]
            diagnosis_date = subj["diagnosis_date"]
            if not diagnosis_date:
                continue
            before_dx = conn.execute(
                "SELECT COUNT(*) FROM clinical_events WHERE subject_id = ? AND event_type = 'diagnosis'",
                (subject_id,),
            ).fetchone()[0]
            result = sync_subject_timeline_offsets(conn, subject_id)
            after_dx = conn.execute(
                "SELECT COUNT(*) FROM clinical_events WHERE subject_id = ? AND event_type = 'diagnosis'",
                (subject_id,),
            ).fetchone()[0]
            if after_dx > before_dx:
                stats["diagnosis_events_inserted"] += 1
            elif result["clinical_events_updated"] > 0:
                stats["diagnosis_events_updated"] += 1
            stats["clinical_events_updated"] += result["clinical_events_updated"]
            stats["sessions_updated"] += result["sessions_updated"]

        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        return stats
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill diagnosis baseline from radiotherapy courses.")
    parser.add_argument("--dataset", default=None, help="Optional dataset filter, e.g. irst or irst_rt")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to DB")
    args = parser.parse_args()
    stats = run(dataset=args.dataset, dry_run=args.dry_run)
    for key, value in stats.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
