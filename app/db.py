"""SQLite connection helpers for GlioTwin."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "gliotwin.db"
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "db" / "schema.sql"
SQLITE_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30_000


def rows_as_dicts(rows) -> list[dict]:
    """Converte una lista di sqlite3.Row in lista di dict."""
    return [dict(row) for row in rows]


def _table_sql(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row[0] if row and row[0] else ""


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _ensure_subjects_table(conn: sqlite3.Connection) -> None:
    sql = _table_sql(conn, "subjects")
    if "CHECK(dataset IN" not in sql:
        return

    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("ALTER TABLE subjects RENAME TO subjects_old")
    conn.execute("""
        CREATE TABLE subjects (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id          TEXT    NOT NULL,
            dataset             TEXT    NOT NULL,
            patient_name        TEXT,
            patient_given_name  TEXT,
            patient_family_name TEXT,
            patient_birth_date  TEXT,
            sex                 TEXT,
            age_at_diagnosis    REAL,
            diagnosis           TEXT,
            idh_status          TEXT,
            mgmt_status         TEXT,
            os_days             INTEGER,
            vital_status        TEXT,
            notes               TEXT,
            created_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            updated_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            UNIQUE(subject_id, dataset)
        )
    """)
    conn.execute("""
        INSERT INTO subjects (
            id, subject_id, dataset, sex, age_at_diagnosis, diagnosis,
            idh_status, mgmt_status, os_days, vital_status, notes, created_at, updated_at
        )
        SELECT
            id, subject_id, dataset, sex, age_at_diagnosis, diagnosis,
            idh_status, mgmt_status, os_days, vital_status, notes, created_at, updated_at
        FROM subjects_old
    """)
    conn.execute("DROP TABLE subjects_old")
    conn.execute("PRAGMA foreign_keys = ON")


def _ensure_column(conn: sqlite3.Connection, table: str, name: str, ddl: str) -> None:
    if name in _column_names(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _repair_sessions_fk_reference(conn: sqlite3.Connection) -> None:
    sql = _table_sql(conn, "sessions")
    if "subjects_old" not in sql:
        return
    version = conn.execute("PRAGMA schema_version").fetchone()[0]
    conn.execute("PRAGMA writable_schema = ON")
    conn.execute(
        """
        UPDATE sqlite_master
        SET sql = REPLACE(sql, 'REFERENCES "subjects_old"(id)', 'REFERENCES subjects(id)')
        WHERE type = 'table' AND name = 'sessions'
        """
    )
    conn.execute("PRAGMA writable_schema = OFF")
    conn.execute(f"PRAGMA schema_version = {version + 1}")


def _ensure_sequences_check_constraint(conn: sqlite3.Connection) -> None:
    sql = _table_sql(conn, "sequences")
    if not sql or "'APT'" in sql:
        return
    old = "'DWI','ADC','DSC','RSI','CBF','CBV','MTT','SWAN','OTHER'"
    new = "'DWI','ADC','DSC','RSI','CBF','CBV','MTT','SWAN','APT','OTHER'"
    if old not in sql:
        return
    version = conn.execute("PRAGMA schema_version").fetchone()[0]
    conn.execute("PRAGMA writable_schema = ON")
    conn.execute(
        """
        UPDATE sqlite_master
        SET sql = REPLACE(sql, ?, ?)
        WHERE type = 'table' AND name = 'sequences'
        """,
        (old, new),
    )
    conn.execute("PRAGMA writable_schema = OFF")
    conn.execute(f"PRAGMA schema_version = {version + 1}")


def _run_migrations(conn: sqlite3.Connection) -> None:
    _ensure_subjects_table(conn)
    _repair_sessions_fk_reference(conn)
    _ensure_sequences_check_constraint(conn)
    _ensure_column(conn, "subjects", "patient_name", "TEXT")
    _ensure_column(conn, "subjects", "patient_given_name", "TEXT")
    _ensure_column(conn, "subjects", "patient_family_name", "TEXT")
    _ensure_column(conn, "subjects", "patient_birth_date", "TEXT")
    _ensure_column(conn, "sessions", "study_date", "TEXT")
    _ensure_column(conn, "sessions", "study_time", "TEXT")
    _ensure_column(conn, "sequences", "display_label", "TEXT")
    _ensure_column(conn, "sequences", "import_class", "TEXT")
    _ensure_column(conn, "sequences", "source_series_uid", "TEXT")
    _ensure_column(conn, "sequences", "metadata_json", "TEXT")
    _ensure_column(conn, "clinical_events", "event_date", "TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processing_jobs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id          INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            job_type            TEXT    NOT NULL DEFAULT 'fets_postop',
            status              TEXT    NOT NULL CHECK(status IN (
                                    'queued','running','failed','completed','cancelled'
                                )),
            progress_stage      TEXT,
            input_dir           TEXT,
            run_dir             TEXT,
            final_dir           TEXT,
            log_path            TEXT,
            pid                 INTEGER,
            return_code         INTEGER,
            error_message       TEXT,
            requested_at        TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            started_at          TEXT,
            finished_at         TEXT,
            updated_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_metric_cache (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id          INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            structure_source    TEXT    NOT NULL CHECK(structure_source IN ('computed','radiological')),
            label               TEXT    NOT NULL,
            label_code          INTEGER,
            sequence_type       TEXT    NOT NULL,
            sequence_id         INTEGER REFERENCES sequences(id) ON DELETE SET NULL,
            sequence_path       TEXT,
            mask_path           TEXT,
            volume_ml           REAL,
            n_voxels            INTEGER,
            median              REAL,
            q1                  REAL,
            q3                  REAL,
            min                 REAL,
            max                 REAL,
            signal_error        TEXT,
            computed_at         TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            UNIQUE(session_id, structure_source, label, sequence_type)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_signal_metric_cache_session
        ON signal_metric_cache(session_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_signal_metric_cache_lookup
        ON signal_metric_cache(session_id, label, sequence_type, structure_source)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_metric_jobs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            scope               TEXT    NOT NULL DEFAULT 'all_missing',
            patient_id          INTEGER REFERENCES subjects(id) ON DELETE CASCADE,
            status              TEXT    NOT NULL CHECK(status IN (
                                    'queued','running','failed','completed'
                                )),
            force_recompute     INTEGER NOT NULL DEFAULT 0,
            total_tasks         INTEGER NOT NULL DEFAULT 0,
            completed_tasks     INTEGER NOT NULL DEFAULT 0,
            failed_tasks        INTEGER NOT NULL DEFAULT 0,
            requested_at        TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            started_at          TEXT,
            finished_at         TEXT,
            error_message       TEXT,
            updated_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS subject_external_refs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id          INTEGER NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
            source_system       TEXT    NOT NULL,
            ref_type            TEXT    NOT NULL,
            ref_value           TEXT    NOT NULL,
            raw_value           TEXT,
            created_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            updated_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            UNIQUE(subject_id, source_system, ref_type)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS radiotherapy_courses (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id          INTEGER NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
            source_system       TEXT    NOT NULL,
            external_course_id  TEXT,
            raw_patient_name    TEXT,
            tax_code            TEXT,
            fractions_count     INTEGER,
            start_date          TEXT,
            diagnosis_date      TEXT,
            source_file         TEXT,
            created_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            updated_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            UNIQUE(subject_id, source_system, external_course_id)
        )
        """
    )


def init_db(db_path: Path = DB_PATH) -> None:
    """Crea le tabelle se non esistono, applicando lo schema SQL."""
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS) as conn:
        conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(sql)
        _run_migrations(conn)


@contextmanager
def get_conn(db_path: Path = DB_PATH):
    """Context manager: apre la connessione, attiva FK e row_factory, commit/rollback automatico."""
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# Alias usato dai router
db = get_conn
