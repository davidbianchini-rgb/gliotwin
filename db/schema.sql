PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS subjects (
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
);

CREATE TABLE IF NOT EXISTS sessions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id          INTEGER NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    session_label       TEXT    NOT NULL,
    days_from_baseline  INTEGER,
    timepoint_type      TEXT    NOT NULL CHECK(timepoint_type IN (
                            'baseline','pre_op','post_op','during_treatment',
                            'end_of_treatment','follow_up','recurrence','other'
                        )),
    clinical_context    TEXT,
    study_date          TEXT,
    study_time          TEXT,
    raw_dir             TEXT,
    processed_dir       TEXT,
    quality_flag        TEXT    NOT NULL DEFAULT 'ok',
    created_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(subject_id, session_label)
);

CREATE TABLE IF NOT EXISTS sequences (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    sequence_type       TEXT    NOT NULL CHECK(sequence_type IN (
                            'T1','T1ce','T2','FLAIR',
                            'DWI','ADC','DSC','RSI','CBF','CBV','MTT','SWAN','APT','OTHER'
                        )),
    contrast_agent      INTEGER NOT NULL DEFAULT 0,
    raw_path            TEXT,
    processed_path      TEXT,
    shape_x             INTEGER,
    shape_y             INTEGER,
    shape_z             INTEGER,
    spacing_x           REAL,
    spacing_y           REAL,
    spacing_z           REAL,
    display_label       TEXT,
    import_class        TEXT,
    source_series_uid   TEXT,
    metadata_json       TEXT,
    created_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(session_id, sequence_type)
);

CREATE TABLE IF NOT EXISTS radiological_structures (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    sequence_id         INTEGER REFERENCES sequences(id) ON DELETE SET NULL,
    label               TEXT    NOT NULL,
    label_code          INTEGER,
    mask_path           TEXT,
    reference_space     TEXT    NOT NULL DEFAULT 'native',
    annotator           TEXT,
    volume_ml           REAL,
    is_ground_truth     INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(session_id, label, annotator)
);

CREATE TABLE IF NOT EXISTS computed_structures (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    sequence_id         INTEGER REFERENCES sequences(id) ON DELETE SET NULL,
    label               TEXT    NOT NULL,
    label_code          INTEGER,
    mask_path           TEXT,
    reference_space     TEXT    NOT NULL DEFAULT 'native',
    model_name          TEXT,
    model_version       TEXT,
    volume_ml           REAL,
    confidence_score    REAL,
    dice_vs_gt          REAL,
    created_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(session_id, model_name, label)
);

CREATE TABLE IF NOT EXISTS clinical_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id          INTEGER NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    session_id          INTEGER REFERENCES sessions(id) ON DELETE SET NULL,
    event_type          TEXT    NOT NULL CHECK(event_type IN (
                            'diagnosis','surgery','radiotherapy_start','radiotherapy_end',
                            'chemotherapy_start','chemotherapy_end','response_assessment',
                            'progression','death','other'
                        )),
    event_date          TEXT,
    days_from_baseline  INTEGER,
    rano_response       TEXT    CHECK(rano_response IN ('CR','PR','SD','PD','not_applicable')),
    treatment_agent     TEXT,
    description         TEXT,
    created_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(subject_id, event_type, days_from_baseline)
);

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
);

CREATE TABLE IF NOT EXISTS subject_aliases (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id          INTEGER NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    source_system       TEXT    NOT NULL,
    alias_type          TEXT    NOT NULL,
    alias_value         TEXT    NOT NULL,
    alias_norm          TEXT,
    raw_value           TEXT,
    created_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(source_system, alias_type, alias_value),
    UNIQUE(source_system, alias_type, alias_norm)
);

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
);

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
);
