"""Shared helpers for GlioTwin dataset importers."""

from __future__ import annotations
import math
import sqlite3
from pathlib import Path

# Suffissi / nomi che indicano download incompleto
_PARTIAL_SUFFIXES = {'.partial', '.aspera-ckpt', '.aspera_ckpt'}


def is_partial(path: Path) -> bool:
    """True se il file è un download incompleto o ha un .partial accanto."""
    if path.suffix in _PARTIAL_SUFFIXES or path.name in _PARTIAL_SUFFIXES:
        return True
    return any((path.parent / (path.name + s)).exists() for s in _PARTIAL_SUFFIXES)


# ── Normalizzatori ───────────────────────────────────────────────────────────

def norm_sex(val) -> str | None:
    if val is None:
        return None
    v = str(val).strip().lower()
    if v in ('nan', 'na', ''):
        return None
    if v.startswith('f'):
        return 'F'
    if v.startswith('m'):
        return 'M'
    return None


def norm_idh(val) -> str:
    if val is None:
        return 'unknown'
    v = str(val).strip().lower()
    if any(x in v for x in ('mutant', 'mutated', 'positive', 'r132')):
        return 'mutated'
    if any(x in v for x in ('wt', 'wildtype', 'wild type', 'wild-type', 'negative')):
        return 'wildtype'
    return 'unknown'


def norm_mgmt(val) -> str:
    if val is None:
        return 'unknown'
    v = str(val).strip().lower()
    if 'unmethyl' in v or 'not methyl' in v:
        return 'unmethylated'
    if 'methyl' in v:
        return 'methylated'
    return 'unknown'


def safe_str(val) -> str | None:
    """Ritorna str pulita o None se vuota/NaN."""
    if val is None:
        return None
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return None
    except Exception:
        pass
    s = str(val).strip()
    return s if s and s.lower() not in ('nan', 'na', 'none', '') else None


def safe_int(val) -> int | None:
    try:
        f = float(val)
        return None if math.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None


def safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def read_nifti_header(path: Path) -> dict:
    """Legge shape e spacing dall'header NIfTI senza caricare i dati."""
    try:
        import nibabel as nib
        img = nib.load(str(path))
        shape = img.shape
        zooms = img.header.get_zooms()
        return {
            'shape_x':   int(shape[0]) if len(shape) > 0 else None,
            'shape_y':   int(shape[1]) if len(shape) > 1 else None,
            'shape_z':   int(shape[2]) if len(shape) > 2 else None,
            'spacing_x': float(zooms[0]) if len(zooms) > 0 else None,
            'spacing_y': float(zooms[1]) if len(zooms) > 1 else None,
            'spacing_z': float(zooms[2]) if len(zooms) > 2 else None,
        }
    except Exception:
        return {}


# ── DB helpers ───────────────────────────────────────────────────────────────

def get_or_create_subject(conn: sqlite3.Connection, subject_id: str,
                           dataset: str, **kw) -> int:
    conn.execute(
        """INSERT OR IGNORE INTO subjects
           (subject_id, dataset, sex, age_at_diagnosis, diagnosis,
            idh_status, mgmt_status, os_days, vital_status, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (subject_id, dataset,
         kw.get('sex'), kw.get('age_at_diagnosis'), kw.get('diagnosis'),
         kw.get('idh_status', 'unknown'), kw.get('mgmt_status', 'unknown'),
         kw.get('os_days'), kw.get('vital_status'), kw.get('notes')),
    )
    return conn.execute(
        "SELECT id FROM subjects WHERE subject_id=? AND dataset=?",
        (subject_id, dataset)
    ).fetchone()[0]


def get_or_create_session(conn: sqlite3.Connection, subject_pk: int,
                           session_label: str, **kw) -> int:
    conn.execute(
        """INSERT OR IGNORE INTO sessions
           (subject_id, session_label, days_from_baseline, timepoint_type,
            clinical_context, raw_dir, processed_dir, quality_flag)
           VALUES (?,?,?,?,?,?,?,?)""",
        (subject_pk, session_label,
         kw.get('days_from_baseline'), kw.get('timepoint_type', 'other'),
         kw.get('clinical_context'), kw.get('raw_dir'), kw.get('processed_dir'),
         kw.get('quality_flag', 'ok')),
    )
    return conn.execute(
        "SELECT id FROM sessions WHERE subject_id=? AND session_label=?",
        (subject_pk, session_label)
    ).fetchone()[0]


def get_or_create_sequence(conn: sqlite3.Connection, session_pk: int,
                            sequence_type: str, **kw) -> int:
    hdr = kw.get('header', {})
    conn.execute(
        """INSERT OR IGNORE INTO sequences
           (session_id, sequence_type, contrast_agent, raw_path, processed_path,
            shape_x, shape_y, shape_z, spacing_x, spacing_y, spacing_z)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (session_pk, sequence_type,
         kw.get('contrast_agent', 1 if sequence_type == 'T1ce' else 0),
         kw.get('raw_path'), kw.get('processed_path'),
         hdr.get('shape_x'), hdr.get('shape_y'), hdr.get('shape_z'),
         hdr.get('spacing_x'), hdr.get('spacing_y'), hdr.get('spacing_z')),
    )
    return conn.execute(
        "SELECT id FROM sequences WHERE session_id=? AND sequence_type=?",
        (session_pk, sequence_type)
    ).fetchone()[0]


def insert_computed_structure(conn: sqlite3.Connection, session_pk: int,
                               seq_pk: int | None, label: str,
                               label_code: int | None, mask_path: str,
                               model_name: str, **kw):
    conn.execute(
        """INSERT OR IGNORE INTO computed_structures
           (session_id, sequence_id, label, label_code, mask_path, reference_space,
            model_name, model_version, volume_ml, confidence_score, dice_vs_gt)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (session_pk, seq_pk, label, label_code, mask_path,
         kw.get('reference_space', 'native'),
         model_name, kw.get('model_version'),
         kw.get('volume_ml'), kw.get('confidence_score'), kw.get('dice_vs_gt')),
    )


def insert_radiological_structure(conn: sqlite3.Connection, session_pk: int,
                                   seq_pk: int | None, label: str,
                                   mask_path: str, **kw):
    conn.execute(
        """INSERT OR IGNORE INTO radiological_structures
           (session_id, sequence_id, label, label_code, mask_path, reference_space,
            annotator, volume_ml, is_ground_truth)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (session_pk, seq_pk, label, kw.get('label_code'), mask_path,
         kw.get('reference_space', 'native'),
         kw.get('annotator'), kw.get('volume_ml'),
         kw.get('is_ground_truth', 1)),
    )


def insert_clinical_event(conn: sqlite3.Connection, subject_pk: int,
                           session_pk: int | None, event_type: str,
                           days: int | None, **kw):
    conn.execute(
        """INSERT OR IGNORE INTO clinical_events
           (subject_id, session_id, event_type, days_from_baseline,
            rano_response, treatment_agent, description)
           VALUES (?,?,?,?,?,?,?)""",
        (subject_pk, session_pk, event_type, days,
         kw.get('rano_response'),
         kw.get('treatment_agent'), kw.get('description')),
    )
