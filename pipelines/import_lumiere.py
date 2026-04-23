"""Importa il dataset LUMIERE in GlioTwin."""

from __future__ import annotations
import csv
import sqlite3
from pathlib import Path

from tqdm import tqdm

from ._utils import (
    get_or_create_subject, get_or_create_session, get_or_create_sequence,
    insert_computed_structure, insert_clinical_event,
    norm_sex, safe_float, read_nifti_header, is_partial,
)

LUMIERE_ROOT = Path("/mnt/dati/lumiere")
IMAGING_ROOT = LUMIERE_ROOT / "Imaging"
DEMO_CSV     = LUMIERE_ROOT / "LUMIERE-Demographics_Pathology.csv"
RATING_CSV   = LUMIERE_ROOT / "LUMIERE-ExpertRating-v202211.csv"

# Sequenze native: nome file → (sequence_type, contrast_agent)
SEQ_FILES = {
    'T1.nii.gz':    ('T1',    0),
    'CT1.nii.gz':   ('T1ce',  1),
    'T2.nii.gz':    ('T2',    0),
    'FLAIR.nii.gz': ('FLAIR', 0),
}

# HD-GLIO-AUTO: label_code → label_name (usato come prefisso nel nome struttura)
HD_LABELS = {
    1: 'tumor_core',
    2: 'enhancing_tumor',
}

# Segmentazione nativa per sequenza: nome file → chiave sequenza
SEG_NATIVE = {
    'segmentation_T1_origspace.nii.gz':    'T1',
    'segmentation_CT1_origspace.nii.gz':   'T1ce',
    'segmentation_T2_origspace.nii.gz':    'T2',
    'segmentation_FLAIR_origspace.nii.gz': 'FLAIR',
}

# ExpertRating → timepoint_type
RATING_TO_TP = {
    'Pre-Op':     'pre_op',
    'Post-Op':    'post_op',
    'Post-Op ':   'post_op',
    'SD':         'follow_up',
    'PR':         'follow_up',
    'CR':         'follow_up',
    'PD':         'recurrence',
    'Post-Op/PD': 'recurrence',
    'None':       'other',
    '':           'other',
}

# RANO valori validi per clinical_events
RANO_VALID = {'CR', 'PR', 'SD', 'PD'}


def _is_native_lumiere_seg(seg_path: Path) -> bool:
    path_lower = str(seg_path).lower()
    return (
        seg_path.name in SEG_NATIVE
        and '/native/' in path_lower
        and 'origspace' in seg_path.name.lower()
        and 'registered' not in path_lower
        and 'montreal' not in path_lower
        and 'mni' not in path_lower
    )


def load_demographics() -> dict[str, dict]:
    result = {}
    with open(DEMO_CSV, newline='', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            pid = row.get('Patient', '').strip()
            if pid:
                result[pid] = row
    return result


def load_ratings() -> dict[str, dict[str, str]]:
    """Ritorna {patient_id: {week_label: rating_string}}"""
    rating_col = None
    result: dict[str, dict[str, str]] = {}
    with open(RATING_CSV, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if rating_col is None:
                rating_col = next(
                    (k for k in row.keys() if 'Rating' in k and 'rationale' not in k), None
                )
            pid   = row.get('Patient', '').strip()
            week  = row.get('Date', '').strip()
            rating = row.get(rating_col, '').strip() if rating_col else ''
            if pid and week:
                result.setdefault(pid, {})[week] = rating
    return result


def _norm_idh(raw: str) -> str:
    v = raw.strip().upper()
    if v == 'WT':
        return 'wildtype'
    if v in ('', 'NA', 'NAN'):
        return 'unknown'
    return 'mutated'


def _norm_mgmt(raw: str) -> str:
    v = raw.strip().lower()
    if 'not' in v or 'un' in v:
        return 'unmethylated'
    if 'methyl' in v:
        return 'methylated'
    return 'unknown'


def _week_to_days(label: str) -> int | None:
    """'week-044' → 308, 'week-000-1' → 0, 'week-040-2' → 280."""
    parts = label.split('-')
    try:
        return int(parts[1]) * 7
    except (IndexError, ValueError):
        return None


def _import_session(conn: sqlite3.Connection, subject_pk: int,
                    week_dir: Path, rating: str) -> None:

    days    = _week_to_days(week_dir.name)
    tp_type = RATING_TO_TP.get(rating, 'other')
    rano    = rating if rating in RANO_VALID else None

    session_pk = get_or_create_session(
        conn, subject_pk, week_dir.name,
        days_from_baseline=days,
        timepoint_type=tp_type,
        clinical_context=rating if rating else None,
        raw_dir=str(week_dir),
    )

    # ── Sequenze native ────────────────────────────────────────────
    seq_pks: dict[str, int] = {}   # sequence_type → pk
    for fname, (seq_type, contrast) in SEQ_FILES.items():
        fpath = week_dir / fname
        if not fpath.exists() or is_partial(fpath):
            continue
        hdr = read_nifti_header(fpath)
        pk  = get_or_create_sequence(
            conn, session_pk, seq_type,
            contrast_agent=contrast,
            raw_path=str(fpath),
            header=hdr,
        )
        seq_pks[seq_type] = pk

    # ── HD-GLIO-AUTO segmentazioni native (una per sequenza) ───────
    hd_native = week_dir / 'HD-GLIO-AUTO-segmentation' / 'native'
    if not hd_native.exists():
        return

    for seg_fname, seq_type in SEG_NATIVE.items():
        seg_path = hd_native / seg_fname
        if not seg_path.exists() or is_partial(seg_path) or not _is_native_lumiere_seg(seg_path):
            continue
        seq_pk = seq_pks.get(seq_type)

        for label_code, label_base in HD_LABELS.items():
            # Nome leggibile: "tumor_core · T1"
            label_name = f'{label_base} · {seq_type}'
            insert_computed_structure(
                conn, session_pk, seq_pk,
                label=label_name,
                label_code=label_code,
                mask_path=str(seg_path),
                model_name='HD-GLIO-AUTO',
                reference_space='native',
            )

    # ── RANO response come clinical event ─────────────────────────
    if rano:
        insert_clinical_event(
            conn, subject_pk, session_pk,
            event_type='response_assessment',
            days=days,
            rano_response=rano,
        )


def run(
    conn: sqlite3.Connection,
    verbose: bool = False,
    limit: int | None = None,
    subjects: set[str] | None = None,
) -> None:
    if not IMAGING_ROOT.exists():
        raise FileNotFoundError(f"Imaging/ non trovata: {IMAGING_ROOT}")

    demo    = load_demographics()
    ratings = load_ratings()

    patient_dirs = sorted(d for d in IMAGING_ROOT.iterdir() if d.is_dir())
    if subjects:
        patient_dirs = [d for d in patient_dirs if d.name in subjects]
    if limit is not None:
        patient_dirs = patient_dirs[:limit]
    print(f"  [lumiere] {len(patient_dirs)} pazienti in Imaging/")

    skipped = 0
    for pat_dir in tqdm(patient_dirs, desc="  LUMIERE", unit="pt"):
        pid = pat_dir.name
        row = demo.get(pid, {})

        os_weeks = safe_float(row.get('Survival time (weeks)', ''))
        os_days  = int(os_weeks * 7) if os_weeks else None

        subject_pk = get_or_create_subject(
            conn, pid, 'lumiere',
            sex=norm_sex(row.get('Sex', '')),
            age_at_diagnosis=safe_float(row.get('Age at surgery (years)', '')),
            diagnosis='Glioblastoma',
            idh_status=_norm_idh(row.get('IDH (WT: wild type)', '')),
            mgmt_status=_norm_mgmt(row.get('MGMT qualitative', '')),
            os_days=os_days,
        )

        insert_clinical_event(conn, subject_pk, None, 'diagnosis', 0)

        pat_ratings = ratings.get(pid, {})

        for week_dir in sorted(d for d in pat_dir.iterdir() if d.is_dir()):
            rating = pat_ratings.get(week_dir.name, '')
            try:
                _import_session(conn, subject_pk, week_dir, rating)
            except Exception as exc:
                skipped += 1
                if verbose:
                    print(f"    SKIP {week_dir}: {exc}")

    conn.commit()
    if skipped:
        print(f"  [lumiere] {skipped} sessioni saltate per errori.")
    print("  [lumiere] Import completato.")
