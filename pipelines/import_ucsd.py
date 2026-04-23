"""Importa il dataset UCSD-PTGBM in GlioTwin."""

from __future__ import annotations
import re
import sqlite3
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from ._utils import (
    get_or_create_subject, get_or_create_session, get_or_create_sequence,
    insert_computed_structure, insert_clinical_event,
    norm_sex, safe_int, safe_float, safe_str, read_nifti_header, is_partial,
)

UCSD_ROOT    = Path("/mnt/dati/UCSD/UCSD")
IMG_ROOT     = UCSD_ROOT / "UCSD-PTGBM"
CLINICAL_XLS = UCSD_ROOT / "Dati" / "UCSD_PTGBM-clinical-information_v3_2026-12-Mar.xlsx"

# Sequenze: nome file (senza prefisso sessione) → (sequence_type, contrast_agent)
# DWI_vendor e ADC_vendor preferiti rispetto alle varianti b4000;
# RSI_Cell (cellulare) preferito come sequenza RSI primaria.
# Tutti i tipi sono nel CHECK del DB; uno per tipo → nessun conflitto UNIQUE.
SEQ_FILES = {
    'T1pre.nii.gz':      ('T1',   0),
    'T1post.nii.gz':     ('T1ce', 1),
    'T2.nii.gz':         ('T2',   0),
    'FLAIR.nii.gz':      ('FLAIR',0),
    'DWI_vendor.nii.gz': ('DWI',  0),
    'ADC_vendor.nii.gz': ('ADC',  0),
    'CBF_svd.nii.gz':    ('CBF',  0),
    'CBV_LC.nii.gz':     ('CBV',  0),
    'MTT_svd.nii.gz':    ('MTT',  0),
    'DSC_raw.nii.gz':    ('DSC',  0),
    'RSI_Cell.nii.gz':   ('RSI',  0),
    'SWAN.nii.gz':       ('SWAN', 0),
}

# BraTS multi-label: label_code → label_name
# Questo dataset usa convenzione BraTS 2023 (label 3 = ET, non 4).
# Label 4 = cavità di resezione (aggiunto per dataset post-treated, non standard BraTS).
BRATS_LABELS = [
    (1, 'necrotic_core'),
    (2, 'edema'),
    (3, 'enhancing_tumor'),
    (4, 'resection_cavity'),
]

# Segmentazioni cellulari RSI-derived (binarie)
CELLULAR_SEGS = [
    ('enhancing_cellular_tumor_seg.nii.gz',     'enhancing_cellular_tumor'),
    ('non_enhancing_cellular_tumor_seg.nii.gz', 'non_enhancing_cellular_tumor'),
    ('total_cellular_tumor_seg.nii.gz',          'total_cellular_tumor'),
]


def _parse_folder(name: str) -> tuple[str, int] | None:
    """'UCSD-PTGBM-0176_02' → ('UCSD-PTGBM-0176', 2)"""
    m = re.match(r'^(UCSD-PTGBM-\d+)_(\d+)$', name)
    return (m.group(1), int(m.group(2))) if m else None


def _norm_ucsd_idh(val) -> str:
    v = str(val or '').strip().lower()
    if 'wild' in v:
        return 'wildtype'
    if 'mut' in v:
        return 'mutated'
    return 'unknown'


def _norm_ucsd_mgmt(val) -> str:
    v = str(val or '').strip().lower()
    if v.startswith('unmethyl') or v == 'negative':
        return 'unmethylated'
    if v.startswith('methyl') or v == 'positive':
        return 'methylated'
    return 'unknown'


def load_clinical() -> dict[str, pd.Series]:
    """Chiave = ID sessione (es. 'UCSD-PTGBM-0001_01')."""
    df = pd.read_excel(CLINICAL_XLS, sheet_name='Cinical info')
    return {str(r['ID']).strip(): r for _, r in df.iterrows()}


def _insert_events(conn: sqlite3.Connection, subject_pk: int, row: pd.Series) -> None:
    """
    I giorni UCSD sono relativi alla data di acquisizione (scan = giorno 0).
    Valori negativi = l'evento precede la scansione.
    """
    def ev(etype, col, agent=None):
        d = safe_int(row.get(col))
        if d is not None:
            insert_clinical_event(conn, subject_pk, None, etype, d, treatment_agent=agent)

    ev('diagnosis',          'Days from Acquisition to Date of initial surgery, treatment or diagnosis ')
    ev('surgery',            'Days from Acquisition to Date of last surgery prior to scan if different')
    ev('radiotherapy_start', 'Days from Acquistion to Date of first radiation ')
    ev('radiotherapy_end',   'Days from Acquistion to Date of last radiation prior to scan')
    ev('chemotherapy_start', 'Days from Acquisition to Date of 1st chemo start ',
       agent=str(row.get('1st Chemo type') or '').strip() or None)
    death = safe_int(row.get('Days from Acquisition to Date of death'))
    if death is not None:
        insert_clinical_event(conn, subject_pk, None, 'death', death)


def run(
    conn: sqlite3.Connection,
    verbose: bool = False,
    limit: int | None = None,
    subjects: set[str] | None = None,
) -> None:
    if not IMG_ROOT.exists():
        raise FileNotFoundError(f"UCSD imaging root non trovato: {IMG_ROOT}")

    clin = load_clinical()
    session_dirs = sorted(
        d for d in IMG_ROOT.iterdir()
        if d.is_dir() and _parse_folder(d.name)
    )

    grouped_dirs: dict[str, list[Path]] = {}
    for ses_dir in session_dirs:
        parsed = _parse_folder(ses_dir.name)
        if not parsed:
            continue
        subject_id, _ = parsed
        grouped_dirs.setdefault(subject_id, []).append(ses_dir)

    subject_ids = sorted(grouped_dirs.keys())
    if subjects:
        subject_ids = [subject_id for subject_id in subject_ids if subject_id in subjects]
    if limit is not None:
        subject_ids = subject_ids[:limit]

    selected_session_dirs = [
        ses_dir
        for subject_id in subject_ids
        for ses_dir in grouped_dirs.get(subject_id, [])
    ]
    print(
        f"  [ucsd] {len(subject_ids)} pazienti, "
        f"{len(selected_session_dirs)} cartelle sessione selezionate"
    )

    subject_pks: dict[str, int] = {}   # subject_id → pk
    skipped_files = 0

    for ses_dir in tqdm(selected_session_dirs, desc="  UCSD-PTGBM", unit="ses"):
        parsed = _parse_folder(ses_dir.name)
        if not parsed:
            continue
        subject_id, ses_num = parsed
        row = clin.get(ses_dir.name)

        # ── Soggetto (creato una sola volta per paziente) ──────────
        if subject_id not in subject_pks:
            kw: dict = dict(diagnosis='Glioblastoma')
            if row is not None:
                os_d  = safe_int(row.get('Overall survival'))
                death = safe_int(row.get('Days from Acquisition to Date of death'))
                kw.update(
                    sex=norm_sex(row.get('Sex at birth', '')),
                    age_at_diagnosis=safe_float(row.get("Patient's Age")),
                    diagnosis=str(
                        safe_str(row.get('Non WHO 2021 Diagnosis'))
                        or safe_str(row.get('Primary Diagnosis'))
                        or 'Glioblastoma'
                    ),
                    idh_status=_norm_ucsd_idh(row.get('IDH')),
                    mgmt_status=_norm_ucsd_mgmt(row.get('MGMT')),
                    os_days=os_d,
                    vital_status='deceased' if death is not None else None,
                )
            subject_pks[subject_id] = get_or_create_subject(
                conn, subject_id, 'ucsd_ptgbm', **kw
            )
            if row is not None:
                _insert_events(conn, subject_pks[subject_id], row)

        subject_pk = subject_pks[subject_id]

        # Timepoint: _01 = prima scansione post-trattamento, _02+ = follow-up
        tp_type = 'post_op' if ses_num == 1 else 'follow_up'

        session_pk = get_or_create_session(
            conn, subject_pk, ses_dir.name,
            days_from_baseline=None,   # giorni relativi alla scansione, non a una baseline comune
            timepoint_type=tp_type,
            raw_dir=str(ses_dir),
        )

        # I file UCSD hanno il prefisso della sessione: UCSD-PTGBM-0176_02_T1pre.nii.gz
        pfx = ses_dir.name + '_'

        # ── Sequenze ──────────────────────────────────────────────
        seq_pks: dict[str, int] = {}
        for fname, (seq_type, contrast) in SEQ_FILES.items():
            fpath = ses_dir / (pfx + fname)
            if not fpath.exists() or is_partial(fpath):
                skipped_files += 1
                continue
            hdr = read_nifti_header(fpath)
            pk  = get_or_create_sequence(
                conn, session_pk, seq_type,
                contrast_agent=contrast,
                raw_path=str(fpath),
                header=hdr,
            )
            seq_pks[seq_type] = pk

        t1ce_pk = seq_pks.get('T1ce')
        rsi_pk  = seq_pks.get('RSI')

        # ── BraTS tumor segmentation (multi-label) ─────────────────
        # Label 1=necrotic_core, 2=edema, 4=enhancing_tumor (standard BraTS)
        brats = ses_dir / (pfx + 'BraTS_tumor_seg.nii.gz')
        if brats.exists() and not is_partial(brats):
            for label_code, label_name in BRATS_LABELS:
                insert_computed_structure(
                    conn, session_pk, t1ce_pk,
                    label=label_name, label_code=label_code,
                    mask_path=str(brats),
                    model_name='BraTS_segmentation',
                    reference_space='native',
                )

        # ── Segmentazioni cellulari RSI-derived (binarie) ──────────
        for fname, label_name in CELLULAR_SEGS:
            fpath = ses_dir / (pfx + fname)
            if fpath.exists() and not is_partial(fpath):
                insert_computed_structure(
                    conn, session_pk, rsi_pk,
                    label=label_name, label_code=None,
                    mask_path=str(fpath),
                    model_name='RSI_cellularity',
                    reference_space='native',
                )

    conn.commit()
    if skipped_files and verbose:
        print(f"  [ucsd] {skipped_files} file sequenza mancanti o parziali (saltati).")
    print("  [ucsd] Import completato.")
