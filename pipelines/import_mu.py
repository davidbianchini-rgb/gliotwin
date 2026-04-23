"""Importa il dataset MU-Glioma-Post in GlioTwin."""

from __future__ import annotations
import sqlite3
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from ._utils import (
    get_or_create_subject, get_or_create_session, get_or_create_sequence,
    insert_radiological_structure, insert_clinical_event,
    norm_sex, safe_int, safe_float, read_nifti_header, is_partial,
)

MU_ROOT      = Path("/mnt/dati/MU-Glioma-Post/PKG - MU-Glioma-Post")
DATA_ROOT    = MU_ROOT / "MU-Glioma-Post"
CLINICAL_XLS = MU_ROOT / "MU-Glioma-Post_DATI" / "MU-Glioma-Post_ClinicalData-July2025.xlsx"
VOLUMES_XLS  = MU_ROOT / "MU-Glioma-Post_DATI" / "MU-Glioma-Post_Segmentation_Volumes.xlsx"

# Suffissi file sequenze → (sequence_type, contrast_agent)
SEQ_PATTERNS = [
    ('_brain_t1n.nii.gz', 'T1',    0),
    ('_brain_t1c.nii.gz', 'T1ce',  1),
    ('_brain_t2w.nii.gz', 'T2',    0),
    ('_brain_t2f.nii.gz', 'FLAIR', 0),
]

# Label tumorMask → nome struttura
MASK_LABELS = {
    1: 'necrotic_core',
    2: 'edema',
    3: 'enhancing_tumor',
    4: 'resection_cavity',
}

# Colonne giorni MRI per timepoint (1-based)
_TP_DAYS_COLS = {
    1: 'Number of Days from Diagnosis to 1st MRI (Timepoint_1) ',
    2: 'Number of Days from Diagnosis to 2nd MRI (Timepoint_2) ',
    3: 'Number of Days from Diagnosis to 3rd MRI (Timepoint_3) ',
    4: 'Number of Days from Diagnosis to 4th MRI (Timepoint_4) ',
    5: 'Number of Days from Diagnosis to 5th MRI (Timepoint_5) ',
    6: 'Number of Days from Diagnosis to 6th MRI (Timepoint_6) ',
}

# MU IDH coding: 0=wildtype, 1=mutated, 2=unknown
_IDH_MAP  = {0: 'wildtype', 1: 'mutated', 2: 'unknown'}
# MU MGMT coding: 0=unmethylated, 1=methylated, 2/3/4=unknown
_MGMT_MAP = {0: 'unmethylated', 1: 'methylated'}


def _is_native_mu_volume(path: Path) -> bool:
    path_lower = str(path).lower()
    return (
        path.name.endswith('.nii.gz')
        and '_brain_' in path.name.lower()
        and 'registered' not in path_lower
        and 'montreal' not in path_lower
        and 'mni' not in path_lower
    )


def _is_native_mu_mask(path: Path) -> bool:
    path_lower = str(path).lower()
    return (
        path.name.lower().endswith('_tumormask.nii.gz')
        and 'registered' not in path_lower
        and 'montreal' not in path_lower
        and 'mni' not in path_lower
    )


# ── Caricamento dati clinici ─────────────────────────────────────────────────

def load_clinical() -> dict[str, pd.Series]:
    df = pd.read_excel(CLINICAL_XLS, sheet_name='MU Glioma Post')
    return {str(r['Patient_ID']).strip(): r for _, r in df.iterrows()}


def load_volumes() -> dict[str, list[dict[int, float]]]:
    """
    Ritorna {patient_id: [{label_code: volume_ml}, ...]}
    L'indice della lista corrisponde al timepoint (0-based → Timepoint_1, Timepoint_2, …).
    """
    sheet_map = {
        'Necrotic Tumor Core (Label1)':   1,
        'Tumor Infiltration and Edema':    2,
        'Enhancing Tumor Core (Label3)':   3,
        'Resection Cavity (Label4)':       4,
    }
    # struttura: {pid: {tp_idx: {label: vol_ml}}}
    data: dict[str, dict[int, dict[int, float]]] = {}

    for sheet, label_code in sheet_map.items():
        df = pd.read_excel(VOLUMES_XLS, sheet_name=sheet)
        for pid, group in df.groupby('Patient ID'):
            pid = str(pid).strip()
            if pid not in data:
                data[pid] = {}
            for tp_idx, (_, row) in enumerate(group.iterrows()):
                vol_mm3 = safe_float(row.get('Volume (mm^3)'))
                vol_ml  = round(vol_mm3 / 1000, 3) if vol_mm3 else None
                if vol_ml is not None:
                    data[pid].setdefault(tp_idx, {})[label_code] = vol_ml

    # Converti in lista indicizzata
    result: dict[str, list[dict[int, float]]] = {}
    for pid, tp_dict in data.items():
        max_tp = max(tp_dict.keys()) + 1
        result[pid] = [{} for _ in range(max_tp)]
        for tp_idx, labels in tp_dict.items():
            result[pid][tp_idx] = labels
    return result


# ── Helpers clinici ──────────────────────────────────────────────────────────

def _idh(row: pd.Series) -> str:
    i1 = int(row.get('IDH1 mutation', 0) or 0)
    i2 = int(row.get('IDH2 mutation', 0) or 0)
    if i1 == 1 or i2 == 1:
        return 'mutated'
    if i1 == 2 or i2 == 2:
        return 'unknown'
    return 'wildtype'


def _mgmt(row: pd.Series) -> str:
    return _MGMT_MAP.get(int(row.get('MGMT methylation', 4) or 4), 'unknown')


def _timepoint_type(days: int | None, rt_start: int | None, rt_end: int | None,
                    tp_num: int) -> str:
    """Classifica il timepoint rispetto alla radioterapia."""
    if days is None:
        return 'post_op' if tp_num == 1 else 'follow_up'
    if rt_start is not None and days < rt_start:
        return 'post_op'                        # post-chirurgia, pre-RT
    if rt_start is not None and rt_end is not None and rt_start <= days <= rt_end:
        return 'during_treatment'               # durante RT
    if rt_end is not None and days > rt_end:
        return 'follow_up'                      # dopo RT
    return 'post_op' if tp_num == 1 else 'follow_up'


def _insert_events(conn: sqlite3.Connection, subject_pk: int, row: pd.Series) -> None:
    insert_clinical_event(conn, subject_pk, None, 'diagnosis', 0)

    def ev(etype, col, agent=None):
        d = safe_int(row.get(col))
        if d is not None:
            insert_clinical_event(conn, subject_pk, None, etype, d,
                                  treatment_agent=agent)

    ev('surgery',            'Number of days from Diagnosis to First surgery or procedure ')
    ev('radiotherapy_start', 'Number of days from Diagnosis to Radiation Therapy Start date')
    ev('radiotherapy_end',   'Number of days from Diagnosis to Radiation Therapy end date')
    ev('chemotherapy_start', ' Number of days from Diagnosis to Initial Chemo Therapy Start date',
       agent=str(row.get('Name of Initial Chemo Therapy') or '').strip() or None)
    ev('chemotherapy_end',   ' Number of days from Diagnosis to Initial Chemo Therapy end date')
    ev('progression',        'Number of days from Diagnosis to date of First Progression')
    if int(row.get('Overall Survival (Death)', 0) or 0) == 1:
        ev('death', 'Number of days from Diagnosis to death (Days)')


# ── Import principale ────────────────────────────────────────────────────────

def run(
    conn: sqlite3.Connection,
    verbose: bool = False,
    limit: int | None = None,
    subjects: set[str] | None = None,
) -> None:
    clin    = load_clinical()
    volumes = load_volumes()

    # Raccoglie tutti i patient_id: unione di clinical + cartelle su disco
    pids_clinical = set(clin.keys())
    pids_disk     = {d.name for d in DATA_ROOT.iterdir() if d.is_dir()} \
                    if DATA_ROOT.exists() else set()
    all_pids = sorted(pids_clinical | pids_disk)
    if subjects:
        all_pids = [pid for pid in all_pids if pid in subjects]
    if limit is not None:
        all_pids = all_pids[:limit]
    print(f"  [mu] {len(all_pids)} pazienti (clinical: {len(pids_clinical)}, "
          f"su disco: {len(pids_disk)})")

    for pid in tqdm(all_pids, desc="  MU-Glioma-Post", unit="pt"):
        row = clin.get(pid)

        # ── Soggetto ──────────────────────────────────────────────
        kw: dict = dict(diagnosis='GBM')
        if row is not None:
            death = int(row.get('Overall Survival (Death)', 0) or 0) == 1
            os_d  = safe_int(row.get('Number of days from Diagnosis to death (Days)'))
            kw.update(
                sex=norm_sex(row.get('Sex at Birth', '')),
                age_at_diagnosis=safe_float(row.get('Age at diagnosis')),
                diagnosis=str(row.get('Primary Diagnosis') or 'GBM'),
                idh_status=_idh(row),
                mgmt_status=_mgmt(row),
                os_days=os_d,
                vital_status='deceased' if death else None,
            )
        subject_pk = get_or_create_subject(conn, pid, 'mu_glioma_post', **kw)
        if row is not None:
            _insert_events(conn, subject_pk, row)

        # RT timing per classificazione timepoint
        rt_start = safe_int(row.get('Number of days from Diagnosis to Radiation Therapy Start date')) \
                   if row is not None else None
        rt_end   = safe_int(row.get('Number of days from Diagnosis to Radiation Therapy end date')) \
                   if row is not None else None

        # Volumi dal foglio Excel (indicizzati per timepoint)
        pat_vols = volumes.get(pid, [])

        # Numero massimo di timepoint: da clinical o da cartelle su disco
        max_tp = 0
        if row is not None:
            for n, col in _TP_DAYS_COLS.items():
                if safe_int(row.get(col)) is not None:
                    max_tp = max(max_tp, n)
        pat_dir = DATA_ROOT / pid
        tp_dirs: dict[int, Path] = {}
        if pat_dir.exists():
            for d in pat_dir.iterdir():
                if d.is_dir():
                    parts = d.name.rsplit('_', 1)
                    if parts[-1].isdigit():
                        n = int(parts[-1])
                        tp_dirs[n] = d
                        max_tp = max(max_tp, n)

        if max_tp == 0:
            continue   # nessun dato per questo paziente

        # ── Sessioni ──────────────────────────────────────────────
        for tp_num in range(1, max_tp + 1):
            days = safe_int(row.get(_TP_DAYS_COLS[tp_num])) \
                   if row is not None and tp_num in _TP_DAYS_COLS else None
            tp_type   = _timepoint_type(days, rt_start, rt_end, tp_num)
            tp_label  = f'Timepoint_{tp_num}'
            tp_dir    = tp_dirs.get(tp_num)

            session_pk = get_or_create_session(
                conn, subject_pk, tp_label,
                days_from_baseline=days,
                timepoint_type=tp_type,
                raw_dir=str(tp_dir) if tp_dir else None,
            )

            # ── Sequenze (solo se cartella presente) ──────────────
            seq_pks: dict[str, int] = {}
            if tp_dir and tp_dir.exists():
                for suffix, seq_type, contrast in SEQ_PATTERNS:
                    matches = [f for f in tp_dir.iterdir()
                               if f.name.endswith(suffix) and not is_partial(f) and _is_native_mu_volume(f)]
                    if not matches:
                        continue
                    hdr = read_nifti_header(matches[0])
                    pk  = get_or_create_sequence(
                        conn, session_pk, seq_type,
                        contrast_agent=contrast,
                        raw_path=str(matches[0]),
                        header=hdr,
                    )
                    seq_pks[seq_type] = pk

            # ── Strutture radiologiche (volumi da Excel, mask da disco) ─
            tp_idx   = tp_num - 1
            tp_vol   = pat_vols[tp_idx] if tp_idx < len(pat_vols) else {}
            seq_ref  = seq_pks.get('T1ce') or seq_pks.get('T1')

            # Trova il file mask su disco (se disponibile)
            mask_path = None
            if tp_dir and tp_dir.exists():
                masks = [f for f in tp_dir.iterdir()
                         if 'tumormask' in f.name.lower()
                         and f.suffix in ('.gz', '.nii')
                         and not is_partial(f)
                         and _is_native_mu_mask(f)]
                if masks:
                    mask_path = str(masks[0])

            # Crea una riga per ogni label presente nei volumi o nel file
            labels_present: set[int] = set(tp_vol.keys())
            if mask_path:
                # Aggiungi tutti i label definiti (verranno creati anche senza volume)
                labels_present |= set(MASK_LABELS.keys())

            for label_code in sorted(labels_present):
                label_name = MASK_LABELS.get(label_code, f'label_{label_code}')
                vol_ml     = tp_vol.get(label_code)
                insert_radiological_structure(
                    conn, session_pk, seq_ref,
                    label=label_name,
                    label_code=label_code,
                    mask_path=mask_path,        # None se immagini non ancora scaricate
                    annotator='expert',
                    volume_ml=vol_ml,
                    is_ground_truth=1,
                )

    conn.commit()
    print("  [mu] Import completato.")
