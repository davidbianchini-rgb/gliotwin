"""
run_rhglioseg.py — Runs rh-glioseg nnUNet segmentation on all sessions that
have FeTS-prepared NIfTIs (already in SRI24, skull-stripped).

Usage (standalone):
    /mnt/dati/irst/conda/envs/nnunet/bin/python pipelines/run_rhglioseg.py [--session-id N] [--force]

Environment variables (auto-set by this script):
    nnUNet_results  → /mnt/dati/irst/nnunet_models
    nnUNet_raw      → /tmp (unused for inference)
    nnUNet_preprocessed → /tmp (unused for inference)
"""

import os
import sys
import shutil
import tempfile
import argparse
import subprocess
import logging
from pathlib import Path
from collections import defaultdict

import nibabel as nib
import numpy as np

# Allow running from gliotwin root
sys.path.insert(0, str(Path(__file__).parent.parent))
from app.db import db, rows_as_dicts

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('rhglioseg')

# ─── configuration ────────────────────────────────────────────────────────────
NNUNET_PYTHON  = "/mnt/dati/irst/conda/envs/nnunet/bin/python"
NNUNET_PREDICT = "/mnt/dati/irst/conda/envs/nnunet/bin/nnUNetv2_predict"
NNUNET_RESULTS = "/mnt/dati/irst/nnunet_models"
DATASET_ID     = "Dataset016_RH-GlioSeg_v3"
MODEL_NAME     = "rh-glioseg-v3"
OUTPUT_BASE    = Path("/home/irst/gliotwin/processed/rhglioseg")

# rh-glioseg label map: int value → (gliotwin label, label_code)
LABEL_MAP = {
    1: ("necrotic_core",      1),
    2: ("edema",              2),
    3: ("enhancing_tumor",    3),
    4: ("resection_cavity",   4),
}

# FeTS sequence_type → nnUNet channel suffix _000X
SEQ_CHANNEL = {
    "FLAIR": "0000",
    "T1":    "0001",
    "T1ce":  "0002",
    "T2":    "0003",
}


def _get_pending_sessions(session_ids=None, force=False):
    """Return list of (subject_id, session_id, session_label, {seq_type: path})."""
    with db() as conn:
        rows = conn.execute("""
            SELECT sub.subject_id, se.id AS session_id, se.session_label,
                   seq.sequence_type, seq.processed_path
            FROM sequences seq
            JOIN sessions se  ON se.id  = seq.session_id
            JOIN subjects sub ON sub.id = se.subject_id
            WHERE seq.sequence_type IN ('FLAIR','T1','T1ce','T2')
              AND seq.processed_path IS NOT NULL
              AND seq.shape_x = 240
        """).fetchall()

        if session_ids:
            id_set = set(session_ids)
            rows = [r for r in rows if r['session_id'] in id_set]

        sessions = defaultdict(dict)
        for r in rows:
            key = (r['subject_id'], r['session_id'], r['session_label'])
            sessions[key][r['sequence_type']] = r['processed_path']

        complete = {k: v for k, v in sessions.items()
                    if all(t in v for t in SEQ_CHANNEL)}

        if not force:
            done = {r['session_id'] for r in conn.execute(
                "SELECT DISTINCT session_id FROM computed_structures WHERE model_name = ?",
                (MODEL_NAME,)
            ).fetchall()}
            complete = {k: v for k, v in complete.items() if k[1] not in done}

    return [(subj, sid, label, seqs) for (subj, sid, label), seqs in complete.items()]


def _voxel_volume_ml(nii_path):
    """Return voxel volume in mL for a NIfTI file."""
    img = nib.load(nii_path)
    vox = np.abs(np.linalg.det(img.header.get_sform()[:3, :3]))
    return vox / 1000.0


def _run_nnunet(session_key, seqs, tmp_dir, out_dir):
    """Create input folder, run nnUNetv2_predict, return output .nii.gz path."""
    subj, sid, label = session_key
    inp_dir = Path(tmp_dir) / "input"
    inp_dir.mkdir()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    case_id = f"{subj}_{label}"
    for seq_type, channel in SEQ_CHANNEL.items():
        src = seqs[seq_type]
        if not os.path.exists(src):
            raise FileNotFoundError(f"Missing {seq_type}: {src}")
        dst = inp_dir / f"{case_id}_{channel}.nii.gz"
        os.symlink(src, dst)

    env = os.environ.copy()
    env.update({
        "nnUNet_results":       NNUNET_RESULTS,
        "nnUNet_raw":           tmp_dir,
        "nnUNet_preprocessed":  tmp_dir,
        "CUDA_VISIBLE_DEVICES": "0",
    })

    cmd = [
        NNUNET_PREDICT,
        "-d", DATASET_ID,
        "-i", str(inp_dir),
        "-o", str(out_dir),
        "-f", "0", "1", "2", "3", "4",
        "-tr", "nnUNetTrainer",
        "-c", "3d_fullres",
        "-p", "nnUNetPlans",
        "--verbose",
    ]
    log.info(f"  Running nnUNet predict for {case_id}")
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=1800)
    if result.returncode != 0:
        raise RuntimeError(f"nnUNetv2_predict failed:\n{result.stderr[-2000:]}")

    out_files = list(out_dir.glob(f"{case_id}.nii.gz"))
    if not out_files:
        raise RuntimeError(f"No output file found in {out_dir}")
    return out_files[0]


def _save_masks(seg_path, subj, label, session_id):
    """Split segmentation into per-label masks, save to OUTPUT_BASE, return DB rows."""
    img = nib.load(seg_path)
    data = img.get_fdata().astype(np.int16)
    vox_ml = _voxel_volume_ml(seg_path)
    out_dir = OUTPUT_BASE / subj / label
    out_dir.mkdir(parents=True, exist_ok=True)

    # copy full segmentation
    shutil.copy2(seg_path, out_dir / "segmentation.nii.gz")

    db_rows = []
    for val, (gliotwin_label, label_code) in LABEL_MAP.items():
        mask = (data == val).astype(np.int16)
        n_voxels = int(mask.sum())
        volume_ml = round(n_voxels * vox_ml, 3)
        mask_path = str(out_dir / f"{gliotwin_label}.nii.gz")
        nib.save(nib.Nifti1Image(mask, img.affine, img.header), mask_path)
        db_rows.append({
            "session_id":      session_id,
            "label":           gliotwin_label,
            "label_code":      1,         # each file is already a binary mask (0/1)
            "mask_path":       mask_path,
            "reference_space": "native",
            "model_name":      MODEL_NAME,
            "model_version":   "v3",
            "volume_ml":       volume_ml,
        })
        log.info(f"    {gliotwin_label}: {n_voxels} voxels ({volume_ml:.1f} mL) → {mask_path}")

    return db_rows


def _save_to_db(db_rows):
    with db() as conn:
        for r in db_rows:
            conn.execute("""
                INSERT INTO computed_structures
                    (session_id, label, label_code, mask_path, reference_space,
                     model_name, model_version, volume_ml)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (r['session_id'], r['label'], r['label_code'], r['mask_path'],
                  r['reference_space'], r['model_name'], r['model_version'], r['volume_ml']))
        conn.commit()


def run_all(session_ids=None, force=False, progress_callback=None):
    """
    Main entry point. progress_callback(current, total, msg) is called after each session.
    session_ids: list of session IDs to process, or None for all pending.
    Returns dict with counts.
    """
    pending = _get_pending_sessions(session_ids=session_ids, force=force)
    total = len(pending)
    log.info(f"Sessions to process: {total}")

    if total == 0:
        return {"processed": 0, "failed": 0, "skipped": 0, "total": 0}

    processed, failed = 0, 0

    for i, (subj, sid, label, seqs) in enumerate(pending):
        log.info(f"[{i+1}/{total}] {subj} / {label}")
        try:
            with tempfile.TemporaryDirectory(prefix="rhglio_") as tmp:
                out_dir = Path(tmp) / "output"
                seg_path = _run_nnunet((subj, sid, label), seqs, tmp, out_dir)
                db_rows  = _save_masks(seg_path, subj, label, sid)
                _save_to_db(db_rows)
            processed += 1
            log.info(f"  ✓ Done")
        except Exception as e:
            failed += 1
            log.error(f"  ✗ Failed: {e}")

        if progress_callback:
            progress_callback(i + 1, total, f"{subj}/{label}")

    return {"processed": processed, "failed": failed, "skipped": 0, "total": total}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-id", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    result = run_all(session_id=args.session_id, force=args.force)
    print(f"\nRisultato: {result}")
