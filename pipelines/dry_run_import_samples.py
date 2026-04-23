#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import nibabel as nib
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.import_lumiere import (
    DEMO_CSV,
    RATING_CSV,
    SEQ_FILES as LUMIERE_SEQ_FILES,
    SEG_NATIVE as LUMIERE_SEG_NATIVE,
    _is_native_lumiere_seg,
)
from pipelines.import_mu import (
    CLINICAL_XLS as MU_CLINICAL_XLS,
    MASK_LABELS as MU_MASK_LABELS,
    SEQ_PATTERNS as MU_SEQ_PATTERNS,
    _is_native_mu_mask,
    _is_native_mu_volume,
)
from pipelines._utils import is_partial


def _header(path: Path) -> dict:
    img = nib.load(str(path))
    return {
        "shape": list(img.shape[:3]),
        "zooms": [round(float(z), 3) for z in img.header.get_zooms()[:3]],
    }


def _mask_stats(path: Path) -> dict:
    img = nib.load(str(path))
    data = np.asanyarray(img.dataobj)
    values, counts = np.unique(data, return_counts=True)
    labels = [
        {"value": int(value), "voxels": int(count)}
        for value, count in zip(values, counts)
        if int(value) != 0
    ]
    return {
        "shape": list(img.shape[:3]),
        "zooms": [round(float(z), 3) for z in img.header.get_zooms()[:3]],
        "labels": labels,
    }


def _load_lumiere_demo(patient_id: str) -> dict | None:
    with open(DEMO_CSV, newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if row.get("Patient") == patient_id:
                return row
    return None


def _load_lumiere_ratings(patient_id: str) -> list[dict]:
    rows = []
    with open(RATING_CSV, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rating_col = next((key for key in reader.fieldnames or [] if "Rating (" in key), None)
        for row in reader:
            if row.get("Patient") != patient_id:
                continue
            rows.append({
                "Date": row.get("Date"),
                "Rating": row.get(rating_col, "") if rating_col else "",
            })
    return rows


def dry_run_lumiere(timepoint_dir: Path) -> dict:
    patient_id = timepoint_dir.parent.name
    session_label = timepoint_dir.name

    sequences = {}
    for filename, (sequence_type, contrast_agent) in LUMIERE_SEQ_FILES.items():
        path = timepoint_dir / filename
        if not path.exists() or is_partial(path):
            continue
        sequences[sequence_type] = {
            "source_file": filename,
            "raw_path": str(path),
            "contrast_agent": contrast_agent,
            **_header(path),
        }

    structures = []
    native_root = timepoint_dir / "HD-GLIO-AUTO-segmentation" / "native"
    for filename, sequence_type in LUMIERE_SEG_NATIVE.items():
        seg_path = native_root / filename
        if not seg_path.exists() or is_partial(seg_path) or not _is_native_lumiere_seg(seg_path):
            continue
        structures.append({
            "kind": "computed_structure",
            "source": "HD-GLIO-AUTO native origspace",
            "sequence_type": sequence_type,
            "mask_path": str(seg_path),
            "reference_space": "native",
            **_mask_stats(seg_path),
        })

    demo = _load_lumiere_demo(patient_id)
    ratings = _load_lumiere_ratings(patient_id)
    session_ratings = [row for row in ratings if row.get("Date") == session_label]

    return {
        "dataset": "lumiere",
        "subject": {
            "subject_id": patient_id,
            "dataset": "lumiere",
            "demographics": demo,
        },
        "session": {
            "session_label": session_label,
            "raw_dir": str(timepoint_dir),
            "ratings_for_session": session_ratings,
            "ratings_preview": ratings[:8],
        },
        "native_only": True,
        "sequences_to_import": sequences,
        "structures_to_import": structures,
        "skips": {
            "registered_segmentation_root": str(timepoint_dir / "HD-GLIO-AUTO-segmentation" / "registered"),
            "reason": "Registered/Montreal/MNI paths are excluded by design.",
        },
    }


def dry_run_mu(timepoint_dir: Path) -> dict:
    patient_id = timepoint_dir.parent.name
    session_label = timepoint_dir.name

    sequences = {}
    for suffix, sequence_type, contrast_agent in MU_SEQ_PATTERNS:
        matches = [
            path for path in timepoint_dir.iterdir()
            if path.name.endswith(suffix) and not is_partial(path) and _is_native_mu_volume(path)
        ]
        if not matches:
            continue
        path = matches[0]
        sequences[sequence_type] = {
            "source_file": path.name,
            "raw_path": str(path),
            "contrast_agent": contrast_agent,
            **_header(path),
        }

    structures = []
    masks = [
        path for path in timepoint_dir.iterdir()
        if path.name.lower().endswith("_tumormask.nii.gz")
        and not is_partial(path)
        and _is_native_mu_mask(path)
    ]
    if masks:
        mask_path = masks[0]
        mask_info = _mask_stats(mask_path)
        structures.append({
            "kind": "radiological_structure",
            "source": "MU native tumorMask",
            "mask_path": str(mask_path),
            "reference_space": "native",
            "label_definitions": MU_MASK_LABELS,
            **mask_info,
        })

    clinical_df = pd.read_excel(MU_CLINICAL_XLS, sheet_name="MU Glioma Post")
    clinical_row = clinical_df[clinical_df["Patient_ID"].astype(str).str.strip() == patient_id].head(1)
    clinical = clinical_row.to_dict(orient="records")[0] if not clinical_row.empty else None

    return {
        "dataset": "mu_glioma_post",
        "subject": {
            "subject_id": patient_id,
            "dataset": "mu_glioma_post",
            "clinical_row": clinical,
        },
        "session": {
            "session_label": session_label,
            "raw_dir": str(timepoint_dir),
        },
        "native_only": True,
        "sequences_to_import": sequences,
        "structures_to_import": structures,
        "skips": {
            "reason": "Any registered/Montreal/MNI or partial files are excluded by design.",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run import preview for one LUMIERE case and one MU case.")
    parser.add_argument(
        "--lumiere",
        type=Path,
        default=Path("/mnt/dati/lumiere/Imaging/Patient-001/week-000-1"),
        help="Path to one complete LUMIERE timepoint directory.",
    )
    parser.add_argument(
        "--mu",
        type=Path,
        default=Path("/mnt/dati/MU-Glioma-Post/PKG - MU-Glioma-Post/MU-Glioma-Post/PatientID_0254/Timepoint_1"),
        help="Path to one complete MU-Glioma-Post timepoint directory.",
    )
    args = parser.parse_args()

    result = {
        "lumiere": dry_run_lumiere(args.lumiere),
        "mu_glioma_post": dry_run_mu(args.mu),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
