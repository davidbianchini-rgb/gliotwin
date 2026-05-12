"""Importa DICOM RTSTRUCT → maschere NIfTI binarie nella tabella radiological_structures.

Per ogni ROI dell'RTSTRUCT:
  1. Legge i contorni con pydicom (coordinate LPS in mm)
  2. Converte LPS → RAS, poi applica l'inverso dell'affine del NIfTI di riferimento
  3. Rasterizza il poligono planare 2D su ogni slice con un algoritmo scan-line
  4. Salva la maschera binaria con stesso affine/header dell'immagine di riferimento
  5. Inserisce o aggiorna la riga in radiological_structures

Immagine di riferimento: T1ce 1mm iso (canonical_1mm), fallback T1.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import nibabel as nib
import numpy as np
import pydicom

from app.db import db

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STRUCTURES_ROOT = PROJECT_ROOT / "processed" / "radiological_structures"


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _lps_to_ras(pts_lps: np.ndarray) -> np.ndarray:
    """Converte coordinate da DICOM Patient CS (LPS) a NIfTI/RAS."""
    pts_ras = pts_lps.copy()
    pts_ras[:, 0] *= -1  # L → R
    pts_ras[:, 1] *= -1  # P → A
    return pts_ras


def _polygon_mask_2d(polygon_xy: np.ndarray, shape: tuple[int, int]) -> np.ndarray:
    """Rasterizza un poligono 2D chiuso con algoritmo scan-line (even-odd rule).

    polygon_xy : array (N, 2) con (col, row) in coordinate di voxel float
    shape      : (nrows, ncols) dell'output
    Returns    : array bool 2D
    """
    rows, cols = shape
    mask = np.zeros((rows, cols), dtype=bool)
    if len(polygon_xy) < 3:
        return mask

    px = polygon_xy[:, 0].astype(float)  # col  (j-axis)
    py = polygon_xy[:, 1].astype(float)  # row  (i-axis)

    r_min = max(0, int(np.floor(py.min())))
    r_max = min(rows - 1, int(np.ceil(py.max())))
    n = len(px)

    for r in range(r_min, r_max + 1):
        crossings: list[float] = []
        for i in range(n):
            j = (i + 1) % n
            yi, yj = py[i], py[j]
            xi, xj = px[i], px[j]
            if (yi <= r < yj) or (yj <= r < yi):
                x_cross = xi + (r - yi) * (xj - xi) / (yj - yi)
                crossings.append(x_cross)
        crossings.sort()
        for k in range(0, len(crossings) - 1, 2):
            c0 = max(0, int(np.ceil(crossings[k])))
            c1 = min(cols - 1, int(np.floor(crossings[k + 1])))
            if c0 <= c1:
                mask[r, c0 : c1 + 1] = True
    return mask


# ---------------------------------------------------------------------------
# Core rasterization
# ---------------------------------------------------------------------------

def _rtstruct_to_masks(
    rtstruct_path: Path,
    ref_nifti_path: Path,
) -> dict[str, np.ndarray]:
    """Rasterizza tutti i ROI di un RTSTRUCT sulla griglia del NIfTI di riferimento.

    Returns {roi_name: mask_3d_uint8} con stessa shape del NIfTI.
    """
    ds = pydicom.dcmread(str(rtstruct_path))
    ref_img = nib.load(str(ref_nifti_path))
    ref_shape = ref_img.shape[:3]
    inv_affine = np.linalg.inv(ref_img.affine)

    roi_names: dict[int, str] = {}
    if hasattr(ds, "StructureSetROISequence"):
        for roi in ds.StructureSetROISequence:
            roi_names[int(roi.ROINumber)] = str(roi.ROIName)

    masks: dict[str, np.ndarray] = {}
    if not hasattr(ds, "ROIContourSequence"):
        return masks

    for roi_contour in ds.ROIContourSequence:
        roi_num = int(roi_contour.ReferencedROINumber)
        roi_name = roi_names.get(roi_num, f"ROI_{roi_num}")
        mask = np.zeros(ref_shape, dtype=np.uint8)

        if not hasattr(roi_contour, "ContourSequence"):
            masks[roi_name] = mask
            continue

        for contour in roi_contour.ContourSequence:
            if not hasattr(contour, "ContourData"):
                continue
            raw = np.array(contour.ContourData, dtype=float)
            if raw.size < 9:
                continue
            pts_lps = raw.reshape(-1, 3)

            # LPS (DICOM) → RAS (NIfTI), poi voxel
            pts_ras = _lps_to_ras(pts_lps)
            pts_homog = np.c_[pts_ras, np.ones(len(pts_ras))]
            pts_vox = (inv_affine @ pts_homog.T).T[:, :3]
            # pts_vox[:,0] = i (prima dim)
            # pts_vox[:,1] = j (seconda dim)
            # pts_vox[:,2] = k (terza dim, z)

            z_vals = pts_vox[:, 2]
            z_range = z_vals.max() - z_vals.min()
            if z_range > 1.5:
                log.warning("Contorno non planare per '%s': z_range=%.2f voxel — saltato", roi_name, z_range)
                continue
            z_idx = int(round(float(z_vals.mean())))
            if z_idx < 0 or z_idx >= ref_shape[2]:
                continue

            # polygon_xy[:,0] = col = j_vox, polygon_xy[:,1] = row = i_vox
            polygon_xy = np.c_[pts_vox[:, 1], pts_vox[:, 0]]
            slice_2d = _polygon_mask_2d(
                polygon_xy, (ref_shape[0], ref_shape[1])
            )
            mask[:, :, z_idx] |= slice_2d.astype(np.uint8)

        masks[roi_name] = mask

    return masks


# ---------------------------------------------------------------------------
# DB lookup
# ---------------------------------------------------------------------------

def _find_reference_nifti(
    conn,
    session_id: int,
    referenced_series_uid: str | None,
) -> tuple[Path | None, int | None]:
    """Cerca il NIfTI processed della serie referenziata dall'RTSTRUCT.

    Priorità: match diretto su source_series_uid → T1ce → T1.
    Returns (path, sequence_id).
    """
    if referenced_series_uid:
        row = conn.execute(
            "SELECT id, processed_path FROM sequences "
            "WHERE session_id = ? AND source_series_uid = ?",
            (session_id, referenced_series_uid),
        ).fetchone()
        if row and row["processed_path"] and Path(row["processed_path"]).exists():
            return Path(row["processed_path"]), int(row["id"])

    for seq_type in ("T1ce", "T1"):
        row = conn.execute(
            "SELECT id, processed_path FROM sequences "
            "WHERE session_id = ? AND sequence_type = ?",
            (session_id, seq_type),
        ).fetchone()
        if row and row["processed_path"] and Path(row["processed_path"]).exists():
            return Path(row["processed_path"]), int(row["id"])

    return None, None


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _save_mask_nifti(mask: np.ndarray, ref_path: Path, out_path: Path) -> None:
    ref_img = nib.load(str(ref_path))
    out_img = nib.Nifti1Image(mask.astype(np.uint8), ref_img.affine, ref_img.header)
    out_img.header.set_data_dtype(np.uint8)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    nib.save(out_img, str(out_path))


def _volume_ml(mask: np.ndarray, ref_path: Path) -> float:
    zooms = nib.load(str(ref_path)).header.get_zooms()[:3]
    voxel_vol_ml = float(np.prod(zooms)) / 1000.0
    return float(mask.sum()) * voxel_vol_ml


def _rts_find_dicom(series_dir: Path) -> Path | None:
    skip_ext = {".txt", ".csv", ".db", ".log"}
    for f in sorted(series_dir.iterdir()):
        if f.is_file() and f.suffix.lower() not in skip_ext:
            return f
    return None


def _rts_referenced_uid(ds) -> str | None:
    try:
        for fref in ds.ReferencedFrameOfReferenceSequence:
            for st in fref.RTReferencedStudySequence:
                for se in st.RTReferencedSeriesSequence:
                    return str(se.SeriesInstanceUID)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def import_rtstruct_series(
    series_dir: str | Path,
    session_id: int,
    subject_id: str,
    session_label: str,
) -> dict[str, Any]:
    """Importa un singolo RTSTRUCT nella tabella radiological_structures.

    Returns dict: annotator, rois, inserted, updated, errors
    """
    result: dict[str, Any] = {
        "annotator": None,
        "rois": [],
        "inserted": 0,
        "updated": 0,
        "errors": [],
    }
    series_dir = Path(series_dir)
    rtstruct_file = _rts_find_dicom(series_dir)
    if rtstruct_file is None:
        result["errors"].append(f"Nessun file DICOM in {series_dir}")
        return result

    try:
        ds = pydicom.dcmread(str(rtstruct_file), stop_before_pixels=True)
    except Exception as exc:
        result["errors"].append(f"Errore lettura RTSTRUCT: {exc}")
        return result

    if getattr(ds, "Modality", "") != "RTSTRUCT":
        result["errors"].append(f"Modality non RTSTRUCT: {getattr(ds, 'Modality', '?')}")
        return result

    annotator = str(getattr(ds, "SeriesDescription", "unknown")).strip() or "unknown"
    result["annotator"] = annotator
    referenced_uid = _rts_referenced_uid(ds)

    with db() as conn:
        ref_nifti, sequence_id = _find_reference_nifti(conn, session_id, referenced_uid)
        if ref_nifti is None:
            result["errors"].append(
                f"Nessun NIfTI di riferimento per session_id={session_id} "
                f"(referenced_uid={referenced_uid}) — preprocessing eseguito?"
            )
            return result

        try:
            masks = _rtstruct_to_masks(rtstruct_file, ref_nifti)
        except Exception as exc:
            result["errors"].append(f"Rasterizzazione fallita: {exc}")
            return result

        safe_ann = annotator.replace("/", "_").replace(" ", "_")
        for roi_name, mask in masks.items():
            out_path = (
                STRUCTURES_ROOT
                / subject_id
                / session_label
                / safe_ann
                / f"{subject_id}_{session_label}_{safe_ann}_{roi_name.lower()}.nii.gz"
            )
            try:
                _save_mask_nifti(mask, ref_nifti, out_path)
            except Exception as exc:
                result["errors"].append(f"Salvataggio maschera '{roi_name}' fallito: {exc}")
                continue

            vol = _volume_ml(mask, ref_nifti)

            existing = conn.execute(
                "SELECT id FROM radiological_structures "
                "WHERE session_id = ? AND label = ? AND annotator = ?",
                (session_id, roi_name, annotator),
            ).fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE radiological_structures
                    SET mask_path = ?, reference_space = 'canonical_1mm',
                        volume_ml = ?, sequence_id = ?
                    WHERE id = ?
                    """,
                    (str(out_path), vol, sequence_id, int(existing["id"])),
                )
                result["updated"] += 1
                action = "updated"
            else:
                conn.execute(
                    """
                    INSERT INTO radiological_structures
                        (session_id, sequence_id, label, mask_path,
                         reference_space, annotator, volume_ml, is_ground_truth)
                    VALUES (?, ?, ?, ?, 'canonical_1mm', ?, ?, 1)
                    """,
                    (session_id, sequence_id, roi_name, str(out_path), annotator, vol),
                )
                result["inserted"] += 1
                action = "inserted"

            result["rois"].append(
                {"name": roi_name, "mask_path": str(out_path),
                 "volume_ml": round(vol, 2), "action": action}
            )
            log.info(
                "RTSTRUCT %s/%s roi=%s annotator=%s vol=%.1f ml %s",
                subject_id, session_label, roi_name, annotator, vol, action,
            )

    return result


def find_and_import_rtstruct_series(
    exam_series: list[dict[str, Any]],
    session_id: int,
    subject_id: str,
    session_label: str,
) -> list[dict[str, Any]]:
    """Importa tutti i RTSTRUCT trovati nell'elenco serie di un exam.

    Chiamata da commit_import_selection dopo che sessione e sequenze sono salvate.
    Non solleva eccezioni: gli errori sono registrati nel risultato.
    """
    results = []
    for series in exam_series:
        if series.get("modality") != "RTSTRUCT":
            continue
        log.info(
            "Import RTSTRUCT %s per %s/%s",
            series["source_dir"], subject_id, session_label,
        )
        res = import_rtstruct_series(
            series_dir=series["source_dir"],
            session_id=session_id,
            subject_id=subject_id,
            session_label=session_label,
        )
        results.append({"source_dir": series["source_dir"], **res})
    return results


def scan_session_for_rtstruct(
    session_id: int,
    subject_id: str,
    session_label: str,
) -> list[dict[str, Any]]:
    """Scansiona la cartella raw della sessione alla ricerca di RTSTRUCT.

    Usa il raw_path di qualsiasi sequenza della sessione per risalire alla
    cartella studio, poi cerca sottocartelle con Modality=RTSTRUCT.
    Utile sia per import retroattivo sia chiamata post-preprocessing.
    """
    results = []
    with db() as conn:
        row = conn.execute(
            "SELECT raw_path FROM sequences WHERE session_id = ? AND raw_path IS NOT NULL LIMIT 1",
            (session_id,),
        ).fetchone()
    if not row:
        log.warning("scan_session_for_rtstruct: nessuna sequenza con raw_path per session_id=%d", session_id)
        return results

    study_dir = Path(row["raw_path"]).parent
    if not study_dir.is_dir():
        log.warning("scan_session_for_rtstruct: cartella studio non trovata: %s", study_dir)
        return results

    for subdir in sorted(study_dir.iterdir()):
        if not subdir.is_dir():
            continue
        dicom_file = _rts_find_dicom(subdir)
        if dicom_file is None:
            continue
        try:
            ds = pydicom.dcmread(str(dicom_file), stop_before_pixels=True)
        except Exception:
            continue
        if getattr(ds, "Modality", "") != "RTSTRUCT":
            continue
        log.info("Trovato RTSTRUCT: %s", subdir)
        res = import_rtstruct_series(
            series_dir=subdir,
            session_id=session_id,
            subject_id=subject_id,
            session_label=session_label,
        )
        results.append({"source_dir": str(subdir), **res})

    return results
