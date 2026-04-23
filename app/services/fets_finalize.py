import json
import shutil
import subprocess
from pathlib import Path

from fastapi import HTTPException

from app.db import db
from app.services.pipeline_state import update_step

APT_CONVERTER_PYTHON = Path("/home/irst/miniconda3/envs/fets-env/bin/python")
APT_REGISTER_SCRIPT  = Path(__file__).resolve().parents[2] / "pipelines" / "register_apt_to_reference.py"

FINAL_ROOT = Path("/mnt/dati/irst_data/irst_preprocessed_final")

SEQUENCE_TARGETS = {
    "T1": "_brain_t1n.nii.gz",
    "T1ce": "_brain_t1c.nii.gz",
    "T2": "_brain_t2w.nii.gz",
    "FLAIR": "_brain_t2f.nii.gz",
}

QC_REORIENTED_NAMES = {
    "T1": "_t1.nii.gz",
    "T1ce": "_t1c.nii.gz",
    "T2": "_t2w.nii.gz",
    "FLAIR": "_t2f.nii.gz",
}

MASK_LABELS = {
    1: "necrotic_core",
    2: "edema",
    3: "enhancing_tumor",
    4: "resection_cavity",
}


def _find_subject_session(conn, session_id: int):
    row = conn.execute(
        """
        SELECT
            ses.id AS session_id,
            ses.session_label,
            sub.subject_id
        FROM sessions ses
        JOIN subjects sub ON sub.id = ses.subject_id
        WHERE ses.id = ?
        """,
        (session_id,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "Session not found")
    return row


def _find_sequence_ids(conn, session_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT id, sequence_type FROM sequences WHERE session_id = ?",
        (session_id,),
    ).fetchall()
    return {row["sequence_type"]: int(row["id"]) for row in rows}


def _resolve_brain_dir(run_dir: Path) -> Path:
    for candidate in (
        run_dir / "output" / "tumor_extracted" / "DataForFeTS",
        run_dir / "output" / "brain_extracted" / "DataForFeTS",
        run_dir / "output" / "DataForFeTS",
    ):
        if candidate.exists():
            return candidate
    raise HTTPException(400, f"FeTS brain output not found in {run_dir}")


def _resolve_run_path(run_dir: str | Path, subject_id: str) -> Path:
    run_path = Path(run_dir).expanduser().resolve()
    if not run_path.exists():
        raise HTTPException(400, f"Run directory not found: {run_path}")
    if run_path.name.startswith(f"{subject_id}_"):
        return run_path
    matches = sorted(run_path.glob(f"{subject_id}_*"))
    if matches:
        return matches[-1]
    return run_path


def _resolve_mask_path(run_dir: Path, case_id: str) -> Path:
    candidates = [
        run_dir / "output_labels" / f"{case_id}.nii.gz",
        run_dir / "output_labels" / f"{case_id}_tumorMask.nii.gz",
        run_dir / "output" / "tumor_extracted" / "tmp-out" / f"{case_id}.nii.gz",
        run_dir / "output" / "tumor_extracted" / "tmp-out" / f"{case_id}_tumorMask.nii.gz",
    ]
    for path in candidates:
        if path.exists():
            return path
    tumor_masks = run_dir / "output" / "tumor_extracted" / "DataForQC"
    if tumor_masks.exists():
        for candidate in sorted(tumor_masks.glob(f"*/{case_id.split('-', 1)[1]}/TumorMasksForQC/*tumorMask*.nii.gz")):
            if candidate.exists():
                return candidate
    raise HTTPException(400, f"FeTS segmentation output not found for {case_id}")


def _brain_output_path(run_path: Path, subject_id: str, session_label: str, sequence_type: str) -> Path:
    official_suffix = SEQUENCE_TARGETS[sequence_type]
    qc_suffix = QC_REORIENTED_NAMES[sequence_type]
    candidates = [
        run_path / "output" / "tumor_extracted" / "DataForFeTS" / subject_id / session_label / f"{subject_id}_{session_label}{official_suffix}",
        run_path / "output" / "brain_extracted" / "DataForFeTS" / subject_id / session_label / f"{subject_id}_{session_label}{official_suffix}",
        run_path / "output" / "DataForFeTS" / subject_id / session_label / f"{subject_id}_{session_label}{official_suffix}",
        run_path / "output" / "brain_extracted" / "DataForQC" / subject_id / session_label / "reoriented" / f"{subject_id}_{session_label}{qc_suffix}",
        run_path / "output" / "prepared" / "DataForQC" / subject_id / session_label / "reoriented" / f"{subject_id}_{session_label}{qc_suffix}",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise HTTPException(400, f"Missing FeTS output file for {sequence_type}: {candidates[0]}")


def _copy_file(src: Path, dst: Path) -> str:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return str(dst)


def _mask_volumes(mask_path: Path) -> dict[int, float]:
    try:
        import nibabel as nib
        import numpy as np
    except ImportError as exc:
        raise HTTPException(500, f"Missing dependency for mask inspection: {exc}") from exc

    img = nib.load(str(mask_path))
    data = img.get_fdata()
    voxel_volume_ml = float(img.header.get_zooms()[0] * img.header.get_zooms()[1] * img.header.get_zooms()[2]) / 1000.0
    out: dict[int, float] = {}
    for label_code in MASK_LABELS:
        voxels = int(np.count_nonzero(data == label_code))
        out[label_code] = round(voxels * voxel_volume_ml, 3) if voxels else 0.0
    return out


def _find_t1ce_prepared(run_root: Path, subject_id: str, session_label: str) -> Path | None:
    """Return the non-skull-stripped T1ce in FeTS canonical space (prepared step output)."""
    candidates = [
        run_root / "output" / "prepared" / "DataForQC" / subject_id / f".{session_label}" / "reoriented" / f"{subject_id}_{session_label}_t1c.nii.gz",
        run_root / "output" / "prepared" / "DataForQC" / subject_id / session_label       / "reoriented" / f"{subject_id}_{session_label}_t1c.nii.gz",
        run_root / "output" / "brain_extracted" / "DataForQC" / subject_id / session_label / "reoriented" / f"{subject_id}_{session_label}_t1c.nii.gz",
    ]
    return next((c for c in candidates if c.exists()), None)


def _finalize_apt_sequences(conn, session_id: int, t1ce_raw_dicom: str, run_root: Path,
                             subject_id: str, session_label: str) -> list[dict]:
    """Register APT from native DICOM space into FeTS canonical space.

    Uses T1ce_prepared (canonical, with skull) as fixed and T1ce_native (DICOM,
    with skull) as moving — same modality, same skull content → NCC affine.
    Saves the transform to disk for traceability.
    """
    t1ce_prepared = _find_t1ce_prepared(run_root, subject_id, session_label)
    if t1ce_prepared is None:
        return [{"status": "skipped", "reason": "T1ce prepared not found"}]

    rows = conn.execute(
        "SELECT id, metadata_json FROM sequences WHERE session_id = ? AND sequence_type = 'APT'",
        (session_id,),
    ).fetchall()
    results = []
    for row in rows:
        seq_id   = row["id"]
        metadata = json.loads(row["metadata_json"] or "{}")
        apt_native = metadata.get("apt_native_path")
        if not apt_native or not Path(apt_native).exists():
            results.append({"seq_id": seq_id, "status": "skipped", "reason": "apt_native_path missing"})
            continue

        apt_native_path = Path(apt_native)
        stem            = apt_native_path.name.replace("_aptw.nii.gz", "")
        output_path     = apt_native_path.parent.parent / "apt_registered" / f"{stem}_aptw_reg.nii.gz"
        transform_path  = apt_native_path.parent.parent / "apt_registered" / f"{stem}_native_to_canonical.mat"

        cmd = [
            str(APT_CONVERTER_PYTHON), str(APT_REGISTER_SCRIPT),
            "--t1ce-dicom",     str(t1ce_raw_dicom),
            "--t1ce-prepared",  str(t1ce_prepared),
            "--apt-nifti",      str(apt_native),
            "--output",         str(output_path),
            "--save-transform", str(transform_path),
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, timeout=300)
        if proc.returncode == 0:
            metadata["apt_registered_path"]   = str(output_path)
            metadata["apt_transform_path"]     = str(transform_path)
            metadata["apt_resample_strategy"]  = "ants_affine_native_to_canonical"
            metadata["apt_registration_error"] = None
            conn.execute(
                "UPDATE sequences SET processed_path = ?, metadata_json = ? WHERE id = ?",
                (str(output_path), json.dumps(metadata, ensure_ascii=True), seq_id),
            )
            results.append({"seq_id": seq_id, "status": "ok", "path": str(output_path)})
        else:
            error = (proc.stderr or proc.stdout or "").strip() or f"exit {proc.returncode}"
            metadata["apt_registration_error"] = error
            conn.execute(
                "UPDATE sequences SET metadata_json = ? WHERE id = ?",
                (json.dumps(metadata, ensure_ascii=True), seq_id),
            )
            results.append({"seq_id": seq_id, "status": "failed", "error": error})
    return results


def finalize_fets_run(session_id: int, run_dir: str) -> dict:
    run_path = Path(run_dir).expanduser().resolve()
    if not run_path.exists():
        raise HTTPException(400, f"Run directory not found: {run_path}")

    with db() as conn:
        session_info = _find_subject_session(conn, session_id)
        subject_id = session_info["subject_id"]
        session_label = session_info["session_label"]
        case_id = f"{subject_id}-{session_label}"
        seq_ids = _find_sequence_ids(conn, session_id)

        missing_sequences = [seq_type for seq_type in SEQUENCE_TARGETS if seq_type not in seq_ids]
        if missing_sequences:
            raise HTTPException(400, f"Missing raw sequences in DB for session {session_id}: {', '.join(missing_sequences)}")

        brain_root = _resolve_brain_dir(run_path)
        brain_case_dir = brain_root / subject_id / session_label
        if not brain_case_dir.exists():
            raise HTTPException(400, f"FeTS case folder not found: {brain_case_dir}")

        final_dir = FINAL_ROOT / subject_id / session_label
        copied_sequences = []

        for sequence_type, suffix in SEQUENCE_TARGETS.items():
            src = brain_case_dir / f"{subject_id}_{session_label}{suffix}"
            if not src.exists():
                raise HTTPException(400, f"Missing FeTS output file: {src}")
            dst = final_dir / src.name
            copied_path = _copy_file(src, dst)
            hdr = {}
            try:
                from pipelines._utils import read_nifti_header
                hdr = read_nifti_header(dst)
            except Exception:
                hdr = {}
            conn.execute(
                """
                UPDATE sequences
                SET processed_path = ?, shape_x = ?, shape_y = ?, shape_z = ?,
                    spacing_x = ?, spacing_y = ?, spacing_z = ?
                WHERE id = ?
                """,
                (
                    copied_path,
                    hdr.get("shape_x"),
                    hdr.get("shape_y"),
                    hdr.get("shape_z"),
                    hdr.get("spacing_x"),
                    hdr.get("spacing_y"),
                    hdr.get("spacing_z"),
                    seq_ids[sequence_type],
                ),
            )
            copied_sequences.append({"sequence_type": sequence_type, "path": copied_path})

        # Transfer APT into FeTS space using T1ce as anchor
        t1ce_raw_path = conn.execute(
            "SELECT raw_path FROM sequences WHERE session_id = ? AND sequence_type = 'T1ce'",
            (session_id,),
        ).fetchone()
        apt_results: list[dict] = []
        if t1ce_raw_path and t1ce_raw_path["raw_path"]:
            apt_results = _finalize_apt_sequences(
                conn, session_id, t1ce_raw_path["raw_path"],
                run_path, subject_id, session_label,
            )

        seg_src = _resolve_mask_path(run_path, case_id)
        seg_dst = final_dir / f"{subject_id}_{session_label}_tumorMask.nii.gz"
        seg_path = _copy_file(seg_src, seg_dst)
        volumes = _mask_volumes(seg_dst)

        conn.execute(
            "UPDATE sessions SET processed_dir = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?",
            (str(final_dir), session_id),
        )
        conn.execute("DELETE FROM computed_structures WHERE session_id = ? AND model_name = ?", (session_id, "fets_postop"))
        for label_code, label_name in MASK_LABELS.items():
            conn.execute(
                """
                INSERT INTO computed_structures (
                    session_id, sequence_id, label, label_code, mask_path, reference_space,
                    model_name, model_version, volume_ml
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    seq_ids["T1ce"],
                    label_name,
                    label_code,
                    seg_path,
                    "native",
                    "fets_postop",
                    "fallback_tmp_out",
                    volumes[label_code],
                ),
            )

    update_step(
        subject_id,
        session_label,
        "segmentation",
        status="done",
        output_path=seg_path,
        error_message=None,
        started=True,
        finished=True,
        session_id=session_id,
    )
    update_step(
        subject_id,
        session_label,
        "final_export",
        status="done",
        output_path=str(final_dir),
        error_message=None,
        started=True,
        finished=True,
        session_id=session_id,
    )
    update_step(
        subject_id,
        session_label,
        "rtstruct_conversion",
        status="pending",
        output_path=None,
        error_message="Step not implemented in current pipeline",
        started=False,
        finished=False,
        session_id=session_id,
    )

    return {
        "session_id": session_id,
        "subject_id": subject_id,
        "session_label": session_label,
        "processed_dir": str(final_dir),
        "brain_outputs": copied_sequences,
        "apt_transfer": apt_results,
        "segmentation_path": seg_path,
        "mask_source": str(seg_src),
        "volumes_ml": volumes,
    }


def sync_fets_outputs(session_id: int, run_dir: str) -> dict:
    with db() as conn:
        session_info = _find_subject_session(conn, session_id)
        subject_id = session_info["subject_id"]
        session_label = session_info["session_label"]
        case_id = f"{subject_id}-{session_label}"
        seq_ids = _find_sequence_ids(conn, session_id)
        run_path = _resolve_run_path(run_dir, subject_id)

        missing_sequences = [seq_type for seq_type in SEQUENCE_TARGETS if seq_type not in seq_ids]
        if missing_sequences:
            raise HTTPException(400, f"Missing raw sequences in DB for session {session_id}: {', '.join(missing_sequences)}")

        brain_outputs = []
        processed_dir = None
        for sequence_type in SEQUENCE_TARGETS:
            src = _brain_output_path(run_path, subject_id, session_label, sequence_type)
            processed_dir = str(src.parent)
            hdr = {}
            try:
                from pipelines._utils import read_nifti_header
                hdr = read_nifti_header(src)
            except Exception:
                hdr = {}
            conn.execute(
                """
                UPDATE sequences
                SET processed_path = ?, shape_x = ?, shape_y = ?, shape_z = ?,
                    spacing_x = ?, spacing_y = ?, spacing_z = ?
                WHERE id = ?
                """,
                (
                    str(src),
                    hdr.get("shape_x"),
                    hdr.get("shape_y"),
                    hdr.get("shape_z"),
                    hdr.get("spacing_x"),
                    hdr.get("spacing_y"),
                    hdr.get("spacing_z"),
                    seq_ids[sequence_type],
                ),
            )
            brain_outputs.append({"sequence_type": sequence_type, "path": str(src)})

        # Transfer APT into FeTS space
        t1ce_raw_row = conn.execute(
            "SELECT raw_path FROM sequences WHERE session_id = ? AND sequence_type = 'T1ce'",
            (session_id,),
        ).fetchone()
        apt_results: list[dict] = []
        if t1ce_raw_row and t1ce_raw_row["raw_path"]:
            apt_results = _finalize_apt_sequences(
                conn, session_id, t1ce_raw_row["raw_path"],
                run_path, subject_id, session_label,
            )

        seg_src = _resolve_mask_path(run_path, case_id)
        volumes = _mask_volumes(seg_src)

        conn.execute(
            "UPDATE sessions SET processed_dir = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?",
            (processed_dir, session_id),
        )
        conn.execute("DELETE FROM computed_structures WHERE session_id = ? AND model_name = ?", (session_id, "fets_postop"))
        for label_code, label_name in MASK_LABELS.items():
            conn.execute(
                """
                INSERT INTO computed_structures (
                    session_id, sequence_id, label, label_code, mask_path, reference_space,
                    model_name, model_version, volume_ml
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    seq_ids["T1ce"],
                    label_name,
                    label_code,
                    str(seg_src),
                    "native",
                    "fets_postop",
                    "fets_official",
                    volumes[label_code],
                ),
            )

    return {
        "session_id": session_id,
        "subject_id": subject_id,
        "session_label": session_label,
        "processed_dir": processed_dir,
        "brain_outputs": brain_outputs,
        "apt_transfer": apt_results,
        "segmentation_path": str(seg_src),
        "volumes_ml": volumes,
    }


def sync_fets_brain_outputs(session_id: int, run_dir: str) -> dict:
    with db() as conn:
        session_info = _find_subject_session(conn, session_id)
        subject_id = session_info["subject_id"]
        session_label = session_info["session_label"]
        seq_ids = _find_sequence_ids(conn, session_id)
        run_path = _resolve_run_path(run_dir, subject_id)

        missing_sequences = [seq_type for seq_type in SEQUENCE_TARGETS if seq_type not in seq_ids]
        if missing_sequences:
            raise HTTPException(400, f"Missing raw sequences in DB for session {session_id}: {', '.join(missing_sequences)}")

        brain_outputs = []
        processed_dir = None
        for sequence_type in SEQUENCE_TARGETS:
            src = _brain_output_path(run_path, subject_id, session_label, sequence_type)
            processed_dir = str(src.parent)
            hdr = {}
            try:
                from pipelines._utils import read_nifti_header
                hdr = read_nifti_header(src)
            except Exception:
                hdr = {}
            conn.execute(
                """
                UPDATE sequences
                SET processed_path = ?, shape_x = ?, shape_y = ?, shape_z = ?,
                    spacing_x = ?, spacing_y = ?, spacing_z = ?
                WHERE id = ?
                """,
                (
                    str(src),
                    hdr.get("shape_x"),
                    hdr.get("shape_y"),
                    hdr.get("shape_z"),
                    hdr.get("spacing_x"),
                    hdr.get("spacing_y"),
                    hdr.get("spacing_z"),
                    seq_ids[sequence_type],
                ),
            )
            brain_outputs.append({"sequence_type": sequence_type, "path": str(src)})

        conn.execute(
            "UPDATE sessions SET processed_dir = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?",
            (processed_dir, session_id),
        )

    return {
        "session_id": session_id,
        "subject_id": subject_id,
        "session_label": session_label,
        "processed_dir": processed_dir,
        "brain_outputs": brain_outputs,
    }
