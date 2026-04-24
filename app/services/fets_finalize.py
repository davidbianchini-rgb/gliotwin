import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from fastapi import HTTPException

from app.db import db
from app.services.pipeline_state import update_step
from app.services.import_scan import _image_type_values, _read_anchor

APT_CONVERTER_PYTHON = Path("/home/irst/miniconda3/envs/fets-env/bin/python")
APT_REGISTER_SCRIPT  = Path(__file__).resolve().parents[2] / "pipelines" / "register_extra_to_reference.py"
APT_CONVERTER_SCRIPT = Path(__file__).resolve().parents[2] / "pipelines" / "convert_apt_dicom.py"
APT_MASK_SCRIPT      = Path(__file__).resolve().parents[2] / "pipelines" / "apply_brain_mask.py"
APT_IMPORT_ROOT = Path(__file__).resolve().parents[2] / "processed" / "imported_sequences"
EXTRA_SEQUENCE_TYPES = {"APT", "ADC", "SWAN", "CBV", "CBF", "MTT", "RSI"}

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


def _resolve_output_subject_id(run_path: Path, expected_subject_id: str, session_label: str) -> str:
    candidates = []
    for root in (
        run_path / "output" / "tumor_extracted" / "DataForFeTS",
        run_path / "output" / "brain_extracted" / "DataForFeTS",
        run_path / "output" / "DataForFeTS",
        run_path / "output" / "brain_extracted" / "DataForQC",
        run_path / "output" / "prepared" / "DataForQC",
    ):
        if not root.exists():
            continue
        direct = root / expected_subject_id / session_label
        if direct.exists():
            return expected_subject_id
        for session_dir in root.glob(f"*/{session_label}"):
            if session_dir.is_dir():
                candidates.append(session_dir.parent.name)
    unique = sorted(set(candidates))
    if len(unique) == 1:
        return unique[0]
    if expected_subject_id in unique:
        return expected_subject_id
    raise HTTPException(400, f"Unable to resolve FeTS output subject for {expected_subject_id}/{session_label}: {unique}")


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


def _resolve_mask_path(run_dir: Path, case_id: str, session_label: str, output_subject_id: str | None = None) -> Path:
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
        if output_subject_id:
            for candidate in sorted((tumor_masks / output_subject_id / session_label / "TumorMasksForQC").glob("*tumorMask*.nii.gz")):
                if candidate.exists():
                    return candidate
    raise HTTPException(400, f"FeTS segmentation output not found for {case_id}")


def _brain_output_path(run_path: Path, output_subject_id: str, session_label: str, sequence_type: str) -> Path:
    official_suffix = SEQUENCE_TARGETS[sequence_type]
    qc_suffix = QC_REORIENTED_NAMES[sequence_type]
    candidates = [
        run_path / "output" / "tumor_extracted" / "DataForFeTS" / output_subject_id / session_label / f"{output_subject_id}_{session_label}{official_suffix}",
        run_path / "output" / "brain_extracted" / "DataForFeTS" / output_subject_id / session_label / f"{output_subject_id}_{session_label}{official_suffix}",
        run_path / "output" / "DataForFeTS" / output_subject_id / session_label / f"{output_subject_id}_{session_label}{official_suffix}",
        run_path / "output" / "brain_extracted" / "DataForQC" / output_subject_id / session_label / "reoriented" / f"{output_subject_id}_{session_label}{qc_suffix}",
        run_path / "output" / "prepared" / "DataForQC" / output_subject_id / session_label / "reoriented" / f"{output_subject_id}_{session_label}{qc_suffix}",
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


def _find_t1ce_prepared(run_root: Path, output_subject_id: str, session_label: str) -> Path | None:
    """Return the non-skull-stripped T1ce in FeTS canonical space (prepared step output)."""
    candidates = [
        run_root / "output" / "prepared" / "DataForQC" / output_subject_id / f".{session_label}" / "reoriented" / f"{output_subject_id}_{session_label}_t1c.nii.gz",
        run_root / "output" / "prepared" / "DataForQC" / output_subject_id / session_label       / "reoriented" / f"{output_subject_id}_{session_label}_t1c.nii.gz",
        run_root / "output" / "brain_extracted" / "DataForQC" / output_subject_id / session_label / "reoriented" / f"{output_subject_id}_{session_label}_t1c.nii.gz",
    ]
    return next((c for c in candidates if c.exists()), None)


def _find_brain_mask(run_root: Path, output_subject_id: str, session_label: str) -> Path | None:
    candidates = [
        run_root / "output" / "brain_extracted" / "DataForQC" / output_subject_id / session_label / "brainMask_fused.nii.gz",
        run_root / "output" / "tumor_extracted" / "DataForQC" / output_subject_id / session_label / "brainMask_fused.nii.gz",
    ]
    return next((c for c in candidates if c.exists()), None)


def _link_or_copy(src: Path, dst: Path) -> None:
    try:
        dst.symlink_to(src)
    except Exception:
        shutil.copy2(src, dst)


def _convert_apt_raw_to_native(raw_path: str, output_path: Path) -> tuple[str | None, str | None]:
    source_dir = Path(raw_path)
    if not source_dir.exists():
        return None, f"APT source directory not found: {source_dir}"

    selected_files: list[Path] = []
    for dicom_path in sorted(source_dir.iterdir()):
        if not dicom_path.is_file():
            continue
        ds = _read_anchor(dicom_path)
        if ds is None:
            continue
        image_type = _image_type_values(ds)
        if any("APTW" in str(item).upper() for item in image_type):
            selected_files.append(dicom_path)

    if not selected_files:
        return None, "No APTW instances found in APT series"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gliotwin_apt_", dir="/tmp") as tmp_dir:
        subset_dir = Path(tmp_dir) / "aptw_subset"
        subset_dir.mkdir(parents=True, exist_ok=True)
        for dicom_path in selected_files:
            _link_or_copy(dicom_path, subset_dir / dicom_path.name)

        cmd = [
            str(APT_CONVERTER_PYTHON),
            str(APT_CONVERTER_SCRIPT),
            "--input-dir",
            str(subset_dir),
            "--output-img",
            str(output_path),
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=300)
        if proc.returncode != 0:
            error = (proc.stderr or proc.stdout or "").strip() or f"APT converter failed with exit code {proc.returncode}"
            return None, error
    return str(output_path), None


def _convert_series_raw_to_native(raw_path: str, output_path: Path) -> tuple[str | None, str | None]:
    source_dir = Path(raw_path)
    if not source_dir.exists():
        return None, f"Source directory not found: {source_dir}"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(APT_CONVERTER_PYTHON),
        str(APT_CONVERTER_SCRIPT),
        "--input-dir",
        str(source_dir),
        "--output-img",
        str(output_path),
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=300)
    if proc.returncode != 0:
        error = (proc.stderr or proc.stdout or "").strip() or f"Sequence converter failed with exit code {proc.returncode}"
        return None, error
    return str(output_path), None


def _apply_brain_mask(image_path: str, mask_path: Path, output_path: Path) -> tuple[str | None, str | None]:
    cmd = [
        str(APT_CONVERTER_PYTHON),
        str(APT_MASK_SCRIPT),
        "--image",
        str(image_path),
        "--mask",
        str(mask_path),
        "--output",
        str(output_path),
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=300)
    if proc.returncode != 0:
        error = (proc.stderr or proc.stdout or "").strip() or f"APT brain mask failed with exit code {proc.returncode}"
        return None, error
    return str(output_path), None


def _extra_meta_keys(sequence_type: str) -> dict[str, str]:
    if sequence_type == "APT":
        return {
            "native_path": "apt_native_path",
            "registered_path": "apt_registered_path",
            "transform_path": "apt_transform_path",
            "brain_mask_path": "apt_brain_mask_path",
            "brain_mask_source": "apt_brain_mask_source",
            "conversion_error": "apt_conversion_error",
            "registration_error": "apt_registration_error",
            "brain_mask_error": "apt_brain_mask_error",
            "resample_strategy": "apt_resample_strategy",
        }
    prefix = sequence_type.lower()
    return {
        "native_path": f"{prefix}_native_path",
        "registered_path": f"{prefix}_registered_path",
        "transform_path": f"{prefix}_transform_path",
        "brain_mask_path": f"{prefix}_brain_mask_path",
        "brain_mask_source": f"{prefix}_brain_mask_source",
        "conversion_error": f"{prefix}_conversion_error",
        "registration_error": f"{prefix}_registration_error",
        "brain_mask_error": f"{prefix}_brain_mask_error",
        "resample_strategy": f"{prefix}_resample_strategy",
    }


def _native_output_path(subject_id: str, session_label: str, sequence_type: str, series_uid: str) -> Path:
    prefix = sequence_type.lower()
    return APT_IMPORT_ROOT / subject_id / session_label / prefix / f"{subject_id}_{session_label}_{series_uid}_{prefix}.nii.gz"


def _registered_output_path(subject_id: str, session_label: str, sequence_type: str, series_uid: str) -> Path:
    prefix = sequence_type.lower()
    return APT_IMPORT_ROOT / subject_id / session_label / f"{prefix}_registered" / f"{subject_id}_{session_label}_{series_uid}_{prefix}_reg.nii.gz"


def _masked_output_path(subject_id: str, session_label: str, sequence_type: str, series_uid: str) -> Path:
    prefix = sequence_type.lower()
    return APT_IMPORT_ROOT / subject_id / session_label / f"{prefix}_registered" / f"{subject_id}_{session_label}_{series_uid}_{prefix}_reg_brain.nii.gz"


def _shared_transform_path(subject_id: str, session_label: str) -> Path:
    return APT_IMPORT_ROOT / subject_id / session_label / "transforms" / f"{subject_id}_{session_label}_native_to_canonical.mat"


def _transfer_extra_sequences(conn, session_id: int, t1ce_raw_dicom: str, run_root: Path,
                              subject_id: str, session_label: str, output_subject_id: str) -> list[dict]:
    t1ce_prepared = _find_t1ce_prepared(run_root, output_subject_id, session_label)
    if t1ce_prepared is None:
        return [{"status": "skipped", "reason": "T1ce prepared not found"}]
    brain_mask = _find_brain_mask(run_root, output_subject_id, session_label)
    shared_transform = _shared_transform_path(subject_id, session_label)

    rows = conn.execute(
        "SELECT id, sequence_type, raw_path, metadata_json FROM sequences WHERE session_id = ?",
        (session_id,),
    ).fetchall()
    results = []
    for row in rows:
        sequence_type = row["sequence_type"]
        if sequence_type not in EXTRA_SEQUENCE_TYPES:
            continue
        seq_id   = row["id"]
        metadata = json.loads(row["metadata_json"] or "{}")
        meta_keys = _extra_meta_keys(sequence_type)
        native_path = metadata.get(meta_keys["native_path"])
        series_uid = str(metadata.get("series_instance_uid") or sequence_type.lower()).replace(".", "_")
        if not native_path or not Path(native_path).exists():
            raw_path = row["raw_path"]
            if not raw_path:
                results.append({"seq_id": seq_id, "sequence_type": sequence_type, "status": "skipped", "reason": "raw_path missing"})
                continue
            native_guess = _native_output_path(subject_id, session_label, sequence_type, series_uid)
            if sequence_type == "APT":
                native_path, convert_error = _convert_apt_raw_to_native(raw_path, native_guess)
            else:
                native_path, convert_error = _convert_series_raw_to_native(raw_path, native_guess)
            metadata[meta_keys["native_path"]] = native_path
            metadata[meta_keys["conversion_error"]] = convert_error
            if not native_path:
                conn.execute(
                    "UPDATE sequences SET metadata_json = ? WHERE id = ?",
                    (json.dumps(metadata, ensure_ascii=True), seq_id),
                )
                results.append({"seq_id": seq_id, "sequence_type": sequence_type, "status": "failed", "error": convert_error or "conversion failed"})
                continue

        output_path = _registered_output_path(subject_id, session_label, sequence_type, series_uid)
        masked_output = _masked_output_path(subject_id, session_label, sequence_type, series_uid)

        cmd = [
            str(APT_CONVERTER_PYTHON), str(APT_REGISTER_SCRIPT),
            "--t1ce-dicom",     str(t1ce_raw_dicom),
            "--t1ce-prepared",  str(t1ce_prepared),
            "--moving-nifti",   str(native_path),
            "--output",         str(output_path),
        ]
        if shared_transform.exists():
            cmd.extend(["--transform-in", str(shared_transform)])
        else:
            cmd.extend(["--save-transform", str(shared_transform)])
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, timeout=300)
        if proc.returncode == 0:
            final_processed = str(output_path)
            metadata[meta_keys["registered_path"]] = str(output_path)
            metadata[meta_keys["transform_path"]] = str(shared_transform)
            metadata[meta_keys["resample_strategy"]] = "ants_affine_native_to_canonical"
            metadata[meta_keys["registration_error"]] = None
            metadata[meta_keys["brain_mask_path"]] = None
            metadata[meta_keys["brain_mask_error"]] = None
            if brain_mask is not None:
                masked_path, mask_error = _apply_brain_mask(str(output_path), brain_mask, masked_output)
                metadata[meta_keys["brain_mask_path"]] = masked_path
                metadata[meta_keys["brain_mask_source"]] = str(brain_mask)
                metadata[meta_keys["brain_mask_error"]] = mask_error
                if masked_path:
                    final_processed = masked_path
            conn.execute(
                "UPDATE sequences SET processed_path = ?, metadata_json = ? WHERE id = ?",
                (final_processed, json.dumps(metadata, ensure_ascii=True), seq_id),
            )
            results.append({
                "seq_id": seq_id,
                "sequence_type": sequence_type,
                "status": "ok",
                "path": final_processed,
                "registered_path": str(output_path),
                "brain_masked": final_processed != str(output_path),
            })
        else:
            error = (proc.stderr or proc.stdout or "").strip() or f"exit {proc.returncode}"
            metadata[meta_keys["registration_error"]] = error
            conn.execute(
                "UPDATE sequences SET metadata_json = ? WHERE id = ?",
                (json.dumps(metadata, ensure_ascii=True), seq_id),
            )
            results.append({"seq_id": seq_id, "sequence_type": sequence_type, "status": "failed", "error": error})
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
        output_subject_id = _resolve_output_subject_id(run_path, subject_id, session_label)

        missing_sequences = [seq_type for seq_type in SEQUENCE_TARGETS if seq_type not in seq_ids]
        if missing_sequences:
            raise HTTPException(400, f"Missing raw sequences in DB for session {session_id}: {', '.join(missing_sequences)}")

        brain_root = _resolve_brain_dir(run_path)
        brain_case_dir = brain_root / output_subject_id / session_label
        if not brain_case_dir.exists():
            raise HTTPException(400, f"FeTS case folder not found: {brain_case_dir}")

        final_dir = FINAL_ROOT / subject_id / session_label
        copied_sequences = []

        for sequence_type, suffix in SEQUENCE_TARGETS.items():
            src = brain_case_dir / f"{output_subject_id}_{session_label}{suffix}"
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

        # Transfer extra static sequences into FeTS space using T1ce as anchor
        t1ce_raw_path = conn.execute(
            "SELECT raw_path FROM sequences WHERE session_id = ? AND sequence_type = 'T1ce'",
            (session_id,),
        ).fetchone()
        extra_results: list[dict] = []
        if t1ce_raw_path and t1ce_raw_path["raw_path"]:
            extra_results = _transfer_extra_sequences(
                conn, session_id, t1ce_raw_path["raw_path"],
                run_path, subject_id, session_label, output_subject_id,
            )

        seg_src = _resolve_mask_path(run_path, case_id, session_label, output_subject_id)
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
        "extra_transfer": extra_results,
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
        output_subject_id = _resolve_output_subject_id(run_path, subject_id, session_label)

        missing_sequences = [seq_type for seq_type in SEQUENCE_TARGETS if seq_type not in seq_ids]
        if missing_sequences:
            raise HTTPException(400, f"Missing raw sequences in DB for session {session_id}: {', '.join(missing_sequences)}")

        brain_outputs = []
        processed_dir = None
        for sequence_type in SEQUENCE_TARGETS:
            src = _brain_output_path(run_path, output_subject_id, session_label, sequence_type)
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

        # Transfer extra static sequences into FeTS space
        t1ce_raw_row = conn.execute(
            "SELECT raw_path FROM sequences WHERE session_id = ? AND sequence_type = 'T1ce'",
            (session_id,),
        ).fetchone()
        extra_results: list[dict] = []
        if t1ce_raw_row and t1ce_raw_row["raw_path"]:
            extra_results = _transfer_extra_sequences(
                conn, session_id, t1ce_raw_row["raw_path"],
                run_path, subject_id, session_label, output_subject_id,
            )

        seg_src = _resolve_mask_path(run_path, case_id, session_label, output_subject_id)
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
        "extra_transfer": extra_results,
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
        output_subject_id = _resolve_output_subject_id(run_path, subject_id, session_label)

        missing_sequences = [seq_type for seq_type in SEQUENCE_TARGETS if seq_type not in seq_ids]
        if missing_sequences:
            raise HTTPException(400, f"Missing raw sequences in DB for session {session_id}: {', '.join(missing_sequences)}")

        brain_outputs = []
        processed_dir = None
        for sequence_type in SEQUENCE_TARGETS:
            src = _brain_output_path(run_path, output_subject_id, session_label, sequence_type)
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

        t1ce_raw_row = conn.execute(
            "SELECT raw_path FROM sequences WHERE session_id = ? AND sequence_type = 'T1ce'",
            (session_id,),
        ).fetchone()
        extra_results: list[dict] = []
        if t1ce_raw_row and t1ce_raw_row["raw_path"]:
            extra_results = _transfer_extra_sequences(
                conn, session_id, t1ce_raw_row["raw_path"],
                run_path, subject_id, session_label, output_subject_id,
            )

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
        "extra_transfer": extra_results,
    }
