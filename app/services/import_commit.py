import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.db import db
from app.services.import_scan import REQUIRED_CLASSES, _image_type_values, _read_anchor, scan_dicom_root
from app.services.pipeline_state import ensure_state, reset_downstream_steps, update_step

DATASET_NAME = "irst_dicom_raw"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
APT_CONVERTER_PYTHON = Path("/home/irst/miniconda3/envs/fets-env/bin/python")
APT_CONVERTER_SCRIPT = PROJECT_ROOT / "pipelines" / "convert_apt_dicom.py"
APT_REGISTER_SCRIPT = PROJECT_ROOT / "pipelines" / "register_apt_to_reference.py"
APT_PROCESSED_ROOT = PROJECT_ROOT / "processed" / "imported_sequences"

CLASS_TO_SEQUENCE = {
    "t1n": ("T1", 0),
    "t1c": ("T1ce", 1),
    "t2w": ("T2", 0),
    "t2f": ("FLAIR", 0),
    "dwi": ("DWI", 0),
    "adc": ("ADC", 0),
    "swi": ("SWAN", 0),
    "perf": ("DSC", 0),
    "ktrans": ("CBV", 0),
    "apt": ("APT", 0),
}


def _seq_key(series: dict[str, Any]) -> str:
    return series.get("series_instance_uid") or series["source_dir"]


def _selected_core(exam: dict[str, Any], core_choice: dict[str, str]) -> dict[str, dict[str, Any] | None]:
    selected: dict[str, dict[str, Any] | None] = {}
    for label in REQUIRED_CLASSES:
        candidates = exam.get("core_candidates", {}).get(label, [])
        chosen_key = core_choice.get(f"{exam['exam_key']}|{label}")
        if chosen_key:
            selected[label] = next((item for item in candidates if _seq_key(item) == chosen_key), None)
        else:
            selected[label] = exam.get("core_selection", {}).get(label)
    return selected


def _upsert_subject(conn, exam: dict[str, Any]) -> int:
    notes = f"Imported from DICOM root · patient_folder={exam['patient_folder']}"
    conn.execute(
        """
        INSERT OR IGNORE INTO subjects (
            subject_id, dataset, notes, sex,
            patient_name, patient_given_name, patient_family_name, patient_birth_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            exam["patient_id"],
            DATASET_NAME,
            notes,
            exam.get("patient_sex") or None,
            exam.get("patient_name") or None,
            exam.get("patient_given_name") or None,
            exam.get("patient_family_name") or None,
            exam.get("patient_birth_date") or None,
        ),
    )
    row = conn.execute(
        "SELECT id, notes FROM subjects WHERE subject_id = ? AND dataset = ?",
        (exam["patient_id"], DATASET_NAME),
    ).fetchone()
    if row:
        conn.execute(
            """
            UPDATE subjects
            SET notes = ?,
                sex = COALESCE(NULLIF(?, ''), sex),
                patient_name = COALESCE(NULLIF(?, ''), patient_name),
                patient_given_name = COALESCE(NULLIF(?, ''), patient_given_name),
                patient_family_name = COALESCE(NULLIF(?, ''), patient_family_name),
                patient_birth_date = COALESCE(NULLIF(?, ''), patient_birth_date),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (
                notes if not row["notes"] else row["notes"],
                exam.get("patient_sex") or "",
                exam.get("patient_name") or "",
                exam.get("patient_given_name") or "",
                exam.get("patient_family_name") or "",
                exam.get("patient_birth_date") or "",
                row["id"],
            ),
        )
    return int(row["id"])


def _upsert_session(conn, subject_pk: int, exam: dict[str, Any]) -> int:
    clinical_context = exam.get("study_description") or "Imported IRST DICOM exam"
    raw_dir = "/".join([exam["patient_folder"], exam["study_folder"]])
    quality_flag = "ok" if exam["status"] != "incomplete" else "pending"
    conn.execute(
        """
        INSERT OR IGNORE INTO sessions (
            subject_id, session_label, days_from_baseline, timepoint_type,
            clinical_context, study_date, study_time, raw_dir, processed_dir, quality_flag
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            subject_pk,
            exam["timepoint_label"],
            None,
            "other",
            clinical_context,
            exam.get("study_date") or None,
            exam.get("study_time") or None,
            raw_dir,
            None,
            quality_flag,
        ),
    )
    conn.execute(
        """
        UPDATE sessions
        SET clinical_context = ?, study_date = ?, study_time = ?, raw_dir = ?, quality_flag = ?,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE subject_id = ? AND session_label = ?
        """,
        (
            clinical_context,
            exam.get("study_date") or None,
            exam.get("study_time") or None,
            raw_dir,
            quality_flag,
            subject_pk,
            exam["timepoint_label"],
        ),
    )
    row = conn.execute(
        "SELECT id FROM sessions WHERE subject_id = ? AND session_label = ?",
        (subject_pk, exam["timepoint_label"]),
    ).fetchone()
    return int(row["id"])


def _sequence_payload(series: dict[str, Any], label: str) -> tuple[str, int, str]:
    sequence_type, contrast_agent = CLASS_TO_SEQUENCE.get(label, ("OTHER", 0))
    metadata_json = json.dumps({
        "series_description": series.get("series_description"),
        "protocol_name": series.get("protocol_name"),
        "image_type": series.get("image_type"),
        "sop_class_uid": series.get("sop_class_uid"),
        "manufacturer": series.get("manufacturer"),
        "model_name": series.get("model_name"),
        "magnetic_field_strength": series.get("magnetic_field_strength"),
        "study_instance_uid": series.get("study_instance_uid"),
        "series_instance_uid": series.get("series_instance_uid"),
        "study_date": series.get("study_date"),
        "study_time": series.get("study_time"),
        "n_files": series.get("n_files"),
        "instance_type_summary": series.get("instance_type_summary"),
        "preferred_instance_count": series.get("preferred_instance_count"),
        "instance_selector": series.get("instance_selector"),
        "apt_conversion_mode": "image_type_contains_aptw" if label == "apt" else None,
    }, ensure_ascii=True)
    return sequence_type, contrast_agent, metadata_json


def _apt_processed_path(subject_id: str, timepoint_label: str, series: dict[str, Any]) -> Path:
    series_uid = str(series.get("series_instance_uid") or "apt").replace(".", "_")
    return APT_PROCESSED_ROOT / subject_id / timepoint_label / "apt" / f"{subject_id}_{timepoint_label}_{series_uid}_aptw.nii.gz"


def _apt_registered_path(subject_id: str, timepoint_label: str, series: dict[str, Any]) -> Path:
    series_uid = str(series.get("series_instance_uid") or "apt").replace(".", "_")
    return APT_PROCESSED_ROOT / subject_id / timepoint_label / "apt_registered" / f"{subject_id}_{timepoint_label}_{series_uid}_aptw_reg.nii.gz"


def _apt_transform_path(subject_id: str, timepoint_label: str, series: dict[str, Any]) -> Path:
    series_uid = str(series.get("series_instance_uid") or "apt").replace(".", "_")
    return APT_PROCESSED_ROOT / subject_id / timepoint_label / "apt_registered" / f"{subject_id}_{timepoint_label}_{series_uid}_aptw_reg.tfm"


def _apt_reference_native_path(subject_id: str, timepoint_label: str, series: dict[str, Any]) -> Path:
    series_uid = str(series.get("series_instance_uid") or "aptref").replace(".", "_")
    return APT_PROCESSED_ROOT / subject_id / timepoint_label / "apt_reference" / f"{subject_id}_{timepoint_label}_{series_uid}_ref.nii.gz"


def _link_or_copy(src: Path, dst: Path) -> None:
    try:
        dst.symlink_to(src)
    except Exception:
        shutil.copy2(src, dst)


def _convert_apt_series(subject_id: str, timepoint_label: str, series: dict[str, Any]) -> tuple[str | None, str | None]:
    source_dir = Path(series["source_dir"])
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

    output_path = _apt_processed_path(subject_id, timepoint_label, series)
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
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if proc.returncode != 0:
            message = (proc.stderr or proc.stdout or "").strip() or f"APT converter failed with exit code {proc.returncode}"
            return None, message

    return str(output_path), None


def _convert_dicom_series_to_nifti(output_path: Path, source_dir: str) -> tuple[str | None, str | None]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(APT_CONVERTER_PYTHON),
        str(APT_CONVERTER_SCRIPT),
        "--input-dir",
        str(source_dir),
        "--output-img",
        str(output_path),
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        message = (proc.stderr or proc.stdout or "").strip() or f"DICOM conversion failed with exit code {proc.returncode}"
        return None, message
    return str(output_path), None


def _reference_t1ce_path(conn, session_pk: int) -> str | None:
    row = conn.execute(
        """
        SELECT processed_path
        FROM sequences
        WHERE session_id = ? AND sequence_type = 'T1ce'
        """,
        (session_pk,),
    ).fetchone()
    if not row or not row["processed_path"]:
        return None
    return str(row["processed_path"])


def apt_fets_transfer_path(subject_id: str, timepoint_label: str, series: dict[str, Any]) -> Path:
    series_uid = str(series.get("series_instance_uid") or "apt").replace(".", "_")
    return APT_PROCESSED_ROOT / subject_id / timepoint_label / "apt_registered" / f"{subject_id}_{timepoint_label}_{series_uid}_aptw_reg.nii.gz"


def _register_apt_to_reference_via_moving(subject_id: str, timepoint_label: str, series: dict[str, Any], apply_path: str, fixed_path: str, moving_for_transform: str) -> tuple[str | None, str | None, str | None]:
    """Rigid Euler3D registration fallback — kept for diagnostic use only."""
    output_path = _apt_registered_path(subject_id, timepoint_label, series)
    transform_path = _apt_transform_path(subject_id, timepoint_label, series)
    cmd = [
        str(APT_CONVERTER_PYTHON),
        str(APT_REGISTER_SCRIPT),
        "--strategy", "registration",
        "--fixed", str(fixed_path),
        "--moving", str(moving_for_transform),
        "--apply-moving", str(apply_path),
        "--output-img", str(output_path),
        "--output-transform", str(transform_path),
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        message = (proc.stderr or proc.stdout or "").strip() or f"APT registration failed with exit code {proc.returncode}"
        return None, None, message
    return str(output_path), str(transform_path), None


def _choose_apt_reference_series(exam: dict[str, Any], apt_series: dict[str, Any]) -> dict[str, Any] | None:
    candidates = []
    apt_key = _seq_key(apt_series)
    for series in exam.get("series", []):
        if series.get("class_label") != "apt":
            continue
        if _seq_key(series) == apt_key:
            continue
        desc = f"{series.get('series_description','')} {series.get('protocol_name','')}".upper()
        image_type = " ".join(str(x).upper() for x in (series.get("image_type") or []))
        score = 100
        if "RIF" in desc:
            score -= 20
        if "MDC" in desc or "FFE" in image_type:
            score -= 10
        if "IR" in image_type:
            score -= 5
        score -= min(int(series.get("n_files") or 0), 50)
        candidates.append((score, series))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _upsert_sequence(conn, session_pk: int, subject_id: str, timepoint_label: str, series: dict[str, Any], label: str, exam: dict[str, Any] | None = None) -> str:
    sequence_type, contrast_agent, metadata_json = _sequence_payload(series, label)
    processed_path = None
    if label == "apt":
        native_path, apt_error = _convert_apt_series(subject_id, timepoint_label, series)
        # Registration into FeTS space happens at FeTS finalize time when T1ce
        # processed_path (FeTS NIfTI) is available.  Store native for now.
        processed_path = native_path
        metadata = json.loads(metadata_json)
        metadata["apt_native_path"] = native_path
        metadata["apt_registered_path"] = None
        metadata["apt_resample_strategy"] = "pending_fets_transfer"
        metadata["apt_conversion_error"] = apt_error
        metadata["apt_registration_error"] = None
        metadata_json = json.dumps(metadata, ensure_ascii=True)
    existing = conn.execute(
        "SELECT id, processed_path FROM sequences WHERE session_id = ? AND sequence_type = ?",
        (session_pk, sequence_type),
    ).fetchone()
    if label != "apt" and existing and existing["processed_path"]:
        processed_path = existing["processed_path"]
    action = "updated" if existing else "inserted"
    conn.execute(
        """
        INSERT OR IGNORE INTO sequences (
            session_id, sequence_type, contrast_agent, raw_path, processed_path,
            shape_x, shape_y, shape_z, spacing_x, spacing_y, spacing_z,
            display_label, import_class, source_series_uid, metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_pk, sequence_type, contrast_agent, series["source_dir"], processed_path,
            None, None, None,
            series.get("resolution_x"), series.get("resolution_y"), series.get("resolution_z"),
            series.get("series_description"), label, series.get("series_instance_uid"), metadata_json,
        ),
    )
    conn.execute(
        """
        UPDATE sequences
        SET contrast_agent = ?, raw_path = ?, processed_path = ?, spacing_x = ?, spacing_y = ?, spacing_z = ?,
            display_label = ?, import_class = ?, source_series_uid = ?, metadata_json = ?
        WHERE session_id = ? AND sequence_type = ?
        """,
        (
            contrast_agent, series["source_dir"], processed_path,
            series.get("resolution_x"), series.get("resolution_y"), series.get("resolution_z"),
            series.get("series_description"), label, series.get("series_instance_uid"), metadata_json,
            session_pk, sequence_type,
        ),
    )
    return action


def commit_import_selection(
    root_path: str,
    exam_keys: list[str],
    include_series: dict[str, bool],
    core_choice: dict[str, str],
) -> dict[str, Any]:
    if not exam_keys:
        raise HTTPException(400, "No exams selected")

    scan = scan_dicom_root(root_path)
    exam_map = {exam["exam_key"]: exam for exam in scan["exams"]}
    selected_exams = [exam_map[key] for key in exam_keys if key in exam_map]
    if not selected_exams:
        raise HTTPException(400, "Selected exams are no longer present in scan root")

    result = {
        "dataset": DATASET_NAME,
        "root_path": root_path,
        "subjects": 0,
        "sessions": 0,
        "sequences_inserted": 0,
        "sequences_updated": 0,
        "sequences_skipped": 0,
        "imported_exam_keys": [],
        "imported_sessions": [],
        "skipped": [],
    }
    seen_subjects: set[int] = set()
    seen_sessions: set[int] = set()

    with db() as conn:
        for exam in selected_exams:
            subject_pk = _upsert_subject(conn, exam)
            session_pk = _upsert_session(conn, subject_pk, exam)
            seen_subjects.add(subject_pk)
            seen_sessions.add(session_pk)

            imported_any = False
            imported_core = False
            imported_extra_sequence_types: set[str] = set()
            for label, series in _selected_core(exam, core_choice).items():
                if not series:
                    result["sequences_skipped"] += 1
                    result["skipped"].append({"exam_key": exam["exam_key"], "class_label": label, "reason": "missing_core"})
                    continue
                series_key = _seq_key(series)
                if include_series.get(series_key) is False:
                    result["sequences_skipped"] += 1
                    result["skipped"].append({"exam_key": exam["exam_key"], "class_label": label, "reason": "deselected"})
                    continue
                action = _upsert_sequence(conn, session_pk, exam["patient_id"], exam["timepoint_label"], series, label, exam)
                result[f"sequences_{action}"] += 1
                imported_any = True
                imported_core = True

            for series in exam["series"]:
                series_key = _seq_key(series)
                if include_series.get(series_key) is not True:
                    continue
                if series["class_label"] in REQUIRED_CLASSES:
                    continue
                if series["class_label"] not in CLASS_TO_SEQUENCE:
                    result["sequences_skipped"] += 1
                    result["skipped"].append({"exam_key": exam["exam_key"], "class_label": series["class_label"], "reason": "unsupported_extra"})
                    continue
                sequence_type, _ = CLASS_TO_SEQUENCE.get(series["class_label"], ("OTHER", 0))
                if sequence_type in imported_extra_sequence_types:
                    result["sequences_skipped"] += 1
                    result["skipped"].append({
                        "exam_key": exam["exam_key"],
                        "class_label": series["class_label"],
                        "reason": f"duplicate_extra_sequence_type:{sequence_type}",
                    })
                    continue
                action = _upsert_sequence(conn, session_pk, exam["patient_id"], exam["timepoint_label"], series, series["class_label"], exam)
                result[f"sequences_{action}"] += 1
                imported_extra_sequence_types.add(sequence_type)
                imported_any = True

            if imported_any:
                raw_dir = "/".join([exam["patient_folder"], exam["study_folder"]])
                ensure_state(
                    exam["patient_id"],
                    exam["timepoint_label"],
                    session_id=session_pk,
                    dataset=DATASET_NAME,
                )
                update_step(
                    exam["patient_id"],
                    exam["timepoint_label"],
                    "import_dicom",
                    status="done",
                    output_path=raw_dir,
                    error_message=None,
                    started=True,
                    finished=True,
                    session_id=session_pk,
                    dataset=DATASET_NAME,
                )
                missing_core = [label for label, series in _selected_core(exam, core_choice).items() if not series]
                if missing_core:
                    update_step(
                        exam["patient_id"],
                        exam["timepoint_label"],
                        "select_core_sequences",
                        status="failed",
                        output_path=raw_dir,
                        error_message="Missing core sequences: " + ", ".join(missing_core),
                        started=True,
                        finished=True,
                        session_id=session_pk,
                        dataset=DATASET_NAME,
                    )
                else:
                    update_step(
                        exam["patient_id"],
                        exam["timepoint_label"],
                        "select_core_sequences",
                        status="done",
                        output_path=raw_dir,
                        error_message=None,
                        started=True,
                        finished=True,
                        session_id=session_pk,
                        dataset=DATASET_NAME,
                    )
                if imported_core:
                    reset_downstream_steps(
                        exam["patient_id"],
                        exam["timepoint_label"],
                        [
                            "initial_validation",
                            "nifti_conversion",
                            "brain_extraction",
                            "tumor_segmentation",
                        ],
                    )
                result["imported_exam_keys"].append(exam["exam_key"])
                result["imported_sessions"].append({
                    "exam_key": exam["exam_key"],
                    "patient_id": exam["patient_id"],
                    "timepoint_label": exam["timepoint_label"],
                    "session_id": session_pk,
                    "subject_pk": subject_pk,
                })

    result["subjects"] = len(seen_subjects)
    result["sessions"] = len(seen_sessions)
    return result
