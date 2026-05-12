import csv
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pydicom
from fastapi import HTTPException
from pydicom.errors import InvalidDicomError

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ALLOWED_SCAN_ROOTS = [
    Path("/mnt/dati"),
    Path("/home/irst"),
]

DEFAULT_IMPORT_ROOTS = [
    Path("/mnt/dati/irst_data/irst_dicom_raw/DICOM GBM"),
    Path("/mnt/dati/irst_data/irst_dicom_raw"),
    Path("/mnt/dati"),
]

RULES_CANDIDATES = [
    Path("/mnt/dati/irst/fets/data/series_rules.csv"),
    PROJECT_ROOT / "app" / "data" / "series_rules.csv",
]

REQUIRED_CLASSES = ("t1n", "t1c", "t2w", "t2f")
SECONDARY_CAPTURE_UID = "1.2.840.10008.5.1.4.1.1.7"

SKIP_FILENAMES = {"DICOMDIR"}
SKIP_EXTENSIONS = {
    ".txt", ".csv", ".json", ".xml", ".jpg", ".jpeg", ".png", ".pdf", ".zip",
    ".ini", ".yaml", ".yml", ".db", ".sqlite",
}
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class Rule:
    priority: int
    cls: str
    field: str
    exact: str


def _norm_text(value: str) -> str:
    if not value:
        return ""
    value = str(value).strip().upper()
    value = value.replace("_", " ").replace("-", " ").replace("/", " ")
    value = re.sub(r"[^A-Z0-9 ]+", " ", value)
    return _SPACE_RE.sub(" ", value).strip()


def _resolve_rules_path() -> Path | None:
    for candidate in RULES_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def _load_rules() -> list[Rule]:
    path = _resolve_rules_path()
    if path is None:
        return []

    rules: list[Rule] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for i, row in enumerate(reader, start=1):
            cls = (row.get("class") or "").strip()
            exact = (row.get("exact") or "").strip()
            if not cls or not exact:
                continue
            try:
                priority = int((row.get("priority") or "").strip() or i)
            except ValueError:
                priority = i
            rules.append(
                Rule(
                    priority=priority,
                    cls=cls,
                    field=(row.get("field") or "Any").strip() or "Any",
                    exact=exact,
                )
            )
    rules.sort(key=lambda rule: rule.priority)
    return rules


RULES = _load_rules()


def list_scan_roots() -> list[str]:
    roots: list[str] = []
    for path in DEFAULT_IMPORT_ROOTS:
        if path.exists():
            roots.append(str(path))
    return roots


def _resolve_scan_root(root_path: str) -> Path:
    if not root_path:
        raise HTTPException(400, "root_path is required")

    resolved = Path(root_path).expanduser().resolve()
    if not resolved.exists():
        raise HTTPException(404, f"Scan root not found: {root_path}")
    if not resolved.is_dir():
        raise HTTPException(400, f"Scan root is not a directory: {root_path}")
    if not any(str(resolved).startswith(str(base)) for base in ALLOWED_SCAN_ROOTS):
        raise HTTPException(403, "Scan root outside allowed paths")
    return resolved


def _is_candidate_file(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.name.upper() in SKIP_FILENAMES:
        return False
    if path.suffix.lower() in SKIP_EXTENSIONS:
        return False
    return True


def _read_anchor(path: Path):
    try:
        return pydicom.dcmread(str(path), stop_before_pixels=True, force=True)
    except (InvalidDicomError, OSError):
        return None
    except Exception:
        return None


def _find_series_dirs(root: Path) -> list[tuple[Path, Any, int]]:
    series_dirs: list[tuple[Path, Any, int]] = []
    for dirpath, _, filenames in os.walk(root):
        directory = Path(dirpath)
        files = [directory / name for name in filenames if _is_candidate_file(directory / name)]
        if not files:
            continue

        anchor_ds = None
        anchor_file = None
        for file_path in sorted(files)[:5]:
            anchor_ds = _read_anchor(file_path)
            if anchor_ds is not None:
                anchor_file = file_path
                break
        if anchor_ds is None or anchor_file is None:
            continue

        series_dirs.append((directory, anchor_ds, len(files)))
    return series_dirs


def _dicom_str(ds: Any, field: str) -> str:
    value = getattr(ds, field, "")
    if value is None:
        return ""
    return str(value).strip()


def _image_type_values(ds: Any) -> list[str]:
    value = getattr(ds, "ImageType", None)
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    raw = str(value).strip().strip("[]")
    return [part.strip().strip("'") for part in raw.split(",") if part.strip()]


def _looks_like_secondary_apt(series_desc: str, protocol_name: str, image_type_values: list[str], sop_class_uid: str) -> bool:
    joined = _norm_text(f"{series_desc} {protocol_name}")
    image_type = " ".join(_norm_text(item) for item in image_type_values)
    if sop_class_uid == SECONDARY_CAPTURE_UID:
        return True
    if "SCREEN CAPTURE" in joined:
        return True
    if "DERIVED SECONDARY" in image_type:
        return True
    if "_SC" in series_desc.upper() or "_SC" in protocol_name.upper():
        return True
    return False


def _series_instance_type_summary(series_dir: Path) -> tuple[dict[str, int], int | None, str | None]:
    counts: dict[str, int] = Counter()
    preferred_count = None
    selector = None
    for file_path in sorted(series_dir.iterdir()):
        if not _is_candidate_file(file_path):
            continue
        ds = _read_anchor(file_path)
        if ds is None:
            continue
        image_type_values = _image_type_values(ds)
        signature = "|".join(image_type_values) or "unknown"
        counts[signature] += 1
    for signature, count in counts.items():
        signature_norm = _norm_text(signature)
        if "APTW" in signature_norm:
            preferred_count = count
            selector = "ImageType contains APTW"
            break
    return dict(counts), preferred_count, selector


def _special_class_label(ds: Any, class_label: str, series_desc: str, protocol_name: str) -> str:
    image_type_values = _image_type_values(ds)
    sop_class_uid = _dicom_str(ds, "SOPClassUID")
    joined = _norm_text(f"{series_desc} {protocol_name}")
    if class_label == "apt" or "APT" in joined or "APTW" in joined:
        if _looks_like_secondary_apt(series_desc, protocol_name, image_type_values, sop_class_uid):
            return "other"
        return "apt"
    it_upper = [v.upper() for v in image_type_values]
    if "KTRANS" in it_upper or "KTRANS" in joined:
        return "ktrans"
    if any(k in it_upper for k in ("RCBVCORR", "RCBV")) or re.search(r"\bNRCBV\b|\bRCBV\b", joined):
        return "nrcbv"
    return class_label


def _match_rule(series_desc: str, protocol_name: str) -> tuple[str, int | None, str | None]:
    sd_raw = (series_desc or "").strip()
    pn_raw = (protocol_name or "").strip()
    sd_norm = _norm_text(sd_raw)
    pn_norm = _norm_text(pn_raw)

    for rule in RULES:
        exact_raw = (rule.exact or "").strip()
        exact_norm = _norm_text(exact_raw)
        field = (rule.field or "Any").lower()

        def match_value(raw_value: str, norm_value: str) -> bool:
            return raw_value == exact_raw or (exact_norm and norm_value == exact_norm)

        if field == "seriesdescription":
            if match_value(sd_raw, sd_norm):
                return rule.cls, rule.priority, "SeriesDescription"
        elif field == "protocolname":
            if match_value(pn_raw, pn_norm):
                return rule.cls, rule.priority, "ProtocolName"
        else:
            if match_value(sd_raw, sd_norm):
                return rule.cls, rule.priority, "SeriesDescription"
            if match_value(pn_raw, pn_norm):
                return rule.cls, rule.priority, "ProtocolName"

    joined = f"{sd_norm} {pn_norm}".strip()
    if "FLAIR" in joined:
        return "t2f", None, "fallback"
    if "T2W" in joined or re.search(r"\bT2\b", joined):
        return "t2w", None, "fallback"
    if "KTRANS" in joined:
        return "ktrans", None, "fallback"
    if re.search(r"\bNRCBV\b|\bRCBV\b", joined) or "PERFUSIONE" in joined:
        return "nrcbv", None, "fallback"
    return "other", None, None


def _resolution(ds: Any) -> tuple[float | None, float | None, float | None]:
    try:
        pixel_spacing = getattr(ds, "PixelSpacing", None) or []
        slice_thickness = getattr(ds, "SliceThickness", None)
        x = float(pixel_spacing[0]) if len(pixel_spacing) > 0 else None
        y = float(pixel_spacing[1]) if len(pixel_spacing) > 1 else None
        z = float(slice_thickness) if slice_thickness not in (None, "") else None
        return x, y, z
    except Exception:
        return None, None, None


def _person_name_parts(value: Any) -> tuple[str, str, str]:
    if not value:
        return "", "", ""
    try:
        family = str(getattr(value, "family_name", "") or "").strip()
        given = str(getattr(value, "given_name", "") or "").strip()
    except Exception:
        family = ""
        given = ""
    raw = str(value).strip()
    if not family and not given:
        parts = raw.split("^")
        family = parts[0].strip() if parts else ""
        given = parts[1].strip() if len(parts) > 1 else ""
    display = " ".join(part for part in (given, family) if part).strip() or raw
    return display, given, family


def _series_record(series_dir: Path, ds: Any, n_files: int) -> dict[str, Any]:
    study_dir = series_dir.parent
    patient_dir = study_dir.parent
    series_description = _dicom_str(ds, "SeriesDescription")
    protocol_name = _dicom_str(ds, "ProtocolName")
    class_label, priority, matched_field = _match_rule(series_description, protocol_name)
    class_label = _special_class_label(ds, class_label, series_description, protocol_name)
    res_x, res_y, res_z = _resolution(ds)
    patient_name, patient_given_name, patient_family_name = _person_name_parts(getattr(ds, "PatientName", ""))
    image_type_values = _image_type_values(ds)
    instance_type_summary = None
    preferred_instance_count = None
    instance_selector = None
    if class_label == "apt":
        instance_type_summary, preferred_instance_count, instance_selector = _series_instance_type_summary(series_dir)
        if preferred_instance_count is None:
            # No APTW instances found → reference/anatomical series named "APT" (e.g. RIF APT), not a CEST map
            class_label = "other"

    return {
        "patient_folder": patient_dir.name,
        "study_folder": study_dir.name,
        "series_folder": series_dir.name,
        "patient_id": _dicom_str(ds, "PatientID") or patient_dir.name,
        "patient_name": patient_name,
        "patient_given_name": patient_given_name,
        "patient_family_name": patient_family_name,
        "patient_birth_date": _dicom_str(ds, "PatientBirthDate"),
        "patient_sex": _dicom_str(ds, "PatientSex"),
        "study_instance_uid": _dicom_str(ds, "StudyInstanceUID"),
        "series_instance_uid": _dicom_str(ds, "SeriesInstanceUID"),
        "study_date": _dicom_str(ds, "StudyDate"),
        "study_time": _dicom_str(ds, "StudyTime"),
        "study_description": _dicom_str(ds, "StudyDescription"),
        "series_description": series_description,
        "protocol_name": protocol_name,
        "image_type": image_type_values,
        "sop_class_uid": _dicom_str(ds, "SOPClassUID"),
        "modality": _dicom_str(ds, "Modality"),
        "series_number": _dicom_str(ds, "SeriesNumber"),
        "manufacturer": _dicom_str(ds, "Manufacturer"),
        "model_name": _dicom_str(ds, "ManufacturerModelName"),
        "magnetic_field_strength": _dicom_str(ds, "MagneticFieldStrength"),
        "source_dir": str(series_dir),
        "n_files": n_files,
        "class_label": class_label,
        "match_priority": priority,
        "matched_field": matched_field,
        "resolution_x": res_x,
        "resolution_y": res_y,
        "resolution_z": res_z,
        "instance_type_summary": instance_type_summary,
        "preferred_instance_count": preferred_instance_count,
        "instance_selector": instance_selector,
    }


def _series_sort_key(series: dict[str, Any]) -> tuple[int, int, int, str]:
    priority = series["match_priority"] if series["match_priority"] is not None else 999_999
    desc = f"{series['series_description']} {series['protocol_name']}".lower()
    penalty = 0
    for token in ("mpr", "smartbrain", "default", "roi"):
        if token in desc:
            penalty += 500
    modality_penalty = 0 if series["modality"] == "MR" else 1_000
    try:
        series_number = int(str(series["series_number"] or "0"))
    except ValueError:
        series_number = 0
    return (
        priority + penalty + modality_penalty,
        -int(series["n_files"] or 0),
        series_number,
        series["source_dir"],
    )


def _selection_note(series: dict[str, Any]) -> str:
    desc = f"{series['series_description']} {series['protocol_name']}".lower()
    notes: list[str] = []
    if series.get("match_priority") is not None:
        notes.append(f"dict#{series['match_priority']}")
    if series["n_files"]:
        notes.append(f"{series['n_files']} files")
    if any(token in desc for token in (" cor", "_cor", " coronal")):
        notes.append("coronal recon")
    if any(token in desc for token in (" sag", "_sag", " sagittal")):
        notes.append("sagittal recon")
    if any(token in desc for token in ("mpr", "smartbrain", "roi", "default")):
        notes.append("derived/secondary")
    if series.get("class_label") == "apt":
        preferred = series.get("preferred_instance_count")
        total = series.get("n_files")
        selector = series.get("instance_selector")
        if preferred and total and preferred < total and selector:
            notes.append(f"{selector} ({preferred}/{total})")
    if not notes:
        notes.append("fallback ranking")
    return " · ".join(notes)


def _build_exam(study_key: tuple[str, str], series_items: list[dict[str, Any]], timepoint_label: str) -> dict[str, Any]:
    patient_id, study_instance_uid = study_key
    series_sorted = sorted(series_items, key=_series_sort_key)
    core_candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    extras: list[dict[str, Any]] = []
    others: list[dict[str, Any]] = []

    for series in series_sorted:
        label = series["class_label"]
        if label in REQUIRED_CLASSES:
            core_candidates[label].append(series)
        elif label == "other":
            others.append(series)
        else:
            extras.append(series)

    selected_core: dict[str, dict[str, Any]] = {}
    missing_classes: list[str] = []
    conflict_classes: list[str] = []
    for label in REQUIRED_CLASSES:
        candidates = core_candidates.get(label, [])
        if not candidates:
            missing_classes.append(label)
            continue
        selected_core[label] = candidates[0]
        if len(candidates) > 1:
            conflict_classes.append(label)

    selected_series_uids = {
        item["series_instance_uid"]
        for item in selected_core.values()
        if item.get("series_instance_uid")
    }
    for series in series_sorted:
        series["selected_for_core"] = series.get("series_instance_uid") in selected_series_uids
        series["is_extra"] = series["class_label"] not in REQUIRED_CLASSES and series["class_label"] != "other"

    if missing_classes:
        status = "incomplete"
    elif conflict_classes:
        status = "review"
    else:
        status = "ready"

    first = series_sorted[0]
    return {
        "exam_key": f"{patient_id}|{study_instance_uid or first['study_folder']}",
        "patient_id": patient_id,
        "patient_name": first["patient_name"],
        "patient_given_name": first["patient_given_name"],
        "patient_family_name": first["patient_family_name"],
        "patient_birth_date": first["patient_birth_date"],
        "patient_sex": first["patient_sex"],
        "patient_folder": first["patient_folder"],
        "study_folder": first["study_folder"],
        "study_instance_uid": study_instance_uid,
        "study_date": first["study_date"],
        "study_time": first["study_time"],
        "study_description": first["study_description"],
        "timepoint_label": timepoint_label,
        "status": status,
        "missing_classes": missing_classes,
        "conflict_classes": conflict_classes,
        "extra_series_count": len(extras),
        "other_series_count": len(others),
        "series_count": len(series_sorted),
        "core_selection": {
            label: selected_core.get(label)
            for label in REQUIRED_CLASSES
        },
        "core_candidates": {
            label: [
                {
                    **candidate,
                    "selection_note": _selection_note(candidate),
                    "candidate_rank": idx + 1,
                }
                for idx, candidate in enumerate(core_candidates.get(label, []))
            ]
            for label in REQUIRED_CLASSES
        },
        "series": series_sorted,
    }


def scan_dicom_root(root_path: str, limit_studies: int | None = None) -> dict[str, Any]:
    root = _resolve_scan_root(root_path)
    raw_series = [_series_record(series_dir, ds, n_files) for series_dir, ds, n_files in _find_series_dirs(root)]
    if not raw_series:
        return {
            "root_path": str(root),
            "rules_path": str(_resolve_rules_path()) if _resolve_rules_path() else None,
            "summary": {
                "total_series": 0,
                "total_exams": 0,
                "ready_exams": 0,
                "review_exams": 0,
                "incomplete_exams": 0,
                "extra_series": 0,
                "other_series": 0,
            },
            "exams": [],
        }

    # Raggruppa per (patient_id, cartella_studio_assoluta) per unire serie dello stesso
    # studio che hanno StudyInstanceUID diversi (anomalia presente in alcuni dataset IRST).
    _merge_key_to_canonical: dict[tuple[str, str], tuple[str, str]] = {}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for series in raw_series:
        patient_id = series["patient_id"]
        study_uid = series["study_instance_uid"] or series["study_folder"]
        abs_study_dir = str(Path(series["source_dir"]).parent)
        merge_key = (patient_id, abs_study_dir)
        if merge_key not in _merge_key_to_canonical:
            _merge_key_to_canonical[merge_key] = (patient_id, study_uid)
        grouped[_merge_key_to_canonical[merge_key]].append(series)

    patient_studies: dict[str, list[tuple[tuple[str, str], list[dict[str, Any]]]]] = defaultdict(list)
    for key, items in grouped.items():
        patient_studies[key[0]].append((key, items))

    exams: list[dict[str, Any]] = []
    for patient_id in sorted(patient_studies):
        studies = sorted(
            patient_studies[patient_id],
            key=lambda item: (
                item[1][0]["study_date"] or "",
                item[1][0]["study_time"] or "",
                item[1][0]["study_folder"],
            ),
        )
        for index, (study_key, items) in enumerate(studies, start=1):
            exams.append(_build_exam(study_key, items, f"timepoint_{index:03d}"))

    exams.sort(key=lambda exam: (exam["patient_id"], exam["study_date"], exam["study_time"], exam["study_folder"]))
    if limit_studies:
        exams = exams[:limit_studies]

    status_counter = Counter(exam["status"] for exam in exams)
    extra_series = sum(exam["extra_series_count"] for exam in exams)
    other_series = sum(exam["other_series_count"] for exam in exams)

    return {
        "root_path": str(root),
        "rules_path": str(_resolve_rules_path()) if _resolve_rules_path() else None,
        "summary": {
            "total_series": sum(exam["series_count"] for exam in exams),
            "total_exams": len(exams),
            "ready_exams": status_counter.get("ready", 0),
            "review_exams": status_counter.get("review", 0),
            "incomplete_exams": status_counter.get("incomplete", 0),
            "extra_series": extra_series,
            "other_series": other_series,
        },
        "exams": exams,
    }
