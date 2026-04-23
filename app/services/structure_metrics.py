from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

import nibabel as nib
import numpy as np
from fastapi import HTTPException

from app.db import db, rows_as_dicts

SEQUENCE_ORDER = {
    "T1ce": 0,
    "CT1": 0,
    "T1": 1,
    "T2": 2,
    "FLAIR": 3,
    "APT": 4,
}

ALLOWED_TIMELINE_SEQUENCE_TYPES = {"T1ce", "CT1", "T1", "T2", "FLAIR", "APT"}
EXPLICIT_SOURCES = ("radiological", "computed")

_METRIC_JOB_LOCK = threading.Lock()
_METRIC_WORKER: threading.Thread | None = None
_METRIC_ACTIVE_JOB_ID: int | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _is_nifti_path(path: str | None) -> bool:
    if not path:
        return False
    return path.endswith(".nii") or path.endswith(".nii.gz")


def _path_to_iso(raw: str | None) -> str | None:
    if not raw:
        return None
    text = str(raw).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text or None


def _safe_date(raw: str | None) -> date | None:
    iso = _path_to_iso(raw)
    if not iso:
        return None
    try:
        return date.fromisoformat(iso)
    except ValueError:
        return None


def _event_date(anchor_date: date | None, days_from_baseline: int | None) -> str | None:
    if isinstance(days_from_baseline, str):
        try:
            days_from_baseline = int(days_from_baseline)
        except ValueError:
            days_from_baseline = None
    if anchor_date is None or days_from_baseline is None:
        return None
    return (anchor_date + timedelta(days=int(days_from_baseline))).isoformat()


def _normalize_space(img: nib.spatialimages.SpatialImage) -> tuple[tuple[int, ...], np.ndarray]:
    return tuple(int(v) for v in img.shape[:3]), np.asarray(img.affine, dtype=float)


def _same_space(
    lhs: nib.spatialimages.SpatialImage,
    rhs: nib.spatialimages.SpatialImage,
    *,
    atol: float = 1e-3,
) -> bool:
    lhs_shape, lhs_affine = _normalize_space(lhs)
    rhs_shape, rhs_affine = _normalize_space(rhs)
    if lhs_shape != rhs_shape:
        return False
    return bool(np.allclose(lhs_affine, rhs_affine, atol=atol, rtol=0.0))


@lru_cache(maxsize=256)
def _load_nifti(path: str):
    img = nib.load(path)
    data = np.asarray(img.get_fdata(dtype=np.float32))
    return img, data


def _voxel_volume_ml(img: nib.spatialimages.SpatialImage) -> float:
    zooms = img.header.get_zooms()
    if len(zooms) < 3:
        return 0.0
    return float(zooms[0] * zooms[1] * zooms[2]) / 1000.0


def _mask_values(mask_row: dict, mask_data: np.ndarray) -> np.ndarray:
    label_code = mask_row.get("label_code")
    if label_code is None:
        return mask_data > 0
    return mask_data == int(label_code)


def _sort_sequence_types(values: set[str]) -> list[str]:
    return sorted(values, key=lambda item: (SEQUENCE_ORDER.get(item, 99), item))


def _pick_structure(candidates: list[dict], structure_source: str) -> dict | None:
    if structure_source == "radiological":
        preferred = [row for row in candidates if row["source"] == "radiological"]
    elif structure_source == "computed":
        preferred = [row for row in candidates if row["source"] == "computed"]
    else:
        preferred = [row for row in candidates if row["source"] == "radiological"] or candidates
    if not preferred:
        return None
    preferred.sort(
        key=lambda row: (
            0 if row["source"] == "radiological" else 1,
            row.get("created_at") or "",
            row.get("id") or 0,
        )
    )
    return preferred[-1]


def _session_rows(conn, patient_id: int | None = None) -> list[dict]:
    query = """
        SELECT *
        FROM sessions
        {where}
        ORDER BY
            CASE WHEN study_date IS NULL OR study_date = '' THEN 1 ELSE 0 END,
            study_date,
            COALESCE(days_from_baseline, 999999),
            session_label
    """
    params: list[int] = []
    where = ""
    if patient_id is not None:
        where = "WHERE subject_id = ?"
        params.append(patient_id)
    rows = conn.execute(query.format(where=where), params).fetchall()
    return rows_as_dicts(rows)


def _sequence_rows(conn, session_ids: list[int]) -> list[dict]:
    if not session_ids:
        return []
    query = """
        SELECT id, session_id, sequence_type, raw_path, processed_path, display_label
        FROM sequences
        WHERE session_id IN ({})
    """.format(",".join("?" for _ in session_ids))
    return rows_as_dicts(conn.execute(query, session_ids).fetchall())


def _structure_rows(conn, session_ids: list[int]) -> list[dict]:
    if not session_ids:
        return []
    placeholders = ",".join("?" for _ in session_ids)
    return rows_as_dicts(
        conn.execute(
            f"""
            SELECT id, session_id, label, label_code, mask_path, volume_ml, created_at, 'computed' AS source
            FROM computed_structures
            WHERE session_id IN ({placeholders})
            UNION ALL
            SELECT id, session_id, label, label_code, mask_path, volume_ml, created_at, 'radiological' AS source
            FROM radiological_structures
            WHERE session_id IN ({placeholders})
            """,
            session_ids + session_ids,
        ).fetchall()
    )


def _sequence_index(rows: list[dict]) -> tuple[dict[int, dict[str, dict]], set[str]]:
    by_session: dict[int, dict[str, dict]] = {}
    available_sequence_types: set[str] = set()
    for row in rows:
        path = row["processed_path"] if _is_nifti_path(row.get("processed_path")) else row.get("raw_path")
        if not _is_nifti_path(path):
            continue
        sequence_type = row.get("sequence_type")
        if not sequence_type or sequence_type not in ALLOWED_TIMELINE_SEQUENCE_TYPES:
            continue
        row["resolved_path"] = path
        by_session.setdefault(int(row["session_id"]), {})[sequence_type] = row
        available_sequence_types.add(sequence_type)
    return by_session, available_sequence_types


def _structure_index(rows: list[dict]) -> tuple[dict[int, list[dict]], set[str]]:
    by_session: dict[int, list[dict]] = {}
    available_labels: set[str] = set()
    for row in rows:
        if not _is_nifti_path(row.get("mask_path")):
            continue
        by_session.setdefault(int(row["session_id"]), []).append(row)
        if row.get("label"):
            available_labels.add(row["label"])
    return by_session, available_labels


@dataclass
class MetricTask:
    session_id: int
    structure_source: str
    label: str
    sequence_type: str
    sequence_id: int | None
    sequence_path: str
    mask_path: str
    label_code: int | None
    volume_ml: float | None


def _build_metric_tasks(
    sessions: list[dict],
    sequence_by_session: dict[int, dict[str, dict]],
    structs_by_session: dict[int, list[dict]],
    *,
    force: bool = False,
    existing_keys: set[tuple[int, str, str, str]] | None = None,
) -> list[MetricTask]:
    tasks: list[MetricTask] = []
    existing = existing_keys or set()
    for session in sessions:
        session_id = int(session["id"])
        seq_map = sequence_by_session.get(session_id, {})
        if not seq_map:
            continue
        structs = structs_by_session.get(session_id, [])
        if not structs:
            continue
        for source in EXPLICIT_SOURCES:
            labels = sorted({row["label"] for row in structs if row.get("label") and row["source"] == source})
            for label in labels:
                chosen_struct = _pick_structure([row for row in structs if row.get("label") == label], source)
                if not chosen_struct:
                    continue
                for sequence_type, seq_row in seq_map.items():
                    task_key = (session_id, source, label, sequence_type)
                    if not force and task_key in existing:
                        continue
                    tasks.append(
                        MetricTask(
                            session_id=session_id,
                            structure_source=source,
                            label=label,
                            sequence_type=sequence_type,
                            sequence_id=seq_row.get("id"),
                            sequence_path=seq_row["resolved_path"],
                            mask_path=chosen_struct["mask_path"],
                            label_code=chosen_struct.get("label_code"),
                            volume_ml=chosen_struct.get("volume_ml"),
                        )
                    )
    return tasks


def _existing_cache_keys(conn, session_ids: list[int]) -> set[tuple[int, str, str, str]]:
    if not session_ids:
        return set()
    query = """
        SELECT session_id, structure_source, label, sequence_type
        FROM signal_metric_cache
        WHERE session_id IN ({})
    """.format(",".join("?" for _ in session_ids))
    rows = conn.execute(query, session_ids).fetchall()
    return {
        (int(row["session_id"]), str(row["structure_source"]), str(row["label"]), str(row["sequence_type"]))
        for row in rows
    }


def _compute_metric_row(task: MetricTask) -> dict:
    entry = {
        "session_id": task.session_id,
        "structure_source": task.structure_source,
        "label": task.label,
        "label_code": task.label_code,
        "sequence_type": task.sequence_type,
        "sequence_id": task.sequence_id,
        "sequence_path": task.sequence_path,
        "mask_path": task.mask_path,
        "volume_ml": task.volume_ml,
        "n_voxels": None,
        "median": None,
        "q1": None,
        "q3": None,
        "min": None,
        "max": None,
        "signal_error": None,
        "computed_at": _utc_now(),
    }
    try:
        seq_img, seq_data = _load_nifti(task.sequence_path)
        mask_img, mask_data = _load_nifti(task.mask_path)
        if not _same_space(seq_img, mask_img):
            raise ValueError("space_mismatch")
        mask_voxels = _mask_values({"label_code": task.label_code}, mask_data)
        values = np.asarray(seq_data[mask_voxels], dtype=np.float32)
        if values.size == 0:
            raise ValueError("empty_mask")
        if entry["volume_ml"] is None:
            entry["volume_ml"] = round(float(np.count_nonzero(mask_voxels)) * _voxel_volume_ml(mask_img), 3)
        entry["n_voxels"] = int(values.size)
        entry["median"] = round(float(np.median(values)), 4)
        entry["q1"] = round(float(np.percentile(values, 25)), 4)
        entry["q3"] = round(float(np.percentile(values, 75)), 4)
        entry["min"] = round(float(np.min(values)), 4)
        entry["max"] = round(float(np.max(values)), 4)
    except Exception as exc:
        entry["signal_error"] = str(exc)
    return entry


def _upsert_metric_row(conn, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO signal_metric_cache (
            session_id,
            structure_source,
            label,
            label_code,
            sequence_type,
            sequence_id,
            sequence_path,
            mask_path,
            volume_ml,
            n_voxels,
            median,
            q1,
            q3,
            min,
            max,
            signal_error,
            computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(session_id, structure_source, label, sequence_type)
        DO UPDATE SET
            label_code = excluded.label_code,
            sequence_id = excluded.sequence_id,
            sequence_path = excluded.sequence_path,
            mask_path = excluded.mask_path,
            volume_ml = excluded.volume_ml,
            n_voxels = excluded.n_voxels,
            median = excluded.median,
            q1 = excluded.q1,
            q3 = excluded.q3,
            min = excluded.min,
            max = excluded.max,
            signal_error = excluded.signal_error,
            computed_at = excluded.computed_at
        """,
        (
            row["session_id"],
            row["structure_source"],
            row["label"],
            row["label_code"],
            row["sequence_type"],
            row["sequence_id"],
            row["sequence_path"],
            row["mask_path"],
            row["volume_ml"],
            row["n_voxels"],
            row["median"],
            row["q1"],
            row["q3"],
            row["min"],
            row["max"],
            row["signal_error"],
            row["computed_at"],
        ),
    )


def _update_metric_job(job_id: int, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = _utc_now()
    assignments = ", ".join(f"{name} = ?" for name in fields)
    params = list(fields.values()) + [job_id]
    with db() as conn:
        conn.execute(f"UPDATE signal_metric_jobs SET {assignments} WHERE id = ?", params)


def _mark_worker_idle(job_id: int) -> None:
    global _METRIC_WORKER, _METRIC_ACTIVE_JOB_ID
    with _METRIC_JOB_LOCK:
        if _METRIC_ACTIVE_JOB_ID == job_id:
            _METRIC_ACTIVE_JOB_ID = None
            _METRIC_WORKER = None


def _run_metric_job(job_id: int) -> None:
    try:
        _update_metric_job(job_id, status="running", started_at=_utc_now(), error_message=None)
        with db() as conn:
            job = conn.execute("SELECT * FROM signal_metric_jobs WHERE id = ?", (job_id,)).fetchone()
            if not job:
                raise RuntimeError("signal metric job not found")
            patient_id = job["patient_id"]
            force = bool(job["force_recompute"])
            sessions = _session_rows(conn, patient_id=patient_id)
            session_ids = [int(item["id"]) for item in sessions]
            sequence_rows = _sequence_rows(conn, session_ids)
            struct_rows = _structure_rows(conn, session_ids)
            sequence_by_session, _ = _sequence_index(sequence_rows)
            structs_by_session, _ = _structure_index(struct_rows)
            existing = set() if force else _existing_cache_keys(conn, session_ids)
            tasks = _build_metric_tasks(
                sessions,
                sequence_by_session,
                structs_by_session,
                force=force,
                existing_keys=existing,
            )
            conn.execute(
                """
                UPDATE signal_metric_jobs
                SET total_tasks = ?, completed_tasks = 0, failed_tasks = 0, updated_at = ?
                WHERE id = ?
                """,
                (len(tasks), _utc_now(), job_id),
            )

        completed = 0
        failed = 0
        for task in tasks:
            row = _compute_metric_row(task)
            with db() as conn:
                _upsert_metric_row(conn, row)
            completed += 1
            if row.get("signal_error"):
                failed += 1
            _update_metric_job(job_id, completed_tasks=completed, failed_tasks=failed)

        _update_metric_job(job_id, status="completed", finished_at=_utc_now(), error_message=None)
    except Exception as exc:
        _update_metric_job(job_id, status="failed", finished_at=_utc_now(), error_message=str(exc))
    finally:
        _mark_worker_idle(job_id)


def _start_metric_job_worker(job_id: int) -> None:
    global _METRIC_WORKER, _METRIC_ACTIVE_JOB_ID
    with _METRIC_JOB_LOCK:
        if _METRIC_WORKER and _METRIC_WORKER.is_alive():
            return
        worker = threading.Thread(target=_run_metric_job, args=(job_id,), daemon=True, name=f"signal-metric-job-{job_id}")
        _METRIC_WORKER = worker
        _METRIC_ACTIVE_JOB_ID = job_id
        worker.start()


def queue_signal_metric_job(patient_id: int | None = None, force: bool = False) -> dict:
    with db() as conn:
        running = conn.execute(
            """
            SELECT *
            FROM signal_metric_jobs
            WHERE status IN ('queued', 'running')
              AND patient_id IS ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (patient_id,),
        ).fetchone()
        if running:
            row = dict(running)
            _start_metric_job_worker(int(row["id"]))
            return row

        conn.execute(
            """
            INSERT INTO signal_metric_jobs (
                scope,
                patient_id,
                status,
                force_recompute,
                requested_at,
                updated_at
            ) VALUES (?, ?, 'queued', ?, ?, ?)
            """,
            ("all_missing" if patient_id is None else "patient_missing", patient_id, int(force), _utc_now(), _utc_now()),
        )
        job_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        row = dict(conn.execute("SELECT * FROM signal_metric_jobs WHERE id = ?", (job_id,)).fetchone())
    _start_metric_job_worker(job_id)
    return row


def latest_signal_metric_job(patient_id: int | None = None) -> dict | None:
    with db() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM signal_metric_jobs
            WHERE patient_id IS ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (patient_id,),
        ).fetchone()
        if not row and patient_id is not None:
            row = conn.execute(
                """
                SELECT *
                FROM signal_metric_jobs
                WHERE patient_id IS NULL
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
    return dict(row) if row else None


def get_signal_metric_job(job_id: int) -> dict:
    with db() as conn:
        row = conn.execute("SELECT * FROM signal_metric_jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Signal metric job not found")
    return dict(row)


def signal_metric_status(patient_id: int | None = None) -> dict:
    latest = latest_signal_metric_job(patient_id=patient_id)
    with db() as conn:
        session_ids = [int(item["id"]) for item in _session_rows(conn, patient_id=patient_id)]
        cached_rows = 0
        if session_ids:
            query = "SELECT COUNT(*) FROM signal_metric_cache WHERE session_id IN ({})".format(",".join("?" for _ in session_ids))
            cached_rows = int(conn.execute(query, session_ids).fetchone()[0])
    return {
        "latest_job": latest,
        "cached_rows": cached_rows,
        "active_job_id": _METRIC_ACTIVE_JOB_ID,
    }


def _timeline_point_from_cache(session: dict, row: dict, structure_source: str) -> dict:
    signal = None
    if row.get("n_voxels") is not None and row.get("median") is not None:
        signal = {
            "n_voxels": int(row["n_voxels"]),
            "median": float(row["median"]),
            "q1": float(row["q1"]),
            "q3": float(row["q3"]),
            "min": float(row["min"]),
            "max": float(row["max"]),
        }
    return {
        "session_id": int(session["id"]),
        "session_label": session["session_label"],
        "study_date": _path_to_iso(session.get("study_date")),
        "days_from_baseline": session.get("days_from_baseline"),
        "source": row["structure_source"] if structure_source == "preferred" else structure_source,
        "sequence_type": row["sequence_type"],
        "sequence_path": row.get("sequence_path"),
        "mask_path": row.get("mask_path"),
        "label": row["label"],
        "label_code": row.get("label_code"),
        "volume_ml": row.get("volume_ml"),
        "signal": signal,
        "signal_error": row.get("signal_error"),
        "computed_at": row.get("computed_at"),
    }


def patient_signal_timeline(
    patient_id: int,
    label: str | None,
    sequence_type: str | None,
    structure_source: str = "preferred",
) -> dict:
    with db() as conn:
        subj = conn.execute("SELECT * FROM subjects WHERE id = ?", (patient_id,)).fetchone()
        if not subj:
            raise HTTPException(404, "Patient not found")

        sessions = _session_rows(conn, patient_id=patient_id)
        if not sessions:
            return {
                "patient": dict(subj),
                "available_labels": [],
                "available_sequence_types": [],
                "available_sources": ["preferred", "radiological", "computed"],
                "selected_label": label,
                "selected_sequence_type": sequence_type,
                "selected_source": structure_source,
                "points": [],
                "clinical_events": [],
                "cache_status": signal_metric_status(patient_id=patient_id),
            }

        session_ids = [int(item["id"]) for item in sessions]
        sequence_rows = _sequence_rows(conn, session_ids)
        struct_rows = _structure_rows(conn, session_ids)
        clinical_events = rows_as_dicts(
            conn.execute(
                """
                SELECT *
                FROM clinical_events
                WHERE subject_id = ?
                ORDER BY COALESCE(event_date, ''), COALESCE(days_from_baseline, 999999), created_at
                """,
                (patient_id,),
            ).fetchall()
        )
        cache_rows = rows_as_dicts(
            conn.execute(
                """
                SELECT *
                FROM signal_metric_cache
                WHERE session_id IN ({})
                """.format(",".join("?" for _ in session_ids)),
                session_ids,
            ).fetchall()
        )

    sequence_by_session, available_sequence_types = _sequence_index(sequence_rows)
    structs_by_session, available_labels = _structure_index(struct_rows)
    available_sequence_types_sorted = _sort_sequence_types(available_sequence_types)
    available_labels_sorted = sorted(available_labels)
    chosen_label = label if label in available_labels_sorted else (available_labels_sorted[0] if available_labels_sorted else None)
    requested_sequence = sequence_type if sequence_type in available_sequence_types_sorted else None
    chosen_sequence = requested_sequence or (
        "APT" if "APT" in available_sequence_types else (available_sequence_types_sorted[0] if available_sequence_types_sorted else None)
    )
    chosen_source = structure_source if structure_source in {"preferred", "radiological", "computed"} else "preferred"

    anchor_dates: list[date] = []
    for session in sessions:
        study_dt = _safe_date(session.get("study_date"))
        days = session.get("days_from_baseline")
        if study_dt is None or days is None:
            continue
        anchor_dates.append(study_dt - timedelta(days=int(days)))
    anchor_date = anchor_dates[0] if anchor_dates else None

    cache_by_key: dict[tuple[int, str, str, str], dict] = {}
    for row in cache_rows:
        cache_by_key[(int(row["session_id"]), row["structure_source"], row["label"], row["sequence_type"])] = row

    points: list[dict] = []
    for session in sessions:
        session_id = int(session["id"])
        if not chosen_label or not chosen_sequence:
            continue
        seq_exists = chosen_sequence in sequence_by_session.get(session_id, {})
        if not seq_exists:
            continue

        resolved_row: dict | None = None
        if chosen_source == "preferred":
            for explicit_source in ("radiological", "computed"):
                row = cache_by_key.get((session_id, explicit_source, chosen_label, chosen_sequence))
                if row:
                    resolved_row = row
                    break
        else:
            resolved_row = cache_by_key.get((session_id, chosen_source, chosen_label, chosen_sequence))

        if resolved_row:
            points.append(_timeline_point_from_cache(session, resolved_row, chosen_source))
            continue

        candidates = [row for row in structs_by_session.get(session_id, []) if row["label"] == chosen_label]
        chosen_struct = _pick_structure(candidates, chosen_source)
        if not chosen_struct:
            continue
        seq_row = sequence_by_session.get(session_id, {}).get(chosen_sequence)
        if not seq_row:
            continue
        points.append(
            {
                "session_id": session_id,
                "session_label": session["session_label"],
                "study_date": _path_to_iso(session.get("study_date")),
                "days_from_baseline": session.get("days_from_baseline"),
                "source": chosen_struct["source"],
                "sequence_type": chosen_sequence,
                "sequence_path": seq_row.get("resolved_path"),
                "mask_path": chosen_struct.get("mask_path"),
                "label": chosen_label,
                "label_code": chosen_struct.get("label_code"),
                "volume_ml": chosen_struct.get("volume_ml"),
                "signal": None,
                "signal_error": "not_precomputed",
                "computed_at": None,
            }
        )

    return {
        "patient": dict(subj),
        "available_labels": available_labels_sorted,
        "available_sequence_types": available_sequence_types_sorted,
        "available_sources": ["preferred", "radiological", "computed"],
        "selected_label": chosen_label,
        "selected_sequence_type": chosen_sequence,
        "selected_source": chosen_source,
        "points": points,
        "clinical_events": [
            {
                **event,
                "event_date": event.get("event_date") or _event_date(anchor_date, event.get("days_from_baseline")),
            }
            for event in clinical_events
        ],
        "cache_status": signal_metric_status(patient_id=patient_id),
    }
