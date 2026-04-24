import threading
from pathlib import Path
from typing import Dict, List

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from app.db import db
from app.services.import_commit import commit_import_selection
from app.services.fets_finalize import finalize_fets_run
from app.services.import_scan import list_scan_roots, scan_dicom_root
from app.services.rt_import import analyze_rt_excel, commit_rt_excel
from pipelines import import_mu, import_lumiere

router = APIRouter(tags=["import"])

_mu_job = {
    "running": False,
    "last_msg": "",
    "result": None,
    "error": None,
    "root_path": "/mnt/dati/MU-Glioma-Post",
}
_mu_job_lock = threading.Lock()


class ImportScanRequest(BaseModel):
    root_path: str = Field(..., description="Absolute path to the DICOM root on server")
    limit_studies: int | None = Field(default=None, ge=1, le=2000)
    requested_structures: List[str] = Field(default_factory=list)


class ImportCommitRequest(BaseModel):
    root_path: str
    exam_keys: List[str]
    include_series: Dict[str, bool] = Field(default_factory=dict)
    core_choice: Dict[str, str] = Field(default_factory=dict)


class FetsFinalizeRequest(BaseModel):
    session_id: int
    run_dir: str = Field(..., description="Absolute path to a completed FeTS run directory")


class RtImportRequest(BaseModel):
    file_path: str = Field(..., description="Absolute path to the Excel file on server")
    dataset: str = Field(default="irst_dicom_raw")


class MuImportRequest(BaseModel):
    root_path: str = Field(default="/mnt/dati/MU-Glioma-Post")
    limit: int | None = Field(default=None, ge=1)
    subjects: List[str] = Field(default_factory=list)
    purge_selected: bool = Field(default=False)


class LumiereImportRequest(BaseModel):
    root_path: str = Field(default="/mnt/dati/lumiere")
    limit: int | None = Field(default=None, ge=1)
    subjects: List[str] = Field(default_factory=list)
    purge_selected: bool = Field(default=False)


_lumiere_job = {
    "running": False,
    "last_msg": "",
    "result": None,
    "error": None,
    "root_path": "/mnt/dati/lumiere",
}
_lumiere_job_lock = threading.Lock()


def _lumiere_discover(root_path: str) -> dict:
    root = Path(root_path).expanduser().resolve()
    imaging = root / "Imaging"
    demo_csv = root / "LUMIERE-Demographics_Pathology.csv"
    rating_csv = root / "LUMIERE-ExpertRating-v202211.csv"
    patient_dirs = sorted([p for p in imaging.iterdir() if p.is_dir()]) if imaging.exists() else []
    session_dirs = 0
    nifti_files = 0
    seg_files = 0
    for pat_dir in patient_dirs:
        for week_dir in pat_dir.iterdir():
            if not week_dir.is_dir():
                continue
            session_dirs += 1
            for f in week_dir.rglob("*.nii.gz"):
                nifti_files += 1
                if "segmentation_CT1_origspace" in f.name:
                    seg_files += 1
    return {
        "root_path": str(root),
        "imaging": str(imaging),
        "available": {
            "imaging": imaging.exists(),
            "demographics": demo_csv.exists(),
            "ratings": rating_csv.exists(),
        },
        "counts": {
            "patients": len(patient_dirs),
            "sessions": session_dirs,
            "nifti_files": nifti_files,
            "seg_files": seg_files,
        },
    }


def _lumiere_dataset_counts() -> dict:
    with db() as conn:
        subject_count = conn.execute(
            "SELECT COUNT(*) FROM subjects WHERE dataset = 'lumiere'"
        ).fetchone()[0]
        session_count = conn.execute(
            """SELECT COUNT(*) FROM sessions
               WHERE subject_id IN (SELECT id FROM subjects WHERE dataset = 'lumiere')"""
        ).fetchone()[0]
        sequence_count = conn.execute(
            """SELECT COUNT(*) FROM sequences
               WHERE session_id IN (
                   SELECT ses.id FROM sessions ses
                   JOIN subjects sub ON sub.id = ses.subject_id
                   WHERE sub.dataset = 'lumiere')"""
        ).fetchone()[0]
        structure_count = conn.execute(
            """SELECT COUNT(*) FROM computed_structures
               WHERE session_id IN (
                   SELECT ses.id FROM sessions ses
                   JOIN subjects sub ON sub.id = ses.subject_id
                   WHERE sub.dataset = 'lumiere')"""
        ).fetchone()[0]
        event_count = conn.execute(
            """SELECT COUNT(*) FROM clinical_events
               WHERE subject_id IN (SELECT id FROM subjects WHERE dataset = 'lumiere')"""
        ).fetchone()[0]
    return {
        "subjects": subject_count,
        "sessions": session_count,
        "sequences": sequence_count,
        "structures": structure_count,
        "clinical_events": event_count,
    }


def _run_lumiere_import(root_path: str, limit: int | None, subjects: list[str], purge_selected: bool) -> None:
    try:
        root = Path(root_path).expanduser().resolve()
        import_lumiere.LUMIERE_ROOT = root
        import_lumiere.IMAGING_ROOT = root / "Imaging"
        import_lumiere.DEMO_CSV = root / "LUMIERE-Demographics_Pathology.csv"
        import_lumiere.RATING_CSV = root / "LUMIERE-ExpertRating-v202211.csv"

        with _lumiere_job_lock:
            _lumiere_job["last_msg"] = "Import in corso…"
            _lumiere_job["error"] = None
            _lumiere_job["result"] = None
            _lumiere_job["root_path"] = str(root)

        with db() as conn:
            if purge_selected:
                conn.execute("DELETE FROM subjects WHERE dataset = 'lumiere'")
                conn.commit()
            import_lumiere.run(
                conn,
                verbose=False,
                limit=limit,
                subjects=set(subjects) if subjects else None,
            )

        with _lumiere_job_lock:
            _lumiere_job["running"] = False
            _lumiere_job["result"] = {
                "root_path": str(root),
                "counts": _lumiere_dataset_counts(),
            }
            _lumiere_job["last_msg"] = "Import completato"
    except Exception as exc:
        with _lumiere_job_lock:
            _lumiere_job["running"] = False
            _lumiere_job["error"] = str(exc)
            _lumiere_job["last_msg"] = f"Errore: {exc}"


def _mu_resolve_paths(root_path: str) -> dict:
    root = Path(root_path).expanduser().resolve()
    if not root.exists():
        raise HTTPException(404, f"MU root not found: {root_path}")
    pkg_root = root / "PKG - MU-Glioma-Post" if (root / "PKG - MU-Glioma-Post").exists() else root
    data_root = pkg_root / "MU-Glioma-Post"
    clinical_xls = pkg_root / "MU-Glioma-Post_DATI" / "MU-Glioma-Post_ClinicalData-July2025.xlsx"
    volumes_xls = pkg_root / "MU-Glioma-Post_DATI" / "MU-Glioma-Post_Segmentation_Volumes.xlsx"
    return {
        "root": root,
        "pkg_root": pkg_root,
        "data_root": data_root,
        "clinical_xls": clinical_xls,
        "volumes_xls": volumes_xls,
    }


def _mu_discover(root_path: str) -> dict:
    paths = _mu_resolve_paths(root_path)
    data_root = paths["data_root"]
    patient_dirs = sorted([p for p in data_root.iterdir() if p.is_dir()]) if data_root.exists() else []
    timepoint_dirs = 0
    nifti_files = 0
    mask_files = 0
    for patient_dir in patient_dirs:
        for tp_dir in patient_dir.iterdir():
            if not tp_dir.is_dir():
                continue
            timepoint_dirs += 1
            for item in tp_dir.iterdir():
                if not item.is_file():
                    continue
                lower = item.name.lower()
                if lower.endswith(".nii.gz"):
                    nifti_files += 1
                    if "tumormask" in lower:
                        mask_files += 1
    return {
        "root_path": str(paths["root"]),
        "pkg_root": str(paths["pkg_root"]),
        "data_root": str(paths["data_root"]),
        "clinical_xls": str(paths["clinical_xls"]),
        "volumes_xls": str(paths["volumes_xls"]),
        "available": {
            "mri": paths["data_root"].exists(),
            "clinical": paths["clinical_xls"].exists(),
            "structures": paths["volumes_xls"].exists(),
        },
        "counts": {
            "patients": len(patient_dirs),
            "timepoints": timepoint_dirs,
            "nifti_files": nifti_files,
            "mask_files": mask_files,
        },
    }


def _mu_dataset_counts() -> dict:
    with db() as conn:
        subject_count = conn.execute(
            "SELECT COUNT(*) FROM subjects WHERE dataset = 'mu_glioma_post'"
        ).fetchone()[0]
        session_count = conn.execute(
            """
            SELECT COUNT(*) FROM sessions
            WHERE subject_id IN (SELECT id FROM subjects WHERE dataset = 'mu_glioma_post')
            """
        ).fetchone()[0]
        sequence_count = conn.execute(
            """
            SELECT COUNT(*) FROM sequences
            WHERE session_id IN (
                SELECT ses.id
                FROM sessions ses
                JOIN subjects sub ON sub.id = ses.subject_id
                WHERE sub.dataset = 'mu_glioma_post'
            )
            """
        ).fetchone()[0]
        structure_count = conn.execute(
            """
            SELECT COUNT(*) FROM radiological_structures
            WHERE session_id IN (
                SELECT ses.id
                FROM sessions ses
                JOIN subjects sub ON sub.id = ses.subject_id
                WHERE sub.dataset = 'mu_glioma_post'
            )
            """
        ).fetchone()[0]
        event_count = conn.execute(
            """
            SELECT COUNT(*) FROM clinical_events
            WHERE subject_id IN (SELECT id FROM subjects WHERE dataset = 'mu_glioma_post')
            """
        ).fetchone()[0]
    return {
        "subjects": subject_count,
        "sessions": session_count,
        "sequences": sequence_count,
        "structures": structure_count,
        "clinical_events": event_count,
    }


def _run_mu_import(root_path: str, limit: int | None, subjects: list[str], purge_selected: bool) -> None:
    try:
        paths = _mu_resolve_paths(root_path)
        import_mu.MU_ROOT = paths["pkg_root"]
        import_mu.DATA_ROOT = paths["data_root"]
        import_mu.CLINICAL_XLS = paths["clinical_xls"]
        import_mu.VOLUMES_XLS = paths["volumes_xls"]

        with _mu_job_lock:
            _mu_job["last_msg"] = "Import in corso…"
            _mu_job["error"] = None
            _mu_job["result"] = None
            _mu_job["root_path"] = str(paths["root"])

        with db() as conn:
            if purge_selected:
                conn.execute("DELETE FROM subjects WHERE dataset = 'mu_glioma_post'")
                conn.commit()
            import_mu.run(
                conn,
                verbose=False,
                limit=limit,
                subjects=set(subjects) if subjects else None,
            )

        with _mu_job_lock:
            _mu_job["running"] = False
            _mu_job["result"] = {
                "root_path": str(paths["root"]),
                "counts": _mu_dataset_counts(),
            }
            _mu_job["last_msg"] = "Import completato"
    except Exception as exc:
        with _mu_job_lock:
            _mu_job["running"] = False
            _mu_job["error"] = str(exc)
            _mu_job["last_msg"] = f"Errore: {exc}"


@router.get("/import/lumiere/status")
def lumiere_import_status(root_path: str = "/mnt/dati/lumiere"):
    with _lumiere_job_lock:
        state = dict(_lumiere_job)
    try:
        dataset = _lumiere_discover(root_path)
    except Exception as exc:
        dataset = {"error": str(exc)}
    return {
        "job": state,
        "dataset": dataset,
        "db_counts": _lumiere_dataset_counts(),
    }


@router.post("/import/lumiere/run")
def run_lumiere_import(payload: LumiereImportRequest, background_tasks: BackgroundTasks):
    with _lumiere_job_lock:
        if _lumiere_job["running"]:
            return {"status": "already_running"}
        _lumiere_job.update({
            "running": True,
            "last_msg": "Avvio import LUMIERE…",
            "result": None,
            "error": None,
            "root_path": payload.root_path,
        })
    background_tasks.add_task(
        _run_lumiere_import,
        payload.root_path,
        payload.limit,
        payload.subjects,
        payload.purge_selected,
    )
    return {"status": "started"}


@router.get("/import/roots")
def get_import_roots():
    return {"roots": list_scan_roots()}


@router.get("/import/mu/status")
def mu_import_status(root_path: str = "/mnt/dati/MU-Glioma-Post"):
    with _mu_job_lock:
        state = dict(_mu_job)
    return {
        "job": state,
        "dataset": _mu_discover(root_path),
        "db_counts": _mu_dataset_counts(),
    }


@router.post("/import/scan")
def scan_import_root(payload: ImportScanRequest):
    return scan_dicom_root(payload.root_path, limit_studies=payload.limit_studies)


@router.post("/import/mu/run")
def run_mu_import(payload: MuImportRequest, background_tasks: BackgroundTasks):
    with _mu_job_lock:
        if _mu_job["running"]:
            return {"status": "already_running"}
        _mu_job.update({
            "running": True,
            "last_msg": "Avvio import MU…",
            "result": None,
            "error": None,
            "root_path": payload.root_path,
        })
    background_tasks.add_task(
        _run_mu_import,
        payload.root_path,
        payload.limit,
        payload.subjects,
        payload.purge_selected,
    )
    return {"status": "started"}


@router.post("/import/commit")
def commit_import_root(payload: ImportCommitRequest):
    return commit_import_selection(
        root_path=payload.root_path,
        exam_keys=payload.exam_keys,
        include_series=payload.include_series,
        core_choice=payload.core_choice,
    )


@router.post("/import/fets/finalize")
def finalize_fets_import(payload: FetsFinalizeRequest):
    return finalize_fets_run(payload.session_id, payload.run_dir)


@router.post("/import/rt/analyze")
def analyze_rt_import(payload: RtImportRequest):
    return analyze_rt_excel(file_path=payload.file_path, dataset=payload.dataset)


@router.post("/import/rt/commit")
def commit_rt_import(payload: RtImportRequest):
    return commit_rt_excel(file_path=payload.file_path, dataset=payload.dataset)
