from typing import Dict, List

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.import_commit import commit_import_selection
from app.services.fets_finalize import finalize_fets_run
from app.services.import_scan import list_scan_roots, scan_dicom_root
from app.services.rt_import import analyze_rt_excel, commit_rt_excel

router = APIRouter(tags=["import"])


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


@router.get("/import/roots")
def get_import_roots():
    return {"roots": list_scan_roots()}


@router.post("/import/scan")
def scan_import_root(payload: ImportScanRequest):
    return scan_dicom_root(payload.root_path, limit_studies=payload.limit_studies)


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
