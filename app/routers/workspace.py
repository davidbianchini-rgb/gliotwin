from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.processing_jobs import (
    cancel_processing_job,
    create_processing_job,
    dispatch_next_job,
    get_job,
    list_jobs,
    queue_all_unprocessed_jobs,
    queue_processing_jobs,
    read_job_log,
    remove_processing_job,
    stop_all_processing,
    start_processing_job,
    workspace_case,
    workspace_overview,
)
from app.services.system_status import system_status

router = APIRouter(tags=["workspace"])


class SessionJobRequest(BaseModel):
    session_id: int


class BatchSessionJobRequest(BaseModel):
    session_ids: list[int]


@router.get("/workspace/{session_id}")
def get_workspace_case(session_id: int):
    return workspace_case(session_id)


@router.get("/workspace")
def get_workspace_overview():
    return workspace_overview()


@router.get("/system/status")
def get_system_status():
    return system_status()


@router.get("/processing/jobs")
def get_processing_jobs(session_id: int | None = None):
    return {"jobs": list_jobs(session_id=session_id)}


@router.post("/processing/jobs")
def create_job(payload: SessionJobRequest):
    return create_processing_job(payload.session_id)


@router.post("/processing/jobs/queue")
def queue_jobs(payload: BatchSessionJobRequest):
    return queue_processing_jobs(payload.session_ids)


@router.post("/processing/jobs/queue-unprocessed")
def queue_unprocessed_jobs():
    return queue_all_unprocessed_jobs()


@router.post("/processing/jobs/{job_id}/start")
def start_job(job_id: int):
    return start_processing_job(job_id)


@router.post("/processing/jobs/{job_id}/cancel")
def cancel_job(job_id: int):
    return cancel_processing_job(job_id)


@router.delete("/processing/jobs/{job_id}")
def remove_job(job_id: int):
    return remove_processing_job(job_id)


@router.get("/processing/jobs/{job_id}")
def get_processing_job(job_id: int):
    return get_job(job_id)


@router.get("/processing/jobs/{job_id}/log")
def get_processing_job_log(job_id: int, tail: int = Query(default=4000, ge=200, le=50000)):
    return read_job_log(job_id, tail=tail)


@router.post("/processing/dispatch")
def run_dispatch():
    return dispatch_next_job()


@router.post("/processing/stop-all")
def stop_all_jobs():
    return stop_all_processing()
