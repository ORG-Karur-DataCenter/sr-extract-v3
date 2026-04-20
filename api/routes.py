"""FastAPI route handlers for sr-extract-v3."""
from __future__ import annotations
import asyncio
import time as _time
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, BackgroundTasks, File, Form, Request, Response, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from api.job_context import JobContext
from api.job_manager import JobNotFoundError, ServerBusyError
from api.pipeline_runner import run_pipeline_for_job
from api.schemas import (
    ErrorResponse, HealthResponse, JobAcceptedResponse, JobStatusResponse,
    validate_model,
)

router = APIRouter()

MAX_PDFS_PER_JOB = 20
MAX_BYTES_PER_PDF = 10 * 1024 * 1024  # 10 MB


def _err(code: str, msg: str, status: int) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content=ErrorResponse(error_code=code, error_message=msg).model_dump(),
    )


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    mgr = request.app.state.job_manager
    started = request.app.state.started_at
    return HealthResponse(
        status="ok",
        active_jobs=mgr.active_count(),
        uptime_seconds=int(_time.time() - started),
    )


# ---------------------------------------------------------------------------
# POST /jobs
# ---------------------------------------------------------------------------

@router.post("/jobs", response_model=JobAcceptedResponse)
async def post_job(
    request: Request,
    api_keys: str = Form(""),
    model: str = Form("gemini-2.0-flash"),
    output_format: str = Form("xlsx"),
    pdfs: List[UploadFile] = File(default=[]),
    template: UploadFile | None = File(default=None),
):
    if getattr(request.app.state, "shutting_down", False):
        return _err("server_shutdown", "Server is shutting down", 503)

    keys = [k.strip() for k in api_keys.split(",") if k.strip()]
    if not keys:
        return _err("missing_api_keys", "At least one Gemini API key required", 400)
    try:
        model = validate_model(model)
    except ValueError as e:
        return _err("invalid_model", str(e), 422)
    if output_format not in ("xlsx", "csv"):
        return _err("invalid_output_format", "output_format must be xlsx or csv", 422)
    if not pdfs:
        return _err("missing_pdfs", "At least one PDF required", 400)
    if len(pdfs) > MAX_PDFS_PER_JOB:
        return _err("too_many_pdfs", f"Max {MAX_PDFS_PER_JOB} PDFs per job", 400)
    if template is None:
        return _err("missing_template", "Template file required", 400)

    mgr = request.app.state.job_manager
    try:
        ctx = mgr.create_job(api_keys=keys, model=model, output_format=output_format)
    except ServerBusyError as e:
        resp = _err("server_busy", str(e), 429)
        resp.headers["Retry-After"] = "60"
        return resp

    # Save uploads into the sandbox
    for up in pdfs:
        data = await up.read()
        if len(data) > MAX_BYTES_PER_PDF:
            mgr.dispose(ctx.job_id)
            return _err("payload_too_large", f"{up.filename} exceeds per-file limit", 413)
        (ctx.sandbox / "pdfs" / up.filename).write_bytes(data)

    # Save template into sandbox/templates/ (where ingest_all looks for it)
    tpl_bytes = await template.read()
    templates_dir = ctx.sandbox / "templates"
    templates_dir.mkdir(exist_ok=True)
    (templates_dir / "template.xlsx").write_bytes(tpl_bytes)

    # Fire-and-forget the pipeline
    task = asyncio.create_task(run_pipeline_for_job(ctx))
    request.app.state.__dict__.setdefault("_job_tasks", {})[ctx.job_id] = task

    return JobAcceptedResponse(
        job_id=ctx.job_id,
        accepted_at=datetime.now(timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# GET /jobs/{id}/status
# ---------------------------------------------------------------------------

@router.get("/jobs/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(request: Request, job_id: str):
    mgr = request.app.state.job_manager
    try:
        ctx = mgr.get(job_id)
    except JobNotFoundError:
        return _err("job_not_found", f"No job with id {job_id!r}", 404)
    snap = ctx.snapshot()
    return JobStatusResponse(
        job_id=snap.job_id,
        status=snap.status,
        progress=snap.progress,
        error_code=snap.error_code,
        error_message=snap.error_message,
        created_at=datetime.fromtimestamp(snap.created_at, tz=timezone.utc).isoformat(),
        updated_at=datetime.fromtimestamp(snap.updated_at, tz=timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# GET /jobs/{id}/result
# ---------------------------------------------------------------------------

@router.get("/jobs/{job_id}/result")
async def get_job_result(request: Request, job_id: str, background: BackgroundTasks):
    mgr = request.app.state.job_manager
    try:
        ctx = mgr.get(job_id)
    except JobNotFoundError:
        return _err("job_not_found", f"No job with id {job_id!r}", 404)
    if ctx.status != "done" or ctx.result_path is None:
        return _err("not_ready", f"Job status is {ctx.status!r}", 409)
    # Guard against a stale result_path whose file has been removed
    # (e.g. sandbox cleanup raced ahead, or writer never flushed).
    if not ctx.result_path.exists():
        return _err(
            "result_missing",
            "Result file is not available on disk",
            409,
        )

    def _cleanup():
        try:
            mgr.dispose(job_id)
        except JobNotFoundError:
            pass

    background.add_task(_cleanup)
    media = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if ctx.output_format == "xlsx"
        else "text/csv"
    )
    filename = f"sr-extract-{job_id}.{ctx.output_format}"
    return FileResponse(path=str(ctx.result_path), media_type=media, filename=filename)


# ---------------------------------------------------------------------------
# DELETE /jobs/{id}
# ---------------------------------------------------------------------------

@router.delete("/jobs/{job_id}", status_code=204)
async def delete_job(request: Request, job_id: str):
    mgr = request.app.state.job_manager
    tasks = request.app.state.__dict__.get("_job_tasks", {})
    t = tasks.pop(job_id, None)
    if t is not None and not t.done():
        t.cancel()
    try:
        mgr.dispose(job_id)
    except JobNotFoundError:
        return _err("job_not_found", f"No job with id {job_id!r}", 404)
    return Response(status_code=204)
