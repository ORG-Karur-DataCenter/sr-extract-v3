"""Tests for Pydantic request/response schemas."""
import pytest
from pydantic import ValidationError

from api.schemas import (
    JobAcceptedResponse, JobStatusResponse, JobProgress,
    HealthResponse, ErrorResponse, SUPPORTED_MODELS, validate_model,
)


def test_supported_models_whitelist():
    assert "gemini-2.5-flash" in SUPPORTED_MODELS
    assert "gemini-2.0-flash" in SUPPORTED_MODELS
    assert "gemini-2.0-flash-lite" in SUPPORTED_MODELS
    assert "gemini-2.5-pro" in SUPPORTED_MODELS


def test_validate_model_accepts_whitelisted():
    assert validate_model("gemini-2.0-flash") == "gemini-2.0-flash"


def test_validate_model_rejects_unknown():
    with pytest.raises(ValueError, match="Unsupported model"):
        validate_model("gpt-4")


def test_job_accepted_response_serialises():
    resp = JobAcceptedResponse(job_id="abc-123", accepted_at="2026-04-19T10:00:00Z")
    assert resp.model_dump()["job_id"] == "abc-123"


def test_job_status_response_progress_required():
    prog = JobProgress(
        studies_total=10, studies_done=3, studies_failed=0,
        chunks_total=50, chunks_done=15, rate_per_min=2.0, eta_seconds=120,
    )
    resp = JobStatusResponse(
        job_id="x", status="running", progress=prog,
        error_code=None, error_message=None,
        created_at="2026-04-19T10:00:00Z", updated_at="2026-04-19T10:01:00Z",
    )
    assert resp.status == "running"


def test_job_status_rejects_invalid_status():
    prog = JobProgress(studies_total=0, studies_done=0, studies_failed=0,
                       chunks_total=0, chunks_done=0, rate_per_min=0.0, eta_seconds=0)
    with pytest.raises(ValidationError):
        JobStatusResponse(
            job_id="x", status="weird", progress=prog,
            error_code=None, error_message=None,
            created_at="t", updated_at="t",
        )


def test_error_response_has_code_and_message():
    err = ErrorResponse(error_code="missing_api_keys", error_message="Keys required")
    dumped = err.model_dump()
    assert dumped["error_code"] == "missing_api_keys"
    assert dumped["error_message"] == "Keys required"
