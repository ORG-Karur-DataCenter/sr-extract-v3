"""Pydantic request/response models for the HTTP API."""
from __future__ import annotations
from typing import Literal, Optional

from pydantic import BaseModel, Field


SUPPORTED_MODELS = frozenset({
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
})


JobStatus = Literal["pending", "running", "done", "failed", "cancelled"]


def validate_model(value: str) -> str:
    if value not in SUPPORTED_MODELS:
        raise ValueError(
            f"Unsupported model {value!r}. Allowed: {sorted(SUPPORTED_MODELS)}"
        )
    return value


class JobAcceptedResponse(BaseModel):
    job_id: str
    accepted_at: str


class JobProgress(BaseModel):
    studies_total: int = Field(ge=0)
    studies_done: int = Field(ge=0)
    studies_failed: int = Field(ge=0)
    chunks_total: int = Field(ge=0)
    chunks_done: int = Field(ge=0)
    rate_per_min: float = Field(ge=0.0)
    eta_seconds: int = Field(ge=0)


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    progress: JobProgress
    error_code: Optional[str]
    error_message: Optional[str]
    created_at: str
    updated_at: str


class HealthResponse(BaseModel):
    status: Literal["ok"]
    active_jobs: int
    uptime_seconds: int


class ErrorResponse(BaseModel):
    error_code: str
    error_message: str
