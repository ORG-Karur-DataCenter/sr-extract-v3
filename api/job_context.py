"""Per-job in-memory state.

Holds user-supplied API keys for the job duration, never written
to disk. Dispose zeroes the key list.
"""
from __future__ import annotations
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from api.schemas import JobProgress, JobStatus


class JobStateSnapshot(BaseModel):
    """Persistable subset of a JobContext — no secrets."""
    job_id: str
    status: JobStatus
    progress: JobProgress
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    created_at: float
    updated_at: float
    result_path: Optional[str] = None


@dataclass
class JobContext:
    job_id: str
    sandbox: Path
    api_keys: list[str]
    model: str
    output_format: str
    status: JobStatus = "pending"
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    result_path: Optional[Path] = None
    # live counters mutated by the pipeline callback
    studies_total: int = 0
    studies_done: int = 0
    studies_failed: int = 0
    chunks_total: int = 0
    chunks_done: int = 0

    def touch(self) -> None:
        self.updated_at = time.time()

    def dispose(self) -> None:
        """Zero out secrets. Safe to call multiple times."""
        self.api_keys = []

    def snapshot(self) -> JobStateSnapshot:
        elapsed = max(self.updated_at - self.created_at, 1e-9)
        rate = (self.chunks_done / elapsed) * 60.0
        remaining = max(self.chunks_total - self.chunks_done, 0)
        eta = int(remaining / rate * 60.0) if rate > 0 else 0
        return JobStateSnapshot(
            job_id=self.job_id,
            status=self.status,
            progress=JobProgress(
                studies_total=self.studies_total,
                studies_done=self.studies_done,
                studies_failed=self.studies_failed,
                chunks_total=self.chunks_total,
                chunks_done=self.chunks_done,
                rate_per_min=round(rate, 2),
                eta_seconds=eta,
            ),
            error_code=self.error_code,
            error_message=self.error_message,
            created_at=self.created_at,
            updated_at=self.updated_at,
            result_path=str(self.result_path) if self.result_path else None,
        )
