"""JobManager — creates sandboxes, tracks contexts, enforces concurrency cap.

Each job gets its own UUID, its own sandbox directory, and lives in memory.
Sandboxes are wiped on dispose and at startup (no state survives restart).
"""
from __future__ import annotations
import os
import shutil
import uuid
import logging
import threading
from pathlib import Path

from api.job_context import JobContext

log = logging.getLogger(__name__)


class JobNotFoundError(LookupError):
    pass


class ServerBusyError(RuntimeError):
    pass


class JobManager:
    def __init__(self, jobs_root: Path):
        self.jobs_root = Path(jobs_root)
        self._wipe_on_startup()
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, JobContext] = {}
        self._lock = threading.Lock()
        self._cap = int(os.getenv("MAX_CONCURRENT_JOBS", "3"))

    def _wipe_on_startup(self) -> None:
        if self.jobs_root.exists():
            log.info("Wiping orphaned jobs dir at %s", self.jobs_root)
            shutil.rmtree(self.jobs_root)

    def create_job(self, *, api_keys: list[str], model: str,
                   output_format: str) -> JobContext:
        with self._lock:
            active = sum(1 for c in self._jobs.values()
                         if c.status in ("pending", "running"))
            if active >= self._cap:
                raise ServerBusyError(
                    f"{active}/{self._cap} concurrent job slots in use"
                )
            job_id = str(uuid.uuid4())
            sandbox = self.jobs_root / job_id
            (sandbox / "pdfs").mkdir(parents=True)
            ctx = JobContext(
                job_id=job_id, sandbox=sandbox, api_keys=list(api_keys),
                model=model, output_format=output_format,
            )
            self._jobs[job_id] = ctx
            return ctx

    def get(self, job_id: str) -> JobContext:
        try:
            return self._jobs[job_id]
        except KeyError as e:
            raise JobNotFoundError(job_id) from e

    def dispose(self, job_id: str) -> None:
        with self._lock:
            ctx = self._jobs.pop(job_id, None)
        if ctx is None:
            raise JobNotFoundError(job_id)
        ctx.dispose()
        if ctx.sandbox.exists():
            shutil.rmtree(ctx.sandbox, ignore_errors=True)

    def active_count(self) -> int:
        return sum(1 for c in self._jobs.values()
                   if c.status in ("pending", "running"))

    def all_jobs(self) -> list[JobContext]:
        return list(self._jobs.values())

    def cleanup_once(self) -> int:
        """Dispose jobs that have outlived their timeouts. Returns count removed."""
        import time as _time
        idle_timeout = int(os.getenv("JOB_IDLE_TIMEOUT_SECONDS", "3600"))
        failed_grace = int(os.getenv("JOB_FAILED_GRACE_SECONDS", "300"))
        now = _time.time()
        removed = 0
        for ctx in list(self._jobs.values()):
            idle = now - ctx.updated_at
            should_remove = (
                (ctx.status in ("done", "cancelled") and idle >= failed_grace)
                or (ctx.status == "failed" and idle >= failed_grace)
                or (idle >= idle_timeout)
            )
            if should_remove:
                try:
                    self.dispose(ctx.job_id)
                    removed += 1
                except JobNotFoundError:
                    pass
        return removed

    async def cleanup_loop(self, interval_seconds: int = 600) -> None:
        """Background async task — runs cleanup every interval."""
        import asyncio as _asyncio
        while True:
            try:
                removed = self.cleanup_once()
                if removed:
                    log.info("Cleanup removed %d stale jobs", removed)
            except Exception:
                log.exception("Cleanup loop iteration failed")
            await _asyncio.sleep(interval_seconds)
