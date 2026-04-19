"""sr-extract-v3 HTTP API entry point.

Wraps the existing pipeline in a FastAPI service. See
docs/superpowers/specs/2026-04-19-backend-frontend-integration-design.md.
"""
from __future__ import annotations
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from api.job_manager import JobManager
from api.routes import router
from api.security import install_cors, install_rate_limiter, install_redaction, install_size_limit
from config.settings import DATA_DIR

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}',
)
install_redaction()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.started_at = time.time()
    app.state.shutting_down = False
    app.state.job_manager = JobManager(jobs_root=DATA_DIR / "jobs")
    app.state.cleanup_task = asyncio.create_task(
        app.state.job_manager.cleanup_loop()
    )
    yield
    # Graceful shutdown
    app.state.shutting_down = True
    logging.getLogger("server").info("Draining in-flight jobs (max 30s)")
    job_tasks = [t for t in app.state.__dict__.get("_job_tasks", {}).values()
                 if not t.done()]
    if job_tasks:
        try:
            await asyncio.wait_for(
                asyncio.gather(*job_tasks, return_exceptions=True), timeout=30.0
            )
        except asyncio.TimeoutError:
            for t in job_tasks:
                if not t.done():
                    t.cancel()
        except RuntimeError:
            # Event loop already closed (e.g. in test teardown) — nothing to do.
            pass
    try:
        app.state.cleanup_task.cancel()
    except RuntimeError:
        pass


app = FastAPI(
    title="sr-extract-v3",
    version="1.0.0",
    lifespan=lifespan,
)
install_cors(app)
install_rate_limiter(app)
install_size_limit(app)
app.include_router(router)
