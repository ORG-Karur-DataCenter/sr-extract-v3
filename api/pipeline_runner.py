"""Per-job pipeline runner.

Wraps the extraction pipeline (ingest → worker → aggregate → write) in an
isolated call that reads from a per-job sandbox directory and writes the
final output there. No global state is mutated except the SQLite DB placed
inside the sandbox.
"""
from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import Optional

from core.aggregator import aggregate_study
from core.extractor import RateLimitError, PermanentAPIError
from core.job_store import JobStore
from core.key_manager import KeyManager
from core.worker import Worker
from config.settings import (
    GEMINI_RPM_LIMIT, GEMINI_TPM_LIMIT, GEMINI_RPD_LIMIT,
    MAX_CONCURRENT_WORKERS,
)
from ingest import ingest_all, load_template, discover_template
from output.writer import IncrementalWriter
from api.job_context import JobContext

log = logging.getLogger(__name__)

# Hard ceiling on wall time per job. Real jobs with many PDFs will finish
# well within this; it guards against infinite retry loops.
_JOB_TIMEOUT_SECONDS = 3600  # 1 hour


def _classify_error(exc: BaseException) -> str:
    """Map an exception to a short stable error code string."""
    if isinstance(exc, RateLimitError):
        return "quota_exhausted"
    if isinstance(exc, PermanentAPIError):
        return "invalid_key"
    log.debug("Unclassified pipeline error type: %s", type(exc).__name__)
    return "pipeline_error"


async def run_pipeline_for_job(ctx: JobContext) -> None:
    """Run the full extraction pipeline for a single job.

    Reads PDFs from ``ctx.sandbox/pdfs/`` and the template from
    ``ctx.sandbox/templates/``. Writes the output file into ``ctx.sandbox/``
    and sets ``ctx.result_path`` on success.

    All exceptions are caught and reflected in ``ctx.status``/``ctx.error_code``
    so the caller never needs to handle them. The only exception that
    propagates is ``asyncio.CancelledError`` (signals external shutdown).
    """
    ctx.status = "running"
    ctx.touch()

    store: Optional[JobStore] = None
    writer: Optional[IncrementalWriter] = None

    try:
        # ── 1. Set up the per-job SQLite store ───────────────────────
        db_path = ctx.sandbox / "sr_jobs.db"
        store = JobStore(db_path=db_path)

        # ── 2. Ingest PDFs + template ────────────────────────────────
        pdf_dir = ctx.sandbox / "pdfs"
        template_dir = ctx.sandbox / "templates"
        ingest_all(store, pdf_dir=pdf_dir, template_dir=template_dir)

        # Load field list directly from the template file
        template_path = discover_template(template_dir)
        schema = load_template(template_path)
        fields: list[str] = schema.get("fields", [])

        # ── 3. Populate ctx counters ─────────────────────────────────
        stats = store.stats()
        ctx.chunks_total = stats.get("pending", 0)
        # Studies = distinct PDF stems; count them from the DB directly
        # (ingest registers one study per PDF)
        ctx.studies_total = store.study_count()
        ctx.touch()

        # ── 4. Build KeyManager and Worker ───────────────────────────
        key_manager = KeyManager(
            keys=ctx.api_keys,
            rpm_limit=GEMINI_RPM_LIMIT,
            tpm_limit=GEMINI_TPM_LIMIT,
            rpd_limit=GEMINI_RPD_LIMIT,
        )

        def _on_progress(job_id: str, info: dict) -> None:
            ctx.chunks_done += 1
            ctx.touch()

        worker = Worker(
            store=store,
            keys=key_manager,
            expected_fields=fields,
            max_concurrent=MAX_CONCURRENT_WORKERS,
            on_progress=_on_progress,
        )

        # ── 5. Set up incremental writer ─────────────────────────────
        writer = IncrementalWriter(
            fields=fields,
            output_dir=ctx.sandbox,
            basename="output",
            fmt=ctx.output_format,
        )

        # ── 6. Aggregator loop (runs concurrently with the worker) ────
        async def _aggregator_loop() -> None:
            """Poll for completed studies and write them incrementally."""
            while True:
                completed = store.get_completed_studies()
                for study_row in completed:
                    study_id = study_row["study_id"]
                    result = aggregate_study(store, study_id, fields)
                    if result is not None:
                        writer.append_record(result.record, fields)
                        store.mark_study_written(study_id, result.record)
                        ctx.studies_done += 1
                        ctx.touch()
                    else:
                        # Unexpected — mark as failed to avoid infinite loop
                        store.mark_study_written(study_id, {})
                        ctx.studies_failed += 1
                        ctx.touch()

                # Termination: no chunks still pending or in-flight
                s = store.stats()
                pending = s.get("pending", 0)
                in_progress = s.get("in_progress", 0)
                if pending == 0 and in_progress == 0:
                    break

                await asyncio.sleep(0.5)

        # ── 7. Run worker + aggregator concurrently ──────────────────
        await asyncio.wait_for(
            asyncio.gather(worker.run(), _aggregator_loop()),
            timeout=_JOB_TIMEOUT_SECONDS,
        )

        # ── 8. Finalise ──────────────────────────────────────────────
        ctx.result_path = writer.output_path
        ctx.status = "done"
        ctx.touch()
        log.info(f"Job {ctx.job_id} done. Output: {ctx.result_path}")

    except asyncio.TimeoutError:
        log.error(f"Job {ctx.job_id} timed out after {_JOB_TIMEOUT_SECONDS}s")
        ctx.status = "failed"
        ctx.error_code = "timeout"
        ctx.error_message = f"Pipeline timed out after {_JOB_TIMEOUT_SECONDS}s"
        ctx.touch()

    except asyncio.CancelledError:
        log.warning(f"Job {ctx.job_id} cancelled")
        ctx.status = "cancelled"
        ctx.touch()
        raise  # propagate so the event loop can clean up

    except Exception as exc:
        log.exception(f"Job {ctx.job_id} failed: {exc}")
        ctx.status = "failed"
        ctx.error_code = _classify_error(exc)
        ctx.error_message = str(exc)
        ctx.touch()

    finally:
        # Always zero out secrets — even if we crash mid-run
        ctx.api_keys = []
        if store is not None:
            store.close()
        if writer is not None:
            writer.close()
