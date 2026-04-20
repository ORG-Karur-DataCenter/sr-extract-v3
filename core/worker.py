"""Async worker pool with semaphore-bounded concurrency.

Contract: given a JobStore and KeyManager, process pending jobs until
queue is empty or graceful shutdown. Each worker:
  1. Claims a pending job atomically
  2. Gets a key with headroom (or sleeps)
  3. Calls Gemini extractor
  4. On 429: block key, requeue with jittered backoff
  5. On 503: requeue with backoff; after N retries try fallback
  6. On success: validate, cache, mark done
"""
from __future__ import annotations
import asyncio
import logging
from typing import Callable, Optional

from config.settings import (
    MAX_CONCURRENT_WORKERS, MAX_RETRIES, USE_CLAUDE_FALLBACK,
)
from core.job_store import JobStore
from core.key_manager import KeyManager, backoff_with_jitter
from core.extractor import (
    extract_chunk, RateLimitError, TransientAPIError, PermanentAPIError,
)
from core.validator import validate, normalize_result

log = logging.getLogger(__name__)


class Worker:
    def __init__(self, store: JobStore, keys: KeyManager,
                 expected_fields: list[str],
                 max_concurrent: int = MAX_CONCURRENT_WORKERS,
                 on_progress: Optional[Callable[[str, dict], None]] = None,
                 model: Optional[str] = None):
        self.store = store
        self.keys = keys
        self.expected_fields = expected_fields
        self.sem = asyncio.Semaphore(max_concurrent)
        self.on_progress = on_progress
        self.model = model  # None → extractor falls back to GEMINI_MODEL
        self._stop = False

    async def run(self):
        """Main dispatch loop. Exits when no pending jobs remain.
        
        Processes one job at a time to stay within free-tier rate limits.
        """
        while not self._stop:
            pending = self.store.get_pending(limit=1)
            if not pending:
                log.info("No pending jobs, exiting loop")
                break

            row = pending[0]
            job_id = row["id"]
            if not self.store.claim_job(job_id):
                continue  # someone else got it

            await self._process(dict(row))

            # Pause between API calls to respect rate limits
            if not self._stop:
                await asyncio.sleep(5)

    def stop(self):
        self._stop = True

    async def _process(self, row: dict):
        async with self.sem:
            job_id = row["id"]
            tokens_needed = row.get("token_estimate") or 2000
            retries = row.get("retries", 0)

            # Pick a key
            key = None
            wait_budget = 0
            while key is None:
                key = self.keys.get_best_key(tokens_needed)
                if key is None:
                    wait = self.keys.earliest_reset()
                    wait_budget += wait
                    log.warning(f"All keys busy, sleeping {wait:.1f}s")
                    await asyncio.sleep(min(wait, 30))
                    if wait_budget > 600:  # 10 min total wait: give up
                        self.store.mark_failed(job_id, "No key available within budget", requeue=True)
                        return

            use_fallback = retries >= 3 and USE_CLAUDE_FALLBACK
            try:
                result = await extract_chunk(
                    chunk_text=row["chunk_text"],
                    fields=self.expected_fields,
                    section_name=row.get("section_name"),
                    gemini_key=key,
                    use_fallback=use_fallback,
                    model=self.model,
                )
            except RateLimitError as e:
                self.keys.mark_rate_limited(key, e.retry_after)
                delay = backoff_with_jitter(retries + 1)
                log.warning(f"[{job_id[:12]}] 429, requeue in {delay:.1f}s")
                await asyncio.sleep(delay)
                self.store.mark_failed(job_id, f"429: {e}", requeue=(retries < MAX_RETRIES))
                return
            except TransientAPIError as e:
                delay = backoff_with_jitter(retries + 1)
                log.warning(f"[{job_id[:12]}] transient ({e}), requeue in {delay:.1f}s")
                await asyncio.sleep(delay)
                self.store.mark_failed(job_id, f"transient: {e}", requeue=(retries < MAX_RETRIES))
                return
            except PermanentAPIError as e:
                log.error(f"[{job_id[:12]}] permanent failure: {e}")
                self.store.mark_failed(job_id, f"permanent: {e}", requeue=False)
                return
            except Exception as e:
                log.exception(f"[{job_id[:12]}] unexpected error")
                self.store.mark_failed(job_id, f"unexpected: {e}", requeue=(retries < MAX_RETRIES))
                return

            # Success path
            self.keys.mark_used(key, result.tokens_in + result.tokens_out)
            norm = normalize_result(result.data, self.expected_fields)
            report = validate(norm, self.expected_fields)
            if not report.ok:
                log.info(f"[{job_id[:12]}] chunk produced no non-null fields (may be OK)")
            self.store.mark_done(job_id, norm, result.model_used)
            if self.on_progress:
                self.on_progress(job_id, {"tokens": result.tokens_in + result.tokens_out,
                                          "non_null": report.non_null_count})
