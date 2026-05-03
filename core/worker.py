"""Async worker pool with semaphore-bounded concurrency.

Contract: given a JobStore and KeyManager, process pending jobs until
queue is empty or graceful shutdown. Each worker:
  1. Claims a pending job atomically
  2. Gets a key with headroom (or sleeps)
  3. Calls Gemini extractor
  4. On 429: block key, try next key; wait for reset when all exhausted
  5. On 503: requeue with backoff; after N retries try fallback
  6. On success: validate, cache, mark done
"""
from __future__ import annotations
import asyncio
import logging
from typing import Callable, Optional

from config.settings import (
    MAX_CONCURRENT_WORKERS, MAX_RETRIES, USE_CLAUDE_FALLBACK,
    get_model_limits,
)
from core.job_store import JobStore
from core.key_manager import KeyManager, backoff_with_jitter
from core.extractor import (
    extract_chunk, RateLimitError, TransientAPIError, PermanentAPIError,
)
from core.validator import validate, normalize_result

log = logging.getLogger(__name__)

# 429s are quota issues (not job bugs), so they get a much higher retry
# budget than transient/permanent errors.
_MAX_429_RETRIES = 30  # covers cycling through 10+ keys multiple times


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

            # Dynamic pause between API calls based on model RPM limit.
            # E.g. gemini-2.5-pro (5 RPM) → 60/5 = 12s gap.
            # Minimum 5s, capped at 15s to keep progress reasonable.
            limits = get_model_limits(self.model or 'gemini-2.5-flash')
            gap = max(5.0, min(60.0 / limits.get('rpm', 10), 15.0))
            if not self._stop:
                await asyncio.sleep(gap)

    def stop(self):
        self._stop = True

    async def _process(self, row: dict):
        async with self.sem:
            job_id = row["id"]
            tokens_needed = row.get("token_estimate") or 2000
            retries = row.get("retries", 0)

            # ── Try keys in rotation until one works or all exhausted ──
            rate_limit_attempts = 0
            while rate_limit_attempts < _MAX_429_RETRIES:
                if self._stop:
                    self.store.mark_failed(job_id, "shutdown", requeue=True)
                    return

                # Pick a key with headroom
                key = None
                wait_budget = 0
                while key is None:
                    key = self.keys.get_best_key(tokens_needed)
                    if key is None:
                        wait = self.keys.earliest_reset()
                        wait_budget += wait
                        log.warning(
                            f"[{job_id[:12]}] All keys exhausted, "
                            f"waiting {wait:.0f}s for quota reset "
                            f"(attempt {rate_limit_attempts + 1})"
                        )
                        await asyncio.sleep(min(wait, 60))
                        if wait_budget > 600:  # 10 min total wait: give up
                            self.store.mark_failed(
                                job_id, "All keys exhausted for 10+ min",
                                requeue=True,
                            )
                            return

                log.info(
                    f"[{job_id[:12]}] Using key {key[:8]}… "
                    f"(attempt {rate_limit_attempts + 1})"
                )

                use_fallback = retries >= 3 and USE_CLAUDE_FALLBACK

                # Pre-increment RPM so KeyManager won't hand this key to
                # another coroutine before the request completes.
                self.keys.mark_used(key, tokens_needed)

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
                    rate_limit_attempts += 1
                    self.keys.mark_rate_limited(key, e.retry_after)
                    log.warning(
                        f"[{job_id[:12]}] 429 on key {key[:8]}… "
                        f"(blocked {e.retry_after:.0f}s). "
                        f"Attempt {rate_limit_attempts}/{_MAX_429_RETRIES}. "
                        f"Error: {str(e)[:200]}"
                    )
                    # If all keys are now blocked, log status + wait for reset
                    if self.keys.all_blocked():
                        wait = self.keys.earliest_reset()
                        key_info = "; ".join(
                            f"{s['key']} fail={s['failures']}"
                            for s in self.keys.status()
                        )
                        log.warning(
                            f"[{job_id[:12]}] ALL keys blocked. "
                            f"Waiting {wait:.0f}s for reset. Keys: {key_info}"
                        )
                        await asyncio.sleep(min(wait, 60))
                    else:
                        await asyncio.sleep(2)
                    continue  # ← try next key, don't requeue
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

                # ── Success path ─────────────────────────────────────
                self.keys.mark_success(key)
                log.info(
                    f"[{job_id[:12]}] ✓ extracted with key {key[:8]}… "
                    f"({result.tokens_in + result.tokens_out} tokens)"
                )
                norm = normalize_result(result.data, self.expected_fields)
                report = validate(norm, self.expected_fields)
                if not report.ok:
                    log.info(f"[{job_id[:12]}] chunk produced no non-null fields (may be OK)")
                self.store.mark_done(job_id, norm, result.model_used)
                if self.on_progress:
                    self.on_progress(job_id, {"tokens": result.tokens_in + result.tokens_out,
                                              "non_null": report.non_null_count})
                return  # done with this job

            # Exhausted all 429 retries
            log.error(
                f"[{job_id[:12]}] Gave up after {rate_limit_attempts} "
                f"rate-limit retries. All keys may share the same "
                f"Google Cloud project quota."
            )
            self.store.mark_failed(
                job_id,
                f"429 on all keys after {rate_limit_attempts} attempts. "
                f"Keys may share same Google project quota — "
                f"use keys from different Google accounts.",
                requeue=True,
            )
