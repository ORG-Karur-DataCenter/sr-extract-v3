"""Token-aware API key manager.

Tracks RPM/TPM per key, applies 85% safety threshold, selects the key
with most headroom. Per-key blocking on 429 with retry_after honored.
"""
from __future__ import annotations
import time
import random
import threading
from dataclasses import dataclass, field
from typing import Optional

from config.settings import (
    GEMINI_RPM_LIMIT, GEMINI_TPM_LIMIT, GEMINI_RPD_LIMIT,
    SAFETY_THRESHOLD, RETRY_BASE_DELAY, RETRY_MAX_DELAY, RETRY_JITTER,
)


@dataclass
class KeyState:
    key: str
    rpm_used: int = 0
    tpm_used: int = 0
    rpd_used: int = 0
    window_reset_at: float = 0.0  # minute window
    day_reset_at: float = 0.0
    blocked_until: float = 0.0
    last_used_at: float = 0.0
    failure_count: int = 0


class KeyManager:
    """Manages a pool of API keys with per-key rate tracking."""

    def __init__(self, keys: list[str],
                 rpm_limit: int = GEMINI_RPM_LIMIT,
                 tpm_limit: int = GEMINI_TPM_LIMIT,
                 rpd_limit: int = GEMINI_RPD_LIMIT):
        if not keys:
            raise ValueError("At least one API key required")
        self.rpm_limit = rpm_limit
        self.tpm_limit = tpm_limit
        self.rpd_limit = rpd_limit
        self.states: dict[str, KeyState] = {k: KeyState(k) for k in keys}
        self._lock = threading.Lock()

    def _tick_windows(self, s: KeyState, now: float):
        if now >= s.window_reset_at:
            s.rpm_used = 0
            s.tpm_used = 0
            s.window_reset_at = now + 60
        if now >= s.day_reset_at:
            s.rpd_used = 0
            s.day_reset_at = now + 86400

    def get_best_key(self, tokens_needed: int = 0) -> Optional[str]:
        """Return the key with most headroom, or None if all exhausted."""
        now = time.time()
        candidates = []
        with self._lock:
            for s in self.states.values():
                self._tick_windows(s, now)
                if now < s.blocked_until:
                    continue
                rpm_ok = s.rpm_used < self.rpm_limit * SAFETY_THRESHOLD
                tpm_ok = (s.tpm_used + tokens_needed) < self.tpm_limit * SAFETY_THRESHOLD
                rpd_ok = s.rpd_used < self.rpd_limit * SAFETY_THRESHOLD
                if rpm_ok and tpm_ok and rpd_ok:
                    headroom = (self.rpm_limit - s.rpm_used)
                    candidates.append((headroom, s.last_used_at, s.key))
            if not candidates:
                return None
            # Pick highest headroom, tie-break by least-recently-used
            candidates.sort(key=lambda x: (-x[0], x[1]))
            return candidates[0][2]

    def mark_used(self, key: str, tokens_used: int):
        now = time.time()
        with self._lock:
            s = self.states[key]
            self._tick_windows(s, now)
            s.rpm_used += 1
            s.tpm_used += tokens_used
            s.rpd_used += 1
            s.last_used_at = now
            # Note: failure_count is NOT reset here — only on explicit
            # mark_success() so the pre-increment call doesn't wipe
            # rate-limit escalation history.

    def mark_success(self, key: str):
        """Reset failure count after a successful API call."""
        with self._lock:
            self.states[key].failure_count = 0

    def mark_rate_limited(self, key: str, retry_after: float = 60.0):
        with self._lock:
            s = self.states[key]
            # Escalate block time if this key has been rate-limited before
            # (signals shared project quota — all keys are exhausted together)
            if s.failure_count >= 2:
                retry_after = max(retry_after, 90.0)
            s.blocked_until = time.time() + retry_after
            s.failure_count += 1

    def all_blocked(self) -> bool:
        """Return True if every key is currently blocked (429 cooldown)."""
        now = time.time()
        with self._lock:
            return all(now < s.blocked_until for s in self.states.values())

    def mark_failure(self, key: str):
        with self._lock:
            self.states[key].failure_count += 1

    def earliest_reset(self) -> float:
        """Seconds until the next key becomes usable."""
        now = time.time()
        with self._lock:
            resets = []
            for s in self.states.values():
                if now < s.blocked_until:
                    resets.append(s.blocked_until - now)
                elif s.rpm_used >= self.rpm_limit * SAFETY_THRESHOLD:
                    resets.append(max(0, s.window_reset_at - now))
            return min(resets) if resets else 1.0

    def status(self) -> list[dict]:
        now = time.time()
        out = []
        with self._lock:
            for s in self.states.values():
                self._tick_windows(s, now)
                out.append({
                    "key": s.key[:8] + "...",
                    "rpm": f"{s.rpm_used}/{self.rpm_limit}",
                    "tpm": f"{s.tpm_used}/{self.tpm_limit}",
                    "rpd": f"{s.rpd_used}/{self.rpd_limit}",
                    "blocked": max(0, s.blocked_until - now),
                    "failures": s.failure_count,
                })
        return out


def backoff_with_jitter(attempt: int,
                        base: float = RETRY_BASE_DELAY,
                        cap: float = RETRY_MAX_DELAY,
                        jitter: float = RETRY_JITTER) -> float:
    """Exponential backoff with additive jitter. Prevents retry storms."""
    delay = min(cap, base ** attempt)
    return delay + random.uniform(0, jitter)
