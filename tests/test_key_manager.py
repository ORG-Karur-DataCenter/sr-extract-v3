"""Key manager tests — verify rate limiting, backoff, key selection."""
import time
import pytest

from core.key_manager import KeyManager, backoff_with_jitter


def test_single_key_basic():
    km = KeyManager(["key1"], rpm_limit=10, tpm_limit=100_000, rpd_limit=1000)
    k = km.get_best_key(tokens_needed=500)
    assert k == "key1"
    km.mark_used("key1", 500)
    status = km.status()
    assert status[0]["rpm"] == "1/10"


def test_multi_key_selects_freshest():
    km = KeyManager(["a", "b", "c"], rpm_limit=10, tpm_limit=100_000, rpd_limit=1000)
    # Use up key 'a'
    for _ in range(8):
        km.mark_used("a", 100)
    # Should prefer b or c now
    k = km.get_best_key()
    assert k in ("b", "c")


def test_85_percent_threshold():
    km = KeyManager(["solo"], rpm_limit=10, tpm_limit=100_000, rpd_limit=1000)
    # 85% of 10 = 8.5, so after 8 uses, still OK; after 9 uses (>8.5), not OK
    for _ in range(9):
        km.mark_used("solo", 100)
    assert km.get_best_key() is None


def test_rate_limited_key_blocked():
    km = KeyManager(["a", "b"], rpm_limit=10, tpm_limit=100_000, rpd_limit=1000)
    km.mark_rate_limited("a", retry_after=30)
    # 'a' is blocked, should return 'b'
    k = km.get_best_key()
    assert k == "b"


def test_all_blocked_returns_none():
    km = KeyManager(["a"], rpm_limit=10, tpm_limit=100_000, rpd_limit=1000)
    km.mark_rate_limited("a", retry_after=60)
    assert km.get_best_key() is None
    assert km.earliest_reset() > 0


def test_window_resets_after_60s():
    km = KeyManager(["a"], rpm_limit=10, tpm_limit=100_000, rpd_limit=1000)
    for _ in range(9):
        km.mark_used("a", 100)
    assert km.get_best_key() is None
    # Fake time travel
    km.states["a"].window_reset_at = time.time() - 1
    assert km.get_best_key() == "a"


def test_backoff_monotonic_with_jitter():
    # Each attempt yields delay >= previous (with small jitter tolerance)
    delays = [backoff_with_jitter(i, base=2, cap=60, jitter=0.1) for i in range(1, 6)]
    # Check general growth, not strict monotonic (jitter can perturb)
    assert delays[-1] > delays[0]
    # Cap respected
    assert all(d <= 60 + 0.1 for d in delays)


def test_tpm_headroom_blocks():
    km = KeyManager(["a"], rpm_limit=100, tpm_limit=10_000, rpd_limit=10_000)
    km.mark_used("a", 8000)
    # 85% of 10k = 8500, so need < 500 more
    assert km.get_best_key(tokens_needed=1000) is None
    assert km.get_best_key(tokens_needed=200) == "a"
