"""Job store tests — in-memory SQLite for isolation."""
import os
import tempfile
import pytest
from pathlib import Path

from core.job_store import JobStore


@pytest.fixture
def store(tmp_path):
    db = tmp_path / "test.db"
    return JobStore(db_path=db)


def test_add_and_claim_job(store):
    store.register_study("study1", "/tmp/study1.pdf", total_chunks=2)
    added = store.add_job("job1", "/tmp/study1.pdf", "study1", 0, "chunk text", "methods", 100)
    assert added
    # Duplicate insert returns False
    again = store.add_job("job1", "/tmp/study1.pdf", "study1", 0, "chunk text", "methods", 100)
    assert not again


def test_claim_is_atomic(store):
    store.register_study("s", "/tmp/s.pdf", 1)
    store.add_job("j1", "/tmp/s.pdf", "s", 0, "text", None, 50)
    assert store.claim_job("j1") is True
    # Second claim fails
    assert store.claim_job("j1") is False


def test_mark_done_bumps_progress(store):
    store.register_study("s", "/tmp/s.pdf", 2)
    store.add_job("j1", "/tmp/s.pdf", "s", 0, "t1", None, 50)
    store.add_job("j2", "/tmp/s.pdf", "s", 1, "t2", None, 50)
    store.claim_job("j1")
    store.mark_done("j1", {"author": "Smith"}, "gemini-1.5-flash")
    stats = store.stats()
    assert stats.get("done") == 1
    assert stats.get("pending") == 1


def test_completed_study_detected(store):
    store.register_study("s", "/tmp/s.pdf", 1)
    store.add_job("j1", "/tmp/s.pdf", "s", 0, "t", None, 50)
    store.claim_job("j1")
    store.mark_done("j1", {"x": 1}, "gemini")
    completed = store.get_completed_studies()
    assert len(completed) == 1
    assert completed[0]["study_id"] == "s"


def test_mark_failed_requeues(store):
    store.register_study("s", "/tmp/s.pdf", 1)
    store.add_job("j1", "/tmp/s.pdf", "s", 0, "t", None, 50)
    store.claim_job("j1")
    store.mark_failed("j1", "429 rate limit", requeue=True)
    pending = store.get_pending()
    assert len(pending) == 1
    assert pending[0]["retries"] == 1
