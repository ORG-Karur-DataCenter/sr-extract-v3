"""Tests for the in-memory JobContext."""
import time
import pytest

from api.job_context import JobContext, JobStateSnapshot


def test_job_context_init(tmp_path):
    ctx = JobContext(
        job_id="abc", sandbox=tmp_path, api_keys=["k1", "k2"],
        model="gemini-2.0-flash", output_format="xlsx",
    )
    assert ctx.job_id == "abc"
    assert ctx.api_keys == ["k1", "k2"]
    assert ctx.status == "pending"
    assert ctx.created_at <= time.time()


def test_api_keys_wiped_on_dispose(tmp_path):
    ctx = JobContext(
        job_id="abc", sandbox=tmp_path, api_keys=["secret_key"],
        model="gemini-2.0-flash", output_format="xlsx",
    )
    ctx.dispose()
    assert ctx.api_keys == []


def test_snapshot_excludes_keys(tmp_path):
    ctx = JobContext(
        job_id="abc", sandbox=tmp_path, api_keys=["SECRET"],
        model="gemini-2.0-flash", output_format="xlsx",
    )
    snap = ctx.snapshot()
    assert isinstance(snap, JobStateSnapshot)
    serialised = snap.model_dump_json()
    assert "SECRET" not in serialised


def test_touch_updates_timestamp(tmp_path):
    ctx = JobContext(
        job_id="abc", sandbox=tmp_path, api_keys=["k"],
        model="gemini-2.0-flash", output_format="xlsx",
    )
    before = ctx.updated_at
    time.sleep(0.01)
    ctx.touch()
    assert ctx.updated_at > before
