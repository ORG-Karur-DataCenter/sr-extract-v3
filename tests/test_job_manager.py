"""Tests for JobManager: isolation, cancellation, cleanup, concurrency."""
import asyncio
import pytest
from pathlib import Path

from api.job_manager import JobManager, JobNotFoundError, ServerBusyError


@pytest.fixture
def manager(tmp_path, monkeypatch):
    monkeypatch.setenv("MAX_CONCURRENT_JOBS", "2")
    monkeypatch.setenv("JOB_TIMEOUT_SECONDS", "60")
    return JobManager(jobs_root=tmp_path / "jobs")


def test_create_job_builds_sandbox(manager):
    ctx = manager.create_job(api_keys=["k"], model="gemini-2.0-flash",
                             output_format="xlsx")
    assert ctx.sandbox.is_dir()
    assert (ctx.sandbox / "pdfs").is_dir()
    assert ctx.job_id in manager._jobs


def test_two_jobs_have_isolated_sandboxes(manager):
    a = manager.create_job(api_keys=["k"], model="gemini-2.0-flash",
                           output_format="xlsx")
    b = manager.create_job(api_keys=["k"], model="gemini-2.0-flash",
                           output_format="xlsx")
    assert a.sandbox != b.sandbox
    assert a.job_id != b.job_id


def test_get_raises_for_unknown(manager):
    with pytest.raises(JobNotFoundError):
        manager.get("no-such-id")


def test_concurrency_cap_enforced(manager):
    manager.create_job(api_keys=["k"], model="gemini-2.0-flash",
                       output_format="xlsx")
    manager.create_job(api_keys=["k"], model="gemini-2.0-flash",
                       output_format="xlsx")
    with pytest.raises(ServerBusyError):
        manager.create_job(api_keys=["k"], model="gemini-2.0-flash",
                           output_format="xlsx")


def test_dispose_wipes_sandbox_and_keys(manager):
    ctx = manager.create_job(api_keys=["SECRET"], model="gemini-2.0-flash",
                             output_format="xlsx")
    sandbox = ctx.sandbox
    manager.dispose(ctx.job_id)
    assert not sandbox.exists()
    assert ctx.api_keys == []
    with pytest.raises(JobNotFoundError):
        manager.get(ctx.job_id)


def test_startup_wipes_existing_jobs_dir(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    orphan = jobs / "orphan-id"
    orphan.mkdir()
    (orphan / "marker.txt").write_text("leftover")
    _ = JobManager(jobs_root=jobs)
    assert not orphan.exists()
    assert jobs.is_dir()
