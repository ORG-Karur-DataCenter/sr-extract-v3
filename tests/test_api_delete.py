"""Tests for DELETE /jobs/{id}."""
import io
import openpyxl
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from server import app


def _start(c):
    wb = openpyxl.Workbook()
    wb.active.append(["a"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    pdf = Path("tests/fixtures/tiny.pdf").read_bytes()
    r = c.post(
        "/jobs",
        data={"api_keys": "TEST_KEY_DO_NOT_USE", "model": "gemini-2.0-flash"},
        files=[
            ("pdfs", ("tiny.pdf", pdf, "application/pdf")),
            ("template", ("t.xlsx", buf.read(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
        ],
    )
    return r.json()["job_id"]


def test_delete_cancels_job():
    with TestClient(app) as c:
        jid = _start(c)
        r = c.delete(f"/jobs/{jid}")
        assert r.status_code == 204
        s = c.get(f"/jobs/{jid}/status")
        assert s.status_code == 404


def test_delete_404_unknown():
    with TestClient(app) as c:
        r = c.delete("/jobs/no-such")
    assert r.status_code == 404
