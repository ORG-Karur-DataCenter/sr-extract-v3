"""Tests for GET /jobs/{id}/status."""
import io
import openpyxl
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from server import app


def _make_template_bytes():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["author", "year"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


def test_status_returns_shape():
    pdf = Path("tests/fixtures/tiny.pdf").read_bytes()
    tpl = _make_template_bytes()
    with TestClient(app) as c:
        r = c.post(
            "/jobs",
            data={"api_keys": "TEST_KEY_DO_NOT_USE", "model": "gemini-2.0-flash"},
            files=[
                ("pdfs", ("tiny.pdf", pdf, "application/pdf")),
                ("template", ("t.xlsx", tpl,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
            ],
        )
        job_id = r.json()["job_id"]
        r2 = c.get(f"/jobs/{job_id}/status")
    assert r2.status_code == 200
    body = r2.json()
    assert body["job_id"] == job_id
    assert body["status"] in ("pending", "running", "done", "failed")
    assert "progress" in body
    assert "chunks_total" in body["progress"]


def test_status_404_for_unknown():
    with TestClient(app) as c:
        r = c.get("/jobs/does-not-exist/status")
    assert r.status_code == 404
    assert r.json()["error_code"] == "job_not_found"
