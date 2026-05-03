"""Tests for GET /jobs/{id}/result."""
import io
import openpyxl
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from server import app


def test_result_404_unknown():
    with TestClient(app) as c:
        r = c.get("/jobs/does-not-exist/result")
    assert r.status_code == 404


def test_result_409_or_200_when_not_yet_done():
    wb = openpyxl.Workbook()
    wb.active.append(["a"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    pdf = Path("tests/fixtures/tiny.pdf").read_bytes()
    with TestClient(app) as c:
        r = c.post(
            "/jobs",
            data={"api_keys": "TEST_KEY_DO_NOT_USE", "model": "gemini-2.0-flash"},
            files=[
                ("pdfs", ("tiny.pdf", pdf, "application/pdf")),
                ("template", ("t.xlsx", buf.read(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
            ],
        )
        job_id = r.json()["job_id"]
        r2 = c.get(f"/jobs/{job_id}/result")
    assert r2.status_code in (200, 409)
