"""Tests for the shutting_down flag."""
import io
import openpyxl
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from server import app


def test_post_jobs_503_when_shutting_down():
    wb = openpyxl.Workbook()
    wb.active.append(["a"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    pdf = Path("tests/fixtures/tiny.pdf").read_bytes()
    with TestClient(app) as c:
        app.state.shutting_down = True
        try:
            r = c.post(
                "/jobs",
                data={"api_keys": "TEST_KEY_DO_NOT_USE", "model": "gemini-2.0-flash"},
                files=[
                    ("pdfs", ("tiny.pdf", pdf, "application/pdf")),
                    ("template", ("t.xlsx", buf.read(),
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
                ],
            )
            assert r.status_code == 503
            assert r.json()["error_code"] == "server_shutdown"
        finally:
            app.state.shutting_down = False


def test_health_still_works_during_shutdown():
    with TestClient(app) as c:
        app.state.shutting_down = True
        try:
            r = c.get("/health")
            assert r.status_code == 200
        finally:
            app.state.shutting_down = False
