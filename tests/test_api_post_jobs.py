"""Tests for POST /jobs."""
import io
from pathlib import Path

import openpyxl
import pytest
from fastapi.testclient import TestClient

from server import app


@pytest.fixture
def template_bytes():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["author", "year", "title", "population", "sample_size"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


@pytest.fixture
def pdf_bytes():
    return Path("tests/fixtures/tiny.pdf").read_bytes()


def _files(pdf, tpl):
    return [
        ("pdfs", ("tiny.pdf", pdf, "application/pdf")),
        ("template", ("t.xlsx", tpl,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
    ]


def test_post_jobs_accepts_valid_request(pdf_bytes, template_bytes):
    with TestClient(app) as c:
        r = c.post(
            "/jobs",
            data={"api_keys": "TEST_KEY_DO_NOT_USE", "model": "gemini-2.0-flash"},
            files=_files(pdf_bytes, template_bytes),
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["job_id"]
    assert body["accepted_at"]


def test_post_jobs_rejects_missing_keys(pdf_bytes, template_bytes):
    with TestClient(app) as c:
        r = c.post("/jobs",
                   data={"api_keys": "", "model": "gemini-2.0-flash"},
                   files=_files(pdf_bytes, template_bytes))
    assert r.status_code == 400
    assert r.json()["error_code"] == "missing_api_keys"


def test_post_jobs_rejects_unknown_model(pdf_bytes, template_bytes):
    with TestClient(app) as c:
        r = c.post("/jobs",
                   data={"api_keys": "k", "model": "gpt-4"},
                   files=_files(pdf_bytes, template_bytes))
    assert r.status_code == 422
    assert r.json()["error_code"] == "invalid_model"


def test_post_jobs_rejects_no_pdfs(template_bytes):
    with TestClient(app) as c:
        r = c.post("/jobs",
                   data={"api_keys": "k", "model": "gemini-2.0-flash"},
                   files=[("template", ("t.xlsx", template_bytes,
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))])
    assert r.status_code == 400
    assert r.json()["error_code"] == "missing_pdfs"
