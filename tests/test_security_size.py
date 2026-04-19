"""Tests for request body size cap."""
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.security import install_size_limit


def test_rejects_oversized_body():
    app = FastAPI()
    install_size_limit(app, max_bytes=100)

    @app.post("/x")
    async def _x(): return {"ok": True}

    client = TestClient(app)
    r = client.post("/x", content=b"a" * 200, headers={"Content-Length": "200"})
    assert r.status_code == 413
    assert r.json()["error_code"] == "payload_too_large"


def test_allows_small_body():
    app = FastAPI()
    install_size_limit(app, max_bytes=1000)

    @app.post("/x")
    async def _x(): return {"ok": True}

    client = TestClient(app)
    r = client.post("/x", content=b"a" * 50)
    assert r.status_code == 200
