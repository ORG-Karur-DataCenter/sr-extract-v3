"""Test the /health endpoint."""
from fastapi.testclient import TestClient
from server import app


def test_health_returns_ok():
    with TestClient(app) as c:
        r = c.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "active_jobs" in body
    assert "uptime_seconds" in body
