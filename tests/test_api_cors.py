"""Verify CORS headers on the live app."""
from fastapi.testclient import TestClient
from server import app


def test_cors_preflight_allows_github_pages():
    with TestClient(app) as c:
        r = c.options("/jobs", headers={
            "Origin": "https://org-karur-datacenter.github.io",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type",
        })
    assert r.status_code == 200
    assert r.headers.get("access-control-allow-origin") == \
        "https://org-karur-datacenter.github.io"


def test_cors_rejects_unknown_origin():
    with TestClient(app) as c:
        r = c.options("/jobs", headers={
            "Origin": "https://evil.example",
            "Access-Control-Request-Method": "POST",
        })
    assert r.headers.get("access-control-allow-origin") != "https://evil.example"
