"""Tests for CORS origin parsing + slowapi wiring."""
from api.security import parse_origins, install_rate_limiter


def test_parse_origins_splits_and_trims():
    assert parse_origins("a.com, b.com ,c.com") == ["a.com", "b.com", "c.com"]


def test_parse_origins_ignores_empty():
    assert parse_origins("a.com,,b.com") == ["a.com", "b.com"]


def test_install_rate_limiter_returns_limiter():
    from fastapi import FastAPI
    app = FastAPI()
    limiter = install_rate_limiter(app)
    assert limiter is not None
    assert hasattr(limiter, "limit")
