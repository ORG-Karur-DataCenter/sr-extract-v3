"""Security middleware: log redaction, CORS, rate limits, size cap."""
from __future__ import annotations
import logging
import os
import re

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

# ---------------------------------------------------------------------------
# Log redaction
# ---------------------------------------------------------------------------

GEMINI_KEY_RE = re.compile(r"AIza[0-9A-Za-z_\-]{35}")


class RedactGeminiKeysFilter(logging.Filter):
    """Redact Gemini-shaped API keys from log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        if GEMINI_KEY_RE.search(msg):
            record.msg = GEMINI_KEY_RE.sub("[REDACTED_GEMINI_KEY]", msg)
            record.args = ()
        return True


def install_redaction(root_logger: logging.Logger | None = None) -> None:
    """Install the redaction filter on every handler of the root logger."""
    logger = root_logger or logging.getLogger()
    for h in logger.handlers:
        h.addFilter(RedactGeminiKeysFilter())


# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

def parse_origins(raw: str) -> list[str]:
    return [o.strip() for o in raw.split(",") if o.strip()]


def install_cors(app: FastAPI) -> None:
    raw = os.getenv(
        "CORS_ALLOWED_ORIGINS",
        "https://org-karur-datacenter.github.io,http://localhost:5500,http://127.0.0.1:5500",
    )
    origins = parse_origins(raw)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Accept"],
    )


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

def install_rate_limiter(app: FastAPI) -> Limiter:
    # No default_limits — we apply limits explicitly per route so that
    # polling endpoints (GET /health, GET /jobs/{id}/status) are never
    # throttled. Only POST /jobs (job creation) is rate-limited.
    limiter = Limiter(
        key_func=get_remote_address,
        default_limits=[],
    )
    app.state.limiter = limiter
    app.add_middleware(SlowAPIMiddleware)

    @app.exception_handler(RateLimitExceeded)
    async def _rate_limit_handler(request, exc):  # pragma: no cover
        return JSONResponse(
            status_code=429,
            content={"error_code": "rate_limit_exceeded",
                     "error_message": f"Rate limit exceeded: {exc.detail}"},
            headers={"Retry-After": "60"},
        )

    return limiter


# ---------------------------------------------------------------------------
# Upload size cap
# ---------------------------------------------------------------------------

class MaxBodySizeMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_bytes: int):
        super().__init__(app)
        self.max_bytes = max_bytes

    async def dispatch(self, request: Request, call_next):
        cl = request.headers.get("content-length")
        if cl and cl.isdigit() and int(cl) > self.max_bytes:
            return JSONResponse(
                status_code=413,
                content={"error_code": "payload_too_large",
                         "error_message": f"Max {self.max_bytes} bytes"},
            )
        return await call_next(request)


def install_size_limit(app: FastAPI, max_bytes: int | None = None) -> None:
    if max_bytes is None:
        max_bytes = int(os.getenv("MAX_UPLOAD_BYTES", str(50 * 1024 * 1024)))
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=max_bytes)
