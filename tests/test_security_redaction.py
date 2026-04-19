"""Tests for the log redaction filter."""
import logging
from io import StringIO

from api.security import RedactGeminiKeysFilter


def test_filter_redacts_gemini_key():
    buf = StringIO()
    handler = logging.StreamHandler(buf)
    handler.addFilter(RedactGeminiKeysFilter())
    logger = logging.getLogger("test.redact")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)

    logger.info("Using key AIzaSyAbCdEfGhIjKlMnOpQrStUvWxYz0123456789 for request")
    out = buf.getvalue()
    assert "AIzaSy" not in out
    assert "[REDACTED_GEMINI_KEY]" in out


def test_filter_leaves_other_strings_alone():
    buf = StringIO()
    handler = logging.StreamHandler(buf)
    handler.addFilter(RedactGeminiKeysFilter())
    logger = logging.getLogger("test.redact2")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)

    logger.info("No secret here, just plain text")
    assert "plain text" in buf.getvalue()
