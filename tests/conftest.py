"""Global pytest fixtures.

Auto-patches the Gemini client so no test can reach Google, even if
the test author forgets to mock. Also refuses to run if real API
keys are configured.
"""
from __future__ import annotations
import os
import pytest
from unittest.mock import patch

from tests.fake_gemini import FakeGeminiClient


def pytest_configure(config):
    """Fail fast if real keys are set — prevents quota burn."""
    keys = os.getenv("GEMINI_API_KEYS", "").strip()
    if keys and keys != "TEST_KEY_DO_NOT_USE":
        raise pytest.UsageError(
            "Refusing to run tests with real GEMINI_API_KEYS set. "
            "Unset the env var or use the sentinel 'TEST_KEY_DO_NOT_USE'."
        )


@pytest.fixture(autouse=True)
def _patch_gemini(monkeypatch):
    """Replace google.genai.Client with the fake for every test."""
    monkeypatch.setattr("google.genai.Client", FakeGeminiClient)
    yield
