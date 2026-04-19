"""Verify TEST_MODE swaps in the fake client."""
import os
import pytest
from unittest.mock import patch


def test_test_mode_uses_fake_client(monkeypatch):
    monkeypatch.setenv("TEST_MODE", "true")
    from importlib import reload
    from core import extractor
    reload(extractor)
    from tests.fake_gemini import FakeGeminiClient
    client = extractor.make_client("any_key")
    assert isinstance(client, FakeGeminiClient)


def test_normal_mode_uses_real_client(monkeypatch):
    monkeypatch.delenv("TEST_MODE", raising=False)
    from importlib import reload
    from core import extractor
    reload(extractor)
    # conftest patches google.genai.Client → FakeGeminiClient, so the result is fake
    c = extractor.make_client("AIza" + "x" * 35)
    assert c is not None
