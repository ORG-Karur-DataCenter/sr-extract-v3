"""Fake Gemini client used by tests and TEST_MODE.

Never makes a real network call. Returns canned JSON loaded from
tests/fixtures/gemini_responses/.
"""
from __future__ import annotations
import json
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures" / "gemini_responses"


class FakeGeminiResponse:
    def __init__(self, text: str):
        self.text = text


class FakeGeminiClient:
    """Drop-in stand-in for google.genai.Client."""

    def __init__(self, api_key: str = "", *, fixture: str = "sample_study"):
        self._fixture = fixture
        self.models = self  # extractor calls client.models.generate_content

    def generate_content(self, *, model: str, contents, **_kwargs):
        path = FIXTURES / f"{self._fixture}.json"
        if not path.exists():
            available = [p.stem for p in FIXTURES.glob("*.json")]
            raise FileNotFoundError(
                f"FakeGeminiClient: fixture '{self._fixture}' not found at {path}. "
                f"Available: {available}"
            )
        payload = json.loads(path.read_text())
        return FakeGeminiResponse(text=json.dumps(payload))
