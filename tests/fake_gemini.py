"""Thin re-export for backward compatibility.

The real implementation lives at core/_test_support/fake_gemini.py so
it ships in the Docker image (tests/ is excluded by .dockerignore).
"""
from core._test_support.fake_gemini import FakeGeminiClient, FakeGeminiResponse

__all__ = ["FakeGeminiClient", "FakeGeminiResponse"]
