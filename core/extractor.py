"""LLM extractor.

Primary: Gemini 1.5 Flash
Fallback: Claude Haiku (only if CLAUDE_API_KEY set)

Extracts structured fields from a chunk + template schema. Returns a dict
with keys matching the template's field names. Missing fields return None.
"""
from __future__ import annotations
import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

import os as _os

from google import genai
from google.genai import types

from config.settings import (
    GEMINI_MODEL, CLAUDE_MODEL, CLAUDE_API_KEY, USE_CLAUDE_FALLBACK,
)

log = logging.getLogger(__name__)


def make_client(api_key: str):
    """Return a Gemini client. Honours TEST_MODE for smoke/integration tests."""
    if _os.getenv("TEST_MODE", "").lower() == "true":
        from core._test_support.fake_gemini import FakeGeminiClient
        return FakeGeminiClient(api_key=api_key)
    return genai.Client(api_key=api_key)


class RateLimitError(Exception):
    def __init__(self, retry_after: float = 60.0, message: str = ""):
        self.retry_after = retry_after
        super().__init__(message or f"Rate limited, retry in {retry_after}s")


class TransientAPIError(Exception):
    """503, timeout, network hiccup — retry worthwhile."""
    pass


class PermanentAPIError(Exception):
    """4xx that won't be fixed by retrying (e.g. 400 bad request)."""
    pass


@dataclass
class ExtractionResult:
    data: dict
    model_used: str
    tokens_in: int
    tokens_out: int


# ── Prompt construction ──────────────────────────────────────────────
_SYSTEM_PROMPT = (
    "You are a systematic-review data extractor. From the provided text chunk, "
    "extract the listed fields and return ONLY a valid JSON object. "
    "Rules:\n"
    "1. Use exact field names as keys.\n"
    "2. If a field is not present in the chunk, return null for that field.\n"
    "3. For numeric fields, extract the number with units if given (e.g. '45.2 years').\n"
    "4. If median (IQR) is given, also compute mean \u00b1 SD using the Wan method "
    "and include as 'mean_sd_wan' subfield.\n"
    "5. For author names, format as 'LastName et al. (YEAR)'.\n"
    "6. Do not invent data. Return null rather than guessing.\n"
    "7. Return ONLY the JSON object, no prose, no markdown fences."
)


def build_prompt(chunk_text: str, fields: list[str], section_name: Optional[str]) -> str:
    fields_json = json.dumps(fields, indent=2)
    section_hint = f"\nSection: {section_name}" if section_name else ""
    return (
        f"Fields to extract:\n{fields_json}\n"
        f"{section_hint}\n\n"
        f"=== CHUNK TEXT ===\n{chunk_text}\n=== END CHUNK ===\n\n"
        f"Return JSON:"
    )


# ── JSON parsing with recovery ───────────────────────────────────────
_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def parse_json_response(text: str) -> dict:
    """Extract JSON from model output. Handles fenced blocks and extra prose."""
    text = text.strip()
    # Strip markdown fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = _JSON_RE.search(text)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
        raise PermanentAPIError(f"Could not parse JSON from response: {text[:200]}")


# ── Gemini client (google.genai SDK) ─────────────────────────────────
async def call_gemini(api_key: str, prompt: str) -> tuple[str, int, int]:
    """Invoke Gemini. Returns (text, tokens_in, tokens_out).

    Raises RateLimitError on 429, TransientAPIError on 503/timeout,
    PermanentAPIError on 4xx.
    """
    def _sync():
        client = make_client(api_key=api_key)
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=_SYSTEM_PROMPT,
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
        except Exception as e:
            msg = str(e).lower()
            if "404" in msg or "not found" in msg:
                raise PermanentAPIError(f"Model not found: {e}")
            if "429" in msg or "quota" in msg or "rate" in msg:
                raise RateLimitError(retry_after=60, message=str(e))
            if "503" in msg or "overload" in msg or "unavailable" in msg:
                raise TransientAPIError(str(e))
            if "500" in msg or "internal" in msg:
                raise TransientAPIError(str(e))
            raise PermanentAPIError(str(e))
        text = resp.text or ""
        usage = getattr(resp, "usage_metadata", None)
        tin = getattr(usage, "prompt_token_count", 0) if usage else 0
        tout = getattr(usage, "candidates_token_count", 0) if usage else 0
        return text, tin, tout

    return await asyncio.to_thread(_sync)


# ── Claude fallback (optional) ───────────────────────────────────────
async def call_claude(prompt: str) -> tuple[str, int, int]:
    """Claude Haiku fallback. Only used if CLAUDE_API_KEY is set."""
    if not CLAUDE_API_KEY:
        raise PermanentAPIError("No Claude API key configured")
    try:
        import anthropic
    except ImportError:
        raise PermanentAPIError("anthropic package not installed")

    def _sync():
        client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
        try:
            resp = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            msg = str(e).lower()
            if "429" in msg or "rate" in msg:
                raise RateLimitError(retry_after=60, message=str(e))
            if "503" in msg or "overload" in msg:
                raise TransientAPIError(str(e))
            raise PermanentAPIError(str(e))
        text = resp.content[0].text if resp.content else ""
        tin = resp.usage.input_tokens
        tout = resp.usage.output_tokens
        return text, tin, tout

    return await asyncio.to_thread(_sync)


# ── Unified extraction ───────────────────────────────────────────────
async def extract_chunk(chunk_text: str,
                        fields: list[str],
                        section_name: Optional[str],
                        gemini_key: str,
                        use_fallback: bool = False) -> ExtractionResult:
    """High-level extract. Tries Gemini, optionally falls back to Claude."""
    prompt = build_prompt(chunk_text, fields, section_name)

    if not use_fallback:
        text, tin, tout = await call_gemini(gemini_key, prompt)
        data = parse_json_response(text)
        return ExtractionResult(data=data, model_used=GEMINI_MODEL,
                                tokens_in=tin, tokens_out=tout)
    else:
        if not USE_CLAUDE_FALLBACK:
            raise PermanentAPIError("Fallback requested but no Claude key configured")
        text, tin, tout = await call_claude(prompt)
        data = parse_json_response(text)
        return ExtractionResult(data=data, model_used=CLAUDE_MODEL,
                                tokens_in=tin, tokens_out=tout)
