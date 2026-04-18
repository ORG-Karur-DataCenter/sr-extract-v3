"""Chunker tests — focus on token estimation + packing, not real PDFs.

Real PDF testing lives in integration tests (require sample files).
"""
import pytest

from core.chunker import Chunker, estimate_tokens


def test_token_estimate_nonzero():
    assert estimate_tokens("Hello world, this is a test sentence.") > 5


def test_token_estimate_scales():
    short = estimate_tokens("cat")
    long = estimate_tokens("cat " * 500)
    assert long > short * 100


def test_split_by_tokens_respects_cap():
    c = Chunker(max_tokens=50, min_tokens=10, overlap=5)
    text = ("This is one sentence. " * 40).strip()
    parts = c._split_by_tokens(text, 50, 5)
    for p in parts:
        assert estimate_tokens(p) <= 55  # small overrun ok due to sentence boundary


def test_split_short_text_one_chunk():
    c = Chunker(max_tokens=500, min_tokens=10, overlap=20)
    parts = c._split_by_tokens("A short sentence.", 500, 20)
    assert len(parts) == 1
