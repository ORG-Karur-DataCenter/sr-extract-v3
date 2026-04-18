"""Extraction result validator.

Lightweight schema validation — every field from the template should
appear in the result dict (value may be null). Flags missing/extra fields.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ValidationReport:
    ok: bool
    missing_fields: list[str] = field(default_factory=list)
    extra_fields: list[str] = field(default_factory=list)
    non_null_count: int = 0
    total_expected: int = 0


def validate(result: dict[str, Any], expected_fields: list[str]) -> ValidationReport:
    """Check a single chunk's extraction against template schema."""
    expected_set = set(expected_fields)
    got_set = set(result.keys())

    missing = sorted(expected_set - got_set)
    extra = sorted(got_set - expected_set)
    non_null = sum(1 for k in expected_set & got_set
                   if result.get(k) not in (None, "", [], {}))

    # A result is "ok" if it parsed and has at least 1 non-null field.
    # Missing fields are acceptable at the chunk level — they may be in another chunk.
    ok = non_null > 0

    return ValidationReport(
        ok=ok,
        missing_fields=missing,
        extra_fields=extra,
        non_null_count=non_null,
        total_expected=len(expected_fields),
    )


def normalize_result(result: dict[str, Any], expected_fields: list[str]) -> dict:
    """Ensure the result dict has every expected key. Fill missing with None.
    Strip extra keys to keep output clean."""
    return {f: result.get(f) for f in expected_fields}
