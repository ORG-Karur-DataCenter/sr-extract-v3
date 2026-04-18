"""Aggregator: merge chunk-level results into study-level records.

Each PDF (study) is split into N chunks; each chunk produces a partial
extraction (some fields filled, others null). The aggregator consolidates
all chunks for a study into one final record using first-non-null wins
with optional conflict flagging.
"""
from __future__ import annotations
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

from core.job_store import JobStore

log = logging.getLogger(__name__)


@dataclass
class AggregatedStudy:
    study_id: str
    pdf_path: str
    record: dict[str, Any]
    conflicts: dict[str, list[Any]]
    chunks_used: int
    chunks_failed: int


def _is_null(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip().lower() in ("", "null", "none", "n/a", "na", "not reported", "nr"))


def _better(existing: Any, candidate: Any) -> Any:
    """Return the better of two candidate values for the same field.

    Rules:
      - If existing is null, use candidate.
      - If candidate is null, keep existing.
      - If both present and equal, keep existing.
      - If both present and differ, keep the longer/more detailed one
        and flag the conflict for user review.
    """
    if _is_null(existing):
        return candidate
    if _is_null(candidate):
        return existing
    if existing == candidate:
        return existing
    # Different non-null values — prefer the longer string (more info)
    if isinstance(existing, str) and isinstance(candidate, str):
        return existing if len(existing) >= len(candidate) else candidate
    return existing


def aggregate_study(store: JobStore, study_id: str,
                    expected_fields: list[str]) -> Optional[AggregatedStudy]:
    """Merge all chunk results for a single study into one record."""
    rows = store.get_study_chunks(study_id)
    if not rows:
        return None

    record: dict[str, Any] = {f: None for f in expected_fields}
    conflicts: dict[str, list[Any]] = defaultdict(list)
    used = 0
    failed = 0
    pdf_path = rows[0]["pdf_path"]

    for row in rows:
        if row["status"] != "done":
            failed += 1
            continue
        try:
            chunk_data = json.loads(row["result_json"] or "{}")
        except json.JSONDecodeError:
            failed += 1
            continue
        used += 1
        for field_name in expected_fields:
            candidate = chunk_data.get(field_name)
            if _is_null(candidate):
                continue
            existing = record[field_name]
            if _is_null(existing):
                record[field_name] = candidate
            elif existing != candidate:
                # Conflict: track both values for user review
                conflicts[field_name].append(candidate)
                record[field_name] = _better(existing, candidate)

    # Add study-level metadata
    record["_study_id"] = study_id
    record["_pdf_path"] = pdf_path
    record["_chunks_used"] = used
    record["_chunks_failed"] = failed
    if conflicts:
        record["_conflicts"] = {k: list(set(map(str, v))) for k, v in conflicts.items()}

    return AggregatedStudy(
        study_id=study_id,
        pdf_path=pdf_path,
        record=record,
        conflicts=dict(conflicts),
        chunks_used=used,
        chunks_failed=failed,
    )


def aggregate_all_complete(store: JobStore,
                           expected_fields: list[str]) -> list[AggregatedStudy]:
    """Return aggregated records for every study whose chunks are all done."""
    studies = store.get_completed_studies()
    out = []
    for s in studies:
        agg = aggregate_study(store, s["study_id"], expected_fields)
        if agg:
            out.append(agg)
    return out
