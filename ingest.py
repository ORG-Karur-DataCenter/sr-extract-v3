"""Ingestion pipeline.

Reads:
  - PDFs from data/pdfs/
  - Extraction template from data/templates/*.xlsx (first sheet, first row = field names)

Produces:
  - Job rows in SQLite, one per chunk, status='pending'
  - Template schema persisted for the extractor to reuse
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any

import openpyxl

from config.settings import PDF_DIR, TEMPLATE_DIR, DATA_DIR
from core.chunker import Chunker
from core.job_store import JobStore

log = logging.getLogger(__name__)

TEMPLATE_CACHE = DATA_DIR / "template_schema.json"


def load_template(template_path: Path) -> dict[str, Any]:
    """Extract field names from the first row of the first sheet.

    Supports fully dynamic templates. Field types are all 'string' by default —
    the LLM returns text, and downstream validation is flexible.
    """
    wb = openpyxl.load_workbook(template_path, read_only=True, data_only=True)
    ws = wb.active
    headers = []
    first_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
    if not first_row:
        raise ValueError(f"Template {template_path} has no header row")
    for cell in first_row:
        if cell is None:
            continue
        name = str(cell).strip()
        if name:
            headers.append(name)
    wb.close()

    if not headers:
        raise ValueError(f"Template {template_path} has no usable headers")

    schema = {
        "template_path": str(template_path),
        "fields": headers,
        "field_count": len(headers),
    }
    TEMPLATE_CACHE.write_text(json.dumps(schema, indent=2))
    log.info(f"Loaded template with {len(headers)} fields: {headers[:5]}...")
    return schema


def discover_pdfs(pdf_dir: Path = PDF_DIR) -> list[Path]:
    return sorted(pdf_dir.glob("*.pdf"))


def discover_template(template_dir: Path = TEMPLATE_DIR) -> Path:
    candidates = list(template_dir.glob("*.xlsx"))
    if not candidates:
        raise FileNotFoundError(
            f"No .xlsx template in {template_dir}. "
            "Drop your extraction template there."
        )
    if len(candidates) > 1:
        log.warning(f"Multiple templates found, using {candidates[0].name}")
    return candidates[0]


def ingest_all(store: JobStore,
               pdf_dir: Path = PDF_DIR,
               template_dir: Path = TEMPLATE_DIR) -> dict:
    """Main ingestion entrypoint.

    Returns: dict with counts {pdfs, chunks, skipped_existing}
    """
    template_path = discover_template(template_dir)
    schema = load_template(template_path)

    pdfs = discover_pdfs(pdf_dir)
    if not pdfs:
        raise FileNotFoundError(f"No PDFs in {pdf_dir}")
    log.info(f"Found {len(pdfs)} PDFs, {schema['field_count']} template fields")

    chunker = Chunker()
    total_chunks = 0
    skipped = 0

    for pdf in pdfs:
        study_id = pdf.stem
        try:
            chunks = chunker.chunk_pdf(pdf)
        except Exception as e:
            log.error(f"Chunking failed for {pdf.name}: {e}")
            continue

        if not chunks:
            log.warning(f"No chunks produced for {pdf.name}")
            continue

        store.register_study(study_id, str(pdf), len(chunks))

        for chunk in chunks:
            added = store.add_job(
                job_id=chunk.chunk_id,
                pdf_path=str(pdf),
                study_id=study_id,
                chunk_index=chunk.chunk_index,
                chunk_text=chunk.text,
                section_name=chunk.section_name,
                token_estimate=chunk.token_estimate,
            )
            if added:
                total_chunks += 1
            else:
                skipped += 1

        log.info(f"  {pdf.name}: {len(chunks)} chunks")

    return {
        "pdfs": len(pdfs),
        "chunks_new": total_chunks,
        "chunks_skipped": skipped,
        "template_fields": schema["field_count"],
    }


def get_cached_schema() -> dict[str, Any]:
    if not TEMPLATE_CACHE.exists():
        raise FileNotFoundError("Template not ingested yet. Run ingest_all() first.")
    return json.loads(TEMPLATE_CACHE.read_text())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = JobStore()
    result = ingest_all(store)
    print(json.dumps(result, indent=2))
    print("DB stats:", store.stats())
