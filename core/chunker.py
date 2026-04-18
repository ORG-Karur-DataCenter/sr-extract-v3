"""Smart PDF chunker.

Goal: minimum tokens sent to LLM, maximum extraction accuracy.

Strategy:
1. Extract text per page with PyMuPDF
2. Detect section headings via regex
3. Keep only sections relevant to systematic review extraction
4. Extract tables separately as structured JSON
5. Fall back to sliding window if sections undetectable
"""
from __future__ import annotations
import re
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import fitz  # PyMuPDF
import tiktoken

from config.settings import (
    RELEVANT_SECTIONS, SKIP_SECTIONS,
    MAX_CHUNK_TOKENS, MIN_CHUNK_TOKENS, CHUNK_OVERLAP_TOKENS,
)

_ENC = tiktoken.get_encoding("cl100k_base")

# Heading pattern: bold/caps lines, numbered sections, common SR headings
_HEADING_RE = re.compile(
    r"^\s*(?:"
    r"(?:\d+\.?\d*\.?\s+)?"
    r"(?:Abstract|Introduction|Background|Methods?|Methodology|"
    r"Study\s+design|Population|Participants|Intervention|Comparator|"
    r"Outcomes?|Results?|Discussion|Conclusions?|"
    r"Statistical\s+[Aa]nalys[ie]s|Data\s+[Ee]xtraction|"
    r"Risk\s+of\s+[Bb]ias|Baseline|Characteristics|"
    r"References?|Bibliography|Acknowledgm?ents?|Funding|"
    r"Conflicts?\s+of\s+[Ii]nterest|Author\s+[Cc]ontributions?|"
    r"Supplementary|Appendix"
    r")"
    r")\s*\.?\s*$",
    re.MULTILINE | re.IGNORECASE,
)


def estimate_tokens(text: str) -> int:
    """Approximate token count. cl100k_base is a reasonable proxy for Gemini."""
    return len(_ENC.encode(text))


@dataclass
class Chunk:
    study_id: str
    chunk_index: int
    text: str
    section_name: Optional[str]
    token_estimate: int
    source_pages: list[int]

    @property
    def chunk_id(self) -> str:
        h = hashlib.md5(f"{self.study_id}-{self.chunk_index}".encode()).hexdigest()[:12]
        return f"chunk_{self.study_id}_{self.chunk_index}_{h}"


class Chunker:
    def __init__(self,
                 max_tokens: int = MAX_CHUNK_TOKENS,
                 min_tokens: int = MIN_CHUNK_TOKENS,
                 overlap: int = CHUNK_OVERLAP_TOKENS):
        self.max_tokens = max_tokens
        self.min_tokens = min_tokens
        self.overlap = overlap

    # ── Public API ───────────────────────────────────────────────────
    def chunk_pdf(self, pdf_path: Path) -> list[Chunk]:
        """Chunk a PDF into LLM-ready pieces."""
        study_id = pdf_path.stem
        doc = fitz.open(pdf_path)
        try:
            sections = self._extract_sections(doc)
            if not sections:
                # No headings detected — fall back to sliding window
                sections = self._sliding_window(doc)
            tables = self._extract_tables(doc)
        finally:
            doc.close()

        # Merge sections into max_tokens-sized chunks
        chunks = self._pack_sections(sections, study_id)

        # Append each table as its own chunk (preserves structure)
        for tbl in tables:
            chunks.append(Chunk(
                study_id=study_id,
                chunk_index=len(chunks),
                text=tbl["text"],
                section_name=f"table_page_{tbl['page']}",
                token_estimate=estimate_tokens(tbl["text"]),
                source_pages=[tbl["page"]],
            ))
        return chunks

    # ── Section extraction ───────────────────────────────────────────
    def _extract_sections(self, doc: fitz.Document) -> list[dict]:
        """Split the PDF into labeled sections using heading regex."""
        full_text = ""
        page_offsets = []
        for page_num, page in enumerate(doc):
            page_offsets.append((page_num, len(full_text)))
            full_text += page.get_text() + "\n"

        # Find all headings
        matches = list(_HEADING_RE.finditer(full_text))
        if not matches:
            return []

        sections = []
        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
            heading = m.group(0).strip()
            body = full_text[start:end].strip()
            section_key = heading.lower().strip(" .0123456789")

            # Filter out sections we don't care about
            if any(skip in section_key for skip in SKIP_SECTIONS):
                continue

            # Determine page range
            pages = self._map_pages(start, end, page_offsets)
            sections.append({
                "name": heading,
                "text": body,
                "pages": pages,
                "relevant": any(rel in section_key for rel in RELEVANT_SECTIONS),
            })

        # Prefer relevant sections; if any are relevant, drop the rest
        relevant = [s for s in sections if s["relevant"]]
        return relevant if relevant else sections

    def _map_pages(self, start: int, end: int,
                   offsets: list[tuple[int, int]]) -> list[int]:
        pages = set()
        for page_num, off in offsets:
            if off <= start < (offsets[page_num + 1][1] if page_num + 1 < len(offsets) else float("inf")):
                pages.add(page_num)
            elif start <= off < end:
                pages.add(page_num)
        return sorted(pages) or [0]

    def _sliding_window(self, doc: fitz.Document) -> list[dict]:
        """Fallback: 3-page windows with half-page overlap."""
        sections = []
        n = len(doc)
        for i in range(0, n, 2):
            pages = list(range(i, min(i + 3, n)))
            text = "\n".join(doc[p].get_text() for p in pages)
            sections.append({
                "name": f"pages_{pages[0] + 1}-{pages[-1] + 1}",
                "text": text,
                "pages": pages,
                "relevant": True,
            })
        return sections

    # ── Table extraction ─────────────────────────────────────────────
    def _extract_tables(self, doc: fitz.Document) -> list[dict]:
        out = []
        for page_num, page in enumerate(doc):
            try:
                tables = page.find_tables()
            except Exception:
                continue
            for tbl in tables or []:
                try:
                    rows = tbl.extract()
                except Exception:
                    continue
                if not rows or len(rows) < 2:
                    continue
                # Render as pipe-delimited — more compact than markdown tables
                txt = "\n".join(
                    " | ".join(str(c) if c else "" for c in row) for row in rows
                )
                if estimate_tokens(txt) > 50:  # skip trivial tables
                    out.append({"page": page_num, "text": f"[TABLE p.{page_num + 1}]\n{txt}"})
        return out

    # ── Chunk packing ────────────────────────────────────────────────
    def _pack_sections(self, sections: list[dict], study_id: str) -> list[Chunk]:
        chunks = []
        for sec in sections:
            tokens = estimate_tokens(sec["text"])
            if tokens <= self.max_tokens:
                chunks.append(Chunk(
                    study_id=study_id,
                    chunk_index=len(chunks),
                    text=sec["text"],
                    section_name=sec["name"],
                    token_estimate=tokens,
                    source_pages=sec["pages"],
                ))
            else:
                # Split large section on sentence boundaries
                for part in self._split_by_tokens(sec["text"], self.max_tokens, self.overlap):
                    chunks.append(Chunk(
                        study_id=study_id,
                        chunk_index=len(chunks),
                        text=part,
                        section_name=sec["name"],
                        token_estimate=estimate_tokens(part),
                        source_pages=sec["pages"],
                    ))
        return chunks

    def _split_by_tokens(self, text: str, max_tokens: int, overlap: int) -> list[str]:
        sentences = re.split(r"(?<=[.!?])\s+", text)
        out = []
        buf = []
        buf_tokens = 0
        for sent in sentences:
            st = estimate_tokens(sent)
            if buf_tokens + st > max_tokens and buf:
                out.append(" ".join(buf))
                # Keep last few sentences for overlap
                tail = []
                tail_tokens = 0
                for s in reversed(buf):
                    ts = estimate_tokens(s)
                    if tail_tokens + ts > overlap:
                        break
                    tail.insert(0, s)
                    tail_tokens += ts
                buf = tail
                buf_tokens = tail_tokens
            buf.append(sent)
            buf_tokens += st
        if buf:
            out.append(" ".join(buf))
        return out
