# sr-extract-v3

**Systematic Review Extraction Agent — V3**

Token-aware, queue-based, crash-safe PDF extraction pipeline for systematic reviews. Built to survive API rate limits and model overload without burning through keys.

---

## Why V3 exists

V1 (web app) and V2 (CLI agent) both failed at scale: 10 API keys exhausted in 2 runs, retry storms, lost progress on crashes. V3 fixes the root causes:

- **Section-based chunking** instead of full-PDF-per-call → ~70% fewer tokens
- **Semaphore-bounded async workers** instead of naive `asyncio.gather()` → no 429 cascades
- **SQLite job state** → restarts skip completed work, zero re-extraction
- **Token-aware key rotation** with 85% safety threshold → never hits the rate wall
- **Jittered exponential backoff** → no retry storms
- **Incremental Excel writes** → every completed study is on disk immediately

---

## Architecture

```
PDFs + Excel template
        ↓
   Smart chunker  (PyMuPDF + section detection + table extraction)
        ↓
   SQLite queue   (pending → in_progress → done / failed)
        ↓
Token-aware key manager  (RPM/TPM tracking, 85% threshold)
        ↓
  Worker pool  (Semaphore(4), async dispatch)
        ↓
 Gemini Flash  ──(on 429/503, retry ≥3x)──→  Claude Haiku (optional)
        ↓
    Validator  (schema check against template)
        ↓
  Result cache  (SQLite; skip on restart)
        ↓
   Aggregator  (merge chunks → study record)
        ↓
  Excel writer  (incremental, crash-safe)
```

---

## Setup

**Requirements:** Python 3.10+

```bash
git clone https://github.com/ORG-Karur-DataCenter/sr-extract-v3.git
cd sr-extract-v3
python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

**Configure keys:**

```bash
cp .env.example .env
# edit .env, add your GEMINI_API_KEYS (comma-separated)
# get keys at: https://aistudio.google.com/app/apikey
```

**Drop your inputs:**

```
data/
├── pdfs/                 # your PDFs here
└── templates/
    └── extraction.xlsx   # your template: first row = field names
```

---

## Usage

```bash
# Full run: ingest + extract + write
python pipeline.py

# Just ingest (chunk PDFs into queue)
python pipeline.py --ingest-only

# Resume after a crash (skip ingest, process pending)
python pipeline.py --resume

# Check queue status
python pipeline.py --status
```

Output lands in `data/outputs/extraction_results.xlsx` — one row per study, columns match your template.

---

## Configuration

All tunables in `config/settings.py`:

| Setting | Default | Purpose |
|---|---|---|
| `MAX_CONCURRENT_WORKERS` | 4 | Parallel API calls (keep ≤ number of keys) |
| `SAFETY_THRESHOLD` | 0.85 | Use only 85% of stated rate limits |
| `MAX_CHUNK_TOKENS` | 6000 | Per-request chunk size |
| `MAX_RETRIES` | 5 | Retry cap before permanent fail |
| `RETRY_MAX_DELAY` | 120s | Backoff cap |

Override any of these via environment variable.

---

## Running tests

```bash
pip install pytest pytest-asyncio
pytest tests/ -v
```

Covers:
- Key manager: rate limiting, threshold, rotation, backoff
- Validator: schema matching, normalization
- Chunker: token estimation, splitting
- Job store: atomic claims, state transitions, study completion

---

## How it handles failure

**Rate limit (429):** Key is marked blocked for `retry_after` seconds. Job requeued with jittered backoff. Other keys keep working.

**Model overload (503):** Job requeued with exponential backoff. After 3 retries, if `CLAUDE_API_KEY` is set, falls back to Claude Haiku for that chunk.

**Crash / power loss:** SQLite WAL mode persists everything. Restart with `--resume` and the pipeline picks up exactly where it left off — zero re-extraction.

**Bad JSON from model:** Caught at parse time, marked permanent failure. Study still completes if other chunks succeed; missing fields remain null in output.

---

## Scaling notes

For 1000+ PDFs:
- Free Gemini tier (15 RPM per key): expect ~50 PDFs/day/key
- With 10 keys + Claude Haiku fallback: ~1 day for 1000 PDFs
- WAL-mode SQLite handles 100k+ rows without issue

For multi-machine scaling, swap `core/job_store.py` for a Redis-backed queue (same interface). Not currently needed.

---

## Project layout

```
sr-extract-v3/
├── config/settings.py       # all tunables
├── core/
│   ├── chunker.py           # PDF → chunks
│   ├── job_store.py         # SQLite state machine
│   ├── key_manager.py       # token-aware key rotator
│   ├── extractor.py         # Gemini + Claude clients
│   ├── validator.py         # schema check
│   ├── worker.py            # async worker pool
│   └── aggregator.py        # chunks → study record
├── output/writer.py         # incremental Excel writer
├── ingest.py                # PDF + template loader
├── pipeline.py              # orchestrator (main entry)
├── tests/                   # unit tests
└── data/
    ├── pdfs/                # your inputs
    ├── templates/           # your template .xlsx
    └── outputs/             # extraction results
```

---

## License

MIT
