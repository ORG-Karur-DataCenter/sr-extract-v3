# sr-extract-v3 — Systematic Review Extraction Agent V3

A production-grade, token-aware, crash-safe PDF extraction pipeline for systematic reviews.

## Features
- Smart PDF section-based chunking (PyMuPDF)
- SQLite job queue with full state persistence (resume on crash)
- Token-aware API key rotation with 85% RPM/TPM threshold guard
- Async worker pool with semaphore-controlled concurrency (max 4)
- Jittered exponential backoff — no retry storms
- Incremental Excel + CSV output (written on each completion)
- Dynamic extraction template support (fully configurable per review)
- Gemini 1.5 Flash primary · Claude Haiku fallback slot ready

## Setup

```bash
git clone https://github.com/ORG-Karur-DataCenter/sr-extract-v3
cd sr-extract-v3
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp config/keys.env.example config/keys.env
# Edit config/keys.env with your API keys
```

## Usage

```bash
# 1. Place PDFs in data/pdfs/
# 2. Place your Excel template in data/templates/
# 3. Run ingestion
python ingest.py --pdfs data/pdfs/ --template data/templates/template.xlsx

# 4. Run extraction pipeline
python pipeline.py

# 5. Find output in data/output/
```

## Project Structure

```
sr-extract-v3/
├── core/
│   ├── chunker.py        # Smart PDF section chunker
│   ├── job_store.py      # SQLite state machine
│   ├── key_manager.py    # Token-aware API key rotator
│   ├── worker.py         # Async worker pool
│   ├── extractor.py      # Gemini API client + fallback slot
│   ├── validator.py      # Schema validator
│   └── aggregator.py     # Chunk merger
├── output/
│   └── writer.py         # Incremental Excel/CSV writer
├── config/
│   ├── settings.py       # All tunable constants
│   └── keys.env.example  # API key template
├── data/
│   ├── pdfs/             # Input PDFs
│   ├── templates/        # Excel extraction templates
│   └── output/           # Results
├── tests/
│   ├── test_chunker.py
│   ├── test_key_manager.py
│   └── test_extractor.py
├── ingest.py             # Ingestion entrypoint
├── pipeline.py           # Main orchestration loop
└── requirements.txt
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full system design.

## Resumability

The pipeline is fully crash-safe. On restart, it skips all completed jobs and resumes from where it stopped. No re-extraction of already-processed studies.

## License
MIT
