"""Central configuration for sr-extract-v3.

All tunables live here. Environment-driven values load from .env.
"""
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Paths ────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
PDF_DIR = DATA_DIR / "pdfs"
TEMPLATE_DIR = DATA_DIR / "templates"
OUTPUT_DIR = DATA_DIR / "outputs"
DB_PATH = DATA_DIR / "sr_jobs.db"

for d in (DATA_DIR, PDF_DIR, TEMPLATE_DIR, OUTPUT_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── API keys (comma-separated in .env: GEMINI_API_KEYS=key1,key2,...) ──
GEMINI_API_KEYS = [
    k.strip() for k in os.getenv("GEMINI_API_KEYS", "").split(",") if k.strip()
]
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY", "").strip()

# ── Rate limits (per API key, per minute) ────────────────────────────
# Gemini 1.5 Flash free tier: 15 RPM, 1M TPM, 1500 RPD
GEMINI_RPM_LIMIT = int(os.getenv("GEMINI_RPM_LIMIT", "15"))
GEMINI_TPM_LIMIT = int(os.getenv("GEMINI_TPM_LIMIT", "1000000"))
GEMINI_RPD_LIMIT = int(os.getenv("GEMINI_RPD_LIMIT", "1500"))

# Safety buffer — only use 85% of stated limit
SAFETY_THRESHOLD = 0.85

# ── Concurrency ──────────────────────────────────────────────────────
MAX_CONCURRENT_WORKERS = int(os.getenv("MAX_CONCURRENT_WORKERS", "4"))

# ── Retry strategy ───────────────────────────────────────────────────
MAX_RETRIES = 5
RETRY_BASE_DELAY = 2  # seconds, exponential base
RETRY_MAX_DELAY = 120  # cap
RETRY_JITTER = 1.0  # uniform 0..1s noise added

# ── Chunking ─────────────────────────────────────────────────────────
MAX_CHUNK_TOKENS = 6000  # max tokens per chunk sent to LLM
MIN_CHUNK_TOKENS = 200  # skip micro-chunks, merge instead
CHUNK_OVERLAP_TOKENS = 150  # overlap between sliding-window chunks

# Sections we care about (regex-matched, case-insensitive)
RELEVANT_SECTIONS = [
    "abstract", "methods", "methodology", "study design",
    "population", "participants", "intervention", "comparator",
    "outcomes", "results", "statistical analysis", "data extraction",
    "risk of bias", "baseline characteristics",
]

# Sections to always skip
SKIP_SECTIONS = [
    "references", "bibliography", "acknowledgments", "acknowledgements",
    "funding", "conflicts of interest", "author contributions",
    "supplementary material", "appendix",
]

# ── Models ───────────────────────────────────────────────────────────
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5")
USE_CLAUDE_FALLBACK = bool(CLAUDE_API_KEY)

# ── Output ───────────────────────────────────────────────────────────
OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "xlsx")  # xlsx | csv | both
INCREMENTAL_WRITE = True  # write each study as it completes

# ── Logging ──────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
