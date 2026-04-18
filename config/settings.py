import os
from dotenv import load_dotenv

load_dotenv('config/keys.env')

# ── Concurrency ──────────────────────────────────────────────
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 4))

# ── Rate limits (Gemini 1.5 Flash free tier defaults) ────────
RPM_LIMIT = int(os.getenv('RPM_LIMIT', 14))        # hard cap 15, use 14 for safety
TPM_LIMIT = int(os.getenv('TPM_LIMIT', 900_000))   # hard cap 1M, use 900k
KEY_THRESHOLD = 0.85                               # use key only up to 85% capacity

# ── Retry / backoff ──────────────────────────────────────────
MAX_RETRIES = 6
BACKOFF_BASE = 2          # seconds
BACKOFF_CAP = 120         # seconds max wait
BACKOFF_JITTER = 1.0      # max random jitter seconds

# ── Chunking ─────────────────────────────────────────────────
MAX_CHUNK_TOKENS = 5_000  # max tokens per chunk sent to LLM
CHUNK_OVERLAP_PAGES = 0   # overlap between page-window chunks

# ── Models ───────────────────────────────────────────────────
GEMINI_MODEL = 'gemini-1.5-flash'
CLAUDE_MODEL  = 'claude-haiku-4-5-20251001'  # fallback slot

# ── Paths ────────────────────────────────────────────────────
DB_PATH       = 'data/sr_jobs.db'
PDF_DIR       = 'data/pdfs'
TEMPLATE_DIR  = 'data/templates'
OUTPUT_DIR    = 'data/output'

# ── API keys (loaded from keys.env) ──────────────────────────
GEMINI_KEYS = [v for k, v in os.environ.items()
               if k.startswith('GEMINI_API_KEY') and v.strip()]
CLAUDE_KEY  = os.getenv('CLAUDE_API_KEY', '')
