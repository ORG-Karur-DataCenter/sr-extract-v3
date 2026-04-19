"""SQLite-backed job queue + result cache.

Crash-safe. Resumable. Every chunk has a row with status and result.
Restarting the pipeline skips 'done' rows and retries 'failed' rows.
"""
from __future__ import annotations
import sqlite3
import json
import logging
import time
from pathlib import Path
from typing import Optional
import threading

from config.settings import DB_PATH

log = logging.getLogger(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    pdf_path        TEXT NOT NULL,
    study_id        TEXT NOT NULL,
    chunk_index     INTEGER NOT NULL,
    chunk_text      TEXT NOT NULL,
    section_name    TEXT,
    token_estimate  INTEGER,
    status          TEXT NOT NULL DEFAULT 'pending',
    result_json     TEXT,
    error_msg       TEXT,
    retries         INTEGER NOT NULL DEFAULT 0,
    model_used      TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_study ON jobs(study_id);

CREATE TABLE IF NOT EXISTS studies (
    study_id        TEXT PRIMARY KEY,
    pdf_path        TEXT NOT NULL,
    total_chunks    INTEGER NOT NULL,
    completed       INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending',
    final_record    TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_study_status ON studies(status);
"""


class JobStore:
    """Thread-safe wrapper around SQLite for the extraction pipeline.
    
    Uses a persistent connection to avoid WAL visibility issues and
    sqlite3.Row invalidation between separate connections.
    """

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()
        # Recover jobs that were in_progress when the process died
        now = time.time()
        cur = self._conn.execute(
            "UPDATE jobs SET status='pending', updated_at=? "
            "WHERE status='in_progress'", (now,)
        )
        self._conn.commit()
        if cur.rowcount:
            log.info(f"Recovered {cur.rowcount} stranded in_progress jobs")

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        """Convert sqlite3.Row to plain dict to avoid stale-reference issues."""
        return dict(row)

    # ── Study registration ───────────────────────────────────────────
    def register_study(self, study_id: str, pdf_path: str, total_chunks: int):
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO studies "
                "(study_id, pdf_path, total_chunks, created_at, updated_at) "
                "VALUES (?,?,?,?,?)",
                (study_id, pdf_path, total_chunks, now, now),
            )
            self._conn.commit()

    # ── Job lifecycle ────────────────────────────────────────────────
    def add_job(self, job_id: str, pdf_path: str, study_id: str,
                chunk_index: int, chunk_text: str,
                section_name: Optional[str] = None,
                token_estimate: Optional[int] = None) -> bool:
        """Insert a new chunk-level job. Returns False if already exists (idempotent)."""
        now = time.time()
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO jobs "
                    "(id, pdf_path, study_id, chunk_index, chunk_text, "
                    "section_name, token_estimate, created_at, updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (job_id, pdf_path, study_id, chunk_index, chunk_text,
                     section_name, token_estimate, now, now),
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def get_pending(self, limit: int = 10) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status='pending' "
                "ORDER BY created_at LIMIT ?", (limit,)
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]

    def claim_job(self, job_id: str) -> bool:
        """Atomically move pending -> in_progress. Returns False if already claimed."""
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                "UPDATE jobs SET status='in_progress', updated_at=? "
                "WHERE id=? AND status='pending'",
                (now, job_id),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def mark_done(self, job_id: str, result: dict, model_used: str):
        now = time.time()
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status='done', result_json=?, model_used=?, "
                "updated_at=? WHERE id=?",
                (json.dumps(result), model_used, now, job_id),
            )
            # Bump study progress
            row = self._conn.execute("SELECT study_id FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row:
                self._conn.execute(
                    "UPDATE studies SET completed=completed+1, updated_at=? "
                    "WHERE study_id=?", (now, row["study_id"])
                )
            self._conn.commit()

    def mark_failed(self, job_id: str, error: str, requeue: bool = True):
        now = time.time()
        new_status = "pending" if requeue else "failed"
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status=?, error_msg=?, retries=retries+1, "
                "updated_at=? WHERE id=?",
                (new_status, error[:500], now, job_id),
            )
            self._conn.commit()

    def requeue(self, job_id: str):
        now = time.time()
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status='pending', updated_at=? WHERE id=?",
                (now, job_id),
            )
            self._conn.commit()

    # ── Query helpers ────────────────────────────────────────────────
    def get_study_chunks(self, study_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE study_id=? ORDER BY chunk_index",
                (study_id,),
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]

    def get_completed_studies(self) -> list[dict]:
        """Studies where all chunks are done."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT s.* FROM studies s "
                "WHERE s.completed >= s.total_chunks AND s.status != 'written'"
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]

    def study_count(self) -> int:
        """Return total number of registered studies."""
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM studies").fetchone()[0]

    def mark_study_written(self, study_id: str, final_record: dict):
        now = time.time()
        with self._lock:
            self._conn.execute(
                "UPDATE studies SET status='written', final_record=?, "
                "updated_at=? WHERE study_id=?",
                (json.dumps(final_record), now, study_id),
            )
            self._conn.commit()

    def stats(self) -> dict:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) as n FROM jobs GROUP BY status"
            ).fetchall()
            return {r["status"]: r["n"] for r in rows}

    def close(self):
        self._conn.close()
