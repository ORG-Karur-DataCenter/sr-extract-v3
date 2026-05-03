"""One-off real-Gemini wiring check — consumes reserved test key quota.

Usage:
  python scripts/verify_real_api.py --url https://sr-extract-api.onrender.com --yes

Hard budget: ≤3 real Gemini calls per run. Requires explicit --yes flag.
Key is read from .env.test (GEMINI_TEST_KEY=...).
"""
from __future__ import annotations
import argparse
import io
import os
import sys
import time
from pathlib import Path

import httpx
import openpyxl
from dotenv import load_dotenv


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--yes", action="store_true", required=True,
                    help="Explicit acknowledgement that this burns real quota")
    args = ap.parse_args()

    load_dotenv(".env.test")
    key = os.getenv("GEMINI_TEST_KEY", "").strip()
    if not key:
        print("ERROR: GEMINI_TEST_KEY not set in .env.test", file=sys.stderr)
        return 2

    print("About to make up to 3 REAL Gemini calls using reserved test key.")
    print("Press Ctrl-C in the next 5 seconds to abort.")
    time.sleep(5)

    wb = openpyxl.Workbook(); wb.active.append(["author", "year"])
    buf = io.BytesIO(); wb.save(buf)
    pdf_bytes = Path("tests/fixtures/tiny.pdf").read_bytes()

    base = args.url.rstrip("/")
    with httpx.Client(timeout=120.0) as client:
        r = client.post(f"{base}/jobs",
            data={"api_keys": key, "model": "gemini-2.0-flash"},
            files=[("pdfs", ("tiny.pdf", pdf_bytes, "application/pdf")),
                   ("template", ("t.xlsx", buf.getvalue(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))])
        assert r.status_code == 200, r.text
        job_id = r.json()["job_id"]
        print(f"job: {job_id}")

        s = {}
        deadline = time.time() + 300
        while time.time() < deadline:
            s = client.get(f"{base}/jobs/{job_id}/status").json()
            print(f"  {s['status']} chunks={s['progress']['chunks_done']}/"
                  f"{s['progress']['chunks_total']}")
            if s["status"] in ("done", "failed"): break
            time.sleep(3)

        if s.get("status") != "done":
            print("FAIL:", s.get("error_code"), s.get("error_message"))
            return 1

        d = client.get(f"{base}/jobs/{job_id}/result")
        assert d.status_code == 200
        print("OK — real Gemini wiring works end-to-end.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
