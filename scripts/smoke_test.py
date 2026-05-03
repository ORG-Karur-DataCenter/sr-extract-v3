"""Post-deploy smoke test — runs against a live backend.

Uses TEST_MODE so no real Gemini calls happen. Requires the backend
to be deployed with env var TEST_MODE=true (set temporarily).

Usage:
  python scripts/smoke_test.py --url https://sr-extract-api.onrender.com
"""
from __future__ import annotations
import argparse
import io
import sys
import time
from pathlib import Path

import httpx
import openpyxl


def build_template_bytes() -> bytes:
    wb = openpyxl.Workbook()
    wb.active.append(["author", "year", "title", "population", "sample_size"])
    buf = io.BytesIO(); wb.save(buf); return buf.getvalue()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--pdf", default="tests/fixtures/tiny.pdf")
    args = ap.parse_args()

    base = args.url.rstrip("/")
    with httpx.Client(timeout=60.0) as client:
        h = client.get(f"{base}/health")
        assert h.status_code == 200, f"health failed: {h.status_code}"
        print("health: ok")

        pdf_bytes = Path(args.pdf).read_bytes()
        tpl_bytes = build_template_bytes()
        r = client.post(f"{base}/jobs",
            data={"api_keys": "TEST_KEY_DO_NOT_USE", "model": "gemini-2.0-flash"},
            files=[
                ("pdfs", ("tiny.pdf", pdf_bytes, "application/pdf")),
                ("template", ("t.xlsx", tpl_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
            ],
        )
        assert r.status_code == 200, f"POST /jobs failed: {r.status_code} {r.text}"
        job_id = r.json()["job_id"]
        print(f"job: {job_id}")

        deadline = time.time() + 180
        s = {}
        while time.time() < deadline:
            s = client.get(f"{base}/jobs/{job_id}/status").json()
            print(f"status: {s['status']} ({s['progress']['chunks_done']}/"
                  f"{s['progress']['chunks_total']})")
            if s["status"] in ("done", "failed"): break
            time.sleep(2)
        assert s.get("status") == "done", f"job ended {s.get('status')}: {s}"

        d = client.get(f"{base}/jobs/{job_id}/result")
        assert d.status_code == 200
        Path("/tmp/smoke_output.xlsx").write_bytes(d.content)
        print("download: ok (/tmp/smoke_output.xlsx)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
