"""sr-extract-v3 main pipeline orchestrator.

Usage:
  python pipeline.py                 # full run: ingest + extract + write
  python pipeline.py --ingest-only   # just chunk PDFs into the queue
  python pipeline.py --resume        # skip ingest, just process pending
  python pipeline.py --status        # show queue stats and exit

Flow:
  1. Ingest PDFs + template into SQLite queue (idempotent)
  2. Launch async worker pool
  3. Monitor progress; as studies complete, aggregate + write row
  4. On completion or Ctrl-C, print final stats
"""
from __future__ import annotations
import argparse
import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.table import Table

from config.settings import (
    GEMINI_API_KEYS, PDF_DIR, TEMPLATE_DIR, OUTPUT_DIR, OUTPUT_FORMAT,
    LOG_LEVEL,
)
from core.job_store import JobStore
from core.key_manager import KeyManager
from core.worker import Worker
from core.aggregator import aggregate_study
from output.writer import IncrementalWriter
import ingest

console = Console()


class ProgressTracker:
    """Tracks chunk completion rate for ETA calculation."""
    def __init__(self):
        self._start = time.time()

    def eta(self, done: int, total: int) -> str:
        if done == 0:
            return "calculating..."
        elapsed = time.time() - self._start
        rate = done / elapsed
        remaining = total - done
        if rate <= 0:
            return "unknown"
        secs = remaining / rate
        if secs < 60:
            return f"{int(secs)}s"
        if secs < 3600:
            return f"{int(secs/60)}m {int(secs%60)}s"
        return f"{int(secs/3600)}h {int((secs%3600)/60)}m"

    def rate_per_min(self, done: int) -> float:
        elapsed = time.time() - self._start
        return round(done / elapsed * 60, 1) if elapsed > 0 else 0.0


log = logging.getLogger("pipeline")


def setup_logging():
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet noisy libs
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def require_keys():
    if not GEMINI_API_KEYS:
        console.print("[red]No GEMINI_API_KEYS in .env[/red]")
        console.print("Copy .env.example to .env and add your keys from aistudio.google.com")
        sys.exit(1)


def build_dashboard(store: JobStore, keys: KeyManager, tracker: ProgressTracker) -> Table:
    from rich import box
    stats = store.stats()
    done = stats.get("done", 0)
    pending = stats.get("pending", 0)
    in_prog = stats.get("in_progress", 0)
    failed = stats.get("failed", 0)
    total_jobs = done + pending + in_prog + failed

    study_stats = store.get_study_stats()
    studies_written = study_stats.get("written", 0)
    studies_total = study_stats.get("total", 0)
    studies_remaining = studies_total - studies_written

    tbl = Table(
        title="[bold cyan]sr-extract-v3[/bold cyan]",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold magenta",
    )
    tbl.add_column("", style="dim")
    tbl.add_column("Chunks", justify="right")
    tbl.add_column("Articles", justify="right")

    tbl.add_row("[green]Done / Written[/green]", f"[green]{done}[/green]", f"[green]{studies_written}[/green]")
    tbl.add_row("[yellow]In Progress[/yellow]", f"[yellow]{in_prog}[/yellow]", "")
    tbl.add_row("Pending", str(pending), str(studies_remaining))
    tbl.add_row("[red]Failed[/red]", f"[red]{failed}[/red]" if failed else "0", "")
    tbl.add_section()
    tbl.add_row("Total", str(total_jobs), str(studies_total))
    tbl.add_section()
    tbl.add_row("[cyan]Rate[/cyan]", f"[cyan]{tracker.rate_per_min(done)} chunks/min[/cyan]", "")
    tbl.add_row("[cyan]ETA[/cyan]", f"[cyan]{tracker.eta(done, total_jobs)}[/cyan]", "")
    tbl.add_section()
    for k in keys.status():
        tbl.add_row(f"[dim]…{k['key'][-6:]}[/dim]", f"[dim]RPM {k['rpm']}[/dim]", f"[dim]fail {k['failures']}[/dim]")
    return tbl


async def aggregator_loop(store: JobStore, writer: IncrementalWriter, stop_event: asyncio.Event):
    """Polls for completed studies and writes rows as they finish."""
    while not stop_event.is_set():
        completed = store.get_completed_studies()
        for study in completed:
            study_id = study["study_id"]
            try:
                agg = aggregate_study(store, study_id, writer.fields)
                if agg is None:
                    continue
                record = agg.record
                writer.write_row(study_id, record)
                store.mark_study_written(study_id, record)
                log.info(f"[green]Wrote study[/green] {study_id} "
                         f"({sum(1 for v in record.values() if v)} fields, "
                         f"{len(agg.conflicts)} conflicts)")
            except Exception as exc:
                log.warning(f"Write failed for {study_id}, will retry: {exc}")
        await asyncio.sleep(2)


async def run_pipeline(resume: bool = False, ingest_only: bool = False):
    setup_logging()
    require_keys()
    store = JobStore()

    # Ingest phase
    if not resume:
        console.print("[cyan]Ingesting PDFs + template...[/cyan]")
        result = ingest.ingest_all(store)
        console.print(f"  {result['pdfs']} PDFs -> {result['chunks_new']} new chunks "
                      f"({result['chunks_skipped']} already in queue)")
        console.print(f"  Template: {result['template_fields']} fields")
        if ingest_only:
            return

    schema = ingest.get_cached_schema()
    fields = schema["fields"]

    keys = KeyManager(GEMINI_API_KEYS)
    writer = IncrementalWriter(fields, OUTPUT_DIR, fmt=OUTPUT_FORMAT)

    worker = Worker(store, keys, expected_fields=fields)
    stop = asyncio.Event()

    # Signal handling — graceful shutdown
    def shutdown(sig):
        log.warning(f"Signal {sig.name}, stopping...")
        worker.stop()
        stop.set()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown, sig)
        except NotImplementedError:
            pass  # Windows

    # Kick off worker + aggregator in parallel
    worker_task = asyncio.create_task(worker.run())
    aggregator_task = asyncio.create_task(aggregator_loop(store, writer, stop))

    # Live status
    tracker = ProgressTracker()
    try:
        with Live(build_dashboard(store, keys, tracker), refresh_per_second=1, console=console) as live:
            while not worker_task.done():
                live.update(build_dashboard(store, keys, tracker))
                await asyncio.sleep(1.5)
    finally:
        stop.set()
        aggregator_task.cancel()
        try:
            await aggregator_task
        except asyncio.CancelledError:
            pass
        await worker_task

        # Final sweep for any remaining completed studies
        completed = store.get_completed_studies()
        for study in completed:
            study_id = study["study_id"]
            agg = aggregate_study(store, study_id, writer.fields)
            if agg:
                writer.write_row(study_id, agg.record)
                store.mark_study_written(study_id, agg.record)

    writer.close()
    console.print("\n[bold green]Pipeline complete.[/bold green]")
    final = store.stats()
    console.print(f"  Done: {final.get('done', 0)} chunks")
    console.print(f"  Failed: {final.get('failed', 0)} chunks")
    console.print(f"  Output: {writer.output_path}")


def main():
    p = argparse.ArgumentParser(description="sr-extract-v3 pipeline")
    p.add_argument("--ingest-only", action="store_true", help="Chunk PDFs into queue, don't extract")
    p.add_argument("--resume", action="store_true", help="Skip ingest, process pending queue")
    p.add_argument("--status", action="store_true", help="Show queue stats and exit")
    args = p.parse_args()

    if args.status:
        setup_logging()
        store = JobStore()
        console.print(store.stats())
        return

    asyncio.run(run_pipeline(resume=args.resume, ingest_only=args.ingest_only))


if __name__ == "__main__":
    main()
