"""GDELT historical data collection script.

Collects GDELT GKG financial news from BigQuery in monthly chunks,
exports one Bronze JSONL file per month, and optionally runs the
Silver preprocessor when all months are done.

Output naming:
    data/raw/news/gdelt/aggregated_YYYYMM.jsonl   (one per month)

Resume behaviour:
    Already-exported months are skipped automatically.
    A manifest file tracks completed/failed months:
    data/raw/news/gdelt/collection_manifest.json

Usage:
    # Full historical backfill (2021-01-01 → 2025-12-31)
    python scripts/collect_gdelt_data.py

    # Custom range
    python scripts/collect_gdelt_data.py --start 2023-01-01 --end 2023-06-30

    # Collect + preprocess to Silver in one shot
    python scripts/collect_gdelt_data.py --preprocess

    # Dry-run: show which months would be collected, no BigQuery calls
    python scripts/collect_gdelt_data.py --dry-run

    # Force re-collect already-exported months
    python scripts/collect_gdelt_data.py --force
"""

import argparse
import json
import os
import sys
from calendar import monthrange
from datetime import datetime
from pathlib import Path

# Ensure project root is on path when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.collectors.gdelt_collector import GDELTCollector
from src.shared.utils import setup_logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_START = "2021-01-01"
DEFAULT_END = "2025-12-31"

OUTPUT_DIR = Path("data/raw/news/gdelt")
SENTIMENT_DIR = Path("data/processed/sentiment")
MANIFEST_PATH = OUTPUT_DIR / "collection_manifest.json"
LOG_PATH = Path("logs/collectors/gdelt_historical.log")

GCP_PROJECT_ID = "fx-alphalab-487213"
CREDENTIALS_PATH = Path("credentials/fx-alphalab-487213-c5033a4c3516.json")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _monthly_chunks(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Generate (month_start, month_end) tuples covering [start, end] inclusive.

    Each chunk spans exactly one calendar month.  The first chunk starts on
    `start` (not necessarily the 1st of the month); the last chunk ends on
    `end`.  All intermediate chunks span the full month.

    Args:
        start: Inclusive start datetime.
        end: Inclusive end datetime.

    Returns:
        List of (chunk_start, chunk_end) tuples in chronological order.
    """
    chunks = []
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current <= end:
        year, month = current.year, current.month
        last_day = monthrange(year, month)[1]
        month_end = current.replace(day=last_day, hour=23, minute=59, second=59)

        chunk_start = max(current, start)
        chunk_end = min(month_end, end)

        chunks.append((chunk_start, chunk_end))

        # Advance to first day of next month
        if month == 12:
            current = current.replace(year=year + 1, month=1, day=1)
        else:
            current = current.replace(month=month + 1, day=1)

    return chunks


def _month_key(dt: datetime) -> str:
    """Return YYYYMM string for a datetime."""
    return dt.strftime("%Y%m")


def _jsonl_path(dt: datetime) -> Path:
    """Return Bronze JSONL path for the month containing dt."""
    return OUTPUT_DIR / f"aggregated_{_month_key(dt)}.jsonl"


def _load_manifest() -> dict:
    """Load or initialise the collection manifest."""
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"completed": {}, "failed": {}}


def _save_manifest(manifest: dict) -> None:
    """Persist manifest to disk."""
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


# ---------------------------------------------------------------------------
# Core collection
# ---------------------------------------------------------------------------


def collect_month(
    collector: GDELTCollector,
    chunk_start: datetime,
    chunk_end: datetime,
    logger,
    force: bool = False,
) -> dict:
    """Collect one month of GDELT data and export to Bronze JSONL.

    Args:
        collector: Initialised GDELTCollector instance.
        chunk_start: Start of the month window.
        chunk_end: End of the month window.
        logger: Logger instance.
        force: If True, overwrite existing JSONL file.

    Returns:
        Result dict with keys: status, path, n_docs, tier_counts, error.
    """
    key = _month_key(chunk_start)
    path = _jsonl_path(chunk_start)

    if path.exists() and not force:
        logger.info("[%s] Already exported -> %s  (skip, use --force to re-collect)", key, path)
        # Count existing rows for manifest accuracy
        existing = sum(1 for _ in open(path, encoding="utf-8") if _.strip())
        return {"status": "skipped", "path": str(path), "n_docs": existing, "tier_counts": {}}

    logger.info("[%s] Collecting %s to %s", key, chunk_start.date(), chunk_end.date())

    try:
        result = collector.collect(start_date=chunk_start, end_date=chunk_end)
        documents = result["aggregated"]
    except Exception as exc:
        logger.error("[%s] Collection failed: %s", key, exc)
        return {
            "status": "failed",
            "path": str(path),
            "n_docs": 0,
            "tier_counts": {},
            "error": str(exc),
        }

    if not documents:
        logger.warning("[%s] Collected 0 documents — no JSONL written", key)
        return {"status": "empty", "path": str(path), "n_docs": 0, "tier_counts": {}}

    # Write JSONL (one record per line)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for doc in documents:
            json.dump(doc, f, ensure_ascii=False)
            f.write("\n")

    tier_counts = {}
    for doc in documents:
        t = doc.get("metadata", {}).get("credibility_tier", 3)
        tier_counts[str(t)] = tier_counts.get(str(t), 0) + 1

    logger.info(
        "[%s] Exported %d docs -> %s  (tiers: %s)",
        key,
        len(documents),
        path,
        tier_counts,
    )
    return {"status": "ok", "path": str(path), "n_docs": len(documents), "tier_counts": tier_counts}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect GDELT GKG historical data in monthly chunks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        help=f"Start date YYYY-MM-DD (default: {DEFAULT_START})",
    )
    parser.add_argument(
        "--end",
        default=DEFAULT_END,
        help=f"End date YYYY-MM-DD (default: {DEFAULT_END})",
    )
    parser.add_argument(
        "--preprocess",
        action="store_true",
        help="Run GDELTPreprocessor on all Bronze data after collection finishes",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-collect months that already have a JSONL file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which months would be collected without making BigQuery calls",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logger("gdelt_collect", LOG_PATH)

    # ------------------------------------------------------------------
    # Credentials
    # ------------------------------------------------------------------
    if CREDENTIALS_PATH.exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(CREDENTIALS_PATH))
        logger.info("Using credentials: %s", CREDENTIALS_PATH)
    elif not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        logger.error(
            "No GCP credentials found. Set GOOGLE_APPLICATION_CREDENTIALS or place "
            "service-account JSON at %s",
            CREDENTIALS_PATH,
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Date range → monthly chunks
    # ------------------------------------------------------------------
    try:
        start_dt = datetime.strptime(args.start, "%Y-%m-%d")
        end_dt = datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError as exc:
        logger.error("Invalid date format: %s", exc)
        sys.exit(1)

    if start_dt > end_dt:
        logger.error("--start must be before --end")
        sys.exit(1)

    chunks = _monthly_chunks(start_dt, end_dt)
    total_months = len(chunks)

    logger.info(
        "Date range: %s to %s  (%d months)",
        args.start,
        args.end,
        total_months,
    )

    # ------------------------------------------------------------------
    # Dry-run: just list months and exit
    # ------------------------------------------------------------------
    if args.dry_run:
        print(f"\nDry-run: {total_months} months to process")
        print(f"{'Month':<10}  {'File':<45}  {'Exists'}")
        print("-" * 70)
        for cs, ce in chunks:
            p = _jsonl_path(cs)
            exists = "YES (skip)" if (p.exists() and not args.force) else "no"
            print(f"{_month_key(cs):<10}  {str(p):<45}  {exists}")
        return

    # ------------------------------------------------------------------
    # Initialise collector
    # ------------------------------------------------------------------
    collector = GDELTCollector(
        output_dir=OUTPUT_DIR,
        log_file=LOG_PATH,
        project_id=GCP_PROJECT_ID,
    )

    # Health check before starting
    logger.info("Running BigQuery health check...")
    try:
        if not collector.health_check():
            logger.error("Health check failed — aborting")
            sys.exit(1)
        logger.info("Health check passed")
    except Exception as exc:
        logger.error("Health check error: %s", exc)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Collect month by month
    # ------------------------------------------------------------------
    manifest = _load_manifest()
    completed = 0
    skipped = 0
    failed = 0
    total_docs = 0

    for idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
        key = _month_key(chunk_start)
        logger.info("--- Month %d/%d: %s ---", idx, total_months, key)

        result = collect_month(collector, chunk_start, chunk_end, logger, force=args.force)

        total_docs += result["n_docs"]

        if result["status"] == "ok":
            completed += 1
            manifest["completed"][key] = {
                "n_docs": result["n_docs"],
                "tier_counts": result["tier_counts"],
                "path": result["path"],
                "collected_at": datetime.utcnow().isoformat(),
            }
            manifest["failed"].pop(key, None)  # clear previous failure if any
        elif result["status"] == "skipped":
            skipped += 1
            # Keep existing manifest entry if present
        elif result["status"] == "empty":
            skipped += 1
            manifest["completed"][key] = {
                "n_docs": 0,
                "tier_counts": {},
                "path": result["path"],
                "collected_at": datetime.utcnow().isoformat(),
                "note": "empty result from BigQuery",
            }
        else:  # failed
            failed += 1
            manifest["failed"][key] = {
                "error": result.get("error", "unknown"),
                "attempted_at": datetime.utcnow().isoformat(),
            }

        # Persist after every month so progress is never lost
        _save_manifest(manifest)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("Collection complete")
    logger.info("  Months processed : %d / %d", completed + skipped, total_months)
    logger.info("  Newly collected  : %d", completed)
    logger.info("  Skipped (exist)  : %d", skipped)
    logger.info("  Failed           : %d", failed)
    logger.info("  Total docs       : %d", total_docs)
    logger.info("  Manifest         : %s", MANIFEST_PATH)

    if failed:
        logger.warning("Failed months: %s", sorted(manifest["failed"].keys()))

    # ------------------------------------------------------------------
    # Optional Silver preprocessing
    # ------------------------------------------------------------------
    if args.preprocess:
        logger.info("=" * 60)
        logger.info("Running GDELTPreprocessor on all Bronze data...")
        try:
            from src.ingestion.preprocessors.gdelt_preprocessor import GDELTPreprocessor

            pp = GDELTPreprocessor(
                input_dir=OUTPUT_DIR,
                output_dir=SENTIMENT_DIR,
                log_file=LOG_PATH,
            )
            df = pp.preprocess()
            paths = pp.export_partitioned(df)

            logger.info("Silver preprocessing complete")
            logger.info("  Total Silver rows : %d", len(df))
            logger.info(
                "  Label distribution: %s",
                df["sentiment_label"].value_counts().to_dict(),
            )
            logger.info("  Partitions written: %d", len(paths))
            for partition_key, path in sorted(paths.items()):
                logger.info("    %s  ->  %s", partition_key, path)

        except Exception as exc:
            logger.error("Silver preprocessing failed: %s", exc)
            sys.exit(1)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
