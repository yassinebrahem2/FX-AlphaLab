import argparse
import json
import os
import sys
from calendar import monthrange
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.collectors.gdelt_gkg_collector import GDELTGKGCollector
from src.shared.utils import setup_logger

DEFAULT_START = "2021-01-01"
DEFAULT_END = "2025-12-31"

OUTPUT_DIR = Path("data/raw/news/gdelt")
MANIFEST_PATH = OUTPUT_DIR / "collection_manifest.json"
LOG_PATH = Path("logs/collectors/gdelt_gkg_historical.log")

GCP_PROJECT_ID = "fx-alphalab-487213"
CREDENTIALS_PATH = Path("credentials/fx-alphalab-487213-c5033a4c3516.json")


def _monthly_chunks(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    chunks: list[tuple[datetime, datetime]] = []
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current <= end:
        year, month = current.year, current.month
        last_day = monthrange(year, month)[1]
        month_end = current.replace(day=last_day, hour=23, minute=59, second=59)

        chunk_start = max(current, start)
        chunk_end = min(month_end, end)
        chunks.append((chunk_start, chunk_end))

        if month == 12:
            current = current.replace(year=year + 1, month=1, day=1)
        else:
            current = current.replace(month=month + 1, day=1)

    return chunks


def _month_key(dt: datetime) -> str:
    return dt.strftime("%Y%m")


def _jsonl_path(dt: datetime) -> Path:
    return OUTPUT_DIR / f"gdelt_{_month_key(dt)}_raw.jsonl"


def _load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, encoding="utf-8") as file_handle:
            return json.load(file_handle)
    return {"completed": {}, "failed": {}}


def _save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as file_handle:
        json.dump(manifest, file_handle, indent=2)


def collect_month(
    collector: GDELTGKGCollector,
    chunk_start: datetime,
    chunk_end: datetime,
    logger,
    force: bool = False,
) -> dict:
    key = _month_key(chunk_start)
    path = _jsonl_path(chunk_start)
    existed_before = path.exists()

    logger.info("[%s] Collecting %s to %s", key, chunk_start.date(), chunk_end.date())

    try:
        result = collector.collect(start_date=chunk_start, end_date=chunk_end, backfill=force)
        n_docs = result["aggregated"]
    except Exception as exc:
        logger.error("[%s] Collection failed: %s", key, exc)
        return {"status": "failed", "path": str(path), "n_docs": 0, "error": str(exc)}

    if n_docs == 0:
        logger.warning("[%s] Collected 0 documents — no JSONL written", key)
        return {"status": "empty", "path": str(path), "n_docs": 0}

    if existed_before and not force:
        logger.info("[%s] Already exported -> %s  (collector skip)", key, path)
        return {"status": "skipped", "path": str(path), "n_docs": n_docs}

    logger.info("[%s] Exported %d docs -> %s", key, n_docs, path)
    return {"status": "ok", "path": str(path), "n_docs": n_docs}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect GDELT GKG historical data in monthly chunks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--start", default=DEFAULT_START, help=f"Start date YYYY-MM-DD (default: {DEFAULT_START})"
    )
    parser.add_argument(
        "--end", default=DEFAULT_END, help=f"End date YYYY-MM-DD (default: {DEFAULT_END})"
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-collect months that already have a JSONL file"
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
    logger = setup_logger("gdelt_gkg_collect", LOG_PATH)

    if CREDENTIALS_PATH.exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(CREDENTIALS_PATH))
        logger.info("Using credentials: %s", CREDENTIALS_PATH)
    elif not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        logger.error(
            "No GCP credentials found. Set GOOGLE_APPLICATION_CREDENTIALS or place service-account JSON at %s",
            CREDENTIALS_PATH,
        )
        sys.exit(1)

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

    logger.info("Date range: %s to %s  (%d months)", args.start, args.end, total_months)

    if args.dry_run:
        print(f"\nDry-run: {total_months} months to process")
        print(f"{'Month':<10}  {'File':<45}  {'Exists'}")
        print("-" * 70)
        for chunk_start, _ in chunks:
            path = _jsonl_path(chunk_start)
            exists = "YES (skip)" if (path.exists() and not args.force) else "no"
            print(f"{_month_key(chunk_start):<10}  {str(path):<45}  {exists}")
        return

    collector = GDELTGKGCollector(
        output_dir=OUTPUT_DIR, log_file=LOG_PATH, project_id=GCP_PROJECT_ID
    )

    logger.info("Running BigQuery health check...")
    try:
        if not collector.health_check():
            logger.error("Health check failed — aborting")
            sys.exit(1)
        logger.info("Health check passed")
    except Exception as exc:
        logger.error("Health check error: %s", exc)
        sys.exit(1)

    manifest = _load_manifest()
    completed = 0
    skipped = 0
    failed = 0
    total_docs = 0

    for index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        key = _month_key(chunk_start)
        logger.info("--- Month %d/%d: %s ---", index, total_months, key)

        result = collect_month(collector, chunk_start, chunk_end, logger, force=args.force)
        status = result["status"]

        if status == "failed":
            failed += 1
            manifest["failed"][key] = {
                "error": result["error"],
                "attempted_at": datetime.utcnow().isoformat(),
            }
            _save_manifest(manifest)
            continue

        if status == "empty":
            skipped += 1
            manifest["completed"][key] = {
                "n_docs": 0,
                "path": result["path"],
                "processed_at": datetime.utcnow().isoformat(),
            }
            manifest["failed"].pop(key, None)
            _save_manifest(manifest)
            continue

        if status == "skipped":
            skipped += 1
            manifest["completed"][key] = {
                "n_docs": result["n_docs"],
                "path": result["path"],
                "processed_at": datetime.utcnow().isoformat(),
            }
            manifest["failed"].pop(key, None)
            _save_manifest(manifest)
            continue

        completed += 1
        total_docs += result["n_docs"]
        manifest["completed"][key] = {
            "n_docs": result["n_docs"],
            "path": result["path"],
            "processed_at": datetime.utcnow().isoformat(),
        }
        manifest["failed"].pop(key, None)
        _save_manifest(manifest)

    logger.info("=" * 60)
    logger.info("Collection complete")
    logger.info("  Months processed : %d / %d", completed + skipped + failed, total_months)
    logger.info("  Completed        : %d", completed)
    logger.info("  Skipped          : %d", skipped)
    logger.info("  Failed           : %d", failed)
    logger.info("  Total documents  : %d", total_docs)
    logger.info("  Manifest         : %s", MANIFEST_PATH)

    if failed:
        logger.warning("Failed months: %s", sorted(manifest["failed"].keys()))

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
