"""GDELT Bronze to Silver preprocessing script.

Processes monthly Bronze JSONL files from data/raw/news/gdelt and writes
Silver Parquet files under data/processed/sentiment/source=gdelt.
"""

import argparse
import json
import sys
from calendar import monthrange
from datetime import datetime
from pathlib import Path

# Ensure project root is on path when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.preprocessors.gdelt_preprocessor import GDELTPreprocessor
from src.shared.utils import setup_logger

DEFAULT_START = "2021-01-01"
DEFAULT_END = "2025-12-31"

INPUT_DIR = Path("data/raw/news/gdelt")
OUTPUT_DIR = Path("data/processed/sentiment/source=gdelt")
MANIFEST_PATH = OUTPUT_DIR / "preprocess_manifest.json"
LOG_PATH = Path("logs/preprocessors/gdelt_preprocess.log")


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


def _silver_path(dt: datetime) -> Path:
    return OUTPUT_DIR / f"year={dt.year}" / f"month={dt.month:02d}" / "sentiment_cleaned.parquet"


def _load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with MANIFEST_PATH.open(encoding="utf-8") as file_handle:
            return json.load(file_handle)
    return {"completed": {}, "failed": {}}


def _save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST_PATH.open("w", encoding="utf-8") as file_handle:
        json.dump(manifest, file_handle, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preprocess GDELT Bronze data into Silver Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
        "--force",
        action="store_true",
        help="Reprocess months that already have a Silver file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which months would be processed without writing files",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logger("gdelt_preprocess", LOG_PATH)

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
        print(f"{'Month':<10}  {'Bronze':<45}  {'Silver exists'}")
        print("-" * 80)
        for chunk_start, _ in chunks:
            bronze_path = INPUT_DIR / f"gdelt_{_month_key(chunk_start)}_raw.jsonl"
            silver_path = _silver_path(chunk_start)
            print(
                f"{_month_key(chunk_start):<10}  {str(bronze_path):<45}  "
                f"{'YES' if silver_path.exists() else 'no'}"
            )
        return

    preprocessor = GDELTPreprocessor(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        log_file=LOG_PATH,
    )

    manifest = _load_manifest()
    completed = 0
    failed = 0
    total_records = 0

    for index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        key = _month_key(chunk_start)
        logger.info("--- Month %d/%d: %s ---", index, total_months, key)

        try:
            result = preprocessor.run(chunk_start, chunk_end, backfill=args.force)
            n_records = result.get(key, 0)
        except Exception as exc:
            failed += 1
            manifest["failed"][key] = {
                "error": str(exc),
                "attempted_at": datetime.utcnow().isoformat(),
            }
            _save_manifest(manifest)
            logger.error("[%s] Preprocessing failed: %s", key, exc)
            continue

        total_records += n_records
        completed += 1
        manifest["completed"][key] = {
            "n_records": n_records,
            "path": str(_silver_path(chunk_start)),
            "processed_at": datetime.utcnow().isoformat(),
        }
        manifest["failed"].pop(key, None)
        _save_manifest(manifest)
        logger.info("[%s] Processed %d records", key, n_records)

    logger.info("=" * 60)
    logger.info("Preprocessing complete")
    logger.info("  Months processed : %d / %d", completed + failed, total_months)
    logger.info("  Completed        : %d", completed)
    logger.info("  Failed           : %d", failed)
    logger.info("  Total records    : %d", total_records)
    logger.info("  Manifest         : %s", MANIFEST_PATH)

    if failed:
        logger.warning("Failed months: %s", sorted(manifest["failed"].keys()))

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
