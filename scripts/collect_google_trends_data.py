"""Google Trends historical collection script."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on path when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.collectors.google_trends_collector import GoogleTrendsCollector
from src.shared.utils import setup_logger

DEFAULT_START = "2021-01-01"
DEFAULT_END = datetime.now().strftime("%Y-%m-%d")

INPUT_DIR = Path("data/raw/google_trends")
LOG_PATH = Path("logs/collectors/google_trends.log")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Google Trends data")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logger("google_trends_collect", LOG_PATH)

    try:
        start_dt = datetime.strptime(args.start, "%Y-%m-%d")
        end_dt = datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError as exc:
        logger.error("Invalid date format: %s", exc)
        sys.exit(1)

    if start_dt > end_dt:
        logger.error("--start must be before --end")
        sys.exit(1)

    collector = GoogleTrendsCollector(output_dir=INPUT_DIR, log_file=LOG_PATH)
    expected_batches = [
        (theme, batch_index, INPUT_DIR / f"trends_{theme}_{batch_index}.csv")
        for theme, batches in collector.KEYWORD_GROUPS.items()
        for batch_index in range(len(batches))
    ]

    if args.dry_run:
        print("\nDry-run: Google Trends batches")
        print(f"{'Batch':<28}  {'File':<45}  {'Action'}")
        print("-" * 90)
        for theme, batch_index, path in expected_batches:
            action = "fetch" if (args.force or not path.exists()) else "skip"
            print(f"trends_{theme}_{batch_index:<17}  {str(path):<45}  {action}")
        return

    results = collector.collect(start_date=start_dt, end_date=end_dt, force=args.force)

    total_rows = sum(results.values())
    fetched_batches = len(collector.fetched_batches)

    logger.info("Total batches fetched: %d", fetched_batches)
    logger.info("Total rows collected: %d", total_rows)

    if collector.failed_batches:
        logger.error("Failed batches: %s", ", ".join(collector.failed_batches))
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
