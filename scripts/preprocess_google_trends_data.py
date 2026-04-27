"""Google Trends Bronze to Silver preprocessing script."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is on path when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.preprocessors.google_trends_preprocessor import GoogleTrendsPreprocessor
from src.shared.utils import setup_logger

INPUT_DIR = Path("data/raw/google_trends")
OUTPUT_DIR = Path("data/processed/sentiment/source=google_trends")
LOG_PATH = Path("logs/preprocessors/google_trends_preprocess.log")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess Google Trends Bronze data")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logger("google_trends_preprocess", LOG_PATH)

    preprocessor = GoogleTrendsPreprocessor(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        log_file=LOG_PATH,
    )
    silver_path = OUTPUT_DIR / "google_trends_weekly.parquet"

    if args.dry_run:
        csv_paths = sorted(INPUT_DIR.glob("trends_*.csv"))
        print("\nDry-run: Google Trends preprocessing")
        print(f"CSV files exist: {'yes' if csv_paths else 'no'}")
        print(f"Silver exists: {'yes' if silver_path.exists() else 'no'}")
        for path in csv_paths:
            print(f"- {path}")
        return

    result = preprocessor.run(force=args.force)
    row_count = result.get("google_trends_weekly", 0)
    logger.info("Wrote %d rows to %s", row_count, silver_path)

    sys.exit(0)


if __name__ == "__main__":
    main()
