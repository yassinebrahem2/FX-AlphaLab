"""FRED data collection and preprocessing script.

Two-stage pipeline:
1. Collection (Bronze): Fetch raw data from FRED API → data/raw/fred/
2. Preprocessing (Silver): Transform to standardized schema → data/processed/macro/

Usage:
    # Collect Bronze (raw) data only
    python scripts/collect_fred_data.py

    # Collect and preprocess to Silver
    python scripts/collect_fred_data.py --preprocess

    # Preprocess existing Bronze data only (no collection)
    python scripts/collect_fred_data.py --preprocess-only

    # Collect specific date range
    python scripts/collect_fred_data.py --start 2023-01-01 --end 2023-12-31

    # Health check only
    python scripts/collect_fred_data.py --health-check

Example:
    $ python scripts/collect_fred_data.py --start 2024-01-01 --preprocess
    [INFO] FREDCollector initialized
    [INFO] Health check: PASSED
    [INFO] Collecting data from 2024-01-01 to 2026-02-10
    [INFO] Collected STLFSI4: 107 observations
    [INFO] Collected DFF: 405 observations
    [INFO] Collected CPIAUCSL: 14 observations
    [INFO] Collected UNRATE: 14 observations
    [INFO] Exported 4 CSV files to data/raw/fred/ (Bronze)
    [INFO] Starting Silver preprocessing...
    [INFO] Processed 4 series to data/processed/macro/ (Silver)
    [SUCCESS] Collection and preprocessing complete
"""

import argparse
import sys
from datetime import datetime, timedelta

from src.ingestion.collectors.fred_collector import FREDCollector
from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer
from src.shared.config import Config
from src.shared.utils import setup_logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect macroeconomic data from FRED API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD). Default: 2 years ago",
        metavar="DATE",
    )

    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD). Default: today",
        metavar="DATE",
    )

    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass cache and force fresh API calls",
    )

    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Run health check only and exit",
    )

    parser.add_argument(
        "--preprocess",
        action="store_true",
        help="Also run Silver preprocessing after collection",
    )

    parser.add_argument(
        "--preprocess-only",
        action="store_true",
        help="Skip collection and only preprocess existing raw data",
    )

    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear all cached data before collecting",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main() -> int:
    """Main collection script."""
    args = parse_args()

    # Setup logger
    logger = setup_logger(
        "collect_fred",
        level="DEBUG" if args.verbose else "INFO",
    )

    try:
        # Handle preprocess-only mode
        if args.preprocess_only:
            logger.info("Preprocess-only mode: skipping collection")
            logger.info("")
            logger.info("=" * 60)
            logger.info("Stage 2: Silver Layer Preprocessing")
            logger.info("=" * 60)

            # Check if raw data exists
            raw_dir = Config.DATA_DIR / "raw" / "fred"
            if not raw_dir.exists() or not list(raw_dir.glob("*.csv")):
                logger.error("No raw FRED data found in %s", raw_dir)
                logger.error("Run without --preprocess-only to collect data first")
                return 1

            logger.info("Processing existing Bronze data from %s", raw_dir)

            # Parse dates if provided
            if args.start:
                try:
                    start_date = datetime.strptime(args.start, "%Y-%m-%d")
                except ValueError:
                    logger.error("Invalid start date format. Use YYYY-MM-DD")
                    return 1
            else:
                start_date = datetime.now() - timedelta(days=730)  # 2 years ago

            if args.end:
                try:
                    end_date = datetime.strptime(args.end, "%Y-%m-%d")
                except ValueError:
                    logger.error("Invalid end date format. Use YYYY-MM-DD")
                    return 1
            else:
                end_date = datetime.now()

            normalizer = MacroNormalizer(
                input_dir=Config.DATA_DIR / "raw",
                output_dir=Config.DATA_DIR / "processed" / "macro",
                sources=["fred"],
            )
            silver_paths = normalizer.process_and_export(start_date=start_date, end_date=end_date)

            if not silver_paths:
                logger.warning("No data preprocessed")
                return 1

            logger.info(
                "Exported %d Silver CSV files to %s:", len(silver_paths), normalizer.output_dir
            )
            for series_id, path in silver_paths.items():
                logger.info("  ✓ %s → %s", series_id, path.name)

            logger.info("")
            logger.info("=" * 60)
            logger.info("✓ Preprocessing Complete!")
            logger.info("=" * 60)
            return 0

        # Initialize collector
        collector = FREDCollector()
        logger.info("FREDCollector initialized (Bronze layer: %s)", collector.output_dir)

        # Health check
        if not collector.health_check():
            logger.error("FRED API health check failed")
            logger.error("Please check your internet connection and API key")
            return 1

        logger.info("Health check: PASSED")

        if args.health_check:
            logger.info("Health check complete, exiting")
            return 0

        # Clear cache if requested
        if args.clear_cache:
            collector.clear_cache()
            logger.info("Cache cleared")

        # Parse dates
        if args.start:
            try:
                start_date = datetime.strptime(args.start, "%Y-%m-%d")
            except ValueError:
                logger.error("Invalid start date format. Use YYYY-MM-DD")
                return 1
        else:
            start_date = datetime.now() - timedelta(days=730)  # 2 years ago

        if args.end:
            try:
                end_date = datetime.strptime(args.end, "%Y-%m-%d")
            except ValueError:
                logger.error("Invalid end date format. Use YYYY-MM-DD")
                return 1
        else:
            end_date = datetime.now()

        # Validate date range
        if start_date > end_date:
            logger.error("Start date must be before end date")
            return 1

        logger.info("Collecting Bronze data from %s to %s", start_date.date(), end_date.date())

        # STAGE 1: Collect Bronze (raw) data
        logger.info("Collecting all predefined series (Bronze layer)")
        data = collector.collect(start_date=start_date, end_date=end_date)

        if not data:
            logger.warning("No data collected")
            return 1

        # Export to Bronze layer
        paths = collector.export_all_to_csv(data=data)
        logger.info("Exported %d Bronze CSV files to %s:", len(paths), collector.output_dir)
        for name, path in paths.items():
            records = len(data.get(name, []))
            logger.info("  - %s: %d records -> %s", name, records, path.name)

        # STAGE 2: Preprocess to Silver (optional)
        if args.preprocess:
            logger.info("Starting Silver preprocessing...")
            normalizer = MacroNormalizer(
                input_dir=Config.DATA_DIR / "raw",
                output_dir=Config.DATA_DIR / "processed" / "macro",
                sources=["fred"],
            )
            silver_paths = normalizer.process_and_export(
                start_date=start_date, end_date=end_date, consolidated=True
            )

            if "all" in silver_paths:
                logger.info("Exported consolidated macro data to %s", silver_paths["all"].name)
            else:
                logger.info(
                    "Exported %d Silver CSV files to %s:", len(silver_paths), normalizer.output_dir
                )
                for series_id, path in silver_paths.items():
                    logger.info("  - %s -> %s", series_id, path.name)

            logger.info("SUCCESS: Collection and preprocessing complete")
        else:
            logger.info("SUCCESS: Collection complete (Bronze only)")
            logger.info("Run with --preprocess to transform to Silver layer")

        return 0

    except ValueError as e:
        logger.error("Configuration error: %s", e)
        logger.error("Make sure FRED_API_KEY is set in .env")
        logger.error("Get a free API key at: https://fred.stlouisfed.org/docs/api/api_key.html")
        return 1

    except KeyboardInterrupt:
        logger.warning("Collection interrupted by user")
        return 130

    except Exception as e:
        logger.exception("Unexpected error during collection: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
