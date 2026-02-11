"""ECB data collection and preprocessing script.

Two-stage pipeline:
1. Collection (Bronze): Fetch raw data from ECB SDMX API → data/raw/ecb/
2. Preprocessing (Silver): Transform to standardized schema → data/processed/ohlcv/ and data/processed/macro/

Usage:
    # Collect Bronze (raw) data only
    python scripts/collect_ecb_data.py

    # Collect and preprocess to Silver
    python scripts/collect_ecb_data.py --preprocess

    # Specific dataset only
    python scripts/collect_ecb_data.py --dataset exchange_rates --preprocess
    python scripts/collect_ecb_data.py --dataset policy_rates --preprocess

    # Custom date range
    python scripts/collect_ecb_data.py --start 2023-01-01 --end 2023-12-31 --preprocess

    # Health check only
    python scripts/collect_ecb_data.py --health-check

Example:
    $ python scripts/collect_ecb_data.py --start 2024-01-01 --preprocess
    [INFO] ECBCollector initialized
    [INFO] Health check: PASSED
    [INFO] Collecting data from 2024-01-01 to 2026-02-10
    [INFO] Collected exchange_rates: 567 observations
    [INFO] Collected policy_rates: 45 observations
    [INFO] Exported 2 CSV files to data/raw/ecb/ (Bronze)
    [INFO] Starting Silver preprocessing...
    [INFO] Processed 2 datasets to data/processed/ (Silver)
    [SUCCESS] Collection and preprocessing complete
"""

import argparse
import sys
from datetime import datetime, timedelta

from src.ingestion.collectors.ecb_collector import ECBCollector
from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer
from src.ingestion.preprocessors.price_normalizer import PriceNormalizer
from src.shared.config import Config
from src.shared.utils import setup_logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect data from European Central Bank SDMX API",
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
        "--dataset",
        type=str,
        choices=["exchange_rates", "policy_rates", "all"],
        default="all",
        help="Dataset to collect (default: all)",
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
        "collect_ecb",
        level="DEBUG" if args.verbose else "INFO",
    )

    try:
        # Initialize collector
        collector = ECBCollector()
        logger.info("ECBCollector initialized (Bronze layer: %s)", collector.output_dir)

        # Health check
        if not collector.health_check():
            logger.error("ECB API health check failed")
            logger.error("Please check your internet connection")
            return 1

        logger.info("Health check: PASSED")

        if args.health_check:
            logger.info("Health check complete, exiting")
            return 0

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

        logger.info("=" * 60)
        logger.info("Stage 1: Bronze Layer Collection")
        logger.info("=" * 60)
        logger.info("Collecting Bronze data from %s to %s", start_date.date(), end_date.date())
        logger.info("Dataset filter: %s", args.dataset)

        # STAGE 1: Collect Bronze (raw) data
        data = collector.collect(start_date=start_date, end_date=end_date)

        if not data:
            logger.warning("No data collected")
            return 1

        # Filter datasets if requested
        if args.dataset != "all":
            data = {k: v for k, v in data.items() if k == args.dataset}
            if not data:
                logger.error("Dataset '%s' not found", args.dataset)
                return 1

        # Export to Bronze layer
        logger.info("Exporting Bronze data...")
        for name, df in data.items():
            if df.empty:
                logger.warning("Skipping empty dataset: %s", name)
                continue
            path = collector.export_csv(df, name)
            logger.info("  ✓ Exported %s: %d records → %s", name, len(df), path.name)

        logger.info("✓ Bronze collection complete: %d datasets", len(data))

        # STAGE 2: Preprocess to Silver (optional)
        if args.preprocess:
            logger.info("")
            logger.info("=" * 60)
            logger.info("Stage 2: Silver Layer Preprocessing")
            logger.info("=" * 60)

            silver_count = 0

            # Exchange rates → OHLCV (PriceNormalizer)
            if "exchange_rates" in data:
                logger.info("Processing exchange_rates to OHLCV...")
                price_normalizer = PriceNormalizer(
                    input_dir=Config.DATA_DIR / "raw",
                    output_dir=Config.DATA_DIR / "processed" / "ohlcv",
                    sources=["ecb"],
                )
                ohlcv_data = price_normalizer.preprocess(start_date=start_date, end_date=end_date)

                for pair_tf, df in ohlcv_data.items():
                    if df.empty:
                        continue
                    min_date = df["timestamp_utc"].min().to_pydatetime()
                    max_date = df["timestamp_utc"].max().to_pydatetime()
                    path = price_normalizer.export(
                        df, pair_tf, min_date, max_date, format="parquet"
                    )
                    logger.info("  ✓ Processed %s: %d records → %s", pair_tf, len(df), path.name)
                    silver_count += 1

            # Policy rates → Macro (MacroNormalizer)
            if "policy_rates" in data:
                logger.info("Processing policy_rates to macro...")
                macro_normalizer = MacroNormalizer(
                    input_dir=Config.DATA_DIR / "raw",
                    output_dir=Config.DATA_DIR / "processed" / "macro",
                    sources=["ecb"],
                )
                macro_data = macro_normalizer.preprocess(start_date=start_date, end_date=end_date)

                for series_id, df in macro_data.items():
                    if df.empty:
                        continue
                    min_date = df["timestamp_utc"].min().to_pydatetime()
                    max_date = df["timestamp_utc"].max().to_pydatetime()
                    path = macro_normalizer.export(df, series_id, min_date, max_date, format="csv")
                    logger.info("  ✓ Processed %s: %d records → %s", series_id, len(df), path.name)
                    silver_count += 1

            logger.info("✓ Silver preprocessing complete: %d datasets", silver_count)

        logger.info("")
        logger.info("=" * 60)
        logger.info("✓ Pipeline Complete!")
        logger.info("=" * 60)

        return 0

    except KeyboardInterrupt:
        logger.warning("Collection interrupted by user")
        return 130

    except Exception as e:
        logger.exception("Unexpected error during collection: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
