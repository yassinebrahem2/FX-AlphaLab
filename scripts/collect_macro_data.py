"""Unified macro data collection script.

Collects macroeconomic indicators from multiple sources and exports as single consolidated file.

Two-stage pipeline:
1. Collection (Bronze): Fetch raw data from FRED + ECB APIs → data/raw/
2. Preprocessing (Silver): Transform to standardized schema → data/processed/macro/macro_all_{start}_{end}.csv

Sources:
    - FRED: US economic indicators (CPI, unemployment, fed funds rate, financial stress)
    - ECB: European policy rates (deposit facility, main refinancing operations)

Usage:
    # Collect and preprocess all macro data (default: 2 years)
    python scripts/collect_macro_data.py --preprocess

    # Custom date range
    python scripts/collect_macro_data.py --start 2021-01-01 --end 2025-12-31 --preprocess

    # Preprocess only (skip collection)
    python scripts/collect_macro_data.py --preprocess-only

    # Health check
    python scripts/collect_macro_data.py --health-check

Example:
    $ python scripts/collect_macro_data.py --start 2024-01-01 --preprocess
    [INFO] Collecting FRED data (4 series)...
    [INFO] Collected 1,826 records from FRED
    [INFO] Collecting ECB data (2 datasets)...
    [INFO] Collected 36 records from ECB
    [INFO] Preprocessing 6 series to Silver layer...
    [INFO] Exported consolidated macro data: macro_all_2024-01-01_2025-12-31.csv (2,241 records)
    [SUCCESS] Pipeline complete
"""

import argparse
import sys
from datetime import datetime, timedelta

from src.ingestion.collectors.ecb_collector import ECBCollector
from src.ingestion.collectors.fred_collector import FREDCollector
from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer
from src.shared.config import Config
from src.shared.utils import setup_logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect macroeconomic data from FRED and ECB",
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
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main() -> int:
    """Main collection script."""
    args = parse_args()

    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    logger = setup_logger(
        "collect_macro",
        Config.LOGS_DIR / "collectors" / "macro_collection.log",
        level=log_level,
    )

    try:
        # Parse dates
        if args.start:
            start_date = datetime.strptime(args.start, "%Y-%m-%d")
        else:
            start_date = datetime.now() - timedelta(days=365 * 2)

        if args.end:
            end_date = datetime.strptime(args.end, "%Y-%m-%d")
        else:
            end_date = datetime.now()

        # Validate config
        Config.validate()

        # Health check mode
        if args.health_check:
            logger.info("Running health checks...")

            # Check FRED
            fred_collector = FREDCollector()
            fred_ok = fred_collector.health_check()
            logger.info("FRED health check: %s", "PASSED" if fred_ok else "FAILED")

            # Check ECB
            ecb_collector = ECBCollector()
            ecb_ok = ecb_collector.health_check()
            logger.info("ECB health check: %s", "PASSED" if ecb_ok else "FAILED")

            if fred_ok and ecb_ok:
                logger.info("✓ All sources healthy")
                return 0
            else:
                logger.error("✗ Some sources failed health check")
                return 1

        # STAGE 1: Bronze Layer Collection
        if not args.preprocess_only:
            logger.info("")
            logger.info("=" * 60)
            logger.info("Stage 1: Bronze Layer Collection")
            logger.info("=" * 60)
            logger.info("Collecting macro data from %s to %s", start_date.date(), end_date.date())

            total_records = 0

            # Collect FRED data
            logger.info("Collecting FRED data (CPI, unemployment, fed funds, financial stress)...")
            fred_collector = FREDCollector(
                output_dir=Config.DATA_DIR / "raw" / "fred",
            )

            if not fred_collector.health_check():
                logger.error("FRED health check failed - skipping FRED collection")
            else:
                fred_data = fred_collector.collect(start_date=start_date, end_date=end_date)

                if fred_data:
                    fred_collector.export_all_to_csv(data=fred_data)
                    fred_count = sum(len(df) for df in fred_data.values())
                    total_records += fred_count
                    logger.info(
                        "  ✓ Collected %d records from FRED (%d series)", fred_count, len(fred_data)
                    )
                else:
                    logger.warning("  ✗ No FRED data collected")

            # Collect ECB data
            logger.info("Collecting ECB data (policy rates)...")
            ecb_collector = ECBCollector(
                output_dir=Config.DATA_DIR / "raw" / "ecb",
            )

            if not ecb_collector.health_check():
                logger.error("ECB health check failed - skipping ECB collection")
            else:
                ecb_data = ecb_collector.collect(start_date=start_date, end_date=end_date)

                if ecb_data and "policy_rates" in ecb_data:
                    ecb_collector.export_csv(ecb_data["policy_rates"], "policy_rates")
                    ecb_count = len(ecb_data["policy_rates"])
                    total_records += ecb_count
                    logger.info("  ✓ Collected %d records from ECB (policy rates)", ecb_count)
                else:
                    logger.warning("  ✗ No ECB policy rate data collected")

            logger.info("✓ Bronze collection complete: %d total records", total_records)

        # STAGE 2: Silver Layer Preprocessing
        if args.preprocess or args.preprocess_only:
            logger.info("")
            logger.info("=" * 60)
            logger.info("Stage 2: Silver Layer Preprocessing")
            logger.info("=" * 60)

            if args.preprocess_only:
                logger.info("Preprocess-only mode: using existing Bronze data")

            # Preprocess all sources together
            logger.info("Preprocessing macro data from FRED and ECB...")
            normalizer = MacroNormalizer(
                input_dir=Config.DATA_DIR / "raw",
                output_dir=Config.DATA_DIR / "processed" / "macro",
                sources=["fred", "ecb"],  # Both sources
            )

            silver_paths = normalizer.process_and_export(
                start_date=start_date,
                end_date=end_date,
                consolidated=True,  # Single file output
            )

            if "all" in silver_paths:
                import pandas as pd

                df = pd.read_csv(silver_paths["all"])
                series_counts = df.groupby("series_id").size()
                source_counts = df.groupby("source").size()

                logger.info("")
                logger.info("✓ Exported consolidated macro data:")
                logger.info("  File: %s", silver_paths["all"].name)
                logger.info("  Total records: %d", len(df))
                logger.info("  Series breakdown:")
                for series_id, count in series_counts.items():
                    logger.info("    - %s: %d records", series_id, count)
                logger.info("  Source breakdown:")
                for source, count in source_counts.items():
                    logger.info("    - %s: %d records", source, count)
            else:
                logger.warning("No consolidated file generated")
                return 1

            logger.info("")
            logger.info("=" * 60)
            logger.info("✓ Pipeline Complete!")
            logger.info("=" * 60)

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
