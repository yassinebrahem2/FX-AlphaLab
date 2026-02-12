"""Forex Factory Calendar data collection script.

Two-stage pipeline:
1. Collection (Bronze): Fetch raw data from Forex Factory → data/raw/forexfactory/
2. Optional Preprocessing (Silver): Transform to standardized schema → data/processed/events/

Usage:
    # Collect Bronze (raw) data only
    python scripts/collect_forexfactory_data.py

    # Collect and preprocess to Silver
    python scripts/collect_forexfactory_data.py --preprocess

    # Custom date range
    python scripts/collect_forexfactory_data.py --start 2023-01-01 --end 2023-12-31 --preprocess

    # Today's events only
    python scripts/collect_forexfactory_data.py --today

    # Health check only
    python scripts/collect_forexfactory_data.py --health-check

    # Validate collected data
    python scripts/collect_forexfactory_data.py --today --validate

Example:
    $ python scripts/collect_forexfactory_data.py --today
    [INFO] ForexFactoryCalendarCollector initialized
    [INFO] Health check: PASSED
    [INFO] Collecting events for 2026-02-12
    [INFO] Total events collected: 45
    [INFO] Exported to data/raw/forexfactory/ (Bronze)
    [SUCCESS] Collection complete
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector
from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor
from src.shared.config import Config
from src.shared.utils import setup_logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect economic calendar events from Forex Factory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD). Default: today",
        metavar="DATE",
    )

    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD). Default: same as start",
        metavar="DATE",
    )

    parser.add_argument(
        "--today",
        action="store_true",
        help="Collect today's events only (overrides --start and --end)",
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
        "--validate",
        action="store_true",
        help="Validate scraped data quality after collection",
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Custom output directory for CSV files",
        metavar="DIR",
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
        "collect_forexfactory",
        level="DEBUG" if args.verbose else "INFO",
    )

    try:
        # Initialize collector
        output_dir = Path(args.output) if args.output else None
        collector = ForexFactoryCalendarCollector(output_dir=output_dir)
        logger.info(
            "ForexFactoryCalendarCollector initialized (Bronze layer: %s)", collector.output_dir
        )

        # Health check
        if not collector.health_check():
            logger.error("Forex Factory health check failed")
            logger.error("Please check your internet connection")
            return 1

        logger.info("Health check: PASSED")

        if args.health_check:
            logger.info("Health check complete, exiting")
            return 0

        # Parse dates
        if args.today:
            start_date = datetime.now()
            end_date = datetime.now()
            logger.info("Mode: Today's events only")
        else:
            if args.start:
                try:
                    start_date = datetime.strptime(args.start, "%Y-%m-%d")
                except ValueError:
                    logger.error("Invalid start date format. Use YYYY-MM-DD")
                    return 1
            else:
                start_date = datetime.now()

            if args.end:
                try:
                    end_date = datetime.strptime(args.end, "%Y-%m-%d")
                except ValueError:
                    logger.error("Invalid end date format. Use YYYY-MM-DD")
                    return 1
            else:
                end_date = start_date

        # Validate date range
        if start_date > end_date:
            logger.error("Start date must be before end date")
            return 1

        logger.info("=" * 60)
        logger.info("Stage 1: Bronze Layer Collection")
        logger.info("=" * 60)
        logger.info("Collecting events from %s to %s", start_date.date(), end_date.date())

        # STAGE 1: Collect Bronze (raw) data
        events = collector.collect_events(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )

        if not events:
            logger.warning("No events collected")
            return 1

        logger.info("Collected %d events", len(events))

        # Validate data if requested
        if args.validate:
            logger.info("Validating scraped data...")
            is_valid, errors = collector.validate_scraped_data(events)
            if not is_valid:
                logger.warning("Data validation found %d issues:", len(errors))
                for error in errors[:10]:  # Show first 10 errors
                    logger.warning("  - %s", error)
                if len(errors) > 10:
                    logger.warning("  ... and %d more", len(errors) - 10)
            else:
                logger.info("Data validation passed")

        # Save to Bronze layer
        output_path = collector.save_to_csv(events)
        if output_path:
            logger.info("✓ Bronze collection complete: saved to %s", output_path)
        else:
            logger.error("Failed to save Bronze data")
            return 1

        # STAGE 2: Preprocess to Silver (optional)
        if args.preprocess:
            logger.info("")
            logger.info("=" * 60)
            logger.info("Stage 2: Silver Layer Preprocessing")
            logger.info("=" * 60)

            preprocessor = CalendarPreprocessor(
                input_dir=Config.DATA_DIR / "raw" / "forexfactory",
                output_dir=Config.DATA_DIR / "processed" / "events",
            )

            # Preprocess Bronze data
            result = preprocessor.preprocess()

            if "events" not in result or result["events"].empty:
                logger.warning("No events to preprocess")
                return 1

            df = result["events"]
            logger.info("Preprocessed %d events", len(df))

            # Validate Silver schema
            try:
                preprocessor.validate(df)
                logger.info("✓ Validation passed")
            except ValueError as e:
                logger.error("Validation failed: %s", e)
                return 1

            # Export to Silver layer
            import pandas as pd

            min_date = pd.to_datetime(df["timestamp_utc"].min()).to_pydatetime()
            max_date = pd.to_datetime(df["timestamp_utc"].max()).to_pydatetime()

            identifier = f"{min_date.strftime('%Y-%m-%d')}_{max_date.strftime('%Y-%m-%d')}"
            path = preprocessor.export(
                df,
                identifier=identifier,
                start_date=min_date,
                end_date=max_date,
                format="csv",
            )

            logger.info("✓ Silver preprocessing complete: %d events → %s", len(df), path.name)

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
