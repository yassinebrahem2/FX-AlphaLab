"""Economic Calendar data collection and preprocessing script.

Two-stage pipeline:
1. Collection (Bronze): Fetch raw data from Investing.com → data/raw/calendar/
2. Preprocessing (Silver): Transform to standardized schema → data/processed/events/

Usage:
    # Collect Bronze (raw) data only
    python scripts/collect_calendar_data.py

    # Collect and preprocess to Silver
    python scripts/collect_calendar_data.py --preprocess

    # Custom date range
    python scripts/collect_calendar_data.py --start 2023-01-01 --end 2023-12-31 --preprocess

    # Filter by countries
    python scripts/collect_calendar_data.py --countries us,eu,uk,jp --preprocess

    # Today's events only
    python scripts/collect_calendar_data.py --today --preprocess

    # Health check only
    python scripts/collect_calendar_data.py --health-check

Example:
    $ python scripts/collect_calendar_data.py --today --countries us,eu,uk --preprocess
    [INFO] EconomicCalendarCollector initialized
    [INFO] Health check: PASSED
    [INFO] Collecting events for 2026-02-10
    [INFO] Countries: us, eu, uk
    [INFO] Collected 45 events
    [INFO] Exported to data/raw/calendar/ (Bronze)
    [INFO] Starting Silver preprocessing...
    [INFO] Processed 45 events to data/processed/events/ (Silver)
    [SUCCESS] Collection and preprocessing complete
"""

import argparse
import sys
from datetime import datetime, timedelta

from src.ingestion.collectors.calendar_collector import EconomicCalendarCollector
from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor
from src.shared.config import Config
from src.shared.utils import setup_logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect economic calendar events from Investing.com",
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
        help="End date (YYYY-MM-DD). Default: 7 days from start",
        metavar="DATE",
    )

    parser.add_argument(
        "--today",
        action="store_true",
        help="Collect today's events only (overrides --start and --end)",
    )

    parser.add_argument(
        "--countries",
        type=str,
        help="Comma-separated country codes (e.g., us,eu,uk,jp). Default: us,eu,uk,jp,ch",
        metavar="CODES",
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
        "collect_calendar",
        level="DEBUG" if args.verbose else "INFO",
    )

    try:
        # Initialize collector
        collector = EconomicCalendarCollector()
        logger.info(
            "EconomicCalendarCollector initialized (Bronze layer: %s)", collector.output_dir
        )

        # Health check
        if not collector.health_check():
            logger.error("Calendar API health check failed")
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
                end_date = start_date + timedelta(days=7)

        # Validate date range
        if start_date > end_date:
            logger.error("Start date must be before end date")
            return 1

        # Parse countries
        if args.countries:
            countries = [c.strip().lower() for c in args.countries.split(",")]
        else:
            countries = ["us", "eu", "uk", "jp", "ch"]  # Default major economies

        logger.info("=" * 60)
        logger.info("Stage 1: Bronze Layer Collection")
        logger.info("=" * 60)
        logger.info("Collecting events from %s to %s", start_date.date(), end_date.date())
        logger.info("Countries: %s", ", ".join(countries))

        # STAGE 1: Collect Bronze (raw) data
        events = collector.collect_events(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            countries=countries,
        )

        if not events:
            logger.warning("No events collected")
            return 1

        logger.info("Collected %d events", len(events))

        # Save to Bronze layer
        success = collector.save_to_csv(events)
        if success:
            logger.info("✓ Bronze collection complete: saved to %s", collector.output_dir)
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
                input_dir=Config.DATA_DIR / "raw" / "calendar",
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
