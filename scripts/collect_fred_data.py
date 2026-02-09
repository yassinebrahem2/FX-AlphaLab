"""FRED data collection script.

Collects macroeconomic indicators from FRED API and exports to CSV.

Usage:
    # Collect last 2 years of data (default)
    python scripts/collect_fred_data.py

    # Collect specific date range
    python scripts/collect_fred_data.py --start 2023-01-01 --end 2023-12-31

    # Collect specific series only
    python scripts/collect_fred_data.py --series DFF UNRATE

    # Force refresh (bypass cache)
    python scripts/collect_fred_data.py --no-cache

    # Health check only
    python scripts/collect_fred_data.py --health-check

Example:
    $ python scripts/collect_fred_data.py --start 2024-01-01
    [INFO] FREDCollector initialized
    [INFO] Health check: PASSED
    [INFO] Collecting data from 2024-01-01 to 2026-02-09
    [INFO] Collected STLFSI4: 107 observations
    [INFO] Collected DFF: 405 observations
    [INFO] Collected CPIAUCSL: 14 observations
    [INFO] Collected UNRATE: 14 observations
    [INFO] Exported 4 CSV files to data/datasets/macro/
    [SUCCESS] Collection complete
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from data.ingestion.fred_collector import FREDCollector
from shared.utils import setup_logger


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
        "--series",
        nargs="+",
        help="Specific FRED series IDs to collect (e.g., DFF UNRATE). Default: all predefined series",
        metavar="ID",
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
        "--output-dir",
        type=str,
        help="Custom output directory for CSV files",
        metavar="PATH",
    )

    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear all cached data before collecting",
    )

    parser.add_argument(
        "--consolidated",
        action="store_true",
        help="Export all series to a single consolidated CSV file",
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
        # Initialize collector
        output_dir = Path(args.output_dir) if args.output_dir else None
        collector = FREDCollector(output_dir=output_dir)
        logger.info("FREDCollector initialized")

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

        logger.info("Collecting data from %s to %s", start_date.date(), end_date.date())

        # Collect data
        if args.series:
            # Collect specific series
            logger.info("Collecting series: %s", ", ".join(args.series))
            data = collector.get_multiple_series(
                args.series,
                start_date=start_date,
                end_date=end_date,
                use_cache=not args.no_cache,
            )

            if args.consolidated:
                # Export to consolidated file
                path = collector.export_consolidated_csv(
                    data=data, filename="fred_custom_indicators"
                )
                logger.info("Exported %d series to consolidated file: %s", len(data), path.name)
            else:
                # Export with series ID as dataset name
                paths = {}
                for series_id, df in data.items():
                    path = collector.export_csv(df, series_id.lower())
                    paths[series_id] = path

        else:
            # Collect all predefined series
            logger.info("Collecting all predefined series")
            data = collector.collect(start_date=start_date, end_date=end_date)

            if args.consolidated:
                # Export to consolidated file
                path = collector.export_consolidated_csv(data=data)
                logger.info("Exported %d series to consolidated file: %s", len(data), path.name)
            else:
                # Export each series separately
                paths = collector.export_all_to_csv(data=data)

        # Report results
        if not data:
            logger.warning("No data collected")
            return 1

        if not args.consolidated:
            logger.info("Exported %d CSV files to %s:", len(paths), collector.output_dir)
            for name, path in paths.items():
                records = len(data.get(name, []))
                logger.info("  - %s: %d records -> %s", name, records, path.name)

        logger.info("SUCCESS: Collection complete")
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
