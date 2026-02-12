"""ECB News data collection script.

Collects European Central Bank press releases, speeches, monetary policy statements,
and economic bulletins from the official ECB RSS feed.

Bronze Layer: Fetches raw news data → data/raw/news/ecb/

Usage:
    # Collect last 3 months (default)
    python scripts/collect_ecb_news_data.py

    # Collect specific date range
    python scripts/collect_ecb_news_data.py --start 2025-11-01 --end 2026-02-11

    # Collect last 6 months
    python scripts/collect_ecb_news_data.py --months 6

    # Collect last 30 days
    python scripts/collect_ecb_news_data.py --days 30

    # Health check only
    python scripts/collect_ecb_news_data.py --health-check

Example:
    $ python scripts/collect_ecb_news_data.py --start 2026-01-01
    [INFO] ECBNewsCollector initialized
    [INFO] Health check: PASSED
    [INFO] Collecting ECB news from 2026-01-01 to 2026-02-11
    [INFO] RSS feed retrieved: 147 total entries
    [INFO] Collection complete: 32 documents collected, 115 skipped
    [INFO] By type - pressreleases: 18, speeches: 6, policy: 2, bulletins: 6
    [INFO] Exported 4 document types to data/raw/news/ecb/
    [SUCCESS] Collection complete
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone

from src.ingestion.collectors.ecb_news_collector import ECBNewsCollector
from src.shared.utils import setup_logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect ECB news, speeches, and policy documents from RSS feed",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD). Default: 3 months ago",
        metavar="DATE",
    )

    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD). Default: today",
        metavar="DATE",
    )

    parser.add_argument(
        "--months",
        type=int,
        help="Collect last N months (overrides --start)",
        metavar="N",
    )

    parser.add_argument(
        "--days",
        type=int,
        help="Collect last N days (overrides --start)",
        metavar="N",
    )

    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Run health check only and exit",
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
        "collect_ecb_news",
        level="DEBUG" if args.verbose else "INFO",
    )

    try:
        # Initialize collector (uses Config defaults for paths)
        collector = ECBNewsCollector()
        logger.info("ECBNewsCollector initialized (Bronze layer: %s)", collector.output_dir)

        # Health check
        if not collector.health_check():
            logger.error("ECB RSS feed health check failed")
            logger.error("Please check your internet connection")
            return 1

        logger.info("Health check: PASSED")

        if args.health_check:
            logger.info("Health check complete, exiting")
            return 0

        # Determine date range
        end_date = datetime.now(timezone.utc)

        if args.days:
            start_date = end_date - timedelta(days=args.days)
            logger.info("Collecting last %d days", args.days)
        elif args.months:
            start_date = end_date - timedelta(days=args.months * 30)
            logger.info("Collecting last %d months", args.months)
        elif args.start:
            try:
                start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                logger.error("Invalid start date format. Use YYYY-MM-DD")
                return 1
        else:
            # Default: 3 months ago
            start_date = end_date - timedelta(days=90)
            logger.info("Using default: last 3 months")

        if args.end:
            try:
                end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                logger.error("Invalid end date format. Use YYYY-MM-DD")
                return 1

        # Validate date range
        if start_date > end_date:
            logger.error("Start date must be before end date")
            return 1

        logger.info("Collecting Bronze data from %s to %s", start_date.date(), end_date.date())

        # STAGE 1: Collect Bronze (raw) data
        logger.info("Collecting all document types (Bronze layer)")
        data = collector.collect(start_date=start_date, end_date=end_date)

        if not data:
            logger.warning("No data collected")
            return 1

        # Calculate total documents
        total_docs = sum(len(docs) for docs in data.values())
        if total_docs == 0:
            logger.warning("No documents found in date range")
            logger.info("Try expanding the date range or check RSS feed content")
            return 0

        # Export to Bronze layer JSONL files
        paths = collector.export_all(data=data)

        logger.info("Exported %d document types to %s:", len(paths), collector.output_dir)
        for doc_type, path in paths.items():
            doc_count = len(data.get(doc_type, []))
            logger.info("  - %s: %d documents → %s", doc_type, doc_count, path.name)

        logger.info("SUCCESS: Collection complete (%d total documents)", total_docs)

        return 0

    except KeyboardInterrupt:
        logger.warning("Collection interrupted by user")
        return 130

    except Exception as e:
        logger.exception("Unexpected error during collection: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
