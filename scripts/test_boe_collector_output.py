"""Bank of England data collection test script.

Collects BoE documents (press releases, MPC statements, speeches, policy summaries)
from official RSS feeds and stores them in the Bronze layer (JSONL format).

Usage:
    # Collect default 90-day window
    python scripts/test_boe_collector_output.py

    # Collect specific date range
    python scripts/test_boe_collector_output.py --start 2026-01-01 --end 2026-02-12

    # Health check only
    python scripts/test_boe_collector_output.py --health-check

    # Verbose output
    python scripts/test_boe_collector_output.py --verbose

Example:
    $ python scripts/test_boe_collector_output.py --start 2026-02-01 --verbose
    [INFO] BoECollector initialized
    [INFO] Health check: PASSED
    [INFO] Collecting BoE documents from 2026-02-01 to 2026-02-12
    [INFO] Collected 15 BoE documents
    [INFO] Exported 3 document types:
    [INFO]   - statements: 8 documents -> statements_20260212.jsonl
    [INFO]   - speeches: 5 documents -> speeches_20260212.jsonl
    [INFO]   - mpc: 2 documents -> mpc_20260212.jsonl
    [SUCCESS] Collection complete
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

from src.ingestion.collectors.boe_collector import BoECollector
from src.shared.utils import setup_logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect Bank of England news and documents from RSS feeds",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD). Default: 90 days ago",
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
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Show detailed sample of collected documents",
    )

    return parser.parse_args()


def inspect_documents(data: dict[str, list[dict]], logger) -> None:
    """Display detailed information about collected documents."""
    logger.info("=" * 70)
    logger.info("DOCUMENT INSPECTION")
    logger.info("=" * 70)

    for doc_type, documents in data.items():
        if not documents:
            continue

        logger.info(f"\n📁 Document Type: {doc_type.upper()}")
        logger.info(f"   Total: {len(documents)} documents")

        # Show first document details
        if documents:
            doc = documents[0]
            logger.info("\n   Sample Document:")
            logger.info(f"   - Title: {doc.get('title', 'N/A')[:80]}")
            logger.info(f"   - Published: {doc.get('timestamp_published', 'N/A')}")
            logger.info(f"   - URL: {doc.get('url', 'N/A')}")
            logger.info(f"   - Type: {doc.get('document_type', 'N/A')}")
            logger.info(f"   - Speaker: {doc.get('speaker', 'N/A')}")
            logger.info(f"   - Content Length: {len(doc.get('content', ''))} chars")

            # Show metadata
            metadata = doc.get("metadata", {})
            logger.info("   - Metadata:")
            logger.info(f"     * RSS Feed: {metadata.get('rss_feed', 'N/A')}")
            logger.info(f"     * Fetch Error: {metadata.get('fetch_error', 'None')}")

    logger.info("\n" + "=" * 70)


def main() -> int:
    """Main collection script."""
    args = parse_args()

    # Setup logger
    logger = setup_logger(
        "test_boe_collector",
        level="DEBUG" if args.verbose else "INFO",
    )

    try:
        # Initialize collector
        collector = BoECollector()
        logger.info("BoECollector initialized (Bronze layer: %s)", collector.output_dir)
        logger.info("Output format: JSONL (one JSON object per line)")

        # Health check
        logger.info("Running health check...")
        if not collector.health_check():
            logger.error("BoE RSS feed health check failed")
            logger.error("Please check your internet connection")
            logger.error("Expected RSS feeds:")
            for url in collector.RSS_URLS:
                logger.error(f"  - {url}")
            return 1

        logger.info("✓ Health check: PASSED")

        if args.health_check:
            logger.info("Health check complete, exiting")
            return 0

        # Parse dates
        if args.start:
            try:
                start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                logger.error("Invalid start date format. Use YYYY-MM-DD")
                return 1
        else:
            start_date = datetime.now(timezone.utc) - timedelta(days=90)

        if args.end:
            try:
                end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                logger.error("Invalid end date format. Use YYYY-MM-DD")
                return 1
        else:
            end_date = datetime.now(timezone.utc)

        # Validate date range
        if start_date > end_date:
            logger.error("Start date must be before end date")
            return 1

        logger.info("Collecting BoE documents from %s to %s", start_date.date(), end_date.date())

        # Collect documents
        data = collector.collect(start_date=start_date, end_date=end_date)

        if not data:
            logger.warning("No documents collected")
            return 1

        # Count total documents
        total_docs = sum(len(docs) for docs in data.values())
        logger.info("✓ Collected %d BoE documents across %d categories", total_docs, len(data))

        # Show category breakdown
        logger.info("\nDocument breakdown:")
        for doc_type, documents in sorted(data.items()):
            logger.info(f"  - {doc_type}: {len(documents)} documents")

        # Export to Bronze layer JSONL files
        logger.info("\nExporting to Bronze layer (JSONL)...")
        paths = collector.export_all(data=data)

        logger.info("✓ Exported %d JSONL files:", len(paths))
        for doc_type, path in sorted(paths.items()):
            file_size = path.stat().st_size / 1024  # KB
            doc_count = len(data.get(doc_type, []))
            logger.info(f"  - {doc_type}: {doc_count} docs → {path.name} ({file_size:.1f} KB)")

        # Detailed inspection if requested
        if args.inspect:
            inspect_documents(data, logger)

        # Verify JSONL format
        logger.info("\nVerifying JSONL format...")
        for doc_type, path in paths.items():
            with open(path, encoding="utf-8") as f:
                lines = f.readlines()
                logger.info(f"  - {path.name}: {len(lines)} lines (valid JSONL)")

                # Validate first line is valid JSON
                if lines:
                    try:
                        obj = json.loads(lines[0])
                        logger.info(f"    ✓ First record has {len(obj)} fields")
                    except json.JSONDecodeError as e:
                        logger.error(f"    ✗ Invalid JSON in first line: {e}")
                        return 1

        logger.info("\n" + "=" * 70)
        logger.info("SUCCESS: Collection complete")
        logger.info("=" * 70)
        logger.info(f"Total documents: {total_docs}")
        logger.info(f"Output directory: {collector.output_dir}")
        logger.info(f"Files created: {len(paths)}")
        logger.info("\nNext steps:")
        logger.info("  1. Review JSONL files in data/raw/news/boe/")
        logger.info("  2. Implement Silver preprocessor for text cleaning")
        logger.info("  3. Use for sentiment analysis pipeline")

        return 0

    except KeyboardInterrupt:
        logger.warning("Collection interrupted by user")
        return 130

    except Exception as e:
        logger.exception("Unexpected error during collection: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
