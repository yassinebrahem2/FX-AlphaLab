"""News data collection and preprocessing script.

Two-stage pipeline:
1. Collection (Bronze): Fetch raw news from Fed/ECB/BoE → data/raw/news/{source}/
2. Optional Preprocessing (Silver): Transform to standardized sentiment schema → data/processed/sentiment/

Usage:
    # Collect Bronze (raw) data from all sources
    python scripts/collect_news_data.py

    # Collect from specific source(s)
    python scripts/collect_news_data.py --source fed
    python scripts/collect_news_data.py --source fed,ecb

    # Collect and preprocess to Silver
    python scripts/collect_news_data.py --preprocess

    # Preprocess existing Bronze data only (no collection)
    python scripts/collect_news_data.py --preprocess-only

    # Custom date range
    python scripts/collect_news_data.py --start 2024-01-01 --end 2024-12-31 --preprocess

    # Recent data only (last 30 days)
    python scripts/collect_news_data.py --days 30

    # Health check only
    python scripts/collect_news_data.py --health-check

    # Validate collected data
    python scripts/collect_news_data.py --validate

Example:
    $ python scripts/collect_news_data.py --source fed,ecb --days 90 --preprocess
    [INFO] Collecting from sources: fed, ecb
    [INFO] Date range: 2025-11-17 to 2026-02-15 (90 days)
    [INFO] Fed: 15 documents collected
    [INFO] ECB: 28 documents collected
    [INFO] Bronze layer export complete
    [INFO] Starting Silver preprocessing...
    [INFO] Processed 43 articles with sentiment analysis
    [SUCCESS] Collection and preprocessing complete
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from src.ingestion.collectors.boe_collector import BoECollector
from src.ingestion.collectors.ecb_news_collector import ECBNewsCollector
from src.ingestion.collectors.fed_collector import FedCollector
from src.ingestion.preprocessors.news_preprocessor import NewsPreprocessor
from src.shared.config import Config
from src.shared.utils import setup_logger

# Available news sources
AVAILABLE_SOURCES = ["fed", "ecb", "boe"]  # GDELT excluded - different schema


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect news data from central bank sources (Fed, ECB, BoE)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--source",
        type=str,
        help=f"Comma-separated list of sources to collect from. Available: {', '.join(AVAILABLE_SOURCES)}. Default: all sources",
        metavar="SOURCE",
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
        "--days",
        type=int,
        help="Collect last N days (overrides --start). Example: --days 30",
        metavar="N",
    )

    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Run health check only and exit",
    )

    parser.add_argument(
        "--preprocess",
        action="store_true",
        help="Also run Silver preprocessing after collection (sentiment analysis)",
    )

    parser.add_argument(
        "--preprocess-only",
        action="store_true",
        help="Skip collection and only preprocess existing raw data",
    )

    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate preprocessed data quality",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def get_collector(source: str, logger):
    """Initialize collector for given source.

    Args:
        source: Source name (fed, ecb, boe)
        logger: Logger instance

    Returns:
        Collector instance

    Raises:
        ValueError: If source is not supported
    """
    collectors = {
        "fed": FedCollector,
        "ecb": ECBNewsCollector,
        "boe": BoECollector,
    }

    if source not in collectors:
        raise ValueError(f"Unknown source: {source}. Available: {', '.join(AVAILABLE_SOURCES)}")

    collector_class = collectors[source]
    output_dir = Config.DATA_DIR / "raw" / "news" / source
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Initializing %s collector", source.upper())
    return collector_class(output_dir=output_dir)


def collect_from_source(
    collector,
    source: str,
    start_date: datetime,
    end_date: datetime,
    logger,
) -> int:
    """Collect data from a single source.

    Args:
        collector: Collector instance
        source: Source name for logging
        start_date: Start of date range
        end_date: End of date range
        logger: Logger instance

    Returns:
        Number of documents collected
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("Collecting from: %s", source.upper())
    logger.info("Date range: %s to %s", start_date.date(), end_date.date())
    logger.info("=" * 60)

    try:
        # Collect raw data (returns dict of document_type -> list[dict])
        data = collector.collect(start_date=start_date, end_date=end_date)

        # Count total documents
        total_docs = sum(len(docs) for docs in data.values())

        if total_docs == 0:
            logger.warning("No documents collected from %s", source)
            return 0

        # Export to Bronze layer (JSONL files)
        exported_paths = collector.export_all(data)

        logger.info("✓ %s: %d documents collected", source.upper(), total_docs)

        # Log breakdown by document type
        for doc_type, docs in data.items():
            if docs:
                logger.info("  • %s: %d documents", doc_type, len(docs))

        logger.info("  • Exported to: %s", collector.output_dir)
        for doc_type, path in exported_paths.items():
            logger.info("    - %s: %s", doc_type, path.name)

        return total_docs

    except Exception as e:
        logger.error("Failed to collect from %s: %s", source, e, exc_info=True)
        return 0


def collect_source_wrapper(source: str, start_date: datetime, end_date: datetime, logger):
    """Wrapper function for parallel collection.

    Args:
        source: Source name (fed, ecb, boe)
        start_date: Start of date range
        end_date: End of date range
        logger: Logger instance

    Returns:
        Tuple of (source, document_count)
    """
    try:
        collector = get_collector(source, logger)
        count = collect_from_source(collector, source, start_date, end_date, logger)
        return (source, count)
    except Exception as e:
        logger.error("Failed to initialize %s collector: %s", source, e, exc_info=True)
        return (source, 0)


def preprocess_news(sources: list[str], logger) -> bool:
    """Run Silver preprocessing on collected Bronze data.

    Args:
        sources: List of sources to preprocess
        logger: Logger instance

    Returns:
        True if preprocessing succeeded
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("Stage 2: Silver Layer Preprocessing (Sentiment Analysis)")
    logger.info("=" * 60)

    # Check if raw data exists
    input_dir = Config.DATA_DIR / "raw" / "news"
    if not input_dir.exists():
        logger.error("No raw news data found in %s", input_dir)
        logger.error("Run without --preprocess-only to collect data first")
        return False

    # Check for JSONL files in each source directory
    has_data = False
    for source in sources:
        source_dir = input_dir / source
        if source_dir.exists() and list(source_dir.glob("*.jsonl")):
            has_data = True
            logger.info("Found Bronze data for: %s", source)

    if not has_data:
        logger.error("No Bronze JSONL files found for selected sources: %s", ", ".join(sources))
        logger.error(
            "Available sources: %s", ", ".join([d.name for d in input_dir.iterdir() if d.is_dir()])
        )
        return False

    # Initialize preprocessor
    output_dir = Config.DATA_DIR / "processed" / "sentiment"
    log_file = Config.LOGS_DIR / "preprocessors" / f"news_{datetime.now():%Y%m%d_%H%M%S}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Initializing NewsPreprocessor with FinBERT sentiment model...")

    preprocessor = NewsPreprocessor(
        input_dir=input_dir,
        output_dir=output_dir,
        log_file=log_file,
    )

    try:
        # Preprocess Bronze data - process each source separately
        logger.info("Processing Bronze JSONL files...")
        result = {}

        for source in sources:
            source_dir = input_dir / source
            if source_dir.exists() and list(source_dir.glob("*.jsonl")):
                try:
                    logger.info("  Processing %s...", source.upper())
                    df = preprocessor.preprocess(source=source)
                    if not df.empty:
                        result[source] = df
                        logger.info("    ✓ %d articles processed", len(df))
                    else:
                        logger.warning("    ⚠ No articles processed from %s", source)
                except Exception as e:
                    logger.error("    ✗ Failed to process %s: %s", source, e)
                    continue

        if not result or all(df.empty for df in result.values()):
            logger.warning("No data to preprocess")
            return False

        # Count total records
        total_records = sum(len(df) for df in result.values())
        logger.info("✓ Processed %d articles with sentiment analysis", total_records)

        # Log breakdown by source
        for source, df in result.items():
            if not df.empty:
                sentiment_counts = df["sentiment_label"].value_counts()
                logger.info("  • %s: %d articles", source.upper(), len(df))
                for label, count in sentiment_counts.items():
                    logger.info("    - %s: %d", label, count)

        # Validate Silver schema
        logger.info("Validating Silver schema...")
        for source, df in result.items():
            try:
                preprocessor.validate(df)
                logger.info("  ✓ %s: validation passed", source.upper())
            except ValueError as e:
                logger.error("  ✗ %s: validation failed: %s", source.upper(), e)
                return False

        # Export to Silver layer (partitioned Parquet)
        logger.info("Exporting to Silver layer...")
        export_summary = {}
        for source, df in result.items():
            if not df.empty:
                paths = preprocessor.export_partitioned(df)
                export_summary[source] = len(paths)
                logger.info("  ✓ %s: %d partitions exported", source.upper(), len(paths))

        logger.info("✓ Silver layer export complete")
        logger.info("  Location: %s", output_dir)
        logger.info(
            "  Structure: source={source}/year={year}/month={month}/sentiment_cleaned.parquet"
        )

        return True

    except Exception as e:
        logger.error("Preprocessing failed: %s", e, exc_info=True)
        return False


def main() -> int:
    """Main collection script."""
    args = parse_args()

    # Setup logger
    logger = setup_logger(
        "collect_news",
        level="DEBUG" if args.verbose else "INFO",
    )

    try:
        # Parse sources
        if args.source:
            sources = [s.strip().lower() for s in args.source.split(",")]
            # Validate sources
            invalid = [s for s in sources if s not in AVAILABLE_SOURCES]
            if invalid:
                logger.error("Invalid source(s): %s", ", ".join(invalid))
                logger.error("Available sources: %s", ", ".join(AVAILABLE_SOURCES))
                return 1
        else:
            sources = AVAILABLE_SOURCES.copy()

        logger.info("Selected sources: %s", ", ".join(sources))

        # Parse dates
        if args.days:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=args.days)
            logger.info("Collecting last %d days", args.days)
        else:
            end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()
            start_date = (
                datetime.strptime(args.start, "%Y-%m-%d")
                if args.start
                else end_date - timedelta(days=90)  # Default: 3 months
            )

        logger.info("Date range: %s to %s", start_date.date(), end_date.date())

        # Handle preprocess-only mode
        if args.preprocess_only:
            logger.info("Preprocess-only mode: skipping collection")
            success = preprocess_news(sources, logger)
            return 0 if success else 1

        # Health check mode
        if args.health_check:
            logger.info("")
            logger.info("=" * 60)
            logger.info("Running Health Checks")
            logger.info("=" * 60)

            all_healthy = True
            for source in sources:
                try:
                    collector = get_collector(source, logger)
                    is_healthy = collector.health_check()
                    status = "✓ PASSED" if is_healthy else "✗ FAILED"
                    logger.info("%s: %s", source.upper(), status)
                    if not is_healthy:
                        all_healthy = False
                except Exception as e:
                    logger.error("%s: ✗ ERROR - %s", source.upper(), e)
                    all_healthy = False

            if all_healthy:
                logger.info("=" * 60)
                logger.info("All health checks passed")
                return 0
            else:
                logger.error("=" * 60)
                logger.error("Some health checks failed")
                return 1

        # Collection stage
        logger.info("")
        logger.info("=" * 60)
        logger.info("Stage 1: Bronze Layer Collection (Raw Data)")
        logger.info("=" * 60)
        logger.info("Running parallel collection from %d source(s)...", len(sources))

        # Run collections in parallel using ThreadPoolExecutor
        total_collected = 0
        collection_results = {}

        with ThreadPoolExecutor(max_workers=len(sources)) as executor:
            # Submit all collection tasks
            future_to_source = {
                executor.submit(
                    collect_source_wrapper, source, start_date, end_date, logger
                ): source
                for source in sources
            }

            # Collect results as they complete
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    source_name, count = future.result()
                    collection_results[source_name] = count
                    total_collected += count
                except Exception as e:
                    logger.error(
                        "Unexpected error collecting from %s: %s", source, e, exc_info=True
                    )
                    collection_results[source] = 0

        if total_collected == 0:
            logger.warning("No documents collected from any source")
            return 1

        logger.info("")
        logger.info("=" * 60)
        logger.info("Collection Summary")
        logger.info("=" * 60)
        logger.info("Total documents collected: %d", total_collected)
        for source, count in collection_results.items():
            logger.info("  • %s: %d documents", source.upper(), count)
        logger.info("Bronze layer location: %s", Config.DATA_DIR / "raw" / "news")

        # Optional preprocessing
        if args.preprocess:
            success = preprocess_news(sources, logger)
            if not success:
                logger.error("Preprocessing failed")
                return 1

        # Optional validation
        if args.validate and args.preprocess:
            logger.info("")
            logger.info("=" * 60)
            logger.info("Validation Summary")
            logger.info("=" * 60)
            logger.info("✓ All validation checks passed")

        logger.info("")
        logger.info("=" * 60)
        logger.info("SUCCESS - Collection pipeline complete")
        logger.info("=" * 60)

        return 0

    except KeyboardInterrupt:
        logger.warning("Operation cancelled by user")
        return 130
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
