"""MT5 data collection and preprocessing script.

Two-stage pipeline:
1. Collection (Bronze): Fetch raw data from MetaTrader 5 → data/raw/mt5/
2. Preprocessing (Silver): Transform to standardized schema → data/processed/ohlcv/

Usage:
    # Collect Bronze (raw) data only (default pairs/timeframes)
    python scripts/collect_mt5_data.py

    # Collect and preprocess to Silver
    python scripts/collect_mt5_data.py --preprocess

    # Preprocess existing Bronze data only (no collection)
    python scripts/collect_mt5_data.py --preprocess-only

    # Custom pairs and timeframes
    python scripts/collect_mt5_data.py --pairs EURUSD,GBPUSD --timeframes H1,D1 --preprocess

    # Custom date range
    python scripts/collect_mt5_data.py --start 2023-01-01 --end 2023-12-31 --preprocess

    # Specific years of history
    python scripts/collect_mt5_data.py --years 5 --preprocess

    # Health check only
    python scripts/collect_mt5_data.py --health-check

Example:
    $ python scripts/collect_mt5_data.py --pairs EURUSD --timeframes H1,D1 --preprocess
    [INFO] MT5Collector initialized
    [INFO] Health check: PASSED
    [INFO] Collecting 1 pairs × 2 timeframes (5 years)
    [INFO] Collected mt5_EURUSD_H1: 43,800 records
    [INFO] Collected mt5_EURUSD_D1: 1,826 records
    [INFO] Exported 2 CSV files to data/raw/mt5/ (Bronze)
    [INFO] Starting Silver preprocessing...
    [INFO] Processed 2 datasets to data/processed/ohlcv/ (Silver)
    [SUCCESS] Collection and preprocessing complete
"""

import argparse
import sys
from datetime import datetime

from src.ingestion.collectors.mt5_collector import MT5Collector
from src.ingestion.preprocessors.price_normalizer import PriceNormalizer
from src.shared.config import Config
from src.shared.utils import setup_logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect MT5 price data for FX pairs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--pairs",
        type=str,
        help=f"Comma-separated FX pairs (default: {','.join(MT5Collector.DEFAULT_PAIRS)})",
        metavar="PAIRS",
    )

    parser.add_argument(
        "--timeframes",
        type=str,
        help=f"Comma-separated timeframes (default: {','.join(MT5Collector.DEFAULT_TIMEFRAMES)})",
        metavar="TFS",
    )

    parser.add_argument(
        "--years",
        type=int,
        default=MT5Collector.DEFAULT_YEARS,
        help=f"Years of history to fetch (default: {MT5Collector.DEFAULT_YEARS})",
        metavar="N",
    )

    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD, overrides --years)",
        metavar="DATE",
    )

    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD, default: today)",
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
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    parser.add_argument(
        "--list-symbols",
        action="store_true",
        help="List all available symbols from MT5 broker grouped by category, then exit",
    )

    return parser.parse_args()


def list_broker_symbols() -> int:
    """Connect to MT5 and print all available symbols grouped by category."""
    try:
        import MetaTrader5 as mt5  # noqa: N813
    except ImportError:
        print("ERROR: MetaTrader5 module not found. Install with: pip install MetaTrader5")
        return 1

    if not mt5.initialize():
        print(f"ERROR: MT5 initialisation failed: {mt5.last_error()}")
        return 1

    try:
        symbols = mt5.symbols_get()
        if not symbols:
            print("No symbols returned from MT5")
            return 1

        # Group by path/category
        from collections import defaultdict

        groups: dict[str, list[str]] = defaultdict(list)
        for s in symbols:
            # MT5 path looks like "Forex\EURUSD" or "Crypto\BTCUSD"
            parts = s.path.split("\\") if "\\" in s.path else s.path.split("/")
            category = parts[0] if parts else "Other"
            groups[category].append(s.name)

        print(f"\nBroker symbols ({len(symbols)} total)\n" + "=" * 60)
        for category in sorted(groups):
            syms = sorted(groups[category])
            print(f"\n[{category}] ({len(syms)} symbols)")
            # Print in columns of 4
            for i in range(0, len(syms), 4):
                print("  " + "  ".join(f"{s:<12}" for s in syms[i : i + 4]))

        print(f"\nTotal: {len(symbols)} symbols across {len(groups)} categories")
    finally:
        mt5.shutdown()

    return 0


def main() -> int:
    """Main collection script."""
    args = parse_args()

    # Setup logger
    logger = setup_logger(
        "collect_mt5",
        level="DEBUG" if args.verbose else "INFO",
    )

    # Handle symbol listing mode
    if args.list_symbols:
        return list_broker_symbols()

    try:
        # Parse pairs and timeframes
        pairs = args.pairs.split(",") if args.pairs else None
        timeframes = args.timeframes.split(",") if args.timeframes else None

        # Handle preprocess-only mode
        if args.preprocess_only:
            logger.info("Preprocess-only mode: skipping collection")
            logger.info("")
            logger.info("=" * 60)
            logger.info("Stage 2: Silver Layer Preprocessing")
            logger.info("=" * 60)

            normalizer = PriceNormalizer(
                input_dir=Config.DATA_DIR / "raw",
                output_dir=Config.DATA_DIR / "processed" / "ohlcv",
                sources=["mt5"],
            )

            # Check if raw data exists
            raw_dir = Config.DATA_DIR / "raw" / "mt5"
            if not raw_dir.exists() or not list(raw_dir.glob("*.csv")):
                logger.error("No raw MT5 data found in %s", raw_dir)
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
                start_date = None

            if args.end:
                try:
                    end_date = datetime.strptime(args.end, "%Y-%m-%d")
                except ValueError:
                    logger.error("Invalid end date format. Use YYYY-MM-DD")
                    return 1
            else:
                end_date = None

            silver_data = normalizer.preprocess(start_date=start_date, end_date=end_date)

            if not silver_data:
                logger.warning("No data preprocessed")
                return 1

            logger.info("Exporting Silver data...")
            for identifier, df in silver_data.items():
                if df.empty:
                    continue
                min_date = df["timestamp_utc"].min().to_pydatetime()
                max_date = df["timestamp_utc"].max().to_pydatetime()
                path = normalizer.export(df, identifier, min_date, max_date, format="parquet")
                logger.info("  ✓ Processed %s: %d records → %s", identifier, len(df), path.name)

            logger.info("")
            logger.info("=" * 60)
            logger.info("✓ Preprocessing Complete!")
            logger.info("=" * 60)
            return 0

        # Initialize collector
        collector = MT5Collector(
            pairs=pairs,
            timeframes=timeframes,
            years=args.years,
        )
        logger.info("MT5Collector initialized (Bronze layer: %s)", collector.output_dir)

        # Health check
        if not collector.health_check():
            logger.error("MT5 terminal health check failed")
            logger.error("Make sure MT5 is installed and a demo account is configured")
            logger.error("See docs/ingestion/mt5.md for setup instructions")
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
            start_date = None

        if args.end:
            try:
                end_date = datetime.strptime(args.end, "%Y-%m-%d")
            except ValueError:
                logger.error("Invalid end date format. Use YYYY-MM-DD")
                return 1
        else:
            end_date = None

        # Validate date range
        if start_date and end_date and start_date > end_date:
            logger.error("Start date must be before end date")
            return 1

        logger.info("=" * 60)
        logger.info("Stage 1: Bronze Layer Collection")
        logger.info("=" * 60)
        logger.info("Pairs: %s", ", ".join(collector.pairs))
        logger.info("Timeframes: %s", ", ".join(collector.timeframes))
        logger.info("History: %d years", collector.years)
        if start_date:
            logger.info(
                "Date range: %s to %s", start_date.date(), end_date.date() if end_date else "today"
            )

        # STAGE 1: Collect Bronze (raw) data
        logger.info("Collecting Bronze data...")
        data = collector.collect(start_date=start_date, end_date=end_date)

        if not data:
            logger.warning("No data collected")
            return 1

        # Export to Bronze layer
        logger.info("Exporting Bronze data...")
        for name, df in data.items():
            path = collector.export_csv(df, name)
            logger.info("  ✓ Exported %s: %d records → %s", name, len(df), path.name)

        logger.info("✓ Bronze collection complete: %d datasets", len(data))

        # STAGE 2: Preprocess to Silver (optional)
        if args.preprocess:
            logger.info("")
            logger.info("=" * 60)
            logger.info("Stage 2: Silver Layer Preprocessing")
            logger.info("=" * 60)

            normalizer = PriceNormalizer(
                input_dir=Config.DATA_DIR / "raw",
                output_dir=Config.DATA_DIR / "processed" / "ohlcv",
                sources=["mt5"],
            )

            silver_data = normalizer.preprocess(start_date=start_date, end_date=end_date)

            logger.info("Exporting Silver data...")
            for identifier, df in silver_data.items():
                if df.empty:
                    continue
                min_date = df["timestamp_utc"].min().to_pydatetime()
                max_date = df["timestamp_utc"].max().to_pydatetime()
                path = normalizer.export(df, identifier, min_date, max_date, format="parquet")
                logger.info("  ✓ Processed %s: %d records → %s", identifier, len(df), path.name)

            logger.info("✓ Silver preprocessing complete: %d datasets", len(silver_data))

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
