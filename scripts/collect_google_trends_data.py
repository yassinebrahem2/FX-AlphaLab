"""
Google Trends data collection script (pytrends).

Two-stage pipeline:
1. Collection (Bronze): Fetch raw interest-over-time data → data/raw/attention/google_trends/
2. Preprocessing (Silver): (Optional) Transform to standardized schema/features → data/processed/attention/

Usage:
    # Collect Bronze (raw) data only
    python scripts/collect_google_trends_data.py

    # Collect specific date range
    python scripts/collect_google_trends_data.py --start 2023-01-01 --end 2023-12-31

    # Override keywords
    python scripts/collect_google_trends_data.py --keywords "inflation,recession,EURUSD,US dollar"

    # Restrict geography (e.g., US, DE, GB; empty = worldwide)
    python scripts/collect_google_trends_data.py --geo US

    # Use google property filter (web/images/news/youtube)
    python scripts/collect_google_trends_data.py --gprop news

    # Health check only
    python scripts/collect_google_trends_data.py --health-check

Notes:
- Google Trends values are indexed 0-100 per keyword/timeframe/geo.
- For stability and comparability, this collector queries ONE keyword at a time.
- If you hit rate-limits (HTTP 429), increase delay in collector or provide proxies.
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone


from src.ingestion.collectors.google_trends_collector import GoogleTrendsCollector
from src.shared.config import Config
from src.shared.utils import setup_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect attention signals from Google Trends (pytrends)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD). Default: 2 years ago")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD). Default: today")

    parser.add_argument(
        "--keywords",
        type=str,
        help='Comma-separated keywords. Example: "inflation,recession,EURUSD,US dollar"',
    )

    parser.add_argument(
        "--geo",
        type=str,
        default=None,
        help="Geography code (e.g., US, DE, GB). Default: from Config or worldwide",
    )

    parser.add_argument(
        "--gprop",
        type=str,
        default=None,
        help="Google property: '' (web), 'news', 'images', 'youtube', 'froogle'. Default: from Config",
    )

    parser.add_argument("--health-check", action="store_true", help="Run health check and exit")

    parser.add_argument(
        "--preprocess",
        action="store_true",
        help="Also run Silver preprocessing after collection (optional / if implemented)",
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    return parser.parse_args()


def _parse_date(s: str) -> datetime:
    # interpret as UTC midnight
    dt = datetime.strptime(s, "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)


def main() -> int:
    args = parse_args()

    logger = setup_logger(
        "collect_google_trends",
        level="DEBUG" if args.verbose else "INFO",
    )

    try:
        collector = GoogleTrendsCollector()
        logger.info(
            "GoogleTrendsCollector initialized (Bronze layer: %s)",
            collector.output_dir,
        )

        # Health check
        if args.health_check:
            if not collector.health_check():
                logger.error("Google Trends health check failed (pytrends)")
                logger.error("Possible causes: network issue, temporary Google blocking, rate limit.")
                logger.error("Tip: try again later or configure proxies in collector if needed.")
                return 1

            logger.info("Health check: PASSED")
            logger.info("Health check complete, exiting")
            return 0


        # Dates
        if args.start:
            try:
                start_date = _parse_date(args.start)
            except ValueError:
                logger.error("Invalid --start date format. Use YYYY-MM-DD")
                return 1
        else:
            start_date = (datetime.now(timezone.utc) - timedelta(days=730)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        if args.end:
            try:
                end_date = _parse_date(args.end)
            except ValueError:
                logger.error("Invalid --end date format. Use YYYY-MM-DD")
                return 1
        else:
            end_date = datetime.now(timezone.utc)

        if start_date > end_date:
            logger.error("Start date must be before end date")
            return 1

        # Keywords
        if args.keywords:
            keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
        else:
            keywords = getattr(Config, "GOOGLE_TRENDS_KEYWORDS", None) or [
                "inflation",
                "recession",
                "interest rates",
                "Federal Reserve",
                "European Central Bank",
                "US dollar",
                "euro",
                "EURUSD",
                "USDJPY",
                "GBPUSD",
            ]

        geo = args.geo if args.geo is not None else getattr(Config, "GOOGLE_TRENDS_GEO", "")
        gprop = args.gprop if args.gprop is not None else getattr(Config, "GOOGLE_TRENDS_GPROP", "")

        logger.info(
            "Collecting Bronze data from %s to %s | keywords=%d geo=%s gprop=%s",
            start_date.date(),
            end_date.date(),
            len(keywords),
            geo or "GLOBAL",
            gprop or "web",
        )

        # Collect (Bronze)
        # NOTE: Our collector currently reads keywords from Config by default.
        # If you want CLI keywords to be used, patch the collector to accept keywords as parameter,
        # OR temporarily set Config.GOOGLE_TRENDS_KEYWORDS before calling collect.
        #
        # Best approach: add collect(keywords=..., ...) signature in collector.
        #
        # If your collector already supports passing keywords, use that.
        try:
            data = collector.collect(start_date=start_date, end_date=end_date, keywords=keywords, geo=geo, gprop=gprop)  # type: ignore[arg-type]
        except TypeError:
            # Fallback if your collector signature is only (start_date, end_date)
            # and reads keywords/geo/gprop from Config.
            # In that case, it will still run with defaults.
            data = collector.collect(start_date=start_date, end_date=end_date)

        if not data or "interest_over_time" not in data:
            logger.warning("No data collected")
            return 1

        df = data["interest_over_time"]
        if df is None or df.empty:
            logger.warning("Collected dataset is empty")
            return 1

        # Export to Bronze (unique filename per run/params)
        run_ts = datetime.now(timezone.utc).strftime("%H%M%S")  # only time
        dataset_name = (
            f"interest_over_time_"
            f"{start_date:%Y%m%d}_{end_date:%Y%m%d}_"
            f"{(geo or 'GLOBAL').replace(' ', '')}_"
            f"{(gprop or 'web').replace(' ', '')}_"
            f"kw{len(keywords)}_"
            f"{run_ts}"
        )

        path = collector.export_csv(df, dataset_name)
        logger.info("Exported Bronze CSV -> %s", path)


        # Optional preprocess step (only if you implement a preprocessor)
        if args.preprocess:
            logger.warning(
                "Preprocess flag set, but no Google Trends preprocessor is wired here yet."
            )
            logger.warning(
                "Implement e.g. AttentionNormalizer and write to data/processed/attention/."
            )
            logger.info("SUCCESS: Collection complete (Bronze only, preprocess placeholder)")
        else:
            logger.info("SUCCESS: Collection complete (Bronze only)")
            logger.info("Run with --preprocess once Silver preprocessing is implemented")

        return 0

    except KeyboardInterrupt:
        logger.warning("Collection interrupted by user")
        return 130

    except Exception as e:
        logger.exception("Unexpected error during collection: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
