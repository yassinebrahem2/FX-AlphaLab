"""Script to preprocess news articles from Bronze to Silver layer.

Reads JSONL files from data/raw/news/{source}/ and transforms to data/processed/sentiment/.

Usage:
    python scripts/preprocess_news.py                  # Process all sources
    python scripts/preprocess_news.py --source fed     # Process specific source
    python scripts/preprocess_news.py --start-date 2026-01-01 --end-date 2026-02-12

Output:
    Parquet file: data/processed/sentiment/sentiment_{start}_{end}.parquet
"""

import argparse
from datetime import datetime

from src.ingestion.preprocessors.news_preprocessor import NewsPreprocessor
from src.shared.config import Config


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess news articles from Bronze to Silver layer"
    )
    parser.add_argument(
        "--source",
        type=str,
        help="Specific source to process (e.g., 'fed', 'ecb', 'gdelt', 'boe'). If not provided, processes all sources.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for filtering (YYYY-MM-DD). Optional.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for filtering (YYYY-MM-DD). Optional.",
    )
    args = parser.parse_args()

    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None

    # Setup directories
    input_dir = Config.DATA_DIR / "raw" / "news"
    output_dir = Config.DATA_DIR / "processed" / "sentiment"
    log_file = Config.LOGS_DIR / "preprocessors" / f"news_{datetime.now():%Y%m%d_%H%M%S}.log"

    print("News Preprocessor")
    print(f"{'=' * 60}")
    print(f"Input:  {input_dir}")
    print(f"Output: {output_dir}")
    print(f"Source: {args.source or 'all'}")
    print(f"Date Range: {start_date or 'any'} to {end_date or 'any'}")
    print(f"Log:    {log_file}")
    print()

    # Initialize preprocessor
    preprocessor = NewsPreprocessor(
        input_dir=input_dir,
        output_dir=output_dir,
        log_file=log_file,
    )

    try:
        # Preprocess
        print("Starting preprocessing...")
        df = preprocessor.preprocess(
            start_date=start_date,
            end_date=end_date,
            source=args.source,
        )

        # Export
        print(f"✓ Preprocessed {len(df)} articles")
        print(f"  - Sources: {df['source'].value_counts().to_dict()}")
        print(f"  - Document types: {df['document_type'].value_counts().to_dict()}")
        print(f"  - Sentiment: {df['sentiment_label'].value_counts().to_dict()}")
        print()

        # Export to partitioned Parquet
        output_paths = preprocessor.export_partitioned(df=df)
        print(f"✓ Exported {len(output_paths)} partitions:")
        for partition_key, path in output_paths.items():
            print(f"  - {partition_key}: {path}")
        print()

        # Sample preview
        print("Sample records:")
        print("-" * 60)
        for _, row in df.head(3).iterrows():
            print(f"[{row['timestamp_utc']}] {row['headline'][:60]}...")
            print(f"  Sentiment: {row['sentiment_label']} ({row['sentiment_score']:.3f})")
            print(f"  Source: {row['source']} | Type: {row['document_type']} | Pair: {row['pair']}")
            if row["speaker"]:
                print(f"  Speaker: {row['speaker']}")
            print()

        print("✓ Preprocessing completed successfully")

    except Exception as e:
        print(f"✗ Error during preprocessing: {e}")
        print(f"  Check log file for details: {log_file}")
        import traceback

        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
