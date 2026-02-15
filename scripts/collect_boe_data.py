"""Recollect BoE news data with fixed HTML parser."""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.collectors.boe_collector import BoECollector


def main():
    collector = BoECollector()

    # Collect last 90 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)

    print(f"Collecting BoE data from {start_date.date()} to {end_date.date()}")

    # Collect
    results = collector.collect(start_date=start_date, end_date=end_date)

    # Export each document type separately
    exported_paths = []
    for doc_type, docs in results.items():
        if docs:  # Only export if we have documents
            path = collector.export_jsonl(docs, document_type=doc_type)
            exported_paths.append(path)

    print(f"\nCollected {sum(len(docs) for docs in results.values())} documents")
    for doc_type, docs in results.items():
        print(f"  {doc_type}: {len(docs)} documents")

    print(f"\nExported to: {collector.output_dir}")
    for path in exported_paths:
        print(f"  {path}")


if __name__ == "__main__":
    main()
