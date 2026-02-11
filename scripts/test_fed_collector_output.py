"""Quick test script to see FedCollector output and data storage."""

import json
from datetime import datetime, timedelta
from pathlib import Path

from src.ingestion.collectors.fed_collector import FedCollector


def main():
    print("=" * 80)
    print("FedCollector Output Test")
    print("=" * 80)

    # Create collector
    output_dir = Path("data/raw/news/fed")
    collector = FedCollector(output_dir=output_dir)

    print("\n✓ Collector initialized")
    print(f"  Output directory: {collector.output_dir}")
    print(f"  Directory exists: {collector.output_dir.exists()}")

    # Test health check
    print("\n✓ Testing health check...")
    is_healthy = collector.health_check()
    print(f"  RSS feed accessible: {is_healthy}")

    if not is_healthy:
        print("\n⚠ RSS feed not accessible. Check internet connection.")
        return

    # Collect recent data (last 30 days)
    print("\n✓ Collecting data from last 30 days...")
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now()

    print(f"  Start date: {start_date.date()}")
    print(f"  End date: {end_date.date()}")

    try:
        data = collector.collect(start_date=start_date, end_date=end_date)

        print("\n✓ Collection complete!")
        print("\n📊 Data Summary:")
        print("-" * 80)

        total_docs = 0
        for doc_type, documents in data.items():
            count = len(documents)
            total_docs += count
            print(f"  {doc_type:20s}: {count:3d} documents")

        print("-" * 80)
        print(f"  {'TOTAL':20s}: {total_docs:3d} documents")

        # Show sample document from first non-empty category
        print("\n📄 Sample Document:")
        print("-" * 80)

        sample_doc = None
        sample_type = None
        for doc_type, documents in data.items():
            if documents:
                sample_doc = documents[0]
                sample_type = doc_type
                break

        if sample_doc:
            print(f"Category: {sample_type}")
            print(f"Title: {sample_doc['title'][:80]}...")
            print(f"URL: {sample_doc['url']}")
            print(f"Document Type: {sample_doc['document_type']}")
            print(f"Published: {sample_doc['timestamp_published']}")
            print(f"Speaker: {sample_doc['speaker'] or '(none)'}")
            print(f"Content Length: {len(sample_doc['content'])} characters")
            print(f"Content Preview: {sample_doc['content'][:200]}...")
            print(f"Metadata keys: {list(sample_doc['metadata'].keys())}")

            # Show full structure
            print("\n📋 Full Document Structure (first doc):")
            print(json.dumps(sample_doc, indent=2, ensure_ascii=False)[:1000] + "...")

        # Export to JSONL
        print("\n💾 Exporting to JSONL...")
        paths = collector.export_all(data=data)

        print("\n✓ Export complete!")
        print("\n📁 Exported Files:")
        print("-" * 80)

        for doc_type, path in paths.items():
            file_size = path.stat().st_size
            with open(path, encoding="utf-8") as f:
                line_count = sum(1 for _ in f)

            print(f"  {doc_type:20s}: {path.name}")
            print(f"  {'':20s}  Size: {file_size:,} bytes")
            print(f"  {'':20s}  Lines: {line_count}")
            print()

        # Show content of one JSONL file
        if paths:
            first_path = list(paths.values())[0]
            print(f"📖 Sample JSONL Content ({first_path.name}):")
            print("-" * 80)
            with open(first_path, encoding="utf-8") as f:
                first_line = f.readline()
                doc = json.loads(first_line)
                print(json.dumps(doc, indent=2, ensure_ascii=False)[:800] + "...")

        print("\n✅ All data successfully collected and exported to:")
        print(f"   {collector.output_dir.absolute()}")

    except Exception as e:
        print(f"\n❌ Error during collection: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
