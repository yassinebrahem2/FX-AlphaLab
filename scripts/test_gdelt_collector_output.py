"""Quick test script to see GDELTCollector output and data storage."""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from src.ingestion.collectors.gdelt_collector import GDELTCollector
from src.shared.config import Config


def main():
    print("=" * 80)
    print("GDELTCollector Output Test")
    print("=" * 80)

    # Check if Google Cloud credentials are configured
    if Config.GOOGLE_APPLICATION_CREDENTIALS:
        # Make sure it's set in the actual environment for Google Cloud SDK
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Config.GOOGLE_APPLICATION_CREDENTIALS
        print("\n✓ Google Cloud credentials loaded from .env")
        print(f"  Path: {Config.GOOGLE_APPLICATION_CREDENTIALS}")
    else:
        print("\n⚠ GOOGLE_APPLICATION_CREDENTIALS not set in .env")
        print("  1. Add to .env file: GOOGLE_APPLICATION_CREDENTIALS=path/to/service_account.json")
        print(
            '  2. Or set temporarily: $env:GOOGLE_APPLICATION_CREDENTIALS="path\\to\\service_account.json"'
        )
        print("  See docs/ingestion/gdelt.md for setup instructions.")
        return

    # Create collector
    output_dir = Path("data/raw/news/gdelt")
    collector = GDELTCollector(output_dir=output_dir)

    print("\n✓ Collector initialized")
    print(f"  Output directory: {collector.output_dir}")
    print(f"  Directory exists: {collector.output_dir.exists()}")

    # Test health check
    print("\n✓ Testing health check...")
    try:
        is_healthy = collector.health_check()
        print(f"  BigQuery accessible: {is_healthy}")

        if not is_healthy:
            print("\n⚠ BigQuery not accessible.")
            print("  Check your Google Cloud credentials and BigQuery API access.")
            return
    except Exception as e:
        print(f"\n⚠ Health check failed: {e}")
        print("  Verify your service account has BigQuery permissions.")
        return

    # Collect recent data (last 2 days to minimize cost)
    print("\n✓ Collecting data from last 2 days...")
    start_date = datetime.now() - timedelta(days=2)
    end_date = datetime.now() - timedelta(days=1)

    print(f"  Start date: {start_date.date()}")
    print(f"  End date: {end_date.date()}")
    print("  (Date range kept small to minimize BigQuery costs)")

    try:
        data = collector.collect(start_date=start_date, end_date=end_date)

        print("\n✓ Collection complete!")
        print("\n📊 Data Summary:")
        print("-" * 80)

        documents = data["aggregated"]
        print(f"  Total documents: {len(documents)}")

        # Count by credibility tier
        tier_counts = {}
        for doc in documents:
            tier = doc["metadata"]["credibility_tier"]
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        print("\n  By credibility tier:")
        for tier in sorted(tier_counts.keys()):
            tier_name = {
                1: "Tier 1 (Reuters/Bloomberg/FT)",
                2: "Tier 2 (WSJ/CNBC)",
                3: "Tier 3 (Other)",
            }
            print(
                f"    {tier_name.get(tier, f'Tier {tier}'):35s}: {tier_counts[tier]:3d} documents"
            )

        # Show sample document
        if documents:
            print("\n📄 Sample Document (first):")
            print("-" * 80)

            sample_doc = documents[0]
            print(f"Source: {sample_doc['source']}")
            print(f"Source Domain: {sample_doc['source_domain']}")
            print(f"URL: {sample_doc['url'][:80]}...")
            print(f"Published: {sample_doc['timestamp_published']}")
            print(f"Collected: {sample_doc['timestamp_collected']}")
            print(f"Tone: {sample_doc['tone']}")
            print(f"Credibility Tier: {sample_doc['metadata']['credibility_tier']}")
            print(f"Themes: {sample_doc['themes'][:5]}...")  # First 5 themes
            print(f"Locations: {sample_doc['locations'][:3]}...")  # First 3 locations
            print(f"Organizations: {sample_doc['organizations'][:3]}...")  # First 3 orgs

            # Show full structure
            print("\n📋 Full Document Structure (first doc):")
            print(json.dumps(sample_doc, indent=2, ensure_ascii=False)[:1500] + "...")

        # Export to JSONL
        print("\n💾 Exporting to JSONL...")
        path = collector.export_jsonl(documents)

        print("\n✓ Export complete!")
        print("\n📁 Exported File:")
        print("-" * 80)

        file_size = path.stat().st_size
        with open(path, encoding="utf-8") as f:
            line_count = sum(1 for _ in f)

        print(f"  Path: {path}")
        print(f"  Size: {file_size:,} bytes")
        print(f"  Lines: {line_count}")

        # Sample a line from the file
        print("\n📝 Sample JSONL Line (first line):")
        print("-" * 80)
        with open(path, encoding="utf-8") as f:
            first_line = f.readline()
            parsed = json.loads(first_line)
            print(json.dumps(parsed, indent=2, ensure_ascii=False)[:1000] + "...")

        print("\n" + "=" * 80)
        print("✓ Test complete!")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ Error during collection: {e}")
        import traceback

        traceback.print_exc()
        return


if __name__ == "__main__":
    main()
