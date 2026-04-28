import os
from datetime import datetime, timedelta
from pathlib import Path

from src.ingestion.collectors.gdelt_gkg_collector import GDELTGKGCollector
from src.shared.config import Config


def main() -> None:
    print("=" * 80)
    print("GDELTGKGCollector Output Test")
    print("=" * 80)

    if Config.GOOGLE_APPLICATION_CREDENTIALS:
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

    output_dir = Path("data/raw/news/gdelt")
    collector = GDELTGKGCollector(output_dir=output_dir)

    print("\n✓ Collector initialized")
    print(f"  Output directory: {collector.output_dir}")
    print(f"  Directory exists: {collector.output_dir.exists()}")

    print("\n✓ Testing health check...")
    try:
        is_healthy = collector.health_check()
        print(f"  BigQuery accessible: {is_healthy}")

        if not is_healthy:
            print("\n⚠ BigQuery not accessible.")
            print("  Check your Google Cloud credentials and BigQuery API access.")
            return
    except Exception as exc:
        print(f"\n⚠ Health check failed: {exc}")
        print("  Verify your service account has BigQuery permissions.")
        return

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
        print(f"  Total documents: {documents}")

        print("\n✓ Test complete!")
        print("=" * 80)
    except Exception as exc:
        print(f"\n❌ Error during collection: {exc}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
