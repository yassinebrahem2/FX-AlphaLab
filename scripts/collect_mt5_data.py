"""MT5 Data Collection Script

Simplified wrapper for MT5 data collection.
For advanced usage with CLI arguments, run the collector module directly:

    python -m data.ingestion.mt5_collector --pairs EURUSD,GBPUSD --timeframes H1,D1

This script provides a quick no-arguments way to collect default datasets.
"""

from data.ingestion.mt5_collector import MT5Collector


def main() -> None:
    """Collect MT5 data with default configuration."""
    print("MT5 Data Collection (Default Configuration)")
    print("=" * 60)
    print("For advanced options, use: python -m data.ingestion.mt5_collector --help")
    print()

    # Use defaults: EURUSD,GBPUSD,USDJPY,EURGBP,USDCHF × H1,H4,D1
    collector = MT5Collector()

    # Health check
    print("Running health check...")
    if not collector.health_check():
        print("❌ MT5 terminal is not accessible!")
        print("   Make sure MT5 is installed and demo account configured.")
        print("   See docs/mt5_setup.md for setup instructions.")
        return

    print("✅ MT5 terminal is accessible")
    print()

    # Collect and export
    print(f"Collecting {len(collector.pairs)} pairs × {len(collector.timeframes)} timeframes...")
    print(f"Pairs: {', '.join(collector.pairs)}")
    print(f"Timeframes: {', '.join(collector.timeframes)}")
    print(f"Bars per dataset: {collector.n_bars:,}")
    print()

    try:
        datasets = collector.collect()

        print(f"✅ Collected {len(datasets)} datasets:")
        for name, df in datasets.items():
            print(f"   - {name}: {len(df):,} records")

        # Export to CSV
        print()
        print("Exporting to CSV...")
        for name, df in datasets.items():
            path = collector.export_csv(df, name)
            print(f"   - {path.name}")

        print()
        print(f"✅ Complete! Files saved to: {collector.output_dir}")

    except Exception as e:
        print(f"❌ Collection failed: {e}")
        raise


if __name__ == "__main__":
    main()
