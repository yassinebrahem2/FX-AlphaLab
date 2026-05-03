#!/usr/bin/env python3
"""Run GDELTSignalNode for a single month and print a short summary."""

from __future__ import annotations

import argparse
import calendar
import datetime
from pathlib import Path

from src.agents.sentiment.gdelt_node import GDELTSignalNode


def main() -> None:
    p = argparse.ArgumentParser(description="Run GDELTSignalNode for a month")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()

    year = args.year
    month = args.month
    last_day = calendar.monthrange(year, month)[1]
    # Use datetime objects (node expects datetime, not date)
    start_date = datetime.datetime(year, month, 1, 0, 0)
    # set end to end of day
    end_date = datetime.datetime(year, month, last_day, 23, 59, 59)

    silver_dir = Path("data/processed/sentiment/source=gdelt")
    node = GDELTSignalNode(silver_dir=silver_dir)

    print(f"Running GDELTSignalNode for {year:04d}-{month:02d} ({start_date} → {end_date})")
    df = node.compute(start_date=start_date, end_date=end_date)

    if df is None or df.empty:
        print("No results produced for the given month.")
        return

    # Summary
    print("\nSummary:")
    print(f"Rows: {len(df)}")
    print(f"Date range: {df['date'].min()} → {df['date'].max()}")
    print("\nSample rows:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
