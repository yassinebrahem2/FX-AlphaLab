#!/usr/bin/env python3
"""Run GoogleTrendsSignalNode for a date range and print a short summary."""

from __future__ import annotations

import argparse
import datetime
from pathlib import Path

from src.agents.sentiment.google_trends_node import GoogleTrendsSignalNode


def main() -> None:
    p = argparse.ArgumentParser(description="Run GoogleTrendsSignalNode for a date range")
    p.add_argument("--start", type=str, required=True)
    p.add_argument("--end", type=str, required=True)
    args = p.parse_args()

    start_date = datetime.datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(args.end, "%Y-%m-%d")

    silver_dir = Path("data/processed/sentiment/source=google_trends")
    node = GoogleTrendsSignalNode(silver_dir=silver_dir)

    print(f"Running GoogleTrendsSignalNode for {start_date} → {end_date}")
    df = node.compute(start_date=start_date, end_date=end_date)

    if df is None or df.empty:
        print("No results produced for the given date range.")
        return

    print("\nSummary:")
    print(f"Rows: {len(df)}")
    print(f"Date range: {df['date'].min()} → {df['date'].max()}")
    print("\nSample rows:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
