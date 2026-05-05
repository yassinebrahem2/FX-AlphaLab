"""Collector for Google Trends FX sentiment data."""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pytrends.request import TrendReq

from src.ingestion.collectors.base_collector import BaseCollector


class GoogleTrendsCollector(BaseCollector):
    """Collect Google Trends data for FX sentiment themes."""

    SOURCE_NAME = "google_trends"

    KEYWORD_GROUPS: dict[str, list[list[str]]] = {
        "fx_pairs": [
            ["EURUSD", "GBPUSD", "USDJPY", "euro dollar", "pound dollar"],
            ["Japanese yen", "GBP exchange rate", "USDCHF", "Swiss franc"],
        ],
        "central_banks": [
            ["ECB", "European Central Bank", "Christine Lagarde", "Federal Reserve", "FOMC"],
            ["Bank of England", "BoE", "Bank of Japan", "BOJ"],
        ],
        "macro_indicators": [
            ["CPI", "inflation", "eurozone inflation", "PMI", "bond yields"],
            ["rate hike", "rate cut", "recession", "UK inflation"],
        ],
        "risk_sentiment": [
            ["VIX", "volatility", "safe haven", "market crash", "DXY"],
            ["dollar index", "forex"],
        ],
    }

    def __init__(self, output_dir: Path, log_file: Path | None = None) -> None:
        super().__init__(output_dir=output_dir, log_file=log_file)
        self.fetched_batches: list[str] = []
        self.failed_batches: list[str] = []

    def collect(
        self,
        start_date: datetime,
        end_date: datetime,
        force: bool = False,
    ) -> dict[str, int]:
        timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"
        results: dict[str, int] = {}
        self.fetched_batches = []
        self.failed_batches = []
        pytrends = TrendReq(hl="en-US", tz=0)

        for theme, keyword_batches in self.KEYWORD_GROUPS.items():
            for batch_index, kw_list in enumerate(keyword_batches):
                batch_key = f"trends_{theme}_{batch_index}"
                output_path = self.output_dir / f"{batch_key}.csv"

                if output_path.exists() and not force:
                    with output_path.open("r", encoding="utf-8") as handle:
                        row_count = max(0, sum(1 for _ in handle) - 1)
                    results[batch_key] = row_count
                    continue

                self.fetched_batches.append(batch_key)

                try:
                    frame = self._fetch_with_retry(pytrends, kw_list, timeframe)
                    if "isPartial" in frame.columns:
                        frame = frame.drop(columns=["isPartial"])

                    frame.index = frame.index.rename("date")
                    frame = frame.reset_index()
                    frame.to_csv(output_path, index=False)
                    results[batch_key] = int(len(frame))
                    time.sleep(random.uniform(45, 75))
                except Exception as exc:
                    self.failed_batches.append(batch_key)
                    self.logger.error("Google Trends batch failed for %s: %s", batch_key, exc)

        return results

    def health_check(self) -> bool:
        try:
            pytrends = TrendReq(hl="en-US", tz=0)
            timeframe_end = datetime.utcnow().date()
            timeframe_start = timeframe_end - timedelta(days=30)
            timeframe = f"{timeframe_start:%Y-%m-%d} {timeframe_end:%Y-%m-%d}"
            frame = self._fetch_with_retry(pytrends, ["EURUSD"], timeframe)
            return not frame.empty
        except Exception:
            return False

    def _fetch_with_retry(
        self,
        pytrends: TrendReq,
        kw_list: list[str],
        timeframe: str,
        max_retries: int = 3,
    ) -> pd.DataFrame:
        retry_delay = 60
        last_error: Exception | None = None

        for attempt in range(max_retries):
            try:
                # Fresh session per attempt — avoids reusing a poisoned/rate-limited connection
                pt = TrendReq(hl="en-US", tz=0)
                pt.build_payload(kw_list=kw_list, timeframe=timeframe)
                return pt.interest_over_time()
            except Exception as exc:
                last_error = exc
                self.logger.warning(
                    "Attempt %d/%d failed for %s: %s: %s",
                    attempt + 1,
                    max_retries,
                    kw_list,
                    type(exc).__name__,
                    exc,
                )
                if attempt == max_retries - 1:
                    break
                time.sleep(retry_delay * (2**attempt))

        raise RuntimeError("Failed to fetch Google Trends data after retries") from last_error
