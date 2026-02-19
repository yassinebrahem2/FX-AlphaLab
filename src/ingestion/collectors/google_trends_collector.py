"""
Google Trends Collector (Bronze Layer).

Collects Interest Over Time time-series for a list of keywords using pytrends.

Bronze Layer output (raw CSV):
data/raw/attention/google_trends/{SOURCE_NAME}_{dataset}_{YYYYMMDD}.csv

Returns datasets as DataFrames with:
- snake_case columns
- source column
- utc timestamps
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
import random
import pandas as pd
from pytrends.request import TrendReq

from src.ingestion.collectors.base_collector import BaseCollector
from src.shared.config import Config


class GoogleTrendsCollector(BaseCollector):
    SOURCE_NAME = "google_trends"

    # Keep it polite to reduce risk of 429
    REQUEST_DELAY = 10.0

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
        hl: str = "en-US",
        tz: int = 360,
        retries: int = 2,
        backoff_factor: float = 0.2,
        timeout: tuple[int, int] = (10, 25),
        proxies: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            output_dir=output_dir or (Config.DATA_DIR / "raw" / "attention" / "google_trends"),
            log_file=log_file or (Config.LOGS_DIR / "collectors" / "google_trends_collector.log"),
        )

        # pytrends uses requests internally; these args help with resilience
        self._pytrends = TrendReq(
            hl=hl,
            tz=tz,
            timeout=timeout,
            proxies=proxies or {},
            retries=retries,
            backoff_factor=backoff_factor,
        )

    @staticmethod
    def _to_timeframe(
        start_date: datetime | None,
        end_date: datetime | None,
        default: str = "today 5-y",
    ) -> str:
        """
        Convert optional datetime window to pytrends timeframe format:
        'YYYY-MM-DD YYYY-MM-DD'
        """
        if start_date is None or end_date is None:
            return default

        # Ensure timezone-aware -> UTC
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        else:
            start_date = start_date.astimezone(timezone.utc)

        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
        else:
            end_date = end_date.astimezone(timezone.utc)

        return f"{start_date.date().isoformat()} {end_date.date().isoformat()}"

    def _fetch_interest_over_time(
        self,
        keyword: str,
        timeframe: str,
        geo: str = "",
        gprop: str = "",
        category: int = 0,
    ) -> pd.DataFrame:
        """
        Fetch a single keyword interest_over_time and normalize to Bronze contract.
        """
        self._pytrends.build_payload(
            kw_list=[keyword],
            cat=category,
            timeframe=timeframe,
            geo=geo,
            gprop=gprop,
        )

        df = self._pytrends.interest_over_time()
        if df is None or df.empty:
            return pd.DataFrame()

        # pytrends returns index as datetime, plus 'isPartial'
        df = df.reset_index()

        # normalize cols to snake_case
        # 'date' already good; keyword may have spaces
        value_col = keyword
        if value_col not in df.columns:
            # defensive: sometimes pytrends renames differently in edge cases
            value_candidates = [c for c in df.columns if c.lower() == keyword.lower()]
            if not value_candidates:
                return pd.DataFrame()
            value_col = value_candidates[0]

        out = pd.DataFrame(
            {
                "date": pd.to_datetime(df["date"], utc=True),
                "keyword": keyword,
                "value": pd.to_numeric(df[value_col], errors="coerce").fillna(0).astype(int),
                "is_partial": df.get("isPartial", False).astype(bool) if "isPartial" in df.columns else False,
            }
        )

        out["source"] = self.SOURCE_NAME
        return out

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        keywords: list[str] | None = None,
        geo: str = "",
        gprop: str = "",
    ) -> dict[str, pd.DataFrame]:
        """
        Collect Google Trends datasets.

        Returns:
            dict of dataset_name -> DataFrame
        """
        # You can later move these into config/collectors/google_trends.yaml if you want.
        keywords = keywords or getattr(Config, "GOOGLE_TRENDS_KEYWORDS", None) or [
            "inflation",
            "recession",
            "Federal Reserve",
            "European Central Bank",
            "US dollar",
            "euro",
            "EURUSD",
            "USDJPY",
            "GBPUSD",
        ]

        geo = geo if geo is not None else getattr(Config, "GOOGLE_TRENDS_GEO", "") # "" = worldwide
        gprop = gprop if gprop is not None else getattr(Config, "GOOGLE_TRENDS_GPROP", "") # "" = web search
        timeframe = self._to_timeframe(start_date, end_date)

        self.logger.info(
        "Collecting Google Trends: keywords=%d timeframe=%s geo=%s gprop=%s",
        len(keywords), timeframe, geo, gprop
        )


        frames: list[pd.DataFrame] = []
        for kw in keywords:
            for attempt in range(2):  # 1 retry
                try:
                    df = self._fetch_interest_over_time(
                        keyword=kw,
                        timeframe=timeframe,
                        geo=geo,
                        gprop=gprop,
                    )
                    if not df.empty:    
                       frames.append(df)
                    break
                except Exception as e:
                    msg = str(e).lower()
                    if "429" in msg or "too many" in msg:
                        wait = 180 if attempt == 0 else 0
                        if wait:
                            self.logger.warning("Rate limited (429). Sleeping %ss then retrying keyword=%s", wait, kw)
                            time.sleep(wait)
                            continue
                    self.logger.warning("Failed keyword=%s: %s", kw, e)
                    break  # don't retry on other errors
            time.sleep(self.REQUEST_DELAY + random.uniform(0, 2.0))

                
                
                    
               


        if not frames:
            return {"interest_over_time": pd.DataFrame()}

        combined = pd.concat(frames, ignore_index=True)

        # Bronze: preserve raw meaning, minimal normalization only.
        # Sort for determinism
        combined = combined.sort_values(["keyword", "date"]).reset_index(drop=True)

        return {"interest_over_time": combined}

    def health_check(self) -> bool:
        """
        Minimal reachability check: try fetching tiny window.
        """
        try:
            df = self._fetch_interest_over_time(
                keyword="inflation",
                timeframe="today 1-m",
                geo="",
                gprop="",
            )
            ok = isinstance(df, pd.DataFrame)
            self.logger.info("Health check ok=%s rows=%d", ok, 0 if df is None else len(df))
            return ok
        except Exception as e:
            self.logger.warning("Health check failed: %s", e)
            return False
