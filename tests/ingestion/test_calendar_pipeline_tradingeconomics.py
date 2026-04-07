"""Integration tests for Trading Economics calendar pipeline."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from scripts.collect_calendar_data import collect_calendar_source, run_preprocess
from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor


def _sample_raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2024-02-08",
                "time": "13:30",
                "currency": "USD",
                "event": "Non Farm Payrolls",
                "impact": "high",
                "actual": "150K",
                "forecast": "180K",
                "previous": "160K",
                "event_url": "https://tradingeconomics.com/united-states/non-farm-payrolls",
                "source_url": "https://api.tradingeconomics.com/calendar/country/All/2024-02-08/2024-02-08?c=test_key&f=json",
                "scraped_at": "2024-02-08T13:31:00Z",
                "source": "tradingeconomics",
            },
            {
                "date": "2024-02-08",
                "time": "14:00",
                "currency": "GBP",
                "event": "RICS House Price Balance",
                "impact": "low",
                "actual": "-18%",
                "forecast": "-25%",
                "previous": "-29%",
                "event_url": "https://tradingeconomics.com/united-kingdom/rics-house-price-balance",
                "source_url": "https://api.tradingeconomics.com/calendar/country/All/2024-02-08/2024-02-08?c=test_key&f=json",
                "scraped_at": "2024-02-08T14:01:00Z",
                "source": "tradingeconomics",
            },
            {
                "date": "2024-02-08",
                "time": "15:00",
                "currency": "EUR",
                "event": "ECB Interest Rate Decision",
                "impact": "high",
                "actual": "4.50%",
                "forecast": "4.50%",
                "previous": "4.50%",
                "event_url": "https://tradingeconomics.com/euro-area/interest-rate",
                "source_url": "https://api.tradingeconomics.com/calendar/country/All/2024-02-08/2024-02-08?c=test_key&f=json",
                "scraped_at": "2024-02-08T15:01:00Z",
                "source": "tradingeconomics",
            },
            {
                "date": "2024-02-08",
                "time": "16:00",
                "currency": "JPY",
                "event": "CPI YoY",
                "impact": "medium",
                "actual": None,
                "forecast": "2.6%",
                "previous": "2.5%",
                "event_url": "https://tradingeconomics.com/japan/consumer-price-index",
                "source_url": "https://api.tradingeconomics.com/calendar/country/All/2024-02-08/2024-02-08?c=test_key&f=json",
                "scraped_at": "2024-02-08T16:01:00Z",
                "source": "tradingeconomics",
            },
        ]
    )


def test_preprocess_tradingeconomics_raw_frame(tmp_path):
    raw_df = _sample_raw_frame()
    processor = CalendarPreprocessor(
        input_dir=tmp_path / "raw" / "calendar",
        output_dir=tmp_path / "processed" / "events",
    )

    result = processor.preprocess(df=raw_df)
    processed = result["events"]

    assert len(processed) == 4
    assert {"timestamp_utc", "event_id", "event_name", "impact", "actual", "forecast", "previous", "source"}.issubset(processed.columns)
    assert "country" not in processed.columns
    assert set(processed["currency"].unique()) == {"USD", "GBP", "EUR", "JPY"}
    assert int(processed["score_available"].fillna(False).sum()) == 3
    assert processed["final_score"].notna().sum() == 3
    assert processed["source"].eq("tradingeconomics").all()


def test_run_preprocess_writes_scored_output(tmp_path):
    raw_df = _sample_raw_frame()
    output_path = tmp_path / "calendar_tradingeconomics_scored.csv"

    processed = run_preprocess(raw_df, output_path=output_path)
    assert output_path.exists()
    assert len(processed) == 4
    assert int(processed["score_available"].fillna(False).sum()) == 3


def test_combined_source_supports_preprocessing(monkeypatch):
    from scripts import collect_calendar_data as script_module

    ff_df = _sample_raw_frame().iloc[:2].copy()
    ff_df["source"] = "forexfactory"
    te_df = _sample_raw_frame().iloc[2:].copy()
    te_df["source"] = "tradingeconomics"

    monkeypatch.setattr(script_module, "_collect_forexfactory", lambda start_date, end_date: ff_df)
    monkeypatch.setattr(script_module, "_collect_tradingeconomics", lambda start_date, end_date: te_df)

    combined = collect_calendar_source("combined", datetime(2024, 2, 8), datetime(2024, 2, 8))
    assert not combined.empty
    assert {"source_count", "source_agreement"}.issubset(combined.columns)
    assert set(combined["source"].unique()) == {"forexfactory", "tradingeconomics"}
