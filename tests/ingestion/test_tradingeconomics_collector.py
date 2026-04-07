"""Tests for Trading Economics calendar collector."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from src.ingestion.collectors.tradingeconomics_collector import (
    TradingEconomicsCalendarCollector,
)


SAMPLE_TE_PAYLOAD = [
    {
        "CalendarId": "1",
        "Date": "2024-02-08T13:30:00",
        "Country": "United States",
        "Category": "Non Farm Payrolls",
        "Event": "Non Farm Payrolls",
        "Reference": "Jan",
        "Source": "U.S. Bureau of Labor Statistics",
        "SourceURL": "https://www.bls.gov",
        "Actual": "150K",
        "Previous": "160K",
        "Forecast": "180K",
        "URL": "/united-states/non-farm-payrolls",
        "DateSpan": "0",
        "Importance": 3,
        "LastUpdate": "2024-02-08T13:31:00",
        "Revised": "",
        "Currency": "",
        "Unit": "K",
        "Ticker": "NFP",
        "Symbol": "NFP",
    },
    {
        "CalendarId": "2",
        "Date": "2024-02-08T14:00:00",
        "Country": "United Kingdom",
        "Category": "RICS House Price Balance",
        "Event": "RICS House Price Balance",
        "Reference": "Jan",
        "Source": "Royal Institution of Chartered Surveyors",
        "SourceURL": "https://www.rics.org/uk/",
        "Actual": "-18%",
        "Previous": "-29%",
        "Forecast": "-25%",
        "URL": "/united-kingdom/rics-house-price-balance",
        "DateSpan": "0",
        "Importance": 1,
        "LastUpdate": "2024-02-08T14:01:00",
        "Revised": "-30%",
        "Currency": "",
        "Unit": "%",
        "Ticker": "GBRRHPB",
        "Symbol": "GBRRHPB",
    },
    {
        "CalendarId": "3",
        "Date": "2024-02-08T15:00:00",
        "Country": "Euro Area",
        "Category": "Interest Rate Decision",
        "Event": "ECB Interest Rate Decision",
        "Reference": "",
        "Source": "European Central Bank",
        "SourceURL": "https://www.ecb.europa.eu",
        "Actual": "4.50%",
        "Previous": "4.50%",
        "Forecast": "4.50%",
        "URL": "/euro-area/interest-rate",
        "DateSpan": "0",
        "Importance": 3,
        "LastUpdate": "2024-02-08T15:01:00",
        "Revised": "",
        "Currency": "",
        "Unit": "%",
        "Ticker": "EURIR",
        "Symbol": "EURIR",
    },
    {
        "CalendarId": "4",
        "Date": "2024-02-08T16:00:00",
        "Country": "Japan",
        "Category": "Consumer Price Index",
        "Event": "CPI YoY",
        "Reference": "Jan",
        "Source": "Statistics Bureau of Japan",
        "SourceURL": "https://www.stat.go.jp",
        "Actual": "2.5%",
        "Previous": "2.6%",
        "Forecast": "2.6%",
        "URL": "/japan/consumer-price-index",
        "DateSpan": "0",
        "Importance": 2,
        "LastUpdate": "2024-02-08T16:01:00",
        "Revised": "",
        "Currency": "",
        "Unit": "%",
        "Ticker": "JPCPI",
        "Symbol": "JPCPI",
    },
]


class MockResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class MockSession:
    def __init__(self, payload):
        self.payload = payload
        self.headers = {}
        self.requested_urls: list[str] = []

    def get(self, url, timeout=None):
        self.requested_urls.append(url)
        return MockResponse(self.payload)


@pytest.fixture
def collector(tmp_path):
    return TradingEconomicsCalendarCollector(
        api_key="test_key",
        output_dir=tmp_path,
        session=MockSession(SAMPLE_TE_PAYLOAD),
        max_retries=0,
    )


def test_initialization(collector):
    assert collector.SOURCE_NAME == "tradingeconomics"
    assert collector.api_key == "test_key"
    assert collector.output_dir.exists()


def test_collect_normalizes_calendar_rows(collector):
    data = collector.collect(
        start_date=datetime(2024, 2, 8),
        end_date=datetime(2024, 2, 8),
    )

    assert "calendar" in data
    df = data["calendar"]
    assert len(df) == 4

    expected_columns = {
        "date",
        "time",
        "currency",
        "event",
        "impact",
        "actual",
        "forecast",
        "previous",
        "event_url",
        "source_url",
        "scraped_at",
        "source",
    }
    assert expected_columns.issubset(df.columns)
    assert set(df["source"].unique()) == {"tradingeconomics"}
    assert set(df["currency"].unique()) == {"USD", "GBP", "EUR", "JPY"}
    assert df.loc[df["currency"] == "EUR", "impact"].iloc[0] == "high"
    assert df.loc[df["currency"] == "GBP", "impact"].iloc[0] == "low"
    assert df.loc[df["currency"] == "JPY", "time"].iloc[0] == "16:00"
    assert df.loc[df["currency"] == "USD", "actual"].iloc[0] == "150K"


def test_collect_preserves_missing_values(collector):
    collector.session = MockSession(
        [
            {
                "Date": "2024-02-08T15:00:00",
                "Country": "Euro Area",
                "Event": "ECB Interest Rate Decision",
                "SourceURL": "https://www.ecb.europa.eu",
                "Actual": "",
                "Forecast": "4.50%",
                "Previous": "4.50%",
                "URL": "/euro-area/interest-rate",
                "Importance": 3,
                "Ticker": "EURIR",
            }
        ]
    )

    data = collector.collect(
        start_date=datetime(2024, 2, 8),
        end_date=datetime(2024, 2, 8),
    )

    df = data["calendar"]
    assert pd.isna(df.iloc[0]["actual"])
    assert df.iloc[0]["forecast"] == "4.50%"
    assert df.iloc[0]["currency"] == "EUR"


def test_health_check_success(monkeypatch, collector):
    monkeypatch.setattr(
        collector,
        "collect",
        lambda start_date=None, end_date=None: {"calendar": pd.DataFrame(SAMPLE_TE_PAYLOAD)},
    )

    assert collector.health_check() is True
