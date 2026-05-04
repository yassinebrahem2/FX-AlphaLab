from __future__ import annotations

import lzma
import struct
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from src.ingestion.collectors import DukascopyCollector


@pytest.fixture
def collector(tmp_path: Path) -> DukascopyCollector:
    return DukascopyCollector(
        output_dir=tmp_path / "raw" / "dukascopy",
        instruments=["EURUSD"],
        log_file=tmp_path / "logs" / "collectors" / "dukascopy_collector.log",
    )


def _make_bi5(records: list[tuple[int, int, int, int, int, float]]) -> bytes:
    raw = b"".join(struct.pack(">IIIIIf", *record) for record in records)
    return lzma.compress(raw)


def test_package_exports_dukascopy_collector() -> None:
    assert DukascopyCollector.__name__ == "DukascopyCollector"


def test_decode_bi5_decodes_expected_bar(collector: DukascopyCollector) -> None:
    raw = _make_bi5([(0, 123456, 123500, 123400, 123450, 7.5)])

    df = collector._decode_bi5(raw, "EURUSD", date(2024, 1, 2))

    assert list(df.columns) == ["timestamp_utc", "open", "high", "low", "close", "volume"]
    assert len(df) == 1
    assert pd.Timestamp(df.loc[0, "timestamp_utc"]) == pd.Timestamp("2024-01-02T00:00:00Z")
    assert df.loc[0, "open"] == 1.23456
    assert df.loc[0, "high"] == 1.235
    assert df.loc[0, "low"] == 1.234
    assert df.loc[0, "close"] == 1.2345
    assert df.loc[0, "volume"] == 7.5


def test_collect_writes_empty_sentinel_for_404(
    collector: DukascopyCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    response = Mock()
    response.status_code = 404
    response.content = b""
    response.raise_for_status = Mock()

    monkeypatch.setattr(collector._session, "get", Mock(return_value=response))
    monkeypatch.setattr("src.ingestion.collectors.dukascopy_collector.time.sleep", Mock())

    result = collector.collect(start_date=datetime(2024, 1, 2), end_date=datetime(2024, 1, 2))

    path = collector._bronze_path("EURUSD", date(2024, 1, 2))
    assert result == {"EURUSD": 0}
    assert path.exists()
    assert pd.read_parquet(path).empty


def test_collect_skips_saturday_without_request(
    collector: DukascopyCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    get_mock = Mock()
    monkeypatch.setattr(collector._session, "get", get_mock)
    monkeypatch.setattr("src.ingestion.collectors.dukascopy_collector.time.sleep", Mock())

    result = collector.collect(start_date=datetime(2024, 1, 6), end_date=datetime(2024, 1, 6))

    path = collector._bronze_path("EURUSD", date(2024, 1, 6))
    assert result == {"EURUSD": 0}
    assert get_mock.call_count == 0
    assert not path.exists()


def test_fetch_day_retries_then_succeeds(
    collector: DukascopyCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    response = Mock()
    response.status_code = 200
    response.content = _make_bi5([(0, 123456, 123500, 123400, 123450, 7.5)])
    response.raise_for_status = Mock()

    get_mock = Mock(
        side_effect=[
            requests.RequestException("boom-1"),
            requests.RequestException("boom-2"),
            response,
        ]
    )
    monkeypatch.setattr(collector._session, "get", get_mock)
    monkeypatch.setattr("src.ingestion.collectors.dukascopy_collector.time.sleep", Mock())

    df = collector._fetch_day("EURUSD", date(2024, 1, 2))

    assert get_mock.call_count == 3
    assert len(df) == 1
    assert df.loc[0, "open"] == 1.23456
    assert df.loc[0, "volume"] == 7.5
