import io
import zipfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import requests

from src.ingestion.collectors.gdelt_events_collector import GDELTEventsCollector


@pytest.fixture
def collector(tmp_path: Path) -> GDELTEventsCollector:
    return GDELTEventsCollector(output_dir=tmp_path, sleep_between=0)


def _make_zip(rows: list[list[str]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        content = "\n".join("\t".join(row) for row in rows).encode("utf-8")
        z.writestr("events.csv", content)
    return buf.getvalue()


def _make_row(fill: dict | None = None) -> list[str]:
    # build 58 fields default empty
    row = [""] * 58
    fill = fill or {}
    for k, v in fill.items():
        row[k] = str(v)
    return row


def test_initialization(collector: GDELTEventsCollector) -> None:
    assert collector.SOURCE_NAME == "gdelt_events"
    assert collector.output_dir.exists()


def test_collect_skips_existing_day(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path, sleep_between=0)
    path = tmp_path / "2024" / "01" / "20240101.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([], columns=collector._normalized_columns()).to_parquet(path, engine="pyarrow")

    with patch("requests.get") as mock_get:
        result = collector.collect(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 1))
        assert result == {}
        mock_get.assert_not_called()


def test_collect_backfill_overwrites(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path, sleep_between=0)
    path = tmp_path / "2024" / "01" / "20240102.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"event_id": "old"}]).to_parquet(path, engine="pyarrow")

    rows = [
        _make_row(
            {
                0: "1",
                1: "20240102",
                7: "USA",
                17: "DEU",
                27: "061",
                30: "1",
                31: "0.1",
                32: "1",
                33: "1",
                34: "1",
                57: "http://example.com",
            }
        )
    ]
    content = _make_zip(rows)

    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = content
        result = collector.collect(
            start_date=datetime(2024, 1, 2), end_date=datetime(2024, 1, 2), backfill=True
        )

    assert result == {"20240102": 1}
    df = pd.read_parquet(path)
    assert len(df) == 1


def test_collect_writes_parquet(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path, sleep_between=0)
    rows = []
    # build 5 rows where two match zone filter
    rows.append(_make_row({0: "1", 1: "20240103", 7: "USA", 17: "", 57: "http://a"}))
    rows.append(_make_row({0: "2", 1: "20240103", 7: "", 17: "DEU", 57: "http://b"}))
    rows.append(_make_row({0: "3", 1: "20240103", 6: "", 16: "", 57: "http://c"}))
    rows.append(_make_row({0: "4", 1: "20240103", 6: "", 16: "", 57: "http://d"}))
    rows.append(_make_row({0: "5", 1: "20240103", 7: "USA", 17: "", 57: "http://e"}))
    content = _make_zip(rows)

    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = content
        result = collector.collect(start_date=datetime(2024, 1, 3), end_date=datetime(2024, 1, 3))

    assert result == {"20240103": 3}
    path = tmp_path / "2024" / "01" / "20240103.parquet"
    df = pd.read_parquet(path)
    assert len(df) == 3


def test_collect_404_writes_empty_sentinel(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path, sleep_between=0)
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 404
        result = collector.collect(start_date=datetime(2024, 1, 4), end_date=datetime(2024, 1, 4))

    assert result == {"20240104": 0}
    path = tmp_path / "2024" / "01" / "20240104.parquet"
    assert path.exists()
    df = pd.read_parquet(path)
    assert len(df) == 0


def test_collect_request_error_writes_empty_sentinel(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path, sleep_between=0)
    with patch("requests.get", side_effect=requests.RequestException("boom")):
        result = collector.collect(start_date=datetime(2024, 1, 5), end_date=datetime(2024, 1, 5))

    assert result == {"20240105": 0}
    path = tmp_path / "2024" / "01" / "20240105.parquet"
    assert path.exists()
    df = pd.read_parquet(path)
    assert len(df) == 0


def test_collect_zone_filter(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path, sleep_between=0)
    rows = []
    # two zone rows two non-zone
    rows.append(_make_row({0: "1", 1: "20240106", 7: "USA", 17: ""}))
    rows.append(_make_row({0: "2", 1: "20240106", 7: "", 17: "DEU"}))
    rows.append(_make_row({0: "3", 1: "20240106", 6: "", 16: ""}))
    rows.append(_make_row({0: "4", 1: "20240106", 6: "", 16: ""}))
    content = _make_zip(rows)

    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = content
        result = collector.collect(start_date=datetime(2024, 1, 6), end_date=datetime(2024, 1, 6))

    assert result == {"20240106": 2}
    df = pd.read_parquet(tmp_path / "2024" / "01" / "20240106.parquet")
    assert len(df) == 2


def test_collect_quadclass_not_filtered(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path, sleep_between=0)
    rows = []
    # create zone-matching rows with different QuadClass values
    rows.append(_make_row({0: "1", 1: "20240107", 7: "USA", 30: "1"}))
    rows.append(_make_row({0: "2", 1: "20240107", 7: "USA", 30: "2"}))
    rows.append(_make_row({0: "3", 1: "20240107", 7: "USA", 30: "3"}))
    rows.append(_make_row({0: "4", 1: "20240107", 7: "USA", 30: "4"}))
    content = _make_zip(rows)

    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = content
        result = collector.collect(start_date=datetime(2024, 1, 7), end_date=datetime(2024, 1, 7))

    assert result == {"20240107": 4}
    df = pd.read_parquet(tmp_path / "2024" / "01" / "20240107.parquet")
    assert len(df) == 4


def test_health_check_success(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)
    with patch("requests.head") as mock_head:
        mock_head.return_value.status_code = 200
        assert collector.health_check() is True


def test_health_check_server_error(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)
    with patch("requests.head") as mock_head:
        mock_head.return_value.status_code = 503
        assert collector.health_check() is False


def test_health_check_exception(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)
    with patch("requests.head", side_effect=requests.RequestException("boom")):
        assert collector.health_check() is False
