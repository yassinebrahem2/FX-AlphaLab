from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from src.ingestion.collectors.google_trends_collector import GoogleTrendsCollector


def _make_collector(tmp_path: Path) -> GoogleTrendsCollector:
    return GoogleTrendsCollector(output_dir=tmp_path / "raw" / "google_trends")


def _make_trends_frame(keyword_list: list[str]) -> pd.DataFrame:
    dates = pd.date_range("2024-01-07", periods=4, freq="W-SUN")
    data = {
        keyword: list(range(index * 10, index * 10 + 4))
        for index, keyword in enumerate(keyword_list)
    }
    return pd.DataFrame(data, index=dates)


def _trendreq_factory(container: dict[str, list[str]]) -> Mock:
    mock = Mock()

    def build_payload(*, kw_list: list[str], timeframe: str, **kwargs: object) -> None:
        container["calls"].append(list(kw_list))
        mock._current_kw_list = list(kw_list)

    def interest_over_time() -> pd.DataFrame:
        return _make_trends_frame(mock._current_kw_list)

    mock.build_payload.side_effect = build_payload
    mock.interest_over_time.side_effect = interest_over_time
    return mock


def test_collect_creates_all_csv_files_when_missing(tmp_path: Path) -> None:
    collector = _make_collector(tmp_path)
    container = {"calls": []}

    with (
        patch(
            "src.ingestion.collectors.google_trends_collector.TrendReq",
            return_value=_trendreq_factory(container),
        ),
        patch("src.ingestion.collectors.google_trends_collector.time.sleep"),
    ):
        result = collector.collect(datetime(2024, 1, 1), datetime(2024, 3, 31), force=False)

    files = sorted(collector.output_dir.glob("trends_*.csv"))
    assert len(files) == 8
    assert set(result) == {
        f"trends_{theme}_{index}"
        for theme, batches in collector.KEYWORD_GROUPS.items()
        for index in range(len(batches))
    }


def test_collect_skips_existing_file_when_not_forced(tmp_path: Path) -> None:
    collector = _make_collector(tmp_path)
    existing_path = collector.output_dir / "trends_fx_pairs_0.csv"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "date": pd.date_range("2024-01-07", periods=4, freq="W-SUN"),
            "EURUSD": [1, 2, 3, 4],
            "GBPUSD": [5, 6, 7, 8],
        }
    ).to_csv(existing_path, index=False)

    container = {"calls": []}
    with (
        patch(
            "src.ingestion.collectors.google_trends_collector.TrendReq",
            return_value=_trendreq_factory(container),
        ),
        patch("src.ingestion.collectors.google_trends_collector.time.sleep"),
    ):
        result = collector.collect(datetime(2024, 1, 1), datetime(2024, 3, 31), force=False)

    assert existing_path.exists()
    assert [
        call for call in container["calls"] if call == collector.KEYWORD_GROUPS["fx_pairs"][0]
    ] == []
    assert result["trends_fx_pairs_0"] == 4


def test_collect_force_calls_api_and_overwrites_existing_file(tmp_path: Path) -> None:
    collector = _make_collector(tmp_path)
    existing_path = collector.output_dir / "trends_fx_pairs_0.csv"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"date": ["2024-01-07"], "old": [999]}).to_csv(existing_path, index=False)

    container = {"calls": []}
    with (
        patch(
            "src.ingestion.collectors.google_trends_collector.TrendReq",
            return_value=_trendreq_factory(container),
        ),
        patch("src.ingestion.collectors.google_trends_collector.time.sleep"),
    ):
        result = collector.collect(datetime(2024, 1, 1), datetime(2024, 3, 31), force=True)

    frame = pd.read_csv(existing_path)
    assert container["calls"]
    assert list(frame.columns) == [
        "date",
        "EURUSD",
        "GBPUSD",
        "USDJPY",
        "euro dollar",
        "pound dollar",
    ]
    assert result["trends_fx_pairs_0"] == 4


def test_collect_returns_correct_keys_and_counts(tmp_path: Path) -> None:
    collector = _make_collector(tmp_path)
    container = {"calls": []}

    with (
        patch(
            "src.ingestion.collectors.google_trends_collector.TrendReq",
            return_value=_trendreq_factory(container),
        ),
        patch("src.ingestion.collectors.google_trends_collector.time.sleep"),
    ):
        result = collector.collect(datetime(2024, 1, 1), datetime(2024, 3, 31), force=True)

    assert all(isinstance(value, int) for value in result.values())
    assert set(result) == {
        f"trends_{theme}_{index}"
        for theme, batches in collector.KEYWORD_GROUPS.items()
        for index in range(len(batches))
    }


def test_written_csv_places_date_first_and_drops_ispartial(tmp_path: Path) -> None:
    collector = _make_collector(tmp_path)
    container = {"calls": []}

    mock = _trendreq_factory(container)

    def interest_over_time_with_partial() -> pd.DataFrame:
        frame = _make_trends_frame(collector.KEYWORD_GROUPS["macro_indicators"][0])
        frame["isPartial"] = False
        return frame

    mock.interest_over_time.side_effect = interest_over_time_with_partial

    with (
        patch("src.ingestion.collectors.google_trends_collector.TrendReq", return_value=mock),
        patch("src.ingestion.collectors.google_trends_collector.time.sleep"),
    ):
        collector.collect(datetime(2024, 1, 1), datetime(2024, 3, 31), force=True)

    frame = pd.read_csv(collector.output_dir / "trends_macro_indicators_0.csv")
    assert list(frame.columns)[0] == "date"
    assert "isPartial" not in frame.columns


def test_health_check_returns_true_when_api_responds(tmp_path: Path) -> None:
    collector = _make_collector(tmp_path)
    container = {"calls": []}

    with patch(
        "src.ingestion.collectors.google_trends_collector.TrendReq",
        return_value=_trendreq_factory(container),
    ):
        assert collector.health_check() is True


def test_health_check_returns_false_on_exception(tmp_path: Path) -> None:
    collector = _make_collector(tmp_path)

    failing_mock = Mock()
    failing_mock.build_payload.side_effect = RuntimeError("boom")

    with patch(
        "src.ingestion.collectors.google_trends_collector.TrendReq",
        return_value=failing_mock,
    ):
        assert collector.health_check() is False
