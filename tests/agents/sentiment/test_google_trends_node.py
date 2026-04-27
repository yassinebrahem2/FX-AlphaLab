from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.agents.sentiment.google_trends_node import GoogleTrendsSignalNode


def _write_silver(silver_dir: Path, n_weeks: int, start: str = "2021-01-03") -> None:
    """Write a deterministic Google Trends Silver parquet for node tests."""
    silver_dir.mkdir(parents=True, exist_ok=True)

    dates = pd.date_range(start=start, periods=n_weeks, freq="W-SUN")
    base = np.arange(n_weeks, dtype=float)
    data = {"date": dates}

    theme_columns = {
        "macro_indicators": ["macro_indicators__signal_a", "macro_indicators__signal_b"],
        "central_banks": ["central_banks__signal_a", "central_banks__signal_b"],
        "risk_sentiment": ["risk_sentiment__signal_a", "risk_sentiment__signal_b"],
        "fx_pairs": ["fx_pairs__signal_a", "fx_pairs__signal_b"],
    }

    for offset, columns in enumerate(theme_columns.values(), start=1):
        data[columns[0]] = base + float(offset)
        data[columns[1]] = base + float(offset + 10)

    df = pd.DataFrame(data)
    df.to_parquet(silver_dir / "google_trends_weekly.parquet", engine="pyarrow", index=False)


@pytest.fixture
def node(tmp_path: Path) -> GoogleTrendsSignalNode:
    """Fixture providing a GoogleTrendsSignalNode with temp silver dir."""
    silver_dir = tmp_path / "silver"
    return GoogleTrendsSignalNode(silver_dir=silver_dir)


def test_compute_returns_one_row_per_calendar_day(node: GoogleTrendsSignalNode) -> None:
    """Test compute returns one row per day for the requested range."""
    _write_silver(node.silver_dir, 35)

    start_date = datetime(2021, 2, 1)
    end_date = datetime(2021, 3, 2)
    result = node.compute(start_date, end_date)

    assert len(result) == (end_date - start_date).days + 1


def test_forward_fill_repeats_signal_within_week(node: GoogleTrendsSignalNode) -> None:
    """Test weekly values are forward-filled across the full calendar week."""
    _write_silver(node.silver_dir, 35)

    start_date = datetime(2021, 5, 2)
    end_date = datetime(2021, 5, 8)
    result = node.compute(start_date, end_date)

    assert len(result) == 7
    assert result["macro_attention_zscore"].notna().all()
    assert result["macro_attention_zscore"].nunique(dropna=False) == 1


def test_rolling_zscore_is_nan_before_min_periods(node: GoogleTrendsSignalNode) -> None:
    """Test that insufficient weekly history leaves zscores empty."""
    _write_silver(node.silver_dir, 8)

    start_date = datetime(2021, 1, 3)
    end_date = datetime(2021, 2, 21)
    result = node.compute(start_date, end_date)

    assert result["macro_attention_zscore"].isna().all()


def test_compute_raises_value_error_when_silver_missing(node: GoogleTrendsSignalNode) -> None:
    """Test compute raises when the Silver parquet is missing."""
    with pytest.raises(ValueError, match="Google Trends Silver file not found"):
        node.compute(datetime(2021, 1, 3), datetime(2021, 1, 10))


def test_output_schema_has_exactly_nine_columns_in_order(node: GoogleTrendsSignalNode) -> None:
    """Test output schema matches the exact expected order."""
    _write_silver(node.silver_dir, 35)

    result = node.compute(datetime(2021, 1, 3), datetime(2021, 9, 5))

    assert list(result.columns) == [
        "date",
        "macro_attention_index",
        "central_bank_attention_index",
        "risk_sentiment_attention_index",
        "fx_pairs_attention_index",
        "macro_attention_zscore",
        "central_bank_attention_zscore",
        "risk_sentiment_attention_zscore",
        "fx_pairs_attention_zscore",
    ]


def test_output_date_column_is_datetime64_ns(node: GoogleTrendsSignalNode) -> None:
    """Test the date column is timezone-naive datetime64[ns]."""
    _write_silver(node.silver_dir, 35)

    result = node.compute(datetime(2021, 1, 3), datetime(2021, 9, 5))

    assert str(result["date"].dtype) == "datetime64[ns]"


def test_date_range_filter_excludes_out_of_range_rows(node: GoogleTrendsSignalNode) -> None:
    """Test that output only includes the requested date window."""
    _write_silver(node.silver_dir, 52)

    start_date = datetime(2021, 4, 1)
    end_date = datetime(2021, 4, 14)
    result = node.compute(start_date, end_date)

    assert result["date"].min() >= pd.Timestamp(start_date.date())
    assert result["date"].max() <= pd.Timestamp(end_date.date())
    assert len(result) == 14


def test_macro_index_equals_mean_of_macro_columns(node: GoogleTrendsSignalNode) -> None:
    """Test the macro attention index matches the raw macro column mean."""
    _write_silver(node.silver_dir, 35)

    silver = pd.read_parquet(node.silver_dir / "google_trends_weekly.parquet")
    start_date = datetime(2021, 1, 3)
    end_date = datetime(2021, 9, 5)
    result = node.compute(start_date, end_date)

    sunday_row = result[result["date"] == pd.Timestamp(start_date.date())].iloc[0]
    silver_row = silver[silver["date"] == pd.Timestamp(start_date.date())].iloc[0]
    macro_columns = [column for column in silver.columns if column.startswith("macro_indicators__")]
    expected = silver_row[macro_columns].mean()

    assert sunday_row["macro_attention_index"] == pytest.approx(expected)


def test_zscore_is_nan_when_rolling_std_is_zero(node: GoogleTrendsSignalNode) -> None:
    """Test zero-variance macro history yields NaN zscores rather than inf."""
    _write_silver(node.silver_dir, 35)

    silver_path = node.silver_dir / "google_trends_weekly.parquet"
    silver = pd.read_parquet(silver_path)
    macro_columns = [column for column in silver.columns if column.startswith("macro_indicators__")]
    silver[macro_columns] = 7.0
    silver.to_parquet(silver_path, engine="pyarrow", index=False)

    result = node.compute(datetime(2021, 1, 3), datetime(2021, 9, 5))

    assert result["macro_attention_zscore"].isna().all()


def test_days_after_last_silver_week_are_forward_filled(node: GoogleTrendsSignalNode) -> None:
    """Test days after the final Silver week inherit the last weekly signal."""
    _write_silver(node.silver_dir, 35)

    last_silver_week = pd.Timestamp("2021-01-03") + pd.Timedelta(weeks=34)
    end_date = (last_silver_week + timedelta(days=10)).to_pydatetime()
    result = node.compute(datetime(2021, 1, 3), end_date)

    future_rows = result[result["date"] > last_silver_week]
    assert len(future_rows) == 10
    assert future_rows["macro_attention_zscore"].notna().all()
