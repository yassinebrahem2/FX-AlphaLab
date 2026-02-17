from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

from src.ingestion.collectors.google_trends_collector import GoogleTrendsCollector


def test_timeframe_conversion_defaults_when_missing_dates():
    tf = GoogleTrendsCollector._to_timeframe(None, None)
    assert tf == "today 5-y"


def test_timeframe_conversion_with_dates():
    start = datetime(2026, 2, 1, tzinfo=timezone.utc)
    end = datetime(2026, 2, 10, tzinfo=timezone.utc)
    tf = GoogleTrendsCollector._to_timeframe(start, end)
    assert tf == "2026-02-01 2026-02-10"


def test_collect_returns_dataframe_schema(tmp_path):
    collector = GoogleTrendsCollector(output_dir=tmp_path)

    fake_df = pd.DataFrame(
        {
            "date": pd.date_range("2026-02-01", periods=3, tz="UTC"),
            "inflation": [10, 20, 30],
            "isPartial": [False, False, False],
        }
    ).set_index("date")  # pytrends returns date index

    with patch.object(collector._pytrends, "build_payload") as bp, patch.object(
        collector._pytrends, "interest_over_time", return_value=fake_df
    ), patch("time.sleep"):
        # Reduce keywords to 1 for deterministic test
        with patch("src.shared.config.Config.GOOGLE_TRENDS_KEYWORDS", ["inflation"], create=True):
            out = collector.collect()

    assert "interest_over_time" in out
    df = out["interest_over_time"]
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3

    required_cols = {"date", "keyword", "value", "is_partial", "source"}
    assert required_cols.issubset(set(df.columns))
    assert df["source"].unique().tolist() == ["google_trends"]
    assert df["keyword"].unique().tolist() == ["inflation"]
    assert df["value"].tolist() == [10, 20, 30]
    assert bp.call_count >= 1


def test_export_csv_works(tmp_path):
    collector = GoogleTrendsCollector(output_dir=tmp_path)
    df = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-02-01", tz="UTC")],
            "keyword": ["inflation"],
            "value": [42],
            "is_partial": [False],
            "source": ["google_trends"],
        }
    )
    path = collector.export_csv(df, "interest_over_time")
    assert path.exists()
