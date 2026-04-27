import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.agents.sentiment.gdelt_node import GDELTSignalNode


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    """Write rows as JSONL to path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


@pytest.fixture
def node(tmp_path: Path) -> GDELTSignalNode:
    """Fixture providing a GDELTSignalNode with temp bronze dir."""
    bronze_dir = tmp_path / "bronze"
    return GDELTSignalNode(bronze_dir=bronze_dir)


def test_basic_compute(node: GDELTSignalNode) -> None:
    """Test basic compute with 90 days of daily records.

    3 articles/day with realistic tones between -3.0 and +3.0.
    Tones vary by day to ensure rolling zscore is non-constant.
    Assert output has correct columns, length, and tone_zscore populated
    after warmup window.
    """
    # Generate 90 days of data with varying tones
    start_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    records = []

    for day_offset in range(90):
        current_date = start_date + timedelta(days=day_offset)
        # Vary tone_mean across days so rolling_std is non-zero
        base_tone = -2.0 + (day_offset % 4) * 1.5  # cycles through -2.0, -0.5, 1.0, 2.5
        for article_idx in range(3):
            tone = base_tone + (article_idx * 0.3)
            records.append(
                {
                    "timestamp_published": current_date.isoformat(),
                    "v2tone": f"{tone},0,0",
                }
            )

    # Write to JSONL (single file for Jan-Mar 2025)
    jsonl_path = node.bronze_dir / "gdelt_202501_raw.jsonl"
    _write_jsonl(jsonl_path, records)

    # Compute for full 90-day range
    end_date = start_date + timedelta(days=89)
    result = node.compute(start_date, end_date)

    # Assertions
    assert list(result.columns) == [
        "date",
        "tone_mean",
        "article_count",
        "tone_zscore",
        "attention_zscore",
    ]
    assert len(result) == 90
    assert result["date"].dtype == "datetime64[ns]"
    assert result["article_count"].dtype in ["int64", "float64"]

    # After MIN_PERIODS (10 days) warmup, some tone_zscore should be non-NaN
    warmup_rows = result.iloc[:10]
    post_warmup_rows = result.iloc[10:]
    assert warmup_rows["tone_zscore"].isna().any()
    # At least some post-warmup rows should have valid zscore (not all NaN)
    assert not post_warmup_rows["tone_zscore"].isna().all()


def test_skips_zero_tone(node: GDELTSignalNode) -> None:
    """Test that records with tone==0.0 are excluded from tone_mean
    but still counted in article_count."""
    start_date = datetime(2025, 2, 1, tzinfo=timezone.utc)
    records = [
        {
            "timestamp_published": (start_date + timedelta(days=d)).isoformat(),
            "v2tone": f"{0.0 if d == 0 else 1.0},0,0",
        }
        for d in range(15)
    ]

    jsonl_path = node.bronze_dir / "gdelt_202502_raw.jsonl"
    _write_jsonl(jsonl_path, records)

    end_date = start_date + timedelta(days=14)
    result = node.compute(start_date, end_date)

    # Day 0 should have article_count=1, tone_mean=NaN
    day0 = result[result["date"] == pd.Timestamp(start_date.date())]
    assert float(day0["article_count"].iloc[0]) == 1
    assert pd.isna(day0["tone_mean"].iloc[0])

    # Day 1 should have article_count=1, tone_mean=1.0
    day1 = result[result["date"] == pd.Timestamp((start_date + timedelta(days=1)).date())]
    assert float(day1["article_count"].iloc[0]) == 1
    assert float(day1["tone_mean"].iloc[0]) == 1.0


def test_missing_tone_skipped(node: GDELTSignalNode) -> None:
    """Test that records with tone=None are skipped entirely."""
    start_date = datetime(2025, 3, 1, tzinfo=timezone.utc)
    records = [
        {
            "timestamp_published": (start_date + timedelta(days=d)).isoformat(),
            "v2tone": "1.0,0,0" if d % 2 == 0 else None,
        }
        for d in range(20)
    ]

    jsonl_path = node.bronze_dir / "gdelt_202503_raw.jsonl"
    _write_jsonl(jsonl_path, records)

    end_date = start_date + timedelta(days=19)
    result = node.compute(start_date, end_date)

    # Days with no tone should have article_count=0
    for day_offset in range(20):
        current_date = start_date + timedelta(days=day_offset)
        day_data = result[result["date"] == pd.Timestamp(current_date.date())]
        if day_offset % 2 == 1:
            assert float(day_data["article_count"].iloc[0]) == 0


def test_low_coverage_mask(node: GDELTSignalNode) -> None:
    """Test that days with < MIN_DAILY_ARTICLES articles have tone_zscore=NaN."""
    start_date = datetime(2025, 4, 1, tzinfo=timezone.utc)
    records = []

    for d in range(30):
        current_date = start_date + timedelta(days=d)
        # Vary article count: 1 (low), 3 (min), 5 (ok)
        count = 1 if d % 3 == 0 else (3 if d % 3 == 1 else 5)
        for _ in range(count):
            records.append(
                {
                    "timestamp_published": current_date.isoformat(),
                    "v2tone": "1.5,0,0",
                }
            )

    jsonl_path = node.bronze_dir / "gdelt_202504_raw.jsonl"
    _write_jsonl(jsonl_path, records)

    end_date = start_date + timedelta(days=29)
    result = node.compute(start_date, end_date)

    # Days with 1 article (d % 3 == 0) should have tone_zscore=NaN
    for d in range(30):
        if d % 3 == 0:
            day_date = start_date + timedelta(days=d)
            day_data = result[result["date"] == pd.Timestamp(day_date.date())]
            # After warmup, should be masked
            if d >= node.MIN_PERIODS:
                assert pd.isna(day_data["tone_zscore"].iloc[0])


def test_min_periods_warmup(node: GDELTSignalNode) -> None:
    """Test with only 5 days of data. tone_zscore must be NaN
    for all rows (below MIN_PERIODS=10)."""
    start_date = datetime(2025, 5, 1, tzinfo=timezone.utc)
    records = [
        {
            "timestamp_published": (start_date + timedelta(days=d)).isoformat(),
            "v2tone": "1.0,0,0",
        }
        for d in range(3)  # 3 articles per day × 5 days
        for _ in range(3)
    ]

    jsonl_path = node.bronze_dir / "gdelt_202505_raw.jsonl"
    _write_jsonl(jsonl_path, records)

    end_date = start_date + timedelta(days=4)
    result = node.compute(start_date, end_date)

    # All tone_zscore should be NaN (below MIN_PERIODS)
    assert result["tone_zscore"].isna().all()


def test_date_filtering(node: GDELTSignalNode) -> None:
    """Test with records spanning multiple months, but compute() with 1-month
    window. Only rows within that window are returned."""
    # Create January records
    jan_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    jan_records = [
        {
            "timestamp_published": (jan_start + timedelta(days=d)).isoformat(),
            "v2tone": "1.0,0,0",
        }
        for d in range(31)
    ]

    # Create February records
    feb_start = datetime(2025, 2, 1, tzinfo=timezone.utc)
    feb_records = [
        {
            "timestamp_published": (feb_start + timedelta(days=d)).isoformat(),
            "v2tone": "1.0,0,0",
        }
        for d in range(28)
    ]

    # Create March records
    mar_start = datetime(2025, 3, 1, tzinfo=timezone.utc)
    mar_records = [
        {
            "timestamp_published": (mar_start + timedelta(days=d)).isoformat(),
            "v2tone": "1.0,0,0",
        }
        for d in range(31)
    ]

    # Write to proper month files
    _write_jsonl(node.bronze_dir / "gdelt_202501_raw.jsonl", jan_records)
    _write_jsonl(node.bronze_dir / "gdelt_202502_raw.jsonl", feb_records)
    _write_jsonl(node.bronze_dir / "gdelt_202503_raw.jsonl", mar_records)

    # Compute for Feb only
    result = node.compute(feb_start, feb_start + timedelta(days=27))

    # Result should only contain Feb dates
    assert result["date"].min() >= pd.Timestamp(feb_start.date())
    assert result["date"].max() <= pd.Timestamp((feb_start + timedelta(days=27)).date())
    assert len(result) == 28  # Feb has 28 days (non-leap 2025)


def test_no_files_raises(node: GDELTSignalNode) -> None:
    """Test ValueError is raised when bronze_dir has no matching JSONL files
    for requested date range."""
    start_date = datetime(2025, 6, 1, tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=29)

    with pytest.raises(ValueError, match="No JSONL files found"):
        node.compute(start_date, end_date)


def test_calendar_gap_filled(node: GDELTSignalNode) -> None:
    """Test that if Bronze has no records for 2 days in the middle of the range,
    those days appear in output with NaN tone_mean and NaN tone_zscore."""
    start_date = datetime(2025, 7, 1, tzinfo=timezone.utc)
    records = []

    # Days 0-4: normal data
    for d in range(5):
        current_date = start_date + timedelta(days=d)
        records.append(
            {
                "timestamp_published": current_date.isoformat(),
                "v2tone": "1.0,0,0",
            }
        )

    # Days 5-6: skip (gap)

    # Days 7-14: normal data
    for d in range(7, 15):
        current_date = start_date + timedelta(days=d)
        records.append(
            {
                "timestamp_published": current_date.isoformat(),
                "v2tone": "1.0,0,0",
            }
        )

    jsonl_path = node.bronze_dir / "gdelt_202507_raw.jsonl"
    _write_jsonl(jsonl_path, records)

    end_date = start_date + timedelta(days=14)
    result = node.compute(start_date, end_date)

    # Result should have 15 rows (full calendar range)
    assert len(result) == 15

    # Days 5-6 should have NaN tone_mean
    gap_day = result[result["date"] == pd.Timestamp((start_date + timedelta(days=5)).date())]
    assert pd.isna(gap_day["tone_mean"].iloc[0])

    gap_day2 = result[result["date"] == pd.Timestamp((start_date + timedelta(days=6)).date())]
    assert pd.isna(gap_day2["tone_mean"].iloc[0])
