from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.preprocessors.gdelt_events_preprocessor import GDELTEventsPreprocessor


def _write_daily_parquet(input_dir: Path, records_by_day: dict[str, list[dict]]) -> None:
    for day, records in records_by_day.items():
        year = day[:4]
        month = day[4:6]
        path = input_dir / year / month / f"{day}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(records).to_parquet(path, engine="pyarrow", index=False)


def _make_preprocessor(tmp_path: Path) -> GDELTEventsPreprocessor:
    return GDELTEventsPreprocessor(
        input_dir=tmp_path / "raw" / "gdelt_events",
        output_dir=tmp_path / "processed" / "geopolitical" / "events",
        log_file=tmp_path / "logs" / "gdelt_events_preprocess.log",
    )


def _bronze_record(**overrides: object) -> dict:
    record = {
        "source": "gdelt_events",
        "timestamp_collected": "2024-01-15T12:00:00Z",
        "event_id": "1001",
        "event_date": "20240115",
        "actor1_name": "United States",
        "actor1_country_code": "USA",
        "actor1_type1_code": "GOV",
        "actor2_name": "Germany",
        "actor2_country_code": "DEU",
        "actor2_type1_code": "GOV",
        "event_code": "061",
        "event_base_code": "06",
        "event_root_code": "06",
        "quad_class": 2,
        "goldstein_scale": 1.5,
        "num_mentions": 10,
        "num_sources": 4,
        "num_articles": 2,
        "avg_tone": -0.5,
        "actor1_geo_country_code": "US",
        "actor2_geo_country_code": "DE",
        "action_geo_country_code": "DE",
        "action_geo_full_name": "Berlin, Germany",
        "source_url": "https://www.reuters.com/world/europe/example",
    }
    record.update(overrides)
    return record


def test_run_writes_silver_parquet(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_daily_parquet(
        preprocessor.input_dir,
        {
            "20240115": [_bronze_record()],
            "20240116": [_bronze_record(event_id="1002", event_date="20240116")],
        },
    )

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))
    silver_path = (
        preprocessor.output_dir / "year=2024" / "month=01" / "gdelt_events_cleaned.parquet"
    )
    df = pd.read_parquet(silver_path)

    assert result == {"202401": 2}
    assert silver_path.exists()
    assert list(df.columns) == GDELTEventsPreprocessor.SILVER_COLUMNS
    assert str(df["event_date"].dtype) == "datetime64[ns, UTC]"
    assert str(df["event_id"].dtype) == "string"
    assert str(df["quad_class"].dtype) == "Int64"
    assert str(df["goldstein_scale"].dtype) == "float64"
    assert str(df["num_mentions"].dtype) == "Int64"
    assert str(df["avg_tone"].dtype) == "float64"


def test_run_skips_existing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    # create daily parquet bronze file
    silver_path = (
        preprocessor.output_dir / "year=2024" / "month=01" / "gdelt_events_cleaned.parquet"
    )
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"value": 1}, {"value": 2}]).to_parquet(
        silver_path, engine="pyarrow", index=False
    )

    # create a bronze file that SHOULD NOT be read
    _write_daily_parquet(preprocessor.input_dir, {"20240115": [_bronze_record()]})

    def fail_read(path, *args, **kwargs):
        if str(preprocessor.input_dir) in str(path):
            raise AssertionError("Bronze file should not be read when Silver exists")
        return pd.read_parquet(path, *args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", fail_read)

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))

    assert result == {"202401": 2}


def test_run_backfill_overwrites(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    silver_path = (
        preprocessor.output_dir / "year=2024" / "month=01" / "gdelt_events_cleaned.parquet"
    )
    _write_daily_parquet(
        preprocessor.input_dir, {"20240115": [_bronze_record(), _bronze_record(event_id="1002")]}
    )
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"value": 1}]).to_parquet(silver_path, engine="pyarrow", index=False)

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31), backfill=True)
    df = pd.read_parquet(silver_path)

    assert result == {"202401": 2}
    assert len(df) == 2


def test_run_skips_missing_bronze(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))

    silver_path = (
        preprocessor.output_dir / "year=2024" / "month=01" / "gdelt_events_cleaned.parquet"
    )
    assert result == {"202401": 0}
    assert not silver_path.exists()


def test_run_drops_null_event_date(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_daily_parquet(
        preprocessor.input_dir, {"20240115": [_bronze_record(), _bronze_record(event_date=None)]}
    )

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))
    df = pd.read_parquet(
        preprocessor.output_dir / "year=2024" / "month=01" / "gdelt_events_cleaned.parquet"
    )

    assert result == {"202401": 1}
    assert len(df) == 1


def test_run_drops_null_event_id(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_daily_parquet(
        preprocessor.input_dir, {"20240115": [_bronze_record(), _bronze_record(event_id=None)]}
    )

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))
    df = pd.read_parquet(
        preprocessor.output_dir / "year=2024" / "month=01" / "gdelt_events_cleaned.parquet"
    )

    assert result == {"202401": 1}
    assert len(df) == 1


def test_run_handles_malformed_json(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    # simulate two valid bronze parquet days and one unreadable file by writing normally
    _write_daily_parquet(
        preprocessor.input_dir,
        {
            "20240115": [_bronze_record(event_id="1001")],
            "20240116": [_bronze_record(event_id="1002")],
        },
    )

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))
    df = pd.read_parquet(
        preprocessor.output_dir / "year=2024" / "month=01" / "gdelt_events_cleaned.parquet"
    )

    assert result == {"202401": 2}
    assert len(df) == 2


def test_health_check_true(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_daily_parquet(preprocessor.input_dir, {"20240115": [_bronze_record()]})

    assert preprocessor.health_check() is True


def test_health_check_false(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    assert preprocessor.health_check() is False
