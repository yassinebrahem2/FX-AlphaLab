import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.preprocessors.gdelt_gkg_preprocessor import GDELTGKGPreprocessor


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        for record in records:
            file_handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _make_preprocessor(tmp_path: Path) -> GDELTGKGPreprocessor:
    return GDELTGKGPreprocessor(
        input_dir=tmp_path / "raw" / "news" / "gdelt",
        output_dir=tmp_path / "processed" / "sentiment" / "source=gdelt",
        log_file=tmp_path / "logs" / "gdelt_preprocess.log",
    )


def _bronze_record(**overrides: object) -> dict:
    record = {
        "url": "https://example.com/article",
        "timestamp_published": "2024-01-15T12:00:00Z",
        "source_domain": "reuters.com",
        "source": "gdelt_gkg",
        "v2tone": "1.0,2.0,3.0,4.0,5.0,6.0,7",
        "themes": "ECON_CURRENCY;USD",
        "locations": "US;New York",
        "organizations": "Federal Reserve;ECB",
    }
    record.update(overrides)
    return record


def test_parse_v2tone_full(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    result = preprocessor._parse_v2tone("1.1,2.2,3.3,4.4,5.5,6.6,7")

    assert result == {
        "tone": 1.1,
        "positive_score": 2.2,
        "negative_score": 3.3,
        "polarity": 4.4,
        "activity_ref_density": 5.5,
        "self_group_ref_density": 6.6,
        "word_count": 7,
    }


def test_parse_v2tone_partial(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    result = preprocessor._parse_v2tone("1,2,3")

    assert result["tone"] == 1.0
    assert result["positive_score"] == 2.0
    assert result["negative_score"] == 3.0
    assert result["polarity"] is None
    assert result["activity_ref_density"] is None
    assert result["self_group_ref_density"] is None
    assert result["word_count"] is None


def test_parse_v2tone_empty(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    result = preprocessor._parse_v2tone("")

    assert all(value is None for value in result.values())


def test_parse_v2tone_non_numeric(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    result = preprocessor._parse_v2tone("1,xyz,3,4,5,6,7")

    assert result["tone"] == 1.0
    assert result["positive_score"] is None
    assert result["negative_score"] == 3.0
    assert result["polarity"] == 4.0
    assert result["activity_ref_density"] == 5.0
    assert result["self_group_ref_density"] == 6.0
    assert result["word_count"] == 7


def test_split_semicolon_normal(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    result = preprocessor._split_semicolon("ECON_CURRENCY;USD; ECON_CENTRAL_BANK ")

    assert result == ["ECON_CURRENCY", "USD", "ECON_CENTRAL_BANK"]


def test_split_semicolon_none_input(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    assert preprocessor._split_semicolon(None) == []


def test_split_semicolon_empty_string(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    assert preprocessor._split_semicolon("") == []


def test_parse_record_skips_missing_url(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    assert preprocessor._parse_record({"timestamp_published": "2024-01-01T00:00:00Z"}) is None


def test_parse_record_skips_empty_url(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    assert (
        preprocessor._parse_record({"url": "", "timestamp_published": "2024-01-01T00:00:00Z"})
        is None
    )


def test_parse_record_skips_missing_timestamp(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    assert preprocessor._parse_record({"url": "https://example.com/article"}) is None


def test_parse_record_keeps_null_tone(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    result = preprocessor._parse_record(
        {
            "url": "https://example.com/article",
            "timestamp_published": "2024-01-01T00:00:00Z",
            "v2tone": "",
        }
    )

    assert result is not None
    assert result["tone"] is None


def test_run_creates_correct_hive_path(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    bronze_path = preprocessor.input_dir / "gdelt_202401_raw.jsonl"
    _write_jsonl(bronze_path, [_bronze_record()])

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))

    silver_path = preprocessor.output_dir / "year=2024" / "month=01" / "sentiment_cleaned.parquet"
    assert result == {"202401": 1}
    assert silver_path.exists()


def test_run_empty_month_no_file_written(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))

    silver_path = preprocessor.output_dir / "year=2024" / "month=01" / "sentiment_cleaned.parquet"
    assert result == {"202401": 0}
    assert not silver_path.exists()


def test_run_skip_existing_no_backfill(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    bronze_path = preprocessor.input_dir / "gdelt_202401_raw.jsonl"
    silver_path = preprocessor.output_dir / "year=2024" / "month=01" / "sentiment_cleaned.parquet"

    _write_jsonl(bronze_path, [_bronze_record()])
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"value": 1}, {"value": 2}, {"value": 3}]).to_parquet(
        silver_path,
        engine="pyarrow",
        index=False,
    )

    original_open = Path.open

    def guarded_open(self: Path, *args: object, **kwargs: object):
        mode = args[0] if args else kwargs.get("mode", "r")
        if self == bronze_path and "r" in str(mode):
            raise AssertionError("Bronze file should not be read when Silver exists")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))

    assert result == {"202401": 3}


def test_run_force_overwrite(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    bronze_path = preprocessor.input_dir / "gdelt_202401_raw.jsonl"
    silver_path = preprocessor.output_dir / "year=2024" / "month=01" / "sentiment_cleaned.parquet"

    _write_jsonl(bronze_path, [_bronze_record(), _bronze_record(url="https://example.com/b")])
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"value": 1}]).to_parquet(silver_path, engine="pyarrow", index=False)

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31), backfill=True)
    df = pd.read_parquet(silver_path)

    assert result == {"202401": 2}
    assert len(df) == 2


def test_run_output_schema(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    bronze_path = preprocessor.input_dir / "gdelt_202401_raw.jsonl"
    records = [
        _bronze_record(
            url="https://example.com/a",
            timestamp_published="2024-01-10T08:30:00Z",
            v2tone="1.5,2.5,3.5,4.5,5.5,6.5,100",
            themes="ECON_CURRENCY;USD",
            locations="US;New York",
            organizations="Federal Reserve;ECB",
        ),
        _bronze_record(
            url="https://example.com/b",
            timestamp_published="2024-01-11T09:45:00Z",
            v2tone="0.1,0.2,0.3,0.4,0.5,0.6,7",
            themes="ECON_GDP",
            locations="GB;London",
            organizations="Bank of England",
        ),
    ]
    _write_jsonl(bronze_path, records)

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))
    silver_path = preprocessor.output_dir / "year=2024" / "month=01" / "sentiment_cleaned.parquet"
    df = pd.read_parquet(silver_path)

    assert result == {"202401": 2}
    assert list(df.columns) == GDELTGKGPreprocessor.SILVER_COLUMNS
    assert str(df["timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    assert str(df["tone"].dtype) == "float64"
    assert str(df["word_count"].dtype) == "Int64"
    assert list(df.loc[0, "themes"]) == ["ECON_CURRENCY", "USD"]
    assert list(df.loc[0, "locations"]) == ["US", "New York"]
    assert list(df.loc[0, "organizations"]) == ["Federal Reserve", "ECB"]


def test_run_null_tone_records_in_output(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    bronze_path = preprocessor.input_dir / "gdelt_202401_raw.jsonl"
    _write_jsonl(
        bronze_path,
        [
            _bronze_record(url="https://example.com/a", v2tone=""),
            _bronze_record(url="https://example.com/b", v2tone="1,2,3,4,5,6,7"),
        ],
    )

    result = preprocessor.run(datetime(2024, 1, 1), datetime(2024, 1, 31))
    df = pd.read_parquet(
        preprocessor.output_dir / "year=2024" / "month=01" / "sentiment_cleaned.parquet"
    )

    assert result == {"202401": 2}
    assert len(df) == 2
    assert pd.isna(df.loc[df["url"] == "https://example.com/a", "tone"].iloc[0])


def test_health_check_no_files(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    assert preprocessor.health_check() is False


def test_health_check_with_files(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    bronze_path = preprocessor.input_dir / "gdelt_202401_raw.jsonl"
    _write_jsonl(bronze_path, [_bronze_record()])

    assert preprocessor.health_check() is True
