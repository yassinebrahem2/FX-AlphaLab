from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.ingestion.preprocessors.google_trends_preprocessor import GoogleTrendsPreprocessor


def _make_preprocessor(tmp_path: Path) -> GoogleTrendsPreprocessor:
    return GoogleTrendsPreprocessor(
        input_dir=tmp_path / "raw" / "google_trends",
        output_dir=tmp_path / "processed" / "sentiment" / "source=google_trends",
        log_file=tmp_path / "logs" / "google_trends_preprocess.log",
    )


def _write_trends_csv(path: Path, columns: list[str], start: str = "2024-01-07") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "date": pd.date_range(start, periods=4, freq="W-SUN"),
            **{
                column: [index * 10 + offset for offset in range(4)]
                for index, column in enumerate(columns, start=1)
            },
        }
    )
    frame.to_csv(path, index=False)


def test_column_naming_renames_keywords_to_theme_prefixed_snake_case(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(
        preprocessor.input_dir / "trends_macro_indicators_0.csv",
        ["CPI", "eurozone inflation", "bond yields"],
    )

    preprocessor.run(force=True)
    frame = pd.read_parquet(preprocessor.output_dir / "google_trends_weekly.parquet")

    assert [
        "date",
        "macro_indicators__bond_yields",
        "macro_indicators__cpi",
        "macro_indicators__eurozone_inflation",
    ] == list(frame.columns)


def test_theme_extraction_from_filename(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(preprocessor.input_dir / "trends_macro_indicators_0.csv", ["CPI"])

    preprocessor.run(force=True)
    frame = pd.read_parquet(preprocessor.output_dir / "google_trends_weekly.parquet")

    assert "macro_indicators__cpi" in frame.columns


def test_all_keyword_columns_are_float64(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(preprocessor.input_dir / "trends_fx_pairs_0.csv", ["EURUSD", "GBPUSD"])

    preprocessor.run(force=True)
    frame = pd.read_parquet(preprocessor.output_dir / "google_trends_weekly.parquet")

    assert str(frame["fx_pairs__eurusd"].dtype) == "float64"
    assert str(frame["fx_pairs__gbpusd"].dtype) == "float64"


def test_date_column_is_datetime64_ns(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(preprocessor.input_dir / "trends_risk_sentiment_0.csv", ["VIX"])

    preprocessor.run(force=True)
    frame = pd.read_parquet(preprocessor.output_dir / "google_trends_weekly.parquet")

    assert str(frame["date"].dtype) == "datetime64[ns]"


def test_output_written_to_correct_silver_path(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(preprocessor.input_dir / "trends_central_banks_0.csv", ["ECB"])

    result = preprocessor.run(force=True)

    assert result == {"google_trends_weekly": 4}
    assert (preprocessor.output_dir / "google_trends_weekly.parquet").exists()


def test_force_false_skips_rebuild_when_silver_exists(tmp_path: Path, monkeypatch) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(preprocessor.input_dir / "trends_central_banks_0.csv", ["ECB"])
    silver_path = preprocessor.output_dir / "google_trends_weekly.parquet"
    pd.DataFrame(
        {"date": pd.date_range("2024-01-07", periods=1, freq="W-SUN"), "x": [1.0]}
    ).to_parquet(silver_path, engine="pyarrow", index=False)

    def fail_read_csv(*args, **kwargs):
        raise AssertionError("CSV should not be read when Silver exists")

    monkeypatch.setattr(pd, "read_csv", fail_read_csv)

    result = preprocessor.run(force=False)

    assert result == {"google_trends_weekly": 1}


def test_force_true_overwrites_existing_silver(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(preprocessor.input_dir / "trends_fx_pairs_0.csv", ["EURUSD"])
    silver_path = preprocessor.output_dir / "google_trends_weekly.parquet"
    pd.DataFrame(
        {"date": pd.date_range("2024-01-07", periods=1, freq="W-SUN"), "old": [1.0]}
    ).to_parquet(silver_path, engine="pyarrow", index=False)

    result = preprocessor.run(force=True)
    frame = pd.read_parquet(silver_path)

    assert result == {"google_trends_weekly": 4}
    assert "fx_pairs__eurusd" in frame.columns


def test_outer_merge_preserves_union_of_dates(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(
        preprocessor.input_dir / "trends_fx_pairs_0.csv", ["EURUSD"], start="2024-01-07"
    )
    _write_trends_csv(
        preprocessor.input_dir / "trends_central_banks_0.csv", ["ECB"], start="2024-01-14"
    )

    result = preprocessor.run(force=True)
    frame = pd.read_parquet(preprocessor.output_dir / "google_trends_weekly.parquet")

    assert result == {"google_trends_weekly": 5}
    assert len(frame) == 5


def test_run_returns_row_count(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(preprocessor.input_dir / "trends_macro_indicators_0.csv", ["CPI"])

    result = preprocessor.run(force=True)

    assert result == {"google_trends_weekly": 4}


def test_health_check_false_when_no_csvs_exist(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)

    assert preprocessor.health_check() is False


def test_health_check_true_when_csv_exists(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_trends_csv(preprocessor.input_dir / "trends_macro_indicators_0.csv", ["CPI"])

    assert preprocessor.health_check() is True
