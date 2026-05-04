from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.ingestion.preprocessors.dukascopy_preprocessor import (
    DukascopyPreprocessor,
    _resample_ohlcv,
)

# ── Helpers ────────────────────────────────────────────────────────────────


def _make_preprocessor(
    tmp_path: Path, instruments: list[str] | None = None
) -> DukascopyPreprocessor:
    return DukascopyPreprocessor(
        input_dir=tmp_path / "raw" / "dukascopy",
        output_dir=tmp_path / "processed" / "ohlcv",
        instruments=instruments or ["EURUSD"],
        log_file=tmp_path / "logs" / "duka_test.log",
    )


def _write_bronze_day(base_dir: Path, instrument: str, date: str, n_bars: int = 1440) -> None:
    """Write a synthetic 1-min Bronze Parquet for one calendar day."""
    d = pd.Timestamp(date, tz="UTC")
    idx = pd.date_range(d, periods=n_bars, freq="min")
    np.random.seed(42)
    prices = 1.10 + np.cumsum(np.random.randn(n_bars) * 0.0001)
    df = pd.DataFrame(
        {
            "timestamp_utc": idx,
            "open": prices,
            "high": prices + 0.0002,
            "low": prices - 0.0002,
            "close": prices,
            "volume": np.ones(n_bars, dtype="float32"),
        }
    )
    out = base_dir / instrument / date[:4] / f"{date[5:7]}" / f"{date.replace('-', '')}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)


# ── _resample_ohlcv unit tests ─────────────────────────────────────────────


def _synthetic_1min(days: int = 3) -> pd.DataFrame:
    idx = pd.date_range("2024-01-02", periods=days * 24 * 60, freq="min", tz="UTC")
    np.random.seed(0)
    p = 1.10 + np.cumsum(np.random.randn(len(idx)) * 0.0001)
    return pd.DataFrame(
        {
            "timestamp_utc": idx,
            "open": p,
            "high": p + 0.0002,
            "low": p - 0.0002,
            "close": p,
            "volume": np.ones(len(idx), dtype="float32"),
        }
    )


def test_resample_h1_bar_count_and_hours() -> None:
    df = _synthetic_1min(days=2)
    h1 = _resample_ohlcv(df, "H1")
    assert len(h1) == 48
    assert set(pd.to_datetime(h1["timestamp_utc"]).dt.hour.unique()) == set(range(24))


def test_resample_h4_midnight_utc_anchor() -> None:
    df = _synthetic_1min(days=2)
    h4 = _resample_ohlcv(df, "H4")
    assert len(h4) == 12
    assert set(pd.to_datetime(h4["timestamp_utc"]).dt.hour.unique()) == {0, 4, 8, 12, 16, 20}


def test_resample_d1_midnight_utc_timestamps() -> None:
    df = _synthetic_1min(days=3)
    d1 = _resample_ohlcv(df, "D1")
    assert len(d1) == 3
    for ts in d1["timestamp_utc"]:
        t = pd.Timestamp(ts)
        assert t.hour == 0 and t.minute == 0 and t.second == 0


def test_resample_ohlc_consistency() -> None:
    df = _synthetic_1min(days=1)
    for tf in ("H1", "H4", "D1"):
        out = _resample_ohlcv(df, tf)
        high_ok = (
            (out["high"] >= out["open"])
            & (out["high"] >= out["close"])
            & (out["high"] >= out["low"])
        )
        low_ok = (
            (out["low"] <= out["open"]) & (out["low"] <= out["close"]) & (out["low"] <= out["high"])
        )
        assert high_ok.all(), f"{tf}: high violation"
        assert low_ok.all(), f"{tf}: low violation"


def test_resample_volume_sums_correctly() -> None:
    df = _synthetic_1min(days=1)
    h1 = _resample_ohlcv(df, "H1")
    # Each H1 bar should aggregate exactly 60 1-min bars (all volume=1)
    assert (h1["volume"] == 60).all()


# ── DukascopyPreprocessor integration tests ────────────────────────────────


def test_preprocess_creates_silver_files(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_bronze_day(preprocessor.input_dir, "EURUSD", "2024-01-02")

    results = preprocessor.preprocess(backfill=True)

    assert set(results.keys()) == {"EURUSD_H1", "EURUSD_H4", "EURUSD_D1"}
    for tf in ("H1", "H4", "D1"):
        path = preprocessor.output_dir / f"ohlcv_EURUSD_{tf}_latest.parquet"
        assert path.exists(), f"Missing Silver file: {path.name}"


def test_preprocess_silver_schema(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_bronze_day(preprocessor.input_dir, "EURUSD", "2024-01-02")

    results = preprocessor.preprocess(backfill=True)

    for key, df in results.items():
        assert set(df.columns) >= {
            "timestamp_utc",
            "pair",
            "timeframe",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
        }
        assert df["pair"].iloc[0] == "EURUSD"
        assert df["source"].iloc[0] == "dukascopy"


def test_preprocess_backfill_false_skips_existing(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_bronze_day(preprocessor.input_dir, "EURUSD", "2024-01-02")

    preprocessor.preprocess(backfill=True)  # first run — creates files

    # Overwrite the D1 file with a marker row count
    d1_path = preprocessor.output_dir / "ohlcv_EURUSD_D1_latest.parquet"
    mtime_before = d1_path.stat().st_mtime

    preprocessor.preprocess(backfill=False)  # should skip

    assert d1_path.stat().st_mtime == mtime_before, "D1 file was unexpectedly rewritten"


def test_preprocess_backfill_true_overwrites(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_bronze_day(preprocessor.input_dir, "EURUSD", "2024-01-02")

    preprocessor.preprocess(backfill=True)
    d1_path = preprocessor.output_dir / "ohlcv_EURUSD_D1_latest.parquet"
    mtime_before = d1_path.stat().st_mtime

    import time

    time.sleep(0.05)  # ensure mtime differs on fast systems
    preprocessor.preprocess(backfill=True)  # should overwrite

    assert d1_path.stat().st_mtime > mtime_before, "D1 file was not rewritten on backfill=True"


def test_preprocess_removes_stale_collision_files(tmp_path: Path) -> None:
    """Old date-range-named files are removed when writing _latest."""
    preprocessor = _make_preprocessor(tmp_path)
    _write_bronze_day(preprocessor.input_dir, "EURUSD", "2024-01-02")

    # Plant a stale collision file that would cause find_parquet to raise
    stale = preprocessor.output_dir / "ohlcv_EURUSD_H1_2021-01-01_2025-12-31.parquet"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_bytes(b"dummy")

    preprocessor.preprocess(backfill=True)

    assert not stale.exists(), "Stale collision file was not removed"
    assert (preprocessor.output_dir / "ohlcv_EURUSD_H1_latest.parquet").exists()


def test_preprocess_empty_bronze_dir_returns_empty(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    # No Bronze files written
    results = preprocessor.preprocess(backfill=True)
    assert results == {}


def test_health_check_false_when_no_bronze(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    assert preprocessor.health_check() is False


def test_health_check_true_when_bronze_exists(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    _write_bronze_day(preprocessor.input_dir, "EURUSD", "2024-01-02")
    assert preprocessor.health_check() is True


def test_validate_raises_on_missing_columns(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    df = pd.DataFrame({"timestamp_utc": pd.date_range("2024-01-01", periods=3, freq="h", tz="UTC")})
    with pytest.raises(ValueError, match="Missing columns"):
        preprocessor.validate(df)


def test_validate_raises_on_ohlc_inconsistency(tmp_path: Path) -> None:
    preprocessor = _make_preprocessor(tmp_path)
    df = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range("2024-01-01", periods=3, freq="h", tz="UTC"),
            "pair": "EURUSD",
            "timeframe": "H1",
            "open": [1.1, 1.2, 1.3],
            "high": [1.0, 1.3, 1.4],  # high < open for first row
            "low": [1.05, 1.15, 1.25],
            "close": [1.08, 1.25, 1.35],
            "volume": [100.0, 100.0, 100.0],
        }
    )
    with pytest.raises(ValueError, match="OHLC high violation"):
        preprocessor.validate(df)


# ── Consumer roundtrip test ────────────────────────────────────────────────


def test_load_pair_roundtrip_produces_datetimeindex(tmp_path: Path, monkeypatch) -> None:
    """Silver files must produce a real DatetimeIndex when consumed via features.load_pair.

    Regression guard: writing with preserve_index=False produces 1970-epoch
    timestamps when load_pair does ``df.index = pd.to_datetime(df.index)``.
    """
    import src.agents.technical.features as features

    preprocessor = _make_preprocessor(tmp_path)
    _write_bronze_day(preprocessor.input_dir, "EURUSD", "2024-01-02", n_bars=1440)
    preprocessor.preprocess(backfill=True)

    monkeypatch.setattr(features, "DATA_DIR", preprocessor.output_dir)

    df = features.load_pair("EURUSDm", "H1")

    assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
    assert df.index[0].year >= 2024, f"Expected 2024+ timestamps, got {df.index[0]}"
    assert {"open", "high", "low", "close", "volume"}.issubset(df.columns)
