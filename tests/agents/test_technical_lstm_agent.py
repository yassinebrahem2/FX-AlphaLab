"""Tests for production technical LSTM agent."""

from pathlib import Path

import numpy as np
import pandas as pd

from src.agents.technical.agent import (
    IndicatorSnapshot,
    TechnicalAgent,
    TechnicalSignal,
    fuse_timeframe_signals,
)


def _make_ohlcv_df(rows: int = 1600) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    idx = pd.date_range("2021-01-01", periods=rows, freq="D")

    close = 1.10 + np.cumsum(rng.normal(0.0, 0.002, size=rows))
    open_ = close + rng.normal(0.0, 0.001, size=rows)
    high = np.maximum(open_, close) + np.abs(rng.normal(0.001, 0.0005, size=rows))
    low = np.minimum(open_, close) - np.abs(rng.normal(0.001, 0.0005, size=rows))
    volume = rng.integers(1000, 5000, size=rows)

    return pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        },
        index=idx,
    )


def test_fit_save_load_predict_roundtrip(tmp_path: Path) -> None:
    df = _make_ohlcv_df()

    agent = TechnicalAgent(
        pair="EURUSDm",
        timeframe="D1",
        seq_len=30,
        epochs=2,
        patience=1,
        batch_size=64,
        hidden_size=16,
        model_version="test_lstm_v1",
    )

    metrics = agent.fit(df)

    assert 0.0 <= metrics.hit_ratio <= 1.0
    assert 0.0 <= metrics.f1 <= 1.0

    artifact_path = tmp_path / "technical_lstm_eurusdm_d1.pkl"
    saved_path = agent.save(artifact_path)
    assert saved_path.exists()
    assert saved_path.suffix == ".pkl"

    loaded_agent = TechnicalAgent.load(saved_path)
    signal = loaded_agent._predict_from_df(df)

    assert signal.pair == "EURUSDm"
    assert signal.timeframe == "D1"
    assert signal.direction in {0, 1}
    assert 0.0 <= signal.prob_up <= 1.0
    assert 0.0 <= signal.prob_down <= 1.0
    assert abs((signal.prob_up + signal.prob_down) - 1.0) < 1e-6
    assert 0.0 <= signal.confidence <= 1.0
    assert signal.volatility_regime in {"high", "low"}
    assert isinstance(signal.indicator_snapshot, IndicatorSnapshot)
    assert 0.0 <= signal.indicator_snapshot.rsi <= 100.0
    assert isinstance(signal.indicator_snapshot.above_ema200, bool)
    assert 0.0 <= signal.indicator_snapshot.atr_pct_rank <= 1.0
    assert signal.timeframe_votes is None


def test_fuse_timeframe_signals() -> None:
    ts = pd.Timestamp("2026-01-01")
    signals = {
        "D1": TechnicalSignal(
            pair="EURUSDm",
            timeframe="D1",
            timestamp_utc=ts,
            direction=1,
            prob_up=0.80,
            prob_down=0.20,
            confidence=0.60,
            volatility_regime="high",
            threshold_used=0.5,
            model_version="test_v1",
        ),
        "H4": TechnicalSignal(
            pair="EURUSDm",
            timeframe="H4",
            timestamp_utc=ts,
            direction=1,
            prob_up=0.55,
            prob_down=0.45,
            confidence=0.10,
            volatility_regime="low",
            threshold_used=0.5,
            model_version="test_v1",
        ),
        "H1": TechnicalSignal(
            pair="EURUSDm",
            timeframe="H1",
            timestamp_utc=ts,
            direction=0,
            prob_up=0.45,
            prob_down=0.55,
            confidence=0.10,
            volatility_regime="low",
            threshold_used=0.5,
            model_version="test_v1",
        ),
    }

    fused = fuse_timeframe_signals(signals)

    expected_score = (
        (1.0 / 3.0) * (0.80 - 0.5) + (1.0 / 3.0) * (0.55 - 0.5) + (1.0 / 3.0) * (0.45 - 0.5)
    )
    expected_prob_up = expected_score + 0.5
    expected_confidence = abs(expected_score) * 2.0

    assert fused.timeframe == "MTF"
    assert fused.direction == 1
    assert abs(fused.prob_up - expected_prob_up) < 1e-9
    assert abs(fused.confidence - expected_confidence) < 1e-9
    assert fused.volatility_regime == "high"
    assert fused.timeframe_votes == {"D1": 1, "H4": 1, "H1": 0}
    assert fused.indicator_snapshot is None  # per-tf signals in this test have no snapshot


def test_fuse_timeframe_signals_requires_d1() -> None:
    ts = pd.Timestamp("2026-01-01")
    signals = {
        "H4": TechnicalSignal(
            pair="EURUSDm",
            timeframe="H4",
            timestamp_utc=ts,
            direction=1,
            prob_up=0.60,
            prob_down=0.40,
            confidence=0.20,
            volatility_regime="low",
            threshold_used=0.5,
            model_version="test_v1",
        )
    }

    try:
        fuse_timeframe_signals(signals)
        assert False, "Expected ValueError when D1 is missing"
    except ValueError as exc:
        assert "D1" in str(exc)
