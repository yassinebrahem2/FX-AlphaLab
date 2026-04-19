"""Tests for production technical LSTM agent."""

from pathlib import Path

import numpy as np
import pandas as pd

from src.agents.technical.lstm_agent import LSTMTechnicalAgent


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

    agent = LSTMTechnicalAgent(
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

    loaded_agent = LSTMTechnicalAgent.load(saved_path)
    signal = loaded_agent.predict(df)

    assert signal.pair == "EURUSDm"
    assert signal.timeframe == "D1"
    assert signal.direction in {0, 1}
    assert 0.0 <= signal.prob_up <= 1.0
    assert 0.0 <= signal.prob_down <= 1.0
    assert abs((signal.prob_up + signal.prob_down) - 1.0) < 1e-6
    assert 0.0 <= signal.confidence <= 1.0
