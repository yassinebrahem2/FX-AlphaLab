"""Tests for macro agent (Node 1: macroeconomics)."""

from __future__ import annotations

import pandas as pd
import pytest

from src.agents.macro import MacroAgent, MacroSignal


@pytest.fixture
def mock_signal_df() -> pd.DataFrame:
    """Minimal valid macro signal DataFrame: 2 pairs, 3 months each."""
    data = {
        "timestamp_utc": [
            pd.Timestamp("2024-10-31", tz="UTC"),
            pd.Timestamp("2024-11-30", tz="UTC"),
            pd.Timestamp("2024-12-31", tz="UTC"),
            pd.Timestamp("2024-10-31", tz="UTC"),
            pd.Timestamp("2024-11-30", tz="UTC"),
            pd.Timestamp("2024-12-31", tz="UTC"),
        ],
        "pair": ["EURUSD", "EURUSD", "EURUSD", "GBPUSD", "GBPUSD", "GBPUSD"],
        "module_c_direction": ["up", "down", "up", "down", "up", "down"],
        "macro_confidence": [0.65, 0.72, 0.58, 0.61, 0.75, 0.68],
        "carry_signal_score": [0.15, -0.22, 0.08, 0.32, -0.18, 0.12],
        "regime_context_score": [0.45, 0.52, 0.38, 0.41, 0.59, 0.48],
        "fundamental_mispricing_score": [0.28, 0.31, 0.19, 0.35, 0.29, 0.24],
        "macro_surprise_score": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "macro_bias_score": [0.51, 0.58, 0.42, 0.53, 0.64, 0.55],
    }
    return pd.DataFrame(data)


@pytest.fixture
def agent_with_mock_data(mock_signal_df: pd.DataFrame) -> MacroAgent:
    """Construct agent and inject mock data without loading from disk."""
    agent = MacroAgent()

    # Convert to indexed format (same as load() does)
    df = mock_signal_df.copy()
    df = df.set_index(["pair", "timestamp_utc"]).sort_index()

    # Inject mock data
    agent.macro_node._data = df

    return agent


def test_macro_agent_imports() -> None:
    """Test MacroAgent and MacroSignal imports."""
    assert MacroAgent is not None
    assert MacroSignal is not None

    # Verify MacroSignal is a dataclass
    import dataclasses

    assert dataclasses.is_dataclass(MacroSignal)


def test_predict_returns_macro_signal(agent_with_mock_data: MacroAgent) -> None:
    """Test predict() returns a MacroSignal with all 9 fields populated."""
    signal = agent_with_mock_data.predict("EURUSD", pd.Timestamp("2024-12-15", tz="UTC"))

    assert isinstance(signal, MacroSignal)
    assert signal.pair == "EURUSD"
    assert signal.signal_timestamp == pd.Timestamp("2024-11-30", tz="UTC")
    assert signal.module_c_direction in {"up", "down"}
    assert isinstance(signal.macro_confidence, float)
    assert isinstance(signal.carry_signal_score, float)
    assert isinstance(signal.regime_context_score, float)
    assert isinstance(signal.fundamental_mispricing_score, float)
    assert isinstance(signal.macro_surprise_score, float)
    assert isinstance(signal.macro_bias_score, float)
    assert signal.node_version == "macro_economics_v1"


def test_temporal_lookup_maps_mid_month_to_prior_month_end(
    agent_with_mock_data: MacroAgent,
) -> None:
    """Test: request for 2024-12-15 returns signal timestamped 2024-11-30."""
    signal_dec15 = agent_with_mock_data.predict("EURUSD", pd.Timestamp("2024-12-15", tz="UTC"))
    assert signal_dec15.signal_timestamp == pd.Timestamp("2024-11-30", tz="UTC")

    # Another request in Dec should still return Nov signal
    signal_dec25 = agent_with_mock_data.predict("EURUSD", pd.Timestamp("2024-12-25", tz="UTC"))
    assert signal_dec25.signal_timestamp == pd.Timestamp("2024-11-30", tz="UTC")


def test_pair_normalization(agent_with_mock_data: MacroAgent) -> None:
    """Test: 'EURUSDm' and 'EURUSD' return the same signal_timestamp."""
    signal_plain = agent_with_mock_data.predict("EURUSD", pd.Timestamp("2024-12-15", tz="UTC"))
    signal_m = agent_with_mock_data.predict("EURUSDm", pd.Timestamp("2024-12-15", tz="UTC"))

    assert signal_plain.signal_timestamp == signal_m.signal_timestamp
    assert signal_plain.macro_confidence == signal_m.macro_confidence


def test_unknown_pair_raises_value_error(agent_with_mock_data: MacroAgent) -> None:
    """Test: unknown pair raises ValueError."""
    with pytest.raises(ValueError, match="not in macro signal artifact"):
        agent_with_mock_data.predict("USDJPY", pd.Timestamp("2024-12-15", tz="UTC"))


def test_date_before_coverage_raises_key_error(agent_with_mock_data: MacroAgent) -> None:
    """Test: request date before coverage raises KeyError."""
    # Fixture has data from 2024-10-31. Request for 2024-01-01 (before) should fail.
    with pytest.raises(KeyError, match="No signal found"):
        agent_with_mock_data.predict("EURUSD", pd.Timestamp("2024-01-01", tz="UTC"))


def test_calendar_node_seam_macro_surprise_is_zero(agent_with_mock_data: MacroAgent) -> None:
    """Test: macro_surprise_score is 0.0 (Node 2 placeholder)."""
    signal = agent_with_mock_data.predict("EURUSD", pd.Timestamp("2024-12-15", tz="UTC"))
    assert signal.macro_surprise_score == 0.0
