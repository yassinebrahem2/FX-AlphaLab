from __future__ import annotations

import pandas as pd
import pytest

from src.agents.macro import MacroAgent, MacroSignal

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PAIRS = ["EURUSD", "GBPUSD"]
MONTH_ENDS = [
    pd.Timestamp("2024-10-31", tz="UTC"),
    pd.Timestamp("2024-11-30", tz="UTC"),
    pd.Timestamp("2024-12-31", tz="UTC"),
]


@pytest.fixture
def mock_signal_df() -> pd.DataFrame:
    rows = []
    for pair in PAIRS:
        for ts in MONTH_ENDS:
            rows.append(
                {
                    "timestamp_utc": ts,
                    "pair": pair,
                    "module_c_direction": "up" if pair == "EURUSD" else "down",
                    "macro_confidence": 0.6,
                    "carry_signal_score": -1.2,
                    "regime_context_score": 0.1,
                    "fundamental_mispricing_score": -1.4,
                    "macro_surprise_score": 0.0,
                    "macro_bias_score": 0.8,
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture
def agent(mock_signal_df: pd.DataFrame) -> MacroAgent:
    a = MacroAgent()
    indexed = mock_signal_df.set_index(["pair", "timestamp_utc"]).sort_index()
    a.macro_node._data = indexed
    return a


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_macro_agent_imports() -> None:
    assert MacroAgent is not None
    assert MacroSignal is not None


def test_predict_returns_macro_signal(agent: MacroAgent) -> None:
    sig = agent.predict("EURUSD", pd.Timestamp("2024-12-15", tz="UTC"))
    assert isinstance(sig, MacroSignal)
    for field in (
        "pair",
        "signal_timestamp",
        "module_c_direction",
        "macro_confidence",
        "carry_signal_score",
        "regime_context_score",
        "fundamental_mispricing_score",
        "macro_surprise_score",
        "macro_bias_score",
        "node_version",
    ):
        assert hasattr(sig, field)


def test_temporal_lookup_maps_mid_month_to_prior_month_end(agent: MacroAgent) -> None:
    # Request 2024-12-15 → should return signal from 2024-11-30
    sig = agent.predict("EURUSD", pd.Timestamp("2024-12-15", tz="UTC"))
    assert sig.signal_timestamp == pd.Timestamp("2024-11-30", tz="UTC")


def test_pair_normalization(agent: MacroAgent) -> None:
    sig_plain = agent.predict("EURUSD", pd.Timestamp("2024-12-15", tz="UTC"))
    sig_m = agent.predict("EURUSDm", pd.Timestamp("2024-12-15", tz="UTC"))
    assert sig_plain.signal_timestamp == sig_m.signal_timestamp


def test_unknown_pair_raises_value_error(agent: MacroAgent) -> None:
    with pytest.raises(ValueError):
        agent.predict("USDJPY", pd.Timestamp("2024-12-15", tz="UTC"))


def test_date_before_coverage_raises_key_error(agent: MacroAgent) -> None:
    with pytest.raises(KeyError):
        agent.predict("EURUSD", pd.Timestamp("2024-01-01", tz="UTC"))


def test_calendar_node_seam_macro_surprise_is_zero(agent: MacroAgent) -> None:
    sig = agent.predict("EURUSD", pd.Timestamp("2024-12-15", tz="UTC"))
    assert sig.macro_surprise_score == 0.0
    assert agent.calendar_node is None
