"""Tests for production geopolitical agent."""

import pandas as pd
import pytest

from src.agents.geopolitical.agent import GeopoliticalAgent


def test_signal_fields_eurusd() -> None:
    agent = GeopoliticalAgent()
    signal = agent.predict("EURUSDm", pd.Timestamp("2023-06-15", tz="UTC"))

    assert signal.base_ccy == "EUR"
    assert signal.quote_ccy == "USD"
    assert signal.base_zone == "EUR"
    assert signal.quote_zone == "USD"
    assert signal.risk_regime in {"high", "low"}
    assert signal.risk_trend in {"rising", "falling", "stable"}
    assert signal.risk_diff == signal.base_zone_risk - signal.quote_zone_risk
    assert signal.risk_level == max(signal.base_zone_risk, signal.quote_zone_risk)
    assert isinstance(signal.top_drivers, list)


def test_signal_fields_usdjpy() -> None:
    agent = GeopoliticalAgent()
    signal = agent.predict("USDJPYm", pd.Timestamp("2023-06-15", tz="UTC"))

    assert signal.base_ccy == "USD"
    assert signal.quote_ccy == "JPY"
    assert signal.risk_diff == signal.base_zone_risk - signal.quote_zone_risk


def test_bilateral_asymmetry() -> None:
    agent = GeopoliticalAgent()
    date = pd.Timestamp("2023-06-15", tz="UTC")

    eurusd_signal = agent.predict("EURUSDm", date)
    gbpusd_signal = agent.predict("GBPUSDm", date)

    assert eurusd_signal.risk_diff != gbpusd_signal.risk_diff


def test_no_zone_raises() -> None:
    agent = GeopoliticalAgent()

    with pytest.raises(ValueError):
        agent.predict("AUDNZDm", pd.Timestamp("2023-01-01", tz="UTC"))


def test_out_of_range_raises() -> None:
    agent = GeopoliticalAgent()

    with pytest.raises(KeyError):
        agent.predict("EURUSDm", pd.Timestamp("2026-06-01", tz="UTC"))


def test_regime_threshold() -> None:
    agent = GeopoliticalAgent()
    agent.load()

    assert agent._data is not None
    high_rows = agent._data[agent._data["geo_risk_index"] > 100.0]
    assert not high_rows.empty

    (signal_date, signal_zone), row = next(iter(high_rows.iterrows()))

    zone_to_currency = {zone: ccy for ccy, zone in agent.CURRENCY_TO_ZONE.items()}
    base_ccy = zone_to_currency[signal_zone]

    # Pair with USD unless dominant zone is USD, then pair with EUR.
    quote_ccy = "USD" if base_ccy != "USD" else "EUR"
    pair = f"{base_ccy}{quote_ccy}m"

    signal = agent.predict(pair, pd.Timestamp(signal_date))

    assert signal.risk_level >= float(row["geo_risk_index"])
    assert signal.risk_regime == "high"


def test_lazy_load() -> None:
    agent = GeopoliticalAgent()

    assert agent._data is None
    _ = agent.predict("EURUSDm", pd.Timestamp("2023-06-15", tz="UTC"))
    assert agent._data is not None
