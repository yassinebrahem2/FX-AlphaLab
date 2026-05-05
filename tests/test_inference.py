"""Unit tests for src.inference.assembler and src.inference.cli."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.agents.coordinator.calibration import CalibrationParams
from src.agents.coordinator.predict import Coordinator
from src.inference.assembler import assemble_row

# ── Helpers ───────────────────────────────────────────────────────────────────

_DATE = pd.Timestamp("2025-01-10")
_PAIRS = ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]
_AGENT_SIGNALS_KEYS = {
    "date",
    "pair",
    "tech_direction",
    "tech_confidence",
    "tech_vol_regime",
    "geo_bilateral_risk",
    "geo_risk_regime",
    "macro_direction",
    "macro_confidence",
    "macro_carry_score",
    "macro_regime_score",
    "macro_fundamental_score",
    "macro_surprise_score",
    "macro_bias_score",
    "macro_dominant_driver",
    "usdjpy_stocktwits_vol_signal",
    "gdelt_tone_zscore",
    "gdelt_attention_zscore",
    "macro_attention_zscore",
    "composite_stress_flag",
    "tech_indicator_snapshot",
    "tech_timeframe_votes",
    "geo_top_events",
    "geo_base_zone_explanation",
    "geo_quote_zone_explanation",
    "geo_graph",
    "macro_top_calendar_events",
    "sentiment_stress_sources",
    "sentiment_stocktwits_breakdown",
}


# ── assembler: schema ─────────────────────────────────────────────────────────


def test_assemble_row_all_none_returns_correct_schema() -> None:
    row = assemble_row("GBPUSD", _DATE, None, None, None, None)
    assert set(row.keys()) == _AGENT_SIGNALS_KEYS


def test_assemble_row_all_none_values_are_none_or_false() -> None:
    row = assemble_row("GBPUSD", _DATE, None, None, None, None)
    for k, v in row.items():
        if k in ("date", "pair", "composite_stress_flag"):
            continue
        assert v is None, f"Expected None for key '{k}', got {v!r}"
    assert row["composite_stress_flag"] is False


@pytest.mark.parametrize("pair", _PAIRS)
def test_assemble_row_pair_preserved(pair: str) -> None:
    row = assemble_row(pair, _DATE, None, None, None, None)
    assert row["pair"] == pair


def test_assemble_row_date_is_date_object() -> None:
    row = assemble_row("EURUSD", _DATE, None, None, None, None)
    import datetime

    assert isinstance(row["date"], (datetime.date,))


# ── assembler: with mock signal objects ───────────────────────────────────────


def _mock_technical():
    from src.agents.technical.agent import TechnicalSignal

    return TechnicalSignal(
        pair="GBPUSD",
        timeframe="MTF",
        timestamp_utc=_DATE,
        direction=1,
        prob_up=0.6,
        prob_down=0.4,
        confidence=0.2,
        volatility_regime="normal",
        threshold_used=0.5,
        model_version="lstm_v1",
    )


def _mock_macro():
    from src.agents.macro.signal import MacroSignal

    return MacroSignal(
        pair="GBPUSD",
        signal_timestamp=_DATE,
        module_c_direction="up",
        macro_confidence=0.7,
        carry_signal_score=0.1,
        regime_context_score=0.05,
        fundamental_mispricing_score=0.2,
        macro_surprise_score=0.0,
        macro_bias_score=0.05,
        node_version="macro_economics_v1",
        dominant_driver="fundamental_mispricing_score",
    )


def _mock_geo():
    from src.agents.geopolitical.signal import (
        GeopoliticalSignal,
        ZoneFeatureExplanation,
        ZoneGraphData,
    )

    expl = ZoneFeatureExplanation(
        zone="GBP",
        risk_score=0.3,
        feature_zscores={},
        dominant_driver="log_count",
    )
    return GeopoliticalSignal(
        pair="GBPUSD",
        timestamp_utc=_DATE,
        base_ccy="GBP",
        quote_ccy="USD",
        base_zone="GBP",
        quote_zone="USD",
        bilateral_risk_score=0.4,
        risk_regime="low",
        base_zone_explanation=expl,
        quote_zone_explanation=expl,
        top_events=[],
        graph=ZoneGraphData(zone_risk_scores={}, edge_weights={}),
    )


def _mock_sentiment():
    from src.agents.sentiment.signal import SentimentSignal

    return SentimentSignal(
        timestamp_utc=_DATE,
        usdjpy_stocktwits_vol_signal=0.3,
        usdjpy_stocktwits_active=True,
        gdelt_tone_zscore=-0.5,
        gdelt_attention_zscore=0.4,
        macro_attention_zscore=0.2,
        composite_stress_flag=False,
        stress_sources=[],
        context=None,
    )


def test_assemble_row_with_all_signals_populated() -> None:
    row = assemble_row(
        "GBPUSD", _DATE, _mock_technical(), _mock_macro(), _mock_geo(), _mock_sentiment()
    )
    assert row["tech_direction"] == 1
    assert abs(row["tech_confidence"] - 0.2) < 1e-8
    assert row["macro_direction"] == "up"
    assert abs(row["geo_bilateral_risk"] - 0.4) < 1e-8
    assert row["geo_risk_regime"] == "low"
    assert abs(row["gdelt_tone_zscore"] - (-0.5)) < 1e-8
    assert abs(row["macro_attention_zscore"] - 0.2) < 1e-8
    assert row["composite_stress_flag"] is False


def test_assemble_row_stocktwits_vol_carried_for_all_pairs() -> None:
    """usdjpy_stocktwits_vol_signal is set on all pairs (coordinator ignores non-USDJPY rows)."""
    for pair in _PAIRS:
        row = assemble_row(pair, _DATE, None, None, None, _mock_sentiment())
        assert abs(row["usdjpy_stocktwits_vol_signal"] - 0.3) < 1e-8


# ── assembler feeds coordinator correctly ─────────────────────────────────────


def test_assembled_rows_feed_coordinator_without_error() -> None:
    """Assemble 4 rows with mock signals and verify Coordinator produces a valid report."""
    cal = CalibrationParams(
        usdjpy_conf_p75=0.088708,
        geo_slope=0.002405,
        geo_intercept=0.004057,
        st_slope=0.004930,
        st_intercept=0.006403,
        baseline_vol=0.004524,
    )
    rows = [
        assemble_row(p, _DATE, _mock_technical(), _mock_macro(), _mock_geo(), _mock_sentiment())
        for p in _PAIRS
    ]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])

    coordinator = Coordinator(calibration=cal)
    report = coordinator.build_report(df)

    assert {pa.pair for pa in report.pairs} == set(_PAIRS)
    assert report.overall_action in ("trade", "hold")
    # narrative_context must be JSON-safe (no NaN tokens)
    json.dumps(report.narrative_context, allow_nan=False)


# ── CLI smoke test ────────────────────────────────────────────────────────────


def test_cli_invalid_date_returns_exit_code_2() -> None:
    from src.inference.cli import main

    rc = main(["--date", "not-a-date"])
    assert rc == 2
