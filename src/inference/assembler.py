"""Pure translation layer: agent signal dataclasses → signals_aligned row dict.

Each `assemble_row()` call takes one set of signals for a single (pair, date) and
returns a flat dict whose keys exactly match the `agent_signals` DB table columns.
None is used for any signal that was unavailable (stale/missing source).
"""

from __future__ import annotations

import pandas as pd

from src.agents.geopolitical.signal import GeopoliticalSignal
from src.agents.macro.signal import MacroSignal
from src.agents.sentiment.signal import SentimentSignal
from src.agents.technical.agent import TechnicalSignal

_PAIRS = frozenset(["EURUSD", "GBPUSD", "USDCHF", "USDJPY"])


def assemble_row(
    pair: str,
    date: pd.Timestamp,
    technical: TechnicalSignal | None,
    macro: MacroSignal | None,
    geopolitical: GeopoliticalSignal | None,
    sentiment: SentimentSignal | None,
) -> dict:
    """Return a flat dict matching the agent_signals schema for one (pair, date).

    Any signal argument may be None when that source was unavailable; the
    corresponding columns are set to None so the coordinator treats the source
    as missing and applies its fallback logic (FLAT / regime-only).
    """
    row: dict = {"date": date.date() if isinstance(date, pd.Timestamp) else date, "pair": pair}

    # ── Technical ─────────────────────────────────────────────────────────────
    if technical is not None:
        row["tech_direction"] = int(technical.direction)
        row["tech_confidence"] = float(technical.confidence)
        row["tech_vol_regime"] = str(technical.volatility_regime)
        snap = technical.indicator_snapshot
        row["tech_indicator_snapshot"] = (
            {
                "rsi": snap.rsi,
                "macd_hist": snap.macd_hist,
                "bb_pct": snap.bb_pct,
                "above_ema200": snap.above_ema200,
                "atr_pct_rank": snap.atr_pct_rank,
            }
            if snap is not None
            else None
        )
        row["tech_timeframe_votes"] = technical.timeframe_votes  # already dict[str, int] | None
    else:
        row["tech_direction"] = None
        row["tech_confidence"] = None
        row["tech_vol_regime"] = None
        row["tech_indicator_snapshot"] = None
        row["tech_timeframe_votes"] = None

    # ── Geopolitical ──────────────────────────────────────────────────────────
    if geopolitical is not None:
        row["geo_bilateral_risk"] = float(geopolitical.bilateral_risk_score)
        row["geo_risk_regime"] = str(geopolitical.risk_regime)
        row["geo_top_events"] = [
            {
                "actor1_name": e.actor1_name,
                "actor2_name": e.actor2_name,
                "goldstein_scale": e.goldstein_scale,
                "avg_tone": e.avg_tone,
                "num_mentions": e.num_mentions,
                "quad_class": e.quad_class,
                "source_url": e.source_url,
            }
            for e in geopolitical.top_events
        ]
        row["geo_base_zone_explanation"] = {
            "zone": geopolitical.base_zone_explanation.zone,
            "risk_score": geopolitical.base_zone_explanation.risk_score,
            "feature_zscores": geopolitical.base_zone_explanation.feature_zscores,
            "dominant_driver": geopolitical.base_zone_explanation.dominant_driver,
        }
        row["geo_quote_zone_explanation"] = {
            "zone": geopolitical.quote_zone_explanation.zone,
            "risk_score": geopolitical.quote_zone_explanation.risk_score,
            "feature_zscores": geopolitical.quote_zone_explanation.feature_zscores,
            "dominant_driver": geopolitical.quote_zone_explanation.dominant_driver,
        }
        row["geo_graph"] = {
            "zone_risk_scores": geopolitical.graph.zone_risk_scores,
            "edge_weights": geopolitical.graph.edge_weights,
        }
    else:
        row["geo_bilateral_risk"] = None
        row["geo_risk_regime"] = None
        row["geo_top_events"] = None
        row["geo_base_zone_explanation"] = None
        row["geo_quote_zone_explanation"] = None
        row["geo_graph"] = None

    # ── Macro ─────────────────────────────────────────────────────────────────
    if macro is not None:
        row["macro_direction"] = str(macro.module_c_direction)
        row["macro_confidence"] = float(macro.macro_confidence)
        row["macro_carry_score"] = float(macro.carry_signal_score)
        row["macro_regime_score"] = float(macro.regime_context_score)
        row["macro_fundamental_score"] = float(macro.fundamental_mispricing_score)
        row["macro_surprise_score"] = float(macro.macro_surprise_score)
        row["macro_bias_score"] = float(macro.macro_bias_score)
        row["macro_dominant_driver"] = str(macro.dominant_driver) if macro.dominant_driver else None
        row["macro_top_calendar_events"] = (
            [
                {
                    "event_name": e.event_name,
                    "country": e.country,
                    "surprise_direction": e.surprise_direction,
                    "surprise_magnitude": e.surprise_magnitude,
                    "impact_weight": e.impact_weight,
                    "prob": e.prob,
                    "contribution": e.contribution,
                }
                for e in macro.top_calendar_events
            ]
            if macro.top_calendar_events is not None
            else None
        )
    else:
        row["macro_direction"] = None
        row["macro_confidence"] = None
        row["macro_carry_score"] = None
        row["macro_regime_score"] = None
        row["macro_fundamental_score"] = None
        row["macro_surprise_score"] = None
        row["macro_bias_score"] = None
        row["macro_dominant_driver"] = None
        row["macro_top_calendar_events"] = None

    # ── Sentiment (global signal, applied per-pair) ───────────────────────────
    if sentiment is not None:
        # usdjpy_stocktwits_vol_signal is stored on every row but only used for USDJPY
        row["usdjpy_stocktwits_vol_signal"] = (
            float(sentiment.usdjpy_stocktwits_vol_signal)
            if sentiment.usdjpy_stocktwits_vol_signal is not None
            else None
        )
        row["gdelt_tone_zscore"] = (
            float(sentiment.gdelt_tone_zscore) if sentiment.gdelt_tone_zscore is not None else None
        )
        row["gdelt_attention_zscore"] = (
            float(sentiment.gdelt_attention_zscore)
            if sentiment.gdelt_attention_zscore is not None
            else None
        )
        row["macro_attention_zscore"] = (
            float(sentiment.macro_attention_zscore)
            if sentiment.macro_attention_zscore is not None
            else None
        )
        row["composite_stress_flag"] = bool(sentiment.composite_stress_flag)
        row["sentiment_stress_sources"] = list(sentiment.stress_sources)
        row["sentiment_stocktwits_breakdown"] = (
            sentiment.context.stocktwits_pair_breakdown if sentiment.context is not None else None
        )
    else:
        row["usdjpy_stocktwits_vol_signal"] = None
        row["gdelt_tone_zscore"] = None
        row["gdelt_attention_zscore"] = None
        row["macro_attention_zscore"] = None
        row["composite_stress_flag"] = False
        row["sentiment_stress_sources"] = None
        row["sentiment_stocktwits_breakdown"] = None

    return row
