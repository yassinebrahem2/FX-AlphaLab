from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class SentimentContext:
    """Layer 2 explainability — per-source breakdown populated when include_context=True."""

    reddit_global_activity_zscore: float | None
    reddit_pair_views: dict[str, dict] | None  # tier C: pair-level Reddit breakdown
    stocktwits_pair_breakdown: dict[str, dict] | None  # all 4 pairs: bullish/bearish/net


@dataclass(frozen=True)
class SentimentSignal:
    """Signal payload produced by SentimentAgent for a single UTC day."""

    timestamp_utc: pd.Timestamp

    # Tier A — directional alpha (USDJPY only; IC confirmed walk-forward)
    usdjpy_stocktwits_vol_signal: float | None
    usdjpy_stocktwits_active: bool

    # Tier B — regime/volatility overlay
    gdelt_tone_zscore: float | None
    gdelt_attention_zscore: float | None
    macro_attention_zscore: float | None
    composite_stress_flag: bool

    # Layer 1 — which z-scores exceeded STRESS_ZSCORE_THRESHOLD, making composite_stress_flag interpretable
    stress_sources: list[str]

    # Layer 2 — per-source breakdown (None when include_context=False)
    context: SentimentContext | None
