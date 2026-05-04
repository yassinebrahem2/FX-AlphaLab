from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class CoordinatorSignal:
    """Intermediate signal produced per pair before PairAnalysis computation."""

    pair: str
    date: pd.Timestamp
    vol_signal: float | None
    vol_source: str  # "stocktwits" | "geo" | "none"
    direction: int | None  # 1 = base appreciates, 0 = base depreciates, None = flat
    direction_source: str
    direction_horizon: str  # "1d" | "5d" | "none"
    direction_ic: float | None
    confidence_tier: str  # "high" | "medium" | "low" | "none"
    flat_reason: str | None
    regime: str  # "high_attention" | "normal" | "unknown"


@dataclass(frozen=True)
class PairAnalysis:
    """Per-pair decision packet with all deterministic trade parameters."""

    pair: str
    direction: int | None
    direction_label: str  # e.g. "LONG GBPUSD"
    confidence_tier: str  # "high" | "medium" | "low" | "none"
    direction_source: str
    direction_horizon: str  # "1d" | "5d" | "none"
    direction_ic: float | None
    vol_signal_norm: float | None  # sign-normalised: higher → more vol expected
    vol_source: str
    estimated_vol_3d: float  # OLS-calibrated expected daily vol (log-return units)
    regime: str
    suggested_action: str  # "LONG" | "SHORT" | "FLAT"
    conviction_score: float  # tier_weight × direction_edge
    position_size_pct: float  # % of equity to risk
    sl_pct: float  # % stop-loss distance from entry
    tp_pct: float  # % take-profit distance from entry
    risk_reward_ratio: float  # tp_pct / sl_pct
    flat_reason: str | None  # why direction is None, if applicable
    raw_signals: dict  # all agent outputs forwarded to LLM


@dataclass(frozen=True)
class CoordinatorReport:
    """Full decision packet consumed by the LLM to generate prose recommendations."""

    date: pd.Timestamp
    pairs: list[PairAnalysis]
    ranked_pairs: list[str]  # sorted by conviction descending
    top_pick: str | None  # None when overall_action == "hold"
    overall_action: str  # "trade" | "hold"
    hold_reason: str | None
    global_regime: str
    narrative_context: dict  # pre-formatted numbers for LLM (LLM must not modify)


@dataclass(frozen=True)
class OpenTrade:
    """Live position state held by the backtester between signal dates."""

    pair: str
    direction: str  # "LONG" | "SHORT"
    entry_date: pd.Timestamp
    entry_price: float
    sl_price: float
    tp_price: float
    position_pct: float  # % of equity (notional allocation)
    cost_pct: float  # round-trip spread as % of entry


@dataclass(frozen=True)
class ClosedTrade:
    """Immutable record of a completed trade."""

    pair: str
    direction: str
    entry_date: pd.Timestamp
    entry_price: float
    exit_date: pd.Timestamp
    exit_price: float
    exit_reason: str  # "SL" | "TP" | "FLIP" | "EOD_HOLD"
    hold_days: int
    position_pct: float
    cost_pct: float
    gross_pnl_pct: float
    net_pnl_pct: float
    equity_delta: float  # net_pnl_pct/100 × position_pct/100 (equity fraction change)
