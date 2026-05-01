from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class TopCalendarEvent:
    """A single economic event driving the macro_surprise_score."""

    event_name: str
    country: str
    surprise_direction: float  # +1 = beat, -1 = miss
    surprise_magnitude: float  # |actual - forecast|; larger = bigger surprise
    impact_weight: float  # 1.0 high / 0.5 medium / 0.25 low
    prob: float  # model probability of directional price impact
    contribution: float  # normalized: score_i / total_weight — sums to macro_surprise_score


@dataclass(frozen=True)
class MacroSignal:
    """Monthly macro signal with baseline direction, sub-scores, and explainability."""

    pair: str
    signal_timestamp: pd.Timestamp  # month-end from parquet (e.g., 2025-01-31)
    module_c_direction: str  # "up" or "down"
    macro_confidence: float
    carry_signal_score: float
    regime_context_score: float
    fundamental_mispricing_score: float
    macro_surprise_score: float  # 0.0 until CalendarEventsNode integrated
    macro_bias_score: float
    node_version: str  # "macro_economics_v1"
    dominant_driver: str = ""  # which sub-score has the highest |value|
    top_calendar_events: list[TopCalendarEvent] | None = None  # Layer 2 — top surprise drivers
