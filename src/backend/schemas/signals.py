"""Pydantic response schemas for agent_signals and coordinator_signals."""

from __future__ import annotations

import datetime

from pydantic import BaseModel


class AgentSignalResponse(BaseModel):
    date: datetime.date
    pair: str
    tech_direction: int | None
    tech_confidence: float | None
    tech_vol_regime: str | None
    geo_bilateral_risk: float | None
    geo_risk_regime: str | None
    macro_direction: str | None
    macro_confidence: float | None
    macro_carry_score: float | None
    macro_regime_score: float | None
    macro_fundamental_score: float | None
    macro_surprise_score: float | None
    macro_bias_score: float | None
    macro_dominant_driver: str | None
    usdjpy_stocktwits_vol_signal: float | None
    gdelt_tone_zscore: float | None
    gdelt_attention_zscore: float | None
    macro_attention_zscore: float | None
    composite_stress_flag: bool | None
    tech_indicator_snapshot: dict | None
    tech_timeframe_votes: dict | None
    geo_top_events: list | None
    geo_base_zone_explanation: dict | None
    geo_quote_zone_explanation: dict | None
    geo_graph: dict | None
    macro_top_calendar_events: list | None
    sentiment_stress_sources: list | None
    sentiment_stocktwits_breakdown: dict | None

    model_config = {"from_attributes": True}


class CoordinatorSignalResponse(BaseModel):
    date: datetime.date
    pair: str
    vol_signal: float | None
    vol_source: str | None
    direction: int | None
    direction_source: str | None
    direction_horizon: str | None
    direction_ic: float | None
    confidence_tier: str | None
    flat_reason: str | None
    regime: str | None
    suggested_action: str | None
    conviction_score: float | None
    position_size_pct: float | None
    sl_pct: float | None
    tp_pct: float | None
    risk_reward_ratio: float | None
    estimated_vol_3d: float | None
    is_top_pick: bool | None
    overall_action: str | None

    model_config = {"from_attributes": True}


class DateSignalsResponse(BaseModel):
    date: datetime.date
    agent_signals: list[AgentSignalResponse]
    coordinator_signals: list[CoordinatorSignalResponse]
