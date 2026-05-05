from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Column,
    Date,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from .base import Base


class FXPrice(Base):
    __tablename__ = "fx_prices"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    pair = Column(String(10), nullable=False)
    timeframe = Column(String(10))
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "pair", "timeframe"),
        Index("idx_fx_prices_time", "timestamp_utc"),
    )


class EconomicEvent(Base):
    __tablename__ = "economic_events"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    country = Column(String(50))
    event_name = Column(String(100))
    impact = Column(String(20))
    actual = Column(Float)
    forecast = Column(Float)
    previous = Column(Float)
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "event_name"),
        Index("idx_economic_events_time", "timestamp_utc"),
    )


class ECBPolicyRate(Base):
    __tablename__ = "ecb_policy_rates"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    rate_type = Column(String(50))
    rate = Column(Float)
    frequency = Column(String(20))
    unit = Column(String(20))
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "rate_type"),
        Index("idx_ecb_policy_rates_time", "timestamp_utc"),
    )


class ECBExchangeRate(Base):
    __tablename__ = "ecb_exchange_rates"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    currency_pair = Column(String(20))
    rate = Column(Float)
    frequency = Column(String(20))
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "currency_pair"),
        Index("idx_ecb_exchange_rates_time", "timestamp_utc"),
    )


class MacroIndicator(Base):
    __tablename__ = "macro_indicators"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    series_id = Column(String(100))
    value = Column(Float)
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "series_id"),
        Index("idx_macro_indicators_time", "timestamp_utc"),
    )


# ── Gold Layer ────────────────────────────────────────────────────────────────


class AgentSignal(Base):
    """Daily raw signals from all agents, one row per (date, pair).

    TimescaleDB hypertable partitioned by date.
    No fwd_* columns — those are leakage and never stored here.
    """

    __tablename__ = "agent_signals"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    pair = Column(String(10), nullable=False)
    tech_direction = Column(Integer)
    tech_confidence = Column(Float)
    tech_vol_regime = Column(String(20))
    geo_bilateral_risk = Column(Float)
    geo_risk_regime = Column(String(20))
    macro_direction = Column(String(10))
    macro_confidence = Column(Float)
    macro_carry_score = Column(Float)
    macro_regime_score = Column(Float)
    macro_fundamental_score = Column(Float)
    macro_surprise_score = Column(Float)
    macro_bias_score = Column(Float)
    macro_dominant_driver = Column(String(100))
    usdjpy_stocktwits_vol_signal = Column(Float)
    gdelt_tone_zscore = Column(Float)
    gdelt_attention_zscore = Column(Float)
    macro_attention_zscore = Column(Float)
    composite_stress_flag = Column(Boolean)
    # Rich nested outputs — stored as JSONB so the frontend can render full explainability
    tech_indicator_snapshot = Column(JSONB)  # {rsi, macd_hist, bb_pct, above_ema200, atr_pct_rank}
    tech_timeframe_votes = Column(JSONB)  # {"D1": 0|1, "H4": 0|1, "H1": 0|1}
    geo_top_events = Column(JSONB)  # list[TopEvent]
    geo_base_zone_explanation = Column(JSONB)  # ZoneFeatureExplanation
    geo_quote_zone_explanation = Column(JSONB)
    geo_graph = Column(JSONB)  # ZoneGraphData
    macro_top_calendar_events = Column(JSONB)  # list[TopCalendarEvent]
    sentiment_stress_sources = Column(JSONB)  # list[str]
    sentiment_stocktwits_breakdown = Column(JSONB)  # SentimentContext.stocktwits_pair_breakdown

    __table_args__ = (
        UniqueConstraint("date", "pair", name="uq_agent_signals_date_pair"),
        Index("idx_agent_signals_date", "date"),
    )


class CoordinatorSignalRow(Base):
    """Per-pair coordinator output (PairAnalysis), one row per (date, pair).

    Stores all deterministic trade parameters produced by Coordinator.build_report().
    """

    __tablename__ = "coordinator_signals"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    pair = Column(String(10), nullable=False)
    vol_signal = Column(Float)
    vol_source = Column(String(20))
    direction = Column(Integer)
    direction_source = Column(String(50))
    direction_horizon = Column(String(10))
    direction_ic = Column(Float)
    confidence_tier = Column(String(20))
    flat_reason = Column(Text)
    regime = Column(String(30))
    suggested_action = Column(String(10))
    conviction_score = Column(Float)
    position_size_pct = Column(Float)
    sl_pct = Column(Float)
    tp_pct = Column(Float)
    risk_reward_ratio = Column(Float)
    estimated_vol_3d = Column(Float)
    is_top_pick = Column(Boolean)
    overall_action = Column(String(10))

    __table_args__ = (
        UniqueConstraint("date", "pair", name="uq_coordinator_signals_date_pair"),
        Index("idx_coordinator_signals_date", "date"),
    )


class CoordinatorReportRow(Base):
    """One CoordinatorReport summary per date.

    narrative_context stored as JSONB for LLM consumption.
    """

    __tablename__ = "coordinator_reports"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    top_pick = Column(String(10))
    overall_action = Column(String(10), nullable=False)
    hold_reason = Column(Text)
    global_regime = Column(String(30))
    narrative_context = Column(JSONB)

    __table_args__ = (Index("idx_coordinator_reports_date", "date"),)


class TradeLogRow(Base):
    """Historical and live trade log, one row per closed position.

    backtest_run distinguishes historical backtest entries ("backtest_nb05_v1")
    from live signal entries ("live").
    """

    __tablename__ = "trade_log"

    id = Column(Integer, primary_key=True)
    backtest_run = Column(String(50), nullable=False, default="live")
    pair = Column(String(10), nullable=False)
    direction = Column(String(10), nullable=False)
    entry_date = Column(Date, nullable=False)
    entry_price = Column(Float, nullable=False)
    exit_date = Column(Date)
    exit_price = Column(Float)
    exit_reason = Column(String(20))
    hold_days = Column(Integer)
    position_pct = Column(Float)
    cost_pct = Column(Float)
    gross_pnl_pct = Column(Float)
    net_pnl_pct = Column(Float)
    equity_delta = Column(Float)

    __table_args__ = (Index("idx_trade_log_entry_date", "entry_date"),)
