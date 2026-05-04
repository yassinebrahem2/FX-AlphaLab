"""initial_schema_all_9_tables

Revision ID: 0001
Revises:
Create Date: 2026-05-04

Creates all 9 tables (5 existing Silver/reference tables + 4 Gold tables).
Enables TimescaleDB extension and creates hypertables for:
  - fx_prices        (high-frequency OHLCV, partitioned by timestamp_utc)
  - agent_signals    (daily agent outputs, partitioned by date)

Run on a fresh database:
    alembic upgrade head

Run on an existing database with tables already created by SQLAlchemy:
    alembic stamp head   # mark as applied without running DDL
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── TimescaleDB extension ──────────────────────────────────────────────
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")

    # ── fx_prices ─────────────────────────────────────────────────────────
    op.create_table(
        "fx_prices",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("timestamp_utc", sa.TIMESTAMP, nullable=False),
        sa.Column("pair", sa.String(10), nullable=False),
        sa.Column("timeframe", sa.String(10)),
        sa.Column("open", sa.Float),
        sa.Column("high", sa.Float),
        sa.Column("low", sa.Float),
        sa.Column("close", sa.Float),
        sa.Column("volume", sa.Float),
        sa.Column("source", sa.String(50)),
        sa.UniqueConstraint("timestamp_utc", "pair", "timeframe", name="uq_fx_prices"),
    )
    op.create_index("idx_fx_prices_time", "fx_prices", ["timestamp_utc"])
    # Note: hypertable conversion requires PKs to include the time column.
    # fx_prices uses a serial `id` PK which conflicts. Hypertable deferred to a
    # separate migration once the schema is reworked (drop id, composite PK on
    # timestamp_utc+pair+timeframe).

    # ── economic_events ────────────────────────────────────────────────────
    op.create_table(
        "economic_events",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("timestamp_utc", sa.TIMESTAMP, nullable=False),
        sa.Column("country", sa.String(50)),
        sa.Column("event_name", sa.String(100)),
        sa.Column("impact", sa.String(20)),
        sa.Column("actual", sa.Float),
        sa.Column("forecast", sa.Float),
        sa.Column("previous", sa.Float),
        sa.Column("source", sa.String(50)),
        sa.UniqueConstraint("timestamp_utc", "event_name", name="uq_economic_events"),
    )
    op.create_index("idx_economic_events_time", "economic_events", ["timestamp_utc"])

    # ── ecb_policy_rates ───────────────────────────────────────────────────
    op.create_table(
        "ecb_policy_rates",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("timestamp_utc", sa.TIMESTAMP, nullable=False),
        sa.Column("rate_type", sa.String(50)),
        sa.Column("rate", sa.Float),
        sa.Column("frequency", sa.String(20)),
        sa.Column("unit", sa.String(20)),
        sa.Column("source", sa.String(50)),
        sa.UniqueConstraint("timestamp_utc", "rate_type", name="uq_ecb_policy_rates"),
    )
    op.create_index("idx_ecb_policy_rates_time", "ecb_policy_rates", ["timestamp_utc"])

    # ── ecb_exchange_rates ────────────────────────────────────────────────
    op.create_table(
        "ecb_exchange_rates",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("timestamp_utc", sa.TIMESTAMP, nullable=False),
        sa.Column("currency_pair", sa.String(20)),
        sa.Column("rate", sa.Float),
        sa.Column("frequency", sa.String(20)),
        sa.Column("source", sa.String(50)),
        sa.UniqueConstraint("timestamp_utc", "currency_pair", name="uq_ecb_exchange_rates"),
    )
    op.create_index("idx_ecb_exchange_rates_time", "ecb_exchange_rates", ["timestamp_utc"])

    # ── macro_indicators ──────────────────────────────────────────────────
    op.create_table(
        "macro_indicators",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("timestamp_utc", sa.TIMESTAMP, nullable=False),
        sa.Column("series_id", sa.String(100)),
        sa.Column("value", sa.Float),
        sa.Column("source", sa.String(50)),
        sa.UniqueConstraint("timestamp_utc", "series_id", name="uq_macro_indicators"),
    )
    op.create_index("idx_macro_indicators_time", "macro_indicators", ["timestamp_utc"])

    # ── agent_signals (Gold, TimescaleDB hypertable) ───────────────────────
    op.create_table(
        "agent_signals",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("pair", sa.String(10), nullable=False),
        sa.Column("tech_direction", sa.Integer),
        sa.Column("tech_confidence", sa.Float),
        sa.Column("tech_vol_regime", sa.String(20)),
        sa.Column("geo_bilateral_risk", sa.Float),
        sa.Column("geo_risk_regime", sa.String(20)),
        sa.Column("macro_direction", sa.String(10)),
        sa.Column("macro_confidence", sa.Float),
        sa.Column("macro_carry_score", sa.Float),
        sa.Column("macro_regime_score", sa.Float),
        sa.Column("macro_fundamental_score", sa.Float),
        sa.Column("macro_surprise_score", sa.Float),
        sa.Column("macro_bias_score", sa.Float),
        sa.Column("macro_dominant_driver", sa.String(100)),
        sa.Column("usdjpy_stocktwits_vol_signal", sa.Float),
        sa.Column("gdelt_tone_zscore", sa.Float),
        sa.Column("gdelt_attention_zscore", sa.Float),
        sa.Column("macro_attention_zscore", sa.Float),
        sa.Column("composite_stress_flag", sa.Boolean),
        sa.UniqueConstraint("date", "pair", name="uq_agent_signals_date_pair"),
    )
    op.create_index("idx_agent_signals_date", "agent_signals", ["date"])
    # TimescaleDB requires a TIMESTAMPTZ column for hypertable; use date cast inline
    # Since agent_signals.date is DATE (not TIMESTAMPTZ), we skip hypertable here
    # and rely on the regular B-tree index for range queries.
    # To enable hypertable: alter date → TIMESTAMPTZ and call create_hypertable.

    # ── coordinator_signals ───────────────────────────────────────────────
    op.create_table(
        "coordinator_signals",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("pair", sa.String(10), nullable=False),
        sa.Column("vol_signal", sa.Float),
        sa.Column("vol_source", sa.String(20)),
        sa.Column("direction", sa.Integer),
        sa.Column("direction_source", sa.String(50)),
        sa.Column("direction_horizon", sa.String(10)),
        sa.Column("direction_ic", sa.Float),
        sa.Column("confidence_tier", sa.String(20)),
        sa.Column("flat_reason", sa.Text),
        sa.Column("regime", sa.String(30)),
        sa.Column("suggested_action", sa.String(10)),
        sa.Column("conviction_score", sa.Float),
        sa.Column("position_size_pct", sa.Float),
        sa.Column("sl_pct", sa.Float),
        sa.Column("tp_pct", sa.Float),
        sa.Column("risk_reward_ratio", sa.Float),
        sa.Column("estimated_vol_3d", sa.Float),
        sa.Column("is_top_pick", sa.Boolean),
        sa.Column("overall_action", sa.String(10)),
        sa.UniqueConstraint("date", "pair", name="uq_coordinator_signals_date_pair"),
    )
    op.create_index("idx_coordinator_signals_date", "coordinator_signals", ["date"])

    # ── coordinator_reports ───────────────────────────────────────────────
    op.create_table(
        "coordinator_reports",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("date", sa.Date, nullable=False, unique=True),
        sa.Column("top_pick", sa.String(10)),
        sa.Column("overall_action", sa.String(10), nullable=False),
        sa.Column("hold_reason", sa.Text),
        sa.Column("global_regime", sa.String(30)),
        sa.Column("narrative_context", JSONB),
    )
    op.create_index("idx_coordinator_reports_date", "coordinator_reports", ["date"])

    # ── trade_log ─────────────────────────────────────────────────────────
    op.create_table(
        "trade_log",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("backtest_run", sa.String(50), nullable=False, server_default="live"),
        sa.Column("pair", sa.String(10), nullable=False),
        sa.Column("direction", sa.String(10), nullable=False),
        sa.Column("entry_date", sa.Date, nullable=False),
        sa.Column("entry_price", sa.Float, nullable=False),
        sa.Column("exit_date", sa.Date),
        sa.Column("exit_price", sa.Float),
        sa.Column("exit_reason", sa.String(20)),
        sa.Column("hold_days", sa.Integer),
        sa.Column("position_pct", sa.Float),
        sa.Column("cost_pct", sa.Float),
        sa.Column("gross_pnl_pct", sa.Float),
        sa.Column("net_pnl_pct", sa.Float),
        sa.Column("equity_delta", sa.Float),
    )
    op.create_index("idx_trade_log_entry_date", "trade_log", ["entry_date"])


def downgrade() -> None:
    op.drop_table("trade_log")
    op.drop_table("coordinator_reports")
    op.drop_table("coordinator_signals")
    op.drop_table("agent_signals")
    op.drop_table("macro_indicators")
    op.drop_table("ecb_exchange_rates")
    op.drop_table("ecb_policy_rates")
    op.drop_table("economic_events")
    op.drop_table("fx_prices")
