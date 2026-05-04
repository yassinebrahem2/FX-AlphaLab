"""Pydantic response schemas for trade_log."""

from __future__ import annotations

import datetime

from pydantic import BaseModel


class TradeLogResponse(BaseModel):
    id: int
    backtest_run: str
    pair: str
    direction: str
    entry_date: datetime.date
    entry_price: float
    exit_date: datetime.date | None
    exit_price: float | None
    exit_reason: str | None
    hold_days: int | None
    position_pct: float | None
    cost_pct: float | None
    gross_pnl_pct: float | None
    net_pnl_pct: float | None
    equity_delta: float | None

    model_config = {"from_attributes": True}
