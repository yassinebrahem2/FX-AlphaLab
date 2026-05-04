"""Pydantic schemas for OHLCV candles."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class OHLCVBar(BaseModel):
    timestamp_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
