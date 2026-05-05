"""Router for display-only OHLCV candlestick data."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from src.backend.schemas.ohlcv import OHLCVBar
from src.ingestion.preprocessors.dukascopy_preprocessor import _TF_RULE, DukascopyPreprocessor
from src.shared.config import Config

router = APIRouter(prefix="/ohlcv", tags=["ohlcv"])

_VALID_INSTRUMENTS = set(DukascopyPreprocessor.DEFAULT_INSTRUMENTS)
_VALID_TIMEFRAMES = set(_TF_RULE)
_DAYS_CAP: dict[str, int] = {"M15": 90, "H1": 365, "H4": 365, "D1": 365}


@router.get("/{instrument}", response_model=list[OHLCVBar])
def get_ohlcv(
    instrument: str,
    tf: str = Query(default="H1"),
    days: int = Query(default=30, ge=1),
) -> list[OHLCVBar]:
    """Return resampled OHLCV bars for a Dukascopy instrument."""
    if instrument not in _VALID_INSTRUMENTS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid instrument '{instrument}'. Must be one of: {sorted(_VALID_INSTRUMENTS)}",
        )
    if tf not in _VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid timeframe '{tf}'. Must be one of: {sorted(_VALID_TIMEFRAMES)}",
        )

    days = min(days, _DAYS_CAP[tf])
    start_dt = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=days)

    silver_path = (
        Config.DATA_DIR / "processed" / "ohlcv" / f"ohlcv_{instrument}_{tf}_latest.parquet"
    )
    if not silver_path.exists():
        raise HTTPException(status_code=404, detail=f"No Silver data for {instrument} {tf}")

    df = pd.read_parquet(silver_path)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for {instrument} {tf}")

    df = df.reset_index()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df[df["timestamp_utc"] >= start_dt]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for {instrument} in last {days} days")

    return [OHLCVBar(**row) for row in df.to_dict("records")]
