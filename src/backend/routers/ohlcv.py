"""Router for display-only OHLCV candlestick data."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from src.backend.schemas.ohlcv import OHLCVBar
from src.ingestion.preprocessors.dukascopy_preprocessor import (
    _TF_RULE,
    DukascopyPreprocessor,
    _resample_ohlcv,
)
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

    bronze_dir = Config.DATA_DIR / "raw" / "dukascopy" / instrument
    if not bronze_dir.exists():
        raise HTTPException(status_code=404, detail=f"No Bronze data for {instrument}")

    frames: list[pd.DataFrame] = []
    for path in sorted(bronze_dir.glob("**/*.parquet")):
        file_dt = _bronze_file_datetime(path)
        if file_dt is None or file_dt < start_dt:
            continue

        df = pd.read_parquet(path)
        if not df.empty:
            frames.append(df)

    if not frames:
        raise HTTPException(status_code=404, detail=f"No data for {instrument} in last {days} days")

    df_1min = pd.concat(frames, ignore_index=True)
    df_1min["timestamp_utc"] = pd.to_datetime(df_1min["timestamp_utc"], utc=True)
    df_1min = (
        df_1min.sort_values("timestamp_utc")
        .drop_duplicates(subset=["timestamp_utc"])
        .reset_index(drop=True)
    )

    if df_1min.empty:
        raise HTTPException(status_code=404, detail=f"No data for {instrument} in last {days} days")

    df_resampled = _resample_ohlcv(df_1min, tf)
    return [OHLCVBar(**row) for row in df_resampled.to_dict("records")]


def _bronze_file_datetime(path: Path) -> datetime | None:
    try:
        return datetime.strptime(path.stem, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
