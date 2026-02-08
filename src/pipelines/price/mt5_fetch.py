"""
MT5 Price Fetcher
Week 4 – Raw Data Only

RULES:
- NO indicators
- NO enrichment
- NO inference
"""

from datetime import datetime, timezone
from typing import Dict

import MetaTrader5 as mt5
import pandas as pd


# -----------------------------
# Constants
# -----------------------------

MT5_TIMEFRAMES: Dict[str, int] = {
    "H1": mt5.TIMEFRAME_H1,
    "D1": mt5.TIMEFRAME_D1,
}

REQUIRED_COLUMNS = [
    "timestamp_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


# -----------------------------
# MT5 Data Fetching
# -----------------------------

def fetch_raw_ohlc(pair: str, timeframe: str, n_bars: int = 5000) -> pd.DataFrame:
    """
    Fetch raw OHLC data from MT5.

    Returns:
        Raw OHLC exactly as provided by MT5 (no transformations).
    """

    if timeframe not in MT5_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    if not mt5.initialize():
        raise RuntimeError("MT5 initialization failed")

    try:
        rates = mt5.copy_rates_from_pos(
            pair,
            MT5_TIMEFRAMES[timeframe],
            0,
            n_bars,
        )
    finally:
        mt5.shutdown()

    if rates is None or len(rates) == 0:
        raise RuntimeError(f"No data returned for {pair} {timeframe}")

    return pd.DataFrame(rates)


# -----------------------------
# Normalisation (Still Raw)
# -----------------------------

def standardize_raw_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize raw MT5 OHLC data without enrichment:
    - Convert timestamps to UTC
    - Canonical column selection
    """

    df = df.copy()

    df["timestamp_utc"] = pd.to_datetime(
        df["time"],
        unit="s",
        utc=True,
    )

    df.rename(
        columns={
            "tick_volume": "volume",
        },
        inplace=True,
    )

    return df[REQUIRED_COLUMNS]


# -----------------------------
# Metadata Attachment
# -----------------------------

def add_metadata(
    df: pd.DataFrame,
    pair: str,
    timeframe: str,
) -> pd.DataFrame:
    """
    Attach mandatory metadata without modifying price data.
    """

    df = df.copy()
    df["pair"] = pair
    df["timeframe"] = timeframe
    df["source"] = "MT5"
    df["ingested_at_utc"] = datetime.now(timezone.utc)

    return df
