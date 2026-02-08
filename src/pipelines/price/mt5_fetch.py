"""
MT5 Price Fetcher
Week 4 – Raw Data Only

RULES:
- NO indicators
- NO enrichment
- NO inference
"""

from datetime import datetime, timezone
import pandas as pd

from src.pipelines.price.mt5_connector import MT5Connector


# -----------------------------
# Constants
# -----------------------------

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

def fetch_raw_ohlc(
    pair: str,
    timeframe: str,
    n_bars: int = 5000,
) -> pd.DataFrame:
    """
    Fetch raw OHLC data from MT5.

    Returns:
        Raw OHLC exactly as provided by MT5 (no transformations).
    """
    connector = MT5Connector()

    try:
        df = connector.fetch_ohlc(
            symbol=pair,
            timeframe=timeframe,
            n_bars=n_bars,
        )
    finally:
        connector.shutdown()

    if df is None or len(df) == 0:
        raise RuntimeError(f"No data returned for {pair} {timeframe}")

    return df


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
