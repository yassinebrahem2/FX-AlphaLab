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


def fetch_raw_ohlc(pair: str, timeframe: str) -> pd.DataFrame:
    """
    Fetch raw OHLC price data from MT5.

    Parameters:
        pair (str): EURUSD, GBPUSD, USDCHF, USDJPY
        timeframe (str): H1 or D1

    Returns:
        pd.DataFrame: raw OHLC data exactly as received
    """
    raise NotImplementedError("MT5 fetch logic not implemented yet")


def add_metadata(df: pd.DataFrame, pair: str, timeframe: str) -> pd.DataFrame:
    """
    Attach mandatory metadata without modifying price data.
    """
    df["pair"] = pair
    df["timeframe"] = timeframe
    df["source"] = "MT5"
    df["ingested_at_utc"] = datetime.now(timezone.utc)
    return df
