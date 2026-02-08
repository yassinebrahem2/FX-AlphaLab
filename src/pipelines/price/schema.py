"""
Price OHLC Data Schema
Week 4 – Foundation & Data
"""

PRICE_SCHEMA = {
    "pair": str,                 # EURUSD, GBPUSD, USDCHF, USDJPY
    "timeframe": str,            # H1, D1
    "timestamp_utc": "datetime64[ns]",
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "volume": int,
    "source": str,               # MT5
    "ingested_at_utc": "datetime64[ns]"
}
