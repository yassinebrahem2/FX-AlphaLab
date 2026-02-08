"""
Database Schema Definitions
Week 4 – Foundation & Data
"""

PRICE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS price_ohlc (
    pair TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,

    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL,

    source TEXT NOT NULL,
    ingested_at_utc TEXT NOT NULL,

    PRIMARY KEY (pair, timeframe, timestamp_utc)
);
"""
