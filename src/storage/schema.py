"""
Database Schema Definitions
Week 4 – Foundation & Data

This module defines all persistent storage contracts.
Tables are append-only and idempotent by primary key.
"""

# -------------------------------------------------------------------
# Price OHLC data
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# Macro / Economic data (RAW)
# -------------------------------------------------------------------

MACRO_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS macro_data (
    event_id TEXT NOT NULL,

    country TEXT NOT NULL,
    currency TEXT NOT NULL,
    indicator TEXT NOT NULL,

    scheduled_datetime_utc TEXT,
    release_datetime_utc TEXT NOT NULL,
    ingested_at_utc TEXT NOT NULL,

    actual_value REAL NOT NULL,
    previous_value REAL,
    revision_value REAL,

    unit TEXT NOT NULL,
    frequency TEXT NOT NULL,
    source TEXT NOT NULL,

    PRIMARY KEY (event_id)
);
"""
