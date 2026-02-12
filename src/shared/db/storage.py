"""
Simple PostgreSQL storage layer for FX-AlphaLab.

This module provides helper functions to insert market data into the database
and export tables to CSV format.

Example:

    from src.shared.db.storage import insert_fx_prices

    insert_fx_prices([
        {
            "timestamp_utc": "2024-01-01 12:00:00",
            "pair": "EURUSD",
            "timeframe": "H1",
            "open": 1.10,
            "high": 1.11,
            "low": 1.09,
            "close": 1.105,
            "volume": 1234,
            "source": "mt5"
        }
    ])
"""

import csv
import os

import psycopg2
from psycopg2 import IntegrityError, OperationalError
from psycopg2.extras import execute_batch


def get_connection():
    """
    Create and return a PostgreSQL database connection using environment variables.

    Example:
        from src.shared.db.storage import get_connection

        conn = get_connection()
        print("Connected:", conn is not None)
        conn.close()
    """
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "fx_alphalab"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
        )
    except OperationalError as e:
        raise RuntimeError(f"Database connection failed: {e}")


def _batch_insert(query: str, data: list[dict]):
    """Internal helper for batch inserts with basic error handling."""
    if not data:
        return

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, data)
    except IntegrityError as e:
        print(f"Insert skipped due to integrity error: {e}")
        conn.rollback()
    finally:
        conn.close()


def insert_fx_prices(data: list[dict]):
    """
    Insert multiple FX price records into the database.

    Required keys per record:
    timestamp_utc, pair, timeframe, open, high, low, close, volume, source

    Example:
        insert_fx_prices([
            {
                "timestamp_utc": "2024-01-01 12:00:00",
                "pair": "EURUSD",
                "timeframe": "H1",
                "open": 1.10,
                "high": 1.11,
                "low": 1.09,
                "close": 1.105,
                "volume": 1234,
                "source": "mt5"
            }
        ])
    """
    query = """
        INSERT INTO fx_prices (timestamp_utc, pair, timeframe, open, high, low, close, volume, source)
        VALUES (%(timestamp_utc)s, %(pair)s, %(timeframe)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(source)s)
        ON CONFLICT DO NOTHING;
    """
    _batch_insert(query, data)


def insert_economic_events(data: list[dict]):
    """
    Insert economic calendar events.

    Example:
        insert_economic_events([
            {
                "timestamp_utc": "2024-01-01 10:00:00",
                "country": "US",
                "event_name": "CPI",
                "impact": "High",
                "actual": 3.2,
                "forecast": 3.0,
                "previous": 2.9,
                "source": "forexfactory"
            }
        ])
    """
    query = """
        INSERT INTO economic_events (timestamp_utc, country, event_name, impact, actual, forecast, previous, source)
        VALUES (%(timestamp_utc)s, %(country)s, %(event_name)s, %(impact)s, %(actual)s, %(forecast)s, %(previous)s, %(source)s)
        ON CONFLICT DO NOTHING;
    """
    _batch_insert(query, data)


def insert_ecb_policy_rates(data: list[dict]):
    """
    Insert ECB policy rate data.

    Example:
        insert_ecb_policy_rates([
            {
                "timestamp_utc": "2024-01-01",
                "rate_type": "Deposit Facility",
                "rate": 4.0,
                "frequency": "Monthly",
                "unit": "Percent",
                "source": "ECB"
            }
        ])
    """
    query = """
        INSERT INTO ecb_policy_rates (timestamp_utc, rate_type, rate, frequency, unit, source)
        VALUES (%(timestamp_utc)s, %(rate_type)s, %(rate)s, %(frequency)s, %(unit)s, %(source)s)
        ON CONFLICT DO NOTHING;
    """
    _batch_insert(query, data)


def insert_ecb_exchange_rates(data: list[dict]):
    """
    Insert ECB exchange rate data.

    Example:
        insert_ecb_exchange_rates([
            {
                "timestamp_utc": "2024-01-01",
                "currency_pair": "EURUSD",
                "rate": 1.09,
                "frequency": "Daily",
                "source": "ECB"
            }
        ])
    """
    query = """
        INSERT INTO ecb_exchange_rates (timestamp_utc, currency_pair, rate, frequency, source)
        VALUES (%(timestamp_utc)s, %(currency_pair)s, %(rate)s, %(frequency)s, %(source)s)
        ON CONFLICT DO NOTHING;
    """
    _batch_insert(query, data)


def insert_macro_indicators(data: list[dict]):
    """
    Insert macroeconomic indicator data.

    Example:
        insert_macro_indicators([
            {
                "timestamp_utc": "2024-01-01",
                "series_id": "GDP_US",
                "value": 2.5,
                "source": "FRED"
            }
        ])
    """
    query = """
        INSERT INTO macro_indicators (timestamp_utc, series_id, value, source)
        VALUES (%(timestamp_utc)s, %(series_id)s, %(value)s, %(source)s)
        ON CONFLICT DO NOTHING;
    """
    _batch_insert(query, data)


def export_to_csv(table_name: str, output_path: str):
    """
    Export a database table to a CSV file.

    Example:
        export_to_csv("fx_prices", "fx_prices.csv")
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name}")
            rows = cur.fetchall()
            headers = [desc[0] for desc in cur.description]

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    except Exception as e:
        print(f"CSV export failed: {e}")

    finally:
        conn.close()
