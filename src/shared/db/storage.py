"""
PostgreSQL storage layer for FX-AlphaLab using SQLAlchemy ORM.

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
from datetime import datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from .models import ECBExchangeRate, ECBPolicyRate, EconomicEvent, FXPrice, MacroIndicator
from .session import get_db

# Whitelist of allowed table names for export (SQL injection prevention)
ALLOWED_TABLES = {
    "fx_prices",
    "economic_events",
    "ecb_policy_rates",
    "ecb_exchange_rates",
    "macro_indicators",
}


def _parse_timestamp(value: str | datetime) -> datetime:
    """Parse timestamp string or return datetime as-is."""
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def insert_fx_prices(data: list[dict]) -> int:
    """
    Insert multiple FX price records into the database using SQLAlchemy ORM.

    Required keys per record:
    timestamp_utc, pair, timeframe, open, high, low, close, volume, source

    Args:
        data: List of dictionaries containing FX price data

    Returns:
        Number of records successfully inserted

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
    if not data:
        return 0

    inserted = 0
    with get_db() as session:
        for record in data:
            try:
                price = FXPrice(
                    timestamp_utc=_parse_timestamp(record["timestamp_utc"]),
                    pair=record["pair"],
                    timeframe=record.get("timeframe"),
                    open=record.get("open"),
                    high=record.get("high"),
                    low=record.get("low"),
                    close=record.get("close"),
                    volume=record.get("volume"),
                    source=record.get("source"),
                )
                session.add(price)
                session.flush()
                inserted += 1
            except IntegrityError:
                session.rollback()
                continue

    return inserted


def insert_economic_events(data: list[dict]) -> int:
    """
    Insert economic calendar events using SQLAlchemy ORM.

    Args:
        data: List of dictionaries containing event data

    Returns:
        Number of records successfully inserted

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
    if not data:
        return 0

    inserted = 0
    with get_db() as session:
        for record in data:
            try:
                event = EconomicEvent(
                    timestamp_utc=_parse_timestamp(record["timestamp_utc"]),
                    country=record.get("country"),
                    event_name=record.get("event_name"),
                    impact=record.get("impact"),
                    actual=record.get("actual"),
                    forecast=record.get("forecast"),
                    previous=record.get("previous"),
                    source=record.get("source"),
                )
                session.add(event)
                session.flush()
                inserted += 1
            except IntegrityError:
                session.rollback()
                continue

    return inserted


def insert_ecb_policy_rates(data: list[dict]) -> int:
    """
    Insert ECB policy rate data using SQLAlchemy ORM.

    Args:
        data: List of dictionaries containing ECB policy rate data

    Returns:
        Number of records successfully inserted

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
    if not data:
        return 0

    inserted = 0
    with get_db() as session:
        for record in data:
            try:
                rate = ECBPolicyRate(
                    timestamp_utc=_parse_timestamp(record["timestamp_utc"]),
                    rate_type=record.get("rate_type"),
                    rate=record.get("rate"),
                    frequency=record.get("frequency"),
                    unit=record.get("unit"),
                    source=record.get("source"),
                )
                session.add(rate)
                session.flush()
                inserted += 1
            except IntegrityError:
                session.rollback()
                continue

    return inserted


def insert_ecb_exchange_rates(data: list[dict]) -> int:
    """
    Insert ECB exchange rate data using SQLAlchemy ORM.

    Args:
        data: List of dictionaries containing ECB exchange rate data

    Returns:
        Number of records successfully inserted

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
    if not data:
        return 0

    inserted = 0
    with get_db() as session:
        for record in data:
            try:
                rate = ECBExchangeRate(
                    timestamp_utc=_parse_timestamp(record["timestamp_utc"]),
                    currency_pair=record.get("currency_pair"),
                    rate=record.get("rate"),
                    frequency=record.get("frequency"),
                    source=record.get("source"),
                )
                session.add(rate)
                session.flush()
                inserted += 1
            except IntegrityError:
                session.rollback()
                continue

    return inserted


def insert_macro_indicators(data: list[dict]) -> int:
    """
    Insert macroeconomic indicator data using SQLAlchemy ORM.

    Args:
        data: List of dictionaries containing macro indicator data

    Returns:
        Number of records successfully inserted

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
    if not data:
        return 0

    inserted = 0
    with get_db() as session:
        for record in data:
            try:
                indicator = MacroIndicator(
                    timestamp_utc=_parse_timestamp(record["timestamp_utc"]),
                    series_id=record.get("series_id"),
                    value=record.get("value"),
                    source=record.get("source"),
                )
                session.add(indicator)
                session.flush()
                inserted += 1
            except IntegrityError:
                session.rollback()
                continue

    return inserted


def export_to_csv(table_name: str, output_path: str) -> None:
    """
    Export a database table to a CSV file with SQL injection protection.

    Args:
        table_name: Name of the table to export (must be in ALLOWED_TABLES)
        output_path: Path to output CSV file

    Raises:
        ValueError: If table_name is not in the whitelist

    Example:
        export_to_csv("fx_prices", "fx_prices.csv")
    """
    # SQL injection protection: validate against whitelist
    if table_name not in ALLOWED_TABLES:
        raise ValueError(
            f"Invalid table name: {table_name}. "
            f"Allowed tables: {', '.join(sorted(ALLOWED_TABLES))}"
        )

    with get_db() as session:
        # Use parameterized query with text() for safe table name
        result = session.execute(text(f"SELECT * FROM {table_name}"))
        rows = result.fetchall()

        # Get column names from result
        headers = list(result.keys()) if rows else []

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        with output.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if headers:
                writer.writerow(headers)
            writer.writerows(rows)
