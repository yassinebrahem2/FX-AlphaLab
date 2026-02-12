"""Tests for SQLAlchemy ORM storage layer."""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError

from src.shared.db import (
    ALLOWED_TABLES,
    ECBExchangeRate,
    ECBPolicyRate,
    EconomicEvent,
    FXPrice,
    MacroIndicator,
    engine,
    export_to_csv,
    get_db,
    insert_ecb_exchange_rates,
    insert_ecb_policy_rates,
    insert_economic_events,
    insert_fx_prices,
    insert_macro_indicators,
)


def postgres_available() -> bool:
    """Check if PostgreSQL is reachable."""
    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except OperationalError:
        return False


pytestmark = pytest.mark.skipif(
    not postgres_available(),
    reason="PostgreSQL is not running or not accessible",
)


class TestFXPrices:
    """Tests for FX prices storage."""

    def test_insert_fx_prices(self):
        """Test inserting FX price records."""
        data = [
            {
                "timestamp_utc": "2024-01-01 12:00:00",
                "pair": "TESTPAIR1",
                "timeframe": "M1",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 100,
                "source": "pytest",
            }
        ]
        inserted = insert_fx_prices(data)
        assert inserted == 1

        # Verify in database
        with get_db() as session:
            result = session.query(FXPrice).filter(FXPrice.pair == "TESTPAIR1").first()
            assert result is not None
            assert result.pair == "TESTPAIR1"
            assert result.open == 1.0
            assert result.close == 1.05

    def test_insert_duplicate_fx_prices(self):
        """Test that duplicate FX prices are handled correctly."""
        data = [
            {
                "timestamp_utc": "2024-01-02 12:00:00",
                "pair": "TESTPAIR2",
                "timeframe": "H1",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 100,
                "source": "pytest",
            }
        ]
        # Insert first time
        first = insert_fx_prices(data)
        assert first == 1

        # Insert duplicate - should be silently ignored
        second = insert_fx_prices(data)
        assert second == 0  # Duplicate not inserted

    def test_insert_empty_fx_prices(self):
        """Test inserting empty list returns 0."""
        result = insert_fx_prices([])
        assert result == 0


class TestEconomicEvents:
    """Tests for economic events storage."""

    def test_insert_economic_events(self):
        """Test inserting economic event records."""
        data = [
            {
                "timestamp_utc": "2024-01-01 10:00:00",
                "country": "US",
                "event_name": "Test CPI",
                "impact": "High",
                "actual": 3.2,
                "forecast": 3.0,
                "previous": 2.9,
                "source": "pytest",
            }
        ]
        inserted = insert_economic_events(data)
        assert inserted == 1

        # Verify in database
        with get_db() as session:
            result = (
                session.query(EconomicEvent).filter(EconomicEvent.event_name == "Test CPI").first()
            )
            assert result is not None
            assert result.country == "US"
            assert result.actual == 3.2


class TestECBRates:
    """Tests for ECB rates storage."""

    def test_insert_ecb_policy_rates(self):
        """Test inserting ECB policy rate records."""
        data = [
            {
                "timestamp_utc": "2024-01-01",
                "rate_type": "Test Facility",
                "rate": 4.0,
                "frequency": "Monthly",
                "unit": "Percent",
                "source": "pytest",
            }
        ]
        inserted = insert_ecb_policy_rates(data)
        assert inserted == 1

        # Verify in database
        with get_db() as session:
            result = (
                session.query(ECBPolicyRate)
                .filter(ECBPolicyRate.rate_type == "Test Facility")
                .first()
            )
            assert result is not None
            assert result.rate == 4.0

    def test_insert_ecb_exchange_rates(self):
        """Test inserting ECB exchange rate records."""
        data = [
            {
                "timestamp_utc": "2024-01-01",
                "currency_pair": "TESTUSD",
                "rate": 1.09,
                "frequency": "Daily",
                "source": "pytest",
            }
        ]
        inserted = insert_ecb_exchange_rates(data)
        assert inserted == 1

        # Verify in database
        with get_db() as session:
            result = (
                session.query(ECBExchangeRate)
                .filter(ECBExchangeRate.currency_pair == "TESTUSD")
                .first()
            )
            assert result is not None
            assert result.rate == 1.09


class TestMacroIndicators:
    """Tests for macro indicators storage."""

    def test_insert_macro_indicators(self):
        """Test inserting macro indicator records."""
        data = [
            {
                "timestamp_utc": "2024-01-01",
                "series_id": "TEST_GDP",
                "value": 2.5,
                "source": "pytest",
            }
        ]
        inserted = insert_macro_indicators(data)
        assert inserted == 1

        # Verify in database
        with get_db() as session:
            result = (
                session.query(MacroIndicator).filter(MacroIndicator.series_id == "TEST_GDP").first()
            )
            assert result is not None
            assert result.value == 2.5


class TestCSVExport:
    """Tests for CSV export functionality."""

    def test_export_to_csv(self):
        """Test exporting a table to CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "fx_prices.csv"
            export_to_csv("fx_prices", str(output_path))

            assert output_path.exists()
            assert output_path.stat().st_size > 0

            # Verify CSV has headers
            with output_path.open() as f:
                first_line = f.readline()
                assert "timestamp_utc" in first_line
                assert "pair" in first_line

    def test_export_invalid_table_name(self):
        """Test that invalid table names are rejected (SQL injection protection)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"

            with pytest.raises(ValueError, match="Invalid table name"):
                export_to_csv("users; DROP TABLE fx_prices; --", str(output_path))

            with pytest.raises(ValueError, match="Invalid table name"):
                export_to_csv("nonexistent_table", str(output_path))

    def test_allowed_tables_constant(self):
        """Test that ALLOWED_TABLES contains expected table names."""
        expected_tables = {
            "fx_prices",
            "economic_events",
            "ecb_policy_rates",
            "ecb_exchange_rates",
            "macro_indicators",
        }
        assert ALLOWED_TABLES == expected_tables
