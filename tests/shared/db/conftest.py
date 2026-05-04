"""Pytest fixtures for storage layer tests."""

import pytest
from sqlalchemy import text

from src.shared.db import (
    engine,
)


@pytest.fixture(autouse=True)
def cleanup_database():
    """Clean up test data from storage tables before each test."""
    # Delete test data before test runs
    with engine.begin() as conn:
        # Clear tables where test data is inserted (all use 'pytest' as source)
        conn.execute(text("DELETE FROM fx_prices WHERE source = 'pytest'"))
        conn.execute(text("DELETE FROM economic_events WHERE source = 'pytest'"))
        conn.execute(text("DELETE FROM ecb_policy_rates WHERE source = 'pytest'"))
        conn.execute(text("DELETE FROM ecb_exchange_rates WHERE source = 'pytest'"))
        conn.execute(text("DELETE FROM macro_indicators WHERE source = 'pytest'"))
        conn.commit()

    yield

    # Clean up after test runs (optional, but good practice)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fx_prices WHERE source = 'pytest'"))
        conn.execute(text("DELETE FROM economic_events WHERE source = 'pytest'"))
        conn.execute(text("DELETE FROM ecb_policy_rates WHERE source = 'pytest'"))
        conn.execute(text("DELETE FROM ecb_exchange_rates WHERE source = 'pytest'"))
        conn.execute(text("DELETE FROM macro_indicators WHERE source = 'pytest'"))
        conn.commit()
