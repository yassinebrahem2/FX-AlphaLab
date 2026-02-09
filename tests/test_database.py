import os
import tempfile

import pytest

from data.storage.database import export_to_csv, get_connection, insert_fx_prices


def postgres_available() -> bool:
    """Check if PostgreSQL is reachable."""
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not postgres_available(),
    reason="PostgreSQL is not running or not accessible",
)


def test_insert_fx_prices():
    """Test inserting a sample FX price row."""
    insert_fx_prices(
        [
            {
                "timestamp_utc": "2024-01-01 12:00:00",
                "pair": "TESTPAIR",
                "timeframe": "M1",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 100,
                "source": "pytest",
            }
        ]
    )

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT pair FROM fx_prices WHERE pair = 'TESTPAIR';")
        result = cur.fetchone()
    conn.close()

    assert result is not None


def test_export_to_csv():
    """Test exporting a table to CSV."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        output_path = tmp.name

    export_to_csv("fx_prices", output_path)

    assert os.path.exists(output_path)
    assert os.path.getsize(output_path) > 0

    os.remove(output_path)
