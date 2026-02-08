import unittest
from unittest.mock import patch
import pandas as pd

from data.ingestion.mt5_collector import MT5Connector



class TestMT5Connector(unittest.TestCase):
    """Unit tests for MT5Connector (CI-safe, fully mocked)."""

    @patch("data.ingestion.mt5_collector.mt5")
    def test_connect_success(self, mock_mt5):
        """MT5 initializes successfully."""
        mock_mt5.initialize.return_value = True

        connector = MT5Connector()
        connector.connect()

        self.assertTrue(connector.connected)
        mock_mt5.initialize.assert_called_once()

    @patch("data.ingestion.mt5_collector.mt5")

    def test_connect_failure_with_retries(self, mock_mt5):
        """MT5 initialization fails after max retries."""
        mock_mt5.initialize.return_value = False

        connector = MT5Connector(max_retries=2, retry_delay_sec=0)

        with self.assertRaises(RuntimeError):
            connector.connect()

        self.assertFalse(connector.connected)
        self.assertEqual(mock_mt5.initialize.call_count, 2)

    def test_invalid_timeframe(self):
        """Unsupported timeframe raises ValueError."""
        connector = MT5Connector()

        with self.assertRaises(ValueError):
            connector.fetch_ohlc("EURUSD", "M5")

    @patch("data.ingestion.mt5_collector.mt5")
    def test_fetch_empty_data(self, mock_mt5):
        """Empty MT5 response raises RuntimeError."""
        mock_mt5.initialize.return_value = True
        mock_mt5.symbol_select.return_value = True
        mock_mt5.copy_rates_from_pos.return_value = []

        connector = MT5Connector()

        with self.assertRaises(RuntimeError):
            connector.fetch_ohlc("EURUSD", "H1")

    @patch("data.ingestion.mt5_collector.mt5")
    def test_fetch_success(self, mock_mt5):
        """Successful OHLC fetch returns non-empty DataFrame."""
        mock_mt5.initialize.return_value = True
        mock_mt5.symbol_select.return_value = True
        mock_mt5.copy_rates_from_pos.return_value = [
            (0, 1.0, 1.2, 0.9, 1.1, 100, 0, 0)
        ]

        connector = MT5Connector()
        df = connector.fetch_ohlc("EURUSD", "H1")

        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        mock_mt5.copy_rates_from_pos.assert_called_once()

    @patch("data.ingestion.mt5_collector.mt5")
    def test_symbol_not_available(self, mock_mt5):
        """Unavailable symbol raises RuntimeError."""
        mock_mt5.initialize.return_value = True
        mock_mt5.symbol_select.return_value = False

        connector = MT5Connector()

        with self.assertRaises(RuntimeError):
            connector.fetch_ohlc("INVALID", "H1")

    @patch("data.ingestion.mt5_collector.mt5")
    def test_shutdown(self, mock_mt5):
        """MT5 shutdown resets connection state."""
        mock_mt5.initialize.return_value = True

        connector = MT5Connector()
        connector.connect()
        connector.shutdown()

        mock_mt5.shutdown.assert_called_once()
        self.assertFalse(connector.connected)


if __name__ == "__main__":
    unittest.main()
