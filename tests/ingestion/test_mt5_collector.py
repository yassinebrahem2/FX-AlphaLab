"""Unit tests for MT5 data collector (CI-safe, fully mocked).

Tests focus on Bronze layer responsibilities:
- Fetching raw data from MT5 terminal
- Adding source="mt5" column
- Lowercase snake_case column names
- Preserving raw Unix epoch timestamps (no conversion)
- Export to data/raw/mt5/

Preprocessing to Silver layer is tested separately in test_price_normalizer.py
"""

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Mock MetaTrader5 before importing collector — MT5 is Windows-only and
# unavailable in CI.  The mock is injected into sys.modules so that the
# module-level ``import MetaTrader5`` inside mt5_collector succeeds.
_mock_mt5 = MagicMock()
_mock_mt5.TIMEFRAME_H1 = 1
_mock_mt5.TIMEFRAME_H4 = 4
_mock_mt5.TIMEFRAME_D1 = 24
sys.modules["MetaTrader5"] = _mock_mt5

from src.ingestion.collectors.mt5_collector import MT5Collector, MT5Connector  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mt5_row(ts: int = 1_609_459_200) -> dict:
    """Single MT5 OHLCV row as a dict (mirrors structured numpy array)."""
    return {
        "time": [ts],
        "open": [1.2200],
        "high": [1.2250],
        "low": [1.2190],
        "close": [1.2230],
        "tick_volume": [1000],
        "spread": [2],
        "real_volume": [0],
    }


_MOCK_DF = pd.DataFrame(_mt5_row())
_MOCK_DF_2 = pd.DataFrame(
    {
        "time": [1_609_459_200, 1_609_462_800],
        "open": [1.2200, 1.2210],
        "high": [1.2250, 1.2260],
        "low": [1.2190, 1.2200],
        "close": [1.2230, 1.2240],
        "tick_volume": [1000, 1500],
        "spread": [2, 2],
        "real_volume": [0, 0],
    }
)


# ---------------------------------------------------------------------------
# MT5Connector
# ---------------------------------------------------------------------------


class TestMT5Connector:
    """Tests for the low-level MT5 connection wrapper."""

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_connect_success(self, mock_mt5):
        mock_mt5.initialize.return_value = True
        connector = MT5Connector()
        connector.connect()

        assert connector.connected is True
        mock_mt5.initialize.assert_called_once()

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_connect_skips_if_already_connected(self, mock_mt5):
        mock_mt5.initialize.return_value = True
        connector = MT5Connector()
        connector.connect()
        connector.connect()  # second call should be a no-op

        assert mock_mt5.initialize.call_count == 1

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_connect_failure_with_retries(self, mock_mt5):
        mock_mt5.initialize.return_value = False
        connector = MT5Connector(max_retries=2, retry_delay_sec=0)

        with pytest.raises(RuntimeError, match="initialisation failed"):
            connector.connect()

        assert connector.connected is False
        assert mock_mt5.initialize.call_count == 2

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_shutdown(self, mock_mt5):
        mock_mt5.initialize.return_value = True
        connector = MT5Connector()
        connector.connect()
        connector.shutdown()

        mock_mt5.shutdown.assert_called_once()
        assert connector.connected is False

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_shutdown_noop_when_not_connected(self, mock_mt5):
        connector = MT5Connector()
        connector.shutdown()  # should not raise

        mock_mt5.shutdown.assert_not_called()

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_invalid_timeframe(self, mock_mt5):
        connector = MT5Connector()
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            connector.fetch_ohlc("EURUSD", "M5", datetime(2023, 1, 1), datetime(2024, 1, 1))

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_symbol_not_available(self, mock_mt5):
        mock_mt5.initialize.return_value = True
        mock_mt5.symbol_select.return_value = False
        connector = MT5Connector()

        with pytest.raises(RuntimeError, match="Symbol not available"):
            connector.fetch_ohlc("INVALID", "H1", datetime(2023, 1, 1), datetime(2024, 1, 1))

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_fetch_ohlc_success(self, mock_mt5):
        mock_mt5.initialize.return_value = True
        mock_mt5.symbol_select.return_value = True
        mock_mt5.copy_rates_range.return_value = [(0, 1.0, 1.2, 0.9, 1.1, 100, 0, 0)]

        connector = MT5Connector()
        df = connector.fetch_ohlc("EURUSD", "H1", datetime(2023, 1, 1), datetime(2024, 1, 1))

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        mock_mt5.copy_rates_range.assert_called_once()

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_fetch_ohlc_empty_raises(self, mock_mt5):
        mock_mt5.initialize.return_value = True
        mock_mt5.symbol_select.return_value = True
        mock_mt5.copy_rates_range.return_value = []

        connector = MT5Connector()
        with pytest.raises(RuntimeError, match="No data returned"):
            connector.fetch_ohlc("EURUSD", "H1", datetime(2023, 1, 1), datetime(2024, 1, 1))

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_fetch_ohlc_none_raises(self, mock_mt5):
        mock_mt5.initialize.return_value = True
        mock_mt5.symbol_select.return_value = True
        mock_mt5.copy_rates_range.return_value = None

        connector = MT5Connector()
        with pytest.raises(RuntimeError, match="No data returned"):
            connector.fetch_ohlc("EURUSD", "H1", datetime(2023, 1, 1), datetime(2024, 1, 1))

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_fetch_auto_connects(self, mock_mt5):
        """fetch_ohlc connects automatically if not already connected."""
        mock_mt5.initialize.return_value = True
        mock_mt5.symbol_select.return_value = True
        mock_mt5.copy_rates_range.return_value = [(0, 1.0, 1.2, 0.9, 1.1, 100, 0, 0)]

        connector = MT5Connector()
        assert connector.connected is False
        connector.fetch_ohlc("EURUSD", "H1", datetime(2023, 1, 1), datetime(2024, 1, 1))
        assert connector.connected is True


# ---------------------------------------------------------------------------
# MT5Collector — initialisation
# ---------------------------------------------------------------------------


class TestMT5CollectorInit:
    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_default_configuration(self, mock_mt5, tmp_path):
        collector = MT5Collector(output_dir=tmp_path)
        assert collector.pairs == MT5Collector.DEFAULT_PAIRS
        assert collector.timeframes == MT5Collector.DEFAULT_TIMEFRAMES
        assert collector.years == MT5Collector.DEFAULT_YEARS

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_custom_configuration(self, mock_mt5, tmp_path):
        collector = MT5Collector(
            output_dir=tmp_path,
            pairs=["EURUSD"],
            timeframes=["D1"],
            years=1,
        )
        assert collector.pairs == ["EURUSD"]
        assert collector.timeframes == ["D1"]
        assert collector.years == 1

    @patch("src.ingestion.collectors.mt5_collector.mt5")
    def test_default_output_dir(self, mock_mt5, tmp_path, monkeypatch):
        monkeypatch.setattr("src.shared.config.Config.DATA_DIR", tmp_path)
        collector = MT5Collector()
        assert collector.output_dir == tmp_path / "raw" / "mt5"


# ---------------------------------------------------------------------------
# MT5Collector — health_check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    def test_success(self, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        collector = MT5Collector(output_dir=tmp_path, pairs=["EURUSD"], timeframes=["H1"])
        assert collector.health_check() is True
        mock_inst.connect.assert_called_once()
        mock_inst.shutdown.assert_called_once()

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    def test_failure(self, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        mock_inst.connect.side_effect = RuntimeError("MT5 unavailable")
        collector = MT5Collector(output_dir=tmp_path)
        assert collector.health_check() is False


# ---------------------------------------------------------------------------
# MT5Collector — _fetch_and_normalise (§3.1 Bronze compliance)
# ---------------------------------------------------------------------------


class TestFetchAndNormalise:
    """Focused unit tests for the §3.1 Bronze normalisation step.

    Bronze layer responsibilities ONLY:
    - Add source column
    - Lowercase column names
    - Preserve raw Unix epoch time (NO datetime conversion)
    - Preserve all MT5 fields
    """

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    def test_adds_source_column(self, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.return_value = _MOCK_DF.copy()

        collector = MT5Collector(output_dir=tmp_path)
        df = collector._fetch_and_normalise(
            "EURUSD", "H1", datetime(2023, 1, 1), datetime(2024, 1, 1)
        )
        assert "source" in df.columns
        assert (df["source"] == "mt5").all()

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    def test_columns_are_lowercase(self, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        raw = _MOCK_DF.copy()
        raw.columns = [c.upper() for c in raw.columns]  # simulate non-lowercase
        mock_inst.fetch_ohlc.return_value = raw

        collector = MT5Collector(output_dir=tmp_path)
        df = collector._fetch_and_normalise(
            "EURUSD", "H1", datetime(2023, 1, 1), datetime(2024, 1, 1)
        )
        for col in df.columns:
            assert col == col.lower(), f"Column '{col}' not lowercase"

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    def test_time_preserved_as_raw(self, mock_cls, tmp_path):
        """Bronze layer preserves raw Unix epoch time (no conversion)."""
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.return_value = _MOCK_DF.copy()

        collector = MT5Collector(output_dir=tmp_path)
        df = collector._fetch_and_normalise(
            "EURUSD", "H1", datetime(2023, 1, 1), datetime(2024, 1, 1)
        )
        # Time should be preserved as integer (Unix epoch)
        assert pd.api.types.is_integer_dtype(df["time"])

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    def test_preserves_all_mt5_fields(self, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.return_value = _MOCK_DF.copy()

        collector = MT5Collector(output_dir=tmp_path)
        df = collector._fetch_and_normalise(
            "EURUSD", "H1", datetime(2023, 1, 1), datetime(2024, 1, 1)
        )
        expected = {
            "time",
            "open",
            "high",
            "low",
            "close",
            "tick_volume",
            "spread",
            "real_volume",
            "source",
        }
        assert set(df.columns) == expected


# ---------------------------------------------------------------------------
# MT5Collector — collect
# ---------------------------------------------------------------------------


class TestCollect:
    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    @patch("time.sleep")
    def test_single_pair_timeframe(self, _sleep, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.return_value = _MOCK_DF_2.copy()

        collector = MT5Collector(output_dir=tmp_path, pairs=["EURUSD"], timeframes=["H1"])
        datasets = collector.collect()

        assert "EURUSD_H1" in datasets
        assert len(datasets["EURUSD_H1"]) == 2
        mock_inst.connect.assert_called_once()
        mock_inst.shutdown.assert_called_once()

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    @patch("time.sleep")
    def test_multiple_pairs_timeframes(self, _sleep, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.return_value = _MOCK_DF.copy()

        pairs = ["EURUSD", "GBPUSD"]
        timeframes = ["H1", "H4"]
        collector = MT5Collector(output_dir=tmp_path, pairs=pairs, timeframes=timeframes)
        datasets = collector.collect()

        assert sorted(datasets.keys()) == ["EURUSD_H1", "EURUSD_H4", "GBPUSD_H1", "GBPUSD_H4"]
        for df in datasets.values():
            assert "source" in df.columns
            assert (df["source"] == "mt5").all()

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    @patch("time.sleep")
    def test_partial_failure_continues(self, _sleep, mock_cls, tmp_path):
        """If one symbol fails, the rest are still collected."""
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.side_effect = [
            _MOCK_DF.copy(),  # EURUSD_H1 OK
            RuntimeError("symbol unavailable"),  # EURUSD_H4 FAIL
            _MOCK_DF.copy(),  # GBPUSD_H1 OK
            RuntimeError("timeout"),  # GBPUSD_H4 FAIL
        ]

        collector = MT5Collector(
            output_dir=tmp_path, pairs=["EURUSD", "GBPUSD"], timeframes=["H1", "H4"]
        )
        datasets = collector.collect()

        assert sorted(datasets.keys()) == ["EURUSD_H1", "GBPUSD_H1"]
        assert len(datasets) == 2

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    @patch("time.sleep")
    def test_complete_failure_raises(self, _sleep, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.side_effect = RuntimeError("connection lost")

        collector = MT5Collector(output_dir=tmp_path, pairs=["EURUSD"], timeframes=["H1"])
        with pytest.raises(RuntimeError, match="No data collected"):
            collector.collect()

        mock_inst.shutdown.assert_called_once()

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    @patch("time.sleep")
    def test_shutdown_called_on_exception(self, _sleep, mock_cls, tmp_path):
        """Connector is shut down even if collect raises."""
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.side_effect = RuntimeError("fail")

        collector = MT5Collector(output_dir=tmp_path, pairs=["EURUSD"], timeframes=["H1"])
        with pytest.raises(RuntimeError):
            collector.collect()

        mock_inst.shutdown.assert_called_once()

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    @patch("time.sleep")
    def test_default_date_range(self, _sleep, mock_cls, tmp_path):
        """Default date range is `years` years back from now."""
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.return_value = _MOCK_DF.copy()

        collector = MT5Collector(output_dir=tmp_path, pairs=["EURUSD"], timeframes=["H1"], years=3)
        collector.collect()

        call_args = mock_inst.fetch_ohlc.call_args
        start_arg = call_args[0][2]  # positional: symbol, timeframe, start, end
        end_arg = call_args[0][3]
        days = (end_arg - start_arg).days
        assert 1090 <= days <= 1100  # ~3 * 365

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    @patch("time.sleep")
    def test_custom_date_range(self, _sleep, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.return_value = _MOCK_DF.copy()

        start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, tzinfo=timezone.utc)

        collector = MT5Collector(output_dir=tmp_path, pairs=["EURUSD"], timeframes=["H1"])
        collector.collect(start_date=start, end_date=end)

        call_args = mock_inst.fetch_ohlc.call_args
        assert call_args[0][2] == start
        assert call_args[0][3] == end


# ---------------------------------------------------------------------------
# CSV export integration
# ---------------------------------------------------------------------------


class TestExportIntegration:
    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    @patch("time.sleep")
    def test_export_follows_naming_convention(self, _sleep, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.return_value = _MOCK_DF.copy()

        collector = MT5Collector(output_dir=tmp_path, pairs=["EURUSD"], timeframes=["H1"])
        datasets = collector.collect()

        path = collector.export_csv(datasets["EURUSD_H1"], "EURUSD_H1")

        assert path.exists()
        assert path.name.startswith("mt5_EURUSD_H1_")
        assert path.suffix == ".csv"

    @patch("src.ingestion.collectors.mt5_collector.MT5Connector")
    @patch("time.sleep")
    def test_exported_csv_has_source_column(self, _sleep, mock_cls, tmp_path):
        mock_inst = mock_cls.return_value
        mock_inst.fetch_ohlc.return_value = _MOCK_DF.copy()

        collector = MT5Collector(output_dir=tmp_path, pairs=["EURUSD"], timeframes=["H1"])
        datasets = collector.collect()
        path = collector.export_csv(datasets["EURUSD_H1"], "EURUSD_H1")

        loaded = pd.read_csv(path)
        assert "source" in loaded.columns
        assert loaded["source"].iloc[0] == "mt5"
