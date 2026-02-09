"""Unit tests for FRED data collector."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from data.ingestion.fred_collector import FREDCollector, FREDSeries

# ---------------------------------------------------------------------------
# Sample Data
# ---------------------------------------------------------------------------

SAMPLE_SERIES_INFO_DFF = {
    "id": "DFF",
    "title": "Federal Funds Effective Rate",
    "observation_start": "1954-07-01",
    "observation_end": "2024-12-31",
    "frequency": "Daily",
    "frequency_short": "D",
    "units": "Percent",
    "units_short": "Percent",
    "seasonal_adjustment": "Not Seasonally Adjusted",
    "seasonal_adjustment_short": "NSA",
}

SAMPLE_SERIES_INFO_UNRATE = {
    "id": "UNRATE",
    "title": "Unemployment Rate",
    "observation_start": "1948-01-01",
    "observation_end": "2024-12-31",
    "frequency": "Monthly",
    "frequency_short": "M",
    "units": "Percent",
    "units_short": "Percent",
    "seasonal_adjustment": "Seasonally Adjusted",
    "seasonal_adjustment_short": "SA",
}

SAMPLE_SERIES_INFO_STLFSI4 = {
    "id": "STLFSI4",
    "title": "St. Louis Fed Financial Stress Index",
    "observation_start": "1993-12-31",
    "observation_end": "2024-12-31",
    "frequency": "Weekly, Ending Friday",
    "frequency_short": "W",
    "units": "Index",
    "units_short": "Index",
    "seasonal_adjustment": "Not Seasonally Adjusted",
    "seasonal_adjustment_short": "NSA",
}


def make_sample_series_data(start_date: datetime, end_date: datetime, freq: str = "D") -> pd.Series:
    """Create sample FRED series data."""
    date_range = pd.date_range(start=start_date, end=end_date, freq=freq)
    values = [3.5 + i * 0.01 for i in range(len(date_range))]
    return pd.Series(values, index=date_range)


# ---------------------------------------------------------------------------
# FREDSeries dataclass
# ---------------------------------------------------------------------------


class TestFREDSeries:
    """Test FREDSeries dataclass."""

    def test_frozen(self):
        series = FREDCollector.FEDERAL_FUNDS_RATE
        with pytest.raises(Exception):
            series.series_id = "CHANGED"  # type: ignore[misc]

    def test_predefined_series(self):
        assert FREDCollector.FINANCIAL_STRESS.series_id == "STLFSI4"
        assert FREDCollector.FEDERAL_FUNDS_RATE.series_id == "DFF"
        assert FREDCollector.CPI.series_id == "CPIAUCSL"
        assert FREDCollector.UNEMPLOYMENT_RATE.series_id == "UNRATE"

    def test_all_series_tuple(self):
        assert len(FREDCollector._ALL_SERIES) == 4
        assert all(isinstance(s, FREDSeries) for s in FREDCollector._ALL_SERIES)


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestFREDCollectorInit:
    """Test FREDCollector initialization."""

    def test_init_with_api_key(self, tmp_path):
        collector = FREDCollector(api_key="test_key_12345", output_dir=tmp_path)
        assert collector._api_key == "test_key_12345"
        assert collector.output_dir == tmp_path
        assert collector._cache_dir.exists()

    def test_init_without_api_key_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr("shared.config.Config.FRED_API_KEY", None)
        with pytest.raises(ValueError, match="FRED API key is required"):
            FREDCollector(output_dir=tmp_path)

    def test_init_with_placeholder_api_key_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr("shared.config.Config.FRED_API_KEY", "your_fred_api_key_here")
        with pytest.raises(ValueError, match="FRED API key is required"):
            FREDCollector(output_dir=tmp_path)

    def test_default_output_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("shared.config.Config.DATA_DIR", tmp_path)
        monkeypatch.setattr("shared.config.Config.FRED_API_KEY", "test_key")
        collector = FREDCollector()
        assert collector.output_dir == tmp_path / "datasets" / "macro"
        assert collector._cache_dir == tmp_path / "cache" / "fred"

    def test_custom_cache_dir(self, tmp_path):
        cache_dir = tmp_path / "custom_cache"
        collector = FREDCollector(api_key="test_key", output_dir=tmp_path, cache_dir=cache_dir)
        assert collector._cache_dir == cache_dir
        assert cache_dir.exists()


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    """Test health_check method."""

    @patch("data.ingestion.fred_collector.Fred")
    def test_health_check_success(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        assert collector.health_check() is True
        mock_fred.get_series_info.assert_called_once_with("DFF")

    @patch("data.ingestion.fred_collector.Fred")
    def test_health_check_failure(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.side_effect = Exception("API Error")
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        assert collector.health_check() is False


# ---------------------------------------------------------------------------
# get_series
# ---------------------------------------------------------------------------


class TestGetSeries:
    """Test get_series method."""

    @patch("data.ingestion.fred_collector.Fred")
    def test_get_series_success(self, mock_fred_class, tmp_path):
        # Setup mock
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        df = collector.get_series("DFF", start_date=start, end_date=end, use_cache=False)

        assert not df.empty
        assert list(df.columns) == [
            "timestamp_utc",
            "series_id",
            "value",
            "source",
            "frequency",
            "units",
        ]
        assert df["series_id"].iloc[0] == "DFF"
        assert df["frequency"].iloc[0] == "D"
        assert df["units"].iloc[0] == "Percent"
        assert df["source"].iloc[0] == "fred"
        assert len(df) == 10

    @patch("data.ingestion.fred_collector.Fred")
    def test_get_series_invalid_id(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.side_effect = ValueError("Bad series ID")
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        with pytest.raises(ValueError, match="Invalid FRED series ID"):
            collector.get_series("INVALID_ID", use_cache=False)

    @patch("data.ingestion.fred_collector.Fred")
    def test_get_series_default_dates(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start = datetime.now() - timedelta(days=730)
        end = datetime.now()
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        df = collector.get_series("DFF", use_cache=False)

        assert not df.empty
        # Verify the API was called with default date range (2 years)
        call_args = mock_fred.get_series.call_args
        assert "observation_start" in call_args.kwargs
        assert "observation_end" in call_args.kwargs

    @patch("data.ingestion.fred_collector.Fred")
    def test_get_series_empty_response(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        mock_fred.get_series.return_value = pd.Series(dtype=float)
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        df = collector.get_series("DFF", use_cache=False)

        assert df.empty


# ---------------------------------------------------------------------------
# get_multiple_series
# ---------------------------------------------------------------------------


class TestGetMultipleSeries:
    """Test get_multiple_series method."""

    @patch("data.ingestion.fred_collector.Fred")
    def test_get_multiple_series_success(self, mock_fred_class, tmp_path):
        mock_fred = Mock()

        def mock_get_info(series_id):
            return {
                "DFF": SAMPLE_SERIES_INFO_DFF,
                "UNRATE": SAMPLE_SERIES_INFO_UNRATE,
            }[series_id]

        mock_fred.get_series_info.side_effect = mock_get_info
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        data = collector.get_multiple_series(
            ["DFF", "UNRATE"], start_date=start, end_date=end, use_cache=False
        )

        assert len(data) == 2
        assert "DFF" in data
        assert "UNRATE" in data
        assert not data["DFF"].empty
        assert not data["UNRATE"].empty

    @patch("data.ingestion.fred_collector.Fred")
    def test_get_multiple_series_partial_failure(self, mock_fred_class, tmp_path):
        mock_fred = Mock()

        def mock_get_info(series_id):
            if series_id == "DFF":
                return SAMPLE_SERIES_INFO_DFF
            raise ValueError("Bad series ID")

        mock_fred.get_series_info.side_effect = mock_get_info
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        data = collector.get_multiple_series(
            ["DFF", "INVALID"], start_date=start, end_date=end, use_cache=False
        )

        # Should return only successful series
        assert len(data) == 1
        assert "DFF" in data
        assert "INVALID" not in data


# ---------------------------------------------------------------------------
# collect (main interface)
# ---------------------------------------------------------------------------


class TestCollect:
    """Test collect method."""

    @patch("data.ingestion.fred_collector.Fred")
    def test_collect_all_series(self, mock_fred_class, tmp_path):
        mock_fred = Mock()

        def mock_get_info(series_id):
            return {
                "STLFSI4": SAMPLE_SERIES_INFO_STLFSI4,
                "DFF": SAMPLE_SERIES_INFO_DFF,
                "CPIAUCSL": {
                    "id": "CPIAUCSL",
                    "title": "Consumer Price Index",
                    "frequency_short": "M",
                    "units": "Index",
                },
                "UNRATE": SAMPLE_SERIES_INFO_UNRATE,
            }[series_id]

        mock_fred.get_series_info.side_effect = mock_get_info
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)

        def mock_get_series(series_id, observation_start, observation_end):
            freq = "D" if series_id == "DFF" else "W"
            return make_sample_series_data(start, end, freq=freq)

        mock_fred.get_series.side_effect = mock_get_series
        mock_fred_class.return_value = mock_fred

        # Use tmp_path for cache to avoid interference
        cache_dir = tmp_path / "cache"
        collector = FREDCollector(api_key="test_key", output_dir=tmp_path, cache_dir=cache_dir)
        data = collector.collect(start_date=start, end_date=end)

        assert len(data) == 4
        assert "financial_stress" in data
        assert "federal_funds_rate" in data
        assert "cpi" in data
        assert "unemployment_rate" in data

        for name, df in data.items():
            assert not df.empty
            assert "timestamp_utc" in df.columns
            assert "value" in df.columns
            assert "source" in df.columns

    @patch("data.ingestion.fred_collector.Fred")
    def test_collect_default_date_range(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start = datetime.now() - timedelta(days=730)
        end = datetime.now()
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        data = collector.collect()

        assert len(data) > 0


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


class TestCaching:
    """Test caching functionality."""

    @patch("data.ingestion.fred_collector.Fred")
    def test_cache_saves_and_loads(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        cache_dir = tmp_path / "cache"
        collector = FREDCollector(api_key="test_key", output_dir=tmp_path, cache_dir=cache_dir)

        # First call - should hit API
        df1 = collector.get_series("DFF", start_date=start, end_date=end, use_cache=True)
        assert mock_fred.get_series.call_count == 1

        # Verify cache file exists
        cache_file = cache_dir / "DFF.json"
        assert cache_file.exists()

        # Second call - should use cache
        df2 = collector.get_series("DFF", start_date=start, end_date=end, use_cache=True)
        assert mock_fred.get_series.call_count == 1  # No additional API call

        # DataFrames should be equivalent
        pd.testing.assert_frame_equal(df1, df2)

    @patch("data.ingestion.fred_collector.Fred")
    def test_cache_respects_date_range(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start1 = datetime(2023, 1, 1)
        end1 = datetime(2023, 1, 10)
        start2 = datetime(2023, 1, 5)
        end2 = datetime(2023, 1, 15)

        mock_fred.get_series.side_effect = [
            make_sample_series_data(start1, end1),
            make_sample_series_data(start2, end2),
        ]
        mock_fred_class.return_value = mock_fred

        cache_dir = tmp_path / "cache"
        collector = FREDCollector(api_key="test_key", output_dir=tmp_path, cache_dir=cache_dir)

        # First call
        collector.get_series("DFF", start_date=start1, end_date=end1)
        assert mock_fred.get_series.call_count == 1

        # Second call with different range - cache doesn't cover it
        collector.get_series("DFF", start_date=start2, end_date=end2)
        assert mock_fred.get_series.call_count == 2

    @patch("data.ingestion.fred_collector.Fred")
    def test_clear_cache_single_series(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        cache_dir = tmp_path / "cache"
        collector = FREDCollector(api_key="test_key", output_dir=tmp_path, cache_dir=cache_dir)

        # Create cache
        collector.get_series("DFF", start_date=start, end_date=end)
        cache_file = cache_dir / "DFF.json"
        assert cache_file.exists()

        # Clear cache
        collector.clear_cache("DFF")
        assert not cache_file.exists()

    @patch("data.ingestion.fred_collector.Fred")
    def test_clear_all_cache(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        cache_dir = tmp_path / "cache"
        collector = FREDCollector(api_key="test_key", output_dir=tmp_path, cache_dir=cache_dir)

        # Create multiple cache files
        collector.get_series("DFF", start_date=start, end_date=end)
        collector.get_series("UNRATE", start_date=start, end_date=end)

        assert (cache_dir / "DFF.json").exists()
        assert (cache_dir / "UNRATE.json").exists()

        # Clear all cache
        collector.clear_cache()
        assert not (cache_dir / "DFF.json").exists()
        assert not (cache_dir / "UNRATE.json").exists()

    @patch("data.ingestion.fred_collector.Fred")
    def test_cache_bypass_when_disabled(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)

        # Two calls with use_cache=False
        collector.get_series("DFF", start_date=start, end_date=end, use_cache=False)
        collector.get_series("DFF", start_date=start, end_date=end, use_cache=False)

        # Should have made 2 API calls
        assert mock_fred.get_series.call_count == 2


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------


class TestCSVExport:
    """Test CSV export functionality."""

    @patch("data.ingestion.fred_collector.Fred")
    def test_export_all_to_csv(self, mock_fred_class, tmp_path):
        mock_fred = Mock()

        def mock_get_info(series_id):
            return {
                "STLFSI4": SAMPLE_SERIES_INFO_STLFSI4,
                "DFF": SAMPLE_SERIES_INFO_DFF,
                "CPIAUCSL": {"frequency_short": "M", "units": "Index"},
                "UNRATE": SAMPLE_SERIES_INFO_UNRATE,
            }[series_id]

        mock_fred.get_series_info.side_effect = mock_get_info
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)
        mock_fred.get_series.return_value = make_sample_series_data(start, end, freq="D")
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        paths = collector.export_all_to_csv(start_date=start, end_date=end)

        assert len(paths) == 4
        for name, path in paths.items():
            assert path.exists()
            assert path.suffix == ".csv"
            assert path.name.startswith("fred_")
            # Verify file is readable
            df = pd.read_csv(path)
            assert not df.empty


# ---------------------------------------------------------------------------
# Rate Limiting
# ---------------------------------------------------------------------------


class TestRateLimiting:
    """Test rate limiting functionality."""

    @patch("data.ingestion.fred_collector.Fred")
    @patch("time.time")
    @patch("time.sleep")
    def test_throttle_request(self, mock_sleep, mock_time, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 10)
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        # Simulate rapid consecutive calls
        # Need enough values for: throttle check + throttle update per request
        mock_time.side_effect = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]  # Time progresses

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        collector._last_request_time = 0.0

        # Make two rapid requests
        collector.get_series("DFF", start_date=start, end_date=end, use_cache=False)
        collector.get_series("DFF", start_date=start, end_date=end, use_cache=False)

        # Should have slept to respect rate limit
        assert mock_sleep.called


# ---------------------------------------------------------------------------
# Integration-like tests
# ---------------------------------------------------------------------------


class TestIntegration:
    """Integration-like tests with realistic scenarios."""

    @patch("data.ingestion.fred_collector.Fred")
    def test_typical_workflow(self, mock_fred_class, tmp_path):
        """Test a typical collection workflow."""
        mock_fred = Mock()

        def mock_get_info(series_id):
            return {
                "STLFSI4": SAMPLE_SERIES_INFO_STLFSI4,
                "DFF": SAMPLE_SERIES_INFO_DFF,
                "CPIAUCSL": {
                    "id": "CPIAUCSL",
                    "title": "Consumer Price Index",
                    "frequency_short": "M",
                    "units": "Index",
                },
                "UNRATE": SAMPLE_SERIES_INFO_UNRATE,
            }[series_id]

        mock_fred.get_series_info.side_effect = mock_get_info
        start = datetime(2023, 1, 1)
        end = datetime(2023, 12, 31)

        def mock_get_series_data(series_id, observation_start, observation_end):
            freq = "ME" if series_id in ["UNRATE", "CPIAUCSL"] else "D"
            return make_sample_series_data(
                datetime.fromisoformat(observation_start),
                datetime.fromisoformat(observation_end),
                freq=freq,
            )

        mock_fred.get_series.side_effect = mock_get_series_data
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)

        # 1. Health check
        assert collector.health_check()

        # 2. Collect all data
        data = collector.collect(start_date=start, end_date=end)
        assert len(data) == 4

        # 3. Export to CSV
        paths = collector.export_all_to_csv(data=data)
        assert len(paths) == 4

        # 4. Verify files
        for path in paths.values():
            assert path.exists()
            df = pd.read_csv(path)
            assert not df.empty
            assert "timestamp_utc" in df.columns
            assert "value" in df.columns
            assert "source" in df.columns


class TestDateValidation:
    """Test date range validation."""

    def test_collect_invalid_date_range(self):
        """Test collect() rejects start_date after end_date."""
        collector = FREDCollector(api_key="test_key")
        start = datetime(2023, 12, 31)
        end = datetime(2023, 1, 1)

        with pytest.raises(ValueError, match="must be before"):
            collector.collect(start_date=start, end_date=end)

    def test_get_series_invalid_date_range(self):
        """Test get_series() rejects start_date after end_date."""
        collector = FREDCollector(api_key="test_key")
        start = datetime(2023, 12, 31)
        end = datetime(2023, 1, 1)

        with pytest.raises(ValueError, match="must be before"):
            collector.get_series("DFF", start_date=start, end_date=end)

    def test_get_multiple_series_invalid_date_range(self):
        """Test get_multiple_series() rejects start_date after end_date."""
        collector = FREDCollector(api_key="test_key")
        start = datetime(2023, 12, 31)
        end = datetime(2023, 1, 1)

        with pytest.raises(ValueError, match="must be before"):
            collector.get_multiple_series([" DFF", "UNRATE"], start_date=start, end_date=end)

    def test_get_series_empty_series_id(self):
        """Test get_series() rejects empty series_id."""
        collector = FREDCollector(api_key="test_key")

        with pytest.raises(ValueError, match="cannot be empty"):
            collector.get_series("")

        with pytest.raises(ValueError, match="cannot be empty"):
            collector.get_series("   ")

    def test_get_multiple_series_empty_list(self):
        """Test get_multiple_series() rejects empty series list."""
        collector = FREDCollector(api_key="test_key")

        with pytest.raises(ValueError, match="cannot be empty"):
            collector.get_multiple_series([])


class TestConsolidatedExport:
    """Test consolidated CSV export."""

    @patch("data.ingestion.fred_collector.Fred")
    def test_export_consolidated_csv(self, mock_fred_class, tmp_path):
        """Test exporting all series to a single consolidated CSV."""
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 5)
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)

        # Collect data
        data = {
            "financial_stress": pd.DataFrame(
                {
                    "timestamp_utc": pd.date_range(start, end, freq="D").strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "series_id": "STLFSI4",
                    "value": [1.0, 1.1, 1.2, 1.3, 1.4],
                    "source": "fred",
                    "frequency": "D",
                    "units": "Index",
                }
            ),
            "federal_funds_rate": pd.DataFrame(
                {
                    "timestamp_utc": pd.date_range(start, end, freq="D").strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "series_id": "DFF",
                    "value": [3.5, 3.6, 3.7, 3.8, 3.9],
                    "source": "fred",
                    "frequency": "D",
                    "units": "Percent",
                }
            ),
        }

        # Export consolidated
        path = collector.export_consolidated_csv(data)

        # Verify file exists
        assert path.exists()
        assert "fred_indicators" in path.name
        assert path.suffix == ".csv"

        # Verify content
        df = pd.read_csv(path)
        assert len(df) == 10  # 5 rows from each series
        assert "series_id" in df.columns
        assert set(df["series_id"].unique()) == {"STLFSI4", "DFF"}
        assert "timestamp_utc" in df.columns
        assert "value" in df.columns
        assert "source" in df.columns

    def test_export_consolidated_csv_no_data(self, tmp_path):
        """Test export_consolidated_csv() with no data raises error."""
        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)

        with pytest.raises(ValueError, match="No data to export"):
            collector.export_consolidated_csv(data={})

    def test_export_consolidated_csv_empty_dataframes(self, tmp_path):
        """Test export_consolidated_csv() with all empty dataframes raises error."""
        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        data = {
            "series1": pd.DataFrame(),
            "series2": pd.DataFrame(),
        }

        with pytest.raises(ValueError, match="All dataframes are empty"):
            collector.export_consolidated_csv(data=data)

    @patch("data.ingestion.fred_collector.Fred")
    def test_export_consolidated_csv_auto_collect(self, mock_fred_class, tmp_path):
        """Test export_consolidated_csv() auto-collects data when none provided."""
        mock_fred = Mock()
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 3)

        def mock_get_info(series_id):
            return SAMPLE_SERIES_INFO_DFF

        mock_fred.get_series_info.side_effect = mock_get_info
        mock_fred.get_series.return_value = make_sample_series_data(start, end)
        mock_fred_class.return_value = mock_fred

        cache_dir = tmp_path / "cache"
        collector = FREDCollector(api_key="test_key", output_dir=tmp_path, cache_dir=cache_dir)

        # Export without providing data (should auto-collect)
        path = collector.export_consolidated_csv(
            start_date=start, end_date=end, filename="custom_indicators"
        )

        assert path.exists()
        assert path.name.startswith("custom_indicators_")
