"""Unit tests for FRED data collector and macro normalizer."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.ingestion.collectors.fred_collector import FREDCollector, FREDSeries
from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer

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
        monkeypatch.setattr("src.shared.config.Config.FRED_API_KEY", None)
        with pytest.raises(ValueError, match="FRED API key is required"):
            FREDCollector(output_dir=tmp_path)

    def test_init_with_placeholder_api_key_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.shared.config.Config.FRED_API_KEY", "your_fred_api_key_here")
        with pytest.raises(ValueError, match="FRED API key is required"):
            FREDCollector(output_dir=tmp_path)

    def test_default_output_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.shared.config.Config.DATA_DIR", tmp_path)
        monkeypatch.setattr("src.shared.config.Config.FRED_API_KEY", "test_key")
        collector = FREDCollector()
        # Bronze layer: data/raw/fred
        assert collector.output_dir == tmp_path / "raw" / "fred"
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
    def test_health_check_success(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.return_value = SAMPLE_SERIES_INFO_DFF
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        assert collector.health_check() is True
        mock_fred.get_series_info.assert_called_once_with("DFF")

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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
        # Bronze format: date (not timestamp_utc)
        assert list(df.columns) == [
            "date",
            "value",
            "series_id",
            "frequency",
            "units",
            "source",
        ]
        assert df["series_id"].iloc[0] == "DFF"
        assert df["frequency"].iloc[0] == "D"
        assert df["units"].iloc[0] == "Percent"
        assert df["source"].iloc[0] == "fred"
        assert len(df) == 10
        # Verify date format (YYYY-MM-DD, not ISO 8601)
        assert df["date"].iloc[0] == "2023-01-01"

    @patch("src.ingestion.collectors.fred_collector.Fred")
    def test_get_series_invalid_id(self, mock_fred_class, tmp_path):
        mock_fred = Mock()
        mock_fred.get_series_info.side_effect = ValueError("Bad series ID")
        mock_fred_class.return_value = mock_fred

        collector = FREDCollector(api_key="test_key", output_dir=tmp_path)
        with pytest.raises(ValueError, match="Invalid FRED series ID"):
            collector.get_series("INVALID_ID", use_cache=False)

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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
            # Bronze format: date (not timestamp_utc)
            assert "date" in df.columns
            assert "value" in df.columns
            assert "source" in df.columns

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

    @patch("src.ingestion.collectors.fred_collector.Fred")
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

        # 4. Verify files (Bronze format)
        for path in paths.values():
            assert path.exists()
            df = pd.read_csv(path)
            assert not df.empty
            assert "date" in df.columns
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


# ---------------------------------------------------------------------------
# MacroNormalizer (Silver Layer Preprocessing)
# ---------------------------------------------------------------------------


class TestMacroNormalizer:
    """Test MacroNormalizer preprocessor (Bronze → Silver)."""

    def test_initialization(self, tmp_path):
        """Test MacroNormalizer initializes correctly."""
        input_dir = tmp_path / "raw"  # Root raw directory
        output_dir = tmp_path / "processed" / "macro"
        input_dir.mkdir(parents=True)

        normalizer = MacroNormalizer(input_dir=input_dir, output_dir=output_dir, sources=["fred"])
        assert normalizer.CATEGORY == "macro"
        assert normalizer.input_dir == input_dir
        assert normalizer.output_dir == output_dir
        assert output_dir.exists()

    def test_preprocess_bronze_to_silver(self, tmp_path):
        """Test Bronze → Silver transformation."""
        # Setup directories (root raw dir)
        input_dir = tmp_path / "raw"
        fred_dir = input_dir / "fred"
        output_dir = tmp_path / "processed" / "macro"
        fred_dir.mkdir(parents=True)

        # Create Bronze file
        bronze_data = pd.DataFrame(
            {
                "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
                "value": [3.5, 3.6, 3.7],
                "series_id": ["DFF", "DFF", "DFF"],
                "frequency": ["D", "D", "D"],
                "units": ["Percent", "Percent", "Percent"],
                "source": ["fred", "fred", "fred"],
            }
        )
        bronze_file = fred_dir / "fred_federal_funds_rate_20260210.csv"
        bronze_data.to_csv(bronze_file, index=False)

        # Process
        normalizer = MacroNormalizer(input_dir=input_dir, output_dir=output_dir, sources=["fred"])
        result = normalizer.preprocess()

        assert len(result) == 1
        assert "DFF" in result

        df = result["DFF"]
        # Verify Silver schema
        assert list(df.columns) == [
            "timestamp_utc",
            "series_id",
            "value",
            "source",
            "frequency",
            "units",
        ]
        assert len(df) == 3
        # Verify UTC timestamp format
        assert df["timestamp_utc"].iloc[0] == "2023-01-01T00:00:00Z"
        assert df["series_id"].iloc[0] == "DFF"

    def test_validate_silver_schema(self, tmp_path):
        """Test Silver schema validation."""
        input_dir = tmp_path / "raw"
        output_dir = tmp_path / "processed" / "macro"
        input_dir.mkdir(parents=True)

        normalizer = MacroNormalizer(input_dir=input_dir, output_dir=output_dir, sources=["fred"])

        # Valid Silver data
        valid_df = pd.DataFrame(
            {
                "timestamp_utc": ["2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"],
                "series_id": ["DFF", "DFF"],
                "value": [3.5, 3.6],
                "source": ["fred", "fred"],
                "frequency": ["D", "D"],
                "units": ["Percent", "Percent"],
            }
        )
        assert normalizer.validate(valid_df) is True

        # Invalid: missing column
        invalid_df = valid_df.drop(columns=["units"])
        with pytest.raises(ValueError, match="Missing required columns"):
            normalizer.validate(invalid_df)

        # Invalid: non-numeric value
        invalid_df = valid_df.copy()
        invalid_df["value"] = ["not_a_number", "3.6"]
        with pytest.raises(ValueError, match="value column must be numeric"):
            normalizer.validate(invalid_df)

    def test_process_and_export(self, tmp_path):
        """Test end-to-end process_and_export with consolidated output (default)."""
        input_dir = tmp_path / "raw"
        fred_dir = input_dir / "fred"
        output_dir = tmp_path / "processed" / "macro"
        fred_dir.mkdir(parents=True)

        # Create Bronze file
        bronze_data = pd.DataFrame(
            {
                "date": ["2023-01-01", "2023-01-02"],
                "value": [3.5, 3.6],
                "series_id": ["DFF", "DFF"],
                "frequency": ["D", "D"],
                "units": ["Percent", "Percent"],
                "source": ["fred", "fred"],
            }
        )
        bronze_file = fred_dir / "fred_federal_funds_rate_20260210.csv"
        bronze_data.to_csv(bronze_file, index=False)

        # Process and export (consolidated by default)
        normalizer = MacroNormalizer(input_dir=input_dir, output_dir=output_dir, sources=["fred"])
        paths = normalizer.process_and_export(consolidated=True)

        assert len(paths) == 1
        assert "all" in paths

        silver_file = paths["all"]
        assert silver_file.exists()
        assert "macro_all_" in silver_file.name

        # Verify Silver file content
        df = pd.read_csv(silver_file)
        assert len(df) == 2
        assert "timestamp_utc" in df.columns
        assert "series_id" in df.columns
        assert df["series_id"].iloc[0] == "DFF"
        assert df["timestamp_utc"].iloc[0] == "2023-01-01T00:00:00Z"

    def test_handles_duplicates(self, tmp_path):
        """Test that duplicates are removed."""
        input_dir = tmp_path / "raw"
        fred_dir = input_dir / "fred"
        output_dir = tmp_path / "processed" / "macro"
        fred_dir.mkdir(parents=True)

        # Create Bronze file with duplicates
        bronze_data = pd.DataFrame(
            {
                "date": ["2023-01-01", "2023-01-01", "2023-01-02"],  # Duplicate date
                "value": [3.5, 3.5, 3.6],
                "series_id": ["DFF", "DFF", "DFF"],
                "frequency": ["D", "D", "D"],
                "units": ["Percent", "Percent", "Percent"],
                "source": ["fred", "fred", "fred"],
            }
        )
        bronze_file = fred_dir / "fred_dff_20260210.csv"
        bronze_data.to_csv(bronze_file, index=False)

        normalizer = MacroNormalizer(input_dir=input_dir, output_dir=output_dir, sources=["fred"])
        result = normalizer.preprocess()

        df = result["DFF"]
        # Should only have 2 records (duplicate removed)
        assert len(df) == 2

    def test_no_bronze_files(self, tmp_path):
        """Test behavior when no Bronze files exist."""
        input_dir = tmp_path / "raw"
        fred_dir = input_dir / "fred"
        output_dir = tmp_path / "processed" / "macro"
        fred_dir.mkdir(parents=True)

        normalizer = MacroNormalizer(input_dir=input_dir, output_dir=output_dir, sources=["fred"])
        result = normalizer.preprocess()

        assert result == {}
