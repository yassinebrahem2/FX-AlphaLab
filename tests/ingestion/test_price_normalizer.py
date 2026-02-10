"""Unit tests for PriceNormalizer (Bronze → Silver OHLCV transformation)."""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.preprocessors.price_normalizer import PriceNormalizer

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def bronze_data() -> pd.DataFrame:
    """Sample Bronze MT5 data (raw format with Unix timestamps)."""
    return pd.DataFrame(
        {
            "time": [1609459200, 1609462800, 1609466400],  # Unix epoch
            "open": [1.2200, 1.2210, 1.2205],
            "high": [1.2250, 1.2260, 1.2255],
            "low": [1.2190, 1.2200, 1.2195],
            "close": [1.2230, 1.2240, 1.2235],
            "tick_volume": [1000, 1500, 1200],
            "spread": [2, 2, 2],
            "real_volume": [0, 0, 0],
            "source": ["mt5", "mt5", "mt5"],
        }
    )


@pytest.fixture
def bronze_csv_file(tmp_path: Path, bronze_data: pd.DataFrame) -> Path:
    """Create a Bronze CSV file in the expected format."""
    bronze_dir = tmp_path / "raw" / "mt5"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    csv_path = bronze_dir / "mt5_EURUSD_H1_20210101.csv"
    bronze_data.to_csv(csv_path, index=False)

    return csv_path


@pytest.fixture
def normalizer(tmp_path: Path) -> PriceNormalizer:
    """PriceNormalizer with temp input/output directories."""
    input_dir = tmp_path / "raw"  # Root raw dir (normalizer looks for source subdirs)
    output_dir = tmp_path / "processed" / "ohlcv"
    input_dir.mkdir(parents=True, exist_ok=True)

    return PriceNormalizer(input_dir=input_dir, output_dir=output_dir, sources=["mt5"])


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestPriceNormalizerInit:
    def test_default_configuration(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.shared.config.Config.DATA_DIR", tmp_path)
        normalizer = PriceNormalizer()

        assert normalizer.CATEGORY == "ohlcv"
        assert normalizer.input_dir == tmp_path / "raw"  # Root raw dir
        assert normalizer.output_dir == tmp_path / "processed" / "ohlcv"
        assert normalizer.sources == ["mt5", "ecb"]  # Default sources

    def test_custom_configuration(self, tmp_path):
        input_dir = tmp_path / "custom_input"
        output_dir = tmp_path / "custom_output"

        normalizer = PriceNormalizer(input_dir=input_dir, output_dir=output_dir)

        assert normalizer.input_dir == input_dir
        assert normalizer.output_dir == output_dir

    def test_creates_output_directory(self, tmp_path):
        output_dir = tmp_path / "processed" / "ohlcv"
        assert not output_dir.exists()

        PriceNormalizer(input_dir=tmp_path, output_dir=output_dir)

        assert output_dir.exists()


# ---------------------------------------------------------------------------
# Transform to Silver
# ---------------------------------------------------------------------------


class TestTransformToSilver:
    def test_converts_unix_time_to_utc_timestamp(self, normalizer, bronze_data):
        result = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")

        assert "timestamp_utc" in result.columns
        assert "time" not in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp_utc"])

        # Check first timestamp conversion (1609459200 = 2021-01-01 00:00:00 UTC)
        expected = pd.Timestamp("2021-01-01 00:00:00", tz="UTC")
        assert result["timestamp_utc"].iloc[0] == expected

    def test_adds_pair_and_timeframe_columns(self, normalizer, bronze_data):
        result = normalizer._transform_mt5_to_silver(bronze_data, "GBPUSD", "H4")

        assert "pair" in result.columns
        assert "timeframe" in result.columns
        assert (result["pair"] == "GBPUSD").all()
        assert (result["timeframe"] == "H4").all()

    def test_renames_tick_volume_to_volume(self, normalizer, bronze_data):
        result = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")

        assert "volume" in result.columns
        assert "tick_volume" not in result.columns
        assert result["volume"].iloc[0] == 1000

    def test_drops_mt5_specific_fields(self, normalizer, bronze_data):
        result = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")

        assert "spread" not in result.columns
        assert "real_volume" not in result.columns

    def test_preserves_ohlc_fields(self, normalizer, bronze_data):
        result = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")

        assert "open" in result.columns
        assert "high" in result.columns
        assert "low" in result.columns
        assert "close" in result.columns
        assert result["open"].iloc[0] == 1.2200

    def test_preserves_source_column(self, normalizer, bronze_data):
        result = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")

        assert "source" in result.columns
        assert (result["source"] == "mt5").all()

    def test_deduplicates_by_timestamp(self, normalizer):
        # Create data with duplicate timestamps
        df = pd.DataFrame(
            {
                "time": [1609459200, 1609459200, 1609462800],  # First two are duplicates
                "open": [1.22, 1.23, 1.24],
                "high": [1.25, 1.26, 1.27],
                "low": [1.21, 1.22, 1.23],
                "close": [1.23, 1.24, 1.25],
                "tick_volume": [1000, 1500, 1200],
                "source": ["mt5", "mt5", "mt5"],
            }
        )

        result = normalizer._transform_mt5_to_silver(df, "EURUSD", "H1")

        assert len(result) == 2  # Duplicate removed
        assert result["open"].iloc[0] == 1.22  # Kept first occurrence

    def test_sorts_by_timestamp(self, normalizer):
        # Create data with unsorted timestamps
        df = pd.DataFrame(
            {
                "time": [1609466400, 1609459200, 1609462800],  # Out of order
                "open": [1.22, 1.23, 1.24],
                "high": [1.25, 1.26, 1.27],
                "low": [1.21, 1.22, 1.23],
                "close": [1.23, 1.24, 1.25],
                "tick_volume": [1000, 1500, 1200],
                "source": ["mt5", "mt5", "mt5"],
            }
        )

        result = normalizer._transform_mt5_to_silver(df, "EURUSD", "H1")

        # Check timestamps are now sorted
        timestamps = result["timestamp_utc"].tolist()
        assert timestamps == sorted(timestamps)

    def test_all_required_columns_present(self, normalizer, bronze_data):
        result = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")

        expected_columns = [
            "timestamp_utc",
            "pair",
            "timeframe",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
        ]
        assert list(result.columns) == expected_columns


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_valid_dataframe_passes(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        assert normalizer.validate(df_silver) is True

    def test_empty_dataframe_raises(self, normalizer):
        df_empty = pd.DataFrame()

        with pytest.raises(ValueError, match="DataFrame is empty"):
            normalizer.validate(df_empty)

    def test_missing_required_columns_raises(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        df_silver.drop(columns=["pair"], inplace=True)

        with pytest.raises(ValueError, match="Missing required columns: {'pair'}"):
            normalizer.validate(df_silver)

    def test_missing_values_in_critical_fields_raises(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        df_silver.loc[0, "open"] = pd.NA

        with pytest.raises(ValueError, match="Missing values found in 'open'"):
            normalizer.validate(df_silver)

    def test_invalid_timestamp_type_raises(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        df_silver["timestamp_utc"] = "invalid"  # Completely invalid, not convertible

        with pytest.raises(ValueError, match="timestamp_utc must be datetime type"):
            normalizer.validate(df_silver)

    def test_ohlc_consistency_high_too_low_raises(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        df_silver.loc[0, "high"] = 1.2180  # Below low/open/close

        with pytest.raises(ValueError, match="OHLC consistency violation.*invalid high prices"):
            normalizer.validate(df_silver)

    def test_ohlc_consistency_low_too_high_raises(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        # Set low above everything - this will fail both high and low validations
        # But high is checked first in the validator
        df_silver.loc[0, "low"] = 1.2260  # Above high/open/close

        with pytest.raises(ValueError, match="OHLC consistency violation"):
            normalizer.validate(df_silver)

    def test_duplicate_timestamps_raises(self, normalizer):
        # Create valid Silver data with duplicate timestamps
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.to_datetime(
                    ["2021-01-01", "2021-01-01", "2021-01-02"], utc=True
                ),
                "pair": ["EURUSD", "EURUSD", "EURUSD"],
                "timeframe": ["H1", "H1", "H1"],
                "open": [1.22, 1.23, 1.24],
                "high": [1.25, 1.26, 1.27],
                "low": [1.21, 1.22, 1.23],
                "close": [1.23, 1.24, 1.25],
                "volume": [1000, 1500, 1200],
                "source": ["mt5", "mt5", "mt5"],
            }
        )

        with pytest.raises(ValueError, match="Found 2 duplicate timestamp records"):
            normalizer.validate(df)


# ---------------------------------------------------------------------------
# Preprocess (end-to-end)
# ---------------------------------------------------------------------------


class TestPreprocess:
    def test_processes_single_csv_file(self, normalizer, bronze_csv_file):
        results = normalizer.preprocess()

        assert "EURUSD_H1" in results
        df = results["EURUSD_H1"]

        assert len(df) == 3
        assert "timestamp_utc" in df.columns
        assert (df["pair"] == "EURUSD").all()
        assert (df["timeframe"] == "H1").all()

    def test_processes_multiple_csv_files(self, tmp_path):
        # PriceNormalizer expects input_dir to be the "raw" dir, then looks for "mt5" subdir
        input_dir = tmp_path / "raw"
        mt5_dir = input_dir / "mt5"
        mt5_dir.mkdir(parents=True, exist_ok=True)

        # Create multiple Bronze files
        for pair, timeframe in [("EURUSD", "H1"), ("GBPUSD", "H4"), ("USDJPY", "D1")]:
            df = pd.DataFrame(
                {
                    "time": [1609459200],
                    "open": [1.22],
                    "high": [1.25],
                    "low": [1.21],
                    "close": [1.23],
                    "tick_volume": [1000],
                    "spread": [2],
                    "real_volume": [0],
                    "source": ["mt5"],
                }
            )
            csv_path = mt5_dir / f"mt5_{pair}_{timeframe}_20210101.csv"
            df.to_csv(csv_path, index=False)

        normalizer = PriceNormalizer(
            input_dir=input_dir, output_dir=tmp_path / "processed", sources=["mt5"]
        )
        results = normalizer.preprocess()

        assert len(results) == 3
        assert "EURUSD_H1" in results
        assert "GBPUSD_H4" in results
        assert "USDJPY_D1" in results

    def test_filters_by_start_date(self, normalizer, bronze_csv_file):
        start = datetime(2021, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        results = normalizer.preprocess(start_date=start)

        df = results["EURUSD_H1"]
        assert len(df) == 2  # First timestamp is 00:00, should be excluded

    def test_filters_by_end_date(self, normalizer, bronze_csv_file):
        end = datetime(2021, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        results = normalizer.preprocess(end_date=end)

        df = results["EURUSD_H1"]
        assert len(df) == 2  # Last timestamp is 02:00, should be excluded

    def test_filters_by_date_range(self, normalizer, bronze_csv_file):
        start = datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2021, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        results = normalizer.preprocess(start_date=start, end_date=end)

        df = results["EURUSD_H1"]
        assert len(df) == 2

    def test_skips_invalid_filenames(self, tmp_path):
        input_dir = tmp_path / "raw"
        mt5_dir = input_dir / "mt5"
        mt5_dir.mkdir(parents=True, exist_ok=True)

        # Create valid file
        valid_df = pd.DataFrame(
            {
                "time": [1609459200],
                "open": [1.22],
                "high": [1.25],
                "low": [1.21],
                "close": [1.23],
                "tick_volume": [1000],
                "spread": [2],
                "real_volume": [0],
                "source": ["mt5"],
            }
        )
        valid_path = mt5_dir / "mt5_EURUSD_H1_20210101.csv"
        valid_df.to_csv(valid_path, index=False)

        # Create invalid filename
        invalid_path = mt5_dir / "invalid_format.csv"
        valid_df.to_csv(invalid_path, index=False)

        normalizer = PriceNormalizer(
            input_dir=input_dir, output_dir=tmp_path / "processed", sources=["mt5"]
        )
        results = normalizer.preprocess()

        assert len(results) == 1  # Only valid file processed
        assert "EURUSD_H1" in results

    def test_continues_on_validation_error(self, tmp_path):
        input_dir = tmp_path / "raw"
        mt5_dir = input_dir / "mt5"
        mt5_dir.mkdir(parents=True, exist_ok=True)

        # Create valid file
        valid_df = pd.DataFrame(
            {
                "time": [1609459200],
                "open": [1.22],
                "high": [1.25],
                "low": [1.21],
                "close": [1.23],
                "tick_volume": [1000],
                "spread": [2],
                "real_volume": [0],
                "source": ["mt5"],
            }
        )
        valid_path = mt5_dir / "mt5_EURUSD_H1_20210101.csv"
        valid_df.to_csv(valid_path, index=False)

        # Create invalid file (high < low)
        invalid_df = pd.DataFrame(
            {
                "time": [1609459200],
                "open": [1.22],
                "high": [1.20],  # Invalid: high < low
                "low": [1.21],
                "close": [1.23],
                "tick_volume": [1000],
                "spread": [2],
                "real_volume": [0],
                "source": ["mt5"],
            }
        )
        invalid_path = mt5_dir / "mt5_GBPUSD_H1_20210101.csv"
        invalid_df.to_csv(invalid_path, index=False)

        normalizer = PriceNormalizer(
            input_dir=input_dir, output_dir=tmp_path / "processed", sources=["mt5"]
        )
        results = normalizer.preprocess()

        assert len(results) == 1  # Only valid file processed
        assert "EURUSD_H1" in results

    def test_raises_if_no_csv_files_found(self, tmp_path):
        input_dir = tmp_path / "raw"
        mt5_dir = input_dir / "mt5"
        mt5_dir.mkdir(parents=True, exist_ok=True)

        normalizer = PriceNormalizer(
            input_dir=input_dir, output_dir=tmp_path / "processed", sources=["mt5"]
        )

        # When no files found in MT5 dir, preprocessor returns empty dict, then raises
        with pytest.raises(ValueError, match="No valid datasets processed"):
            normalizer.preprocess()

    def test_raises_if_input_dir_missing(self, tmp_path):
        normalizer = PriceNormalizer(
            input_dir=tmp_path / "nonexistent", output_dir=tmp_path / "processed"
        )

        with pytest.raises(ValueError, match="Input directory does not exist"):
            normalizer.preprocess()

    def test_raises_if_no_valid_datasets(self, tmp_path):
        input_dir = tmp_path / "raw"
        mt5_dir = input_dir / "mt5"
        mt5_dir.mkdir(parents=True, exist_ok=True)

        # Create file with invalid data (all high < low)
        invalid_df = pd.DataFrame(
            {
                "time": [1609459200],
                "open": [1.22],
                "high": [1.20],  # Invalid
                "low": [1.21],
                "close": [1.23],
                "tick_volume": [1000],
                "spread": [2],
                "real_volume": [0],
                "source": ["mt5"],
            }
        )
        invalid_path = mt5_dir / "mt5_EURUSD_H1_20210101.csv"
        invalid_df.to_csv(invalid_path, index=False)

        normalizer = PriceNormalizer(
            input_dir=input_dir, output_dir=tmp_path / "processed", sources=["mt5"]
        )

        with pytest.raises(ValueError, match="No valid datasets processed"):
            normalizer.preprocess()


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


class TestExport:
    def test_export_creates_parquet_file(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        start = df_silver["timestamp_utc"].min().to_pydatetime()
        end = df_silver["timestamp_utc"].max().to_pydatetime()

        path = normalizer.export(df_silver, "EURUSD_H1", start, end, format="parquet")

        assert path.exists()
        assert path.suffix == ".parquet"
        assert "ohlcv_EURUSD_H1" in path.name

    def test_export_follows_naming_convention(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        start = datetime(2021, 1, 1, tzinfo=timezone.utc)
        end = datetime(2021, 1, 2, tzinfo=timezone.utc)

        path = normalizer.export(df_silver, "EURUSD_H1", start, end, format="parquet")

        expected_name = "ohlcv_EURUSD_H1_2021-01-01_2021-01-02.parquet"
        assert path.name == expected_name

    def test_export_csv_format(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        start = df_silver["timestamp_utc"].min().to_pydatetime()
        end = df_silver["timestamp_utc"].max().to_pydatetime()

        path = normalizer.export(df_silver, "EURUSD_H1", start, end, format="csv")

        assert path.exists()
        assert path.suffix == ".csv"

        # Verify can read back
        loaded = pd.read_csv(path)
        assert len(loaded) == len(df_silver)

    def test_export_parquet_preserves_data(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        start = df_silver["timestamp_utc"].min().to_pydatetime()
        end = df_silver["timestamp_utc"].max().to_pydatetime()

        path = normalizer.export(df_silver, "EURUSD_H1", start, end, format="parquet")

        # Verify can read back
        loaded = pd.read_parquet(path)
        assert len(loaded) == len(df_silver)
        assert list(loaded.columns) == list(df_silver.columns)
        assert loaded["pair"].iloc[0] == "EURUSD"

    def test_export_raises_on_empty_dataframe(self, normalizer):
        df_empty = pd.DataFrame()
        start = datetime(2021, 1, 1, tzinfo=timezone.utc)
        end = datetime(2021, 1, 2, tzinfo=timezone.utc)

        with pytest.raises(ValueError, match="Cannot export empty DataFrame"):
            normalizer.export(df_empty, "EURUSD_H1", start, end)

    def test_export_raises_on_invalid_format(self, normalizer, bronze_data):
        df_silver = normalizer._transform_mt5_to_silver(bronze_data, "EURUSD", "H1")
        start = df_silver["timestamp_utc"].min().to_pydatetime()
        end = df_silver["timestamp_utc"].max().to_pydatetime()

        with pytest.raises(ValueError, match="Invalid format 'json'"):
            normalizer.export(df_silver, "EURUSD_H1", start, end, format="json")
