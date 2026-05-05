"""Tests for CollectionOrchestrator."""

import os
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.ingestion.orchestrator import CollectionOrchestrator, CollectionResult
from src.shared.config.sources import InferenceConfig, SourceConfig, SourcesConfig


@pytest.fixture
def test_config():
    """Create a minimal test SourcesConfig."""
    return SourcesConfig(
        sources={
            "dukascopy_d1": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=400,
                fetch_from=None,
                description="Test D1",
            ),
            "gdelt_events": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=50,
                fetch_from=None,
                description="Test GDELT Events",
            ),
            "google_trends": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=None,
                fetch_from="2021-01-01",
                description="Test Google Trends",
            ),
            "fred_macro": SourceConfig(
                enabled=False,
                interval_hours=24,
                min_silver_days=730,
                fetch_from=None,
                description="Test FRED (disabled)",
            ),
        },
        inference=InferenceConfig(
            schedule_cron="30 22 * * *",
            dry_run=False,
        ),
    )


@pytest.fixture
def temp_root():
    """Create temporary project root with required data directories."""
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "data" / "processed" / "ohlcv").mkdir(parents=True)
        (root / "data" / "processed" / "events").mkdir(parents=True)
        (root / "data" / "processed" / "macro").mkdir(parents=True)
        (root / "data" / "processed" / "sentiment" / "source=stocktwits").mkdir(parents=True)
        (root / "data" / "processed" / "sentiment" / "source=google_trends").mkdir(parents=True)
        (root / "logs" / "collectors").mkdir(parents=True)
        yield root


@pytest.fixture
def orchestrator(test_config, temp_root):
    """CollectionOrchestrator with guaranteed close() on teardown.

    Depends on temp_root so pytest tears this down BEFORE temp_root's
    TemporaryDirectory context exits — preventing Windows file-lock errors.
    """
    o = CollectionOrchestrator(config=test_config, root=temp_root)
    yield o
    o.close()


class TestCollectionOrchestrator:
    """Test CollectionOrchestrator functionality."""

    def test_initialization(self, orchestrator, temp_root, test_config):
        assert orchestrator.config == test_config
        assert orchestrator.root == temp_root
        assert len(orchestrator._registry) == 13

    def test_invalid_source_id(self, orchestrator):
        result = orchestrator.run_source("unknown_source")
        assert result.error is not None
        assert "Unknown source" in result.error

    def test_disabled_source_skipped(self, orchestrator):
        result = orchestrator.run_source("fred_macro")
        assert result.source_id == "fred_macro"
        assert result.rows_written == 0
        assert result.backfill_performed is False
        assert result.error is None

    def test_silver_has_minimum_nonexistent(self, orchestrator, temp_root):
        import shutil

        shutil.rmtree(temp_root / "data" / "processed" / "ohlcv")
        assert orchestrator._silver_has_minimum("dukascopy_d1") is False

    def test_silver_has_minimum_empty_directory(self, orchestrator):
        assert orchestrator._silver_has_minimum("dukascopy_d1") is False

    def test_silver_has_minimum_parquet_below_threshold(self, orchestrator, temp_root):
        ohlcv_dir = temp_root / "data" / "processed" / "ohlcv"
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-01-01", periods=100),
                "pair": "EURUSD",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 1000,
            }
        )
        df.to_parquet(ohlcv_dir / "ohlcv_EURUSD_D1_20240101_20240410.parquet")
        assert orchestrator._silver_has_minimum("dukascopy_d1") is False

    def test_silver_has_minimum_parquet_meets_threshold(self, orchestrator, temp_root):
        from datetime import datetime, timedelta, timezone

        ohlcv_dir = temp_root / "data" / "processed" / "ohlcv"
        # 500 rows with max timestamp = 1 day ago (within staleness_limit)
        recent_end = datetime.now(timezone.utc) - timedelta(days=1)
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range(recent_end - timedelta(days=499), periods=500),
                "pair": "EURUSD",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 1000,
            }
        )
        df.to_parquet(ohlcv_dir / "ohlcv_EURUSD_D1_20250115_20250825.parquet")
        # 500 rows >= 400 (min_silver_days) AND data is fresh → should return True
        assert orchestrator._silver_has_minimum("dukascopy_d1") is True

    def test_silver_has_minimum_null_min_silver_days(self, orchestrator):
        # google_trends has min_silver_days=None → always returns False
        assert orchestrator._silver_has_minimum("google_trends") is False

    def test_silver_has_minimum_macro_signal_meets_threshold(self, orchestrator, temp_root):
        from datetime import datetime, timedelta, timezone

        macro_dir = temp_root / "data" / "processed" / "macro"
        # 800 rows with max timestamp = 1 day ago (within staleness_limit)
        recent_end = datetime.now(timezone.utc) - timedelta(days=1)
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range(recent_end - timedelta(days=799), periods=800),
                "pair": "EURUSD",
                "feature1": 1.0,
            }
        )
        df.to_parquet(macro_dir / "macro_signal.parquet")
        # 800 rows >= 730 (min_silver_days) AND data is fresh → should return True
        assert orchestrator._silver_has_minimum("fred_macro") is True

    def test_silver_has_minimum_macro_signal_below_threshold(self, orchestrator, temp_root):
        macro_dir = temp_root / "data" / "processed" / "macro"
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-01-01", periods=100),
                "pair": "EURUSD",
                "feature1": 1.0,
            }
        )
        df.to_parquet(macro_dir / "macro_signal.parquet")
        assert orchestrator._silver_has_minimum("fred_macro") is False

    def test_run_source_google_trends_supported(self, orchestrator):
        from unittest.mock import Mock, patch

        collector_mock = Mock()
        collector_mock.collect.return_value = {"trends_fx_pairs_0": 7}

        preprocessor_mock = Mock()
        preprocessor_mock.run.return_value = {"google_trends_weekly": 7}

        with (
            patch(
                "src.ingestion.collectors.google_trends_collector.GoogleTrendsCollector",
                return_value=collector_mock,
            ),
            patch(
                "src.ingestion.preprocessors.google_trends_preprocessor.GoogleTrendsPreprocessor",
                return_value=preprocessor_mock,
            ),
        ):
            result = orchestrator.run_source("google_trends")

        assert result.source_id == "google_trends"
        assert result.rows_written == 7
        assert result.backfill_performed is True
        assert result.error is None

    def test_collection_result_structure(self):
        result = CollectionResult(
            source_id="test_source",
            rows_written=1000,
            backfill_performed=True,
            error=None,
        )
        assert result.source_id == "test_source"
        assert result.rows_written == 1000
        assert result.backfill_performed is True
        assert result.error is None

    def test_collection_result_with_error(self):
        result = CollectionResult(
            source_id="test_source",
            rows_written=0,
            backfill_performed=False,
            error="Collection failed",
        )
        assert result.error == "Collection failed"

    def test_silver_stale_forces_backfill(self, orchestrator, temp_root):
        """Test that stale Silver with sufficient rows still forces backfill."""
        from datetime import datetime, timedelta, timezone

        ohlcv_dir = temp_root / "data" / "processed" / "ohlcv"
        # 500 rows but with max timestamp 180 days ago (very stale)
        stale_date = datetime.now(timezone.utc) - timedelta(days=180)
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range(stale_date - timedelta(days=499), periods=500),
                "pair": "EURUSD",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 1000,
            }
        )
        df.to_parquet(ohlcv_dir / "ohlcv_EURUSD_D1_20250101_20250815.parquet")
        # Even though 500 > 400 (min_silver_days), data is stale → should return False
        assert orchestrator._silver_has_minimum("dukascopy_d1") is False

    def test_silver_recent_and_sufficient(self, orchestrator, temp_root):
        """Test that recent Silver with sufficient rows returns True."""
        from datetime import datetime, timedelta, timezone

        ohlcv_dir = temp_root / "data" / "processed" / "ohlcv"
        # 500 rows with max timestamp = yesterday (very recent)
        recent_end = datetime.now(timezone.utc) - timedelta(days=1)
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range(recent_end - timedelta(days=499), periods=500),
                "pair": "EURUSD",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 1000,
            }
        )
        df.to_parquet(ohlcv_dir / "ohlcv_EURUSD_D1_20240901_20250815.parquet")
        # 500 rows > 400 (min_silver_days) AND data is fresh → should return True
        assert orchestrator._silver_has_minimum("dukascopy_d1") is True

    def test_silver_recent_but_insufficient_rows(self, orchestrator, temp_root):
        """Test that recent Silver but insufficient rows still returns False."""
        from datetime import datetime, timedelta, timezone

        ohlcv_dir = temp_root / "data" / "processed" / "ohlcv"
        # 10 rows with max timestamp = yesterday (very recent, but not enough rows)
        recent_end = datetime.now(timezone.utc) - timedelta(days=1)
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range(recent_end - timedelta(days=9), periods=10),
                "pair": "EURUSD",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 1000,
            }
        )
        df.to_parquet(ohlcv_dir / "ohlcv_EURUSD_D1_20250805_20250815.parquet")
        # 10 rows < 400 (min_silver_days) → should return False (row count gate applies first)
        assert orchestrator._silver_has_minimum("dukascopy_d1") is False

    def test_silver_stale_macro_signal_forces_backfill(self, orchestrator, temp_root):
        """Test that stale macro_signal.parquet forces backfill even with sufficient rows."""
        from datetime import datetime, timedelta, timezone

        macro_dir = temp_root / "data" / "processed" / "macro"
        # 800 rows (exceeds 730 min) but stale data from 180 days ago
        stale_date = datetime.now(timezone.utc) - timedelta(days=180)
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range(stale_date - timedelta(days=799), periods=800),
                "series_id": "GDP",
                "value": 1.0,
            }
        )
        df.to_parquet(macro_dir / "macro_signal.parquet")
        # 800 rows > 730 (min_silver_days) but data is stale → should return False
        assert orchestrator._silver_has_minimum("fred_macro") is False

    def test_fred_macro_start_dt_is_always_utc_aware(self, orchestrator):
        from unittest.mock import Mock, patch

        mock_fred = Mock()
        # Return one small DataFrame so code proceeds
        mock_fred.collect.return_value = {
            "S": pd.DataFrame({"timestamp_utc": [pd.Timestamp.now()]})
        }

        mock_ecb = Mock()
        mock_ecb.collect.return_value = {"policy_rates": 0}

        with (
            patch(
                "src.ingestion.collectors.fred_collector.FREDCollector",
                return_value=mock_fred,
            ),
            patch(
                "src.ingestion.collectors.ecb_collector.ECBCollector",
                return_value=mock_ecb,
            ),
            patch.object(orchestrator, "_bronze_has_recent_data", return_value=False),
        ):
            orchestrator._collect_fred_macro(
                source_config=orchestrator.config.sources["fred_macro"], fetch_from=None
            )

        mock_fred.collect.assert_called_once()
        call_args = mock_fred.collect.call_args
        start_date = call_args.kwargs.get("start_date")
        assert start_date is not None
        assert getattr(start_date, "tzinfo", None) is not None

    def test_fred_macro_collects_ecb_when_ecb_bronze_absent(self, orchestrator):
        from unittest.mock import Mock, patch

        mock_fred = Mock()
        mock_fred.collect.return_value = {
            "S": pd.DataFrame({"timestamp_utc": [pd.Timestamp.now()]})
        }
        mock_ecb = Mock()
        mock_ecb.collect.return_value = {"policy_rates": 5}

        def bronze_side(bronze_dir, staleness_limit, glob_pattern="**/*.csv"):
            return bronze_dir.name == "fred"

        with (
            patch("src.ingestion.collectors.fred_collector.FREDCollector", return_value=mock_fred),
            patch("src.ingestion.collectors.ecb_collector.ECBCollector", return_value=mock_ecb),
            patch.object(orchestrator, "_bronze_has_recent_data", side_effect=bronze_side),
        ):
            orchestrator._collect_fred_macro(
                source_config=orchestrator.config.sources["fred_macro"], fetch_from=None
            )

        # FRED should be skipped (fred_ok True) and ECB collected
        mock_fred.collect.assert_not_called()
        mock_ecb.collect.assert_called_once()

    def test_ecb_macro_collects_fred_when_fred_bronze_absent(self, orchestrator):
        from unittest.mock import Mock, patch

        mock_fred = Mock()
        mock_fred.collect.return_value = {
            "S": pd.DataFrame({"timestamp_utc": [pd.Timestamp.now()]})
        }
        mock_ecb = Mock()
        mock_ecb.collect.return_value = {"policy_rates": 5}

        def bronze_side(bronze_dir, staleness_limit, glob_pattern="**/*.csv"):
            return bronze_dir.name == "ecb"

        with (
            patch("src.ingestion.collectors.fred_collector.FREDCollector", return_value=mock_fred),
            patch("src.ingestion.collectors.ecb_collector.ECBCollector", return_value=mock_ecb),
            patch.object(orchestrator, "_bronze_has_recent_data", side_effect=bronze_side),
        ):
            orchestrator._collect_ecb_macro(
                source_config=orchestrator.config.sources["dukascopy_d1"], fetch_from=None
            )

        mock_ecb.collect.assert_not_called()
        mock_fred.collect.assert_called_once()

    def test_macro_skips_both_collectors_when_both_bronze_fresh(self, orchestrator):
        from unittest.mock import Mock, patch

        mock_fred = Mock()
        mock_ecb = Mock()
        mock_normalizer = Mock()

        with (
            patch("src.ingestion.collectors.fred_collector.FREDCollector", return_value=mock_fred),
            patch("src.ingestion.collectors.ecb_collector.ECBCollector", return_value=mock_ecb),
            patch(
                "src.ingestion.preprocessors.macro_normalizer.MacroNormalizer",
                return_value=mock_normalizer,
            ),
            patch.object(orchestrator, "_bronze_has_recent_data", return_value=True),
        ):
            orchestrator._collect_fred_macro(
                source_config=orchestrator.config.sources["fred_macro"], fetch_from=None
            )

        mock_fred.collect.assert_not_called()
        mock_ecb.collect.assert_not_called()
        mock_normalizer.preprocess.assert_called_once()

    def test_run_source_logs_error_when_result_has_error(self, orchestrator):
        from unittest.mock import Mock

        # Replace registry entry with stub returning CollectionResult with error
        def fake_collector(source_config, fetch_from):
            return CollectionResult(
                source_id="dukascopy_d1", rows_written=0, backfill_performed=False, error="boom"
            )

        orchestrator._registry["dukascopy_d1"] = fake_collector
        # Patch silver check to avoid other paths
        orchestrator._silver_has_minimum = lambda sid: True

        # Spy on existing logger methods to capture calls (preserve handlers)
        orchestrator.logger.error = Mock()
        orchestrator.logger.info = Mock()

        res = orchestrator.run_source("dukascopy_d1")

        assert res.error == "boom"
        orchestrator.logger.error.assert_called()
        # Ensure the success message was not logged
        info_calls = [
            c for c in orchestrator.logger.info.call_args_list if "Completed successfully" in str(c)
        ]
        assert not info_calls

    def test_bronze_has_recent_data_returns_true_when_fresh(self, orchestrator, temp_root):
        bronze_dir = temp_root / "data" / "raw" / "dukascopy" / "EURUSD"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"x": [1]}).to_parquet(bronze_dir / "fresh.parquet")

        assert orchestrator._bronze_has_recent_data(
            bronze_dir=bronze_dir,
            staleness_limit=timedelta(days=3),
            glob_pattern="**/*.parquet",
        )

    def test_bronze_has_recent_data_returns_false_when_dir_missing(self, orchestrator, temp_root):
        missing_dir = temp_root / "data" / "raw" / "does_not_exist"

        assert not orchestrator._bronze_has_recent_data(
            bronze_dir=missing_dir,
            staleness_limit=timedelta(days=3),
            glob_pattern="**/*.parquet",
        )

    def test_bronze_has_recent_data_returns_false_when_stale(self, orchestrator, temp_root):
        bronze_dir = temp_root / "data" / "raw" / "dukascopy" / "GBPUSD"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        bronze_file = bronze_dir / "stale.parquet"
        pd.DataFrame({"x": [1]}).to_parquet(bronze_file)

        stale_seconds = 10 * 24 * 3600
        stale_mtime = bronze_file.stat().st_mtime - stale_seconds
        os.utime(bronze_file, (stale_mtime, stale_mtime))

        assert not orchestrator._bronze_has_recent_data(
            bronze_dir=bronze_dir,
            staleness_limit=timedelta(days=3),
            glob_pattern="**/*.parquet",
        )

    def test_dukascopy_skips_collect_when_bronze_fresh(self, orchestrator):
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.instruments = ["EURUSD", "GBPUSD", "USDJPY"]
        mock_preprocessor.preprocess.return_value = {"EURUSD_D1": pd.DataFrame({"a": [1]})}

        with (
            patch(
                "src.ingestion.collectors.dukascopy_collector.DukascopyCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.dukascopy_preprocessor.DukascopyPreprocessor",
                return_value=mock_preprocessor,
            ),
            patch.object(orchestrator, "_bronze_has_recent_data", return_value=True),
        ):
            orchestrator._collect_dukascopy(
                source_config=orchestrator.config.sources["dukascopy_d1"],
                fetch_from=None,
            )

        mock_collector.collect.assert_not_called()
        mock_preprocessor.preprocess.assert_called_once_with(backfill=True)

    def test_dukascopy_calls_collect_when_bronze_absent(self, orchestrator):
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.instruments = ["EURUSD", "GBPUSD", "USDJPY"]
        mock_preprocessor.preprocess.return_value = {"EURUSD_D1": pd.DataFrame({"a": [1]})}

        with (
            patch(
                "src.ingestion.collectors.dukascopy_collector.DukascopyCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.dukascopy_preprocessor.DukascopyPreprocessor",
                return_value=mock_preprocessor,
            ),
            patch.object(orchestrator, "_bronze_has_recent_data", return_value=False),
        ):
            orchestrator._collect_dukascopy(
                source_config=orchestrator.config.sources["dukascopy_d1"],
                fetch_from=None,
            )

        mock_collector.collect.assert_called_once()
        assert mock_collector.collect.call_args.kwargs["backfill"] is True
        mock_preprocessor.preprocess.assert_called_once_with(backfill=True)

    def test_dukascopy_calls_collect_with_backfill_false_when_partial_bronze(self, orchestrator):
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.instruments = ["EURUSD", "GBPUSD", "USDJPY"]
        mock_preprocessor.preprocess.return_value = {"EURUSD_D1": pd.DataFrame({"a": [1]})}

        def has_recent(bronze_dir, staleness_limit, glob_pattern="**/*.parquet"):
            return bronze_dir.name == "EURUSD"

        with (
            patch(
                "src.ingestion.collectors.dukascopy_collector.DukascopyCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.dukascopy_preprocessor.DukascopyPreprocessor",
                return_value=mock_preprocessor,
            ),
            patch.object(orchestrator, "_bronze_has_recent_data", side_effect=has_recent),
        ):
            orchestrator._collect_dukascopy(
                source_config=orchestrator.config.sources["dukascopy_d1"],
                fetch_from=None,
            )

        mock_collector.collect.assert_called_once()
        assert mock_collector.collect.call_args.kwargs["backfill"] is False
        mock_preprocessor.preprocess.assert_called_once_with(backfill=True)
