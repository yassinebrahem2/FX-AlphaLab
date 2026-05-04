"""Tests for CollectionOrchestrator."""

from pathlib import Path
from tempfile import TemporaryDirectory

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
        ohlcv_dir = temp_root / "data" / "processed" / "ohlcv"
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2023-01-01", periods=500),
                "pair": "EURUSD",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 1000,
            }
        )
        df.to_parquet(ohlcv_dir / "ohlcv_EURUSD_D1_20230101_20240410.parquet")
        assert orchestrator._silver_has_minimum("dukascopy_d1") is True

    def test_silver_has_minimum_null_min_silver_days(self, orchestrator):
        # google_trends has min_silver_days=None → always returns False
        assert orchestrator._silver_has_minimum("google_trends") is False

    def test_silver_has_minimum_macro_signal_meets_threshold(self, orchestrator, temp_root):
        macro_dir = temp_root / "data" / "processed" / "macro"
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2022-01-01", periods=800),
                "pair": "EURUSD",
                "feature1": 1.0,
            }
        )
        df.to_parquet(macro_dir / "macro_signal.parquet")
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

    def test_run_source_not_implemented_caught_as_error(self, orchestrator):
        result = orchestrator.run_source("dukascopy_d1")
        assert result.source_id == "dukascopy_d1"
        assert result.error is not None
        assert "not yet integrated" in result.error

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
