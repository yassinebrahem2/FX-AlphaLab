"""Tests for CollectionOrchestrator collector method implementations.

Tests the four implemented collector stubs:
- _collect_dukascopy_* (D1, H4, H1)
- _collect_gdelt_events
- _collect_gdelt_gkg
- _collect_forex_factory
"""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.ingestion.orchestrator import CollectionOrchestrator
from src.shared.config.sources import InferenceConfig, SourceConfig, SourcesConfig


@pytest.fixture
def test_config():
    """Create minimal SourcesConfig with all four collector sources."""
    return SourcesConfig(
        sources={
            "dukascopy_d1": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=400,
                fetch_from=None,
                description="Test D1",
            ),
            "dukascopy_h4": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=400,
                fetch_from=None,
                description="Test H4",
            ),
            "dukascopy_h1": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=400,
                fetch_from=None,
                description="Test H1",
            ),
            "gdelt_events": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=50,
                fetch_from=None,
                description="Test GDELT Events",
            ),
            "gdelt_gkg": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=30,
                fetch_from=None,
                description="Test GDELT GKG",
            ),
            "forex_factory": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=90,
                fetch_from=None,
                description="Test ForexFactory",
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
        (root / "data" / "raw" / "dukascopy").mkdir(parents=True)
        (root / "data" / "raw" / "gdelt_events").mkdir(parents=True)
        (root / "data" / "raw" / "gdelt_gkg").mkdir(parents=True)
        (root / "data" / "raw" / "forexfactory").mkdir(parents=True)
        (root / "data" / "processed" / "ohlcv").mkdir(parents=True)
        (root / "data" / "processed" / "events").mkdir(parents=True)
        (root / "data" / "processed" / "gkg").mkdir(parents=True)
        (root / "logs" / "collectors").mkdir(parents=True)
        yield root


@pytest.fixture
def orchestrator(test_config, temp_root):
    """CollectionOrchestrator with guaranteed close() on teardown."""
    o = CollectionOrchestrator(config=test_config, root=temp_root)
    yield o
    o.close()


class TestDukascopyCollectors:
    """Tests for Dukascopy D1, H4, H1 collectors."""

    def test_dukascopy_backfill_calls_collector_with_none_dates(self, orchestrator):
        """Backfill (fetch_from=None) should call collector with None dates."""
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.preprocess.return_value = {}

        with (
            patch(
                "src.ingestion.collectors.dukascopy_collector.DukascopyCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.dukascopy_preprocessor.DukascopyPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            orchestrator._collect_dukascopy(
                source_config=orchestrator.config.sources["dukascopy_d1"],
                fetch_from=None,
            )

            # Collector should be called with None dates on backfill
            mock_collector.collect.assert_called_once()
            call_args = mock_collector.collect.call_args
            assert call_args.kwargs["start_date"] is None
            assert call_args.kwargs["end_date"] is None
            assert call_args.kwargs["backfill"] is True

    def test_dukascopy_incremental_calls_collector_with_fetch_from_date(self, orchestrator):
        """Incremental (fetch_from=date) should call collector with correct datetime range."""
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.preprocess.return_value = {}

        fetch_from_date = date(2026, 5, 2)

        with (
            patch(
                "src.ingestion.collectors.dukascopy_collector.DukascopyCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.dukascopy_preprocessor.DukascopyPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            orchestrator._collect_dukascopy(
                source_config=orchestrator.config.sources["dukascopy_d1"],
                fetch_from=fetch_from_date,
            )

            # Collector should be called with start_date from fetch_from and end_date as yesterday
            mock_collector.collect.assert_called_once()
            call_args = mock_collector.collect.call_args

            # Start should be fetch_from at midnight UTC
            assert call_args.kwargs["start_date"] is not None
            assert call_args.kwargs["start_date"].tzinfo == timezone.utc
            assert call_args.kwargs["start_date"].date() == fetch_from_date

            # End should be yesterday
            assert call_args.kwargs["end_date"] is not None
            assert call_args.kwargs["end_date"].tzinfo == timezone.utc

            # Backfill should be False for incremental
            assert call_args.kwargs["backfill"] is False

    def test_dukascopy_always_calls_preprocessor_with_backfill_true(self, orchestrator):
        """Preprocessor should always be called with backfill=True."""
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.preprocess.return_value = {}

        with (
            patch(
                "src.ingestion.collectors.dukascopy_collector.DukascopyCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.dukascopy_preprocessor.DukascopyPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            # Test both backfill and incremental
            for fetch_from in [None, date(2026, 5, 2)]:
                mock_preprocessor.reset_mock()
                orchestrator._collect_dukascopy(
                    source_config=orchestrator.config.sources["dukascopy_d1"],
                    fetch_from=fetch_from,
                )
                mock_preprocessor.preprocess.assert_called_once()
                assert mock_preprocessor.preprocess.call_args.kwargs["backfill"] is True

    def test_dukascopy_returns_total_silver_rows(self, orchestrator):
        """Should return sum of rows across all timeframe DataFrames."""
        mock_collector = Mock()
        mock_preprocessor = Mock()

        # Mock preprocessor returns DataFrames for different instrument×timeframe combinations
        df_eurusd_d1 = pd.DataFrame({"close": [1.0, 2.0, 3.0]})  # 3 rows
        df_gbpusd_d1 = pd.DataFrame({"close": [1.0, 2.0]})  # 2 rows
        df_usdjpy_h4 = pd.DataFrame({"close": [100.0, 101.0, 102.0, 103.0, 104.0]})  # 5 rows

        mock_preprocessor.preprocess.return_value = {
            "EURUSD_D1": df_eurusd_d1,
            "GBPUSD_D1": df_gbpusd_d1,
            "USDJPY_H4": df_usdjpy_h4,
        }

        with (
            patch(
                "src.ingestion.collectors.dukascopy_collector.DukascopyCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.dukascopy_preprocessor.DukascopyPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            rows = orchestrator._collect_dukascopy(
                source_config=orchestrator.config.sources["dukascopy_d1"],
                fetch_from=None,
            )

            assert rows == 10  # 3 + 2 + 5

    def test_all_three_dukascopy_stubs_delegate_to_shared_helper(self, orchestrator):
        """All three stubs (D1, H4, H1) should delegate to shared helper."""
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.preprocess.return_value = {"TEST": pd.DataFrame({"a": [1, 2, 3]})}

        with (
            patch(
                "src.ingestion.collectors.dukascopy_collector.DukascopyCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.dukascopy_preprocessor.DukascopyPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            # All three stubs should produce same result
            result_d1 = orchestrator._collect_dukascopy_d1(
                source_config=orchestrator.config.sources["dukascopy_d1"],
                fetch_from=date(2026, 5, 2),
            )
            result_h4 = orchestrator._collect_dukascopy_h4(
                source_config=orchestrator.config.sources["dukascopy_h4"],
                fetch_from=date(2026, 5, 2),
            )
            result_h1 = orchestrator._collect_dukascopy_h1(
                source_config=orchestrator.config.sources["dukascopy_h1"],
                fetch_from=date(2026, 5, 2),
            )

            # All should return 3 (one DataFrame with 3 rows)
            assert result_d1 == 3
            assert result_h4 == 3
            assert result_h1 == 3


class TestGDELTEventsCollector:
    """Tests for GDELT Events collector."""

    def test_gdelt_events_backfill_uses_min_silver_days_as_lookback(self, orchestrator):
        """Backfill should use min_silver_days to calculate start_dt."""
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.run.return_value = {}

        source_config = orchestrator.config.sources["gdelt_events"]
        assert source_config.min_silver_days == 50

        with (
            patch(
                "src.ingestion.collectors.gdelt_events_collector.GDELTEventsCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.gdelt_events_preprocessor.GDELTEventsPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            orchestrator._collect_gdelt_events(
                source_config=source_config,
                fetch_from=None,
            )

            # Collector should be called with dates 50 days apart
            mock_collector.collect.assert_called_once()
            call_args = mock_collector.collect.call_args
            start_dt = call_args.kwargs["start_date"]
            end_dt = call_args.kwargs["end_date"]

            assert start_dt is not None
            assert end_dt is not None
            # Should be approximately 50 days apart
            diff_days = (end_dt - start_dt).days
            assert 49 <= diff_days <= 51

    def test_gdelt_events_incremental_uses_fetch_from_as_start(self, orchestrator):
        """Incremental should use fetch_from as start_date."""
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.run.return_value = {}

        fetch_from_date = date(2026, 5, 2)

        with (
            patch(
                "src.ingestion.collectors.gdelt_events_collector.GDELTEventsCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.gdelt_events_preprocessor.GDELTEventsPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            orchestrator._collect_gdelt_events(
                source_config=orchestrator.config.sources["gdelt_events"],
                fetch_from=fetch_from_date,
            )

            mock_collector.collect.assert_called_once()
            call_args = mock_collector.collect.call_args
            start_dt = call_args.kwargs["start_date"]

            assert start_dt is not None
            assert start_dt.date() == fetch_from_date

    def test_gdelt_events_end_date_is_yesterday_utc(self, orchestrator):
        """End date should always be yesterday UTC midnight."""
        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.run.return_value = {}

        with (
            patch(
                "src.ingestion.collectors.gdelt_events_collector.GDELTEventsCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.gdelt_events_preprocessor.GDELTEventsPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            orchestrator._collect_gdelt_events(
                source_config=orchestrator.config.sources["gdelt_events"],
                fetch_from=None,
            )

            call_args = mock_collector.collect.call_args
            end_dt = call_args.kwargs["end_date"]

            # Should be yesterday
            today_utc = datetime.now(timezone.utc).date()
            yesterday_utc = today_utc - timedelta(days=1)

            assert end_dt.date() == yesterday_utc
            assert end_dt.hour == 0
            assert end_dt.minute == 0

    def test_gdelt_events_returns_sum_of_silver_rows(self, orchestrator):
        """Should return sum of rows across all silver results."""
        mock_collector = Mock()
        mock_preprocessor = Mock()

        # Mock preprocessor returns dict with row counts per month
        mock_preprocessor.run.return_value = {
            "202605": 100,
            "202606": 150,
            "202607": 200,
        }

        with (
            patch(
                "src.ingestion.collectors.gdelt_events_collector.GDELTEventsCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.gdelt_events_preprocessor.GDELTEventsPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            rows = orchestrator._collect_gdelt_events(
                source_config=orchestrator.config.sources["gdelt_events"],
                fetch_from=None,
            )

            assert rows == 450  # 100 + 150 + 200


class TestGDELTGKGCollector:
    """Tests for GDELT GKG collector."""

    def test_gdelt_gkg_passes_project_id_from_env(self, orchestrator, monkeypatch):
        """Should pass GOOGLE_CLOUD_PROJECT from environment to collector."""
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project-123")

        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.run.return_value = {}

        with (
            patch(
                "src.ingestion.collectors.gdelt_gkg_collector.GDELTGKGCollector",
                return_value=mock_collector,
            ) as mock_collector_class,
            patch(
                "src.ingestion.preprocessors.gdelt_gkg_preprocessor.GDELTGKGPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            orchestrator._collect_gdelt_gkg(
                source_config=orchestrator.config.sources["gdelt_gkg"],
                fetch_from=None,
            )

            # Collector class should be instantiated with project_id
            mock_collector_class.assert_called_once()
            call_args = mock_collector_class.call_args
            assert call_args.kwargs.get("project_id") == "test-project-123"

    def test_gdelt_gkg_passes_none_project_id_when_env_unset(self, orchestrator, monkeypatch):
        """Should pass None as project_id when GOOGLE_CLOUD_PROJECT not set."""
        monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)

        mock_collector = Mock()
        mock_preprocessor = Mock()
        mock_preprocessor.run.return_value = {}

        with (
            patch(
                "src.ingestion.collectors.gdelt_gkg_collector.GDELTGKGCollector",
                return_value=mock_collector,
            ) as mock_collector_class,
            patch(
                "src.ingestion.preprocessors.gdelt_gkg_preprocessor.GDELTGKGPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            orchestrator._collect_gdelt_gkg(
                source_config=orchestrator.config.sources["gdelt_gkg"],
                fetch_from=None,
            )

            # Collector class should be instantiated with None project_id
            mock_collector_class.assert_called_once()
            call_args = mock_collector_class.call_args
            assert call_args.kwargs.get("project_id") is None

    def test_gdelt_gkg_returns_sum_of_silver_rows(self, orchestrator):
        """Should return sum of rows across all silver results."""
        mock_collector = Mock()
        mock_preprocessor = Mock()

        # Mock preprocessor returns dict with row counts per month
        mock_preprocessor.run.return_value = {
            "202605": 250,
            "202606": 300,
        }

        with (
            patch(
                "src.ingestion.collectors.gdelt_gkg_collector.GDELTGKGCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.gdelt_gkg_preprocessor.GDELTGKGPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            rows = orchestrator._collect_gdelt_gkg(
                source_config=orchestrator.config.sources["gdelt_gkg"],
                fetch_from=None,
            )

            assert rows == 550  # 250 + 300


class TestForexFactoryCollector:
    """Tests for Forex Factory collector."""

    def test_forex_factory_always_passes_backfill_true(self, orchestrator):
        """Should always pass backfill=True to collector regardless of fetch_from."""
        mock_collector = Mock()
        mock_collector.collect.return_value = {"calendar": 100}
        mock_preprocessor = Mock()
        mock_preprocessor.preprocess.return_value = {"events": pd.DataFrame({"a": [1, 2, 3]})}

        with (
            patch(
                "src.ingestion.collectors.forexfactory_collector.ForexFactoryCalendarCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.calendar_parser.CalendarPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            # Test both backfill and incremental
            for fetch_from in [None, date(2026, 5, 2)]:
                mock_collector.reset_mock()
                orchestrator._collect_forex_factory(
                    source_config=orchestrator.config.sources["forex_factory"],
                    fetch_from=fetch_from,
                )

                mock_collector.collect.assert_called_once()
                call_args = mock_collector.collect.call_args
                assert call_args.kwargs["backfill"] is True

    def test_forex_factory_closes_driver_on_success(self, orchestrator):
        """Should call collector.close() after successful collect."""
        mock_collector = Mock()
        mock_collector.collect.return_value = {"calendar": 100}
        mock_preprocessor = Mock()
        mock_preprocessor.preprocess.return_value = {"events": pd.DataFrame({"a": [1, 2, 3]})}

        with (
            patch(
                "src.ingestion.collectors.forexfactory_collector.ForexFactoryCalendarCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.calendar_parser.CalendarPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            orchestrator._collect_forex_factory(
                source_config=orchestrator.config.sources["forex_factory"],
                fetch_from=None,
            )

            mock_collector.close.assert_called_once()

    def test_forex_factory_closes_driver_on_exception(self, orchestrator):
        """Should call collector.close() even if collect() raises exception."""
        mock_collector = Mock()
        mock_collector.collect.side_effect = RuntimeError("Test error")
        mock_collector.close = Mock()

        with (
            patch(
                "src.ingestion.collectors.forexfactory_collector.ForexFactoryCalendarCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.calendar_parser.CalendarPreprocessor",
            ),
        ):
            with pytest.raises(RuntimeError):
                orchestrator._collect_forex_factory(
                    source_config=orchestrator.config.sources["forex_factory"],
                    fetch_from=None,
                )

            mock_collector.close.assert_called_once()

    def test_forex_factory_returns_calendar_row_count(self, orchestrator):
        """Should return number of rows collected from Bronze calendar key."""
        mock_collector = Mock()
        mock_collector.collect.return_value = {"calendar": 50}

        events_df = pd.DataFrame(
            {
                "timestamp_utc": ["2026-05-02T10:30:00Z", "2026-05-02T14:00:00Z"],
                "country": ["US", "EU"],
                "event_name": ["NFP", "CPI"],
            }
        )  # 2 rows

        mock_preprocessor = Mock()
        mock_preprocessor.preprocess.return_value = {"events": events_df}

        with (
            patch(
                "src.ingestion.collectors.forexfactory_collector.ForexFactoryCalendarCollector",
                return_value=mock_collector,
            ),
            patch(
                "src.ingestion.preprocessors.calendar_parser.CalendarPreprocessor",
                return_value=mock_preprocessor,
            ),
        ):
            rows = orchestrator._collect_forex_factory(
                source_config=orchestrator.config.sources["forex_factory"],
                fetch_from=None,
            )

            # Must return Bronze row count from collector's "calendar" key
            assert rows == 50
