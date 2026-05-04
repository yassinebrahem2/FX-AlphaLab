from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.ingestion.orchestrator import CollectionOrchestrator
from src.shared.config.sources import InferenceConfig, SourceConfig, SourcesConfig


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = datetime(2026, 5, 4, 12, 0, 0, tzinfo=timezone.utc)
        if tz is None:
            return value.replace(tzinfo=None)
        return value.astimezone(tz)

    @classmethod
    def utcnow(cls):
        return datetime(2026, 5, 4, 12, 0, 0)


@pytest.fixture
def orchestrator(tmp_path: Path):
    (tmp_path / "data" / "raw").mkdir(parents=True)
    (tmp_path / "data" / "processed" / "macro").mkdir(parents=True)
    (tmp_path / "data" / "processed" / "google_trends").mkdir(parents=True)
    (tmp_path / "logs" / "collectors").mkdir(parents=True)

    config = SourcesConfig(
        sources={
            "fred_macro": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=730,
                fetch_from=None,
                description="FRED",
            ),
            "ecb_macro": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=730,
                fetch_from=None,
                description="ECB",
            ),
            "google_trends": SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=None,
                fetch_from="2021-01-01",
                description="Google Trends",
            ),
        },
        inference=InferenceConfig(schedule_cron="0 0 * * *", dry_run=True),
    )

    orch = CollectionOrchestrator(config=config, root=tmp_path)
    yield orch
    orch.close()


def _frame(rows: int) -> pd.DataFrame:
    return pd.DataFrame({"value": list(range(rows))})


def test_fred_macro_happy_path(orchestrator):
    collector = Mock()
    collector.collect.return_value = {
        "financial_stress": _frame(2),
        "federal_funds_rate": _frame(3),
    }
    collector.export_csv.return_value = Path("ignored.csv")

    normalizer = Mock()
    normalizer.preprocess.return_value = {"macro_all": _frame(5)}

    with (
        patch("src.ingestion.orchestrator.datetime", FixedDateTime),
        patch("src.ingestion.collectors.fred_collector.FREDCollector", return_value=collector),
        patch(
            "src.ingestion.preprocessors.macro_normalizer.MacroNormalizer",
            return_value=normalizer,
        ) as normalizer_cls,
    ):
        result = orchestrator.run_source("fred_macro")

    assert result.source_id == "fred_macro"
    assert result.rows_written == 5
    assert result.backfill_performed is True
    assert result.error is None
    assert collector.collect.call_count == 1
    assert collector.export_csv.call_count == 2
    assert normalizer_cls.call_args.kwargs["sources"] == ["fred", "ecb"]
    assert normalizer.preprocess.call_count == 1
    assert normalizer.preprocess.call_args.kwargs["backfill"] is True


def test_fred_macro_collector_error(orchestrator):
    collector = Mock()
    collector.collect.side_effect = RuntimeError("fred failed")
    normalizer = Mock()

    with (
        patch("src.ingestion.orchestrator.datetime", FixedDateTime),
        patch("src.ingestion.collectors.fred_collector.FREDCollector", return_value=collector),
        patch(
            "src.ingestion.preprocessors.macro_normalizer.MacroNormalizer",
            return_value=normalizer,
        ),
    ):
        result = orchestrator.run_source("fred_macro")

    assert result.source_id == "fred_macro"
    assert result.rows_written == 0
    assert result.error == "fred failed"
    assert normalizer.preprocess.call_count == 0


def test_ecb_macro_happy_path(orchestrator):
    collector = Mock()
    collector.collect.return_value = {"policy_rates": 4, "exchange_rates": 6}
    normalizer = Mock()
    normalizer.preprocess.return_value = {"macro_all": _frame(10)}

    with (
        patch("src.ingestion.orchestrator.datetime", FixedDateTime),
        patch("src.ingestion.collectors.ecb_collector.ECBCollector", return_value=collector),
        patch(
            "src.ingestion.preprocessors.macro_normalizer.MacroNormalizer",
            return_value=normalizer,
        ) as normalizer_cls,
    ):
        result = orchestrator.run_source("ecb_macro")

    assert result.source_id == "ecb_macro"
    assert result.rows_written == 10
    assert result.backfill_performed is True
    assert result.error is None
    assert collector.collect.call_count == 1
    assert normalizer_cls.call_args.kwargs["sources"] == ["fred", "ecb"]
    assert normalizer.preprocess.call_count == 1
    assert normalizer.preprocess.call_args.kwargs["backfill"] is True


def test_ecb_macro_collector_error(orchestrator):
    collector = Mock()
    collector.collect.side_effect = RuntimeError("ecb failed")
    normalizer = Mock()

    with (
        patch("src.ingestion.orchestrator.datetime", FixedDateTime),
        patch("src.ingestion.collectors.ecb_collector.ECBCollector", return_value=collector),
        patch(
            "src.ingestion.preprocessors.macro_normalizer.MacroNormalizer",
            return_value=normalizer,
        ),
    ):
        result = orchestrator.run_source("ecb_macro")

    assert result.source_id == "ecb_macro"
    assert result.rows_written == 0
    assert result.error == "ecb failed"
    assert normalizer.preprocess.call_count == 0


def test_google_trends_happy_path(orchestrator):
    collector = Mock()
    collector.collect.return_value = {"trends_fx_pairs_0": 11, "trends_fx_pairs_1": 9}
    preprocessor = Mock()
    preprocessor.run.return_value = {"google_trends_weekly": 20}

    with (
        patch("src.ingestion.orchestrator.datetime", FixedDateTime),
        patch(
            "src.ingestion.collectors.google_trends_collector.GoogleTrendsCollector",
            return_value=collector,
        ),
        patch(
            "src.ingestion.preprocessors.google_trends_preprocessor.GoogleTrendsPreprocessor",
            return_value=preprocessor,
        ),
    ):
        result = orchestrator.run_source("google_trends")

    assert result.source_id == "google_trends"
    assert result.rows_written == 20
    assert result.backfill_performed is True
    assert result.error is None
    call_args = collector.collect.call_args.kwargs
    assert call_args["force"] is True
    assert call_args["start_date"] == datetime(2021, 1, 1, tzinfo=timezone.utc)
    assert call_args["end_date"] == datetime(2026, 5, 4, 12, 0, 0, tzinfo=timezone.utc)
    assert preprocessor.run.call_args.kwargs["backfill"] is True


def test_google_trends_collector_error(orchestrator):
    collector = Mock()
    collector.collect.side_effect = RuntimeError("google trends failed")
    preprocessor = Mock()

    with (
        patch("src.ingestion.orchestrator.datetime", FixedDateTime),
        patch(
            "src.ingestion.collectors.google_trends_collector.GoogleTrendsCollector",
            return_value=collector,
        ),
        patch(
            "src.ingestion.preprocessors.google_trends_preprocessor.GoogleTrendsPreprocessor",
            return_value=preprocessor,
        ),
    ):
        result = orchestrator.run_source("google_trends")

    assert result.source_id == "google_trends"
    assert result.rows_written == 0
    assert result.error == "google trends failed"
    assert preprocessor.run.call_count == 0
