import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import GoogleAPIError

from src.ingestion.collectors.gdelt_events_collector import GDELTEventsCollector


@pytest.fixture
def collector(tmp_path: Path) -> GDELTEventsCollector:
    return GDELTEventsCollector(output_dir=tmp_path)


def _event_row(**overrides: object) -> dict:
    row = {
        "GlobalEventID": 1001,
        "SQLDATE": 20240101,
        "Actor1Name": "United States",
        "Actor1Code": "USA",
        "Actor1CountryCode": "USA",
        "Actor1Type1Code": "GOV",
        "Actor2Name": "Germany",
        "Actor2Code": "DEU",
        "Actor2CountryCode": "DEU",
        "Actor2Type1Code": "GOV",
        "EventCode": "061",
        "EventBaseCode": "06",
        "EventRootCode": "06",
        "QuadClass": 2,
        "GoldsteinScale": 1.5,
        "NumMentions": 10,
        "NumSources": 4,
        "NumArticles": 2,
        "AvgTone": -0.5,
        "Actor1Geo_CountryCode": "US",
        "Actor1Geo_FullName": "Washington, United States",
        "Actor2Geo_CountryCode": "DE",
        "Actor2Geo_FullName": "Berlin, Germany",
        "ActionGeo_CountryCode": "DE",
        "ActionGeo_FullName": "Berlin, Germany",
        "SOURCEURL": "https://www.reuters.com/world/europe/example",
    }
    row.update(overrides)
    return row


def test_initialization(collector: GDELTEventsCollector) -> None:
    assert collector.SOURCE_NAME == "gdelt_events"
    assert collector.output_dir.exists()


def test_collect_writes_jsonl(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    dry_job = MagicMock()
    dry_job.total_bytes_processed = 0
    real_job = MagicMock()
    real_result = MagicMock()
    real_result.to_dataframe.return_value.to_dict.return_value = [
        _event_row(),
    ]
    real_job.result.return_value = real_result
    mock_client.query.side_effect = [dry_job, real_job]
    collector.client = mock_client

    result = collector.collect(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 1))

    assert result == {"aggregated": 1}

    output_path = tmp_path / "gdelt_events_202401_raw.jsonl"
    with output_path.open("r", encoding="utf-8") as file_handle:
        lines = [line for line in file_handle if line.strip()]

    assert len(lines) == 1
    sample = json.loads(lines[0])
    assert set(sample) == {
        "source",
        "timestamp_collected",
        "event_id",
        "event_date",
        "actor1_name",
        "actor1_code",
        "actor1_country_code",
        "actor1_type1_code",
        "actor2_name",
        "actor2_code",
        "actor2_country_code",
        "actor2_type1_code",
        "event_code",
        "event_base_code",
        "event_root_code",
        "quad_class",
        "goldstein_scale",
        "num_mentions",
        "num_sources",
        "num_articles",
        "avg_tone",
        "actor1_geo_country_code",
        "actor1_geo_full_name",
        "actor2_geo_country_code",
        "actor2_geo_full_name",
        "action_geo_country_code",
        "action_geo_full_name",
        "source_url",
    }
    assert sample["source"] == "gdelt_events"
    assert sample["event_id"] == "1001"
    assert sample["event_date"] == "20240101"


def test_collect_skips_existing(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)

    output_path = tmp_path / "gdelt_events_202401_raw.jsonl"
    with output_path.open("w", encoding="utf-8") as file_handle:
        file_handle.write(json.dumps({"source": "gdelt_events", "event_id": "1"}) + "\n")

    mock_client = MagicMock()
    collector.client = mock_client

    result = collector.collect(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 31))

    assert result == {"aggregated": 1}
    assert mock_client.query.call_count == 0


def test_collect_deduplicates(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    dry_job = MagicMock()
    dry_job.total_bytes_processed = 0
    real_job = MagicMock()
    real_result = MagicMock()
    real_result.to_dataframe.return_value.to_dict.return_value = [
        _event_row(GlobalEventID=42),
        _event_row(GlobalEventID=42, SOURCEURL="https://www.ft.com/content/example"),
    ]
    real_job.result.return_value = real_result
    mock_client.query.side_effect = [dry_job, real_job]
    collector.client = mock_client

    result = collector.collect(start_date=datetime(2024, 2, 1), end_date=datetime(2024, 2, 1))

    assert result == {"aggregated": 1}
    with (tmp_path / "gdelt_events_202402_raw.jsonl").open("r", encoding="utf-8") as file_handle:
        docs = [json.loads(line) for line in file_handle if line.strip()]

    assert len(docs) == 1
    assert docs[0]["event_id"] == "42"


def test_collect_skips_null_event_id(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    dry_job = MagicMock()
    dry_job.total_bytes_processed = 0
    real_job = MagicMock()
    real_result = MagicMock()
    real_result.to_dataframe.return_value.to_dict.return_value = [
        _event_row(GlobalEventID=None),
    ]
    real_job.result.return_value = real_result
    mock_client.query.side_effect = [dry_job, real_job]
    collector.client = mock_client

    result = collector.collect(start_date=datetime(2024, 3, 1), end_date=datetime(2024, 3, 1))

    assert result == {"aggregated": 0}
    assert not (tmp_path / "gdelt_events_202403_raw.jsonl").exists()


def test_dry_run_cost_guard(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    dry_job = MagicMock()
    dry_job.total_bytes_processed = 6 * 1024**3
    mock_client.query.return_value = dry_job
    collector.client = mock_client

    with pytest.raises(RuntimeError):
        collector.collect(start_date=datetime(2024, 4, 1), end_date=datetime(2024, 4, 1))


def test_health_check_success(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_job.result.return_value = MagicMock()
    mock_client.query.return_value = mock_job
    collector.client = mock_client

    assert collector.health_check() is True


def test_health_check_failure(tmp_path: Path) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    mock_client.query.side_effect = GoogleAPIError("boom")
    collector.client = mock_client

    assert collector.health_check() is False


def test_run_query_with_retry_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    collector = GDELTEventsCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_client.query.side_effect = [GoogleAPIError("temp"), mock_job]
    collector.client = mock_client

    monkeypatch.setattr(
        "src.ingestion.collectors.gdelt_events_collector.time.sleep", lambda _delay: None
    )

    result = collector._run_query_with_retry("SELECT 1", max_retries=2)

    assert result is mock_job
    assert mock_client.query.call_count == 2
