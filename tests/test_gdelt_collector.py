import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import GoogleAPIError

from src.ingestion.collectors.gdelt_collector import GDELTCollector


# -------------------------------------------------------
# Initialization
# -------------------------------------------------------
def test_collector_initialization(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    assert collector.output_dir == tmp_path
    assert collector.output_dir.exists()


# -------------------------------------------------------
# Retry Logic
# -------------------------------------------------------
def test_retry_logic():
    collector = GDELTCollector(output_dir=Path("dummy"))

    mock_client = MagicMock()
    collector.client = mock_client

    mock_client.query.side_effect = GoogleAPIError("Temporary failure")

    with pytest.raises(GoogleAPIError):
        collector._run_query_with_retry("SELECT 1", max_retries=3)

    assert mock_client.query.call_count == 3


# -------------------------------------------------------
# Prioritization by Credibility
# -------------------------------------------------------
def test_prioritizes_by_credibility(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_result = MagicMock()

    mock_rows = [
        {
            "DATE": "2024-01-01",
            "SourceCommonName": "random.com",
            "DocumentIdentifier": "url_1",
            "V2Tone": 0,
            "Themes": "",
            "Locations": "",
            "Organizations": "",
        },
        {
            "DATE": "2024-01-01",
            "SourceCommonName": "reuters.com",
            "DocumentIdentifier": "url_2",
            "V2Tone": 0,
            "Themes": "",
            "Locations": "",
            "Organizations": "",
        },
        {
            "DATE": "2024-01-01",
            "SourceCommonName": "wsj.com",
            "DocumentIdentifier": "url_3",
            "V2Tone": 0,
            "Themes": "",
            "Locations": "",
            "Organizations": "",
        },
    ]

    # Mock dry-run + real query safely
    mock_job.total_bytes_processed = 0
    mock_result.to_dataframe.return_value.to_dict.return_value = mock_rows
    mock_job.result.return_value = mock_result
    mock_client.query.return_value = mock_job

    collector.client = mock_client

    result = collector.collect(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 1))

    assert result == {"aggregated": 3}

    output_path = tmp_path / "gdelt_202401_raw.jsonl"
    assert output_path.exists()

    with output_path.open("r", encoding="utf-8") as fh:
        docs = [json.loads(line) for line in fh if line.strip()]

    def _tier_from_domain(domain: str | None) -> int:
        lowered = (domain or "").lower()
        if any(d in lowered for d in {"reuters.com", "bloomberg.com", "ft.com"}):
            return 1
        if any(d in lowered for d in {"wsj.com", "cnbc.com"}):
            return 2
        return 3

    tiers = [_tier_from_domain(d.get("source_domain")) for d in docs]
    assert tiers == sorted(tiers)


def test_collect_writes_jsonl(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_result = MagicMock()

    mock_rows = [
        {
            "DATE": "20240301090000",
            "SourceCommonName": "reuters.com",
            "DocumentIdentifier": "url_1",
            "V2Tone": "-2.42,2.34,4.77",
            "Themes": "ECON_CURRENCY;USD",
            "Locations": "US",
            "Organizations": "FED",
        },
        {
            "DATE": "20240301100000",
            "SourceCommonName": "wsj.com",
            "DocumentIdentifier": "url_2",
            "V2Tone": "1.10,0.00,0.00",
            "Themes": "ECON_CENTRAL_BANK;EUR",
            "Locations": "EU",
            "Organizations": "ECB",
        },
    ]

    mock_job.total_bytes_processed = 0
    mock_result.to_dataframe.return_value.to_dict.return_value = mock_rows
    mock_job.result.return_value = mock_result
    mock_client.query.return_value = mock_job
    collector.client = mock_client

    result = collector.collect(start_date=datetime(2024, 3, 1), end_date=datetime(2024, 3, 1))

    n_docs = result["aggregated"]
    assert n_docs > 0

    output_path = collector.output_dir / "gdelt_202403_raw.jsonl"
    assert output_path.exists()

    with output_path.open("r", encoding="utf-8") as fh:
        lines = [line for line in fh if line.strip()]
    assert len(lines) == n_docs

    sample = json.loads(lines[0])
    assert isinstance(sample["v2tone"], str)
    assert "metadata" not in sample


def test_collect_empty_returns_zero(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_result = MagicMock()

    mock_job.total_bytes_processed = 0
    mock_result.to_dataframe.return_value.to_dict.return_value = []
    mock_job.result.return_value = mock_result
    mock_client.query.return_value = mock_job
    collector.client = mock_client

    result = collector.collect(start_date=datetime(2024, 4, 1), end_date=datetime(2024, 4, 1))

    assert result == {"aggregated": 0}
    assert not (tmp_path / "gdelt_202404_raw.jsonl").exists()


def test_collect_skip_existing(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    existing_path = tmp_path / "gdelt_202405_raw.jsonl"
    with existing_path.open("w", encoding="utf-8") as fh:
        for i in range(5):
            fh.write(json.dumps({"url": f"existing_{i}"}) + "\n")

    mock_client = MagicMock()
    collector.client = mock_client

    result = collector.collect(
        start_date=datetime(2024, 5, 1),
        end_date=datetime(2024, 5, 31),
        backfill=False,
    )

    assert result == {"aggregated": 5}
    assert mock_client.query.call_count == 0


def test_collect_force_overwrite(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    existing_path = tmp_path / "gdelt_202405_raw.jsonl"
    with existing_path.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps({"url": "stale"}) + "\n")

    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_result = MagicMock()

    mock_rows = [
        {
            "DATE": "20240501090000",
            "SourceCommonName": "reuters.com",
            "DocumentIdentifier": "fresh_url",
            "V2Tone": "-1.0,0,0",
            "Themes": "ECON_CURRENCY;USD",
            "Locations": "US",
            "Organizations": "FED",
        }
    ]

    mock_job.total_bytes_processed = 0
    mock_result.to_dataframe.return_value.to_dict.return_value = mock_rows
    mock_job.result.return_value = mock_result
    mock_client.query.return_value = mock_job
    collector.client = mock_client

    result = collector.collect(
        start_date=datetime(2024, 5, 1),
        end_date=datetime(2024, 5, 31),
        backfill=True,
    )

    assert result["aggregated"] > 0
    assert mock_client.query.call_count > 0
