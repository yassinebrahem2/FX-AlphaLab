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

    expected_path = Path("data/raw/news/gdelt")

    assert collector.output_dir == expected_path
    assert collector.output_dir.exists()
    assert collector.output_dir.name == "gdelt"


# -------------------------------------------------------
# Credibility Tier
# -------------------------------------------------------
def test_assign_credibility_tier():
    collector = GDELTCollector(output_dir=Path("dummy"))

    assert collector._assign_credibility_tier("reuters.com") == 1
    assert collector._assign_credibility_tier("bloomberg.com") == 1
    assert collector._assign_credibility_tier("wsj.com") == 2
    assert collector._assign_credibility_tier("randomblog.com") == 3
    assert collector._assign_credibility_tier(None) == 3


# -------------------------------------------------------
# URL Hashing
# -------------------------------------------------------
def test_hash_url_consistency():
    collector = GDELTCollector(output_dir=Path("dummy"))

    url = "https://example.com/article"

    hash1 = collector._hash_url(url)
    hash2 = collector._hash_url(url)

    assert hash1 == hash2
    assert isinstance(hash1, str)
    assert len(hash1) == 64  # SHA256 length


# -------------------------------------------------------
# Parse Field
# -------------------------------------------------------
def test_parse_field():
    collector = GDELTCollector(output_dir=Path("dummy"))

    assert collector._parse_field("A;B;C") == ["A", "B", "C"]
    assert collector._parse_field("SingleValue") == ["SingleValue"]
    assert collector._parse_field("") == []
    assert collector._parse_field(None) == []


# -------------------------------------------------------
# In-Memory Dedup Logic
# -------------------------------------------------------
def test_dedup_in_memory():
    collector = GDELTCollector(output_dir=Path("dummy"))

    urls = [
        "https://a.com",
        "https://a.com",
        "https://b.com",
    ]

    hashes = set()
    unique = []

    for url in urls:
        h = collector._hash_url(url)
        if h not in hashes:
            hashes.add(h)
            unique.append(url)

    assert len(unique) == 2


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

    result = collector.collect(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 1),
    )

    docs = result["aggregated"]

    tiers = [d["metadata"]["credibility_tier"] for d in docs]

    assert tiers == sorted(tiers)


# -------------------------------------------------------
# Export JSONL
# -------------------------------------------------------
def test_export_jsonl_success(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    data = [
        {"url": "a", "metadata": {"credibility_tier": 1}},
        {"url": "b", "metadata": {"credibility_tier": 2}},
    ]

    path = collector.export_jsonl(data)

    assert path.exists()

    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 2


def test_export_jsonl_empty_raises(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    with pytest.raises(ValueError):
        collector.export_jsonl([])


def test_export_to_jsonl_with_data(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    data = [
        {"url": "a", "metadata": {"credibility_tier": 1}},
        {"url": "b", "metadata": {"credibility_tier": 2}},
    ]

    path = collector.export_to_jsonl(data=data)

    assert path.exists()

    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 2


def test_export_to_jsonl_calls_collect(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    mock_data = [
        {"url": "x", "metadata": {"credibility_tier": 1}},
    ]

    collector.collect = MagicMock(return_value={"aggregated": mock_data})

    path = collector.export_to_jsonl(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 1),
    )

    collector.collect.assert_called_once()

    assert path.exists()

    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 1
