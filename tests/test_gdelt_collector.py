import pytest
from pathlib import Path
from unittest.mock import MagicMock

from google.api_core.exceptions import GoogleAPIError

from ingestion.collectors.gdelt_collector import GDELTCollector


# -------------------------------------------------------
# Initialization
# -------------------------------------------------------
def test_collector_initialization(tmp_path):
    collector = GDELTCollector(output_dir=tmp_path)

    assert collector.output_dir == tmp_path
    assert collector.gdelt_dir.exists()
    assert collector.gdelt_dir.name == "gdelt"


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

    # Always fail with correct exception type
    mock_client.query.side_effect = GoogleAPIError("Temporary failure")

    with pytest.raises(GoogleAPIError):
        collector._run_query_with_retry("SELECT 1", max_retries=3)

    # Ensure it retried the correct number of times
    assert mock_client.query.call_count == 3
