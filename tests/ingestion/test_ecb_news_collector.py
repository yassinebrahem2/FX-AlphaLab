"""Unit tests for ECB collector."""

import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from src.ingestion.collectors.ecb_news_collector import ECBNewsCollector, ECBNewsDocument

# ---------------------------------------------------------------------------
# Sample Data
# ---------------------------------------------------------------------------

SAMPLE_RSS_ENTRY_SPEECH = {
    "title": "Speech by Christine Lagarde, President of the ECB",
    "link": "https://www.ecb.europa.eu/press/key/date/2026/html/ecb.sp260211~abc123.en.html",
    "summary": "Remarks on monetary policy outlook at European Parliament",
    "id": "ecb.sp260211~abc123",
    "published": "Tue, 11 Feb 2026 10:00:00 GMT",
    "published_parsed": (2026, 2, 11, 10, 0, 0, 1, 42, 0),
    "tags": [{"term": "Monetary policy"}],
}

SAMPLE_RSS_ENTRY_POLICY = {
    "title": "Monetary policy decisions",
    "link": "https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.mp260130~xyz789.en.html",
    "summary": "Governing Council decisions on key ECB interest rates",
    "id": "ecb.mp260130~xyz789",
    "published": "Thu, 30 Jan 2026 13:45:00 GMT",
    "published_parsed": (2026, 1, 30, 13, 45, 0, 3, 30, 0),
    "tags": [{"term": "Interest rates"}],
}

SAMPLE_RSS_ENTRY_BULLETIN = {
    "title": "Economic Bulletin Issue 1, 2026",
    "link": "https://www.ecb.europa.eu/pub/economic-bulletin/html/eb202601.en.html",
    "summary": "ECB Economic Bulletin quarterly report",
    "id": "ecb.eb202601",
    "published": "Mon, 27 Jan 2026 09:00:00 GMT",
    "published_parsed": (2026, 1, 27, 9, 0, 0, 0, 27, 0),
    "tags": [{"term": "Economic analysis"}],
}

SAMPLE_RSS_ENTRY_PRESS = {
    "title": "ECB publishes draft regulatory technical standards",
    "link": "https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.pr260208~def456.en.html",
    "summary": "New regulatory framework published",
    "id": "ecb.pr260208~def456",
    "published": "Sat, 8 Feb 2026 14:30:00 GMT",
    "published_parsed": (2026, 2, 8, 14, 30, 0, 5, 39, 0),
    "tags": [{"term": "Regulation"}],
}

SAMPLE_HTML_CONTENT = """
<!DOCTYPE html>
<html>
<head><title>Test Document</title></head>
<body>
<nav>Navigation menu</nav>
<header>Header content</header>
<main>
<article>
<div class="content">
<h1>Test Document Title</h1>
<p>This is the main content of the test document.</p>
<p>It contains multiple paragraphs with relevant information.</p>
<p>This content should be extracted by the collector.</p>
</div>
</article>
</main>
<footer>Footer content</footer>
<script>console.log('test');</script>
</body>
</html>
"""


def make_mock_feed(entries: list[dict]) -> Mock:
    """Create mock feedparser.FeedParserDict."""
    feed = Mock()
    feed.entries = entries
    feed.bozo = False
    return feed


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Temporary output directory for tests."""
    output_dir = tmp_path / "raw" / "news" / "ecb"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


@pytest.fixture
def collector(tmp_output_dir):
    """ECBNewsCollector instance with temporary output."""
    return ECBNewsCollector(output_dir=tmp_output_dir)


# ---------------------------------------------------------------------------
# ECBNewsDocument
# ---------------------------------------------------------------------------


class TestECBNewsDocument:
    """Test ECBNewsDocument dataclass."""

    def test_creation(self):
        doc = ECBNewsDocument(
            source="ecb",
            timestamp_collected="2026-02-11T10:00:00Z",
            timestamp_published="2026-02-11T09:00:00Z",
            url="https://example.com",
            title="Test Title",
            content="Test content",
            document_type="speeches",
            speaker="Christine Lagarde",
            language="en",
            metadata={"key": "value"},
        )
        assert doc.source == "ecb"
        assert doc.speaker == "Christine Lagarde"
        assert doc.document_type == "speeches"

    def test_to_dict(self):
        doc = ECBNewsDocument(
            source="ecb",
            timestamp_collected="2026-02-11T10:00:00Z",
            timestamp_published="2026-02-11T09:00:00Z",
            url="https://example.com",
            title="Test",
            content="Content",
            document_type="policy",
            speaker=None,
            language="en",
        )
        doc_dict = doc.to_dict()
        assert isinstance(doc_dict, dict)
        assert doc_dict["source"] == "ecb"
        assert doc_dict["speaker"] is None
        assert "metadata" in doc_dict


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestECBNewsCollectorInit:
    """Test ECBNewsCollector initialization."""

    def test_init_custom_output_dir(self, tmp_output_dir):
        collector = ECBNewsCollector(output_dir=tmp_output_dir)
        assert collector.SOURCE_NAME == "ecb"
        assert collector.output_dir == tmp_output_dir
        assert collector.output_dir.exists()


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    """Test health_check method."""

    @patch("src.ingestion.collectors.ecb_news_collector.requests.Session.head")
    def test_health_check_success(self, mock_head, collector):
        mock_response = Mock()
        mock_response.ok = True
        mock_head.return_value = mock_response

        assert collector.health_check() is True
        mock_head.assert_called_once()

    @patch("src.ingestion.collectors.ecb_news_collector.requests.Session.head")
    def test_health_check_failure(self, mock_head, collector):
        mock_head.side_effect = Exception("Network error")

        assert collector.health_check() is False


# ---------------------------------------------------------------------------
# Date Parsing
# ---------------------------------------------------------------------------


class TestDateParsing:
    """Test _parse_entry_date method."""

    def test_parse_published_parsed(self, collector):
        entry = {
            "published_parsed": (2026, 2, 11, 10, 30, 0, 1, 42, 0),
        }
        dt = collector._parse_entry_date(entry)
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 2
        assert dt.day == 11
        assert dt.hour == 10
        assert dt.minute == 30
        assert dt.tzinfo == timezone.utc

    def test_parse_updated_parsed_fallback(self, collector):
        entry = {
            "updated_parsed": (2025, 12, 15, 14, 0, 0, 0, 349, 0),
        }
        dt = collector._parse_entry_date(entry)
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 12

    def test_parse_published_string(self, collector):
        entry = {
            "published": "Tue, 11 Feb 2026 10:00:00 GMT",
        }
        dt = collector._parse_entry_date(entry)
        # Should parse successfully
        assert dt is not None

    def test_parse_no_date_returns_none(self, collector):
        entry = {"title": "No date"}
        dt = collector._parse_entry_date(entry)
        assert dt is None


# ---------------------------------------------------------------------------
# Document Type Classification
# ---------------------------------------------------------------------------


class TestDocumentClassification:
    """Test _classify_document_type method."""

    def test_classify_policy(self, collector):
        doc_type = collector._classify_document_type(
            "Monetary policy decisions", "Governing Council interest rate"
        )
        assert doc_type == "policy"

    def test_classify_speech(self, collector):
        doc_type = collector._classify_document_type(
            "Speech by Christine Lagarde", "Keynote address"
        )
        assert doc_type == "speeches"

    def test_classify_bulletin(self, collector):
        doc_type = collector._classify_document_type("Economic Bulletin Issue 1", "")
        assert doc_type == "bulletins"

    def test_classify_press_release_default(self, collector):
        doc_type = collector._classify_document_type("ECB publishes report", "New statistical data")
        assert doc_type == "pressreleases"


# ---------------------------------------------------------------------------
# Speaker Extraction
# ---------------------------------------------------------------------------


class TestSpeakerExtraction:
    """Test _extract_speaker_name method."""

    def test_extract_from_speech_by_pattern(self, collector):
        title = "Speech by Christine Lagarde, President of the ECB"
        speaker = collector._extract_speaker_name(title)
        assert speaker == "Christine Lagarde"

    def test_extract_from_remarks_by_pattern(self, collector):
        title = "Remarks by Philip Lane at the conference"
        speaker = collector._extract_speaker_name(title)
        assert speaker == "Philip Lane"

    def test_extract_no_speaker(self, collector):
        title = "Monetary policy decisions"
        speaker = collector._extract_speaker_name(title)
        assert speaker is None


# ---------------------------------------------------------------------------
# Content Fetching
# ---------------------------------------------------------------------------


class TestContentFetching:
    """Test _fetch_full_content method."""

    @patch("src.ingestion.collectors.ecb_news_collector.requests.Session.get")
    def test_fetch_content_success(self, mock_get, collector):
        mock_response = Mock()
        mock_response.content = SAMPLE_HTML_CONTENT.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        content = collector._fetch_full_content("https://example.com")
        assert "main content of the test document" in content
        assert "Navigation menu" not in content  # Should be removed
        assert "console.log" not in content  # Script should be removed

    @patch("src.ingestion.collectors.ecb_news_collector.requests.Session.get")
    def test_fetch_content_failure(self, mock_get, collector):
        mock_get.side_effect = Exception("Network error")

        content = collector._fetch_full_content("https://example.com")
        assert content == ""


# ---------------------------------------------------------------------------
# Document Extraction
# ---------------------------------------------------------------------------


class TestDocumentExtraction:
    """Test _extract_document method."""

    @patch.object(ECBNewsCollector, "_fetch_full_content")
    def test_extract_speech_document(self, mock_fetch, collector):
        mock_fetch.return_value = "Full speech content here"

        pub_date = datetime(2026, 2, 11, 10, 0, 0, tzinfo=timezone.utc)
        doc = collector._extract_document(SAMPLE_RSS_ENTRY_SPEECH, pub_date)

        assert doc is not None
        assert doc["source"] == "ecb"
        assert doc["document_type"] == "speech"
        assert doc["title"] == SAMPLE_RSS_ENTRY_SPEECH["title"]
        assert doc["url"] == SAMPLE_RSS_ENTRY_SPEECH["link"]
        assert doc["speaker"] == "Christine Lagarde"
        assert doc["language"] == "en"
        assert "timestamp_collected" in doc
        assert doc["timestamp_published"] == pub_date.isoformat()

    @patch.object(ECBNewsCollector, "_fetch_full_content")
    def test_extract_policy_document(self, mock_fetch, collector):
        mock_fetch.return_value = "Policy decision content"

        pub_date = datetime(2026, 1, 30, 13, 45, 0, tzinfo=timezone.utc)
        doc = collector._extract_document(SAMPLE_RSS_ENTRY_POLICY, pub_date)

        assert doc is not None
        assert doc["document_type"] == "policy_decision"
        assert doc["speaker"] is None  # No speaker for policy docs

    def test_extract_missing_required_fields(self, collector):
        entry = {"summary": "No title or link"}
        pub_date = datetime.now(timezone.utc)

        doc = collector._extract_document(entry, pub_date)
        assert doc is None


# ---------------------------------------------------------------------------
# Collect Method
# ---------------------------------------------------------------------------


class TestCollectMethod:
    """Test collect method (pure, no I/O)."""

    @patch("src.ingestion.collectors.ecb_news_collector.feedparser.parse")
    @patch.object(ECBNewsCollector, "_fetch_full_content")
    def test_collect_success(self, mock_fetch_content, mock_parse, collector):
        # Setup mocks
        mock_feed = make_mock_feed(
            [
                SAMPLE_RSS_ENTRY_SPEECH,
                SAMPLE_RSS_ENTRY_POLICY,
                SAMPLE_RSS_ENTRY_BULLETIN,
                SAMPLE_RSS_ENTRY_PRESS,
            ]
        )
        mock_parse.return_value = mock_feed
        mock_fetch_content.return_value = "Full content here"

        # Collect
        start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2026, 2, 28, tzinfo=timezone.utc)

        data = collector.collect(start_date=start_date, end_date=end_date)

        # Verify structure
        assert isinstance(data, dict)
        assert set(data.keys()) == {"pressreleases", "speeches", "policy", "bulletins"}

        # Verify content
        assert len(data["speeches"]) == 1
        assert len(data["policy"]) == 1
        assert len(data["bulletins"]) == 1
        assert len(data["pressreleases"]) == 1

        # Verify speech document
        speech = data["speeches"][0]
        assert speech["source"] == "ecb"
        assert speech["document_type"] == "speech"
        assert "Christine Lagarde" in speech["title"]

    @patch("src.ingestion.collectors.ecb_news_collector.feedparser.parse")
    def test_collect_date_filtering(self, mock_parse, collector):
        # Entry outside date range
        old_entry = SAMPLE_RSS_ENTRY_SPEECH.copy()
        old_entry["published_parsed"] = (2025, 1, 1, 10, 0, 0, 0, 1, 0)

        mock_feed = make_mock_feed([old_entry, SAMPLE_RSS_ENTRY_POLICY])
        mock_parse.return_value = mock_feed

        # Collect only February 2026
        start_date = datetime(2026, 2, 1, tzinfo=timezone.utc)
        end_date = datetime(2026, 2, 28, tzinfo=timezone.utc)

        with patch.object(collector, "_fetch_full_content", return_value="content"):
            data = collector.collect(start_date=start_date, end_date=end_date)

        # Only the January 2026 policy entry should be excluded
        # (Old entry from 2025 should be filtered out)
        total_docs = sum(len(docs) for docs in data.values())
        assert total_docs == 0  # Both entries are outside Feb 2026 range

    @patch("src.ingestion.collectors.ecb_news_collector.feedparser.parse")
    def test_collect_invalid_date_range(self, mock_parse, collector):
        start_date = datetime(2026, 2, 1, tzinfo=timezone.utc)
        end_date = datetime(2026, 1, 1, tzinfo=timezone.utc)

        with pytest.raises(ValueError, match="must be before end_date"):
            collector.collect(start_date=start_date, end_date=end_date)

    @patch("src.ingestion.collectors.ecb_news_collector.feedparser.parse")
    def test_collect_empty_feed(self, mock_parse, collector):
        mock_feed = make_mock_feed([])
        mock_parse.return_value = mock_feed

        data = collector.collect()

        assert all(len(docs) == 0 for docs in data.values())

    @patch("src.ingestion.collectors.ecb_news_collector.feedparser.parse")
    def test_collect_no_entries_attribute(self, mock_parse, collector):
        mock_feed = Mock()
        mock_feed.entries = None
        mock_parse.return_value = mock_feed

        with pytest.raises(RuntimeError, match="Failed to fetch RSS feed"):
            collector.collect()


# ---------------------------------------------------------------------------
# Export JSONL
# ---------------------------------------------------------------------------


class TestExportJSONL:
    """Test export_jsonl method."""

    def test_export_jsonl_success(self, collector, tmp_output_dir):
        documents = [
            {
                "source": "ecb",
                "title": "Test Document 1",
                "content": "Content 1",
                "document_type": "speech",
            },
            {
                "source": "ecb",
                "title": "Test Document 2",
                "content": "Content 2",
                "document_type": "speech",
            },
        ]

        path = collector.export_jsonl(documents, "speeches")

        assert path.exists()
        assert path.name.startswith("speeches_")
        assert path.suffix == ".jsonl"

        # Read and verify JSONL format
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 2
        doc1 = json.loads(lines[0])
        assert doc1["title"] == "Test Document 1"
        doc2 = json.loads(lines[1])
        assert doc2["title"] == "Test Document 2"

    def test_export_jsonl_empty_data(self, collector):
        with pytest.raises(ValueError, match="Cannot export empty data"):
            collector.export_jsonl([], "speeches")

    def test_export_jsonl_invalid_type(self, collector):
        documents = [{"title": "Test"}]
        with pytest.raises(ValueError, match="Invalid document_type"):
            collector.export_jsonl(documents, "invalid_type")

    def test_export_jsonl_custom_date(self, collector, tmp_output_dir):
        documents = [{"source": "ecb", "title": "Test"}]
        collection_date = datetime(2025, 12, 15)

        path = collector.export_jsonl(documents, "policy", collection_date)

        assert "20251215" in path.name


# ---------------------------------------------------------------------------
# Export All to JSONL
# ---------------------------------------------------------------------------


class TestExportAll:
    """Test export_all method (inherited from DocumentCollector)."""

    def test_export_all_with_data(self, collector, tmp_output_dir):
        data = {
            "speeches": [{"source": "ecb", "title": "Speech 1"}],
            "policy": [{"source": "ecb", "title": "Policy 1"}],
            "bulletins": [],  # Empty
            "pressreleases": [{"source": "ecb", "title": "Press 1"}],
        }

        paths = collector.export_all(data=data)

        assert len(paths) == 3  # Only non-empty types
        assert "speeches" in paths
        assert "policy" in paths
        assert "pressreleases" in paths
        assert "bulletins" not in paths  # Empty, not exported

        # Verify files exist
        for path in paths.values():
            assert path.exists()

    @patch.object(ECBNewsCollector, "collect")
    def test_export_all_without_data_collects(self, mock_collect, collector, tmp_output_dir):
        mock_collect.return_value = {
            "speeches": [{"source": "ecb", "title": "Speech 1"}],
            "policy": [],
            "bulletins": [],
            "pressreleases": [],
        }

        start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2026, 2, 1, tzinfo=timezone.utc)

        paths = collector.export_all(start_date=start_date, end_date=end_date)

        mock_collect.assert_called_once_with(start_date=start_date, end_date=end_date)
        assert len(paths) == 1
        assert "speeches" in paths


# ---------------------------------------------------------------------------
# Integration Test
# ---------------------------------------------------------------------------


class TestIntegration:
    """Integration test for full workflow."""

    @patch("src.ingestion.collectors.ecb_news_collector.feedparser.parse")
    @patch.object(ECBNewsCollector, "_fetch_full_content")
    def test_full_collection_and_export_workflow(
        self, mock_fetch_content, mock_parse, tmp_output_dir
    ):
        # Setup
        collector = ECBNewsCollector(output_dir=tmp_output_dir)

        mock_feed = make_mock_feed(
            [
                SAMPLE_RSS_ENTRY_SPEECH,
                SAMPLE_RSS_ENTRY_POLICY,
            ]
        )
        mock_parse.return_value = mock_feed
        mock_fetch_content.return_value = "Full content text here"

        # Collect
        start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2026, 2, 28, tzinfo=timezone.utc)

        data = collector.collect(start_date=start_date, end_date=end_date)

        # Verify collection
        assert len(data["speeches"]) == 1
        assert len(data["policy"]) == 1

        # Export
        paths = collector.export_all(data=data)

        # Verify exports
        assert len(paths) == 2
        assert paths["speeches"].exists()
        assert paths["policy"].exists()

        # Verify JSONL content
        with open(paths["speeches"], encoding="utf-8") as f:
            speech_doc = json.loads(f.readline())
            assert speech_doc["source"] == "ecb"
            assert speech_doc["document_type"] == "speech"
            assert "Christine Lagarde" in speech_doc["title"]
