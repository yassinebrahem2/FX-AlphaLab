"""Unit tests for Federal Reserve RSS Feed Collector.

Tests verify that FedCollector:
- Inherits from DocumentCollector
- Fetches and parses RSS feed from Fed
- Categorizes documents by type
- Extracts full text content from URLs
- Returns dict[str, list[dict]] structure
- Exports to JSONL format
- Handles errors gracefully
"""

import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import requests

from src.ingestion.collectors.document_collector import DocumentCollector
from src.ingestion.collectors.fed_collector import FedCollector

# ---------------------------------------------------------------------------
# Test Helpers
# ---------------------------------------------------------------------------


def _make_response(content: str | bytes, status: int = 200) -> Mock:
    """Build a mock requests.Response."""
    resp = Mock()
    resp.status_code = status
    resp.content = content.encode() if isinstance(content, str) else content
    resp.text = content if isinstance(content, str) else content.decode()
    resp.ok = 200 <= status < 300
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    else:
        resp.raise_for_status = Mock()
    return resp


# Sample RSS feed XML
SAMPLE_RSS_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Board of Governors of the Federal Reserve System</title>
    <description>Latest Press Releases</description>
    <item>
      <title>Federal Reserve issues FOMC statement</title>
      <link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20240131a.htm</link>
      <description>Recent indicators suggest that economic activity has been expanding at a solid pace.</description>
      <pubDate>Wed, 31 Jan 2024 14:00:00 EST</pubDate>
      <guid>https://www.federalreserve.gov/newsevents/pressreleases/monetary20240131a.htm</guid>
    </item>
    <item>
      <title>Chair Powell remarks at Economic Forum</title>
      <link>https://www.federalreserve.gov/newsevents/speech/powell20240115a.htm</link>
      <description>Speech by Jerome H. Powell, Chair, at the Economic Club.</description>
      <pubDate>Mon, 15 Jan 2024 10:30:00 EST</pubDate>
      <guid>https://www.federalreserve.gov/newsevents/speech/powell20240115a.htm</guid>
    </item>
    <item>
      <title>Federal Reserve Board announces enforcement action</title>
      <link>https://www.federalreserve.gov/newsevents/pressreleases/enforcement20240120a.htm</link>
      <description>The Federal Reserve Board announces enforcement action.</description>
      <pubDate>Sat, 20 Jan 2024 16:00:00 EST</pubDate>
      <guid>https://www.federalreserve.gov/newsevents/pressreleases/enforcement20240120a.htm</guid>
    </item>
    <item>
      <title>Chair Powell testimony before Senate Committee</title>
      <link>https://www.federalreserve.gov/newsevents/testimony/powell20240207a.htm</link>
      <description>Testimony by Chair Jerome H. Powell before the Committee.</description>
      <pubDate>Wed, 07 Feb 2024 09:00:00 EST</pubDate>
      <guid>https://www.federalreserve.gov/newsevents/testimony/powell20240207a.htm</guid>
    </item>
  </channel>
</rss>
"""

# Sample HTML content from Fed publication
SAMPLE_HTML_CONTENT = """<!DOCTYPE html>
<html>
<head><title>FOMC Statement</title></head>
<body>
  <div id="article">
    <h1>Federal Reserve issues FOMC statement</h1>
    <p>Recent indicators suggest that economic activity has been expanding at a solid pace.
    Job gains have remained strong, and the unemployment rate has remained low.
    Inflation has eased over the past year but remains elevated.</p>
    <p>The Committee seeks to achieve maximum employment and inflation at the rate of 2 percent over the longer run.</p>
  </div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Test DocumentCollector Contract
# ---------------------------------------------------------------------------


class TestDocumentCollectorContract:
    """Verify FedCollector implements DocumentCollector interface."""

    def test_inherits_from_document_collector(self):
        """FedCollector must inherit from DocumentCollector."""
        assert issubclass(FedCollector, DocumentCollector)

    def test_has_source_name(self):
        """FedCollector must define SOURCE_NAME."""
        assert hasattr(FedCollector, "SOURCE_NAME")
        assert FedCollector.SOURCE_NAME == "fed"

    def test_implements_collect_method(self, tmp_path):
        """FedCollector must implement collect() method."""
        collector = FedCollector(output_dir=tmp_path)
        assert hasattr(collector, "collect")
        assert callable(collector.collect)

    def test_implements_health_check_method(self, tmp_path):
        """FedCollector must implement health_check() method."""
        collector = FedCollector(output_dir=tmp_path)
        assert hasattr(collector, "health_check")
        assert callable(collector.health_check)


# ---------------------------------------------------------------------------
# Test Initialization
# ---------------------------------------------------------------------------


class TestFedCollectorInit:
    """Test FedCollector initialization."""

    def test_default_output_dir(self, tmp_path, monkeypatch):
        """Test default output directory is data/raw/news/fed."""
        monkeypatch.setattr("src.shared.config.Config.DATA_DIR", tmp_path)
        collector = FedCollector()
        assert collector.output_dir == tmp_path / "raw" / "news" / "fed"
        assert collector.output_dir.exists()

    def test_custom_output_dir(self, tmp_path):
        """Test custom output directory is respected."""
        custom_dir = tmp_path / "custom_fed"
        collector = FedCollector(output_dir=custom_dir)
        assert collector.output_dir == custom_dir
        assert collector.output_dir.exists()

    def test_has_session_with_retry_logic(self, tmp_path):
        """Test session is configured with retry logic."""
        collector = FedCollector(output_dir=tmp_path)
        assert hasattr(collector, "_session")
        adapter = collector._session.get_adapter("https://")
        assert adapter is not None
        assert hasattr(adapter, "max_retries")

    def test_rss_url_is_correct(self, tmp_path):
        """Test RSS URL points to Fed press feed."""
        collector = FedCollector(output_dir=tmp_path)
        assert collector.RSS_URL == "https://www.federalreserve.gov/feeds/press_all.xml"


# ---------------------------------------------------------------------------
# Test Health Check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    """Test health_check() method."""

    def test_health_check_success(self, tmp_path):
        """Test health check returns True when RSS feed is available."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "head") as mock_head:
            mock_head.return_value = _make_response("", 200)
            result = collector.health_check()

            assert result is True
            mock_head.assert_called_once()

    def test_health_check_failure_http_error(self, tmp_path):
        """Test health check returns False on HTTP error."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "head") as mock_head:
            mock_head.return_value = _make_response("", 404)
            result = collector.health_check()

            assert result is False

    def test_health_check_failure_connection_error(self, tmp_path):
        """Test health check returns False on connection error."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "head") as mock_head:
            mock_head.side_effect = requests.RequestException("Connection failed")
            result = collector.health_check()

            assert result is False


# ---------------------------------------------------------------------------
# Test Document Type Classification
# ---------------------------------------------------------------------------


class TestDocumentTypeClassification:
    """Test document type classification logic."""

    def test_classify_fomc_statement(self, tmp_path):
        """Test FOMC statement is correctly classified."""
        collector = FedCollector(output_dir=tmp_path)

        result = collector._classify_document_type(
            "Federal Reserve issues FOMC statement", "The Federal Open Market Committee decided..."
        )

        assert result.name == "fomc_statement"
        assert result.key == "statements"

    def test_classify_speech(self, tmp_path):
        """Test speech is correctly classified."""
        collector = FedCollector(output_dir=tmp_path)

        result = collector._classify_document_type(
            "Chair Powell remarks at Economic Forum",
            "Speech by Jerome H. Powell at the conference...",
        )

        assert result.name == "speech"
        assert result.key == "speeches"

    def test_classify_testimony(self, tmp_path):
        """Test testimony is correctly classified."""
        collector = FedCollector(output_dir=tmp_path)

        result = collector._classify_document_type(
            "Testimony before Senate Committee",
            "Testimony by Federal Reserve officials before Congress...",
        )

        assert result.name == "testimony"
        assert result.key == "testimony"

    def test_classify_press_release(self, tmp_path):
        """Test press release is correctly classified."""
        collector = FedCollector(output_dir=tmp_path)

        result = collector._classify_document_type(
            "Federal Reserve announces enforcement action", "The Federal Reserve Board announces..."
        )

        assert result.name == "press_release"
        assert result.key == "press_releases"

    def test_classify_minutes(self, tmp_path):
        """Test meeting minutes are correctly classified."""
        collector = FedCollector(output_dir=tmp_path)

        result = collector._classify_document_type(
            "Minutes of the January Meeting",
            "Meeting minutes from the monetary policy meeting held on January 30-31, 2024",
        )

        assert result.name == "minutes"
        assert result.key == "minutes"


# ---------------------------------------------------------------------------
# Test Speaker Extraction
# ---------------------------------------------------------------------------


class TestSpeakerExtraction:
    """Test speaker name extraction from speech titles."""

    def test_extract_chair_speaker(self, tmp_path):
        """Test extracting Chair name."""
        collector = FedCollector(output_dir=tmp_path)

        result = collector._extract_speaker("Chair Powell remarks at Economic Forum")

        assert result == "Chair Powell"

    def test_extract_governor_speaker(self, tmp_path):
        """Test extracting Governor name."""
        collector = FedCollector(output_dir=tmp_path)

        result = collector._extract_speaker("Governor Waller speech on monetary policy")

        assert result == "Governor Waller"

    def test_extract_vice_chair_speaker(self, tmp_path):
        """Test extracting Vice Chair name."""
        collector = FedCollector(output_dir=tmp_path)

        result = collector._extract_speaker("Vice Chair Williams remarks on economy")

        assert result == "Vice Chair Williams"

    def test_no_speaker_in_title(self, tmp_path):
        """Test when no speaker is in title."""
        collector = FedCollector(output_dir=tmp_path)

        result = collector._extract_speaker("FOMC statement on monetary policy")

        assert result == ""


# ---------------------------------------------------------------------------
# Test RSS Parsing and Collection
# ---------------------------------------------------------------------------


class TestRSSParsingAndCollection:
    """Test RSS feed parsing and data collection."""

    @patch("time.sleep")  # Skip delays in tests
    def test_parse_rss_feed_with_mock_data(self, mock_sleep, tmp_path):
        """Test RSS feed is parsed correctly with mock data."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "get") as mock_get:
            mock_get.return_value = _make_response(SAMPLE_RSS_FEED)

            # Mock content extraction to avoid actual HTTP requests
            with patch.object(collector, "_extract_content_from_url") as mock_extract:
                mock_extract.return_value = "Sample content text"

                result = collector.fetch_and_categorize_publications(
                    start_date=datetime(2024, 1, 1), end_date=datetime(2024, 2, 29)
                )

                # Verify result structure
                assert isinstance(result, dict)
                assert "statements" in result
                assert "speeches" in result
                assert "press_releases" in result
                assert "testimony" in result
                assert "minutes" in result

    @patch("time.sleep")
    def test_collect_categorizes_documents(self, mock_sleep, tmp_path):
        """Test collect() returns properly categorized documents."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "get") as mock_get:
            mock_get.return_value = _make_response(SAMPLE_RSS_FEED)

            with patch.object(collector, "_extract_content_from_url") as mock_extract:
                mock_extract.return_value = "Full text content"

                result = collector.collect(
                    start_date=datetime(2024, 1, 1), end_date=datetime(2024, 2, 29)
                )

                # Check return type
                assert isinstance(result, dict)

                # Check all document types have list values
                for doc_type, docs in result.items():
                    assert isinstance(docs, list)
                    for doc in docs:
                        assert isinstance(doc, dict)

    @patch("time.sleep")
    def test_collected_document_schema(self, mock_sleep, tmp_path):
        """Test collected documents have correct schema."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "get") as mock_get:
            mock_get.return_value = _make_response(SAMPLE_RSS_FEED)

            with patch.object(collector, "_extract_content_from_url") as mock_extract:
                mock_extract.return_value = "Full text content"

                result = collector.collect(
                    start_date=datetime(2024, 1, 1), end_date=datetime(2024, 2, 29)
                )

                # Get first document from any non-empty category
                doc = None
                for docs in result.values():
                    if docs:
                        doc = docs[0]
                        break

                if doc:
                    # Verify required fields
                    assert "source" in doc
                    assert doc["source"] == "fed"
                    assert "timestamp_collected" in doc
                    assert "timestamp_published" in doc
                    assert "url" in doc
                    assert "title" in doc
                    assert "content" in doc
                    assert "document_type" in doc
                    assert "speaker" in doc
                    assert "metadata" in doc

                    # Verify metadata is a dict (not JSON string)
                    assert isinstance(doc["metadata"], dict)


# ---------------------------------------------------------------------------
# Test Timestamp Handling
# ---------------------------------------------------------------------------


class TestTimestampHandling:
    """Test timestamp parsing and normalization."""

    @patch("time.sleep")
    def test_timestamps_are_utc_iso8601(self, mock_sleep, tmp_path):
        """Test timestamps are in UTC ISO 8601 format."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "get") as mock_get:
            mock_get.return_value = _make_response(SAMPLE_RSS_FEED)

            with patch.object(collector, "_extract_content_from_url") as mock_extract:
                mock_extract.return_value = "Content"

                result = collector.collect(
                    start_date=datetime(2024, 1, 1), end_date=datetime(2024, 2, 29)
                )

                # Check timestamp format
                for docs in result.values():
                    for doc in docs:
                        # Parse timestamps to verify format
                        datetime.fromisoformat(doc["timestamp_collected"])
                        datetime.fromisoformat(doc["timestamp_published"])

                        # Verify UTC timezone
                        assert "+00:00" in doc["timestamp_published"] or doc[
                            "timestamp_published"
                        ].endswith("Z")


# ---------------------------------------------------------------------------
# Test Error Handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Test error handling and retry logic."""

    def test_collect_raises_on_invalid_date_range(self, tmp_path):
        """Test collect() raises ValueError for invalid date range."""
        collector = FedCollector(output_dir=tmp_path)

        with pytest.raises(ValueError, match="start_date.*must be before.*end_date"):
            collector.collect(start_date=datetime(2024, 2, 1), end_date=datetime(2024, 1, 1))

    @patch("time.sleep")
    def test_handles_empty_rss_feed(self, mock_sleep, tmp_path):
        """Test handling of empty RSS feed."""
        collector = FedCollector(output_dir=tmp_path)

        empty_feed = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>Fed Press</title>
          </channel>
        </rss>
        """

        with patch.object(collector._session, "get") as mock_get:
            mock_get.return_value = _make_response(empty_feed)

            result = collector.collect()

            # Should return empty lists for all document types
            assert all(len(docs) == 0 for docs in result.values())

    @patch("time.sleep")
    def test_handles_malformed_rss(self, mock_sleep, tmp_path):
        """Test handling of malformed RSS feed."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "get") as mock_get:
            mock_get.return_value = _make_response("<invalid>xml</invalid>")

            result = collector.collect()

            # Should handle gracefully and return empty results
            assert isinstance(result, dict)

    @patch("time.sleep")
    def test_handles_failed_content_extraction(self, mock_sleep, tmp_path):
        """Test handling when content extraction fails."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "get") as mock_get:
            # RSS feed succeeds
            mock_get.return_value = _make_response(SAMPLE_RSS_FEED)

            # Content extraction fails
            with patch.object(collector, "_extract_content_from_url") as mock_extract:
                mock_extract.return_value = ""  # Empty content

                result = collector.collect(
                    start_date=datetime(2024, 1, 1), end_date=datetime(2024, 2, 29)
                )

                # Should still collect documents even with empty content
                assert isinstance(result, dict)
                for docs in result.values():
                    for doc in docs:
                        assert "content" in doc


# ---------------------------------------------------------------------------
# Test JSONL Export
# ---------------------------------------------------------------------------


class TestJSONLExport:
    """Test JSONL export functionality."""

    def test_export_jsonl_creates_file(self, tmp_path):
        """Test export_jsonl creates JSONL file."""
        collector = FedCollector(output_dir=tmp_path)

        documents = [
            {
                "source": "fed",
                "timestamp_collected": "2024-01-15T12:00:00+00:00",
                "timestamp_published": "2024-01-15T14:00:00+00:00",
                "url": "https://example.com/doc1",
                "title": "Test Document",
                "content": "Content here",
                "document_type": "fomc_statement",
                "speaker": "",
                "metadata": {"key": "value"},
            }
        ]

        path = collector.export_jsonl(documents, "statements")

        assert path.exists()
        assert path.suffix == ".jsonl"
        assert "statements" in path.name

    def test_export_jsonl_correct_format(self, tmp_path):
        """Test exported JSONL has correct format (one JSON per line)."""
        collector = FedCollector(output_dir=tmp_path)

        documents = [
            {"title": "Doc 1", "source": "fed", "metadata": {}},
            {"title": "Doc 2", "source": "fed", "metadata": {}},
        ]

        path = collector.export_jsonl(documents, "test")

        # Read and verify
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 2
        # Each line should be valid JSON
        for line in lines:
            doc = json.loads(line)
            assert isinstance(doc, dict)
            assert "title" in doc

    def test_export_jsonl_preserves_unicode(self, tmp_path):
        """Test JSONL export preserves Unicode characters."""
        collector = FedCollector(output_dir=tmp_path)

        documents = [
            {
                "title": "Document with €uro and ¥en symbols",
                "content": "Résumé façade naïve",
                "source": "fed",
                "metadata": {},
            }
        ]

        path = collector.export_jsonl(documents, "test")

        # Read back
        with open(path, encoding="utf-8") as f:
            doc = json.loads(f.readline())

        assert "€uro" in doc["title"]
        assert "Résumé" in doc["content"]

    def test_export_jsonl_raises_on_empty(self, tmp_path):
        """Test export_jsonl raises ValueError for empty list."""
        collector = FedCollector(output_dir=tmp_path)

        with pytest.raises(ValueError, match="Cannot export empty document list"):
            collector.export_jsonl([], "empty")

    def test_export_jsonl_filename_includes_date(self, tmp_path):
        """Test JSONL filename includes date."""
        collector = FedCollector(output_dir=tmp_path)

        documents = [{"title": "Test", "source": "fed", "metadata": {}}]

        path = collector.export_jsonl(documents, "statements")

        # Filename should match pattern: statements_YYYYMMDD.jsonl
        import re

        assert re.match(r"statements_\d{8}\.jsonl", path.name)

    @patch("time.sleep")
    def test_export_all_exports_all_types(self, mock_sleep, tmp_path):
        """Test export_all exports all document types."""
        collector = FedCollector(output_dir=tmp_path)

        with patch.object(collector._session, "get") as mock_get:
            mock_get.return_value = _make_response(SAMPLE_RSS_FEED)

            with patch.object(collector, "_extract_content_from_url") as mock_extract:
                mock_extract.return_value = "Content"

                paths = collector.export_all(
                    start_date=datetime(2024, 1, 1), end_date=datetime(2024, 2, 29)
                )

                # Should return dict of paths
                assert isinstance(paths, dict)

                # All exported paths should exist
                for doc_type, path in paths.items():
                    assert path.exists()
                    assert path.suffix == ".jsonl"

    def test_export_all_with_provided_data(self, tmp_path):
        """Test export_all with pre-collected data."""
        collector = FedCollector(output_dir=tmp_path)

        data = {
            "statements": [{"title": "FOMC", "source": "fed", "metadata": {}}],
            "speeches": [{"title": "Speech", "source": "fed", "metadata": {}}],
            "press_releases": [],
            "testimony": [],
            "minutes": [],
        }

        paths = collector.export_all(data=data)

        # Should export only non-empty types
        assert "statements" in paths
        assert "speeches" in paths
        assert paths["statements"].exists()
        assert paths["speeches"].exists()

    def test_export_all_skips_empty_types(self, tmp_path):
        """Test export_all skips empty document types."""
        collector = FedCollector(output_dir=tmp_path)

        data = {
            "statements": [{"title": "Test", "source": "fed", "metadata": {}}],
            "speeches": [],
            "press_releases": [],
            "testimony": [],
            "minutes": [],
        }

        paths = collector.export_all(data=data)

        # Should only export statements
        assert len(paths) == 1
        assert "statements" in paths


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
