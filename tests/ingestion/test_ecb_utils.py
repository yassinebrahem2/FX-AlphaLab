"""Tests for ECB shared utilities."""

from unittest.mock import Mock, patch

import pytest
import requests

from src.ingestion.collectors import ecb_utils
from src.ingestion.collectors.ecb_utils import ECBNewsDocument

# ---------------------------------------------------------------------------
# ECBNewsDocument
# ---------------------------------------------------------------------------


class TestECBNewsDocument:
    """Test ECBNewsDocument dataclass."""

    def test_creation(self):
        doc = ECBNewsDocument(
            source="ecb",
            timestamp_collected="2026-02-15T10:00:00+00:00",
            timestamp_published="2026-02-15T09:00:00+00:00",
            url="https://ecb.europa.eu/test",
            title="Test Title",
            content="Test content",
            document_type="speech",
            speaker="Christine Lagarde",
            language="en",
            metadata={"rss_id": "123"},
        )
        assert doc.source == "ecb"
        assert doc.speaker == "Christine Lagarde"

    def test_to_dict(self):
        doc = ECBNewsDocument(
            source="ecb",
            timestamp_collected="2026-02-15T10:00:00+00:00",
            timestamp_published="2026-02-15T09:00:00+00:00",
            url="https://ecb.europa.eu/test",
            title="Test",
            content="Content",
            document_type="press_release",
            speaker=None,
            language="en",
        )
        d = doc.to_dict()
        assert isinstance(d, dict)
        assert d["source"] == "ecb"
        assert d["speaker"] is None
        assert "metadata" in d

    def test_default_metadata(self):
        doc = ECBNewsDocument(
            source="ecb",
            timestamp_collected="t",
            timestamp_published="t",
            url="u",
            title="t",
            content="c",
            document_type="speech",
            speaker=None,
            language="en",
        )
        assert doc.metadata == {}


# ---------------------------------------------------------------------------
# classify_document_type
# ---------------------------------------------------------------------------


class TestClassifyDocumentType:
    """Test document type classification."""

    @pytest.mark.parametrize(
        "title,expected",
        [
            ("Monetary policy decisions of the ECB", "policy"),
            ("Governing Council meeting summary", "policy"),
            ("Key ECB interest rates update", "policy"),
            ("Speech by Christine Lagarde at forum", "speeches"),
            ("Remarks by Philip Lane on inflation", "speeches"),
            ("Interview with Isabel Schnabel", "speeches"),
            ("Keynote address at conference", "speeches"),
            ("Economic Bulletin Issue 1/2026", "bulletins"),
            ("Monthly Bulletin January 2026", "bulletins"),
            ("ECB launches new banknote design", "pressreleases"),
            ("Staff changes at ECB", "pressreleases"),
        ],
    )
    def test_classification(self, title: str, expected: str):
        assert ecb_utils.classify_document_type(title) == expected

    def test_summary_contributes(self):
        result = ecb_utils.classify_document_type(
            "General title", "This contains monetary policy decisions"
        )
        assert result == "policy"

    def test_empty_strings(self):
        assert ecb_utils.classify_document_type("", "") == "pressreleases"


# ---------------------------------------------------------------------------
# extract_speaker_name
# ---------------------------------------------------------------------------


class TestExtractSpeakerName:
    """Test speaker name extraction."""

    def test_speech_by_pattern(self):
        name = ecb_utils.extract_speaker_name("Speech by Christine Lagarde at ECB Forum")
        assert name == "Christine Lagarde"

    def test_remarks_by_pattern(self):
        name = ecb_utils.extract_speaker_name("Remarks by Philip Lane at ECB conference")
        assert name == "Philip Lane"

    def test_president_pattern(self):
        name = ecb_utils.extract_speaker_name("Christine Lagarde, President of the ECB")
        assert name == "Christine Lagarde"

    def test_no_speaker(self):
        assert ecb_utils.extract_speaker_name("ECB press release on rates") is None

    def test_empty_input(self):
        assert ecb_utils.extract_speaker_name("") is None

    def test_content_fallback(self):
        name = ecb_utils.extract_speaker_name(
            "Some title", "Speech by Isabel Schnabel at conference"
        )
        assert name == "Isabel Schnabel"


# ---------------------------------------------------------------------------
# fetch_full_content
# ---------------------------------------------------------------------------


class TestFetchFullContent:
    """Test content extraction from HTML pages."""

    def test_empty_url(self):
        result = ecb_utils.fetch_full_content(Mock(), "", Mock(), lambda: None, 30)
        assert result == ""

    @patch("src.ingestion.collectors.ecb_utils.requests.Session.get")
    def test_success(self, mock_get):
        html = """<html><body>
        <script>console.log('x')</script>
        <nav>Menu</nav>
        <article>
            <p>This is the main content of the article with enough text to pass threshold.</p>
            <p>Another paragraph with important information about monetary policy.</p>
        </article>
        </body></html>"""

        mock_response = Mock()
        mock_response.content = html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        session = requests.Session()
        result = ecb_utils.fetch_full_content(
            session, "https://example.com", Mock(), lambda: None, 30
        )

        assert "main content" in result
        assert "console.log" not in result
        assert "Menu" not in result

    def test_network_error(self):
        session = Mock()
        session.get.side_effect = Exception("Connection refused")

        result = ecb_utils.fetch_full_content(
            session, "https://example.com", Mock(), lambda: None, 30
        )
        assert result == ""


# ---------------------------------------------------------------------------
# create_ecb_session
# ---------------------------------------------------------------------------


class TestCreateECBSession:
    """Test session creation with retry logic."""

    def test_returns_session(self):
        session = ecb_utils.create_ecb_session()
        assert isinstance(session, requests.Session)

    def test_custom_params(self):
        session = ecb_utils.create_ecb_session(max_retries=5, backoff_factor=1.0)
        assert isinstance(session, requests.Session)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    """Test shared constants are properly defined."""

    def test_valid_document_types(self):
        assert ecb_utils.VALID_DOCUMENT_TYPES == {
            "pressreleases",
            "speeches",
            "policy",
            "bulletins",
        }

    def test_document_type_field_map_keys(self):
        assert set(ecb_utils.DOCUMENT_TYPE_FIELD_MAP.keys()) == ecb_utils.VALID_DOCUMENT_TYPES

    def test_document_type_field_map_values(self):
        expected = {"press_release", "speech", "policy_decision", "bulletin"}
        assert set(ecb_utils.DOCUMENT_TYPE_FIELD_MAP.values()) == expected

    def test_patterns_cover_all_types(self):
        assert set(ecb_utils.DOCUMENT_TYPE_PATTERNS.keys()) == ecb_utils.VALID_DOCUMENT_TYPES
