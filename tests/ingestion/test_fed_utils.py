"""Tests for Fed utilities (fed_utils.py)."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import requests

from src.ingestion.collectors.fed_utils import (
    BASE_URL,
    CATEGORY_TO_DOCUMENT_TYPE,
    classify_document_type,
    create_fed_session,
    extract_speaker_name,
    fetch_full_content,
    parse_date_from_text,
)


class TestConstants:
    """Test module constants."""

    def test_base_url(self):
        """BASE_URL should point to Federal Reserve."""
        assert BASE_URL == "https://www.federalreserve.gov"

    def test_category_mapping(self):
        """CATEGORY_TO_DOCUMENT_TYPE should map all Fed categories."""
        assert CATEGORY_TO_DOCUMENT_TYPE["Monetary Policy"] == "policy"
        assert CATEGORY_TO_DOCUMENT_TYPE["Orders on Banking Applications"] == "regulation"
        assert CATEGORY_TO_DOCUMENT_TYPE["Enforcement Actions"] == "regulation"
        assert CATEGORY_TO_DOCUMENT_TYPE["Banking and Consumer Regulatory Policy"] == "regulation"
        assert CATEGORY_TO_DOCUMENT_TYPE["Other Announcements"] == "other"


class TestParseDateFromText:
    """Test date parsing from MM/DD/YYYY format."""

    def test_parse_valid_date(self):
        """Should parse MM/DD/YYYY format correctly."""
        result = parse_date_from_text("12/30/2025")
        assert result == datetime(2025, 12, 30)

    def test_parse_single_digit_month_day(self):
        """Should parse single-digit month and day."""
        result = parse_date_from_text("1/5/2024")
        assert result == datetime(2024, 1, 5)

    def test_parse_with_whitespace(self):
        """Should strip whitespace before parsing."""
        result = parse_date_from_text("  12/30/2025  ")
        assert result == datetime(2025, 12, 30)

    def test_parse_invalid_format(self):
        """Should return None for invalid format."""
        assert parse_date_from_text("2025-12-30") is None
        assert parse_date_from_text("Dec 30, 2025") is None
        assert parse_date_from_text("invalid") is None

    def test_parse_invalid_date(self):
        """Should return None for invalid date values."""
        assert parse_date_from_text("13/40/2025") is None
        assert parse_date_from_text("2/30/2025") is None

    def test_parse_none(self):
        """Should return None for None input."""
        assert parse_date_from_text(None) is None


class TestClassifyDocumentType:
    """Test document type classification."""

    def test_classify_speech(self):
        """Speeches should always map to 'speeches' regardless of category."""
        assert classify_document_type("Monetary Policy", is_speech=True) == "speeches"
        assert classify_document_type(None, is_speech=True) == "speeches"

    def test_classify_monetary_policy(self):
        """'Monetary Policy' category should map to 'policy'."""
        assert classify_document_type("Monetary Policy", is_speech=False) == "policy"

    def test_classify_regulation_categories(self):
        """Regulation categories should map to 'regulation'."""
        assert (
            classify_document_type("Orders on Banking Applications", is_speech=False)
            == "regulation"
        )
        assert classify_document_type("Enforcement Actions", is_speech=False) == "regulation"
        assert (
            classify_document_type("Banking and Consumer Regulatory Policy", is_speech=False)
            == "regulation"
        )

    def test_classify_other(self):
        """'Other Announcements' should map to 'other'."""
        assert classify_document_type("Other Announcements", is_speech=False) == "other"

    def test_classify_unknown_category(self):
        """Unknown categories should default to 'other'."""
        assert classify_document_type("Unknown Category", is_speech=False) == "other"

    def test_classify_none_category(self):
        """None category should default to 'other'."""
        assert classify_document_type(None, is_speech=False) == "other"


class TestExtractSpeakerName:
    """Test speaker name extraction."""

    def test_extract_governor_title(self):
        """Should remove 'Governor' title."""
        result = extract_speaker_name("Governor Stephen I. Miran")
        assert result == "Stephen I. Miran"

    def test_extract_chair_title(self):
        """Should remove 'Chair' title."""
        result = extract_speaker_name("Chair Jerome H. Powell")
        assert result == "Jerome H. Powell"

    def test_extract_vice_chair_title(self):
        """Should remove 'Vice Chair' title."""
        result = extract_speaker_name("Vice Chair Philip N. Jefferson")
        assert result == "Philip N. Jefferson"

    def test_extract_president_title(self):
        """Should remove 'President' title."""
        result = extract_speaker_name("President John Doe")
        assert result == "John Doe"

    def test_extract_mixed_case_title(self):
        """Should handle mixed case titles."""
        result = extract_speaker_name("gOvErNoR John Smith")
        assert result == "John Smith"

    def test_extract_with_whitespace(self):
        """Should strip leading/trailing whitespace."""
        result = extract_speaker_name("  Governor John Doe  ")
        assert result == "John Doe"

    def test_extract_none(self):
        """Should return None for None input."""
        assert extract_speaker_name(None) is None

    def test_extract_empty_string(self):
        """Should handle empty strings gracefully."""
        assert extract_speaker_name("") is None


class TestFetchFullContent:
    """Test article content fetching."""

    @patch("src.ingestion.collectors.fed_utils.requests.Session")
    def test_fetch_with_relative_url(self, mock_session_class):
        """Should prepend BASE_URL for relative URLs."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.content = b"<article><p>Test content</p></article>"
        mock_session.get.return_value = mock_response

        result = fetch_full_content("/newsevents/test.htm", mock_session)

        mock_session.get.assert_called_once_with(
            "https://www.federalreserve.gov/newsevents/test.htm", timeout=30
        )
        assert "Test content" in result

    @patch("src.ingestion.collectors.fed_utils.requests.Session")
    def test_fetch_with_absolute_url(self, mock_session_class):
        """Should use absolute URLs as-is."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.content = b"<article><p>Test content</p></article>"
        mock_session.get.return_value = mock_response

        result = fetch_full_content("https://example.com/test.htm", mock_session)

        mock_session.get.assert_called_once_with("https://example.com/test.htm", timeout=30)
        assert "Test content" in result

    @patch("src.ingestion.collectors.fed_utils.requests.Session")
    def test_fetch_extracts_article_tag(self, mock_session_class):
        """Should prioritize <article> tag for content extraction."""
        mock_session = Mock()
        mock_response = Mock()
        # Content must be >100 chars to pass minimum threshold
        article_content = "Main content here. " * 10  # Repeat to exceed 100 chars
        mock_response.content = (
            b"<html><body>"
            b"<nav>Navigation</nav>"
            b"<article><p>" + article_content.encode() + b"</p></article>"
            b"<footer>Footer</footer>"
            b"</body></html>"
        )
        mock_session.get.return_value = mock_response

        result = fetch_full_content("/test.htm", mock_session)

        assert "Main content here" in result
        assert "Navigation" not in result
        assert "Footer" not in result

    @patch("src.ingestion.collectors.fed_utils.requests.Session")
    def test_fetch_removes_script_tags(self, mock_session_class):
        """Should remove <script> and <style> tags."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.content = (
            b"<article>"
            b"<p>Good content</p>"
            b"<script>alert('bad');</script>"
            b"<style>.class { color: red; }</style>"
            b"</article>"
        )
        mock_session.get.return_value = mock_response

        result = fetch_full_content("/test.htm", mock_session)

        assert "Good content" in result
        assert "alert" not in result
        assert "color: red" not in result

    @patch("src.ingestion.collectors.fed_utils.requests.Session")
    def test_fetch_http_error(self, mock_session_class):
        """Should raise HTTPError on failed request."""
        mock_session = Mock()
        mock_session.get.side_effect = requests.HTTPError("404 Not Found")

        with pytest.raises(requests.HTTPError):
            fetch_full_content("/test.htm", mock_session)


class TestCreateFedSession:
    """Test session creation."""

    def test_create_session_returns_session(self):
        """Should return a requests.Session object."""
        session = create_fed_session()
        assert isinstance(session, requests.Session)

    def test_create_session_has_retry_logic(self):
        """Should configure retry adapter for HTTPS."""
        session = create_fed_session()
        adapter = session.get_adapter("https://example.com")
        assert isinstance(adapter, requests.adapters.HTTPAdapter)
        assert adapter.max_retries.total == 3

    def test_create_session_has_user_agent(self):
        """Should set User-Agent header."""
        session = create_fed_session()
        assert "User-Agent" in session.headers
        assert "FX-AlphaLab" in session.headers["User-Agent"]
