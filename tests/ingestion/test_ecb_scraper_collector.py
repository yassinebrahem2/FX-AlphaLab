"""Tests for ECB Scraper Collector (historical backfill via Selenium)."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from src.ingestion.collectors.ecb_scraper_collector import (
    ARCHIVE_SECTIONS,
    ECBScraperCollector,
)

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_ARCHIVE_HTML = """<html><body>
<div class="content-box">
    <a href="/press/key/date/2026/html/ecb.sp260115~abc123.en.html">
        Speech by Christine Lagarde at ECB Forum
    </a>
    <span class="date">15 January 2026</span>
</div>
<div class="content-box">
    <a href="/press/key/date/2025/html/ecb.sp251010~def456.en.html">
        Remarks by Philip Lane on inflation
    </a>
    <span class="date">10 October 2025</span>
</div>
<div class="content-box">
    <a href="/press/key/date/2024/html/ecb.sp240601~ghi789.en.html">
        Speech by Isabel Schnabel at conference
    </a>
    <span class="date">1 June 2024</span>
</div>
<!-- Non-English page (should be skipped) -->
<a href="/press/key/date/2026/html/ecb.sp260115~abc123.de.html">German version</a>
</body></html>"""

SAMPLE_ARTICLE_HTML = """<html><body>
<script>console.log('x');</script>
<nav>Menu items</nav>
<article>
    <h1>Speech by Christine Lagarde at ECB Forum</h1>
    <p>This is the full speech content about monetary policy outlook and inflation
    targets. The European Central Bank continues to monitor price stability closely.
    More content here to pass the minimum threshold for extraction.</p>
</article>
<footer>Copyright ECB</footer>
</body></html>"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    output = tmp_path / "raw" / "news" / "ecb"
    output.mkdir(parents=True)
    return output


@pytest.fixture
def collector(tmp_output_dir: Path) -> ECBScraperCollector:
    return ECBScraperCollector(output_dir=tmp_output_dir, headless=True)


@pytest.fixture
def mock_driver():
    """Mock Selenium WebDriver."""
    driver = Mock()
    driver.page_source = SAMPLE_ARCHIVE_HTML
    driver.title = "ECB"
    driver.execute_script = Mock(
        side_effect=[
            1080,  # viewport height (first call)
            5000,  # initial scroll height
            5000,  # same height = stop scrolling
            5000,  # confirmation check
        ]
    )

    # Mock find_elements for WebDriverWait article detection poll
    mock_link = Mock()
    mock_link.get_attribute = Mock(
        return_value="/press/key/date/2026/html/ecb.sp260115~abc123.en.html"
    )
    driver.find_elements = Mock(return_value=[mock_link])

    return driver


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestECBScraperInit:
    """Test collector initialization."""

    def test_source_name(self, collector: ECBScraperCollector):
        assert collector.SOURCE_NAME == "ecb"

    def test_output_dir(self, collector: ECBScraperCollector, tmp_output_dir: Path):
        assert collector.output_dir == tmp_output_dir

    def test_driver_starts_none(self, collector: ECBScraperCollector):
        assert collector._driver is None

    def test_crawl_delay(self, collector: ECBScraperCollector):
        assert collector.CRAWL_DELAY == 5.0


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    """Test health_check method."""

    @patch("src.ingestion.collectors.ecb_utils.requests.Session.head")
    def test_success(self, mock_head, collector: ECBScraperCollector):
        mock_response = Mock()
        mock_response.ok = True
        mock_head.return_value = mock_response
        assert collector.health_check() is True

    @patch("src.ingestion.collectors.ecb_utils.requests.Session.head")
    def test_failure(self, mock_head, collector: ECBScraperCollector):
        mock_head.side_effect = Exception("Connection error")
        assert collector.health_check() is False


# ---------------------------------------------------------------------------
# Date Parsing from URLs
# ---------------------------------------------------------------------------


class TestParseDateFromURL:
    """Test _parse_date_from_url static method."""

    def test_standard_speech_url(self):
        url = "https://www.ecb.europa.eu/press/key/date/2026/html/ecb.sp260115~abc123.en.html"
        result = ECBScraperCollector._parse_date_from_url(url)
        assert result == datetime(2026, 1, 15, tzinfo=timezone.utc)

    def test_press_release_url(self):
        url = "https://www.ecb.europa.eu/press/pr/date/2025/html/ecb.pr251201~xyz.en.html"
        result = ECBScraperCollector._parse_date_from_url(url)
        assert result == datetime(2025, 12, 1, tzinfo=timezone.utc)

    def test_year_only_fallback(self):
        url = "https://www.ecb.europa.eu/press/govcdec/date/2025/html/index.en.html"
        result = ECBScraperCollector._parse_date_from_url(url)
        assert result == datetime(2025, 1, 1, tzinfo=timezone.utc)

    def test_no_date_returns_none(self):
        url = "https://www.ecb.europa.eu/press/html/index.en.html"
        assert ECBScraperCollector._parse_date_from_url(url) is None

    def test_invalid_date_digits_falls_back_to_year(self):
        url = "https://www.ecb.europa.eu/press/key/date/2026/html/ecb.sp261399~x.en.html"
        # Month 13 day 99 — filename parse fails, falls back to /date/2026/ year extraction
        result = ECBScraperCollector._parse_date_from_url(url)
        assert result == datetime(2026, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# URL Discovery
# ---------------------------------------------------------------------------


class TestDiscoverArticleURLs:
    """Test _discover_article_urls with mocked Selenium."""

    @patch.object(ECBScraperCollector, "_init_driver")
    @patch.object(ECBScraperCollector, "_scroll_to_load_all")
    def test_discovers_urls_in_date_range(
        self, mock_scroll, mock_init_driver, collector: ECBScraperCollector, mock_driver
    ):
        mock_init_driver.return_value = mock_driver

        section = ARCHIVE_SECTIONS[0]  # speeches
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 12, 31, tzinfo=timezone.utc)

        results = collector._discover_article_urls(section, start, end)

        # Should find 2 articles in range (2025-10-10 and 2026-01-15)
        # 2024-06-01 is outside range
        assert len(results) == 2
        urls = {r["url"] for r in results}
        assert (
            "https://www.ecb.europa.eu/press/key/date/2026/html/ecb.sp260115~abc123.en.html" in urls
        )
        assert (
            "https://www.ecb.europa.eu/press/key/date/2025/html/ecb.sp251010~def456.en.html" in urls
        )

    @patch.object(ECBScraperCollector, "_init_driver")
    @patch.object(ECBScraperCollector, "_scroll_to_load_all")
    def test_skips_non_english_pages(
        self, mock_scroll, mock_init_driver, collector: ECBScraperCollector, mock_driver
    ):
        mock_init_driver.return_value = mock_driver

        section = ARCHIVE_SECTIONS[0]
        start = datetime(2020, 1, 1, tzinfo=timezone.utc)
        end = datetime(2030, 12, 31, tzinfo=timezone.utc)

        results = collector._discover_article_urls(section, start, end)
        urls = {r["url"] for r in results}
        # German page should not appear
        assert not any(".de.html" in u for u in urls)

    @patch.object(ECBScraperCollector, "_init_driver")
    def test_timeout_returns_empty(self, mock_init_driver, collector: ECBScraperCollector):
        from selenium.common.exceptions import TimeoutException

        mock_driver = Mock()
        mock_driver.get.side_effect = TimeoutException("Timeout")
        mock_init_driver.return_value = mock_driver

        section = ARCHIVE_SECTIONS[0]
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 12, 31, tzinfo=timezone.utc)

        results = collector._discover_article_urls(section, start, end)
        assert results == []


# ---------------------------------------------------------------------------
# Document Building
# ---------------------------------------------------------------------------


class TestFetchAndBuildDocument:
    """Test _fetch_and_build_document."""

    @patch("src.ingestion.collectors.ecb_utils.fetch_full_content")
    def test_builds_document(self, mock_fetch, collector: ECBScraperCollector):
        mock_fetch.return_value = (
            "Speech by Christine Lagarde at ECB Forum. "
            "This is the full speech content about monetary policy outlook."
        )

        discovery = {
            "url": "https://www.ecb.europa.eu/press/key/date/2026/html/ecb.sp260115~abc.en.html",
            "title": "Speech by Christine Lagarde at ECB Forum",
            "date_hint": datetime(2026, 1, 15, tzinfo=timezone.utc),
            "category": "speeches",
        }

        doc = collector._fetch_and_build_document(discovery)

        assert doc is not None
        assert doc["source"] == "ecb"
        assert doc["url"] == discovery["url"]
        assert doc["document_type"] == "speech"
        assert doc["speaker"] == "Christine Lagarde"
        assert doc["language"] == "en"
        assert doc["metadata"]["scrape_method"] == "historical"
        assert "_category" in doc

    @patch("src.ingestion.collectors.ecb_utils.fetch_full_content")
    def test_returns_none_on_empty_content(self, mock_fetch, collector: ECBScraperCollector):
        mock_fetch.return_value = ""

        discovery = {
            "url": "https://example.com/empty",
            "title": "Empty page",
            "date_hint": None,
            "category": "pressreleases",
        }

        assert collector._fetch_and_build_document(discovery) is None

    @patch("src.ingestion.collectors.ecb_utils.fetch_full_content")
    def test_section_category_override(self, mock_fetch, collector: ECBScraperCollector):
        """When text classification returns pressreleases but section says policy,
        the section context wins."""
        mock_fetch.return_value = "General announcement text about ECB operations and activities."

        discovery = {
            "url": "https://example.com/article",
            "title": "ECB announcement",
            "date_hint": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "category": "policy",
        }

        doc = collector._fetch_and_build_document(discovery)
        assert doc is not None
        assert doc["document_type"] == "policy_decision"


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


class TestDeduplication:
    """Test _load_existing_urls."""

    def test_loads_urls_from_jsonl(self, collector: ECBScraperCollector, tmp_output_dir: Path):
        # Write a sample JSONL file
        jsonl_file = tmp_output_dir / "speeches_20260215.jsonl"
        docs = [
            {"url": "https://ecb.europa.eu/article1", "title": "Article 1"},
            {"url": "https://ecb.europa.eu/article2", "title": "Article 2"},
        ]
        with open(jsonl_file, "w", encoding="utf-8") as f:
            for doc in docs:
                f.write(json.dumps(doc) + "\n")

        existing = collector._load_existing_urls()
        assert "https://ecb.europa.eu/article1" in existing
        assert "https://ecb.europa.eu/article2" in existing
        assert len(existing) == 2

    def test_empty_dir(self, collector: ECBScraperCollector):
        existing = collector._load_existing_urls()
        assert existing == set()

    def test_handles_malformed_jsonl(self, collector: ECBScraperCollector, tmp_output_dir: Path):
        jsonl_file = tmp_output_dir / "bad_20260215.jsonl"
        with open(jsonl_file, "w", encoding="utf-8") as f:
            f.write('{"url": "https://ecb.europa.eu/good"}\n')
            f.write("not valid json\n")
            f.write('{"url": "https://ecb.europa.eu/also_good"}\n')

        # Should not raise, and should get the valid URLs
        existing = collector._load_existing_urls()
        assert "https://ecb.europa.eu/good" in existing


# ---------------------------------------------------------------------------
# Collect (full pipeline)
# ---------------------------------------------------------------------------


class TestCollect:
    """Test collect method with mocked discovery and content fetching."""

    @patch.object(ECBScraperCollector, "_fetch_and_build_document")
    @patch.object(ECBScraperCollector, "_discover_article_urls")
    @patch.object(ECBScraperCollector, "close")
    def test_collect_routes_documents(
        self, mock_close, mock_discover, mock_fetch, collector: ECBScraperCollector
    ):
        mock_discover.return_value = [
            {
                "url": "https://ecb.europa.eu/speech1",
                "title": "Speech by Christine Lagarde",
                "date_hint": datetime(2026, 1, 15, tzinfo=timezone.utc),
                "category": "speeches",
            },
        ]
        mock_fetch.return_value = {
            "source": "ecb",
            "timestamp_collected": "2026-02-15T10:00:00+00:00",
            "timestamp_published": "2026-01-15T00:00:00+00:00",
            "url": "https://ecb.europa.eu/speech1",
            "title": "Speech by Christine Lagarde",
            "content": "Full speech content",
            "document_type": "speech",
            "speaker": "Christine Lagarde",
            "language": "en",
            "metadata": {"scrape_method": "historical"},
            "_category": "speeches",
        }

        start = datetime(2025, 7, 1, tzinfo=timezone.utc)
        end = datetime(2026, 2, 15, tzinfo=timezone.utc)

        result = collector.collect(start_date=start, end_date=end)

        assert len(result["speeches"]) == 1
        assert result["speeches"][0]["speaker"] == "Christine Lagarde"
        assert mock_close.call_count >= 1

    def test_invalid_date_range(self, collector: ECBScraperCollector):
        with pytest.raises(ValueError, match="must be before"):
            collector.collect(
                start_date=datetime(2026, 3, 1, tzinfo=timezone.utc),
                end_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )

    @patch.object(ECBScraperCollector, "_discover_article_urls")
    @patch.object(ECBScraperCollector, "close")
    def test_collect_empty_result(self, mock_close, mock_discover, collector: ECBScraperCollector):
        mock_discover.return_value = []

        result = collector.collect(
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 2, 1, tzinfo=timezone.utc),
        )

        for docs in result.values():
            assert docs == []


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


class TestExportJSONL:
    """Test export_jsonl validation."""

    def test_empty_raises(self, collector: ECBScraperCollector):
        with pytest.raises(ValueError, match="Cannot export empty"):
            collector.export_jsonl([], "speeches")

    def test_invalid_type_raises(self, collector: ECBScraperCollector):
        with pytest.raises(ValueError, match="Invalid document_type"):
            collector.export_jsonl([{"data": "x"}], "invalid_type")

    def test_valid_export(self, collector: ECBScraperCollector):
        docs = [{"source": "ecb", "title": "Test"}]
        path = collector.export_jsonl(docs, "speeches")
        assert path.exists()
        assert "speeches_" in path.name


# ---------------------------------------------------------------------------
# Driver Management
# ---------------------------------------------------------------------------


class TestDriverManagement:
    """Test Selenium driver lifecycle."""

    def test_close_when_no_driver(self, collector: ECBScraperCollector):
        # Should not raise
        collector.close()
        assert collector._driver is None

    def test_close_quits_driver(self, collector: ECBScraperCollector):
        mock_driver = Mock()
        collector._driver = mock_driver
        collector.close()
        mock_driver.quit.assert_called_once()
        assert collector._driver is None

    def test_close_handles_quit_error(self, collector: ECBScraperCollector):
        mock_driver = Mock()
        mock_driver.quit.side_effect = Exception("Already closed")
        collector._driver = mock_driver
        collector.close()  # Should not raise
        assert collector._driver is None
