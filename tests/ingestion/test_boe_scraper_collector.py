"""Tests for Bank of England sitemap-based scraper collector."""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from src.ingestion.collectors.boe_scraper_collector import BoEScraperCollector


class TestBoEScraperCollectorInit:
    """Test collector initialization."""

    def test_init_with_custom_paths(self, tmp_path):
        """Should initialize with custom output directory."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        assert collector.output_dir == tmp_path
        assert collector.output_dir.exists()

    def test_init_creates_output_dir(self, tmp_path):
        """Should create output directory if it doesn't exist."""
        output_dir = tmp_path / "test_output"
        BoEScraperCollector(output_dir=output_dir)
        assert output_dir.exists()

    def test_init_source_name(self, tmp_path):
        """Should set SOURCE_NAME to 'boe'."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        assert collector.SOURCE_NAME == "boe"

    def test_init_creates_session(self, tmp_path):
        """Should create requests session with retry logic."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        assert collector.session is not None


class TestHealthCheck:
    """Test health check functionality."""

    @patch("src.ingestion.collectors.boe_scraper_collector.requests.Session")
    def test_health_check_success(self, mock_session_class, tmp_path):
        """Should return True when sitemap is accessible."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.content = b"""<?xml version="1.0"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://www.bankofengland.co.uk/speech/2024/test</loc>
                <lastmod>2024-01-01T00:00:00Z</lastmod>
            </url>
        </urlset>"""
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        collector = BoEScraperCollector(output_dir=tmp_path)
        collector.session = mock_session
        assert collector.health_check() is True

    @patch("src.ingestion.collectors.boe_scraper_collector.requests.Session")
    def test_health_check_failure_http_error(self, mock_session_class, tmp_path):
        """Should return False on HTTP error."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 404
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        collector = BoEScraperCollector(output_dir=tmp_path)
        collector.session = mock_session
        assert collector.health_check() is False

    @patch("src.ingestion.collectors.boe_scraper_collector.requests.Session")
    def test_health_check_failure_invalid_xml(self, mock_session_class, tmp_path):
        """Should return False on invalid XML."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.content = b"Not valid XML"
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        collector = BoEScraperCollector(output_dir=tmp_path)
        collector.session = mock_session
        assert collector.health_check() is False


class TestURLPatternMatching:
    """Test URL pattern filtering."""

    def test_matches_speech_url(self, tmp_path):
        """Should match speech URLs."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/speech/2024/december/test-speech"
            )
            is True
        )

    def test_matches_news_url(self, tmp_path):
        """Should match news URLs."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/news/2024/november/test-news"
            )
            is True
        )

    def test_matches_minutes_url(self, tmp_path):
        """Should match minutes URLs."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/minutes/2024/october/mpc-minutes"
            )
            is True
        )

    def test_matches_summary_url(self, tmp_path):
        """Should match monetary policy summary URLs."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/monetary-policy-summary/2024/september"
            )
            is True
        )

    def test_does_not_match_other_urls(self, tmp_path):
        """Should not match unrelated URLs."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        assert collector._matches_url_pattern("https://www.bankofengland.co.uk/about") is False
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/careers/current-vacancies"
            )
            is False
        )

    def test_matches_any_year_without_range(self, tmp_path):
        """Should match URLs from any year when no year range specified."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        # Without year range, all years match
        assert (
            collector._matches_url_pattern("https://www.bankofengland.co.uk/speech/2019/test")
            is True
        )
        assert (
            collector._matches_url_pattern("https://www.bankofengland.co.uk/speech/2015/test")
            is True
        )

    def test_filters_by_year_range(self, tmp_path):
        """Should filter URLs by year range when specified."""
        collector = BoEScraperCollector(output_dir=tmp_path)

        # Request 2021-2025 range
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/speech/2023/test", start_year=2021, end_year=2025
            )
            is True
        )

        # 2019 is before range
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/speech/2019/test", start_year=2021, end_year=2025
            )
            is False
        )

        # 2026 is after range
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/speech/2026/test", start_year=2021, end_year=2025
            )
            is False
        )

        # 2021 and 2025 are at boundaries (inclusive)
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/speech/2021/test", start_year=2021, end_year=2025
            )
            is True
        )
        assert (
            collector._matches_url_pattern(
                "https://www.bankofengland.co.uk/speech/2025/test", start_year=2021, end_year=2025
            )
            is True
        )


class TestDocumentClassification:
    """Test document type classification."""

    def test_classify_speech(self, tmp_path):
        """Should classify speech URLs."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        bucket, doc_type = collector._classify_document_type(
            "https://www.bankofengland.co.uk/speech/2024/test"
        )
        assert bucket == "speeches"
        assert doc_type == "boe_speech"

    def test_classify_news(self, tmp_path):
        """Should classify news URLs as statements."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        bucket, doc_type = collector._classify_document_type(
            "https://www.bankofengland.co.uk/news/2024/test"
        )
        assert bucket == "statements"
        assert doc_type == "press_release"

    def test_classify_default_statement(self, tmp_path):
        """Should classify non-matching URLs as statements."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        bucket, doc_type = collector._classify_document_type(
            "https://www.bankofengland.co.uk/monetary-policy-committee/2024/test"
        )
        assert bucket == "statements"
        assert doc_type == "press_release"

    def test_classify_summary(self, tmp_path):
        """Should classify monetary policy summary URLs."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        bucket, doc_type = collector._classify_document_type(
            "https://www.bankofengland.co.uk/monetary-policy-summary/2024/test"
        )
        assert bucket == "summaries"
        assert doc_type == "monetary_policy_summary"


class TestDateParsing:
    """Test date extraction and parsing."""

    def test_parse_date_string_dd_month_yyyy(self, tmp_path):
        """Should parse 'DD Month YYYY' format."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        result = collector._parse_date_string("08 February 2026")
        assert result is not None
        assert "2026-02-08" in result

    def test_parse_date_string_dd_mon_yyyy(self, tmp_path):
        """Should parse 'DD Mon YYYY' format."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        result = collector._parse_date_string("08 Feb 2026")
        assert result is not None
        assert "2026-02-08" in result

    def test_parse_date_string_iso(self, tmp_path):
        """Should parse ISO 8601 format."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        result = collector._parse_date_string("2026-02-08")
        assert result is not None
        assert "2026-02-08" in result

    def test_parse_date_string_invalid(self, tmp_path):
        """Should return None for invalid dates."""
        collector = BoEScraperCollector(output_dir=tmp_path)
        assert collector._parse_date_string("invalid date") is None
        assert collector._parse_date_string("") is None


class TestSitemapFetching:
    """Test sitemap fetching and filtering."""

    @patch("src.ingestion.collectors.boe_scraper_collector.requests.Session")
    def test_fetch_sitemap_filters_by_pattern(self, mock_session_class, tmp_path):
        """Should filter sitemap URLs by pattern."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.content = b"""<?xml version="1.0"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://www.bankofengland.co.uk/speech/2024/test-speech</loc>
                <lastmod>2024-06-15T00:00:00Z</lastmod>
            </url>
            <url>
                <loc>https://www.bankofengland.co.uk/about</loc>
                <lastmod>2024-06-15T00:00:00Z</lastmod>
            </url>
            <url>
                <loc>https://www.bankofengland.co.uk/news/2024/test-news</loc>
                <lastmod>2024-06-15T00:00:00Z</lastmod>
            </url>
        </urlset>"""
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        collector = BoEScraperCollector(output_dir=tmp_path)
        collector.session = mock_session

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 12, 31, tzinfo=timezone.utc)

        urls = collector._fetch_sitemap_urls(start, end)

        # Should only get speech and news, not about page
        assert len(urls) == 2
        assert any("speech" in url["url"] for url in urls)
        assert any("news" in url["url"] for url in urls)
        assert not any("about" in url["url"] for url in urls)

    @patch("src.ingestion.collectors.boe_scraper_collector.requests.Session")
    def test_fetch_sitemap_filters_by_date(self, mock_session_class, tmp_path):
        """Should filter sitemap URLs by lastmod date."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.content = b"""<?xml version="1.0"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://www.bankofengland.co.uk/speech/2024/in-range</loc>
                <lastmod>2024-06-15T00:00:00Z</lastmod>
            </url>
            <url>
                <loc>https://www.bankofengland.co.uk/speech/2023/too-old</loc>
                <lastmod>2023-12-31T00:00:00Z</lastmod>
            </url>
            <url>
                <loc>https://www.bankofengland.co.uk/speech/2025/too-new</loc>
                <lastmod>2025-01-01T00:00:00Z</lastmod>
            </url>
        </urlset>"""
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        collector = BoEScraperCollector(output_dir=tmp_path)
        collector.session = mock_session

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 12, 31, tzinfo=timezone.utc)

        urls = collector._fetch_sitemap_urls(start, end)

        # Should only get 2024 URL
        assert len(urls) == 1
        assert "in-range" in urls[0]["url"]


class TestDocumentParsing:
    """Test document HTML parsing."""

    def test_parse_document_extracts_title(self, tmp_path):
        """Should extract title from h1 itemprop=name."""
        html = """
        <html>
            <body>
                <h1 itemprop="name">Test Speech Title</h1>
                <div class="published-date">Published on 08 February 2026</div>
                <div class="content-block">
                    <p>Test content that is long enough to meet the minimum threshold.
                    More content here to ensure we exceed 100 characters total.</p>
                </div>
            </body>
        </html>
        """

        collector = BoEScraperCollector(output_dir=tmp_path)
        # Set date range for filtering
        collector._start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        collector._end_date = datetime(2026, 12, 31, tzinfo=timezone.utc)

        doc = collector._parse_document(
            html=html,
            url="https://www.bankofengland.co.uk/speech/2026/test",
            bucket="speeches",
            doc_type="boe_speech",
            lastmod="2026-02-08T00:00:00Z",
            collected_at=datetime(2026, 2, 8, tzinfo=timezone.utc),
        )

        assert doc is not None
        assert doc["title"] == "Test Speech Title"

    def test_parse_document_extracts_date(self, tmp_path):
        """Should extract published date from published-date div."""
        html = """
        <html>
            <body>
                <h1 itemprop="name">Test Speech</h1>
                <div class="published-date">Published on 08 February 2026</div>
                <div class="content-block">
                    <p>Test content that is long enough to meet the minimum threshold.
                    More content here to ensure we exceed 100 characters total.</p>
                </div>
            </body>
        </html>
        """

        collector = BoEScraperCollector(output_dir=tmp_path)
        # Set date range for filtering
        collector._start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        collector._end_date = datetime(2026, 12, 31, tzinfo=timezone.utc)

        doc = collector._parse_document(
            html=html,
            url="https://www.bankofengland.co.uk/speech/2026/test",
            bucket="speeches",
            doc_type="boe_speech",
            lastmod="2026-02-08T00:00:00Z",
            collected_at=datetime(2026, 2, 8, tzinfo=timezone.utc),
        )

        assert doc is not None
        assert "2026-02-08" in doc["timestamp_published"]

    def test_parse_document_extracts_content(self, tmp_path):
        """Should extract content from content-block div."""
        html = """
        <html>
            <body>
                <h1 itemprop="name">Test Speech</h1>
                <div class="published-date">Published on 08 February 2026</div>
                <div class="content-block">
                    <p>This is the main content of the document. It should be extracted
                    and cleaned properly. This needs to be long enough to exceed the
                    minimum content threshold of 100 characters for the test to pass.</p>
                </div>
            </body>
        </html>
        """

        collector = BoEScraperCollector(output_dir=tmp_path)
        # Set date range for filtering
        collector._start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        collector._end_date = datetime(2026, 12, 31, tzinfo=timezone.utc)

        doc = collector._parse_document(
            html=html,
            url="https://www.bankofengland.co.uk/speech/2026/test",
            bucket="speeches",
            doc_type="boe_speech",
            lastmod="2026-02-08T00:00:00Z",
            collected_at=datetime(2026, 2, 8, tzinfo=timezone.utc),
        )

        assert doc is not None
        assert "main content" in doc["content"]
        assert len(doc["content"]) > 100

    def test_parse_document_returns_none_for_short_content(self, tmp_path):
        """Should return None if content is too short."""
        html = """
        <html>
            <body>
                <h1 itemprop="name">Test Speech</h1>
                <div class="published-date">Published on 08 February 2026</div>
                <div class="content-block"><p>Short</p></div>
            </body>
        </html>
        """

        collector = BoEScraperCollector(output_dir=tmp_path)
        # Set date range for filtering
        collector._start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        collector._end_date = datetime(2026, 12, 31, tzinfo=timezone.utc)

        doc = collector._parse_document(
            html=html,
            url="https://www.bankofengland.co.uk/speech/2026/test",
            bucket="speeches",
            doc_type="boe_speech",
            lastmod="2026-02-08T00:00:00Z",
            collected_at=datetime(2026, 2, 8, tzinfo=timezone.utc),
        )

        assert doc is None

    def test_parse_document_returns_none_without_title(self, tmp_path):
        """Should return None if no title found."""
        html = """
        <html>
            <body>
                <div class="published-date">Published on 08 February 2026</div>
                <div class="content-block">
                    <p>Content without title should be rejected even if long enough
                    to meet the minimum threshold of 100 characters for validation.</p>
                </div>
            </body>
        </html>
        """

        collector = BoEScraperCollector(output_dir=tmp_path)
        # Set date range for filtering
        collector._start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        collector._end_date = datetime(2026, 12, 31, tzinfo=timezone.utc)

        doc = collector._parse_document(
            html=html,
            url="https://www.bankofengland.co.uk/speech/2026/test",
            bucket="speeches",
            doc_type="boe_speech",
            lastmod="2026-02-08T00:00:00Z",
            collected_at=datetime(2026, 2, 8, tzinfo=timezone.utc),
        )

        assert doc is None


class TestCollect:
    """Test main collection workflow."""

    @patch("src.ingestion.collectors.boe_scraper_collector.time.sleep")
    @patch("src.ingestion.collectors.boe_scraper_collector.requests.Session")
    def test_collect_returns_dict_by_type(self, mock_session_class, mock_sleep, tmp_path):
        """Should return documents grouped by type."""
        # Mock sitemap response
        sitemap_response = Mock()
        sitemap_response.ok = True
        sitemap_response.content = b"""<?xml version="1.0"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://www.bankofengland.co.uk/speech/2024/test-speech</loc>
                <lastmod>2024-06-15T00:00:00Z</lastmod>
            </url>
        </urlset>"""

        # Mock page response
        page_response = Mock()
        page_response.ok = True
        page_response.text = """
        <html>
            <body>
                <h1 itemprop="name">Test Speech</h1>
                <div class="published-date">Published on 15 June 2024</div>
                <div class="content-block">
                    <p>This is test content for the speech that needs to be long
                    enough to pass validation. Adding more text to meet minimum length.</p>
                </div>
            </body>
        </html>
        """
        page_response.apparent_encoding = "utf-8"

        mock_session = Mock()
        mock_session.get.side_effect = [sitemap_response, page_response]
        mock_session_class.return_value = mock_session

        collector = BoEScraperCollector(output_dir=tmp_path)
        collector.session = mock_session

        result = collector.collect(
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
        )

        assert "speeches" in result
        assert len(result["speeches"]) == 1
        assert result["speeches"][0]["source"] == "BoE"
        assert result["speeches"][0]["document_type"] == "boe_speech"


class TestExport:
    """Test JSONL export functionality."""

    @patch("src.ingestion.collectors.boe_scraper_collector.time.sleep")
    @patch("src.ingestion.collectors.boe_scraper_collector.requests.Session")
    def test_export_all(self, mock_session_class, mock_sleep, tmp_path):
        """Should export documents to JSONL files."""
        # Mock sitemap
        sitemap_response = Mock()
        sitemap_response.ok = True
        sitemap_response.content = b"""<?xml version="1.0"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://www.bankofengland.co.uk/speech/2024/test</loc>
                <lastmod>2024-06-15T00:00:00Z</lastmod>
            </url>
        </urlset>"""

        # Mock page
        page_response = Mock()
        page_response.ok = True
        page_response.text = """
        <html>
            <body>
                <h1 itemprop="name">Test Speech</h1>
                <div class="published-date">Published on 15 June 2024</div>
                <div class="content-block">
                    <p>Content that is long enough to meet validation requirements
                    for the minimum threshold of 100 characters total length.</p>
                </div>
            </body>
        </html>
        """
        page_response.apparent_encoding = "utf-8"

        mock_session = Mock()
        mock_session.get.side_effect = [sitemap_response, page_response]
        mock_session_class.return_value = mock_session

        collector = BoEScraperCollector(output_dir=tmp_path)
        collector.session = mock_session

        data = collector.collect(
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
        )

        paths = collector.export_all(data=data)

        assert "speeches" in paths
        assert paths["speeches"].exists()
        assert "speeches_" in paths["speeches"].name
        assert paths["speeches"].suffix == ".jsonl"
