"""Tests for Federal Reserve scraper collector."""

from datetime import datetime
from unittest.mock import Mock, patch

from src.ingestion.collectors.fed_scraper_collector import FedScraperCollector


class TestFedScraperCollectorInit:
    """Test collector initialization."""

    def test_init_with_custom_paths(self, tmp_path):
        """Should initialize with custom output directory."""
        collector = FedScraperCollector(output_dir=tmp_path)
        assert collector.output_dir == tmp_path
        assert collector.output_dir.exists()

    def test_init_creates_output_dir(self, tmp_path):
        """Should create output directory if it doesn't exist."""
        output_dir = tmp_path / "test_output"
        FedScraperCollector(output_dir=output_dir)
        assert output_dir.exists()

    def test_init_source_name(self, tmp_path):
        """Should set SOURCE_NAME to 'fed'."""
        collector = FedScraperCollector(output_dir=tmp_path)
        assert collector.SOURCE_NAME == "fed"

    def test_init_creates_session(self, tmp_path):
        """Should create HTTP session on initialization."""
        collector = FedScraperCollector(output_dir=tmp_path)
        assert collector.session is not None


class TestHealthCheck:
    """Test health check functionality."""

    @patch("src.ingestion.collectors.fed_scraper_collector.create_fed_session")
    def test_health_check_success(self, mock_create_session, tmp_path):
        """Health check should return True if site is reachable."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_session.head.return_value = mock_response
        mock_create_session.return_value = mock_session

        collector = FedScraperCollector(output_dir=tmp_path)
        assert collector.health_check() is True

    @patch("src.ingestion.collectors.fed_scraper_collector.create_fed_session")
    def test_health_check_failure_404(self, mock_create_session, tmp_path):
        """Health check should return False on 404."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 404
        mock_session.head.return_value = mock_response
        mock_create_session.return_value = mock_session

        collector = FedScraperCollector(output_dir=tmp_path)
        assert collector.health_check() is False

    @patch("src.ingestion.collectors.fed_scraper_collector.create_fed_session")
    def test_health_check_failure_exception(self, mock_create_session, tmp_path):
        """Health check should return False on connection error."""
        mock_session = Mock()
        mock_session.head.side_effect = Exception("Connection failed")
        mock_create_session.return_value = mock_session

        collector = FedScraperCollector(output_dir=tmp_path)
        assert collector.health_check() is False


class TestParseReleaseItems:
    """Test press release HTML parsing."""

    def test_parse_release_items_valid(self, tmp_path):
        """Should parse valid press release items (div.row structure)."""
        html = """
        <html>
            <body>
                <div class="row">
                    <div class="col-xs-3 col-md-2 eventlist__time">
                        <time>12/30/2025</time>
                    </div>
                    <div class="col-xs-9 col-md-10 eventlist__event">
                        <p><a href="/newsevents/pressreleases/monetary20251230a.htm">
                            <em>Minutes of the Federal Open Market Committee</em>
                        </a></p>
                        <p class='eventlist__press'><em><strong>Monetary Policy</strong></em></p>
                    </div>
                </div>
                <div class="row">
                    <div class="col-xs-3 col-md-2 eventlist__time">
                        <time>12/23/2025</time>
                    </div>
                    <div class="col-xs-9 col-md-10 eventlist__event">
                        <p><a href="/newsevents/pressreleases/bcreg20251223a.htm">
                            <em>Agencies release annual asset-size thresholds</em>
                        </a></p>
                        <p class='eventlist__press'><em><strong>Banking and Consumer Regulatory Policy</strong></em></p>
                    </div>
                </div>
            </body>
        </html>
        """

        collector = FedScraperCollector(output_dir=tmp_path)
        items = collector._parse_release_items(html)

        assert len(items) == 2
        assert items[0]["title"] == "Minutes of the Federal Open Market Committee"
        assert items[0]["category"] == "Monetary Policy"
        assert items[0]["document_type"] == "policy"
        assert items[0]["timestamp_published"] == datetime(2025, 12, 30)

    def test_parse_release_items_no_category(self, tmp_path):
        """Should handle items without category."""
        html = """
        <div class="row">
            <div class="col-xs-3 col-md-2 eventlist__time">
                <time>12/30/2025</time>
            </div>
            <div class="col-xs-9 col-md-10 eventlist__event">
                <p><a href="/newsevents/pressreleases/test.htm"><em>Test Article</em></a></p>
            </div>
        </div>
        """

        collector = FedScraperCollector(output_dir=tmp_path)
        items = collector._parse_release_items(html)

        assert len(items) == 1
        assert items[0]["category"] == "other"
        assert items[0]["document_type"] == "other"

    def test_parse_release_items_no_link(self, tmp_path):
        """Should skip items without links."""
        html = """
        <div class="row">
            <div class="col-xs-3 col-md-2 eventlist__time">
                <time>12/30/2025</time>
            </div>
            <div class="col-xs-9 col-md-10 eventlist__event">
                <p>Text without link</p>
            </div>
        </div>
        """

        collector = FedScraperCollector(output_dir=tmp_path)
        items = collector._parse_release_items(html)

        assert len(items) == 0

    def test_parse_release_items_invalid_date(self, tmp_path):
        """Should skip items with invalid dates."""
        html = """
        <div class="row">
            <div class="col-xs-3 col-md-2 eventlist__time">
                <time>Invalid Date</time>
            </div>
            <div class="col-xs-9 col-md-10 eventlist__event">
                <p><a href="/test.htm"><em>Test Article</em></a></p>
            </div>
        </div>
        """

        collector = FedScraperCollector(output_dir=tmp_path)
        items = collector._parse_release_items(html)

        assert len(items) == 0

    def test_parse_release_items_single_digit_date(self, tmp_path):
        """Should parse single-digit month/day dates."""
        html = """
        <div class="row">
            <div class="col-xs-3 col-md-2 eventlist__time">
                <time>1/5/2024</time>
            </div>
            <div class="col-xs-9 col-md-10 eventlist__event">
                <p><a href="/test.htm"><em>Test Article</em></a></p>
                <p class='eventlist__press'><em><strong>Other Announcements</strong></em></p>
            </div>
        </div>
        """

        collector = FedScraperCollector(output_dir=tmp_path)
        items = collector._parse_release_items(html)

        assert len(items) == 1
        assert items[0]["timestamp_published"] == datetime(2024, 1, 5)


class TestParseSpeechItems:
    """Test speech HTML parsing."""

    def test_parse_speech_items_valid(self, tmp_path):
        """Should parse valid speech items."""
        html = """
        <html>
            <body>
                <div class="row">
                    <div class="col-xs-3 col-md-2 eventlist__time">
                        <time>12/15/2025</time>
                    </div>
                    <div class="col-xs-9 col-md-10 eventlist__event">
                        <p><a href="/newsevents/speech/miran20251215a.htm">
                            <em>The Inflation Outlook</em>
                        </a></p>
                        <p class="news__speaker">Governor Stephen I. Miran</p>
                        <p>At the School of International and Public Affairs, Columbia University</p>
                    </div>
                </div>
            </body>
        </html>
        """

        collector = FedScraperCollector(output_dir=tmp_path)
        items = collector._parse_speech_items(html)

        assert len(items) == 1
        assert items[0]["title"] == "The Inflation Outlook"
        assert items[0]["speaker"] == "Stephen I. Miran"
        assert (
            items[0]["location"]
            == "At the School of International and Public Affairs, Columbia University"
        )
        assert items[0]["timestamp_published"] == datetime(2025, 12, 15)

    def test_parse_speech_items_no_speaker(self, tmp_path):
        """Should handle speeches without speaker."""
        html = """
        <div class="row">
            <div class="col-xs-3 col-md-2 eventlist__time">
                <time>12/15/2025</time>
            </div>
            <div class="col-xs-9 col-md-10 eventlist__event">
                <p><a href="/test.htm"><em>Test Speech</em></a></p>
            </div>
        </div>
        """

        collector = FedScraperCollector(output_dir=tmp_path)
        items = collector._parse_speech_items(html)

        assert len(items) == 1
        assert items[0]["speaker"] is None
        assert items[0]["location"] is None

    def test_parse_speech_items_missing_time(self, tmp_path):
        """Should skip items without time tag."""
        html = """
        <div class="row">
            <div class="col-xs-3 col-md-2 eventlist__time">
            </div>
            <div class="col-xs-9 col-md-10 eventlist__event">
                <p><a href="/test.htm">Test</a></p>
            </div>
        </div>
        """

        collector = FedScraperCollector(output_dir=tmp_path)
        items = collector._parse_speech_items(html)

        assert len(items) == 0

    def test_parse_speech_items_missing_event_div(self, tmp_path):
        """Should skip rows without event div."""
        html = """
        <div class="row">
            <div class="col-xs-3 col-md-2 eventlist__time">
                <time>12/15/2025</time>
            </div>
        </div>
        """

        collector = FedScraperCollector(output_dir=tmp_path)
        items = collector._parse_speech_items(html)

        assert len(items) == 0


class TestCollect:
    """Test main collection method."""

    @patch("src.ingestion.collectors.fed_scraper_collector.time.sleep")
    @patch("src.ingestion.collectors.fed_scraper_collector.create_fed_session")
    def test_collect_default_date_range(self, mock_create_session, mock_sleep, tmp_path):
        """Should use default date range if not provided."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = "<html></html>"
        mock_session.get.return_value = mock_response
        mock_create_session.return_value = mock_session

        collector = FedScraperCollector(output_dir=tmp_path)

        # Collect without dates - should default to 2020-present
        collector.collect()

        # Should fetch multiple years (2020-2026)
        assert mock_session.get.call_count >= 6  # At least 6 years of press releases

    @patch("src.ingestion.collectors.fed_scraper_collector.time.sleep")
    @patch("src.ingestion.collectors.fed_scraper_collector.fetch_full_content")
    @patch("src.ingestion.collectors.fed_scraper_collector.create_fed_session")
    def test_collect_with_date_range(
        self, mock_create_session, mock_fetch_content, mock_sleep, tmp_path
    ):
        """Should fetch only years in specified date range."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = """
        <div class="row">
            <div class="col-xs-3 col-md-2 eventlist__time">
                <time>6/15/2024</time>
            </div>
            <div class="col-xs-9 col-md-10 eventlist__event">
                <p><a href="/test.htm"><em>Test Article</em></a></p>
                <p class='eventlist__press'><em><strong>Monetary Policy</strong></em></p>
            </div>
        </div>
        """
        mock_session.get.return_value = mock_response
        mock_create_session.return_value = mock_session
        mock_fetch_content.return_value = "Test content"

        collector = FedScraperCollector(output_dir=tmp_path)

        # Collect for single year
        result = collector.collect(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 12, 31),
            include_speeches=False,
        )

        # Should only fetch 2024 press releases
        assert "policy" in result
        assert len(result["policy"]) == 1

    @patch("src.ingestion.collectors.fed_scraper_collector.time.sleep")
    @patch("src.ingestion.collectors.fed_scraper_collector.fetch_full_content")
    @patch("src.ingestion.collectors.fed_scraper_collector.create_fed_session")
    def test_collect_filters_by_date(
        self, mock_create_session, mock_fetch_content, mock_sleep, tmp_path
    ):
        """Should filter documents by exact date range."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = """
        <div class="row">
            <div class="eventlist__time"><time>1/1/2024</time></div>
            <div class="eventlist__event">
                <p><a href="/test1.htm"><em>In Range</em></a></p>
                <p class='eventlist__press'><em><strong>Other Announcements</strong></em></p>
            </div>
        </div>
        <div class="row">
            <div class="eventlist__time"><time>12/31/2023</time></div>
            <div class="eventlist__event">
                <p><a href="/test2.htm"><em>Too Old</em></a></p>
                <p class='eventlist__press'><em><strong>Other Announcements</strong></em></p>
            </div>
        </div>
        <div class="row">
            <div class="eventlist__time"><time>1/1/2025</time></div>
            <div class="eventlist__event">
                <p><a href="/test3.htm"><em>Too New</em></a></p>
                <p class='eventlist__press'><em><strong>Other Announcements</strong></em></p>
            </div>
        </div>
        """
        mock_session.get.return_value = mock_response
        mock_create_session.return_value = mock_session
        mock_fetch_content.return_value = "Content"

        collector = FedScraperCollector(output_dir=tmp_path)

        result = collector.collect(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 12, 31),
            include_speeches=False,
        )

        # Should only include the one in range
        assert len(result["other"]) == 1
        assert result["other"][0]["title"] == "In Range"

    @patch("src.ingestion.collectors.fed_scraper_collector.time.sleep")
    @patch("src.ingestion.collectors.fed_scraper_collector.create_fed_session")
    def test_collect_without_speeches(self, mock_create_session, mock_sleep, tmp_path):
        """Should skip speeches when include_speeches=False."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = "<html></html>"
        mock_session.get.return_value = mock_response
        mock_create_session.return_value = mock_session

        collector = FedScraperCollector(output_dir=tmp_path)

        result = collector.collect(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 12, 31),
            include_speeches=False,
        )

        # Should not have speeches key
        assert "speeches" not in result


class TestExport:
    """Test JSONL export functionality."""

    @patch("src.ingestion.collectors.fed_scraper_collector.time.sleep")
    @patch("src.ingestion.collectors.fed_scraper_collector.fetch_full_content")
    @patch("src.ingestion.collectors.fed_scraper_collector.create_fed_session")
    def test_export_jsonl(self, mock_create_session, mock_fetch_content, mock_sleep, tmp_path):
        """Should export documents to JSONL files."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = """
        <div class="row">
            <div class="eventlist__time"><time>6/15/2024</time></div>
            <div class="eventlist__event">
                <p><a href="/test.htm"><em>Test Article</em></a></p>
                <p class='eventlist__press'><em><strong>Monetary Policy</strong></em></p>
            </div>
        </div>
        """
        mock_session.get.return_value = mock_response
        mock_create_session.return_value = mock_session
        mock_fetch_content.return_value = "Test content"

        collector = FedScraperCollector(output_dir=tmp_path)

        data = collector.collect(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 12, 31),
            include_speeches=False,
        )

        paths = collector.export_all(data=data)

        # Should create JSONL file
        assert "policy" in paths
        assert paths["policy"].exists()
        assert paths["policy"].suffix == ".jsonl"

    @patch("src.ingestion.collectors.fed_scraper_collector.time.sleep")
    @patch("src.ingestion.collectors.fed_scraper_collector.fetch_full_content")
    @patch("src.ingestion.collectors.fed_scraper_collector.create_fed_session")
    def test_export_all_document_types(
        self, mock_create_session, mock_fetch_content, mock_sleep, tmp_path
    ):
        """Should export all document types to separate files."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = """
        <div class="row">
            <div class="eventlist__time"><time>6/15/2024</time></div>
            <div class="eventlist__event">
                <p><a href="/test1.htm"><em>Policy Doc</em></a></p>
                <p class='eventlist__press'><em><strong>Monetary Policy</strong></em></p>
            </div>
        </div>
        <div class="row">
            <div class="eventlist__time"><time>6/16/2024</time></div>
            <div class="eventlist__event">
                <p><a href="/test2.htm"><em>Reg Doc</em></a></p>
                <p class='eventlist__press'><em><strong>Enforcement Actions</strong></em></p>
            </div>
        </div>
        """
        mock_session.get.return_value = mock_response
        mock_create_session.return_value = mock_session
        mock_fetch_content.return_value = "Content"

        collector = FedScraperCollector(output_dir=tmp_path)

        data = collector.collect(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 12, 31),
            include_speeches=False,
        )

        paths = collector.export_all(data=data)

        # Should have separate files for each type
        assert "policy" in paths
        assert "regulation" in paths
