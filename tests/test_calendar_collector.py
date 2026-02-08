"""
Unit tests for Economic Calendar Collector.

Tests include:
- Web scraping functionality with mocked HTML responses
- Robots.txt compliance
- Error handling and retry logic
- CSV output functionality
- Data parsing and validation
"""

import csv
import os
import tempfile
from unittest.mock import Mock, patch

import pytest
from bs4 import BeautifulSoup

from data.ingestion.calendar_collector import EconomicCalendarCollector


class TestEconomicCalendarCollector:
    """Test cases for EconomicCalendarCollector class."""

    @pytest.fixture
    def collector(self):
        """Create a collector instance for testing."""
        return EconomicCalendarCollector(
            base_url="https://www.investing.com",
            min_delay=0.1,  # Shorter delay for tests
            max_delay=0.2,
            max_retries=2,
            timeout=10,
        )

    @pytest.fixture
    def sample_html_response(self):
        """Sample HTML response from Investing.com economic calendar."""
        return """
        <html>
        <body>
            <table class="genTbl" id="economicCalendarData">
                <tr class="header">
                    <th>Date</th>
                    <th>Time</th>
                    <th>Country</th>
                    <th>Event</th>
                    <th>Impact</th>
                    <th>Actual</th>
                    <th>Forecast</th>
                    <th>Previous</th>
                </tr>
                <tr>
                    <td>2024-02-08</td>
                    <td>13:30</td>
                    <td><img src="/flags/us.svg" title="United States" alt="US"></td>
                    <td><a href="/events/non-farm-payrolls">Non-Farm Payrolls</a></td>
                    <td class="high"><span class="impact high">High</span></td>
                    <td>150K</td>
                    <td>180K</td>
                    <td>160K</td>
                </tr>
                <tr>
                    <td>2024-02-08</td>
                    <td>14:00</td>
                    <td><img src="/flags/eu.svg" title="Eurozone" alt="EU"></td>
                    <td><a href="/events/ecb-interest-rate">ECB Interest Rate Decision</a></td>
                    <td class="medium"><span class="impact medium">Medium</span></td>
                    <td>4.50%</td>
                    <td>4.50%</td>
                    <td>4.50%</td>
                </tr>
                <tr>
                    <td>2024-02-08</td>
                    <td>09:30</td>
                    <td><img src="/flags/gb.svg" title="United Kingdom" alt="UK"></td>
                    <td><a href="/events/gdp-growth">GDP Growth (QoQ)</a></td>
                    <td class="low"><span class="impact low">Low</span></td>
                    <td>0.2%</td>
                    <td>0.3%</td>
                    <td>0.1%</td>
                </tr>
                <tr>
                    <td>2024-02-08</td>
                    <td>15:00</td>
                    <td><img src="/flags/us.svg" title="United States" alt="US"></td>
                    <td><a href="/events/cpi">Consumer Price Index</a></td>
                    <td class="high"><span class="impact high">High</span></td>
                    <td>-</td>
                    <td>3.2%</td>
                    <td>3.4%</td>
                </tr>
            </table>
        </body>
        </html>
        """

    @pytest.fixture
    def empty_html_response(self):
        """Empty HTML response for testing error cases."""
        return """
        <html>
        <body>
            <table class="genTbl">
                <tr class="header">
                    <th>Date</th>
                    <th>Time</th>
                    <th>Country</th>
                </tr>
            </table>
        </body>
        </html>
        """

    def test_initialization(self, collector):
        """Test collector initialization with default parameters."""
        assert collector.base_url == "https://www.investing.com"
        assert collector.min_delay == 0.1
        assert collector.max_delay == 0.2
        assert collector.max_retries == 2
        assert collector.timeout == 10
        assert len(collector.user_agents) > 0
        assert isinstance(collector.headers, dict)

    def test_get_random_delay(self, collector):
        """Test random delay generation."""
        for _ in range(10):
            delay = collector._get_random_delay()
            assert collector.min_delay <= delay <= collector.max_delay

    def test_get_random_user_agent(self, collector):
        """Test random user agent selection."""
        for _ in range(10):
            ua = collector._get_random_user_agent()
            assert ua in collector.user_agents

    def test_parse_impact_level(self, collector):
        """Test impact level parsing from HTML elements."""
        # Test high impact
        high_element = Mock()
        high_element.get.return_value = ["impact", "high"]
        assert collector._parse_impact_level(high_element) == "High"

        # Test medium impact
        medium_element = Mock()
        medium_element.get.return_value = ["medium"]
        assert collector._parse_impact_level(medium_element) == "Medium"

        # Test low impact
        low_element = Mock()
        low_element.get.return_value = ["low"]
        assert collector._parse_impact_level(low_element) == "Low"

        # Test unknown impact
        unknown_element = Mock()
        unknown_element.get.return_value = ["other"]
        unknown_element.get_text.return_value = ""
        assert collector._parse_impact_level(unknown_element) == "Unknown"

        # Test None element
        assert collector._parse_impact_level(None) == "Unknown"

    def test_clean_numeric_value(self, collector):
        """Test numeric value cleaning."""
        # Test valid values
        assert collector._clean_numeric_value("150K") == "150K"
        assert collector._clean_numeric_value("4.50%") == "4.50%"
        assert collector._clean_numeric_value("0.2%") == "0.2%"

        # Test empty/invalid values
        assert collector._clean_numeric_value("") is None
        assert collector._clean_numeric_value("-") is None
        assert collector._clean_numeric_value("N/A") is None
        assert collector._clean_numeric_value("NA") is None
        assert collector._clean_numeric_value(None) is None

        # Test whitespace handling
        assert collector._clean_numeric_value("  150K  ") == "150K"

    @patch("data.ingestion.calendar_collector.RobotFileParser")
    def test_robots_txt_compliance(self, mock_robots, collector):
        """Test robots.txt compliance checking."""
        # Mock robots.txt allowing access
        mock_parser = Mock()
        mock_parser.can_fetch.return_value = True
        mock_robots.return_value = mock_parser

        collector.robots_parser = mock_parser

        with patch.object(collector, "_get_random_delay", return_value=0):
            with patch.object(collector, "_get_random_user_agent", return_value="test-ua"):
                mock_response = Mock()
                mock_response.raise_for_status.return_value = None

                with patch.object(collector.session, "get", return_value=mock_response) as mock_get:
                    result = collector._make_request_with_retry(
                        "https://example.com/economic-calendar/"
                    )

                    assert result == mock_response
                    mock_parser.can_fetch.assert_called_with("*", "/economic-calendar/")
                    mock_get.assert_called_once()

    @patch("data.ingestion.calendar_collector.RobotFileParser")
    def test_robots_txt_blocking(self, mock_robots, collector):
        """Test robots.txt blocking access."""
        # Mock robots.txt blocking access
        mock_parser = Mock()
        mock_parser.can_fetch.return_value = False
        mock_robots.return_value = mock_parser

        collector.robots_parser = mock_parser

        result = collector._make_request_with_retry("https://example.com/economic-calendar/")
        assert result is None

    def test_parse_calendar_row(self, collector):
        """Test parsing of individual calendar rows."""
        # Create a mock HTML row
        html_row = """
        <tr>
            <td>2024-02-08</td>
            <td>13:30</td>
            <td><img src="/flags/us.svg" title="United States" alt="US"></td>
            <td><a href="/events/non-farm-payrolls">Non-Farm Payrolls</a></td>
            <td class="high"><span class="impact high">High</span></td>
            <td>150K</td>
            <td>180K</td>
            <td>160K</td>
        </tr>
        """
        soup = BeautifulSoup(html_row, "html.parser")
        row = soup.find("tr")

        event_data = collector._parse_calendar_row(row)

        assert event_data is not None
        assert event_data["date"] == "2024-02-08"
        assert event_data["time"] == "13:30"
        assert event_data["country"] == "United States"
        assert event_data["event"] == "Non-Farm Payrolls"
        assert event_data["impact"] == "High"
        assert event_data["actual"] == "150K"
        assert event_data["forecast"] == "180K"
        assert event_data["previous"] == "160K"
        assert event_data["event_url"] == "https://www.investing.com/events/non-farm-payrolls"
        assert "scraped_at" in event_data

    def test_parse_calendar_row_missing_values(self, collector):
        """Test parsing calendar rows with missing values."""
        html_row = """
        <tr>
            <td>2024-02-08</td>
            <td>15:00</td>
            <td><img src="/flags/us.svg" title="United States" alt="US"></td>
            <td><a href="/events/cpi">Consumer Price Index</a></td>
            <td class="high"><span class="impact high">High</span></td>
            <td>-</td>
            <td>3.2%</td>
            <td>3.4%</td>
        </tr>
        """
        soup = BeautifulSoup(html_row, "html.parser")
        row = soup.find("tr")

        event_data = collector._parse_calendar_row(row)

        assert event_data is not None
        assert event_data["actual"] is None  # Missing actual value
        assert event_data["forecast"] == "3.2%"
        assert event_data["previous"] == "3.4%"

    def test_parse_calendar_row_invalid_structure(self, collector):
        """Test parsing rows with invalid structure."""
        html_row = """
        <tr>
            <td>Only one column</td>
        </tr>
        """
        soup = BeautifulSoup(html_row, "html.parser")
        row = soup.find("tr")

        event_data = collector._parse_calendar_row(row)
        assert event_data is None

    @patch.object(EconomicCalendarCollector, "_make_request_with_retry")
    def test_fetch_calendar_data_success(self, mock_request, collector, sample_html_response):
        """Test successful calendar data fetching."""
        mock_response = Mock()
        mock_response.content = sample_html_response.encode("utf-8")
        mock_request.return_value = mock_response

        events = collector._fetch_calendar_data("2024-02-08", "us")

        assert len(events) == 4

        # Check first event
        first_event = events[0]
        assert first_event["date"] == "2024-02-08"
        assert first_event["time"] == "13:30"
        assert first_event["country"] == "United States"
        assert first_event["event"] == "Non-Farm Payrolls"
        assert first_event["impact"] == "High"
        assert first_event["actual"] == "150K"
        assert first_event["forecast"] == "180K"
        assert first_event["previous"] == "160K"

        # Check event with missing actual
        last_event = events[3]
        assert last_event["actual"] is None
        assert last_event["forecast"] == "3.2%"

    @patch.object(EconomicCalendarCollector, "_make_request_with_retry")
    def test_fetch_calendar_data_no_table(self, mock_request, collector, empty_html_response):
        """Test fetching when no calendar table is found."""
        mock_response = Mock()
        mock_response.content = empty_html_response.encode("utf-8")
        mock_request.return_value = mock_response

        events = collector._fetch_calendar_data("2024-02-08", "us")

        assert events == []

    @patch.object(EconomicCalendarCollector, "_make_request_with_retry")
    def test_fetch_calendar_data_request_failure(self, mock_request, collector):
        """Test fetching when request fails."""
        mock_request.return_value = None

        events = collector._fetch_calendar_data("2024-02-08", "us")

        assert events == []

    def test_collect_events_single_day(self, collector):
        """Test collecting events for a single day."""
        with patch.object(collector, "_fetch_calendar_data") as mock_fetch:
            mock_fetch.return_value = [
                {
                    "date": "2024-02-08",
                    "time": "13:30",
                    "country": "United States",
                    "event": "Non-Farm Payrolls",
                    "impact": "High",
                    "actual": "150K",
                    "forecast": "180K",
                    "previous": "160K",
                }
            ]

            events = collector.collect_events(
                start_date="2024-02-08", end_date="2024-02-08", countries=["us"]
            )

            assert len(events) == 1
            mock_fetch.assert_called_once_with("2024-02-08", "us")

    def test_collect_events_date_range(self, collector):
        """Test collecting events for a date range."""
        with patch.object(collector, "_fetch_calendar_data") as mock_fetch:
            mock_fetch.return_value = [
                {
                    "date": "2024-02-08",
                    "time": "13:30",
                    "country": "United States",
                    "event": "Non-Farm Payrolls",
                    "impact": "High",
                    "actual": "150K",
                    "forecast": "180K",
                    "previous": "160K",
                }
            ]

            events = collector.collect_events(
                start_date="2024-02-08", end_date="2024-02-09", countries=["us", "eu"]
            )

            # Should be called for 2 dates x 2 countries = 4 times
            assert mock_fetch.call_count == 4
            assert len(events) == 4  # Same event returned 4 times

    def test_collect_events_invalid_date(self, collector):
        """Test collecting events with invalid date format."""
        events = collector.collect_events(
            start_date="invalid-date", end_date="2024-02-08", countries=["us"]
        )

        assert events == []

    def test_save_to_csv_success(self, collector):
        """Test successful CSV saving."""
        events = [
            {
                "date": "2024-02-08",
                "time": "13:30",
                "country": "United States",
                "event": "Non-Farm Payrolls",
                "impact": "High",
                "actual": "150K",
                "forecast": "180K",
                "previous": "160K",
                "event_url": "https://example.com",
                "scraped_at": "2024-02-08T12:00:00",
            },
            {
                "date": "2024-02-08",
                "time": "14:00",
                "country": "Eurozone",
                "event": "ECB Interest Rate",
                "impact": "Medium",
                "actual": "4.50%",
                "forecast": "4.50%",
                "previous": "4.50%",
                "event_url": "https://example.com",
                "scraped_at": "2024-02-08T12:00:00",
            },
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp_file:
            temp_filename = temp_file.name

        try:
            result = collector.save_to_csv(events, temp_filename)

            assert result is True

            # Verify CSV content
            with open(temp_filename, encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)

                assert len(rows) == 2
                assert rows[0]["event"] == "Non-Farm Payrolls"
                assert rows[0]["impact"] == "High"
                assert rows[0]["actual"] == "150K"
                assert rows[1]["event"] == "ECB Interest Rate"
                assert rows[1]["impact"] == "Medium"
                assert rows[1]["actual"] == "4.50%"

        finally:
            os.unlink(temp_filename)

    def test_save_to_csv_no_events(self, collector):
        """Test CSV saving with no events."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp_file:
            temp_filename = temp_file.name

        try:
            result = collector.save_to_csv([], temp_filename)
            assert result is False
        finally:
            os.unlink(temp_filename)

    def test_get_events_dataframe(self, collector):
        """Test converting events to DataFrame."""
        events = [
            {
                "date": "2024-02-08",
                "time": "13:30",
                "country": "United States",
                "event": "Non-Farm Payrolls",
                "impact": "High",
                "actual": "150K",
                "forecast": "180K",
                "previous": "160K",
                "event_url": "https://example.com",
                "scraped_at": "2024-02-08T12:00:00",
            }
        ]

        df = collector.get_events_dataframe(events)

        assert df is not None
        assert len(df) == 1
        assert df.iloc[0]["event"] == "Non-Farm Payrolls"
        assert df.iloc[0]["impact"] == "High"

        # Check date conversion
        import pandas as pd

        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_get_events_dataframe_no_events(self, collector):
        """Test DataFrame conversion with no events."""
        df = collector.get_events_dataframe([])
        assert df is None


class TestIntegration:
    """Integration tests for the Economic Calendar Collector."""

    @patch("data.ingestion.calendar_collector.EconomicCalendarCollector._make_request_with_retry")
    def test_full_collection_workflow(self, mock_request):
        """Test the complete event collection workflow."""
        # Mock successful response
        sample_html = """
        <html>
        <body>
            <table class="genTbl">
                <tr class="header">
                    <th>Date</th><th>Time</th><th>Country</th><th>Event</th><th>Impact</th><th>Actual</th><th>Forecast</th><th>Previous</th>
                </tr>
                <tr>
                    <td>2024-02-08</td><td>13:30</td><td><img title="United States"></td><td>Test Event</td><td class="high"></td><td>100</td><td>110</td><td>90</td>
                </tr>
            </table>
        </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = sample_html.encode("utf-8")
        mock_request.return_value = mock_response

        collector = EconomicCalendarCollector(min_delay=0.1, max_delay=0.2)

        # Collect events
        events = collector.collect_events(
            start_date="2024-02-08", end_date="2024-02-08", countries=["us"]
        )

        # Save to CSV
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp_file:
            temp_filename = temp_file.name

        try:
            success = collector.save_to_csv(events, temp_filename)

            assert success is True
            assert len(events) == 3  # 3 countries x 1 event

            # Verify CSV was created and has content
            assert os.path.exists(temp_filename)
            with open(temp_filename) as f:
                content = f.read()
                assert "Test Event" in content
                assert "100" in content

        finally:
            os.unlink(temp_filename)
