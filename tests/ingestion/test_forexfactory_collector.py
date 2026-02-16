"""
Unit tests for Forex Factory Calendar Collector.

Tests include:
- Web scraping functionality with mocked HTML responses
- Robots.txt compliance
- Rate limiting functionality
- Error handling and retry logic
- CSV output functionality
- Data parsing and validation
- Health check functionality
"""

import csv
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from bs4 import BeautifulSoup

from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector


class TestForexFactoryCalendarCollector:
    """Test cases for ForexFactoryCalendarCollector class."""

    @pytest.fixture
    def collector(self, tmp_path):
        """Create a collector instance for testing."""
        return ForexFactoryCalendarCollector(
            base_url="https://www.forexfactory.com",
            min_delay=0.1,  # Shorter delay for tests
            max_delay=0.2,
            max_retries=1,  # Low retries for tests
            timeout=10,
            output_dir=tmp_path,
        )

    @pytest.fixture
    def sample_html_response(self):
        """Sample HTML response matching Forex Factory calendar layout (10-cell structure)."""
        return """
        <html>
        <body>
            <table class="calendar__table">
                <tr class="calendar__row calendar__row--date">
                    <td colspan="10">
                        <span class="date">Monday, February 12, 2024</span>
                    </td>
                </tr>
                <tr class="calendar__row calendar__row--event">
                    <td class="calendar__time">8:30am</td>
                    <td class="calendar__currency">USD</td>
                    <td class="calendar__impact high">
                        <span title="High Impact">High</span>
                    </td>
                    <td class="calendar__event">CPI m/m</td>
                    <td class="calendar__sub"></td>
                    <td class="calendar__detail"><a href="/news/1234"></a></td>
                    <td class="calendar__actual">0.3%</td>
                    <td class="calendar__forecast">0.3%</td>
                    <td class="calendar__previous">0.4%</td>
                    <td></td>
                </tr>
                <tr class="calendar__row calendar__row--event">
                    <td class="calendar__time">10:00am</td>
                    <td class="calendar__currency">EUR</td>
                    <td class="calendar__impact medium">
                        <span title="Medium Impact">Medium</span>
                    </td>
                    <td class="calendar__event">ECB Interest Rate</td>
                    <td class="calendar__sub"></td>
                    <td class="calendar__detail"><a href="/news/5678"></a></td>
                    <td class="calendar__actual">4.50%</td>
                    <td class="calendar__forecast">4.50%</td>
                    <td class="calendar__previous">4.50%</td>
                    <td></td>
                </tr>
                <tr class="calendar__row calendar__row--event">
                    <td class="calendar__time">2:00pm</td>
                    <td class="calendar__currency">GBP</td>
                    <td class="calendar__impact low">
                        <span title="Low Impact">Low</span>
                    </td>
                    <td class="calendar__event">GDP Estimate</td>
                    <td class="calendar__sub"></td>
                    <td class="calendar__detail"><a href="/news/9012"></a></td>
                    <td class="calendar__actual">-</td>
                    <td class="calendar__forecast">0.2%</td>
                    <td class="calendar__previous">0.1%</td>
                    <td></td>
                </tr>
                <tr class="calendar__row calendar__row--event">
                    <td class="calendar__time">4:30pm</td>
                    <td class="calendar__currency">JPY</td>
                    <td class="calendar__impact high">
                        <span title="High Impact">High</span>
                    </td>
                    <td class="calendar__event">BOJ Policy Rate</td>
                    <td class="calendar__sub"></td>
                    <td class="calendar__detail"><a href="/news/3456"></a></td>
                    <td class="calendar__actual"></td>
                    <td class="calendar__forecast">-0.1%</td>
                    <td class="calendar__previous">-0.1%</td>
                    <td></td>
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
            <table class="calendar__table">
                <tr class="calendar__row calendar__row--date">
                    <td colspan="10">
                        <span class="date">Monday, February 12, 2024</span>
                    </td>
                </tr>
            </table>
        </body>
        </html>
        """

    def test_initialization(self, collector):
        """Test collector initialization with default parameters."""
        assert collector.base_url == "https://www.forexfactory.com"
        assert collector.min_delay == 0.1
        assert collector.max_delay == 0.2
        assert collector.max_retries == 1
        assert collector.timeout == 10
        assert len(collector.user_agents) > 0
        assert isinstance(collector.headers, dict)
        assert collector.output_dir.exists()

    def test_source_name(self):
        """Test SOURCE_NAME class attribute."""
        assert ForexFactoryCalendarCollector.SOURCE_NAME == "forexfactory"

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
        high_html = '<td class="calendar__impact high"><span>High</span></td>'
        high_el = BeautifulSoup(high_html, "html.parser").find("td")
        assert collector._parse_impact_level(high_el) == "High"

        # Test medium impact
        medium_html = (
            '<td class="calendar__impact medium"><span title="Medium Impact">Medium</span></td>'
        )
        medium_el = BeautifulSoup(medium_html, "html.parser").find("td")
        assert collector._parse_impact_level(medium_el) == "Medium"

        # Test low impact
        low_html = '<td class="calendar__impact low"><span>Low</span></td>'
        low_el = BeautifulSoup(low_html, "html.parser").find("td")
        assert collector._parse_impact_level(low_el) == "Low"

        # Test title attribute parsing
        title_html = '<td><span title="High Impact Expected">Icon</span></td>'
        title_el = BeautifulSoup(title_html, "html.parser").find("td")
        assert collector._parse_impact_level(title_el) == "High"

        # Test None element
        assert collector._parse_impact_level(None) == "Unknown"

        # Test unknown impact
        unknown_html = '<td class="calendar__impact"><span>Unknown</span></td>'
        unknown_el = BeautifulSoup(unknown_html, "html.parser").find("td")
        assert collector._parse_impact_level(unknown_el) == "Unknown"

    def test_clean_value(self, collector):
        """Test value cleaning functionality."""
        # Valid values
        assert collector._clean_value("0.3%") == "0.3%"
        assert collector._clean_value("  150K  ") == "150K"
        assert collector._clean_value("4.50%") == "4.50%"

        # Invalid/null values
        assert collector._clean_value("-") is None
        assert collector._clean_value("N/A") is None
        assert collector._clean_value("na") is None
        assert collector._clean_value("") is None
        assert collector._clean_value("—") is None
        assert collector._clean_value(None) is None

    def test_parse_calendar_row(self, collector):
        """Test parsing of individual calendar rows (10-cell structure)."""
        html_row = """
        <tr class="calendar__row calendar__row--event">
            <td class="calendar__time">8:30am</td>
            <td class="calendar__currency">USD</td>
            <td class="calendar__impact high">
                <span title="High Impact">High</span>
            </td>
            <td class="calendar__event">CPI m/m</td>
            <td class="calendar__sub"></td>
            <td class="calendar__detail"><a href="/news/1234"></a></td>
            <td class="calendar__actual">0.3%</td>
            <td class="calendar__forecast">0.3%</td>
            <td class="calendar__previous">0.4%</td>
            <td></td>
        </tr>
        """
        soup = BeautifulSoup(html_row, "html.parser")
        row = soup.find("tr")

        event_data = collector._parse_calendar_row(row)

        assert event_data is not None
        assert event_data["time"] == "8:30am"
        assert event_data["currency"] == "USD"
        assert event_data["impact"] == "High"
        assert event_data["event"] == "CPI m/m"
        assert event_data["actual"] == "0.3%"
        assert event_data["forecast"] == "0.3%"
        assert event_data["previous"] == "0.4%"
        assert event_data["event_url"] == "https://www.forexfactory.com/news/1234"
        assert event_data["source_url"] == "https://www.forexfactory.com/calendar"
        assert event_data["source"] == "forexfactory.com"
        assert "scraped_at" in event_data

    def test_parse_calendar_row_missing_values(self, collector):
        """Test parsing calendar rows with missing values (10-cell structure)."""
        html_row = """
        <tr class="calendar__row calendar__row--event">
            <td class="calendar__time">4:30pm</td>
            <td class="calendar__currency">JPY</td>
            <td class="calendar__impact high">
                <span title="High Impact">High</span>
            </td>
            <td class="calendar__event">BOJ Policy Rate</td>
            <td class="calendar__sub"></td>
            <td class="calendar__detail"><a href="/news/3456"></a></td>
            <td class="calendar__actual"></td>
            <td class="calendar__forecast">-0.1%</td>
            <td class="calendar__previous">0.4%</td>
            <td></td>
        </tr>
        """
        soup = BeautifulSoup(html_row, "html.parser")
        row = soup.find("tr")

        event_data = collector._parse_calendar_row(row)

        assert event_data is not None
        assert event_data["actual"] is None  # "-" cleaned to None
        assert event_data["forecast"] == "-0.1%"
        assert event_data["previous"] == "0.4%"

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

    def test_parse_calendar_row_no_event_name(self, collector):
        """Test parsing rows without event name."""
        html_row = """
        <tr class="calendar__row calendar__row--event">
            <td class="calendar__time">8:30am</td>
            <td class="calendar__currency">USD</td>
            <td class="calendar__impact high"></td>
            <td class="calendar__event"></td>
            <td class="calendar__actual">0.3%</td>
            <td class="calendar__forecast">0.3%</td>
            <td class="calendar__previous">0.4%</td>
        </tr>
        """
        soup = BeautifulSoup(html_row, "html.parser")
        row = soup.find("tr")

        event_data = collector._parse_calendar_row(row)
        assert event_data is None

    @patch.object(ForexFactoryCalendarCollector, "_fetch_page_with_selenium")
    def test_fetch_calendar_for_date_success(self, mock_fetch, collector, sample_html_response):
        """Test successful calendar data fetching."""
        mock_fetch.return_value = sample_html_response

        events, date = collector._fetch_calendar_for_date("2024-02-12")

        assert len(events) == 4
        assert date is not None

        # Check first event
        first_event = events[0]
        assert first_event["currency"] == "USD"
        assert first_event["event"] == "CPI m/m"
        assert first_event["impact"] == "High"
        assert first_event["actual"] == "0.3%"
        assert first_event["forecast"] == "0.3%"
        assert first_event["previous"] == "0.4%"

        # Check event with missing actual value
        third_event = events[2]
        assert third_event["currency"] == "GBP"
        assert third_event["actual"] is None  # "-" converted to None

    @patch.object(ForexFactoryCalendarCollector, "_fetch_page_with_selenium")
    def test_fetch_calendar_for_date_no_table(self, mock_fetch, collector, empty_html_response):
        """Test fetching when no calendar table is found."""
        mock_fetch.return_value = empty_html_response

        events, date = collector._fetch_calendar_for_date("2024-02-12")

        assert events == []

    @patch.object(ForexFactoryCalendarCollector, "_fetch_page_with_selenium")
    def test_fetch_calendar_for_date_request_failure(self, mock_fetch, collector):
        """Test fetching when request fails."""
        mock_fetch.return_value = None

        events, date = collector._fetch_calendar_for_date("2024-02-12")

        assert events == []
        assert date is None

    def test_fetch_calendar_data_single_day(self, collector):
        """Test collecting events for a single day."""
        with patch.object(collector, "_fetch_calendar_by_url") as mock_fetch:
            mock_fetch.return_value = [
                {
                    "date": "2024-02-12",
                    "time": "8:30am",
                    "currency": "USD",
                    "event": "CPI m/m",
                    "impact": "High",
                    "actual": "0.3%",
                    "forecast": "0.3%",
                    "previous": "0.4%",
                }
            ]

            events = collector._fetch_calendar_data("2024-02-12", "2024-02-12")

            assert len(events) == 1
            # For single day, should use day view
            mock_fetch.assert_called_once()
            call_args = mock_fetch.call_args[0]
            assert call_args[1] == "day"  # view_type

    def test_fetch_calendar_data_date_range(self, collector):
        """Test collecting events for a date range (same week)."""
        mock_event = {
            "date": "2024-02-12",
            "time": "8:30am",
            "currency": "USD",
            "event": "CPI m/m",
            "impact": "High",
            "actual": "0.3%",
            "forecast": "0.3%",
            "previous": "0.4%",
        }

        with patch.object(collector, "_fetch_calendar_by_url") as mock_fetch:
            # Return same event for all days in the week view
            mock_fetch.return_value = [mock_event] * 7
            with patch.object(collector, "_is_event_in_range") as mock_range:
                # Only accept first 3 events
                mock_range.side_effect = [True, True, True] + [False] * 4

                events = collector._fetch_calendar_data("2024-02-12", "2024-02-14")

            # Should use week view (1 request) for 3 days in same week
            assert mock_fetch.call_count == 1
            assert len(events) == 3

    def test_fetch_calendar_data_invalid_date(self, collector):
        """Test collecting events with invalid date format."""
        events = collector._fetch_calendar_data("invalid-date", "2024-02-12")

        assert events == []

    def test_fetch_calendar_data_start_after_end(self, collector):
        """Test collecting events with start after end date."""
        events = collector._fetch_calendar_data("2024-02-14", "2024-02-12")

        assert events == []

    def test_collect_events(self, collector):
        """Test collect_events method."""
        with patch.object(collector, "_fetch_calendar_data") as mock_fetch:
            mock_fetch.return_value = [
                {
                    "date": "2024-02-12",
                    "time": "8:30am",
                    "currency": "USD",
                    "event": "CPI m/m",
                    "impact": "High",
                    "actual": "0.3%",
                    "forecast": "0.3%",
                    "previous": "0.4%",
                }
            ]

            events = collector.collect_events(start_date="2024-02-12", end_date="2024-02-12")

            assert len(events) == 1
            mock_fetch.assert_called_once_with("2024-02-12", "2024-02-12")

    def test_collect_events_defaults(self, collector):
        """Test collect_events with default dates."""
        with patch.object(collector, "_fetch_calendar_data") as mock_fetch:
            mock_fetch.return_value = []

            # Call without dates - should default to today
            _ = collector.collect_events()

            # Verify it was called with today's date
            today = datetime.now().strftime("%Y-%m-%d")
            mock_fetch.assert_called_once_with(today, today)

    def test_save_to_csv_success(self, collector, tmp_path):
        """Test successful CSV saving."""
        events = [
            {
                "date": "2024-02-12",
                "time": "8:30am",
                "currency": "USD",
                "event": "CPI m/m",
                "impact": "High",
                "actual": "0.3%",
                "forecast": "0.3%",
                "previous": "0.4%",
                "event_url": "https://www.forexfactory.com/news/1234",
                "source_url": "https://www.forexfactory.com/calendar",
                "scraped_at": "2024-02-12T10:00:00Z",
                "source": "forexfactory.com",
            },
            {
                "date": "2024-02-12",
                "time": "10:00am",
                "currency": "EUR",
                "event": "ECB Interest Rate",
                "impact": "Medium",
                "actual": "4.50%",
                "forecast": "4.50%",
                "previous": "4.50%",
                "event_url": "https://www.forexfactory.com/news/5678",
                "source_url": "https://www.forexfactory.com/calendar",
                "scraped_at": "2024-02-12T10:00:00Z",
                "source": "forexfactory.com",
            },
        ]

        output_path = collector.save_to_csv(events)

        assert output_path is not None
        assert output_path.exists()
        assert "forexfactory_calendar_" in output_path.name

        # Verify CSV content
        with open(output_path, encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)

            assert len(rows) == 2
            assert rows[0]["event"] == "CPI m/m"
            assert rows[0]["impact"] == "High"
            assert rows[0]["actual"] == "0.3%"
            assert rows[1]["event"] == "ECB Interest Rate"
            assert rows[1]["impact"] == "Medium"
            assert rows[1]["actual"] == "4.50%"

    def test_save_to_csv_no_events(self, collector):
        """Test CSV saving with no events."""
        output_path = collector.save_to_csv([])
        assert output_path is None

    def test_save_to_csv_custom_filename(self, collector, tmp_path):
        """Test CSV saving with custom filename."""
        events = [
            {
                "date": "2024-02-12",
                "time": "8:30am",
                "currency": "USD",
                "event": "CPI m/m",
                "impact": "High",
                "actual": "0.3%",
                "forecast": "0.3%",
                "previous": "0.4%",
            }
        ]

        custom_file = str(tmp_path / "custom_output.csv")
        output_path = collector.save_to_csv(events, custom_file)

        assert output_path is not None
        assert output_path.name == "custom_output.csv"

    def test_get_events_dataframe(self, collector):
        """Test converting events to DataFrame."""
        events = [
            {
                "date": "2024-02-12",
                "time": "8:30am",
                "currency": "USD",
                "event": "CPI m/m",
                "impact": "High",
                "actual": "0.3%",
                "forecast": "0.3%",
                "previous": "0.4%",
                "event_url": "https://www.forexfactory.com/news/1234",
                "source_url": "https://www.forexfactory.com/calendar",
                "scraped_at": "2024-02-12T10:00:00Z",
                "source": "forexfactory.com",
            }
        ]

        df = collector.get_events_dataframe(events)

        assert df is not None
        assert len(df) == 1
        assert df.iloc[0]["event"] == "CPI m/m"
        assert df.iloc[0]["impact"] == "High"

        # Check date conversion
        import pandas as pd

        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_get_events_dataframe_no_events(self, collector):
        """Test DataFrame conversion with no events."""
        df = collector.get_events_dataframe([])
        assert df is None

    def test_validate_scraped_data_valid(self, collector):
        """Test data validation with valid events."""
        events = [
            {
                "date": "2024-02-12",
                "currency": "USD",
                "event": "CPI m/m",
                "impact": "High",
                "actual": "0.3%",
                "forecast": "0.3%",
                "previous": "0.4%",
            },
            {
                "date": "2024-02-12",
                "currency": "EUR",
                "event": "ECB Rate",
                "impact": "Medium",
            },
        ]

        is_valid, errors = collector.validate_scraped_data(events)

        assert is_valid is True
        assert len(errors) == 0

    def test_validate_scraped_data_empty(self, collector):
        """Test data validation with empty events."""
        is_valid, errors = collector.validate_scraped_data([])

        assert is_valid is False
        assert len(errors) == 1
        assert "No events" in errors[0]

    def test_validate_scraped_data_missing_fields(self, collector):
        """Test data validation with missing required fields."""
        events = [
            {
                "date": "2024-02-12",
                # Missing currency, event, impact
                "actual": "0.3%",
            }
        ]

        is_valid, errors = collector.validate_scraped_data(events)

        assert is_valid is False
        assert len(errors) == 3  # Missing currency, event, impact

    def test_validate_scraped_data_invalid_date(self, collector):
        """Test data validation with invalid date format."""
        events = [
            {
                "date": "invalid-date",
                "currency": "USD",
                "event": "CPI m/m",
                "impact": "High",
            }
        ]

        is_valid, errors = collector.validate_scraped_data(events)

        assert is_valid is False
        assert len(errors) == 1
        assert "Invalid date format" in errors[0]

    @patch.object(ForexFactoryCalendarCollector, "_init_driver")
    def test_health_check_success(self, mock_init_driver, collector):
        """Test successful health check."""
        mock_driver = Mock()
        mock_driver.title = "Forex Factory - Economic Calendar"
        mock_init_driver.return_value = mock_driver

        result = collector.health_check()

        assert result is True
        mock_driver.get.assert_called_once()

    @patch.object(ForexFactoryCalendarCollector, "_init_driver")
    def test_health_check_failure(self, mock_init_driver, collector):
        """Test health check when site returns error."""
        mock_driver = Mock()
        mock_driver.title = "Error - 503 Service Unavailable"
        mock_init_driver.return_value = mock_driver

        result = collector.health_check()

        assert result is False

    @patch.object(ForexFactoryCalendarCollector, "_init_driver")
    def test_health_check_exception(self, mock_init_driver, collector):
        """Test health check when request raises exception."""
        mock_init_driver.side_effect = Exception("Connection error")

        result = collector.health_check()

        assert result is False

    def test_inherits_from_base_collector(self):
        """Test that ForexFactoryCalendarCollector inherits from BaseCollector."""
        from src.ingestion.collectors.base_collector import BaseCollector

        assert issubclass(ForexFactoryCalendarCollector, BaseCollector)

    def test_export_csv_method_inherited(self, collector):
        """Test that export_csv() method is inherited from BaseCollector."""
        import pandas as pd

        df = pd.DataFrame([{"date": "2024-02-12", "event": "Test", "source": "test"}])

        output_path = collector.export_csv(df, "test_dataset")

        assert output_path.exists()
        assert "forexfactory_test_dataset_" in output_path.name
        assert output_path.suffix == ".csv"


class TestRateLimiting:
    """Test rate limiting functionality."""

    @pytest.fixture
    def collector(self, tmp_path):
        """Create a collector instance for testing."""
        return ForexFactoryCalendarCollector(
            min_delay=0.1,
            max_delay=0.2,
            max_retries=1,
            output_dir=tmp_path,
        )

    def test_apply_rate_limit(self, collector):
        """Test rate limiting between requests."""
        import time

        collector._apply_rate_limit()
        first_request = time.time()

        collector._apply_rate_limit()
        second_request = time.time()

        # Should have waited at least min_delay between requests
        elapsed = second_request - first_request
        assert elapsed >= collector.min_delay - 0.05  # Allow small tolerance

    def test_rate_limit_respects_minimum(self, collector):
        """Test that rate limit respects minimum delay."""
        import time

        # Set last request time to now, forcing a wait on next call
        collector._last_request_time = time.time()

        start = time.time()
        collector._apply_rate_limit()
        elapsed = time.time() - start

        # Should wait at least min_delay (allowing small tolerance for random jitter)
        assert elapsed >= collector.min_delay - 0.01


class TestRobotsTxtCompliance:
    """Test robots.txt compliance functionality."""

    @pytest.fixture
    def collector(self, tmp_path):
        """Create a collector instance for testing."""
        return ForexFactoryCalendarCollector(
            min_delay=0.1,
            max_delay=0.2,
            output_dir=tmp_path,
        )

    @patch("src.ingestion.collectors.forexfactory_collector.RobotFileParser")
    def test_is_calendar_access_allowed_no_restrictions(self, mock_robots, collector):
        """Test calendar access check with no robots.txt restrictions."""
        mock_parser = Mock()
        mock_parser.entries = []  # No entries = no restrictions
        mock_parser.can_fetch.return_value = True
        mock_robots.return_value = mock_parser

        collector.robots_parser = mock_parser

        result = collector._is_calendar_access_allowed()
        assert result is True

    @patch("src.ingestion.collectors.forexfactory_collector.RobotFileParser")
    def test_is_calendar_access_allowed_blocked(self, mock_robots, collector):
        """Test calendar access check when blocked by robots.txt."""
        mock_parser = Mock()
        mock_parser.entries = [{"*": ["/calendar"]}]
        mock_parser.can_fetch.return_value = False
        mock_robots.return_value = mock_parser

        collector.robots_parser = mock_parser

        result = collector._is_calendar_access_allowed()
        assert result is False


class TestBaseCollectorInterface:
    """Test BaseCollector interface implementation."""

    def test_collect_method_interface(self, tmp_path):
        """Test collect() method conforms to BaseCollector interface."""
        from datetime import datetime

        collector = ForexFactoryCalendarCollector(
            min_delay=0.1,
            max_delay=0.2,
            output_dir=tmp_path,
        )

        # Mock the collect_events method
        with patch.object(collector, "collect_events") as mock_collect_events:
            mock_collect_events.return_value = [
                {
                    "date": "2024-02-12",
                    "time": "8:30am",
                    "currency": "USD",
                    "event": "CPI m/m",
                    "impact": "High",
                    "actual": "0.3%",
                    "forecast": "0.3%",
                    "previous": "0.4%",
                }
            ]

            # Call collect() with datetime objects
            result = collector.collect(
                start_date=datetime(2024, 2, 12), end_date=datetime(2024, 2, 12)
            )

            # Verify return type
            assert isinstance(result, dict)
            assert "calendar" in result
            assert isinstance(result["calendar"], __import__("pandas").DataFrame)

            # Verify source column exists
            df = result["calendar"]
            assert "source" in df.columns
            assert df["source"].iloc[0] == "forexfactory.com"


class TestErrorHandling:
    """Test error handling functionality."""

    @pytest.fixture
    def collector(self, tmp_path):
        """Create a collector instance for testing."""
        return ForexFactoryCalendarCollector(
            min_delay=0.1,
            max_delay=0.2,
            max_retries=1,
            output_dir=tmp_path,
        )

    @patch.object(ForexFactoryCalendarCollector, "_apply_rate_limit")
    def test_make_request_rate_limited(self, mock_rate_limit, collector):
        """Test handling of 429 rate limit response."""
        with patch.object(collector.session, "get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 429
            mock_get.return_value = mock_response

            result = collector._make_request("https://www.forexfactory.com/calendar")

            assert result is None

    @patch.object(ForexFactoryCalendarCollector, "_apply_rate_limit")
    def test_make_request_service_unavailable(self, mock_rate_limit, collector):
        """Test handling of 503 service unavailable response."""
        with patch.object(collector.session, "get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 503
            mock_get.return_value = mock_response

            result = collector._make_request("https://www.forexfactory.com/calendar")

            # Should retry but eventually fail
            assert result is None

    @patch.object(ForexFactoryCalendarCollector, "_apply_rate_limit")
    def test_make_request_success_after_retry(self, mock_rate_limit, collector):
        """Test successful request after retry."""
        with patch.object(collector.session, "get") as mock_get:
            # First call fails, second succeeds
            mock_response_fail = Mock()
            mock_response_fail.status_code = 503
            mock_response_fail.raise_for_status.side_effect = Exception("503 error")

            mock_response_success = Mock()
            mock_response_success.status_code = 200
            mock_response_success.raise_for_status.return_value = None

            mock_get.side_effect = [mock_response_fail, mock_response_success]

            result = collector._make_request("https://www.forexfactory.com/calendar")

            assert result == mock_response_success


class TestIntegration:
    """Integration tests for the Forex Factory Calendar Collector."""

    @patch.object(ForexFactoryCalendarCollector, "_fetch_page_with_selenium")
    def test_full_collection_workflow(self, mock_fetch, tmp_path):
        """Test the complete event collection workflow."""
        sample_html = """
        <html>
        <body>
            <table class="calendar__table">
                <tr class="calendar__row calendar__row--date">
                    <td colspan="10">
                        <span class="date">Monday, February 12, 2024</span>
                    </td>
                </tr>
                <tr class="calendar__row calendar__row--event">
                    <td class="calendar__time">8:30am</td>
                    <td class="calendar__currency">USD</td>
                    <td class="calendar__impact high">
                        <span title="High Impact">High</span>
                    </td>
                    <td class="calendar__event">Test Event</td>
                    <td class="calendar__sub"></td>
                    <td class="calendar__detail"><a href="/news/1234"></a></td>
                    <td class="calendar__actual">100</td>
                    <td class="calendar__forecast">110</td>
                    <td class="calendar__previous">90</td>
                    <td></td>
                </tr>
            </table>
        </body>
        </html>
        """

        mock_fetch.return_value = sample_html

        collector = ForexFactoryCalendarCollector(
            min_delay=0.1,
            max_delay=0.2,
            output_dir=tmp_path,
        )

        # Collect events
        events = collector.collect_events(start_date="2024-02-12", end_date="2024-02-12")

        # Save to CSV
        output_path = collector.save_to_csv(events)

        assert output_path is not None
        assert output_path.exists()
        assert len(events) == 1

        # Verify CSV was created and has content
        with open(output_path) as f:
            content = f.read()
            assert "Test Event" in content
            assert "100" in content

    @patch.object(ForexFactoryCalendarCollector, "_fetch_page_with_selenium")
    def test_collection_with_robots_txt_blocking(self, mock_fetch, tmp_path):
        """Test collection when robots.txt blocks access."""
        collector = ForexFactoryCalendarCollector(
            min_delay=0.1,
            max_delay=0.2,
            output_dir=tmp_path,
        )

        # Mock robots.txt to block access
        with patch.object(collector, "robots_parser") as mock_robots:
            mock_robots.entries = [{"*": ["/calendar"]}]
            mock_robots.can_fetch.return_value = False

            events = collector._fetch_calendar_data("2024-02-12", "2024-02-12")

            assert events == []
