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

from src.ingestion.collectors.calendar_collector import EconomicCalendarCollector


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
        """Sample HTML response matching modern Investing.com datatable-v2 layout."""
        return """
        <html>
        <body>
            <table class="datatable-v2_table__xMDOH">
                <tr class="header">
                    <th>Cur.</th><th>Time</th><th>Cur.</th><th>Event</th>
                    <th>Imp.</th><th>Actual</th><th>Forecast</th><th>Previous</th><th></th>
                </tr>
                <tr id="1-100-UnitedStates-0" class="datatable-v2_row__hkEus">
                    <td><span class="flag_flag__gUPtc flag_flag--UnitedStates__abc" title="United States"></span> USD</td>
                    <td><div></div></td>
                    <td><span class="flag_flag__gUPtc flag_flag--UnitedStates__abc" title="United States"></span><span>USD</span></td>
                    <td><a href="/events/non-farm-payrolls">Non-Farm Payrolls</a></td>
                    <td><div><svg class="opacity-60"><use href="#star"></use></svg><svg class="opacity-60"><use href="#star"></use></svg><svg class="opacity-60"><use href="#star"></use></svg></div></td>
                    <td>150K</td>
                    <td>180K</td>
                    <td>160K</td>
                    <td></td>
                </tr>
                <tr id="2-101-EuroZone-1" class="datatable-v2_row__hkEus">
                    <td><span class="flag_flag__gUPtc flag_flag--Europe__abc" title="Europe"></span> EUR</td>
                    <td><div></div></td>
                    <td><span class="flag_flag__gUPtc flag_flag--Europe__abc" title="Europe"></span><span>EUR</span></td>
                    <td><a href="/events/ecb-interest-rate">ECB Interest Rate Decision</a></td>
                    <td><div><svg class="opacity-60"><use href="#star"></use></svg><svg class="opacity-60"><use href="#star"></use></svg><svg class="opacity-20"><use href="#star"></use></svg></div></td>
                    <td>4.50%</td>
                    <td>4.50%</td>
                    <td>4.50%</td>
                    <td></td>
                </tr>
                <tr id="3-102-UnitedKingdom-2" class="datatable-v2_row__hkEus">
                    <td><span class="flag_flag__gUPtc flag_flag--UnitedKingdom__abc" title="United Kingdom"></span> GBP</td>
                    <td><div></div></td>
                    <td><span class="flag_flag__gUPtc flag_flag--UnitedKingdom__abc" title="United Kingdom"></span><span>GBP</span></td>
                    <td><a href="/events/gdp-growth">GDP Growth (QoQ)</a></td>
                    <td><div><svg class="opacity-60"><use href="#star"></use></svg><svg class="opacity-20"><use href="#star"></use></svg><svg class="opacity-20"><use href="#star"></use></svg></div></td>
                    <td>0.2%</td>
                    <td>0.3%</td>
                    <td>0.1%</td>
                    <td></td>
                </tr>
                <tr id="4-103-UnitedStates-3" class="datatable-v2_row__hkEus">
                    <td><span class="flag_flag__gUPtc flag_flag--UnitedStates__abc" title="United States"></span> USD</td>
                    <td><div></div></td>
                    <td><span class="flag_flag__gUPtc flag_flag--UnitedStates__abc" title="United States"></span><span>USD</span></td>
                    <td><a href="/events/cpi">Consumer Price Index</a></td>
                    <td><div><svg class="opacity-60"><use href="#star"></use></svg><svg class="opacity-60"><use href="#star"></use></svg><svg class="opacity-60"><use href="#star"></use></svg></div></td>
                    <td></td>
                    <td>3.2%</td>
                    <td>3.4%</td>
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
            <table class="datatable-v2_table__xMDOH">
                <tr class="header">
                    <th>Cur.</th><th>Time</th><th>Cur.</th><th>Event</th>
                    <th>Imp.</th><th>Actual</th><th>Forecast</th><th>Previous</th><th></th>
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
        # Test SVG-based impact (modern layout): 3 filled = High
        high_html = '<td><div><svg class="opacity-60"></svg><svg class="opacity-60"></svg><svg class="opacity-60"></svg></div></td>'
        high_el = BeautifulSoup(high_html, "html.parser").find("td")
        assert collector._parse_impact_level(high_el) == "High"

        # 2 filled = Medium
        medium_html = '<td><div><svg class="opacity-60"></svg><svg class="opacity-60"></svg><svg class="opacity-20"></svg></div></td>'
        medium_el = BeautifulSoup(medium_html, "html.parser").find("td")
        assert collector._parse_impact_level(medium_el) == "Medium"

        # 1 filled = Low
        low_html = '<td><div><svg class="opacity-60"></svg><svg class="opacity-20"></svg><svg class="opacity-20"></svg></div></td>'
        low_el = BeautifulSoup(low_html, "html.parser").find("td")
        assert collector._parse_impact_level(low_el) == "Low"

        # Legacy CSS class fallback
        legacy_el = Mock()
        legacy_el.find_all.return_value = []  # No SVGs
        legacy_el.get.return_value = ["impact", "high"]
        assert collector._parse_impact_level(legacy_el) == "High"

        # Test None element
        assert collector._parse_impact_level(None) == "Unknown"

    @patch("src.ingestion.collectors.calendar_collector.RobotFileParser")
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

    @patch("src.ingestion.collectors.calendar_collector.RobotFileParser")
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
        """Test parsing of individual calendar rows (modern datatable-v2 layout)."""
        html_row = """
        <tr id="1-100-UnitedStates-0" class="datatable-v2_row__hkEus">
            <td><span class="flag_flag__gUPtc flag_flag--UnitedStates__abc" title="United States"></span> USD</td>
            <td><div></div></td>
            <td><span class="flag_flag__gUPtc flag_flag--UnitedStates__abc" title="United States"></span><span>USD</span></td>
            <td><a href="/events/non-farm-payrolls">Non-Farm Payrolls</a></td>
            <td><div><svg class="opacity-60"><use href="#s"></use></svg><svg class="opacity-60"><use href="#s"></use></svg><svg class="opacity-60"><use href="#s"></use></svg></div></td>
            <td>150K</td>
            <td>180K</td>
            <td>160K</td>
            <td></td>
        </tr>
        """
        soup = BeautifulSoup(html_row, "html.parser")
        row = soup.find("tr")

        event_data = collector._parse_calendar_row(row)

        assert event_data is not None
        assert event_data["country"] == "United States"
        assert event_data["event"] == "Non-Farm Payrolls"
        assert event_data["impact"] == "High"  # 3 filled stars
        assert event_data["actual"] == "150K"
        assert event_data["forecast"] == "180K"
        assert event_data["previous"] == "160K"
        assert event_data["event_url"] == "https://www.investing.com/events/non-farm-payrolls"
        assert "date" in event_data
        assert "scraped_at" in event_data

    def test_parse_calendar_row_missing_values(self, collector):
        """Test parsing calendar rows with missing values."""
        html_row = """
        <tr id="4-103-UnitedStates-3" class="datatable-v2_row__hkEus">
            <td><span class="flag_flag__gUPtc flag_flag--UnitedStates__abc" title="United States"></span> USD</td>
            <td><div></div></td>
            <td><span class="flag_flag__gUPtc flag_flag--UnitedStates__abc" title="United States"></span><span>USD</span></td>
            <td><a href="/events/cpi">Consumer Price Index</a></td>
            <td><div><svg class="opacity-60"></svg><svg class="opacity-60"></svg><svg class="opacity-60"></svg></div></td>
            <td>-</td>
            <td>3.2%</td>
            <td>3.4%</td>
            <td></td>
        </tr>
        """
        soup = BeautifulSoup(html_row, "html.parser")
        row = soup.find("tr")

        event_data = collector._parse_calendar_row(row)

        assert event_data is not None
        assert event_data["actual"] is None  # "-" cleaned to None
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
        assert first_event["country"] == "United States"
        assert first_event["event"] == "Non-Farm Payrolls"
        assert first_event["impact"] == "High"  # 3 filled stars
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

    @patch(
        "src.ingestion.collectors.calendar_collector.EconomicCalendarCollector._make_request_with_retry"
    )
    def test_full_collection_workflow(self, mock_request):
        """Test the complete event collection workflow."""
        sample_html = """
        <html>
        <body>
            <table class="datatable-v2_table__xMDOH">
                <tr class="header">
                    <th>Cur.</th><th>Time</th><th>Cur.</th><th>Event</th>
                    <th>Imp.</th><th>Actual</th><th>Forecast</th><th>Previous</th><th></th>
                </tr>
                <tr id="1-100-UnitedStates-0" class="datatable-v2_row__hkEus">
                    <td><span class="flag_flag__gUPtc flag_flag--UnitedStates__abc" title="United States"></span> USD</td>
                    <td><div></div></td>
                    <td><span>USD</span></td>
                    <td><a href="/events/test">Test Event</a></td>
                    <td><div><svg class="opacity-60"></svg><svg class="opacity-60"></svg><svg class="opacity-60"></svg></div></td>
                    <td>100</td>
                    <td>110</td>
                    <td>90</td>
                    <td></td>
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
            assert len(events) == 1

            # Verify CSV was created and has content
            assert os.path.exists(temp_filename)
            with open(temp_filename) as f:
                content = f.read()
                assert "Test Event" in content
                assert "100" in content

        finally:
            os.unlink(temp_filename)


class TestBaseCollectorInterface:
    """Test BaseCollector interface implementation."""

    def test_source_name(self):
        """Test SOURCE_NAME class attribute is defined."""
        from src.ingestion.collectors.calendar_collector import EconomicCalendarCollector

        assert hasattr(EconomicCalendarCollector, "SOURCE_NAME")
        assert EconomicCalendarCollector.SOURCE_NAME == "investing"

    def test_collect_method_interface(self):
        """Test collect() method conforms to BaseCollector interface."""
        import tempfile
        from datetime import datetime
        from pathlib import Path

        from src.ingestion.collectors.calendar_collector import EconomicCalendarCollector

        with tempfile.TemporaryDirectory() as tmpdir:
            collector = EconomicCalendarCollector(
                min_delay=0.1,
                max_delay=0.2,
                output_dir=Path(tmpdir),
            )

            # Mock the collect_events method
            with patch.object(collector, "collect_events") as mock_collect_events:
                mock_collect_events.return_value = [
                    {
                        "date": "2024-02-08",
                        "time": "13:30",
                        "country": "United States",
                        "event": "Test Event",
                        "impact": "High",
                        "actual": "100",
                        "forecast": "110",
                        "previous": "90",
                    }
                ]

                # Call collect() with datetime objects
                result = collector.collect(
                    start_date=datetime(2024, 2, 8), end_date=datetime(2024, 2, 8)
                )

                # Verify return type
                assert isinstance(result, dict)
                assert "economic_events" in result
                assert isinstance(result["economic_events"], __import__("pandas").DataFrame)

                # Verify source column exists
                df = result["economic_events"]
                assert "source" in df.columns
                assert df["source"].iloc[0] == "investing.com"

    def test_health_check_method_interface(self):
        """Test health_check() method conforms to BaseCollector interface."""
        from src.ingestion.collectors.calendar_collector import EconomicCalendarCollector

        collector = EconomicCalendarCollector(min_delay=0.1, max_delay=0.2)

        # Mock requests.head
        with patch("requests.head") as mock_head:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_head.return_value = mock_response

            result = collector.health_check()

            assert isinstance(result, bool)
            assert result is True

    def test_inherits_from_base_collector(self):
        """Test that EconomicCalendarCollector inherits from BaseCollector."""
        from src.ingestion.collectors.base_collector import BaseCollector
        from src.ingestion.collectors.calendar_collector import EconomicCalendarCollector

        assert issubclass(EconomicCalendarCollector, BaseCollector)

    def test_export_csv_method_inherited(self):
        """Test that export_csv() method is inherited from BaseCollector."""
        import tempfile
        from pathlib import Path

        from src.ingestion.collectors.calendar_collector import EconomicCalendarCollector

        with tempfile.TemporaryDirectory() as tmpdir:
            collector = EconomicCalendarCollector(
                min_delay=0.1, max_delay=0.2, output_dir=Path(tmpdir)
            )

            # Create a test DataFrame
            import pandas as pd

            df = pd.DataFrame([{"date": "2024-02-08", "event": "Test", "source": "test"}])

            # Call inherited export_csv method
            output_path = collector.export_csv(df, "test_dataset")

            assert output_path.exists()
            assert "investing_test_dataset_" in output_path.name
            assert output_path.suffix == ".csv"
