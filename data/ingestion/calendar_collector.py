import csv
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import pandas as pd
import requests
from bs4 import BeautifulSoup


class EconomicCalendarCollector:
    """
    Web scraper for Investing.com economic calendar data.

    Features:
    - Scrapes US, Eurozone, UK economic events
    - Captures actual vs forecast vs previous values
    - Respects robots.txt with configurable delays
    - User-agent rotation and request headers
    - Error handling with retry logic and exponential backoff
    - CSV output functionality
    """

    def __init__(
        self,
        base_url: str = "https://www.investing.com",
        min_delay: float = 3.0,
        max_delay: float = 5.0,
        max_retries: int = 3,
        timeout: int = 30,
    ):
        """
        Initialize the Economic Calendar Collector.

        Args:
            base_url: Base URL for Investing.com
            min_delay: Minimum delay between requests (seconds)
            max_delay: Maximum delay between requests (seconds)
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.timeout = timeout

        # User agents for rotation
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        ]

        # Request headers
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        # Setup logging first
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Robots.txt parser
        self.robots_parser = RobotFileParser()
        robots_url = urljoin(base_url, "/robots.txt")
        self.robots_parser.set_url(robots_url)
        self._load_robots_txt()

        # Session for connection pooling
        self.session = requests.Session()

    def _load_robots_txt(self) -> None:
        """Load and parse robots.txt file."""
        try:
            # Try the simple read first
            self.robots_parser.read()

            # If no entries were parsed, try manual fetching
            if not self.robots_parser.entries:
                self.logger.warning("Standard robots.txt parsing failed, trying manual fetch...")
                robots_url = self.robots_parser.url

                # Fetch robots.txt manually using requests directly
                import requests

                response = requests.get(robots_url, timeout=10)
                response.raise_for_status()

                # Parse manually
                from io import StringIO

                robots_content = response.text
                self.robots_parser.parse(StringIO(robots_content))
                self.robots_parser.url = robots_url

            if self.robots_parser.entries:
                self.logger.info(f"Successfully loaded robots.txt from {self.robots_parser.url}")
                self.logger.info(f"Found {len(self.robots_parser.entries)} rule entries")
            else:
                self.logger.warning("No robots.txt rules found, allowing access by default")

        except Exception as e:
            self.logger.warning(f"Failed to load robots.txt: {e}. Proceeding with caution.")
            # Since we know from manual inspection that economic-calendar is allowed,
            # we'll create a minimal robots.txt parser with the known rules
            self._create_fallback_robots_parser()

    def _create_fallback_robots_parser(self) -> None:
        """Create a fallback robots.txt parser with known Investing.com rules."""
        self.logger.info("Creating fallback robots.txt parser with known rules")

        # Known disallowed paths from robots.txt
        disallowed_paths = [
            "/registration",
            "/login",
            "/images",
            "/admin",
            "/content",
            "/common",
            "/charts_xml",
            "/index.php",
            "/unsubscribe",
            "/toolbar",
            "/webmaster-tools/verify",
            "/upload_images",
            "/jp.php",
            "/charts/advinion.php",
            "/signup",
            "/studios/financialounge",
            "/members",
            "/mobile/mobile-redirect",
            "/mobile/footer-redirect",
            "/research/",
            "/brokers2/",
            "/cdn-cgi/",
            "/brokers/house/guest/",
            "/warrenai/conversation/",
        ]

        # Manual check: is economic-calendar in disallowed paths?
        economic_calendar_path = "/economic-calendar/"
        is_disallowed = any(
            economic_calendar_path.startswith(path.rstrip("/") + "/")
            or economic_calendar_path == path.rstrip("/")
            for path in disallowed_paths
        )

        if is_disallowed:
            self.logger.error(
                f"Economic calendar path {economic_calendar_path} is in disallowed list"
            )
        else:
            self.logger.info(
                f"Economic calendar path {economic_calendar_path} is NOT in disallowed list"
            )

        # Store this information for later use
        self._fallback_calendar_allowed = not is_disallowed
        self._fallback_robots_loaded = True

    def _get_random_delay(self) -> float:
        """Get random delay between min_delay and max_delay."""
        return random.uniform(self.min_delay, self.max_delay)

    def _is_calendar_access_allowed(self) -> bool:
        """Check if economic calendar access is allowed by robots.txt."""
        calendar_url = f"{self.base_url}/economic-calendar/"
        parsed_url = urlparse(calendar_url)
        path_only = parsed_url.path

        # Try the robots parser first
        if self.robots_parser.entries:
            allowed = self.robots_parser.can_fetch("*", path_only)
        else:
            # Use fallback logic if robots.txt couldn't be loaded
            if hasattr(self, "_fallback_robots_loaded") and self._fallback_robots_loaded:
                allowed = self._fallback_calendar_allowed
                self.logger.info("Using fallback robots.txt analysis")
            else:
                # If no fallback available, conservatively deny access
                self.logger.warning(
                    "No robots.txt information available, conservatively denying access"
                )
                allowed = False

        if allowed:
            self.logger.info(f"* Economic calendar access allowed by robots.txt: {path_only}")
        else:
            self.logger.error(f"* Economic calendar access BLOCKED by robots.txt: {path_only}")

        return allowed

    def _get_random_user_agent(self) -> str:
        """Get random user agent from the list."""
        return random.choice(self.user_agents)

    def _make_request_with_retry(
        self, url: str, params: dict | None = None
    ) -> requests.Response | None:
        """
        Make HTTP request with retry logic and exponential backoff.

        Args:
            url: URL to request
            params: Query parameters

        Returns:
            Response object or None if all retries failed
        """
        for attempt in range(self.max_retries):
            try:
                # Check robots.txt compliance
                parsed_url = urlparse(url)
                path_only = parsed_url.path

                # Check robots.txt compliance
                if self.robots_parser.entries:
                    allowed = self.robots_parser.can_fetch("*", path_only)
                else:
                    # Use fallback logic if robots.txt couldn't be loaded
                    if hasattr(self, "_fallback_robots_loaded") and self._fallback_robots_loaded:
                        # For calendar URLs, use the pre-computed result
                        if "/economic-calendar/" in path_only:
                            allowed = self._fallback_calendar_allowed
                        else:
                            # For other URLs, conservatively deny
                            allowed = False
                    else:
                        # If no fallback available, conservatively deny access
                        self.logger.warning(
                            "No robots.txt information available, conservatively denying access"
                        )
                        allowed = False

                if allowed:
                    # Add delay
                    delay = self._get_random_delay()
                    time.sleep(delay)

                    # Update headers with random user agent
                    headers = self.headers.copy()
                    headers["User-Agent"] = self._get_random_user_agent()

                    response = self.session.get(
                        url, params=params, headers=headers, timeout=self.timeout
                    )
                    response.raise_for_status()
                    return response
                else:
                    self.logger.warning(f"Request to {path_only} BLOCKED by robots.txt")
                    self.logger.info("Respecting robots.txt and skipping request")
                    return None

            except requests.exceptions.RequestException as e:
                wait_time = (2**attempt) + random.uniform(0, 1)
                self.logger.warning(
                    f"Attempt {attempt + 1} failed for {url}: {e}. "
                    f"Retrying in {wait_time:.2f} seconds..."
                )
                time.sleep(wait_time)

        self.logger.error(f"All {self.max_retries} attempts failed for {url}")
        return None

    def _parse_impact_level(self, element) -> str:
        """Parse impact level from HTML element."""
        if not element:
            return "Unknown"

        # Check for impact indicators (usually color-coded dots or text)
        class_str = " ".join(element.get("class", []))

        for impact in ["high", "medium", "low"]:
            if impact.lower() in class_str.lower():
                return impact.capitalize()

        # Look for text indicators
        text = element.get_text(strip=True).lower()
        for impact in ["high", "medium", "low"]:
            if impact in text:
                return impact.capitalize()

        return "Unknown"

    def _clean_numeric_value(self, value: str) -> str | None:
        """
        Clean numeric values by removing extra characters and handling special cases.

        Args:
            value: Raw string value

        Returns:
            Cleaned string or None if value is empty/invalid
        """
        if not value or value.strip() in ["", "-", "N/A", "NA"]:
            return None

        cleaned = value.strip()

        # Handle common patterns
        if "%" in cleaned:
            return cleaned
        if "K" in cleaned.upper():
            return cleaned
        if "M" in cleaned.upper():
            return cleaned
        if "B" in cleaned.upper():
            return cleaned

        return cleaned

    def _parse_calendar_row(self, row) -> dict[str, Any] | None:
        """
        Parse a single calendar row from the HTML table.

        Args:
            row: BeautifulSoup tr element

        Returns:
            Dictionary with parsed event data or None if parsing failed
        """
        try:
            cells = row.find_all("td")
            if len(cells) < 7:  # Minimum expected columns
                return None

            # Extract data based on typical Investing.com table structure
            event_data = {}

            # Date (usually first column)
            date_cell = cells[0].get_text(strip=True)
            event_data["date"] = date_cell if date_cell else None

            # Time (second column)
            time_cell = cells[1].get_text(strip=True)
            event_data["time"] = time_cell if time_cell else None

            # Country (third column - often contains flag image)
            country_cell = cells[2]
            country_img = country_cell.find("img")
            if country_img:
                country_title = country_img.get("title", "").strip()
                event_data["country"] = country_title if country_title else None
            else:
                event_data["country"] = country_cell.get_text(strip=True) or None

            # Event name (fourth column)
            event_cell = cells[3]
            event_link = event_cell.find("a")
            if event_link:
                event_data["event"] = event_link.get_text(strip=True)
                event_data["event_url"] = urljoin(self.base_url, event_link.get("href", ""))
            else:
                event_data["event"] = event_cell.get_text(strip=True)
                event_data["event_url"] = None

            # Impact level (fifth column)
            impact_cell = cells[4] if len(cells) > 4 else None
            event_data["impact"] = (
                self._parse_impact_level(impact_cell) if impact_cell else "Unknown"
            )

            # Actual value (sixth column)
            actual_cell = cells[5] if len(cells) > 5 else None
            actual_text = actual_cell.get_text(strip=True) if actual_cell else ""
            event_data["actual"] = self._clean_numeric_value(actual_text)

            # Forecast value (seventh column)
            forecast_cell = cells[6] if len(cells) > 6 else None
            forecast_text = forecast_cell.get_text(strip=True) if forecast_cell else ""
            event_data["forecast"] = self._clean_numeric_value(forecast_text)

            # Previous value (eighth column - if available)
            previous_cell = cells[7] if len(cells) > 7 else None
            previous_text = previous_cell.get_text(strip=True) if previous_cell else ""
            event_data["previous"] = self._clean_numeric_value(previous_text)

            # Add timestamp
            event_data["scraped_at"] = datetime.utcnow().isoformat()

            return event_data

        except Exception as e:
            self.logger.error(f"Error parsing calendar row: {e}")
            return None

    def _fetch_calendar_data(
        self, date_str: str | None = None, country: str = ""
    ) -> list[dict[str, Any]]:
        """
        Fetch economic calendar data for a specific date and country.

        Args:
            date_str: Date string in YYYY-MM-DD format (None for today)
            country: Country filter (empty for all countries)

        Returns:
            List of economic event dictionaries
        """
        # Construct calendar URL
        calendar_url = f"{self.base_url}/economic-calendar/"

        # Prepare parameters
        params = {}
        if date_str:
            params["date"] = date_str
        if country:
            params["country"] = country.lower()

        # Fetch the page
        response = self._make_request_with_retry(calendar_url, params)
        if not response:
            return []

        try:
            soup = BeautifulSoup(response.content, "html.parser")

            # Find the calendar table (typical selectors)
            calendar_table = (
                soup.find("table", {"class": "genTbl"})
                or soup.find("table", {"id": "economicCalendarData"})
                or soup.find("table", {"class": "economicCalendarTable"})
            )

            if not calendar_table:
                self.logger.error("Calendar table not found on the page")
                return []

            # Parse table rows
            events = []
            rows = calendar_table.find_all("tr")

            for row in rows:
                # Skip header row and empty rows
                if row.get("class") and any("header" in str(c).lower() for c in row.get("class")):
                    continue

                event_data = self._parse_calendar_row(row)
                if event_data:
                    events.append(event_data)

            self.logger.info(f"Successfully parsed {len(events)} events")
            return events

        except Exception as e:
            self.logger.error(f"Error parsing calendar data: {e}")
            return []

    def collect_events(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        countries: list[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Collect economic events for a date range and countries.

        Args:
            start_date: Start date in YYYY-MM-DD format (None for today)
            end_date: End date in YYYY-MM-DD format (None for today only)
            countries: List of country codes/filters (None for all)

        Returns:
            List of all collected economic events
        """
        if countries is None:
            countries = ["us", "eu", "uk"]  # Default to major economies

        # Check robots.txt compliance before starting collection
        if not self._is_calendar_access_allowed():
            self.logger.error("Cannot proceed: Economic calendar access is blocked by robots.txt")
            return []

        all_events = []

        # Parse date range
        if start_date is None:
            start_date = datetime.now().strftime("%Y-%m-%d")
        if end_date is None:
            end_date = start_date

        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            self.logger.error("Invalid date format. Use YYYY-MM-DD")
            return []

        # Collect events for each date in the range
        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime("%Y-%m-%d")
            self.logger.info(f"Collecting events for {date_str}")

            for country in countries:
                self.logger.info(f"Collecting events for {country.upper()}")
                events = self._fetch_calendar_data(date_str, country)
                all_events.extend(events)

            current_date += timedelta(days=1)

        self.logger.info(f"Total events collected: {len(all_events)}")
        return all_events

    def save_to_csv(
        self, events: list[dict[str, Any]], filename: str = "economic_events.csv"
    ) -> bool:
        """
        Save collected events to CSV file.

        Args:
            events: List of event dictionaries
            filename: Output CSV filename

        Returns:
            True if successful, False otherwise
        """
        if not events:
            self.logger.warning("No events to save")
            return False

        try:
            # Define CSV columns in desired order
            fieldnames = [
                "date",
                "time",
                "country",
                "event",
                "impact",
                "actual",
                "forecast",
                "previous",
                "event_url",
                "scraped_at",
            ]

            with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for event in events:
                    # Ensure all fields are present in the correct order
                    row = {field: event.get(field, "") for field in fieldnames}
                    writer.writerow(row)

            self.logger.info(f"Successfully saved {len(events)} events to {filename}")
            return True

        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            return False

    def get_events_dataframe(self, events: list[dict[str, Any]]) -> pd.DataFrame | None:
        """
        Convert events list to pandas DataFrame.

        Args:
            events: List of event dictionaries

        Returns:
            DataFrame with events or None if conversion failed
        """
        if not events:
            return None

        try:
            df = pd.DataFrame(events)

            # Convert date columns to datetime if present
            date_columns = ["date"]
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            return df

        except Exception as e:
            self.logger.error(f"Error converting to DataFrame: {e}")
            return None


def main():
    """Example usage of the EconomicCalendarCollector."""
    collector = EconomicCalendarCollector()

    # Collect today's events for major economies
    events = collector.collect_events(
        start_date=None, end_date=None, countries=["us", "eu", "uk"]  # Today  # Today only
    )

    # Save to CSV
    if events:
        collector.save_to_csv(events)
        print(f"Collected {len(events)} economic events")

        # Display first few events
        df = collector.get_events_dataframe(events)
        if df is not None:
            print("\nFirst 5 events:")
            print(df[["date", "time", "country", "event", "impact", "actual", "forecast"]].head())
    else:
        print("No events found")


if __name__ == "__main__":
    main()
