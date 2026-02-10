import csv
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.ingestion.collectors.base_collector import BaseCollector
from src.shared.config import Config


class EconomicCalendarCollector(BaseCollector):
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

    SOURCE_NAME = "investing"
    BASE_URL = "https://www.investing.com"
    DEFAULT_MIN_DELAY = 3.0
    DEFAULT_MAX_DELAY = 5.0
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_TIMEOUT = 30

    def __init__(
        self,
        base_url: str = BASE_URL,
        min_delay: float = DEFAULT_MIN_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: int = DEFAULT_TIMEOUT,
        output_dir: Path | None = None,
        log_file: Path | None = None,
    ):
        """
        Initialize the Economic Calendar Collector.

        Args:
            base_url: Base URL for Investing.com
            min_delay: Minimum delay between requests (seconds)
            max_delay: Maximum delay between requests (seconds)
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
            output_dir: Output directory for raw CSVs (default: data/raw/calendar/)
            log_file: Optional log file path
        """
        # Initialize BaseCollector first (sets up self.logger)
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "calendar",
            log_file=log_file or Config.LOGS_DIR / "calendar_collector.log",
        )

        # Investing.com specific settings
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

        # Session for connection pooling
        self.session = requests.Session()

        # Robots.txt parser (logger already set up by BaseCollector)
        self.robots_parser = RobotFileParser()
        robots_url = urljoin(self.base_url, "/robots.txt")
        self.robots_parser.set_url(robots_url)
        self._load_robots_txt()

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
        """Parse impact level from HTML element.

        Investing.com uses 3 SVG star icons with opacity classes:
        - opacity-60 = filled/active star
        - opacity-20 = empty/inactive star
        Impact: 3 filled=High, 2 filled=Medium, 1 filled=Low
        """
        if not element:
            return "Unknown"

        # Modern layout: count filled SVG stars (opacity-60)
        svgs = element.find_all("svg")
        if svgs:
            filled = sum(1 for svg in svgs if "opacity-60" in " ".join(svg.get("class", [])))
            if filled >= 3:
                return "High"
            elif filled == 2:
                return "Medium"
            elif filled == 1:
                return "Low"

        # Fallback: check CSS classes for legacy layout
        class_str = " ".join(element.get("class", []))
        for impact in ["high", "medium", "low"]:
            if impact.lower() in class_str.lower():
                return impact.capitalize()

        # Fallback: check text content
        text = element.get_text(strip=True).lower()
        for impact in ["high", "medium", "low"]:
            if impact in text:
                return impact.capitalize()

        return "Unknown"

    def _parse_calendar_row(self, row) -> dict[str, Any] | None:
        """
        Parse a single calendar row from the HTML table.

        Modern Investing.com Tailwind CSS datatable-v2 layout (9 cells):
            Cell[0]: Currency + flag (mobile)   — flag span title=Country
            Cell[1]: Time (usually empty in SSR, JS-rendered)
            Cell[2]: Currency + flag (desktop)
            Cell[3]: Event cell — <a> tag = event name
            Cell[4]: Impact — 3 SVG stars (opacity-60=filled, opacity-20=empty)
            Cell[5]: Actual value (plain text)
            Cell[6]: Forecast (plain text)
            Cell[7]: Previous (plain text)
            Cell[8]: Expand button

        Args:
            row: BeautifulSoup tr element

        Returns:
            Dictionary with parsed event data or None if parsing failed
        """
        try:
            cells = row.find_all("td")
            if len(cells) < 8:
                return None

            event_data = {}

            # Cell[3]: Event name from <a> tag
            event_link = cells[3].find("a")
            if not event_link:
                # Fallback: search entire row
                event_link = row.find("a")
            if not event_link:
                return None
            event_data["event"] = event_link.get_text(strip=True)
            event_data["event_url"] = urljoin(self.base_url, event_link.get("href", ""))

            # Cell[0]: Country from flag span title
            country = None
            for span in cells[0].find_all("span"):
                css_classes = span.get("class", [])
                if any("flag_flag--" in c for c in css_classes):
                    country = span.get("title", "").strip()
                    break
            if not country:
                # Fallback: try row ID (format: eventId-subId-Country-index)
                row_id = row.get("id", "")
                parts = row_id.split("-")
                if len(parts) >= 3:
                    country = parts[2]
            if not country:
                # Last fallback: currency text in Cell[0]
                country = cells[0].get_text(strip=True) or None
            event_data["country"] = country

            # Cell[1]: Time (usually empty in SSR — JS-rendered)
            time_text = cells[1].get_text(strip=True)
            event_data["time"] = time_text if time_text else None

            # Cell[4]: Impact from SVG star opacity
            event_data["impact"] = self._parse_impact_level(cells[4])

            # Cell[5]: Actual | Cell[6]: Forecast | Cell[7]: Previous (keep raw strings)
            actual_text = cells[5].get_text(strip=True)
            forecast_text = cells[6].get_text(strip=True)
            previous_text = cells[7].get_text(strip=True)

            event_data["actual"] = (
                actual_text if actual_text and actual_text not in ["-", "N/A", "NA", ""] else None
            )
            event_data["forecast"] = (
                forecast_text
                if forecast_text and forecast_text not in ["-", "N/A", "NA", ""]
                else None
            )
            event_data["previous"] = (
                previous_text
                if previous_text and previous_text not in ["-", "N/A", "NA", ""]
                else None
            )

            # Date: use today's date (time is JS-rendered, not in SSR HTML)
            event_data["date"] = datetime.now().strftime("%Y-%m-%d")
            event_data["scraped_at"] = datetime.now(timezone.utc).isoformat()

            # Add source column (Bronze layer requirement)
            event_data["source"] = "investing.com"

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

            # Find the calendar table (modern Tailwind CSS + legacy selectors)
            calendar_table = (
                soup.find(
                    "table",
                    {"class": lambda x: x and "datatable-v2_table" in x},
                )
                or soup.find("table", {"class": "genTbl"})
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
        self,
        events: list[dict[str, Any]],
        filename: str | None = None,
    ) -> bool:
        """
        Save collected events to CSV file (raw Bronze format).

        Args:
            events: List of event dictionaries
            filename: Output CSV filename (default: auto-generated in output_dir)

        Returns:
            True if successful, False otherwise
        """
        if not events:
            self.logger.warning("No events to save")
            return False

        try:
            # Use default filename if not provided
            if filename is None:
                date_str = datetime.now().strftime("%Y%m%d")
                filename = str(self.output_dir / f"investing_economic_events_{date_str}.csv")

            # Create output directory if needed
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            # Define CSV columns in desired order (Bronze layer - preserve all source fields)
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
                "source",
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

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Collect economic calendar events from Investing.com.

        Implements BaseCollector.collect() interface.

        Args:
            start_date: Start of the collection window (default: today)
            end_date: End of the collection window (default: today)

        Returns:
            Dictionary with single key 'economic_events' containing raw DataFrame
        """
        # Convert datetime to string format expected by collect_events
        start_str = start_date.strftime("%Y-%m-%d") if start_date else None
        end_str = end_date.strftime("%Y-%m-%d") if end_date else None

        # Collect events using existing method
        events = self.collect_events(
            start_date=start_str,
            end_date=end_str,
            countries=["us", "eu", "uk"],
        )

        # Convert to DataFrame with all source fields (§3.1)
        if events:
            df = pd.DataFrame(events)
            # Ensure source column exists
            if "source" not in df.columns:
                df["source"] = "investing.com"
            return {"economic_events": df}
        else:
            return {}

    def health_check(self) -> bool:
        """Verify Investing.com is reachable.

        Implements BaseCollector.health_check() interface.

        Returns:
            True if the site is accessible, False otherwise
        """
        try:
            response = requests.head(
                self.base_url,
                timeout=10,
                headers={"User-Agent": self._get_random_user_agent()},
            )
            is_healthy = response.status_code < 500
            if is_healthy:
                self.logger.info(
                    "Health check passed: %s (status %d)", self.base_url, response.status_code
                )
            else:
                self.logger.warning(
                    "Health check failed: %s (status %d)", self.base_url, response.status_code
                )
            return is_healthy
        except Exception as e:
            self.logger.error("Health check failed: %s", e)
            return False
