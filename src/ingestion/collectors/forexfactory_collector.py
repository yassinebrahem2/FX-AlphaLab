"""Forex Factory Calendar Collector - Bronze Layer (Raw Data Collection).

Web scraper for Forex Factory's economic calendar data.
https://www.forexfactory.com/calendar

Features:
- Scrapes economic events with impact levels, forecast, actual, and previous values
- Respects robots.txt crawl rules
- Conservative rate limiting (3-5 seconds between requests)
- User-agent rotation to appear as legitimate browser traffic
- Comprehensive error handling and logging
- Exports raw scraped data to CSV in Bronze layer format

Rate Limiting Strategy:
- Start conservatively: 3-5 second delays between requests
- Implement exponential backoff on 429 or 503 responses
- Do NOT retry aggressively - Forex Factory may have IP-based limits
- Request jitter (randomize delay within range)

Data Fields Captured:
- Date/Time (in original timezone)
- Currency/Country
- Event Name
- Impact Level (High/Medium/Low)
- Forecast Value
- Previous Value
- Actual Value (for historical data)

CSV Output Schema (Bronze layer):
timestamp,currency,event,impact,forecast,previous,actual,source_url,scraped_at

Example:
    >>> from pathlib import Path
    >>> from datetime import datetime
    >>> from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector
    >>>
    >>> collector = ForexFactoryCalendarCollector()
    >>> # Collect raw data
    >>> data = collector.collect(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 31))
    >>> # Export to Bronze layer
    >>> for dataset_name, df in data.items():
    >>>     path = collector.export_csv(df, dataset_name)
"""

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
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

# Use undetected_chromedriver to bypass Cloudflare
try:
    import undetected_chromedriver as uc

    HAS_UNDETECTED_CHROMEDRIVER = True
except ImportError:
    HAS_UNDETECTED_CHROMEDRIVER = False
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

try:
    from webdriver_manager.chrome import ChromeDriverManager

    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

from src.ingestion.collectors.base_collector import BaseCollector
from src.shared.config import Config


class ForexFactoryCalendarCollector(BaseCollector):
    """
    Web scraper for Forex Factory economic calendar data.

    Features:
    - Scrapes economic events with impact, forecast, actual, and previous values
    - Captures data for all major currencies (USD, EUR, GBP, JPY, CHF, etc.)
    - Respects robots.txt with configurable delays
    - User-agent rotation and request headers
    - Error handling with retry logic and exponential backoff
    - CSV output functionality following Bronze layer standards

    Rate Limiting:
        - Minimum 3-5 seconds between requests
        - Exponential backoff on rate limit (429) or service unavailable (503)
        - Random jitter to avoid predictable patterns
        - No aggressive retries to prevent IP bans

    Note:
        This collector handles ONLY Bronze layer (raw collection).
        For Silver layer (UTC timestamps, schema normalization), use CalendarPreprocessor.
    """

    SOURCE_NAME = "forexfactory"
    BASE_URL = "https://www.forexfactory.com"
    CALENDAR_URL = "https://www.forexfactory.com/calendar"

    # Rate limiting settings (conservative to avoid IP bans)
    DEFAULT_MIN_DELAY = 3.0
    DEFAULT_MAX_DELAY = 5.0
    DEFAULT_MAX_RETRIES = 2  # Low retries to avoid escalation
    DEFAULT_TIMEOUT = 30
    DEFAULT_BACKOFF_MULTIPLIER = 2.0

    def __init__(
        self,
        base_url: str = BASE_URL,
        min_delay: float = DEFAULT_MIN_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: int = DEFAULT_TIMEOUT,
        output_dir: Path | None = None,
        log_file: Path | None = None,
        headless: bool = False,
    ):
        """
        Initialize the Forex Factory Calendar Collector.

        Args:
            base_url: Base URL for Forex Factory
            min_delay: Minimum delay between requests (seconds)
            max_delay: Maximum delay between requests (seconds)
            max_retries: Maximum number of retry attempts (kept low to avoid bans)
            timeout: Request timeout in seconds
            output_dir: Output directory for raw CSVs (default: data/raw/forexfactory/)
            log_file: Optional log file path
            headless: Run browser in headless mode (default: False for better Cloudflare bypass)
        """
        # Initialize BaseCollector first (sets up self.logger)
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "forexfactory",
            log_file=log_file or Config.LOGS_DIR / "collectors" / "forexfactory_collector.log",
        )

        # Forex Factory specific settings
        self.base_url = base_url
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.timeout = timeout

        # User agents for rotation (common browsers)
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]

        # Request headers to appear as legitimate browser
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

        # Session for connection pooling (fallback)
        self.session = requests.Session()

        # Selenium WebDriver (lazy initialization)
        self._driver = None
        # Headless mode is disabled by default for better Cloudflare bypass
        # Set to True for CI/server environments (may not work on Cloudflare-protected sites)
        self._headless = headless

        # Robots.txt parser
        self.robots_parser = RobotFileParser()
        robots_url = urljoin(self.base_url, "/robots.txt")
        self.robots_parser.set_url(robots_url)
        self._load_robots_txt()

        # Track last request time for rate limiting
        self._last_request_time: float = 0.0

        self.logger.info(
            "ForexFactoryCalendarCollector initialized (delay: %.1f-%.1fs, retries: %d, selenium: enabled)",
            self.min_delay,
            self.max_delay,
            self.max_retries,
        )

    def _init_driver(self):
        """
        Initialize Selenium WebDriver with anti-detection options.
        Uses undetected_chromedriver to bypass Cloudflare protection.

        Returns:
            Configured Chrome WebDriver instance
        """
        if self._driver is not None:
            return self._driver

        self.logger.info("Initializing Selenium WebDriver (undetected mode)...")

        try:
            if HAS_UNDETECTED_CHROMEDRIVER:
                # Use undetected_chromedriver for Cloudflare bypass
                options = uc.ChromeOptions()

                # Headless mode (use new headless mode)
                if self._headless:
                    options.add_argument("--headless=new")

                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
                options.add_argument("--window-size=1920,1080")

                # Explicitly specify Chrome version to download matching driver
                # Get Chrome version dynamically
                import subprocess

                try:
                    # Try to get Chrome version from registry on Windows
                    result = subprocess.run(
                        [
                            "reg",
                            "query",
                            r"HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon",
                            "/v",
                            "version",
                        ],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode == 0:
                        version_line = [
                            line for line in result.stdout.split("\n") if "version" in line.lower()
                        ]
                        if version_line:
                            chrome_version = int(version_line[0].split()[-1].split(".")[0])
                            self.logger.info(f"Detected Chrome version: {chrome_version}")
                        else:
                            chrome_version = None
                    else:
                        chrome_version = None
                except Exception:
                    chrome_version = None

                if chrome_version:
                    self._driver = uc.Chrome(options=options, version_main=chrome_version)
                else:
                    self._driver = uc.Chrome(options=options)

                self._driver.set_page_load_timeout(self.timeout)
                self.logger.info("Undetected ChromeDriver initialized successfully")
            else:
                # Fallback to regular Selenium
                self.logger.warning("undetected_chromedriver not available, using regular Selenium")
                chrome_options = Options()

                if self._headless:
                    chrome_options.add_argument("--headless=new")

                chrome_options.add_argument("--disable-blink-features=AutomationControlled")
                chrome_options.add_argument("--disable-extensions")
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--window-size=1920,1080")
                chrome_options.add_argument(f"--user-agent={self._get_random_user_agent()}")

                chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                chrome_options.add_experimental_option("useAutomationExtension", False)

                prefs = {
                    "profile.default_content_setting_values.notifications": 2,
                    "credentials_enable_service": False,
                    "profile.password_manager_enabled": False,
                }
                chrome_options.add_experimental_option("prefs", prefs)

                if HAS_WEBDRIVER_MANAGER:
                    service = Service(ChromeDriverManager().install())
                    self._driver = webdriver.Chrome(service=service, options=chrome_options)
                else:
                    self._driver = webdriver.Chrome(options=chrome_options)

                self._driver.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {"source": """
                            Object.defineProperty(navigator, 'webdriver', {
                                get: () => undefined
                            });
                        """},
                )

                self._driver.set_page_load_timeout(self.timeout)
                self.logger.info("Selenium WebDriver initialized successfully")

            return self._driver

        except Exception as e:
            self.logger.error(f"Failed to initialize WebDriver: {e}")
            raise

    def close(self) -> None:
        """Close the Selenium WebDriver and clean up resources."""
        if self._driver is not None:
            try:
                self._driver.quit()
                self.logger.info("WebDriver closed successfully")
            except Exception as e:
                self.logger.warning(f"Error closing WebDriver: {e}")
            finally:
                self._driver = None

    def __del__(self):
        """Destructor to ensure WebDriver is closed."""
        self.close()

    def _load_robots_txt(self) -> None:
        """Load and parse robots.txt file."""
        try:
            # Try the simple read first
            self.robots_parser.read()

            # If no entries were parsed, try manual fetching
            if not self.robots_parser.entries:
                self.logger.warning("Standard robots.txt parsing failed, trying manual fetch...")
                robots_url = self.robots_parser.url

                # Fetch robots.txt manually using requests
                response = requests.get(
                    robots_url, timeout=10, headers={"User-Agent": self.user_agents[0]}
                )
                response.raise_for_status()

                # Parse manually
                from io import StringIO

                robots_content = response.text
                self.robots_parser.parse(StringIO(robots_content).readlines())
                self.robots_parser.url = robots_url

            if self.robots_parser.entries:
                self.logger.info(f"Successfully loaded robots.txt from {self.robots_parser.url}")
                self.logger.info(f"Found {len(self.robots_parser.entries)} rule entries")
            else:
                self.logger.info("No robots.txt restrictions found - calendar access allowed")

        except Exception as e:
            self.logger.warning(f"Failed to load robots.txt: {e}. Proceeding with caution.")
            # Forex Factory has minimal robots.txt restrictions, so we can proceed

    def _get_random_delay(self) -> float:
        """Get random delay between min_delay and max_delay with jitter."""
        return random.uniform(self.min_delay, self.max_delay)

    def _apply_rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.min_delay:
            sleep_time = self.min_delay - elapsed + random.uniform(0, 1)
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def _get_random_user_agent(self) -> str:
        """Get random user agent from the list."""
        return random.choice(self.user_agents)

    def _is_calendar_access_allowed(self) -> bool:
        """Check if calendar access is allowed by robots.txt."""
        calendar_url = f"{self.base_url}/calendar"
        parsed_url = urlparse(calendar_url)
        path_only = parsed_url.path

        # Try the robots parser
        if self.robots_parser.entries:
            allowed = self.robots_parser.can_fetch("*", path_only)
        else:
            # If no robots.txt loaded, check known restrictions
            # Forex Factory generally allows calendar access
            allowed = True
            self.logger.info("No robots.txt restrictions - assuming calendar access is allowed")

        if allowed:
            self.logger.info(f"Calendar access allowed: {path_only}")
        else:
            self.logger.error(f"Calendar access BLOCKED by robots.txt: {path_only}")

        return allowed

    def _fetch_page_with_selenium(self, url: str) -> str | None:
        """
        Fetch a page using Selenium WebDriver.
        Handles Cloudflare challenge by waiting for it to complete.

        Args:
            url: URL to fetch

        Returns:
            Page HTML content or None if failed
        """
        for attempt in range(self.max_retries + 1):
            try:
                # Apply rate limiting
                self._apply_rate_limit()

                driver = self._init_driver()
                self.logger.info(f"Fetching {url} with Selenium (attempt {attempt + 1})")

                driver.get(url)

                # Wait for Cloudflare challenge to complete
                # Check for "Just a moment" and wait for it to disappear
                max_cloudflare_wait = 30  # seconds
                start_time = time.time()
                while time.time() - start_time < max_cloudflare_wait:
                    page_title = driver.title
                    if "just a moment" not in page_title.lower():
                        self.logger.info(f"Cloudflare challenge passed (title: {page_title[:50]})")
                        break
                    self.logger.debug("Waiting for Cloudflare challenge to complete...")
                    time.sleep(2)
                else:
                    self.logger.warning("Cloudflare challenge did not complete in time")

                # Wait for the calendar table to be present
                try:
                    WebDriverWait(driver, 20).until(
                        ec.presence_of_element_located(
                            (
                                By.CSS_SELECTOR,
                                "table.calendar__table, table#calendar_table, .calendar, tr.calendar__row",
                            )
                        )
                    )
                    self.logger.info("Calendar element found")
                except TimeoutException:
                    self.logger.warning(
                        "Calendar table not found within timeout, checking page content..."
                    )

                # Additional wait for dynamic content
                time.sleep(3)

                # Get page source
                page_source = driver.page_source

                # Check for Cloudflare challenge page
                if "Just a moment" in page_source or "checking your browser" in page_source.lower():
                    self.logger.warning(
                        f"Still on Cloudflare challenge page on attempt {attempt + 1}"
                    )
                    if attempt < self.max_retries:
                        wait_time = (self.DEFAULT_BACKOFF_MULTIPLIER**attempt) + random.uniform(
                            5, 10
                        )
                        self.logger.info(f"Waiting {wait_time:.1f}s before retry...")
                        # Close driver and reinitialize to get fresh session
                        self.close()
                        time.sleep(wait_time)
                        continue
                    return None

                if "Access Denied" in page_source or "blocked" in page_source.lower():
                    self.logger.warning(f"Access denied on attempt {attempt + 1}")
                    if attempt < self.max_retries:
                        wait_time = (self.DEFAULT_BACKOFF_MULTIPLIER**attempt) + random.uniform(
                            2, 5
                        )
                        self.logger.info(f"Waiting {wait_time:.1f}s before retry...")
                        time.sleep(wait_time)
                        continue
                    return None

                self.logger.info(f"Successfully fetched page ({len(page_source)} bytes)")
                return page_source

            except TimeoutException as e:
                self.logger.warning(f"Page load timeout (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries:
                    wait_time = (self.DEFAULT_BACKOFF_MULTIPLIER**attempt) + random.uniform(1, 3)
                    time.sleep(wait_time)
                else:
                    self.logger.error("All attempts failed due to timeout")
                    return None

            except WebDriverException as e:
                self.logger.error(f"WebDriver error (attempt {attempt + 1}): {e}")
                # Reset driver on error
                self.close()
                if attempt < self.max_retries:
                    wait_time = (self.DEFAULT_BACKOFF_MULTIPLIER**attempt) + random.uniform(1, 3)
                    time.sleep(wait_time)
                else:
                    return None

            except Exception as e:
                self.logger.error(f"Unexpected error fetching page: {e}")
                if attempt < self.max_retries:
                    wait_time = (self.DEFAULT_BACKOFF_MULTIPLIER**attempt) + random.uniform(1, 3)
                    time.sleep(wait_time)
                else:
                    return None

        return None

    def _make_request(self, url: str, params: dict | None = None) -> requests.Response | None:
        """
        Make HTTP request with rate limiting and retry logic.
        Falls back to requests library (kept for compatibility).

        Args:
            url: URL to request
            params: Query parameters

        Returns:
            Response object or None if all retries failed
        """
        for attempt in range(self.max_retries + 1):
            try:
                # Check robots.txt compliance
                parsed_url = urlparse(url)
                path_only = parsed_url.path

                if self.robots_parser.entries:
                    allowed = self.robots_parser.can_fetch("*", path_only)
                else:
                    allowed = True  # No restrictions known

                if not allowed:
                    self.logger.error(f"Request to {path_only} BLOCKED by robots.txt")
                    return None

                # Apply rate limiting
                self._apply_rate_limit()

                # Update headers with random user agent
                headers = self.headers.copy()
                headers["User-Agent"] = self._get_random_user_agent()

                response = self.session.get(
                    url, params=params, headers=headers, timeout=self.timeout
                )

                # Handle rate limiting (429)
                if response.status_code == 429:
                    self.logger.warning(
                        f"Rate limited (429) on attempt {attempt + 1}. "
                        "Exiting gracefully to avoid IP ban."
                    )
                    return None

                # Handle service unavailable (503)
                if response.status_code == 503:
                    if attempt < self.max_retries:
                        wait_time = (self.DEFAULT_BACKOFF_MULTIPLIER**attempt) + random.uniform(
                            0, 2
                        )
                        self.logger.warning(
                            f"Service unavailable (503) on attempt {attempt + 1}. "
                            f"Waiting {wait_time:.1f}s before retry..."
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error("Service unavailable after all retries")
                        return None

                response.raise_for_status()
                return response

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    self.logger.warning("Rate limited (429). Exiting gracefully to avoid ban.")
                    return None
                self.logger.error(f"HTTP error: {e}")
                if attempt < self.max_retries:
                    wait_time = (self.DEFAULT_BACKOFF_MULTIPLIER**attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
                else:
                    return None

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries:
                    wait_time = (self.DEFAULT_BACKOFF_MULTIPLIER**attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All {self.max_retries + 1} attempts failed for {url}")
                    return None

        return None

    def _parse_impact_level(self, impact_cell) -> str:
        """
        Parse impact level from HTML cell.

        Forex Factory uses icon classes:
        - high-impact = High impact
        - medium-impact = Medium impact
        - low-impact = Low impact
        """
        if not impact_cell:
            return "Unknown"

        # Check for impact classes
        class_str = " ".join(impact_cell.get("class", []))

        if "high" in class_str.lower():
            return "High"
        elif "medium" in class_str.lower() or "moderate" in class_str.lower():
            return "Medium"
        elif "low" in class_str.lower():
            return "Low"

        # Check for icon/span titles
        icon = impact_cell.find(["span", "i", "div"])
        if icon:
            title = icon.get("title", "").lower()
            if "high" in title:
                return "High"
            elif "medium" in title or "moderate" in title:
                return "Medium"
            elif "low" in title:
                return "Low"

        # Check text content
        text = impact_cell.get_text(strip=True).lower()
        if "high" in text:
            return "High"
        elif "medium" in text or "moderate" in text:
            return "Medium"
        elif "low" in text:
            return "Low"

        return "Unknown"

    def _clean_value(self, value: str) -> str | None:
        """
        Clean and validate scraped values.

        Args:
            value: Raw value string from scraping

        Returns:
            Cleaned value or None if invalid/empty
        """
        if not value:
            return None

        value = value.strip()

        # Check for common null indicators
        null_indicators = ["-", "n/a", "na", "", "—", "–"]
        if value.lower() in null_indicators:
            return None

        return value

    def _parse_calendar_row(self, row) -> dict[str, Any] | None:
        """
        Parse a single calendar row from the HTML table.

        Forex Factory calendar table structure:
            - Date header rows (class containing 'date')
            - Event rows with cells for time, currency, impact, event, actual, forecast, previous

        Args:
            row: BeautifulSoup tr element

        Returns:
            Dictionary with parsed event data or None if parsing failed
        """
        try:
            cells = row.find_all("td")
            if len(cells) < 6:
                return None

            # Skip header rows
            if row.get("class") and any("head" in str(c).lower() for c in row.get("class")):
                return None

            event_data = {}

            # Time (usually first cell)
            time_text = cells[0].get_text(strip=True) if len(cells) > 0 else ""
            event_data["time"] = time_text if time_text else None

            # Currency (second cell)
            currency_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            event_data["currency"] = currency_text if currency_text else None

            # Impact (third cell)
            impact_cell = cells[2] if len(cells) > 2 else None
            event_data["impact"] = self._parse_impact_level(impact_cell)

            # Event name (fourth cell)
            event_cell = cells[3] if len(cells) > 3 else None
            if event_cell:
                event_link = event_cell.find("a")
                if event_link:
                    event_data["event"] = event_link.get_text(strip=True)
                    event_data["event_url"] = urljoin(self.base_url, event_link.get("href", ""))
                else:
                    event_data["event"] = event_cell.get_text(strip=True)
                    event_data["event_url"] = None
            else:
                event_data["event"] = None
                event_data["event_url"] = None

            # Actual value (fifth cell)
            actual_text = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            event_data["actual"] = self._clean_value(actual_text)

            # Forecast value (sixth cell)
            forecast_text = cells[5].get_text(strip=True) if len(cells) > 5 else ""
            event_data["forecast"] = self._clean_value(forecast_text)

            # Previous value (seventh cell, if available)
            previous_text = cells[6].get_text(strip=True) if len(cells) > 6 else ""
            event_data["previous"] = self._clean_value(previous_text)

            # Source URL
            event_data["source_url"] = self.CALENDAR_URL

            # Scraped timestamp
            event_data["scraped_at"] = datetime.now(timezone.utc).isoformat()

            # Source column (Bronze layer requirement)
            event_data["source"] = "forexfactory.com"

            # Validate minimum required fields
            if not event_data["event"]:
                return None

            return event_data

        except Exception as e:
            self.logger.error(f"Error parsing calendar row: {e}")
            return None

    def _fetch_calendar_for_date(self, date_str: str) -> tuple[list[dict[str, Any]], str | None]:
        """
        Fetch economic calendar data for a specific date.

        Args:
            date_str: Date string in YYYY-MM-DD format

        Returns:
            Tuple of (list of event dictionaries, date string for events)
        """
        # Construct calendar URL with date parameter
        # Forex Factory uses week navigation, so we may need to fetch the week containing the date
        # Try to construct date-specific URL
        # Forex Factory uses format: /calendar?day=Feb12.2024
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            day_param = dt.strftime("%b%d.%Y")  # e.g., "Feb12.2024"
            url = f"{self.CALENDAR_URL}?day={day_param}"
        except ValueError:
            self.logger.warning(f"Invalid date format: {date_str}")
            return [], None

        # Fetch the page using Selenium
        page_content = self._fetch_page_with_selenium(url)
        if not page_content:
            return [], None

        try:
            soup = BeautifulSoup(page_content, "html.parser")

            # Find the calendar table - try multiple selectors
            # Forex Factory uses various table structures
            calendar_table = (
                soup.find("table", {"class": lambda x: x and "calendar__table" in x})
                or soup.find("table", {"id": "calendar_table"})
                or soup.find("table", {"class": lambda x: x and "calendar" in str(x).lower()})
                or soup.find("div", {"class": lambda x: x and "calendar" in str(x).lower()})
            )

            # If still not found, look for any table with calendar-related rows
            if not calendar_table:
                # Try to find by looking for rows with calendar classes
                calendar_rows = soup.find_all(
                    "tr", {"class": lambda x: x and "calendar" in str(x).lower()}
                )
                if calendar_rows:
                    calendar_table = calendar_rows[0].find_parent("table")
                    self.logger.info("Found calendar table via row detection")

            if not calendar_table:
                self.logger.error("Calendar table not found on the page")
                # Save debug HTML for analysis
                debug_file = self.output_dir / f"debug_page_{date_str}.html"
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(page_content)
                self.logger.info(f"Saved debug HTML to {debug_file}")

                # Log available classes for debugging
                all_tables = soup.find_all("table")
                self.logger.debug(f"Found {len(all_tables)} tables on page")
                for i, t in enumerate(all_tables[:5]):
                    self.logger.debug(f"Table {i}: classes={t.get('class')}, id={t.get('id')}")

                return [], None

            # Parse table rows
            events = []
            current_date = date_str

            rows = calendar_table.find_all("tr")
            self.logger.info(f"Found {len(rows)} rows in calendar table")

            for row in rows:
                row_classes = row.get("class", [])

                # Check if this is a date row
                if row_classes and any("date" in str(c).lower() for c in row_classes):
                    # Extract date from date row
                    date_span = row.find("span", {"class": lambda x: x and "date" in x.lower()})
                    if date_span:
                        date_text = date_span.get_text(strip=True)
                        # Try to parse date
                        try:
                            # Forex Factory format: "Monday, February 12, 2024"
                            parsed_date = datetime.strptime(date_text, "%A, %B %d, %Y")
                            current_date = parsed_date.strftime("%Y-%m-%d")
                        except ValueError:
                            try:
                                # Alternative format: "Feb 12"
                                parsed_date = datetime.strptime(
                                    date_text + f" {dt.year}", "%b %d %Y"
                                )
                                current_date = parsed_date.strftime("%Y-%m-%d")
                            except ValueError:
                                pass

                # Check if this is an event row
                elif row_classes and any(
                    cls in ["calendar__row", "event", "calendar_row"] for cls in row_classes
                ):
                    event_data = self._parse_calendar_row(row)
                    if event_data:
                        event_data["date"] = current_date
                        events.append(event_data)

            self.logger.info(f"Successfully parsed {len(events)} events for {date_str}")
            return events, current_date

        except Exception as e:
            self.logger.error(f"Error parsing calendar data: {e}")
            return [], None

    def _fetch_calendar_data(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """
        Fetch economic calendar data for a date range.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            List of all collected economic events
        """
        if not self._is_calendar_access_allowed():
            self.logger.error("Cannot proceed: Calendar access is blocked by robots.txt")
            return []

        all_events = []

        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            self.logger.error("Invalid date format. Use YYYY-MM-DD")
            return []

        if start_dt > end_dt:
            self.logger.error("Start date must be before or equal to end date")
            return []

        # Calculate number of days
        num_days = (end_dt - start_dt).days + 1
        self.logger.info(f"Collecting events for {num_days} day(s) from {start_date} to {end_date}")

        # Collect events for each date in the range
        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime("%Y-%m-%d")
            self.logger.info(f"Collecting events for {date_str}")

            events, _ = self._fetch_calendar_for_date(date_str)
            all_events.extend(events)

            current_date += timedelta(days=1)

        self.logger.info(f"Total events collected: {len(all_events)}")
        return all_events

    def collect_events(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Collect economic events for a date range.

        Args:
            start_date: Start date in YYYY-MM-DD format (None for today)
            end_date: End date in YYYY-MM-DD format (None for today only)

        Returns:
            List of all collected economic events
        """
        # Parse date range
        if start_date is None:
            start_date = datetime.now().strftime("%Y-%m-%d")
        if end_date is None:
            end_date = start_date

        return self._fetch_calendar_data(start_date, end_date)

    def save_to_csv(
        self,
        events: list[dict[str, Any]],
        filename: str | None = None,
    ) -> Path | None:
        """
        Save collected events to CSV file (raw Bronze format).

        Args:
            events: List of event dictionaries
            filename: Output CSV filename (default: auto-generated in output_dir)

        Returns:
            Path to saved file or None if failed
        """
        if not events:
            self.logger.warning("No events to save")
            return None

        try:
            # Use default filename if not provided
            if filename is None:
                date_str = datetime.now().strftime("%Y%m%d")
                filename = str(self.output_dir / f"forexfactory_calendar_{date_str}.csv")

            # Ensure output directory exists
            os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

            # Define CSV columns in desired order (Bronze layer)
            fieldnames = [
                "date",
                "time",
                "currency",
                "event",
                "impact",
                "actual",
                "forecast",
                "previous",
                "event_url",
                "source_url",
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
            return Path(filename)

        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            return None

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
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")

            return df

        except Exception as e:
            self.logger.error(f"Error converting to DataFrame: {e}")
            return None

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """
        Collect economic calendar events from Forex Factory.

        Implements BaseCollector.collect() interface.

        Args:
            start_date: Start of the collection window (default: today)
            end_date: End of the collection window (default: today)

        Returns:
            Dictionary with single key 'calendar' containing raw DataFrame
        """
        # Convert datetime to string format expected by collect_events
        start_str = start_date.strftime("%Y-%m-%d") if start_date else None
        end_str = end_date.strftime("%Y-%m-%d") if end_date else None

        # Collect events
        events = self.collect_events(
            start_date=start_str,
            end_date=end_str,
        )

        # Convert to DataFrame with all source fields (§3.1)
        if events:
            df = pd.DataFrame(events)
            # Ensure source column exists
            if "source" not in df.columns:
                df["source"] = "forexfactory.com"
            return {"calendar": df}
        else:
            return {}

    def health_check(self) -> bool:
        """
        Verify Forex Factory is reachable using Selenium.

        Implements BaseCollector.health_check() interface.

        Returns:
            True if the site is accessible, False otherwise
        """
        try:
            driver = self._init_driver()
            driver.get(self.base_url)

            # Wait for page to load
            time.sleep(2)

            # Check if we got a valid page
            page_title = driver.title
            is_healthy = bool(page_title) and "error" not in page_title.lower()

            if is_healthy:
                self.logger.info(
                    "Health check passed: %s (title: %s)", self.base_url, page_title[:50]
                )
            else:
                self.logger.warning(
                    "Health check failed: %s (title: %s)", self.base_url, page_title
                )
            return is_healthy
        except Exception as e:
            self.logger.error("Health check failed: %s", e)
            return False

    def validate_scraped_data(self, events: list[dict[str, Any]]) -> tuple[bool, list[str]]:
        """
        Validate scraped data for quality issues.

        Args:
            events: List of event dictionaries

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        if not events:
            errors.append("No events collected")
            return False, errors

        required_fields = ["date", "currency", "event", "impact"]

        for i, event in enumerate(events):
            # Check for empty rows
            if not any(event.values()):
                errors.append(f"Row {i}: Empty event data")
                continue

            # Check required fields
            for field in required_fields:
                if not event.get(field):
                    errors.append(f"Row {i}: Missing required field '{field}'")

            # Validate date format
            date_str = event.get("date")
            if date_str:
                try:
                    datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    errors.append(f"Row {i}: Invalid date format '{date_str}'")

        is_valid = len(errors) == 0
        return is_valid, errors
