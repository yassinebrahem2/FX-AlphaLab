"""ECB Scraper Collector - Bronze Layer (Historical News Backfill).

Selenium-based scraper for collecting historical ECB press releases, speeches,
monetary policy statements, and economic bulletins from the ECB website archive.

The ECB RSS feed only provides ~10 days of recent content. This scraper navigates
the JS-rendered archive listing pages to discover article URLs, then fetches
individual articles using requests+BeautifulSoup (same as the RSS collector).

Architecture:
    - **Selenium** for archive page discovery (JS-rendered listing pages)
    - **requests + BS4** for individual article content (static HTML pages)
    - Same JSONL output contract as ECBNewsCollector
    - Deduplicates against existing Bronze data from RSS collector

robots.txt compliance:
    - Crawl-delay: 5 seconds (mandatory)
    - No press paths disallowed

Usage:
    >>> from datetime import datetime, timedelta
    >>> from src.ingestion.collectors.ecb_scraper_collector import ECBScraperCollector
    >>>
    >>> collector = ECBScraperCollector()
    >>> data = collector.collect(
    ...     start_date=datetime.now() - timedelta(days=180),
    ...     end_date=datetime.now()
    ... )
    >>> paths = collector.export_all(data)
    >>> collector.close()
"""

import json
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from src.ingestion.collectors import ecb_utils
from src.ingestion.collectors.document_collector import DocumentCollector
from src.shared.config import Config

# Selenium imports with graceful fallback
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

from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

# ---------------------------------------------------------------------------
# Archive section configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArchiveSection:
    """Configuration for an ECB archive listing page."""

    name: str
    url_path: str
    default_category: str
    link_pattern: str  # regex to match article URLs in href attributes


#: Archive sections to scrape for historical articles.
ARCHIVE_SECTIONS: list[ArchiveSection] = [
    ArchiveSection(
        name="speeches",
        url_path="/press/key/html/index.en.html",
        default_category="speeches",
        link_pattern=r"/press/key/date/\d{4}/html/",
    ),
    ArchiveSection(
        name="press_releases",
        url_path="/press/pr/html/index.en.html",
        default_category="pressreleases",
        link_pattern=r"/press/pr/date/\d{4}/html/",
    ),
    ArchiveSection(
        name="press_conferences",
        url_path="/press/press_conference/monetary-policy-statement/html/index.en.html",
        default_category="policy",
        link_pattern=r"/press/press_conference/monetary-policy-statement/\d{4}/html/",
    ),
    ArchiveSection(
        name="monetary_policy_accounts",
        url_path="/press/accounts/html/index.en.html",
        default_category="policy",
        link_pattern=r"/press/accounts/\d{4}/html/",
    ),
]

#: Regex to extract date from ECB article URL filenames.
#: Matches patterns like ``ecb.sp260115~abc123.en.html`` → ``260115``.
URL_DATE_PATTERN = re.compile(r"ecb\.\w+(\d{6})~")


class ECBScraperCollector(DocumentCollector):
    """Selenium-based collector for historical ECB news - Bronze Layer.

    Navigates ECB archive listing pages (JS-rendered) to discover article URLs,
    then fetches content using requests+BS4. Produces identical JSONL output
    to :class:`ECBNewsCollector` for seamless preprocessing.

    Features:
        - Selenium with undetected-chromedriver for JS-rendered archive pages
        - Human-like scrolling to trigger lazy-loaded content
        - requests+BS4 for individual article content (no Selenium needed)
        - Deduplication against existing JSONL files from RSS collector
        - robots.txt compliant (5-second crawl delay)
        - Comprehensive error handling and logging

    Note:
        This collector is intended for historical backfill (>14 days).
        For daily/scheduled collection, use :class:`ECBNewsCollector` (RSS).
    """

    SOURCE_NAME = "ecb"
    BASE_URL = "https://www.ecb.europa.eu"

    # robots.txt: Crawl-delay: 5
    CRAWL_DELAY = 5.0
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0

    # Selenium page load timeout
    PAGE_LOAD_TIMEOUT = 30

    # Scrolling parameters (human-like behavior)
    SCROLL_PAUSE_MIN = 0.2
    SCROLL_PAUSE_MAX = 0.6
    SCROLL_DISTANCE_MIN = 1.6  # 160% of viewport
    SCROLL_DISTANCE_MAX = 2.0  # 200% of viewport
    BACKSCROLL_PROBABILITY = 0.15
    # Consecutive scrolls with no new articles before giving up
    MAX_STALE_SCROLLS = 20
    # Check article count every N scrolls (avoid expensive find_elements every scroll)
    ARTICLE_CHECK_INTERVAL = 10

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
        headless: bool = True,
    ) -> None:
        """Initialize the ECB scraper collector.

        Args:
            output_dir: Directory for Bronze JSONL exports (default: data/raw/news/ecb).
            log_file: Optional path for file-based logging.
            headless: Run Selenium in headless mode (default: True).
        """
        final_output_dir = output_dir or Config.DATA_DIR / "raw" / "news" / "ecb"
        final_log_file = log_file or Config.LOGS_DIR / "collectors" / "ecb_scraper.log"

        super().__init__(output_dir=final_output_dir, log_file=final_log_file)

        self._headless = headless
        self._driver = None
        self._session = ecb_utils.create_ecb_session(self.MAX_RETRIES, self.RETRY_BACKOFF)
        self._last_request_time: float = 0.0
        self._discovered_urls: set[str] = set()

        self.logger.info(
            "ECBScraperCollector initialized, output_dir=%s, headless=%s",
            self.output_dir,
            headless,
        )

    # ------------------------------------------------------------------
    # DocumentCollector interface
    # ------------------------------------------------------------------

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Collect historical ECB news via Selenium scraping.

        Phase 1: Selenium navigates archive pages to discover article URLs.
        Phase 2: requests+BS4 fetches individual article content.

        Args:
            start_date: Start of range (default: 6 months ago).
            end_date: End of range (default: now).

        Returns:
            Dictionary mapping document type to list of document dicts,
            identical schema to ECBNewsCollector output.

        Raises:
            ValueError: If start_date is after end_date.
        """
        start = start_date or datetime.now(timezone.utc) - timedelta(days=180)
        end = end_date or datetime.now(timezone.utc)

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        if start > end:
            raise ValueError(f"start_date ({start.date()}) must be before end_date ({end.date()})")

        self.logger.info("Scraping ECB historical news from %s to %s", start.date(), end.date())

        result: dict[str, list[dict[str, Any]]] = {
            "pressreleases": [],
            "speeches": [],
            "policy": [],
            "bulletins": [],
        }

        # Load existing URLs for deduplication
        existing_urls = self._load_existing_urls()
        self.logger.info("Loaded %d existing URLs for deduplication", len(existing_urls))

        # Phase 1: Discover article URLs from all archive sections
        all_discoveries: list[dict[str, Any]] = []
        try:
            for section in ARCHIVE_SECTIONS:
                discoveries = self._discover_article_urls(section, start, end)
                # Filter already-collected URLs
                new_discoveries = [
                    d
                    for d in discoveries
                    if d["url"] not in existing_urls and d["url"] not in self._discovered_urls
                ]
                for d in new_discoveries:
                    self._discovered_urls.add(d["url"])
                all_discoveries.extend(new_discoveries)
                self.logger.info(
                    "Section '%s': %d discovered, %d new",
                    section.name,
                    len(discoveries),
                    len(new_discoveries),
                )
        finally:
            # Close Selenium driver after discovery phase
            self.close()

        self.logger.info("Total new articles to fetch: %d", len(all_discoveries))

        # Phase 2: Fetch content for each discovered article
        for i, discovery in enumerate(all_discoveries):
            try:
                doc = self._fetch_and_build_document(discovery)
                if doc:
                    category = doc.pop("_category", "pressreleases")
                    result[category].append(doc)

                if (i + 1) % 10 == 0:
                    self.logger.info(
                        "Progress: %d/%d articles fetched", i + 1, len(all_discoveries)
                    )

            except Exception as e:
                self.logger.warning("Failed to fetch article %s: %s", discovery["url"], e)
                continue

        total = sum(len(docs) for docs in result.values())
        self.logger.info(
            "Scraping complete: %d documents collected "
            "(pressreleases: %d, speeches: %d, policy: %d, bulletins: %d)",
            total,
            len(result["pressreleases"]),
            len(result["speeches"]),
            len(result["policy"]),
            len(result["bulletins"]),
        )

        return result

    def health_check(self) -> bool:
        """Verify ECB website is reachable (no Selenium needed).

        Returns:
            True if ECB press page responds, False otherwise.
        """
        try:
            self._throttle_request()
            response = self._session.head(self.BASE_URL + "/press/html/index.en.html", timeout=10)
            return response.ok
        except Exception as e:
            self.logger.error("ECB scraper health check failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # Selenium driver management
    # ------------------------------------------------------------------

    def _init_driver(self):
        """Initialize Selenium WebDriver with anti-detection.

        Uses undetected-chromedriver first, falls back to standard Selenium.
        Follows the same pattern as ForexFactoryCalendarCollector.
        """
        if self._driver is not None:
            return self._driver

        self.logger.info("Initializing Selenium WebDriver...")

        try:
            if HAS_UNDETECTED_CHROMEDRIVER:
                try:
                    options = uc.ChromeOptions()
                    if self._headless:
                        options.add_argument("--headless=new")
                    options.add_argument("--no-sandbox")
                    options.add_argument("--disable-dev-shm-usage")
                    options.add_argument("--disable-gpu")
                    options.add_argument("--window-size=1920,1080")

                    self._driver = uc.Chrome(options=options, use_subprocess=False)
                    self._driver.set_page_load_timeout(self.PAGE_LOAD_TIMEOUT)
                    _ = self._driver.title  # Verify driver works
                    self.logger.info("Undetected ChromeDriver initialized")
                    return self._driver

                except Exception as e:
                    self.logger.warning("Undetected ChromeDriver failed: %s", e)
                    if self._driver:
                        try:
                            self._driver.quit()
                        except Exception:
                            pass
                        self._driver = None

            # Fallback to standard Selenium
            self.logger.info("Using standard Selenium WebDriver")
            chrome_options = Options()
            if self._headless:
                chrome_options.add_argument("--headless=new")

            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")

            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option("useAutomationExtension", False)

            if HAS_WEBDRIVER_MANAGER:
                service = Service(ChromeDriverManager().install())
                self._driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                self._driver = webdriver.Chrome(options=chrome_options)

            self._driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                },
            )
            self._driver.set_page_load_timeout(self.PAGE_LOAD_TIMEOUT)
            self.logger.info("Standard Selenium WebDriver initialized")
            return self._driver

        except Exception as e:
            self.logger.error("Failed to initialize WebDriver: %s", e)
            raise

    def close(self) -> None:
        """Close Selenium WebDriver and release resources."""
        if self._driver is not None:
            try:
                self._driver.quit()
                self.logger.info("WebDriver closed")
            except Exception as e:
                self.logger.warning("Error closing WebDriver: %s", e)
            finally:
                self._driver = None

    def __del__(self) -> None:
        """Ensure WebDriver cleanup on garbage collection."""
        self.close()

    # ------------------------------------------------------------------
    # Article discovery (Selenium)
    # ------------------------------------------------------------------

    def _discover_article_urls(
        self,
        section: ArchiveSection,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict[str, Any]]:
        """Discover article URLs from an archive listing page.

        Navigates to the archive section, scrolls to load all items,
        then parses the rendered HTML for article links.

        Args:
            section: Archive section configuration.
            start_date: Start of date range filter.
            end_date: End of date range filter.

        Returns:
            List of dicts with keys: url, title, date_hint, category.
        """
        driver = self._init_driver()
        url = self.BASE_URL + section.url_path

        self.logger.info("Navigating to archive: %s", url)

        try:
            driver.get(url)

            # Poll every 5s for article links matching this section's pattern
            link_re = re.compile(section.link_pattern)

            def _articles_present(_driver):
                """Scroll down and check if at least one matching article link exists."""
                _driver.execute_script("window.scrollBy(0, window.innerHeight);")
                els = _driver.find_elements(By.TAG_NAME, "a")
                return any(link_re.search(el.get_attribute("href") or "") for el in els)

            try:
                WebDriverWait(driver, self.PAGE_LOAD_TIMEOUT, poll_frequency=5).until(
                    _articles_present
                )
                self.logger.info("Section '%s': articles detected, starting scroll", section.name)
            except TimeoutException:
                self.logger.warning(
                    "Section '%s': no articles found within %ds, moving on",
                    section.name,
                    self.PAGE_LOAD_TIMEOUT,
                )
                return []

            # Scroll to trigger lazy-loaded content (date-aware early stop)
            self._scroll_to_load_all(driver, section.link_pattern, start_date)

            # Parse rendered HTML
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # Find all links matching the section's URL pattern
            candidates = []
            link_re = re.compile(section.link_pattern)

            for link in soup.find_all("a", href=link_re):
                href = link.get("href", "")
                if not href:
                    continue

                full_url = urljoin(self.BASE_URL, href)

                # Skip non-English pages
                if not full_url.endswith(".en.html"):
                    continue

                # Extract title from link text or parent
                title = link.get_text(strip=True)
                if not title:
                    parent = link.find_parent(["div", "li", "span"])
                    if parent:
                        title = parent.get_text(strip=True)

                # Extract date from URL
                date_hint = self._parse_date_from_url(full_url)

                candidates.append(
                    {
                        "url": full_url,
                        "title": title or "",
                        "date_hint": date_hint,
                        "category": section.default_category,
                    }
                )

            # Log date range found on the page before filtering
            all_dates = [c["date_hint"] for c in candidates if c["date_hint"]]
            if all_dates:
                earliest = min(all_dates)
                latest = max(all_dates)
                self.logger.info(
                    "Section '%s': page contains %d links, date range: %s to %s",
                    section.name,
                    len(candidates),
                    earliest.date(),
                    latest.date(),
                )
            else:
                self.logger.info(
                    "Section '%s': page contains %d links, no dates parsed",
                    section.name,
                    len(candidates),
                )

            # Filter by date range
            discoveries = [
                c
                for c in candidates
                if not c["date_hint"] or (start_date <= c["date_hint"] <= end_date)
            ]

            return discoveries

        except TimeoutException:
            self.logger.warning("Timeout loading archive page: %s", url)
            return []
        except WebDriverException as e:
            self.logger.error("WebDriver error on %s: %s", url, e)
            return []

    def _scroll_to_load_all(
        self,
        driver,
        link_pattern: str,
        start_date: datetime,
    ) -> None:
        """Scroll smoothly one step at a time, tracking discovered article count.

        ECB archives are infinite-scroll, reverse-chronological (newest first).
        Each iteration scrolls one human-like step, then checks progress via
        the number of visible article links.

        Stops when:
        1. The oldest visible article date falls before ``start_date``, OR
        2. No new articles appear for ``MAX_STALE_SCROLLS`` consecutive scrolls.

        Args:
            driver: Selenium WebDriver instance.
            link_pattern: Regex pattern to identify article links on the page.
            start_date: Earliest date of interest.
        """
        link_re = re.compile(link_pattern)
        prev_link_count = 0
        stale_scrolls = 0
        scroll_num = 0

        while True:
            scroll_num += 1

            # Human-like smooth scroll: one step at a time
            viewport_height = driver.execute_script("return window.innerHeight")
            distance = int(
                viewport_height * random.uniform(self.SCROLL_DISTANCE_MIN, self.SCROLL_DISTANCE_MAX)
            )

            # Occasional backscroll (15% chance)
            if random.random() < self.BACKSCROLL_PROBABILITY and scroll_num > 1:
                back_distance = random.randint(100, 300)
                driver.execute_script(
                    f"window.scrollBy({{top: -{back_distance}, behavior: 'smooth'}});"
                )
                time.sleep(random.uniform(0.2, 0.5))

            driver.execute_script(f"window.scrollBy({{top: {distance}, behavior: 'smooth'}});")
            time.sleep(random.uniform(self.SCROLL_PAUSE_MIN, self.SCROLL_PAUSE_MAX))

            # Only check article count periodically (expensive operation)
            if scroll_num % self.ARTICLE_CHECK_INTERVAL != 0:
                continue

            try:
                current_links = driver.find_elements(By.TAG_NAME, "a")
                current_count = sum(
                    1
                    for el in current_links
                    if link_re.search(el.get_attribute("href") or "")
                    and (el.get_attribute("href") or "").endswith(".en.html")
                )
            except StaleElementReferenceException:
                # DOM updated mid-iteration; skip this check cycle
                continue

            if current_count > prev_link_count:
                self.logger.debug(
                    "Scroll %d: %d articles (+%d new)",
                    scroll_num,
                    current_count,
                    current_count - prev_link_count,
                )
                prev_link_count = current_count
                stale_scrolls = 0
            else:
                stale_scrolls += 1

            # Date check for early stop
            if current_count > 0:
                from bs4 import BeautifulSoup

                soup = BeautifulSoup(driver.page_source, "html.parser")
                dates = []
                for link in soup.find_all("a", href=link_re):
                    href = link.get("href", "")
                    if href and href.endswith(".en.html"):
                        d = self._parse_date_from_url(urljoin(self.BASE_URL, href))
                        if d:
                            dates.append(d)
                if dates:
                    oldest = min(dates)
                    if oldest < start_date:
                        self.logger.info(
                            "Date-aware stop: oldest %s < start %s (%d articles, scroll %d)",
                            oldest.date(),
                            start_date.date(),
                            current_count,
                            scroll_num,
                        )
                        break

            # No new articles for N consecutive checks — page exhausted
            if stale_scrolls >= self.MAX_STALE_SCROLLS:
                self.logger.info(
                    "Scrolling complete: no new articles for %d checks (%d total, scroll %d)",
                    self.MAX_STALE_SCROLLS,
                    current_count,
                    scroll_num,
                )
                break

    # ------------------------------------------------------------------
    # Date parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_date_from_url(url: str) -> datetime | None:
        """Extract publication date from an ECB article URL.

        ECB URLs follow patterns like:
        - ``ecb.sp260115~abc123.en.html`` → 2026-01-15
        - ``ecb.pr251201~xyz.en.html`` → 2025-12-01

        Also tries ``/date/YYYY/`` path component as fallback.

        Args:
            url: Full article URL.

        Returns:
            Timezone-aware UTC datetime or None.
        """
        # Try filename pattern: ecb.{type}{YYMMDD}~{id}
        match = URL_DATE_PATTERN.search(url)
        if match:
            try:
                date_str = match.group(1)  # e.g. "260115"
                yy = int(date_str[:2])
                mm = int(date_str[2:4])
                dd = int(date_str[4:6])
                year = 2000 + yy if yy < 50 else 1900 + yy
                return datetime(year, mm, dd, tzinfo=timezone.utc)
            except (ValueError, IndexError):
                pass

        # Fallback: extract year from /date/YYYY/ path
        year_match = re.search(r"/date/(\d{4})/", url)
        if year_match:
            try:
                year = int(year_match.group(1))
                # Use Jan 1 as approximation
                return datetime(year, 1, 1, tzinfo=timezone.utc)
            except ValueError:
                pass

        return None

    # ------------------------------------------------------------------
    # Content fetching (requests, NOT Selenium)
    # ------------------------------------------------------------------

    def _throttle_request(self) -> None:
        """Enforce crawl delay between requests (5s per robots.txt)."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.CRAWL_DELAY:
            time.sleep(self.CRAWL_DELAY - elapsed)
        self._last_request_time = time.time()

    def _fetch_and_build_document(
        self,
        discovery: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Fetch article content and build a document dict.

        Uses requests+BS4 (not Selenium) since individual article pages
        are static HTML.

        Args:
            discovery: Dict with keys: url, title, date_hint, category.

        Returns:
            Document dict matching ECBNewsCollector schema, or None.
        """
        url = discovery["url"]
        title_hint = discovery.get("title", "")
        date_hint = discovery.get("date_hint")
        category = discovery.get("category", "pressreleases")

        # Fetch content
        content = ecb_utils.fetch_full_content(
            self._session, url, self.logger, self._throttle_request, Config.REQUEST_TIMEOUT
        )

        if not content:
            self.logger.warning("No content extracted from %s", url)
            return None

        # Use fetched page title if available, otherwise use hint
        title = title_hint or "Untitled"

        # Classify document type from title/content
        doc_category = ecb_utils.classify_document_type(title, content[:500])

        # Override with section default if classification returns pressreleases
        # (section context is more reliable for archive pages)
        if doc_category == "pressreleases" and category != "pressreleases":
            doc_category = category

        # Extract speaker for speeches
        speaker = None
        if doc_category == "speeches":
            speaker = ecb_utils.extract_speaker_name(title, content)

        # Determine publication date
        pub_date = date_hint or datetime.now(timezone.utc)

        return {
            "source": "ecb",
            "timestamp_collected": datetime.now(timezone.utc).isoformat(),
            "timestamp_published": pub_date.isoformat(),
            "url": url,
            "title": title,
            "content": content,
            "document_type": ecb_utils.DOCUMENT_TYPE_FIELD_MAP[doc_category],
            "speaker": speaker,
            "language": "en",
            "metadata": {
                "archive_section": discovery.get("category", "unknown"),
                "scrape_method": "historical",
            },
            "_category": doc_category,
        }

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _load_existing_urls(self) -> set[str]:
        """Load URLs from existing JSONL files for deduplication.

        Scans all ``*.jsonl`` files in the output directory and extracts
        the ``url`` field from each document.

        Returns:
            Set of already-collected URLs.
        """
        existing: set[str] = set()

        if not self.output_dir.exists():
            return existing

        for jsonl_file in self.output_dir.glob("*.jsonl"):
            try:
                with open(jsonl_file, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            doc = json.loads(line)
                            url = doc.get("url", "")
                            if url:
                                existing.add(url)
            except Exception as e:
                self.logger.warning("Error reading %s for dedup: %s", jsonl_file.name, e)

        return existing

    # ------------------------------------------------------------------
    # Export override
    # ------------------------------------------------------------------

    def export_jsonl(
        self,
        documents: list[dict],
        document_type: str,
        collection_date: datetime | None = None,
    ) -> Path:
        """Export documents to JSONL with type validation.

        Args:
            documents: List of document dictionaries.
            document_type: Must be one of VALID_DOCUMENT_TYPES.
            collection_date: Date for filename (default: today).

        Returns:
            Path to written JSONL file.

        Raises:
            ValueError: If documents empty or type invalid.
        """
        if not documents:
            raise ValueError(f"Cannot export empty data for '{document_type}'")

        if document_type not in ecb_utils.VALID_DOCUMENT_TYPES:
            raise ValueError(
                f"Invalid document_type '{document_type}'. "
                f"Must be one of: {', '.join(sorted(ecb_utils.VALID_DOCUMENT_TYPES))}"
            )

        return super().export_jsonl(documents, document_type, collection_date)
