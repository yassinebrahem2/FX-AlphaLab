"""Bank of England Sitemap-Based Scraper Collector.

Collects historical BoE documents (2021-present) using XML sitemap API.
Provides efficient historical backfill compared to RSS feeds (~10-14 days).

Architecture:
1. Fetch sitemap from /_api/sitemap/getsitemap (1 request)
2. Filter URLs by pattern and date range
3. Direct HTTP requests to each URL (static HTML)
4. Classify by URL pattern (speeches, mpc, summaries, statements)

Performance:
- Sitemap fetch: <1 second
- Per-document: ~0.5-1 second (static HTML)
- Total: ~15-20 minutes for full 5-year archive (~1,000 documents)

Bronze Layer output (immutable raw JSONL):
data/raw/news/boe/{document_type}_{YYYYMMDD}.jsonl

Example usage:
    >>> from datetime import datetime
    >>> from src.ingestion.collectors.boe_scraper_collector import BoEScraperCollector
    >>> collector = BoEScraperCollector()
    >>> data = collector.collect(
    ...     start_date=datetime(2021, 1, 1),
    ...     end_date=datetime(2025, 12, 31)
    ... )
    >>> paths = collector.export_all(data=data)
"""

import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.ingestion.collectors.document_collector import DocumentCollector
from src.shared.config import Config


class BoEScraperCollector(DocumentCollector):
    """Bank of England sitemap-based scraper for historical documents."""

    SOURCE_NAME = "boe"

    BASE_URL = "https://www.bankofengland.co.uk"
    SITEMAP_URL = f"{BASE_URL}/_api/sitemap/getsitemap"

    DEFAULT_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0
    REQUEST_DELAY_MIN = 0.5  # Minimum delay between requests
    REQUEST_DELAY_MAX = 1.0  # Maximum delay between requests

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        """Initialize BoE scraper collector.

        Args:
            output_dir: Directory for Bronze JSONL output
            log_file: Optional log file path
        """
        log_path = log_file or (Config.LOGS_DIR / "collectors" / "boe_scraper_collector.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)

        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "news" / "boe",
            log_file=log_path,
        )

        self.session = self._create_session()
        self.logger.info("BoEScraperCollector initialized, output_dir=%s", self.output_dir)

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, list[dict]]:
        """Collect BoE documents from sitemap for date range.

        Args:
            start_date: Start date for collection (inclusive)
            end_date: End date for collection (inclusive)

        Returns:
            Dict mapping document_type → list of raw records (Bronze contract)
        """
        from collections import defaultdict

        # Default to last 5 years if not provided
        now_utc = datetime.now(timezone.utc)
        if end_date is None:
            end_date = now_utc
        if start_date is None:
            start_date = datetime(2021, 1, 1, tzinfo=timezone.utc)

        # Normalize to UTC aware
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        self.logger.info(
            "Collecting BoE documents from %s to %s via sitemap",
            start_date.isoformat(),
            end_date.isoformat(),
        )

        # Step 1: Fetch and parse sitemap
        urls = self._fetch_sitemap_urls(start_date, end_date)
        self.logger.info("Found %d URLs in sitemap matching date range", len(urls))

        if not urls:
            self.logger.warning("No URLs found in sitemap for date range")
            return {}

        # Step 2: Fetch and parse each document
        results: dict[str, list[dict]] = defaultdict(list)
        collected_count = 0
        total_urls = len(urls)

        # ETA: ~1 doc/sec (0.5-1s delay + fetch time)
        eta_min = total_urls // 60  # Optimistic: 60 docs/min
        eta_max = total_urls // 30  # Conservative: 30 docs/min
        self.logger.info("Fetching %d documents (ETA: %d-%d min)", total_urls, eta_min, eta_max)

        for idx, url_info in enumerate(urls, 1):
            url = url_info["url"]
            lastmod = url_info["lastmod"]

            try:
                # Classify document type by URL pattern
                bucket, doc_type = self._classify_document_type(url)

                # Fetch full page HTML
                self.logger.debug("Fetching %s", url)
                html = self._fetch_page(url)

                # Parse document metadata and content
                doc = self._parse_document(html, url, bucket, doc_type, lastmod, now_utc)

                if doc:
                    results[bucket].append(doc)
                    collected_count += 1

                # Progress logging every 50 documents
                if idx % 50 == 0 or idx == total_urls:
                    self.logger.info(
                        "Progress: %d/%d (%.1f%%) | Collected: %d | speeches=%d mpc=%d summaries=%d statements=%d",
                        idx,
                        total_urls,
                        (idx / total_urls) * 100,
                        collected_count,
                        len(results["speeches"]),
                        len(results["mpc"]),
                        len(results["summaries"]),
                        len(results["statements"]),
                    )

                # Polite crawling delay
                time.sleep(random.uniform(self.REQUEST_DELAY_MIN, self.REQUEST_DELAY_MAX))

            except Exception as e:
                self.logger.warning("Failed to fetch %s: %s", url, e)
                continue

        self.logger.info("Collected %d BoE documents from sitemap", collected_count)
        return dict(results)

    def _fetch_sitemap_urls(self, start_date: datetime, end_date: datetime) -> list[dict[str, str]]:
        """Fetch sitemap and filter URLs by date range and patterns.

        Args:
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)

        Returns:
            List of dicts with 'url' and 'lastmod' keys
        """
        self.logger.info("Fetching sitemap from %s", self.SITEMAP_URL)

        try:
            response = self.session.get(self.SITEMAP_URL, timeout=self.DEFAULT_TIMEOUT)
            response.raise_for_status()
        except Exception as e:
            self.logger.error("Failed to fetch sitemap: %s", e)
            return []

        # Parse XML sitemap
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            self.logger.error("Failed to parse sitemap XML: %s", e)
            return []

        # Extract URLs matching our patterns and date range
        urls = []
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        for url_elem in root.findall("ns:url", ns):
            loc = url_elem.find("ns:loc", ns)
            lastmod = url_elem.find("ns:lastmod", ns)

            if loc is None or loc.text is None:
                continue

            url = loc.text
            lastmod_str = lastmod.text if lastmod is not None and lastmod.text else None

            # Filter by URL pattern (speeches, news, minutes, summaries)
            if not self._matches_url_pattern(url):
                continue

            # Filter by lastmod date if available
            if lastmod_str:
                try:
                    # Parse ISO 8601 date: 2026-02-05T12:02:14+00:00
                    lastmod_dt = datetime.fromisoformat(lastmod_str.replace("Z", "+00:00"))
                    if lastmod_dt < start_date or lastmod_dt > end_date:
                        continue
                except ValueError:
                    # If can't parse lastmod, include URL anyway (will filter later by pub date)
                    pass

            urls.append({"url": url, "lastmod": lastmod_str or ""})

        return urls

    def _matches_url_pattern(self, url: str) -> bool:
        """Check if URL matches our target document patterns.

        Args:
            url: Full URL to check

        Returns:
            True if URL matches speech/news/minutes/summary patterns
        """
        # Match URLs with years 2020-2026 (allowing some buffer)
        patterns = [
            r"/speech/202[0-6]/",
            r"/news/202[0-6]/",
            r"/minutes/202[0-6]/",
            r"/monetary-policy-summary",
            r"/monetary-policy-committee/",
        ]

        return any(re.search(pattern, url) for pattern in patterns)

    def _fetch_page(self, url: str) -> str:
        """Fetch HTML content from URL.

        Args:
            url: Full URL to fetch

        Returns:
            Raw HTML string

        Raises:
            requests.HTTPError: If request fails
        """
        headers = {"User-Agent": "FX-AlphaLab/1.0 (Research Project)"}
        response = self.session.get(url, headers=headers, timeout=self.DEFAULT_TIMEOUT)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        return response.text

    def _parse_document(
        self,
        html: str,
        url: str,
        bucket: str,
        doc_type: str,
        lastmod: str,
        collected_at: datetime,
    ) -> dict | None:
        """Parse document from HTML.

        Args:
            html: Raw HTML content
            url: Document URL
            bucket: Document bucket (speeches, mpc, summaries, statements)
            doc_type: Specific document type
            lastmod: Last modified timestamp from sitemap
            collected_at: Collection timestamp

        Returns:
            Document dict matching Bronze schema, or None if parsing fails
        """
        soup = BeautifulSoup(html, "html.parser")

        # Extract title from <h1 itemprop="name">
        title = None
        h1 = soup.find("h1", attrs={"itemprop": "name"})
        if h1:
            title = h1.get_text(strip=True)

        if not title:
            self.logger.warning("No title found for %s", url)
            return None

        # Extract published date from div.published-date
        # Example: "Published on 08 February 2026"
        published_iso = self._extract_published_date(soup)

        if not published_iso:
            # Fallback to lastmod if no published date found
            if lastmod:
                try:
                    published_dt = datetime.fromisoformat(lastmod.replace("Z", "+00:00"))
                    published_iso = published_dt.isoformat()
                except ValueError:
                    pass

            if not published_iso:
                self.logger.warning("No published date found for %s", url)
                return None

        # Extract speaker for speeches
        speaker = None
        if bucket == "speeches":
            speaker = self._extract_speaker_from_page(soup, url)

        # Extract content using existing BoE logic
        content = self._extract_content(soup)

        if not content or len(content) < 100:
            self.logger.warning("Insufficient content for %s (%d chars)", url, len(content or ""))
            return None

        return {
            "source": "BoE",
            "timestamp_collected": collected_at.isoformat(),
            "timestamp_published": published_iso,
            "url": url,
            "title": title,
            "content": content,
            "document_type": doc_type,
            "speaker": speaker,
            "metadata": {
                "sitemap_lastmod": lastmod,
                "bucket": bucket,
            },
        }

    def _extract_published_date(self, soup: BeautifulSoup) -> str | None:
        """Extract published date from page.

        BoE pages have: <div class="published-date">Published on 08 February 2026</div>

        Args:
            soup: BeautifulSoup parsed HTML

        Returns:
            ISO 8601 timestamp string, or None if not found
        """
        # Try published-date div
        date_div = soup.find("div", class_="published-date")
        if date_div:
            date_text = date_div.get_text(strip=True)
            # Remove "Published on " prefix
            date_text = re.sub(r"^Published on\s+", "", date_text, flags=re.IGNORECASE)
            return self._parse_date_string(date_text)

        # Try other common date elements
        for meta_name in ["article:published_time", "pubdate", "datePublished"]:
            meta = soup.find("meta", attrs={"property": meta_name}) or soup.find(
                "meta", attrs={"name": meta_name}
            )
            if meta and meta.get("content"):
                try:
                    dt = datetime.fromisoformat(meta["content"].replace("Z", "+00:00"))
                    return dt.isoformat()
                except ValueError:
                    continue

        return None

    def _parse_date_string(self, date_str: str) -> str | None:
        """Parse various date formats to ISO 8601.

        Args:
            date_str: Date string like "08 February 2026"

        Returns:
            ISO 8601 timestamp, or None if parsing fails
        """
        # Common BoE formats
        formats = [
            "%d %B %Y",  # "08 February 2026"
            "%d %b %Y",  # "08 Feb 2026"
            "%B %d, %Y",  # "February 08, 2026"
            "%Y-%m-%d",  # "2026-02-08"
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except ValueError:
                continue

        return None

    def _extract_speaker_from_page(self, soup: BeautifulSoup, url: str) -> str | None:
        """Extract speaker name from speech page.

        Reuses existing BoE collector logic.

        Args:
            soup: BeautifulSoup parsed HTML
            url: Page URL

        Returns:
            Speaker name, or None if not found
        """
        # Try meta author
        meta_author = soup.find("meta", attrs={"name": "author"})
        if meta_author and meta_author.get("content"):
            return meta_author["content"].strip()

        # Look for "speech by NAME" pattern near title
        h1 = soup.find("h1")
        if h1:
            container = h1.parent
            text_block = container.get_text("\n", strip=True) if container else ""
            lines = [ln.strip() for ln in text_block.splitlines() if ln.strip()]

            for ln in lines[:25]:
                low = ln.lower()
                if "speech by " in low:
                    idx = low.find("speech by ")
                    return ln[idx + len("speech by ") :].strip()

        # Scan main content
        main = soup.find("main")
        if main:
            lines = [
                ln.strip() for ln in main.get_text("\n", strip=True).splitlines() if ln.strip()
            ]
            for ln in lines[:80]:
                low = ln.lower()
                if "speech by " in low:
                    idx = low.find("speech by ")
                    return ln[idx + len("speech by ") :].strip()

        return None

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract clean text content from page.

        Uses BoE-specific content selectors.

        Args:
            soup: BeautifulSoup parsed HTML

        Returns:
            Clean text content
        """
        # Remove unwanted elements
        for element in soup(
            ["script", "style", "nav", "header", "footer", "aside", "form", "button"]
        ):
            element.decompose()

        # Remove BoE-specific noise
        for noise_class in [
            "cookie-notice",
            "nav-bypass",
            "global-header",
            "global-footer",
            "page-survey",
            "accordion",
            "pdf-button",
            "footer-social-bank",
            "footer-topics",
        ]:
            for element in soup.find_all(class_=noise_class):
                element.decompose()

        # Try content selectors in priority order
        content_selectors = [
            "div.content-block",  # Primary content from screenshot
            "div.page-content",
            "section.page-section",
            "div.accordion-content",
            "article",
            "main#main-content",
            "body",
        ]

        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                text = content_div.get_text(separator="\n", strip=True)
                if len(text) > 200:  # Minimum threshold
                    return text

        # Fallback to body
        body = soup.find("body")
        if body:
            return body.get_text(separator="\n", strip=True)

        return ""

    def _classify_document_type(self, url: str) -> tuple[str, str]:
        """Classify document by URL pattern.

        Reuses existing BoE classifier logic.

        Args:
            url: Document URL

        Returns:
            Tuple of (bucket, document_type)
        """
        u = url.lower()

        # Speeches
        if "/speech/" in u or "/speeches/" in u:
            return "speeches", "boe_speech"

        # Monetary Policy Summary
        if "monetary-policy-summary" in u or "monetary-policy-summary-and-minutes" in u:
            return "summaries", "monetary_policy_summary"

        # MPC
        if "/monetary-policy-committee/" in u or "/mpc/" in u:
            return "mpc", "mpc_statement"

        # Default to statements
        return "statements", "press_release"

    def health_check(self) -> bool:
        """Verify BoE sitemap is reachable.

        Returns:
            True if sitemap returns valid XML
        """
        try:
            response = self.session.get(self.SITEMAP_URL, timeout=10)
            if not response.ok:
                self.logger.warning("Health check failed: HTTP %s", response.status_code)
                return False

            # Try to parse XML
            try:
                root = ET.fromstring(response.content)
                ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                urls = root.findall("ns:url", ns)

                if len(urls) > 0:
                    self.logger.info("Health check OK: sitemap has %d URLs", len(urls))
                    return True

                self.logger.warning("Health check: sitemap has no URLs")
                return False

            except ET.ParseError as e:
                self.logger.warning("Health check: invalid XML: %s", e)
                return False

        except requests.exceptions.RequestException as e:
            self.logger.warning("Health check request error: %s", e)
            return False

    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic.

        Returns:
            Configured requests.Session
        """
        session = requests.Session()

        # Set browser-like headers (BoE requires complete headers, not just User-Agent)
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        retry = Retry(
            total=self.MAX_RETRIES,
            connect=self.MAX_RETRIES,
            read=self.MAX_RETRIES,
            status=self.MAX_RETRIES,
            backoff_factor=self.RETRY_BACKOFF,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session
