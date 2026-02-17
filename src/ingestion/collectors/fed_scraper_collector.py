"""Federal Reserve year-based scraper for historical document collection.

Collects Federal Reserve press releases and speeches from year-specific archive pages.
Uses simple HTTP requests + BeautifulSoup (no Selenium required) since Fed provides
static HTML pages with all documents for each year.

Data Sources:
    - Press releases: /newsevents/pressreleases/{YEAR}-press.htm
    - Speeches: /newsevents/speech/{YEAR}-speeches.htm

Example:
    >>> from pathlib import Path
    >>> from datetime import datetime
    >>> from src.ingestion.collectors.fed_scraper_collector import FedScraperCollector
    >>>
    >>> collector = FedScraperCollector()
    >>> # Collect documents for 2021-2025
    >>> data = collector.collect(
    ...     start_date=datetime(2021, 1, 1),
    ...     end_date=datetime(2025, 12, 31),
    ...     include_speeches=True
    ... )
    >>> # Export to JSONL
    >>> paths = collector.export_all(data=data)
"""

import random
import time
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup

from src.ingestion.collectors.document_collector import DocumentCollector
from src.ingestion.collectors.fed_utils import (
    BASE_URL,
    classify_document_type,
    create_fed_session,
    extract_speaker_name,
    fetch_full_content,
    parse_date_from_text,
)
from src.shared.config import Config


class FedScraperCollector(DocumentCollector):
    """Collect Federal Reserve documents from year-based archive pages.

    This collector fetches press releases and speeches from Fed's static HTML
    archive pages. Each year has a dedicated page containing all documents for
    that year, eliminating the need for Selenium or pagination handling.

    Architecture:
        - Uses requests + BeautifulSoup4 (no Selenium overhead)
        - Fetches year-specific pages (2021-press.htm, 2022-press.htm, etc.)
        - Parses HTML to extract metadata (date, title, category, speaker)
        - Fetches full article content from individual URLs
        - Filters documents by exact date range
        - Exports to JSONL following Bronze layer contract

    Advantages over RSS-based collector:
        - Historical backfill (RSS only has ~10-14 days of data)
        - Complete document coverage for any year
        - Faster for multi-year ranges (5 requests vs 227+ paginated requests)
        - More reliable (static HTML vs JavaScript pagination)

    Note:
        This is a Bronze layer collector. For Silver layer transformation
        (sentiment analysis, entity extraction), use preprocessors.
    """

    SOURCE_NAME = "fed"

    # URL templates for year-based archives
    PRESS_RELEASES_URL_TEMPLATE = f"{BASE_URL}/newsevents/pressreleases/{{year}}-press.htm"
    SPEECHES_URL_TEMPLATE = f"{BASE_URL}/newsevents/speech/{{year}}-speeches.htm"

    # Polite crawling delays (seconds)
    REQUEST_DELAY_MIN = 1.0
    REQUEST_DELAY_MAX = 2.0
    ARTICLE_FETCH_DELAY_MIN = 0.5
    ARTICLE_FETCH_DELAY_MAX = 1.0

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        """Initialize the Fed scraper collector.

        Args:
            output_dir: Directory for JSONL exports (default: data/raw/news/fed).
            log_file: Optional path for file-based logging.
        """
        if output_dir is None:
            output_dir = Config.DATA_DIR / "raw" / "news" / "fed"

        super().__init__(output_dir, log_file)
        self.session = create_fed_session()

    def health_check(self) -> bool:
        """Verify Fed website is reachable.

        Returns:
            True if site responds with 200, False otherwise.
        """
        try:
            response = self.session.head(BASE_URL, timeout=10)
            return response.status_code == 200
        except Exception as e:
            self.logger.error("Health check failed: %s", e)
            return False

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        include_speeches: bool = True,
    ) -> dict[str, list[dict]]:
        """Collect Fed documents for date range.

        Fetches press releases and optionally speeches from year-specific
        archive pages. Documents are filtered by exact date range and
        categorized by type.

        Args:
            start_date: Start of collection window (default: 2020-01-01).
            end_date: End of collection window (default: today).
            include_speeches: Whether to collect speeches (default: True).

        Returns:
            Dictionary mapping document type to list of document dicts.
            Document types: policy, regulation, speeches, other.

        Example:
            >>> collector = FedScraperCollector()
            >>> data = collector.collect(
            ...     start_date=datetime(2024, 1, 1),
            ...     end_date=datetime(2024, 12, 31)
            ... )
            >>> # Returns: {
            >>> #     "policy": [{...}, {...}],
            >>> #     "regulation": [{...}],
            >>> #     "speeches": [{...}, {...}],
            >>> #     "other": [{...}]
            >>> # }
        """
        # Set defaults
        if start_date is None:
            start_date = datetime(2020, 1, 1)
        if end_date is None:
            end_date = datetime.now()

        # Calculate year range
        years = list(range(start_date.year, end_date.year + 1))
        self.logger.info(
            "Collecting Fed documents for %d-%d (%d years)",
            start_date.year,
            end_date.year,
            len(years),
        )

        # Collect documents by type
        documents_by_type: dict[str, list[dict]] = {
            "policy": [],
            "regulation": [],
            "speeches": [],
            "other": [],
        }

        # Fetch press releases for each year
        for year in years:
            self.logger.info("Fetching press releases for %d", year)
            docs = self._fetch_year_releases(year, start_date, end_date)

            # Categorize by document type
            for doc in docs:
                doc_type = doc["document_type"]
                documents_by_type[doc_type].append(doc)

            # Polite delay between years
            time.sleep(random.uniform(self.REQUEST_DELAY_MIN, self.REQUEST_DELAY_MAX))

        # Fetch speeches if requested
        if include_speeches:
            for year in years:
                self.logger.info("Fetching speeches for %d", year)
                docs = self._fetch_year_speeches(year, start_date, end_date)
                documents_by_type["speeches"].extend(docs)

                # Polite delay
                time.sleep(random.uniform(self.REQUEST_DELAY_MIN, self.REQUEST_DELAY_MAX))

        # Log summary
        total = sum(len(docs) for docs in documents_by_type.values())
        self.logger.info(
            "Collected %d total documents: policy=%d, regulation=%d, speeches=%d, other=%d",
            total,
            len(documents_by_type["policy"]),
            len(documents_by_type["regulation"]),
            len(documents_by_type["speeches"]),
            len(documents_by_type["other"]),
        )

        # Remove empty categories
        return {k: v for k, v in documents_by_type.items() if v}

    def _fetch_year_releases(
        self,
        year: int,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Fetch all press releases for a specific year.

        Args:
            year: Year to fetch (e.g., 2025).
            start_date: Filter start date.
            end_date: Filter end date.

        Returns:
            List of document dictionaries with full content.
        """
        url = self.PRESS_RELEASES_URL_TEMPLATE.format(year=year)

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            self.logger.error("Failed to fetch %s: %s", url, e)
            return []

        # Parse HTML to extract metadata
        items = self._parse_release_items(response.text)

        # Filter by date range
        items = [item for item in items if start_date <= item["timestamp_published"] <= end_date]

        self.logger.info("Found %d press releases for %d in date range", len(items), year)

        # Fetch full content for each document
        documents = []
        for item in items:
            try:
                full_content = fetch_full_content(item["url"], self.session)

                # Build complete document
                doc = {
                    "source": "fed",
                    "timestamp_collected": datetime.now().isoformat() + "Z",
                    "timestamp_published": item["timestamp_published"].isoformat() + "Z",
                    "url": item["url"],
                    "title": item["title"],
                    "content": full_content,
                    "document_type": item["document_type"],
                    "category": item["category"],
                    "speaker": None,
                    "metadata": {},
                }

                documents.append(doc)

                # Polite delay between articles
                time.sleep(
                    random.uniform(
                        self.ARTICLE_FETCH_DELAY_MIN,
                        self.ARTICLE_FETCH_DELAY_MAX,
                    )
                )

            except Exception as e:
                self.logger.warning("Failed to fetch article %s: %s", item["url"], e)
                continue

        return documents

    def _parse_release_items(self, html: str) -> list[dict]:
        """Parse press release items from year page HTML.

        Press release structure (same as speeches):
            <div class="row">
                <div class="col-xs-3 col-md-2 eventlist__time">
                    <time>12/30/2025</time>
                </div>
                <div class="col-xs-9 col-md-10 eventlist__event">
                    <p><a href="/newsevents/pressreleases/monetary20251230a.htm">
                        <em>Minutes of the Federal Open Market Committee...</em>
                    </a></p>
                    <p class='eventlist__press'><em><strong>Monetary Policy</strong></em></p>
                </div>
            </div>

        Args:
            html: Raw HTML content from year page.

        Returns:
            List of parsed item dictionaries with metadata (no full content yet).
        """
        soup = BeautifulSoup(html, "html.parser")
        items = []

        # Find all event rows (same structure as speeches)
        for row in soup.find_all("div", class_="row"):
            time_div = row.find("div", class_="eventlist__time")
            event_div = row.find("div", class_="eventlist__event")

            if not (time_div and event_div):
                continue

            # Extract date from <time> tag
            time_tag = time_div.find("time")
            if not time_tag:
                continue

            timestamp = parse_date_from_text(time_tag.get_text(strip=True))
            if not timestamp:
                continue

            # Extract title and URL (first <a> in event div)
            link = event_div.find("a")
            if not link:
                continue

            url = link.get("href", "")
            title = link.get_text(strip=True)

            # Extract category (p.eventlist__press > em > strong)
            category = None
            category_p = event_div.find("p", class_="eventlist__press")
            if category_p:
                strong_tag = category_p.find("strong")
                if strong_tag:
                    category = strong_tag.get_text(strip=True)

            # Classify document type
            doc_type = classify_document_type(category, is_speech=False)

            items.append(
                {
                    "url": url,
                    "title": title,
                    "timestamp_published": timestamp,
                    "category": category or "other",
                    "document_type": doc_type,
                }
            )

        return items

    def _fetch_year_speeches(
        self,
        year: int,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Fetch all speeches for a specific year.

        Args:
            year: Year to fetch (e.g., 2025).
            start_date: Filter start date.
            end_date: Filter end date.

        Returns:
            List of speech document dictionaries with full content.
        """
        url = self.SPEECHES_URL_TEMPLATE.format(year=year)

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            self.logger.error("Failed to fetch %s: %s", url, e)
            return []

        # Parse HTML to extract metadata
        items = self._parse_speech_items(response.text)

        # Filter by date range
        items = [item for item in items if start_date <= item["timestamp_published"] <= end_date]

        self.logger.info("Found %d speeches for %d in date range", len(items), year)

        # Fetch full content
        documents = []
        for item in items:
            try:
                full_content = fetch_full_content(item["url"], self.session)

                doc = {
                    "source": "fed",
                    "timestamp_collected": datetime.now().isoformat() + "Z",
                    "timestamp_published": item["timestamp_published"].isoformat() + "Z",
                    "url": item["url"],
                    "title": item["title"],
                    "content": full_content,
                    "document_type": "speeches",
                    "category": "speeches",
                    "speaker": item["speaker"],
                    "metadata": {"location": item.get("location", "")},
                }

                documents.append(doc)

                # Polite delay
                time.sleep(
                    random.uniform(
                        self.ARTICLE_FETCH_DELAY_MIN,
                        self.ARTICLE_FETCH_DELAY_MAX,
                    )
                )

            except Exception as e:
                self.logger.warning("Failed to fetch speech %s: %s", item["url"], e)
                continue

        return documents

    def _parse_speech_items(self, html: str) -> list[dict]:
        """Parse speech items from year page HTML.

        Speech structure:
            <div class="row">
                <div class="col-xs-3 col-md-2 eventlist__time">
                    <time>12/15/2025</time>
                </div>
                <div class="col-xs-9 col-md-10 eventlist__event">
                    <p><a href="/newsevents/speech/miran20251215a.htm">
                        <em>The Inflation Outlook</em>
                    </a></p>
                    <p class="news__speaker">Governor Stephen I. Miran</p>
                    <p>At the School of International...</p>
                </div>
            </div>

        Args:
            html: Raw HTML content from year page.

        Returns:
            List of parsed speech dictionaries with metadata.
        """
        soup = BeautifulSoup(html, "html.parser")
        items = []

        # Find all event rows
        for row in soup.find_all("div", class_="row"):
            time_div = row.find("div", class_="eventlist__time")
            event_div = row.find("div", class_="eventlist__event")

            if not (time_div and event_div):
                continue

            # Extract date from <time> tag
            time_tag = time_div.find("time")
            if not time_tag:
                continue

            timestamp = parse_date_from_text(time_tag.get_text(strip=True))
            if not timestamp:
                continue

            # Extract title and URL (first <a> in event div)
            link = event_div.find("a")
            if not link:
                continue

            url = link.get("href", "")
            title = link.get_text(strip=True)

            # Extract speaker (p.news__speaker)
            speaker_p = event_div.find("p", class_="news__speaker")
            speaker = None
            if speaker_p:
                speaker = extract_speaker_name(speaker_p.get_text(strip=True))

            # Extract location (next <p> after speaker)
            location = None
            if speaker_p:
                location_p = speaker_p.find_next_sibling("p")
                if location_p:
                    location = location_p.get_text(strip=True)

            items.append(
                {
                    "url": url,
                    "title": title,
                    "timestamp_published": timestamp,
                    "speaker": speaker,
                    "location": location,
                }
            )

        return items
