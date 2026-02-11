"""Federal Reserve RSS Feed Collector - Bronze Layer (Raw Data Collection).

Collects Federal Reserve press releases, speeches, and testimony from official RSS feed:
    - FOMC statements (monetary policy decisions)
    - Press releases (regulation, supervision, operations)
    - Speeches and statements by Fed officials
    - Congressional testimony
    - Meeting minutes

This collector handles ONLY the Bronze layer (§3.1):
- Fetches raw publication metadata from Fed RSS feed
- Extracts full text content from publication URLs
- Categorizes by document type
- Preserves all source fields with standardized schema
- Adds `source="fed"` column
- Exports to data/raw/fed/ as CSV

Schema (Bronze Layer):
    - source: str (always "fed")
    - timestamp_collected: str (ISO 8601 UTC when data was collected)
    - timestamp_published: str (ISO 8601 UTC when published)
    - url: str (full URL to publication)
    - title: str (publication title)
    - content: str (full text content extracted from URL)
    - document_type: str (fomc_statement|press_release|speech|testimony|minutes)
    - speaker: str (name for speeches, empty otherwise)
    - metadata: str (JSON string with additional fields)

For Silver layer transformation (entity recognition, sentiment analysis),
use appropriate preprocessors.

RSS Feed: https://www.federalreserve.gov/feeds/press_all.xml
Documentation: https://www.federalreserve.gov/feeds/feeds.htm

Example:
    >>> from pathlib import Path
    >>> from datetime import datetime
    >>> from src.ingestion.collectors.fedcollector import FedCollector
    >>>
    >>> collector = FedCollector()
    >>> # Collect publications (categorized by type)
    >>> data = collector.collect(start_date=datetime(2024, 1, 1))
    >>> # Returns: {"statements": df, "speeches": df, "press_releases": df, ...}
    >>> # Export to Bronze layer
    >>> for doc_type, df in data.items():
    >>>     path = collector.export_csv(df, doc_type)
"""

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.ingestion.collectors.base_collector import BaseCollector
from src.shared.config import Config


@dataclass(frozen=True)
class FedDocumentType:
    """Immutable descriptor for a Fed document type."""

    key: str
    name: str
    keywords: tuple[str, ...]


class FedCollector(BaseCollector):
    """Collector for Federal Reserve RSS publications - Bronze Layer (Raw Data).

    Uses the official Federal Reserve RSS feed to collect publication metadata
    and extracts full text content. Publications are categorized by document type.
    Raw data stored in data/raw/fed/ following §3.1 Bronze contract.

    Features:
        - Automatic retry logic for RSS feed and content requests
        - Date-based filtering of publications
        - Document type classification (FOMC statements, speeches, etc.)
        - Full text content extraction from publication URLs
        - Speaker name extraction for speeches
        - UTC timestamp normalization
        - Preserves all metadata in JSON format
        - Polite request delays
        - Comprehensive error handling

    Note:
        This collector handles ONLY Bronze layer (raw collection).
        For Silver layer (text extraction, NLP, sentiment), use preprocessors.
    """

    SOURCE_NAME = "fed"

    # RSS feed URL
    RSS_URL = "https://www.federalreserve.gov/feeds/press_all.xml"

    # Request configuration
    DEFAULT_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0
    REQUEST_DELAY = 1.5  # politeness delay between requests (seconds)

    # Document type definitions with classification keywords
    FOMC_STATEMENTS = FedDocumentType(
        key="statements",
        name="fomc_statement",
        keywords=("fomc", "federal open market committee", "policy statement"),
    )

    PRESS_RELEASES = FedDocumentType(
        key="press_releases",
        name="press_release",
        keywords=(
            "press release",
            "announces",
            "announcement",
            "enforcement action",
            "regulation",
        ),
    )

    SPEECHES = FedDocumentType(
        key="speeches",
        name="speech",
        keywords=("speech", "remarks", "statement by", "governor", "chair"),
    )

    TESTIMONY = FedDocumentType(
        key="testimony",
        name="testimony",
        keywords=("testimony", "testifies", "congress"),
    )

    MINUTES = FedDocumentType(
        key="minutes",
        name="minutes",
        keywords=("minutes", "meeting minutes"),
    )

    _DOCUMENT_TYPES: tuple[FedDocumentType, ...] = (
        FOMC_STATEMENTS,
        SPEECHES,
        TESTIMONY,
        MINUTES,
        PRESS_RELEASES,  # Catch-all, should be last
    )

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        """Initialize the Federal Reserve collector.

        Args:
            output_dir: Directory for Bronze CSV exports (defaults to data/raw/fed).
            log_file: Optional path for file-based logging.
        """
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "fed",
            log_file=log_file or Config.LOGS_DIR / "collectors" / "fed_collector.log",
        )
        self._session = self._create_session()
        self.logger.info("FedCollector initialized, output_dir=%s", self.output_dir)

    # ------------------------------------------------------------------
    # BaseCollector interface
    # ------------------------------------------------------------------

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Collect Fed publications categorized by document type (Bronze - raw data).

        Args:
            start_date: Start of range (default: 90 days ago for 3 months of data).
            end_date: End of range (default: today).

        Returns:
            Dictionary mapping document type keys to DataFrames:
            {
                "statements": DataFrame,     # FOMC statements
                "speeches": DataFrame,       # Speeches and remarks
                "press_releases": DataFrame, # Press releases
                "testimony": DataFrame,      # Congressional testimony
                "minutes": DataFrame         # Meeting minutes
            }

            Each DataFrame contains columns:
            [source, timestamp_collected, timestamp_published, url, title,
             content, document_type, speaker, metadata]

        Raises:
            ValueError: If start_date is after end_date.
            requests.RequestException: If RSS feed request fails after retries.

        Example:
            >>> collector = FedCollector()
            >>> data = collector.collect(start_date=datetime(2024, 1, 1))
            >>> for doc_type, df in data.items():
            >>>     print(f"{doc_type}: {len(df)} publications")
            >>>     collector.export_csv(df, doc_type)
        """
        start = start_date or datetime.now() - timedelta(days=90)  # 3 months
        end = end_date or datetime.now()

        if start > end:
            raise ValueError(f"start_date ({start.date()}) must be before end_date ({end.date()})")

        self.logger.info("Collecting Fed publications from %s to %s", start.date(), end.date())

        # Fetch and categorize publications
        categorized_data = self.fetch_and_categorize_publications(start_date=start, end_date=end)

        # Log collection summary
        total = sum(len(df) for df in categorized_data.values())
        self.logger.info("Collected %d total publications", total)
        for doc_type, df in categorized_data.items():
            if not df.empty:
                self.logger.info("  - %s: %d publications", doc_type, len(df))

        return categorized_data

    def health_check(self) -> bool:
        """Verify Federal Reserve RSS feed is reachable.

        Returns:
            True if RSS feed responds successfully, False otherwise.

        Example:
            >>> collector = FedCollector()
            >>> if collector.health_check():
            ...     print("Fed RSS feed is available")
        """
        try:
            response = self._session.head(self.RSS_URL, timeout=10)
            return response.ok
        except requests.RequestException as e:
            self.logger.error("Fed RSS health check failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # Fed-specific collection methods
    # ------------------------------------------------------------------

    def fetch_and_categorize_publications(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Fetch, parse, and categorize Fed publications by document type.

        Args:
            start_date: Start date for filtering publications.
            end_date: End date for filtering publications.

        Returns:
            Dictionary mapping document type keys to DataFrames with full schema.

        Example:
            >>> collector = FedCollector()
            >>> data = collector.fetch_and_categorize_publications(
            ...     start_date=datetime(2024, 1, 1)
            ... )
            >>> print(data.keys())
            dict_keys(['statements', 'speeches', 'press_releases', 'testimony', 'minutes'])
        """
        start = start_date or datetime.now() - timedelta(days=90)
        end = end_date or datetime.now()

        self.logger.info("Fetching RSS feed from %s", self.RSS_URL)

        # Polite delay before request
        time.sleep(self.REQUEST_DELAY)

        # Fetch RSS feed
        try:
            response = self._session.get(self.RSS_URL, timeout=self.DEFAULT_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error("Failed to fetch RSS feed: %s", e)
            raise

        # Parse RSS feed
        feed = feedparser.parse(response.content)

        if feed.bozo:
            self.logger.warning(
                "RSS feed parsing warning: %s", feed.get("bozo_exception", "Unknown")
            )

        if not feed.entries:
            self.logger.warning("No entries found in RSS feed")
            return self._empty_categorized_dataframes()

        self.logger.info("Parsed %d entries from RSS feed", len(feed.entries))

        # Categorize entries by document type
        categorized_records = {doc_type.key: [] for doc_type in self._DOCUMENT_TYPES}
        timestamp_collected = datetime.now(timezone.utc).isoformat()

        for entry in feed.entries:
            try:
                # Parse published date
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    pub_date = datetime(*entry.published_parsed[:6])
                elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                    pub_date = datetime(*entry.updated_parsed[:6])
                else:
                    self.logger.debug(
                        "Skipping entry without date: %s", entry.get("title", "Unknown")
                    )
                    continue

                # Filter by date range
                if not (start <= pub_date <= end):
                    continue

                # Classify document type
                title = entry.get("title", "").strip()
                summary = entry.get("summary", "").strip()
                doc_type = self._classify_document_type(title, summary)

                # Extract speaker name (for speeches)
                speaker = self._extract_speaker(title) if doc_type.name == "speech" else ""

                # Get URL
                url = entry.get("link", "").strip()

                # Extract full content from URL
                content = self._extract_content_from_url(url)

                # Convert to UTC ISO 8601
                timestamp_published = pub_date.replace(tzinfo=timezone.utc).isoformat()

                # Build metadata JSON
                metadata = json.dumps(
                    {
                        "rss_summary": summary,
                        "rss_published": entry.get("published", ""),
                        "feed_id": entry.get("id", ""),
                    }
                )

                # Create record with full schema
                record = {
                    "source": self.SOURCE_NAME,
                    "timestamp_collected": timestamp_collected,
                    "timestamp_published": timestamp_published,
                    "url": url,
                    "title": title,
                    "content": content,
                    "document_type": doc_type.name,
                    "speaker": speaker,
                    "metadata": metadata,
                }

                categorized_records[doc_type.key].append(record)

            except Exception as e:
                self.logger.warning("Error processing entry: %s", e)
                continue

        # Convert to DataFrames
        result = {}
        for doc_type_key, records in categorized_records.items():
            if records:
                result[doc_type_key] = pd.DataFrame(records)
            else:
                # Return empty DataFrame with correct schema
                result[doc_type_key] = self._empty_dataframe_with_schema()

        self.logger.info(
            "Categorized %d publications into %d document types",
            sum(len(df) for df in result.values()),
            len([df for df in result.values() if not df.empty]),
        )

        return result

    def _empty_categorized_dataframes(self) -> dict[str, pd.DataFrame]:
        """Return empty DataFrames for all document types with correct schema."""
        return {
            doc_type.key: self._empty_dataframe_with_schema() for doc_type in self._DOCUMENT_TYPES
        }

    def _empty_dataframe_with_schema(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct schema."""
        return pd.DataFrame(
            columns=[
                "source",
                "timestamp_collected",
                "timestamp_published",
                "url",
                "title",
                "content",
                "document_type",
                "speaker",
                "metadata",
            ]
        )

    def _classify_document_type(self, title: str, summary: str) -> FedDocumentType:
        """Classify document type based on title and summary keywords.

        Args:
            title: Publication title.
            summary: Publication summary.

        Returns:
            FedDocumentType matching the classification.
        """
        text = (title + " " + summary).lower()

        # Check each document type for keyword matches
        for doc_type in self._DOCUMENT_TYPES:
            if any(keyword in text for keyword in doc_type.keywords):
                return doc_type

        # Default to press release if no match
        return self.PRESS_RELEASES

    def _extract_speaker(self, title: str) -> str:
        """Extract speaker name from speech title.

        Args:
            title: Speech title (e.g., "Chair Powell remarks at...")

        Returns:
            Speaker name or empty string if not found.

        Example:
            >>> collector._extract_speaker("Chair Powell remarks at conference")
            "Chair Powell"
            >>> collector._extract_speaker("Governor Waller speech on...")
            "Governor Waller"
        """
        # Common patterns: "Chair X", "Governor Y", "Vice Chair Z"
        patterns = [
            r"(Chair(?:man)?\s+\w+)",
            r"(Governor\s+\w+)",
            r"(Vice\s+Chair(?:man)?\s+\w+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return ""

    def _extract_content_from_url(self, url: str) -> str:
        """Extract full text content from a Federal Reserve publication URL.

        Args:
            url: Full URL to the publication.

        Returns:
            Extracted text content or empty string on failure.

        Note:
            Implements polite delay between requests.
        """
        if not url:
            return ""

        try:
            # Polite delay
            time.sleep(self.REQUEST_DELAY)

            response = self._session.get(url, timeout=self.DEFAULT_TIMEOUT)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Remove unwanted elements
            for element in soup(["script", "style", "nav", "header", "footer", "aside"]):
                element.decompose()

            # Try multiple Fed-specific content selectors (in priority order)
            content_selectors = [
                "div#article",  # Primary article container
                "div.col-xs-12.col-sm-8.col-md-8",  # Common Fed layout
                "div.row",  # Sometimes content is in row
                "article",  # Semantic HTML5
                "main",  # Semantic HTML5
                "div#content",  # Generic content div
                "body",  # Last resort
            ]

            text_content = ""
            for selector in content_selectors:
                content_div = soup.select_one(selector)
                if content_div:
                    # Get text but skip if it's too short (likely just nav/header)
                    temp_text = content_div.get_text(separator="\n", strip=True)
                    if len(temp_text) > 200:  # Minimum threshold
                        text_content = temp_text
                        break

            # If still empty, get all text from body
            if not text_content or len(text_content) < 200:
                body = soup.find("body")
                if body:
                    text_content = body.get_text(separator="\n", strip=True)

            # Clean up whitespace and common Fed page elements
            text_content = re.sub(r"\n\s*\n", "\n\n", text_content)

            # Remove common navigation/footer text
            noise_patterns = [
                r"Skip to main content",
                r"Board of Governors.*?Federal Reserve System",
                r"Stay Connected.*?RSS",
                r"Last Update:.*?\d{4}",
            ]
            for pattern in noise_patterns:
                text_content = re.sub(pattern, "", text_content, flags=re.IGNORECASE | re.DOTALL)

            text_content = text_content.strip()

            self.logger.debug("Extracted %d chars from %s", len(text_content), url)
            return text_content

        except Exception as e:
            self.logger.warning("Failed to extract content from %s: %s", url, e)
            return ""

    # ------------------------------------------------------------------
    # Private helper methods
    # ------------------------------------------------------------------

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic.

        Returns:
            Configured requests.Session with automatic retries.
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=self.RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set headers
        session.headers.update(
            {
                "User-Agent": "FX-AlphaLab/1.0 (Educational Research Project)",
                "Accept": "application/xml, application/rss+xml, */*",
            }
        )

        return session
