"""ECB News Collector - Bronze Layer (Raw News & Sentiment Data Collection).

Collects European Central Bank press releases, speeches, monetary policy statements,
and economic bulletins from the official ECB RSS feed.

This collector handles ONLY the Bronze layer (§3.1):
- Fetches raw news content from ECB RSS feed
- Preserves all source fields immutably
- Categorizes by document type (press releases, speeches, policy, bulletins)
- Adds `source="ecb"` column
- Exports to data/raw/news/ecb/ in JSONL format

For Silver layer transformation (sentiment scoring, entity extraction),
use NewsNormalizer preprocessor (future implementation).

RSS Feed: https://www.ecb.europa.eu/rss/press.html
Press Page: https://www.ecb.europa.eu/press/html/index.en.html

Example:
    >>> from datetime import datetime, timedelta
    >>> from pathlib import Path
    >>> from src.ingestion.collectors.ecb_news_collector import ECBNewsCollector
    >>>
    >>> collector = ECBNewsCollector(output_dir=Path("data/raw/news/ecb"))
    >>> # Collect raw news data (3 months)
    >>> data = collector.collect(
    ...     start_date=datetime.now() - timedelta(days=90),
    ...     end_date=datetime.now()
    ... )
    >>> # Export to Bronze layer JSONL files
    >>> paths = collector.export_all(data)
"""

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import feedparser
import requests
from bs4 import BeautifulSoup

from src.ingestion.collectors.document_collector import DocumentCollector
from src.shared.config import Config


@dataclass
class ECBNewsDocument:
    """Immutable descriptor for an ECB news document."""

    source: str
    timestamp_collected: str
    timestamp_published: str
    url: str
    title: str
    content: str
    document_type: str
    speaker: str | None
    language: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "source": self.source,
            "timestamp_collected": self.timestamp_collected,
            "timestamp_published": self.timestamp_published,
            "url": self.url,
            "title": self.title,
            "content": self.content,
            "document_type": self.document_type,
            "speaker": self.speaker,
            "language": self.language,
            "metadata": self.metadata,
        }


class ECBNewsCollector(DocumentCollector):
    """Collector for ECB news, speeches, and policy documents - Bronze Layer.

    Fetches content from ECB's official RSS feed and categorizes by document type.
    Raw data stored in data/raw/news/ecb/ following §3.1 Bronze contract in JSONL format.

    Features:
        - RSS feed parsing with feedparser
        - Document type classification (press releases, speeches, policy, bulletins)
        - Full content extraction from individual pages
        - Speaker name extraction for speeches
        - Automatic retry with exponential backoff
        - Rate limiting to avoid overwhelming ECB servers
        - Comprehensive error handling

    Document Types:
        - pressreleases: General ECB press releases
        - speeches: Speeches by ECB officials
        - policy: Monetary policy decisions and statements
        - bulletins: Economic bulletins and reports

    Note:
        This collector handles ONLY Bronze layer (raw collection).
        For Silver layer (sentiment analysis, NLP), use NewsNormalizer (future).
    """

    SOURCE_NAME = "ecb"

    # Valid document types for validation
    VALID_DOCUMENT_TYPES = {"pressreleases", "speeches", "policy", "bulletins"}

    # Mapping from internal category to document_type field value
    DOCUMENT_TYPE_FIELD_MAP = {
        "pressreleases": "press_release",
        "speeches": "speech",
        "policy": "policy_decision",
        "bulletins": "bulletin",
    }

    # RSS feed URLs
    RSS_FEED_URL = "https://www.ecb.europa.eu/rss/press.html"
    BASE_URL = "https://www.ecb.europa.eu"

    # Rate limiting: be polite to ECB servers
    MIN_REQUEST_INTERVAL = 1.0  # seconds between requests
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0

    # Document type classification patterns
    DOCUMENT_TYPE_PATTERNS = {
        "policy": [
            r"monetary policy decisions?",
            r"governing council",
            r"interest rate",
            r"key ecb interest rates",
            r"policy decision",
        ],
        "speeches": [
            r"speech by",
            r"remarks by",
            r"keynote",
            r"interview with",
            r"statement by",
        ],
        "bulletins": [
            r"economic bulletin",
            r"monthly bulletin",
            r"quarterly bulletin",
        ],
        "pressreleases": [
            # Default catch-all
        ],
    }

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        """Initialize the ECB collector.

        Args:
            output_dir: Directory for Bronze JSONL exports (default: data/raw/news/ecb).
            log_file: Optional path for file-based logging.
        """
        # Use Config defaults if not provided
        final_output_dir = output_dir or Config.DATA_DIR / "raw" / "news" / "ecb"
        final_log_file = log_file or Config.LOGS_DIR / "collectors" / "ecb_collector.log"

        super().__init__(output_dir=final_output_dir, log_file=final_log_file)
        self._last_request_time: float = 0.0
        self._session = self._create_session()

        self.logger.info(
            "ECBNewsCollector initialized, output_dir=%s",
            self.output_dir,
        )

    # ------------------------------------------------------------------
    # DocumentCollector interface
    # ------------------------------------------------------------------

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Collect all ECB news documents (Bronze - raw data).

        Pure collection method with NO file I/O. Returns structured data only.

        Args:
            start_date: Start of range (default: 3 months ago).
            end_date: End of range (default: now).

        Returns:
            Dictionary mapping document type to list of document dicts:
            {
                "pressreleases": [{...}, {...}],
                "speeches": [{...}, {...}],
                "policy": [{...}, {...}],
                "bulletins": [{...}, {...}]
            }

            Each document dict contains:
            - source: "ecb"
            - timestamp_collected: ISO 8601 UTC
            - timestamp_published: ISO 8601 UTC
            - url: Document URL
            - title: Document title
            - content: Full text content
            - document_type: One of the 4 types
            - speaker: Speaker name (for speeches) or None
            - language: Document language (usually "en")
            - metadata: Additional fields from RSS

        Raises:
            ValueError: If start_date is after end_date.
            RuntimeError: If RSS feed is unreachable after retries.

        Example:
            >>> from pathlib import Path
            >>> collector = ECBNewsCollector(output_dir=Path("data/raw/news/ecb"))
            >>> data = collector.collect(start_date=datetime(2025, 11, 1))
            >>> press_releases = data["pressreleases"]
            >>> speeches = data["speeches"]
        """
        start = start_date or datetime.now(timezone.utc) - timedelta(days=90)
        end = end_date or datetime.now(timezone.utc)

        # Ensure timezone-aware
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        if start > end:
            raise ValueError(f"start_date ({start.date()}) must be before end_date ({end.date()})")

        self.logger.info("Collecting ECB news from %s to %s", start.date(), end.date())

        # Initialize result structure
        result: dict[str, list[dict[str, Any]]] = {
            "pressreleases": [],
            "speeches": [],
            "policy": [],
            "bulletins": [],
        }

        # Fetch RSS feed
        feed = self._fetch_rss_feed()

        if not feed or not hasattr(feed, "entries") or feed.entries is None:
            raise RuntimeError("Failed to fetch RSS feed or feed has no entries")

        self.logger.info("RSS feed retrieved: %d total entries", len(feed.entries))

        # Process each entry
        collected_count = 0
        skipped_count = 0

        for entry in feed.entries:
            try:
                # Parse publication date
                pub_date = self._parse_entry_date(entry)
                if not pub_date:
                    self.logger.warning("Skipping entry with no date: %s", entry.get("title", ""))
                    skipped_count += 1
                    continue

                # Filter by date range
                if not (start <= pub_date <= end):
                    skipped_count += 1
                    continue

                # Extract document
                doc = self._extract_document(entry, pub_date)
                if doc:
                    # Route to correct category using internal _category field
                    category = doc.pop("_category", "pressreleases")
                    result[category].append(doc)
                    collected_count += 1
                    self.logger.debug("Collected %s: %s", doc["document_type"], doc["title"][:50])
                else:
                    skipped_count += 1

            except Exception as e:
                self.logger.error("Failed to process entry: %s", e)
                skipped_count += 1
                continue

        self.logger.info(
            "Collection complete: %d documents collected, %d skipped",
            collected_count,
            skipped_count,
        )
        self.logger.info(
            "By type - pressreleases: %d, speeches: %d, policy: %d, bulletins: %d",
            len(result["pressreleases"]),
            len(result["speeches"]),
            len(result["policy"]),
            len(result["bulletins"]),
        )

        return result

    def health_check(self) -> bool:
        """Verify ECB RSS feed is reachable.

        Returns:
            True if RSS feed responds successfully, False otherwise.

        Example:
            >>> from pathlib import Path
            >>> collector = ECBNewsCollector(output_dir=Path("data/raw/news/ecb"))
            >>> if collector.health_check():
            ...     print("ECB RSS feed is available")
        """
        try:
            self._throttle_request()
            response = self._session.head(self.RSS_FEED_URL, timeout=10)
            return response.ok
        except Exception as e:
            self.logger.error("ECB RSS health check failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # Private: RSS feed parsing
    # ------------------------------------------------------------------

    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic."""
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retry = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=self.RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _throttle_request(self) -> None:
        """Ensure minimum interval between requests to be polite to ECB servers."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.MIN_REQUEST_INTERVAL:
            time.sleep(self.MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

    def _fetch_rss_feed(self) -> feedparser.FeedParserDict:
        """Fetch and parse ECB RSS feed.

        Returns:
            Parsed RSS feed.

        Raises:
            RuntimeError: If feed cannot be fetched after retries.
        """
        self._throttle_request()

        try:
            self.logger.info("Fetching RSS feed: %s", self.RSS_FEED_URL)
            feed = feedparser.parse(self.RSS_FEED_URL)

            if feed.bozo:
                self.logger.warning("RSS feed parsing encountered issues: %s", feed.bozo_exception)

            return feed

        except Exception as e:
            self.logger.error("Failed to fetch RSS feed: %s", e)
            raise RuntimeError(f"Cannot fetch ECB RSS feed: {e}") from e

    def _parse_entry_date(self, entry: dict[str, Any]) -> datetime | None:
        """Parse publication date from RSS entry.

        Args:
            entry: RSS feed entry dictionary.

        Returns:
            Timezone-aware datetime or None if parsing fails.
        """
        # Try published_parsed first (most reliable)
        if "published_parsed" in entry and entry["published_parsed"]:
            try:
                from time import struct_time

                time_tuple: struct_time = entry["published_parsed"]
                dt = datetime(*time_tuple[:6], tzinfo=timezone.utc)
                return dt
            except Exception as e:
                self.logger.debug("Failed to parse published_parsed: %s", e)

        # Try updated_parsed
        if "updated_parsed" in entry and entry["updated_parsed"]:
            try:
                time_tuple = entry["updated_parsed"]
                dt = datetime(*time_tuple[:6], tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

        # Try parsing published string
        if "published" in entry:
            try:
                from email.utils import parsedate_to_datetime

                dt = parsedate_to_datetime(entry["published"])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

        self.logger.warning("Could not parse date for entry: %s", entry.get("title", "Unknown"))
        return None

    def _classify_document_type(self, title: str, summary: str = "") -> str:
        """Classify document type based on title and summary patterns.

        Args:
            title: Document title.
            summary: Document summary/description.

        Returns:
            One of: "policy", "speeches", "bulletins", "pressreleases"
        """
        text = (title + " " + summary).lower()

        # Check patterns in priority order
        for doc_type, patterns in self.DOCUMENT_TYPE_PATTERNS.items():
            if doc_type == "pressreleases":
                continue  # Skip default catch-all

            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return doc_type

        # Default to press releases
        return "pressreleases"

    def _extract_speaker_name(self, title: str, content: str = "") -> str | None:
        """Extract speaker name from title or content (for speeches).

        Args:
            title: Document title.
            content: Document content.

        Returns:
            Speaker name or None.
        """
        # Pattern: "Speech by [Name]" or "Remarks by [Name]"
        patterns = [
            r"(?:speech|remarks|interview|statement)\s+by\s+([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)(?:\s+at|\s+in|\s+to|\s+for|\s+on|,|$)",
            r"([A-Z][a-z]+\s+[A-Z][a-z]+)(?:\s*,\s*(?:President|Vice-President|Member|Chief))",
        ]

        text = title + " " + content[:500]  # First 500 chars

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                # Clean up whitespace
                name = re.sub(r"\s+", " ", name)
                if len(name) > 3 and len(name) < 50:  # Sanity check
                    return name

        return None

    def _fetch_full_content(self, url: str) -> str:
        """Fetch full text content from document URL.

        Args:
            url: Document URL.

        Returns:
            Full text content or empty string if fetch fails.
        """
        if not url:
            return ""

        try:
            self._throttle_request()
            response = self._session.get(url, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Remove script, style, and navigation elements
            for element in soup(["script", "style", "nav", "header", "footer"]):
                element.decompose()

            # Try to find main content area
            content_areas = [
                soup.find("div", class_=re.compile(r"content|article|body")),
                soup.find("article"),
                soup.find("main"),
                soup.find("div", id=re.compile(r"content|article")),
            ]

            for area in content_areas:
                if area:
                    text = area.get_text(separator="\n", strip=True)
                    if len(text) > 100:  # Minimum content threshold
                        return text

            # Fallback: get all body text
            body = soup.find("body")
            if body:
                return body.get_text(separator="\n", strip=True)

            return ""

        except Exception as e:
            self.logger.warning("Failed to fetch content from %s: %s", url, e)
            return ""

    def _extract_document(self, entry: dict[str, Any], pub_date: datetime) -> dict[str, Any] | None:
        """Extract structured document from RSS entry.

        Args:
            entry: RSS feed entry.
            pub_date: Parsed publication date.

        Returns:
            Document dictionary or None if extraction fails.
        """
        try:
            title = entry.get("title", "").strip()
            url = entry.get("link", "")
            summary = entry.get("summary", "")

            if not title or not url:
                self.logger.warning("Entry missing title or URL, skipping")
                return None

            # Classify document type (returns category key)
            doc_category = self._classify_document_type(title, summary)

            # Fetch full content (with throttling)
            content = self._fetch_full_content(url)
            if not content:
                # Fallback to summary if full content unavailable
                content = summary

            # Extract speaker for speeches
            speaker = None
            if doc_category == "speeches":
                speaker = self._extract_speaker_name(title, content)

            # Detect language (default to English)
            language = "en"
            if "language" in entry:
                language = entry["language"]

            # Build metadata
            metadata = {
                "rss_id": entry.get("id", ""),
                "tags": [tag.get("term", "") for tag in entry.get("tags", [])],
            }

            # Build document with proper document_type field value
            doc = {
                "source": "ecb",
                "timestamp_collected": datetime.now(timezone.utc).isoformat(),
                "timestamp_published": pub_date.isoformat(),
                "url": url,
                "title": title,
                "content": content,
                "document_type": self.DOCUMENT_TYPE_FIELD_MAP[doc_category],
                "speaker": speaker,
                "language": language,
                "metadata": metadata,
                "_category": doc_category,  # Internal field for routing
            }

            return doc

        except Exception as e:
            self.logger.error("Failed to extract document: %s", e)
            return None

    def export_jsonl(
        self,
        documents: list[dict],
        document_type: str,
        collection_date: datetime | None = None,
    ) -> Path:
        """Export documents to JSONL with document type validation.

        Overrides parent to validate document_type against ECB's valid types.

        Args:
            documents: List of document dictionaries to export.
            document_type: Document type identifier (must be one of VALID_DOCUMENT_TYPES).
            collection_date: Date to use in filename (default: today).

        Returns:
            Path to the written JSONL file.

        Raises:
            ValueError: If documents list is empty or document_type is invalid.
        """
        if not documents:
            raise ValueError(f"Cannot export empty data for '{document_type}'")

        if document_type not in self.VALID_DOCUMENT_TYPES:
            raise ValueError(
                f"Invalid document_type '{document_type}'. "
                f"Must be one of: {', '.join(sorted(self.VALID_DOCUMENT_TYPES))}"
            )

        return super().export_jsonl(documents, document_type, collection_date)
