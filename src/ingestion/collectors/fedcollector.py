"""Federal Reserve RSS Feed Collector - Bronze Layer (Raw Data Collection).

Collects Federal Reserve press releases, speeches, and testimony from official RSS feed:
    - Press releases (monetary policy, regulation, supervision, etc.)
    - Speeches and statements by Fed officials
    - Congressional testimony
    - Other Fed announcements

This collector handles ONLY the Bronze layer (§3.1):
- Fetches raw publication metadata from Fed RSS feed
- Preserves all source fields (title, link, published date, summary)
- Adds `source="fed"` column
- Exports to data/raw/fed/

For Silver layer transformation (text extraction, entity recognition, sentiment),
use appropriate preprocessors.

RSS Feed: https://www.federalreserve.gov/feeds/press_all.xml
Documentation: https://www.federalreserve.gov/feeds/feeds.htm

Example:
    >>> from pathlib import Path
    >>> from datetime import datetime
    >>> from src.ingestion.collectors.fedcollector import FedCollector
    >>>
    >>> collector = FedCollector()
    >>> # Collect raw publication metadata
    >>> data = collector.collect(start_date=datetime(2024, 1, 1))
    >>> # Export to Bronze layer
    >>> for dataset_name, df in data.items():
    >>>     path = collector.export_csv(df, dataset_name)
"""

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import feedparser
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.ingestion.collectors.base_collector import BaseCollector
from src.shared.config import Config


@dataclass(frozen=True)
class FedPublication:
    """Immutable descriptor for a Fed publication type."""

    name: str
    description: str


class FedCollector(BaseCollector):
    """Collector for Federal Reserve RSS publications - Bronze Layer (Raw Data).

    Uses the official Federal Reserve RSS feed to collect publication metadata.
    Raw data stored in data/raw/fed/ following §3.1 Bronze contract.

    Features:
        - Automatic retry logic for RSS feed requests
        - Date-based filtering of publications
        - Preserves all RSS entry metadata
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
    REQUEST_DELAY = 1.0  # politeness delay (seconds)

    # Publication types
    PUBLICATIONS = FedPublication(
        name="publications",
        description="Fed press releases, speeches, testimony, and announcements",
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
        """Collect Fed publications within a date range (Bronze - raw data).

        Args:
            start_date: Start of range (default: 30 days ago).
            end_date: End of range (default: today).

        Returns:
            Dictionary with key "publications" mapping to raw DataFrame with columns:
            [title, link, published, summary, source]

        Raises:
            ValueError: If start_date is after end_date.
            requests.RequestException: If RSS feed request fails after retries.

        Example:
            >>> collector = FedCollector()
            >>> data = collector.collect(start_date=datetime(2024, 1, 1))
            >>> df = data["publications"]
            >>> collector.export_csv(df, "publications")
        """
        start = start_date or datetime.now() - timedelta(days=30)
        end = end_date or datetime.now()

        if start > end:
            raise ValueError(f"start_date ({start.date()}) must be before end_date ({end.date()})")

        self.logger.info("Collecting Fed publications from %s to %s", start.date(), end.date())

        df = self.fetch_publications(start_date=start, end_date=end)

        if not df.empty:
            self.logger.info("Collected %d publications", len(df))
        else:
            self.logger.warning("No publications found in date range")

        return {self.PUBLICATIONS.name: df}

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

    def fetch_publications(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> pd.DataFrame:
        """Fetch and parse Fed publications from RSS feed (Bronze - raw data).

        Args:
            start_date: Start date for filtering publications.
            end_date: End date for filtering publications.

        Returns:
            Raw DataFrame with columns: [title, link, published, summary, source]
            Note: published is in original RSS format (RFC 2822), NOT UTC normalized.

        Raises:
            requests.RequestException: If RSS feed request fails after retries.

        Example:
            >>> collector = FedCollector()
            >>> df = collector.fetch_publications(start_date=datetime(2024, 1, 1))
            >>> print(df.columns)
            Index(['title', 'link', 'published', 'summary', 'source'])
        """
        start = start_date or datetime.now() - timedelta(days=30)
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
            # Feed parsing had issues but might still have entries
            self.logger.warning(
                "RSS feed parsing warning: %s", feed.get("bozo_exception", "Unknown")
            )

        if not feed.entries:
            self.logger.warning("No entries found in RSS feed")
            return pd.DataFrame()

        self.logger.info("Parsed %d entries from RSS feed", len(feed.entries))

        # Extract and filter entries
        records = []
        for entry in feed.entries:
            try:
                # Parse published date
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    pub_date = datetime(*entry.published_parsed[:6])
                elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                    pub_date = datetime(*entry.updated_parsed[:6])
                else:
                    # No date available, skip
                    self.logger.debug(
                        "Skipping entry without date: %s", entry.get("title", "Unknown")
                    )
                    continue

                # Filter by date range
                if not (start <= pub_date <= end):
                    continue

                # Extract fields (preserve all RSS metadata)
                record = {
                    "title": entry.get("title", "").strip(),
                    "link": entry.get("link", "").strip(),
                    "published": entry.get(
                        "published", ""
                    ).strip(),  # Keep original RFC 2822 format
                    "summary": entry.get("summary", "").strip(),
                    "source": self.SOURCE_NAME,
                }

                records.append(record)

            except Exception as e:
                self.logger.warning("Error parsing entry: %s", e)
                continue

        if not records:
            self.logger.info(
                "No publications found in date range %s to %s", start.date(), end.date()
            )
            return pd.DataFrame()

        df = pd.DataFrame(records)
        self.logger.info("Filtered to %d publications in date range", len(df))

        return df

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
