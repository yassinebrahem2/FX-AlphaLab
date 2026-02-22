"""Stocktwits Social Sentiment Collector - Bronze Layer (Raw Data Collection).

Collects retail trader social sentiment from the Stocktwits financial social network.
Stocktwits is unique because every post is tagged to a specific ticker AND carries
an optional user-applied bullish/bearish label — making it pre-labeled sentiment data.

FX tickers collected: EURUSD, GBPUSD, USDJPY

This collector handles ONLY the Bronze layer (§3.1):
- Fetches raw message stream from Stocktwits public REST API
- Preserves all source fields immutably
- Adds `source="stocktwits"` field
- Exports to data/raw/news/stocktwits/ in JSONL format
- No sentiment inference — uses user-applied labels directly

For Silver layer transformation (schema normalization, ratio computation),
see the notebook notebooks/04e_stocktwits.ipynb.

API Reference:
    Base URL: https://api.stocktwits.com/api/2
    Streams:  GET /streams/symbol/{symbol}.json
    Pagination: ?max={message_id} (returns messages older than given ID)
    Rate limit: ~200 requests/hour (unauthenticated)
    No authentication required for public symbol streams.

Bronze JSONL Schema (one record per message):
    source              : str  — always "stocktwits"
    timestamp_collected : str  — ISO 8601 UTC when collected
    timestamp_published : str  — ISO 8601 UTC when posted (from Stocktwits)
    message_id          : int  — Stocktwits unique message ID
    symbol              : str  — FX ticker (e.g. "EURUSD")
    body                : str  — raw message text
    sentiment           : str | None — "Bullish", "Bearish", or null
    username            : str  — Stocktwits username
    user_id             : int  — Stocktwits user ID
    followers_count     : int  — poster's follower count (credibility signal)
    url                 : str  — direct link to the message

Example:
    >>> from pathlib import Path
    >>> from datetime import datetime, timedelta
    >>> from src.ingestion.collectors.stocktwits_collector import StocktwitsCollector
    >>>
    >>> collector = StocktwitsCollector()
    >>> data = collector.collect(pages_per_symbol=5)
    >>> for dataset_name, docs in data.items():
    ...     path = collector.export_jsonl(docs, dataset_name)
"""

import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.ingestion.collectors.document_collector import DocumentCollector
from src.shared.config import Config


class StocktwitsCollector(DocumentCollector):
    """Collector for Stocktwits retail trader sentiment - Bronze Layer.

    Fetches public message streams for FX tickers from the Stocktwits REST API.
    Each message carries an optional user-applied Bullish/Bearish label, making
    this the only pre-labeled social sentiment source available without NLP inference.

    Raw data stored in data/raw/news/stocktwits/ following §3.1 Bronze contract.

    Features:
        - Pagination via cursor-based `max` parameter
        - Automatic rate limiting (1.5s between requests)
        - Exponential backoff on 429 / 5xx responses
        - Deduplication by message_id within a single run
        - Configurable pages per symbol and symbol list

    Rate Limits (unauthenticated):
        ~200 requests/hour. Collector enforces MIN_REQUEST_INTERVAL to stay safe.
        On 429 response, backs off for RETRY_BACKOFF_ON_429 seconds.
    """

    SOURCE_NAME = "stocktwits"

    API_BASE = "https://api.stocktwits.com/api/2"
    STREAM_ENDPOINT = "/streams/symbol/{symbol}.json"

    # FX pairs with active Stocktwits streams
    DEFAULT_SYMBOLS: list[str] = ["EURUSD", "GBPUSD", "USDJPY"]

    # Rate limiting
    MIN_REQUEST_INTERVAL: float = 1.5  # seconds between requests
    RETRY_BACKOFF_ON_429: float = 60.0  # seconds to wait on rate-limit response
    MAX_RETRIES: int = 3

    # Pagination
    DEFAULT_PAGES_PER_SYMBOL: int = 10  # 10 pages × 30 messages = up to 300 per symbol

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
        symbols: list[str] | None = None,
    ) -> None:
        """Initialize the Stocktwits collector.

        Args:
            output_dir: Directory for Bronze JSONL exports (default: data/raw/news/stocktwits).
            log_file: Optional path for file-based logging.
            symbols: FX ticker symbols to collect (default: EURUSD, GBPUSD, USDJPY).
        """
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "news" / "stocktwits",
            log_file=log_file or Config.LOGS_DIR / "collectors" / "stocktwits_collector.log",
        )
        self.symbols = symbols or self.DEFAULT_SYMBOLS
        self._session = self._build_session()
        self._last_request_time: float = 0.0
        self.logger.info(
            "StocktwitsCollector initialized — symbols=%s, output_dir=%s",
            self.symbols,
            self.output_dir,
        )

    # ------------------------------------------------------------------
    # DocumentCollector interface
    # ------------------------------------------------------------------

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        pages_per_symbol: int | None = None,
    ) -> dict[str, list[dict]]:
        """Collect raw Stocktwits messages for all configured FX symbols.

        Note: The Stocktwits public API does not support historical date-range
        queries. `start_date` / `end_date` are used as post-collection filters
        only. Pagination depth is controlled via `pages_per_symbol`.

        Args:
            start_date: Discard messages published before this date (optional).
            end_date: Discard messages published after this date (optional).
            pages_per_symbol: Number of API pages to fetch per symbol.
                              Each page returns up to 30 messages.
                              Defaults to DEFAULT_PAGES_PER_SYMBOL.

        Returns:
            Mapping of symbol (lowercase) to list of Bronze message dicts.
            E.g. {"eurusd": [...], "gbpusd": [...], "usdjpy": [...]}
        """
        pages = pages_per_symbol or self.DEFAULT_PAGES_PER_SYMBOL
        result: dict[str, list[dict]] = {}

        for symbol in self.symbols:
            self.logger.info("Collecting Stocktwits stream for %s (%d pages)", symbol, pages)
            messages = self._collect_symbol(symbol, pages, start_date, end_date)
            result[symbol.lower()] = messages
            self.logger.info("Collected %d messages for %s", len(messages), symbol)

        total = sum(len(v) for v in result.values())
        self.logger.info(
            "Collection complete — %d total messages across %d symbols", total, len(self.symbols)
        )
        return result

    def health_check(self) -> bool:
        """Verify the Stocktwits API is reachable.

        Makes a lightweight request for a single message from EURUSD stream.

        Returns:
            True if the API responds with HTTP 200, False otherwise.
        """
        try:
            url = f"{self.API_BASE}{self.STREAM_ENDPOINT.format(symbol='EURUSD')}"
            response = self._session.get(url, timeout=10, params={"limit": 1})
            return bool(response.status_code == 200)
        except requests.RequestException:
            return False

    # ------------------------------------------------------------------
    # Internal collection logic
    # ------------------------------------------------------------------

    def _collect_symbol(
        self,
        symbol: str,
        pages: int,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[dict]:
        """Paginate through a symbol's Stocktwits stream.

        Args:
            symbol: FX ticker (e.g. "EURUSD").
            pages: Maximum number of API pages to fetch.
            start_date: Optional lower bound filter on published timestamp.
            end_date: Optional upper bound filter on published timestamp.

        Returns:
            List of Bronze message dicts for the symbol.
        """
        url = f"{self.API_BASE}{self.STREAM_ENDPOINT.format(symbol=symbol)}"
        collected: list[dict] = []
        seen_ids: set[int] = set()
        cursor_max: int | None = None
        timestamp_collected = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for page_num in range(1, pages + 1):
            params: dict[str, int | str] = {}
            if cursor_max is not None:
                params["max"] = cursor_max

            response_data = self._request(url, params)
            if response_data is None:
                self.logger.warning("Page %d for %s returned no data, stopping.", page_num, symbol)
                break

            messages = response_data.get("messages", [])
            if not messages:
                self.logger.info("No more messages for %s at page %d.", symbol, page_num)
                break

            stop_early = False
            for msg in messages:
                msg_id = msg.get("id")
                if msg_id in seen_ids:
                    continue
                seen_ids.add(msg_id)

                record = self._parse_message(msg, symbol, timestamp_collected)

                # Apply date filters
                published = datetime.fromisoformat(
                    record["timestamp_published"].replace("Z", "+00:00")
                )
                if start_date is not None and start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=timezone.utc)
                if end_date is not None and end_date.tzinfo is None:
                    end_date = end_date.replace(tzinfo=timezone.utc)

                if start_date and published < start_date:
                    stop_early = True
                    continue
                if end_date and published > end_date:
                    continue

                collected.append(record)

            # Advance cursor to oldest message ID on this page
            if messages:
                cursor_max = min(msg["id"] for msg in messages if "id" in msg)

            # Check if API indicates more pages exist
            cursor_info = response_data.get("cursor", {})
            if not cursor_info.get("more", False):
                self.logger.info("Stocktwits cursor exhausted for %s at page %d.", symbol, page_num)
                break

            if stop_early:
                self.logger.info("Reached start_date boundary for %s at page %d.", symbol, page_num)
                break

            self.logger.debug(
                "Page %d/%d for %s: +%d messages", page_num, pages, symbol, len(messages)
            )

        return collected

    def _parse_message(self, msg: dict, symbol: str, timestamp_collected: str) -> dict:
        """Parse a raw Stocktwits API message dict into a Bronze record.

        Args:
            msg: Raw message dict from Stocktwits API.
            symbol: FX ticker this stream belongs to.
            timestamp_collected: ISO 8601 UTC collection timestamp.

        Returns:
            Bronze-compliant record dict.
        """
        user = msg.get("user") or {}
        entities = msg.get("entities") or {}
        sentiment_info = entities.get("sentiment") or {}
        sentiment = sentiment_info.get("basic")  # "Bullish", "Bearish", or None

        msg_id = msg.get("id", 0)
        username = user.get("username", "")

        return {
            "source": self.SOURCE_NAME,
            "timestamp_collected": timestamp_collected,
            "timestamp_published": msg.get("created_at", ""),
            "message_id": msg_id,
            "symbol": symbol.upper(),
            "body": msg.get("body", ""),
            "sentiment": sentiment,
            "username": username,
            "user_id": user.get("id"),
            "followers_count": user.get("followers", 0),
            "url": f"https://stocktwits.com/{username}/message/{msg_id}",
        }

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _request(self, url: str, params: dict) -> dict | None:
        """Execute a rate-limited GET request with retry on 429.

        Args:
            url: Fully qualified API URL.
            params: Query parameters.

        Returns:
            Parsed JSON dict, or None if all retries failed.
        """
        self._throttle()

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = self._session.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)

                if response.status_code == 429:
                    self.logger.warning(
                        "Rate limited (429). Backing off for %.0fs (attempt %d/%d).",
                        self.RETRY_BACKOFF_ON_429,
                        attempt,
                        self.MAX_RETRIES,
                    )
                    time.sleep(self.RETRY_BACKOFF_ON_429)
                    continue

                if response.status_code == 200:
                    return dict(response.json())

                self.logger.warning(
                    "Unexpected HTTP %d from %s (attempt %d/%d).",
                    response.status_code,
                    url,
                    attempt,
                    self.MAX_RETRIES,
                )
                time.sleep(2**attempt)

            except requests.RequestException as exc:
                self.logger.warning(
                    "Request error (attempt %d/%d): %s", attempt, self.MAX_RETRIES, exc
                )
                time.sleep(2**attempt)

        return None

    def _throttle(self) -> None:
        """Enforce minimum interval between consecutive requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.MIN_REQUEST_INTERVAL:
            time.sleep(self.MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def _build_session(self) -> requests.Session:
        """Build a requests Session with retry adapter for transient errors."""
        session = requests.Session()
        retry = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=1.0,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; FX-AlphaLab/0.1; research)",
                "Accept": "application/json",
            }
        )
        return session
