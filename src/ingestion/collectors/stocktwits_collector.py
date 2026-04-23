"""Stocktwits Social Sentiment Collector - Bronze Layer (Raw Data Collection).

Collects retail trader social sentiment from the Stocktwits financial social network.
Stocktwits is unique because every post is tagged to a specific ticker and carries
an optional user-applied bullish/bearish label - making it pre-labeled sentiment data.

FX tickers collected: EURUSD, GBPUSD, USDCHF, USDJPY.

This collector handles only the Bronze layer:
- Fetches raw message stream from Stocktwits public REST API
- Preserves all source fields immutably
- Adds source="stocktwits" field
- Appends to data/raw/news/stocktwits/{symbol.lower()}_raw.jsonl
"""

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.ingestion.collectors.base_collector import BaseCollector
from src.shared.config import Config

try:
    from curl_cffi import requests as _cffi_requests  # type: ignore[import-untyped]

    _HAS_CURL_CFFI = True
except ImportError:  # pragma: no cover
    _HAS_CURL_CFFI = False


__all__ = ["StocktwitsCollector"]


class StocktwitsCollector(BaseCollector):
    """Collector for Stocktwits retail trader sentiment - Bronze Layer."""

    SOURCE_NAME = "stocktwits"
    API_BASE = "https://api.stocktwits.com/api/2"
    STREAM_ENDPOINT = "/streams/symbol/{symbol}.json"
    DEFAULT_SYMBOLS: list[str] = ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]
    MIN_REQUEST_INTERVAL: float = 1.5
    RETRY_BACKOFF_BASE_429: float = 60.0
    MAX_RETRIES: int = 3
    DEFAULT_LOOKBACK_DAYS: int = 30

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
        symbols: list[str] | None = None,
    ) -> None:
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "news" / "stocktwits",
            log_file=log_file or Config.LOGS_DIR / "collectors" / "stocktwits_collector.log",
        )
        self.symbols = symbols or self.DEFAULT_SYMBOLS
        self._session = self._build_session()
        self._last_request_time = 0.0
        self.logger.info(
            "StocktwitsCollector initialized - symbols=%s, output_dir=%s",
            self.symbols,
            self.output_dir,
        )

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        backfill: bool = False,
    ) -> dict[str, int]:
        """Collect raw Stocktwits messages for all configured FX symbols."""
        result: dict[str, int] = {}
        for symbol in self.symbols:
            self.logger.info("Collecting Stocktwits stream for %s", symbol)
            result[symbol.lower()] = self.collect_one(
                symbol,
                start_date=start_date,
                end_date=end_date,
                backfill=backfill,
            )
        return result

    def collect_one(
        self,
        symbol: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        backfill: bool = False,
    ) -> int:
        """Collect and append one symbol stream into its canonical JSONL file."""
        canonical_path = self._canonical_path(symbol)
        url = f"{self.API_BASE}{self.STREAM_ENDPOINT.format(symbol=symbol)}"
        timestamp_collected = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        seen_ids: set[int] = set()
        cursor_max: int | None = None
        written = 0

        if start_date is not None:
            stop_ts = self._to_utc(start_date)
            filter_ts = self._to_utc(end_date) if end_date is not None else None
            max_id: int | None = None
        elif backfill:
            max_id = self._get_max_id(symbol)
            if max_id is None:
                stop_ts = datetime.now(timezone.utc) - timedelta(days=self.DEFAULT_LOOKBACK_DAYS)
            else:
                stop_ts = None
            filter_ts = None
        else:
            stop_ts = datetime.now(timezone.utc) - timedelta(days=self.DEFAULT_LOOKBACK_DAYS)
            filter_ts = None
            max_id = None

        with canonical_path.open("a", encoding="utf-8") as file_handle:
            while True:
                params: dict[str, int] = {}
                if cursor_max is not None:
                    params["max"] = cursor_max

                response_data = self._request(url, params)
                if response_data is None:
                    break

                messages = response_data.get("messages", [])
                if not messages:
                    break

                stop_paging = False
                oldest_id: int | None = None

                for msg in messages:
                    msg_id_raw = msg.get("id")
                    try:
                        msg_id = int(msg_id_raw)
                    except (TypeError, ValueError):
                        self.logger.warning(
                            "Skipping message with invalid id for %s: %r",
                            symbol,
                            msg_id_raw,
                        )
                        continue

                    if oldest_id is None or msg_id < oldest_id:
                        oldest_id = msg_id

                    if msg_id in seen_ids:
                        continue

                    if max_id is not None and msg_id <= max_id:
                        stop_paging = True
                        break

                    published = self._parse_timestamp(msg.get("created_at"), symbol)
                    if published is None:
                        continue

                    if stop_ts is not None and published < stop_ts:
                        stop_paging = True
                        break

                    if filter_ts is not None and published > filter_ts:
                        continue

                    record = self._parse_message(msg, symbol, timestamp_collected)
                    file_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                    seen_ids.add(msg_id)
                    written += 1

                file_handle.flush()

                cursor_info = response_data.get("cursor", {})
                if stop_paging or not cursor_info.get("more", False) or oldest_id is None:
                    break

                cursor_max = oldest_id

        return written

    def health_check(self) -> bool:
        """Verify the Stocktwits API is reachable."""
        try:
            url = f"{self.API_BASE}{self.STREAM_ENDPOINT.format(symbol='EURUSD')}"
            response = self._session.get(url, timeout=10, params={"limit": 1})
            return bool(response.status_code == 200)
        except requests.RequestException:
            return False

    def _canonical_path(self, symbol: str) -> Path:
        return self.output_dir / f"{symbol.lower()}_raw.jsonl"

    def _get_max_id(self, symbol: str) -> int | None:
        path = self._canonical_path(symbol)
        if not path.exists():
            return None

        max_id: int | None = None
        has_valid_record = False
        with path.open("r", encoding="utf-8") as file_handle:
            for line_number, line in enumerate(file_handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    item = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    self.logger.warning(
                        "Skipping malformed JSON in %s line=%d: %s",
                        path,
                        line_number,
                        exc,
                    )
                    continue

                if not isinstance(item, dict):
                    self.logger.warning("Skipping non-object JSON in %s line=%d", path, line_number)
                    continue

                message_id = item.get("message_id")
                try:
                    message_id_int = int(message_id)
                except (TypeError, ValueError):
                    self.logger.warning(
                        "Skipping line without valid message_id in %s line=%d",
                        path,
                        line_number,
                    )
                    continue

                has_valid_record = True
                if max_id is None or message_id_int > max_id:
                    max_id = message_id_int

        if not has_valid_record:
            return None
        return max_id

    def _parse_message(self, msg: dict, symbol: str, timestamp_collected: str) -> dict:
        """Parse a raw Stocktwits API message dict into a Bronze record."""
        user = msg.get("user") or {}
        entities = msg.get("entities") or {}
        sentiment_info = entities.get("sentiment") or {}
        sentiment = sentiment_info.get("basic")

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

    def _parse_timestamp(self, timestamp_value: object, symbol: str) -> datetime | None:
        if not isinstance(timestamp_value, str) or not timestamp_value:
            self.logger.warning(
                "Skipping message with invalid created_at for %s: %r",
                symbol,
                timestamp_value,
            )
            return None

        try:
            parsed = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))
        except ValueError:
            self.logger.warning(
                "Skipping message with unparseable created_at for %s: %r",
                symbol,
                timestamp_value,
            )
            return None

        return self._to_utc(parsed)

    def _request(self, url: str, params: dict[str, int]) -> dict | None:
        """Execute a rate-limited GET request with retry on 429."""
        self._throttle()

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = self._session.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)

                if response.status_code == 429:
                    backoff = min(self.RETRY_BACKOFF_BASE_429 * (2 ** (attempt - 1)), 300.0)
                    self.logger.warning(
                        "Rate limited (429). Backing off for %.0fs (attempt %d/%d).",
                        backoff,
                        attempt,
                        self.MAX_RETRIES,
                    )
                    time.sleep(backoff)
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

            except (requests.RequestException, Exception) as exc:
                self.logger.warning(
                    "Request error (attempt %d/%d): %s",
                    attempt,
                    self.MAX_RETRIES,
                    exc,
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
        """Build an HTTP session that can reach the Stocktwits API."""
        if _HAS_CURL_CFFI:
            self.logger.info(
                "Using curl_cffi (impersonate=chrome110) to bypass Cloudflare on Stocktwits API."
            )
            session = _cffi_requests.Session(impersonate="chrome110")
            session.headers.update({"Accept": "application/json"})
            return session  # type: ignore[return-value]

        self.logger.warning(
            "curl_cffi not available - falling back to requests.Session. "
            "Stocktwits API may return 403 (Cloudflare block)."
        )
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
