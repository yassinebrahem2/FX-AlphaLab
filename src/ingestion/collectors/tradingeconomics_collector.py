"""Trading Economics Calendar Collector - Bronze Layer (Raw Data Collection).

Fetches economic calendar events from Trading Economics and normalizes them
into the existing Module C bronze schema used by the calendar preprocessor.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin

import pandas as pd
import requests

from src.ingestion.collectors.base_collector import BaseCollector
from src.shared.config import Config


class TradingEconomicsCalendarCollector(BaseCollector):
    """Collect Trading Economics calendar events in the existing bronze schema."""

    SOURCE_NAME = "tradingeconomics"
    BASE_URL = "https://api.tradingeconomics.com"
    EVENT_BASE_URL = "https://tradingeconomics.com"

    DEFAULT_TIMEOUT = 30
    DEFAULT_MAX_RETRIES = 2
    DEFAULT_BACKOFF_SECONDS = 1.5

    COUNTRY_TO_CURRENCY = {
        "australia": "AUD",
        "brazil": "BRL",
        "canada": "CAD",
        "china": "CNY",
        "euro area": "EUR",
        "euro zone": "EUR",
        "eurozone": "EUR",
        "europe": "EUR",
        "european union": "EUR",
        "european monetary union": "EUR",
        "france": "EUR",
        "germany": "EUR",
        "italy": "EUR",
        "japan": "JPY",
        "new zealand": "NZD",
        "singapore": "SGD",
        "switzerland": "CHF",
        "united kingdom": "GBP",
        "united states": "USD",
        "spain": "EUR",
        "netherlands": "EUR",
        "belgium": "EUR",
        "austria": "EUR",
        "portugal": "EUR",
        "ireland": "EUR",
        "finland": "EUR",
        "greece": "EUR",
    }

    IMPACT_MAP = {1: "low", 2: "medium", 3: "high"}

    def __init__(
        self,
        api_key: str | None = None,
        output_dir: Path | None = None,
        log_file: Path | None = None,
        base_url: str = BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        session: requests.Session | None = None,
    ) -> None:
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / self.SOURCE_NAME,
            log_file=log_file or Config.LOGS_DIR / "collectors" / "tradingeconomics_collector.log",
        )
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.api_key = self._resolve_api_key(api_key)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "FX-AlphaLab/ModuleC TradingEconomicsCollector",
            }
        )
        self.logger.info(
            "TradingEconomicsCalendarCollector initialized (timeout=%s, retries=%s)",
            self.timeout,
            self.max_retries,
        )

    def _resolve_api_key(self, api_key: str | None) -> str:
        candidate = (api_key or Config.TRADINGECONOMICS_API_KEY or "guest:guest").strip()
        if not candidate or candidate.lower() in {"your_tradingeconomics_api_key_here", "your_api_key_here"}:
            candidate = "guest:guest"
        return candidate

    def _build_request_url(self, start_date: datetime, end_date: datetime) -> str:
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        return (
            f"{self.base_url}/calendar/country/All/{start_str}/{end_str}"
            f"?c={quote(self.api_key, safe=':')}&f=json"
        )

    def _month_ranges(self, start_date: datetime, end_date: datetime) -> list[tuple[datetime, datetime]]:
        ranges: list[tuple[datetime, datetime]] = []
        current = datetime(start_date.year, start_date.month, 1)
        while current <= end_date:
            next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
            chunk_end = min(end_date, next_month - timedelta(days=1))
            chunk_start = max(start_date, current)
            ranges.append((chunk_start, chunk_end))
            current = next_month
        return ranges

    def _clean_value(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text in {"-", "N/A", "NA", "nan", "None"}:
            return None
        return text

    def _map_country_to_currency(self, country: Any, url_path: Any = None, ticker: Any = None) -> str:
        country_text = str(country or "").strip().lower()
        if country_text in self.COUNTRY_TO_CURRENCY:
            return self.COUNTRY_TO_CURRENCY[country_text]

        if url_path:
            path = str(url_path).lower()
            if "united-kingdom" in path:
                return "GBP"
            if "united-states" in path:
                return "USD"
            if "switzerland" in path:
                return "CHF"
            if "japan" in path:
                return "JPY"
            if any(token in path for token in ["euro-zone", "euro-area", "european-union"]):
                return "EUR"
            if any(token in path for token in ["germany", "france", "italy", "spain", "netherlands", "belgium", "austria", "ireland", "finland", "greece", "portugal"]):
                return "EUR"

        ticker_text = str(ticker or "").upper().strip()
        if ticker_text.startswith("GBP"):
            return "GBP"
        if ticker_text.startswith("USD"):
            return "USD"
        if ticker_text.startswith("JPY"):
            return "JPY"
        if ticker_text.startswith("CHF"):
            return "CHF"
        if ticker_text.startswith("EUR"):
            return "EUR"
        return ""

    def _impact_from_importance(self, importance: Any) -> str:
        try:
            importance_int = int(float(importance))
        except (TypeError, ValueError):
            return "unknown"
        return self.IMPACT_MAP.get(importance_int, "unknown")

    def _request_json(self, url: str) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, list):
                    return payload
                if isinstance(payload, dict):
                    for key in ("calendar", "Calendar", "data", "items", "result"):
                        value = payload.get(key)
                        if isinstance(value, list):
                            return value
                self.logger.warning("Unexpected Trading Economics payload type: %s", type(payload).__name__)
                return []
            except Exception as exc:  # pragma: no cover - retry path
                last_error = exc
                if attempt < self.max_retries:
                    sleep_for = self.DEFAULT_BACKOFF_SECONDS * (attempt + 1)
                    self.logger.warning(
                        "Trading Economics request failed (attempt %s/%s): %s",
                        attempt + 1,
                        self.max_retries + 1,
                        exc,
                    )
                    time.sleep(sleep_for)
                    continue
                break

        if last_error is not None:
            raise last_error
        return []

    def _normalize_record(self, record: dict[str, Any], request_url: str) -> dict[str, Any] | None:
        timestamp = pd.to_datetime(record.get("Date"), errors="coerce", utc=True)
        if pd.isna(timestamp):
            return None

        country = record.get("Country") or ""
        currency = self._map_country_to_currency(country, record.get("URL"), record.get("Ticker"))
        event_name = self._clean_value(record.get("Event") or record.get("Category"))
        if not event_name:
            return None

        url_path = self._clean_value(record.get("URL"))
        event_url = urljoin(self.EVENT_BASE_URL, url_path) if url_path else None

        return {
            "date": timestamp.strftime("%Y-%m-%d"),
            "time": timestamp.strftime("%H:%M"),
            "currency": currency,
            "event": event_name,
            "impact": self._impact_from_importance(record.get("Importance")),
            "actual": self._clean_value(record.get("Actual")),
            "forecast": self._clean_value(record.get("Forecast")),
            "previous": self._clean_value(record.get("Previous")),
            "event_url": event_url,
            "source_url": request_url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source": self.SOURCE_NAME,
        }

    def _fetch_range(self, start_date: datetime, end_date: datetime) -> list[dict[str, Any]]:
        request_url = self._build_request_url(start_date, end_date)
        self.logger.info("Fetching Trading Economics calendar: %s -> %s", start_date.date(), end_date.date())
        payload = self._request_json(request_url)
        self.logger.info("Received %d raw Trading Economics records", len(payload))

        normalized: list[dict[str, Any]] = []
        for record in payload:
            if not isinstance(record, dict):
                continue
            event = self._normalize_record(record, request_url)
            if event is not None:
                normalized.append(event)

        self.logger.info(
            "Normalized %d Trading Economics rows for %s -> %s",
            len(normalized),
            start_date.date(),
            end_date.date(),
        )
        return normalized

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        if start_date is None:
            start_date = datetime.utcnow() - timedelta(days=30)
        if end_date is None:
            end_date = datetime.utcnow()

        if start_date > end_date:
            raise ValueError("start_date must be <= end_date")

        all_rows: list[dict[str, Any]] = []
        for chunk_start, chunk_end in self._month_ranges(start_date, end_date):
            all_rows.extend(self._fetch_range(chunk_start, chunk_end))

        if not all_rows:
            self.logger.warning("No Trading Economics rows collected")
            return {}

        df = pd.DataFrame(all_rows)
        self.logger.info("Trading Economics collection complete: %d rows", len(df))
        return {"calendar": df}

    def health_check(self) -> bool:
        try:
            today = datetime.now(timezone.utc).date()
            data = self.collect(
                start_date=datetime.combine(today, datetime.min.time()),
                end_date=datetime.combine(today, datetime.min.time()),
            )
            return bool(data.get("calendar") is not None and not data["calendar"].empty)
        except Exception as exc:
            self.logger.warning("Trading Economics health check failed: %s", exc)
            return False