"""
Bank of England (BoE) News Collector.

Collects press releases, MPC statements, speeches, and policy decisions from:
- RSS feed: https://www.bankofengland.co.uk/rss/news
- Official pages linked from RSS entries

Bronze Layer output (immutable raw JSONL):
data/raw/news/boe/{document_type}_{YYYYMMDD}.jsonl

Each line schema:
{
  "source": "BoE",
  "timestamp_collected": "...",
  "timestamp_published": "...",
  "url": "...",
  "title": "...",
  "content": "...",
  "document_type": "...",
  "speaker": "...",
  "metadata": {...}
}
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.ingestion.collectors.document_collector import DocumentCollector
from src.shared.config import Config


class BoECollector(DocumentCollector):
    SOURCE_NAME = "boe"

    RSS_URLS = [
        "https://www.bankofengland.co.uk/RSS/News",
        "https://www.bankofengland.co.uk/rss/news",
        "https://www.bankofengland.co.uk/rss/speeches",
        "https://www.bankofengland.co.uk/rss/prudential-regulation-publications",
    ]

    DEFAULT_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0
    REQUEST_DELAY = 1.0  # politeness delay

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        log_path = log_file or (Config.LOGS_DIR / "collectors" / "boe_collector.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "news" / "boe",
            log_file=log_path,
        )
        self._session = self._create_session()
        self.logger.info("BoECollector initialized, output_dir=%s", self.output_dir)

    def _to_utc_iso(self, published: str | None, published_parsed: Any | None) -> str | None:
        """
        Convert RSS published date to UTC ISO-8601 string.
        Uses feedparser's published_parsed (time.struct_time) when available.
        """
        from email.utils import parsedate_to_datetime

        if published_parsed:
            dt = datetime(
                published_parsed.tm_year,
                published_parsed.tm_mon,
                published_parsed.tm_mday,
                published_parsed.tm_hour,
                published_parsed.tm_min,
                published_parsed.tm_sec,
                tzinfo=timezone.utc,
            )
            return dt.isoformat()
        if published:
            try:
                dt = parsedate_to_datetime(published)

                # Ensure UTC aware
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)

                return dt.isoformat()

            except Exception:
                return None
        return None

    def _classify_document_type(self, url: str, title: str | None = None) -> tuple[str, str]:
        u = (url or "").lower()
        t = (title or "").lower()

        # Speeches
        if "/speech/" in u or "/speeches/" in u:
            return "speeches", "boe_speech"

        # Monetary Policy Summary
        if "monetary-policy-summary" in u or "monetary-policy-summary-and-minutes" in u:
            return "summaries", "monetary_policy_summary"

        # MPC
        if (
            "/monetary-policy-committee/" in u
            or "/mpc/" in u
            or t.startswith("mpc")
            or "monetary policy committee" in t
        ):
            return "mpc", "mpc_statement"

        # Default
        return "statements", "press_release"

    def _fetch_article_text(self, url: str) -> str:
        from bs4 import BeautifulSoup

        headers = {"User-Agent": "Mozilla/5.0"}
        resp = self._session.get(
            url, headers=headers, timeout=self.DEFAULT_TIMEOUT, allow_redirects=True
        )

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Attach status code for better upstream metadata
            raise RuntimeError(f"HTTP {resp.status_code} for {url}") from e

        resp.encoding = resp.apparent_encoding

        # Parse HTML and extract clean text
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove unwanted elements
        for element in soup(
            ["script", "style", "nav", "header", "footer", "aside", "form", "button"]
        ):
            element.decompose()

        # Remove cookie notice, navigation bypass, and other BoE-specific noise
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

        # Try BoE-specific content selectors (in priority order)
        content_selectors = [
            "div.page-content",  # Primary content div
            "section.page-section",  # Section container
            "div.accordion-content",  # Sometimes in accordion
            "article",  # Semantic HTML5
            "main#main-content",  # Main content area
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

        return text_content if text_content else resp.text

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, list[dict]]:
        """
        Collect BoE documents for a given date range (pure: no file I/O).

        Returns:
              Mapping of document_type -> list of raw records (Bronze contract).

        """
        from collections import defaultdict

        import feedparser

        results: dict[str, list[dict]] = defaultdict(list)

        # Default window: last 1 day if not provided
        now_utc = datetime.now(timezone.utc)
        if end_date is None:
            end_date = now_utc
        if start_date is None:
            start_date = end_date - pd.Timedelta(days=90)

        # Normalize to UTC aware
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        self.logger.info(
            "Collecting BoE documents from %s to %s",
            start_date.isoformat(),
            end_date.isoformat(),
        )

        headers = {"User-Agent": "Mozilla/5.0"}

        collected_count = 0
        seen_urls: set[str] = set()

        for feed_url in self.RSS_URLS:
            resp = self._session.get(feed_url, headers=headers, timeout=20, allow_redirects=True)
            resp.raise_for_status()

            feed = feedparser.parse(resp.text)
            if getattr(feed, "bozo", False):
                self.logger.warning(
                    "RSS bozo flag for %s: %s", feed_url, getattr(feed, "bozo_exception", None)
                )

            for entry in feed.entries:
                url = entry.get("link")
                if not url or url in seen_urls:
                    continue
                title = entry.get("title")
                published_iso = self._to_utc_iso(
                    entry.get("published"), entry.get("published_parsed")
                )

                if not url or not published_iso:
                    continue

                published_dt = datetime.fromisoformat(published_iso)

                # date filter (inclusive start, inclusive end)
                if published_dt < start_date or published_dt > end_date:
                    continue

                bucket, doc_type = self._classify_document_type(url, title)

                speaker = None
                if bucket == "speeches":
                    try:
                        speaker = self._extract_speaker_from_page(url)
                    except Exception:
                        speaker = None

                fetch_error = None
                fetch_error_type = None
                try:
                    content = self._fetch_article_text(url)
                except Exception as e:
                    fetch_error = str(e)
                    fetch_error_type = e.__class__.__name__
                    content = entry.get("summary") or ""
                    self.logger.info(
                        "Article fetch failed, falling back to RSS summary for %s", url
                    )

                record = {
                    "source": "BoE",
                    "timestamp_collected": now_utc.isoformat(),
                    "timestamp_published": published_iso,
                    "url": url,
                    "title": title,
                    "content": content,
                    "document_type": doc_type,
                    "speaker": speaker,
                    "metadata": {
                        "rss_feed": feed_url,
                        "rss_id": entry.get("id") or entry.get("guid"),
                        "rss_summary": entry.get("summary"),
                        "rss_tags": [
                            t.get("term") for t in entry.get("tags", []) if isinstance(t, dict)
                        ],
                        "fetch_error": fetch_error,
                        "fetch_error_type": fetch_error_type,
                    },
                }

                results[bucket].append(record)
                seen_urls.add(url)
                collected_count += 1
                time.sleep(self.REQUEST_DELAY)

        self.logger.info("Collected %d BoE documents", collected_count)
        return dict(results)

    def _extract_speaker_from_page(self, url: str) -> str | None:
        """
        Best-effort extraction of speaker name from a BoE speech page.
        Returns None if not found.
        """
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            resp = self._session.get(
                url, headers=headers, timeout=self.DEFAULT_TIMEOUT, allow_redirects=True
            )
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding
            soup = BeautifulSoup(resp.text, "html.parser")

            # 1) Try obvious meta author
            meta_author = soup.find("meta", attrs={"name": "author"})
            if meta_author and meta_author.get("content"):
                return meta_author["content"].strip()

            # 2) Many BoE pages have the title in h1; speaker often appears nearby.
            h1 = soup.find("h1")
            if h1:
                # Look in the next few text lines after the title
                container = h1.parent
                text_block = container.get_text("\n", strip=True) if container else ""
                lines = [ln.strip() for ln in text_block.splitlines() if ln.strip()]

                # Try to find "speech by NAME"
                for ln in lines[:25]:
                    low = ln.lower()
                    if "speech by " in low:
                        idx = low.find("speech by ")
                        return ln[idx + len("speech by ") :].strip()

            # 3) Fallback: scan first part of main content for "speech by X"
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
        except Exception:
            return None

    def health_check(self) -> bool:
        """
        Verify BoE RSS feed connectivity.

        Returns True if at least one RSS feed is reachable (HTTP 200)
        and contains at least 1 parsable entry.
        """
        import feedparser

        headers = {"User-Agent": "Mozilla/5.0"}

        for url in self.RSS_URLS:
            try:
                resp = self._session.get(url, headers=headers, timeout=10, allow_redirects=True)
                if not resp.ok:
                    self.logger.warning(
                        "Health check failed for %s: HTTP %s", url, resp.status_code
                    )
                    continue

                feed = feedparser.parse(resp.text)

                if getattr(feed, "bozo", False):
                    self.logger.warning(
                        "Health check RSS bozo for %s: %s",
                        url,
                        getattr(feed, "bozo_exception", None),
                    )

                if getattr(feed, "entries", None) and len(feed.entries) > 0:
                    self.logger.info("Health check OK for %s (entries=%d)", url, len(feed.entries))
                    return True

                self.logger.warning("Health check no entries for %s", url)

            except requests.exceptions.RequestException as e:
                self.logger.warning("Health check request error for %s: %s", url, e)

        return False

    # -----------------------
    # Helpers
    # -----------------------

    def _create_session(self) -> requests.Session:
        session = requests.Session()

        retry = Retry(
            total=self.MAX_RETRIES,
            connect=self.MAX_RETRIES,
            read=self.MAX_RETRIES,
            status=self.MAX_RETRIES,
            # Exponential backoff: 0, 2, 4, 8... seconds-ish (depends on urllib3)
            backoff_factor=self.RETRY_BACKOFF,
            # Retry on these HTTP status codes
            status_forcelist=(429, 500, 502, 503, 504),
            # Only retry GETs
            allowed_methods=frozenset(["GET"]),
            # Respect Retry-After header (important for 429 rate limit)
            respect_retry_after_header=True,
            # Retry even if status code is returned (not just exceptions)
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def _write_jsonl(self, path: Path, records: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            for obj in records:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    collector = BoECollector()
    data = collector.collect(
        start_date=datetime(2026, 2, 9, tzinfo=timezone.utc),
        end_date=datetime(2026, 2, 11, tzinfo=timezone.utc),
    )
    print(data.keys())
    paths = collector.export_all(data=data)
    print(paths)
