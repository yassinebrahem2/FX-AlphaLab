"""
Central Bank RSS Fetcher (RAW)
STEP 5 – Policy Source Data

Fetches official central bank document metadata from RSS feeds.
No document body fetching.
No parsing, no NLP, no interpretation.
"""

from datetime import datetime, timezone
from typing import Dict, List
import hashlib

import feedparser
import pandas as pd


# -------------------------------------------------------------------
# Official RSS feeds
# -------------------------------------------------------------------

RSS_FEEDS: Dict[str, str] = {
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "ECB": "https://www.ecb.europa.eu/rss/press.xml",
    "BOE": "https://www.bankofengland.co.uk/rss/news",
    "BOJ": "https://www.boj.or.jp/en/rss/whatsnew.xml",
}


# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------

def fetch_rss_documents() -> pd.DataFrame:
    """
    Fetch raw central bank document metadata from official RSS feeds.

    Returns:
        pd.DataFrame with raw, unvalidated document records.
    """
    records: List[dict] = []
    ingested_at = datetime.now(timezone.utc)

    for institution, feed_url in RSS_FEEDS.items():
        feed = feedparser.parse(feed_url)

        for entry in feed.entries:
            published_dt = _parse_published_datetime(entry)
            url = entry.get("link")

            document_id = _build_document_id(
                institution=institution,
                document_type="press_release",
                published_dt=published_dt,
                url=url,
            )

            records.append({
                "document_id": document_id,
                "institution": institution,
                "document_type": "press_release",
                "title": entry.get("title"),
                "publication_datetime_utc": published_dt,
                "url": url,
                "raw_text": None,  # document bodies fetched later
                "source": "official_website",
                "ingested_at_utc": ingested_at,
            })

    return pd.DataFrame(records)


# -------------------------------------------------------------------
# Internal helpers
# -------------------------------------------------------------------

def _parse_published_datetime(entry) -> datetime | None:
    """
    Parse publication datetime from RSS entry.

    Returns UTC datetime or None if unavailable.
    """
    published = entry.get("published_parsed")
    if not published:
        return None

    return datetime(*published[:6], tzinfo=timezone.utc)


def _build_document_id(
    institution: str,
    document_type: str,
    published_dt: datetime | None,
    url: str | None,
) -> str:
    """
    Build a deterministic document identifier.

    Uses a hash to ensure:
    - idempotent ingestion
    - stability across runs
    - independence from URL structure
    """
    base = f"{institution}|{document_type}|{published_dt}|{url}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()
