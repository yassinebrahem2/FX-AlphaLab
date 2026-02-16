"""Shared ECB utilities for news collection.

Provides common constants, document classification, speaker extraction,
content fetching, and session creation used by both ECBNewsCollector (RSS)
and ECBScraperCollector (Selenium-based historical backfill).
"""

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Valid document type keys (used for JSONL file naming and routing).
VALID_DOCUMENT_TYPES: set[str] = {"pressreleases", "speeches", "policy", "bulletins"}

#: Maps internal category key → Silver schema ``document_type`` value.
DOCUMENT_TYPE_FIELD_MAP: dict[str, str] = {
    "pressreleases": "press_release",
    "speeches": "speech",
    "policy": "policy_decision",
    "bulletins": "bulletin",
}

#: Regex patterns for classifying documents by title/summary text.
#: Checked in priority order (policy → speeches → bulletins → pressreleases).
DOCUMENT_TYPE_PATTERNS: dict[str, list[str]] = {
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
    "pressreleases": [],  # Default catch-all
}

#: Priority-ordered CSS selectors for extracting main content from ECB pages.
CONTENT_SELECTORS: list[str] = [
    "div.content",
    "div.article",
    "div.body",
    "article",
    "main",
]

#: Regex fallback for ``id``-based content areas.
CONTENT_ID_PATTERN = re.compile(r"content|article")


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


def classify_document_type(title: str, summary: str = "") -> str:
    """Classify an ECB document into a category based on title/summary text.

    Args:
        title: Document title.
        summary: Optional summary or description text.

    Returns:
        Category key: ``"policy"``, ``"speeches"``, ``"bulletins"``, or
        ``"pressreleases"`` (default).
    """
    text = (title + " " + summary).lower()

    for doc_type, patterns in DOCUMENT_TYPE_PATTERNS.items():
        if doc_type == "pressreleases":
            continue  # catch-all, skip
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return doc_type

    return "pressreleases"


def extract_speaker_name(title: str, content: str = "") -> str | None:
    """Extract a speaker name from the title or first 500 chars of content.

    Args:
        title: Document title.
        content: Optional document body.

    Returns:
        Speaker name or ``None``.
    """
    patterns = [
        r"(?:speech|remarks|interview|statement)\s+by\s+"
        r"([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)"
        r"(?:\s+at|\s+in|\s+to|\s+for|\s+on|,|$)",
        r"([A-Z][a-z]+\s+[A-Z][a-z]+)" r"(?:\s*,\s*(?:President|Vice-President|Member|Chief))",
    ]

    text = title + " " + content[:500]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            name = re.sub(r"\s+", " ", match.group(1).strip())
            if 3 < len(name) < 50:
                return name

    return None


def fetch_full_content(
    session: requests.Session,
    url: str,
    logger: logging.Logger,
    throttle_fn: Callable[[], None],
    timeout: int = 30,
) -> str:
    """Fetch and extract main text content from an ECB page.

    Uses BeautifulSoup to strip navigation/scripts and locate the main
    content area via priority-ordered CSS selectors.

    Args:
        session: Configured :class:`requests.Session` with retry logic.
        url: Page URL to fetch.
        logger: Logger instance for warnings.
        throttle_fn: Callable that enforces rate-limiting delay.
        timeout: HTTP request timeout in seconds.

    Returns:
        Extracted text or empty string on failure.
    """
    if not url:
        return ""

    try:
        throttle_fn()
        response = session.get(url, timeout=timeout)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Remove unwanted elements
        for element in soup(["script", "style", "nav", "header", "footer"]):
            element.decompose()

        # Try priority selectors
        content_areas = [
            soup.find("div", class_=re.compile(r"content|article|body")),
            soup.find("article"),
            soup.find("main"),
            soup.find("div", id=CONTENT_ID_PATTERN),
        ]

        for area in content_areas:
            if area:
                text = area.get_text(separator="\n", strip=True)
                if len(text) > 100:
                    return text

        # Fallback to body
        body = soup.find("body")
        if body:
            return body.get_text(separator="\n", strip=True)

        return ""

    except Exception as e:
        logger.warning("Failed to fetch content from %s: %s", url, e)
        return ""


def create_ecb_session(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
) -> requests.Session:
    """Create a :class:`requests.Session` with exponential-backoff retry.

    Args:
        max_retries: Maximum retry attempts per request.
        backoff_factor: Multiplier for retry delay (1s, 2s, 4s …).

    Returns:
        Configured session.
    """
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
