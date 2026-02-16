"""Shared Federal Reserve utilities for document collection.

Provides common constants, parsing, classification, and content extraction
used by both FedCollector (RSS) and FedScraperCollector (year-based archives).
"""

import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://www.federalreserve.gov"

# Category mapping from Fed HTML to standardized document types
CATEGORY_TO_DOCUMENT_TYPE: dict[str, str] = {
    "Monetary Policy": "policy",
    "Orders on Banking Applications": "regulation",
    "Enforcement Actions": "regulation",
    "Banking and Consumer Regulatory Policy": "regulation",
    "Other Announcements": "other",
}

# Valid document types for routing
VALID_DOCUMENT_TYPES: set[str] = {"policy", "regulation", "speeches", "other"}

# ---------------------------------------------------------------------------
# Date Parsing
# ---------------------------------------------------------------------------


def parse_date_from_text(date_str: str) -> datetime | None:
    """Parse MM/DD/YYYY format date string to datetime.

    Args:
        date_str: Date string in MM/DD/YYYY format (e.g., "12/30/2025").

    Returns:
        Parsed datetime object with time set to 00:00:00, or None if parsing fails.

    Example:
        >>> parse_date_from_text("12/30/2025")
        datetime.datetime(2025, 12, 30, 0, 0)
        >>> parse_date_from_text("invalid") is None
        True
    """
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Document Classification
# ---------------------------------------------------------------------------


def classify_document_type(category: str | None, is_speech: bool) -> str:
    """Map Fed category to standardized document type.

    Args:
        category: Category text from Fed HTML (e.g., "Monetary Policy").
        is_speech: Whether this is a speech document.

    Returns:
        Standardized document type key (policy|regulation|speeches|other).

    Example:
        >>> classify_document_type("Monetary Policy", False)
        'policy'
        >>> classify_document_type(None, True)
        'speeches'
        >>> classify_document_type("Unknown Category", False)
        'other'
    """
    if is_speech:
        return "speeches"

    if category:
        return CATEGORY_TO_DOCUMENT_TYPE.get(category, "other")

    return "other"


# ---------------------------------------------------------------------------
# Speaker Extraction
# ---------------------------------------------------------------------------


def extract_speaker_name(speaker_text: str | None) -> str | None:
    """Extract speaker name from speech metadata.

    Handles formats like:
        - "Governor Stephen I. Miran"
        - "Chair Jerome H. Powell"
        - "Vice Chair Philip N. Jefferson"

    Args:
        speaker_text: Raw speaker text from HTML.

    Returns:
        Extracted speaker name or None if not found.

    Example:
        >>> extract_speaker_name("Governor Stephen I. Miran")
        'Stephen I. Miran'
        >>> extract_speaker_name("Chair Jerome H. Powell")
        'Jerome H. Powell'
        >>> extract_speaker_name(None) is None
        True
    """
    if not speaker_text:
        return None

    # Remove title prefixes (Governor, Chair, Vice Chair, etc.)
    cleaned = speaker_text.strip()
    title_pattern = r"^(Governor|Chair|Vice Chair|President|Member)\s+"
    match = re.sub(title_pattern, "", cleaned, flags=re.IGNORECASE)

    return match.strip() if match else None


# ---------------------------------------------------------------------------
# Content Extraction
# ---------------------------------------------------------------------------


def fetch_full_content(url: str, session: requests.Session) -> str:
    """Fetch full article content from Fed URL.

    Args:
        url: Article URL (relative or absolute).
        session: Requests session with retry logic.

    Returns:
        Extracted text content from article.

    Raises:
        requests.HTTPError: If request fails after retries.
        requests.Timeout: If request times out.

    Example:
        >>> session = create_fed_session()
        >>> content = fetch_full_content("/newsevents/pressreleases/monetary20251230a.htm", session)
    """
    full_url = url if url.startswith("http") else BASE_URL + url

    response = session.get(full_url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")

    # Fed articles typically have main content in specific containers
    # Priority order: article, main, div.col-xs-12, body
    content_selectors = [
        "article",
        "main",
        "div.col-xs-12",
        "div#article",
        "div.row",
    ]

    for selector in content_selectors:
        element = soup.select_one(selector)
        if element:
            # Remove script, style, nav elements
            for unwanted in element.select("script, style, nav, header, footer"):
                unwanted.decompose()

            text = element.get_text(separator="\n", strip=True)
            if len(text) > 100:  # Ensure we got substantial content
                return text

    # Fallback: extract all body text
    return soup.get_text(separator="\n", strip=True)


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------


def create_fed_session() -> requests.Session:
    """Create requests session with retry logic and timeout.

    Returns:
        Configured requests session with:
            - 3 retries with exponential backoff
            - Automatic retry on 5xx errors
            - Connection pooling

    Example:
        >>> session = create_fed_session()
        >>> response = session.get("https://www.federalreserve.gov")
    """
    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Set default headers
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (FX-AlphaLab Research Bot)",
        }
    )

    return session
