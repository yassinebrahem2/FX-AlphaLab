"""
Economic Calendar Fetcher (RAW)
STEP 4 – Exogenous Event Data
"""

from datetime import datetime, timezone
from typing import Protocol

import pandas as pd


# -------------------------------------------------------------------
# Fetcher contract
# -------------------------------------------------------------------

class EconomicCalendarFetcher(Protocol):
    """
    Contract for any economic calendar data source.

    Implementations MUST return raw, unvalidated data.
    No cleaning, no inference, no normalization.
    """

    def fetch(self, start_date: str, end_date: str) -> pd.DataFrame:
        ...


# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------

def fetch_economic_calendar(
    fetcher: EconomicCalendarFetcher,
    start_date: str,
    end_date: str,
    source: str,
) -> pd.DataFrame:
    """
    Fetch raw economic calendar data and attach ingestion metadata.

    Parameters:
        fetcher: object implementing EconomicCalendarFetcher
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        source: canonical source name (e.g. FRED, ECB, Investing)

    Returns:
        Raw pd.DataFrame (schema not yet enforced)
    """
    df = fetcher.fetch(start_date=start_date, end_date=end_date)
    return _add_metadata(df, source)


# -------------------------------------------------------------------
# Internal helpers
# -------------------------------------------------------------------

def _add_metadata(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Attach ingestion metadata.
    This function MUST be side-effect free.
    """
    df = df.copy()
    df["source"] = source
    df["ingested_at_utc"] = datetime.now(timezone.utc)
    return df
