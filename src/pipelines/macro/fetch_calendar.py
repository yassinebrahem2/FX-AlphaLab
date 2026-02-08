"""
Economic Calendar Fetcher (RAW)
STEP 4 – Exogenous Event Data

This module defines the source-agnostic contract for macro / economic data
fetching and attaches ingestion metadata. No validation, cleaning, or
normalization is performed here.
"""

from datetime import datetime, timezone
from typing import Protocol

import pandas as pd


# -------------------------------------------------------------------
# Fetcher contract
# -------------------------------------------------------------------

class EconomicCalendarFetcher(Protocol):
    """
    Contract for any macro / economic data source.

    Implementations MUST:
    - return raw, unvalidated data
    - make no assumptions about importance, impact, or interpretation
    - preserve source timestamps as-is
    """

    def fetch(self) -> pd.DataFrame:
        """
        Fetch raw economic data.

        Returns:
            pd.DataFrame containing raw macroeconomic records
        """
        ...


# -------------------------------------------------------------------
# Public helpers
# -------------------------------------------------------------------

def attach_metadata(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Attach ingestion metadata to a raw macro DataFrame.

    Parameters:
        df: raw DataFrame returned by a fetcher
        source: canonical source name (e.g. FRED, Investing, ECB)

    Returns:
        Copy of df with provenance metadata attached
    """
    df = df.copy()
    df["source"] = source
    df["ingested_at_utc"] = datetime.now(timezone.utc)
    return df
