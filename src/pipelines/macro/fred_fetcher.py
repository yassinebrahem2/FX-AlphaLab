"""
FRED Economic Data Fetcher (RAW)
STEP 4F – Source Implementation
"""

from fredapi import Fred
import pandas as pd

from src.pipelines.macro.fred_indicators import FRED_INDICATORS


class FredFetcher:
    """
    Fetches macroeconomic time series from FRED.

    Notes:
    - Dates returned by FRED represent observation periods,
      not real-time release timestamps.
    - No assumptions or transformations are applied.
    """

    def __init__(self, api_key: str):
        self._fred = Fred(api_key=api_key)

    def fetch(self) -> pd.DataFrame:
        records = []

        for spec in FRED_INDICATORS.values():
            series = self._fred.get_series(spec["fred_id"])

            for date, value in series.items():
                if pd.isna(value):
                    continue

                records.append({
                    # Deterministic event identity
                    "event_id": f"{spec['country']}_{spec['indicator']}_{date}",

                    "country": spec["country"],
                    "currency": spec["currency"],
                    "indicator": spec["indicator"],

                    # FRED dates are observation-period timestamps
                    "release_datetime_utc": pd.to_datetime(date).tz_localize("UTC"),

                    "actual_value": float(value),
                    "unit": spec["unit"],
                    "frequency": spec["frequency"],
                })

        return pd.DataFrame(records)
