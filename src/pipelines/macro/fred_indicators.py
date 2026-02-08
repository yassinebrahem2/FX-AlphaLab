"""
FRED Indicator Registry (RAW)
STEP 4 – Exogenous Event Data
"""

from typing import Dict, TypedDict


class FredIndicatorSpec(TypedDict):
    fred_id: str
    country: str
    currency: str
    indicator: str
    unit: str
    frequency: str


FRED_INDICATORS: Dict[str, FredIndicatorSpec] = {
    "US_CPI": {
        "fred_id": "CPIAUCSL",
        "country": "US",
        "currency": "USD",
        "indicator": "CPI",
        "unit": "index",
        "frequency": "monthly",
    },
    "US_GDP": {
        "fred_id": "GDP",
        "country": "US",
        "currency": "USD",
        "indicator": "GDP",
        "unit": "usd",
        "frequency": "quarterly",
    },
    "US_UNRATE": {
        "fred_id": "UNRATE",
        "country": "US",
        "currency": "USD",
        "indicator": "Unemployment Rate",
        "unit": "percent",
        "frequency": "monthly",
    },
}
