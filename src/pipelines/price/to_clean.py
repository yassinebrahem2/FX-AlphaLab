"""
Raw → Clean Price Data Transformer
Week 4 – Foundation & Data
"""

from typing import List

import pandas as pd

from src.pipelines.price.validate import validate_schema
from src.pipelines.price.quality_checks import check_monotonic_time


# -----------------------------
# Canonical Clean Schema Order
# -----------------------------

CLEAN_PRICE_COLUMNS: List[str] = [
    "timestamp_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "pair",
    "timeframe",
    "source",
    "ingested_at_utc",
]


# -----------------------------
# Transformation
# -----------------------------

def raw_to_clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert standardized raw OHLC data into clean, validated form.

    Allowed operations (Week 4 only):
    - column selection & ordering
    - deterministic sorting
    - schema validation
    - temporal consistency checks

    Explicitly NOT allowed:
    - enrichment
    - inference
    - imputation
    - feature engineering
    """

    df = df.copy()

    _select_and_order_columns(df)
    _sort_by_timestamp(df)
    _validate_clean_data(df)

    return df


# -----------------------------
# Internal Helpers
# -----------------------------

def _select_and_order_columns(df: pd.DataFrame) -> None:
    """
    Enforce canonical clean column ordering.
    """

    missing = set(CLEAN_PRICE_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required clean columns: {sorted(missing)}")

    df[:] = df[CLEAN_PRICE_COLUMNS]


def _sort_by_timestamp(
    df: pd.DataFrame,
    timestamp_column: str = "timestamp_utc",
) -> None:
    """
    Ensure deterministic temporal ordering.
    """

    df.sort_values(timestamp_column, inplace=True)
    df.reset_index(drop=True, inplace=True)


def _validate_clean_data(df: pd.DataFrame) -> None:
    """
    Apply hard validation gates for clean price data.
    """

    validate_schema(df)
    check_monotonic_time(df)
