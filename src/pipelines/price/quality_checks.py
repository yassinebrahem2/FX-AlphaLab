"""
Price Data Quality Checks
Week 4 – Foundation & Data
"""

import pandas as pd


def check_missing_values(df: pd.DataFrame):
    """
    Detect missing values explicitly.
    Returns a JSON-serialisable report dictionary.
    """

    total_rows = int(len(df))

    missing_series = df.isna().sum()
    missing_by_column = {
        col: int(count)
        for col, count in missing_series.items()
        if count > 0
    }

    return {
        "total_rows": total_rows,
        "missing_by_column": missing_by_column,
        "has_missing": bool(missing_by_column),
    }



def check_monotonic_time(df: pd.DataFrame):
    """
    Ensure timestamps are strictly increasing.
    """

    if not df["timestamp_utc"].is_monotonic_increasing:
        raise ValueError("Timestamps are not strictly increasing")

    return True
