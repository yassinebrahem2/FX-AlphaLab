"""
Price Data Validation
Week 4 – Foundation & Data
"""

from typing import Mapping, Type

import pandas as pd

from src.pipelines.price.schema import PRICE_SCHEMA


# -----------------------------
# Schema Validation
# -----------------------------

def validate_schema(df: pd.DataFrame) -> None:
    """
    Validate that the dataframe matches the expected PRICE_SCHEMA.

    Enforces:
    - required columns
    - correct data types
    - explicit, early failure on schema drift

    Raises:
        ValueError: missing columns
        TypeError: invalid column types
    """

    _validate_required_columns(df, PRICE_SCHEMA)
    _validate_column_types(df, PRICE_SCHEMA)


def _validate_required_columns(
    df: pd.DataFrame,
    schema: Mapping[str, Type],
) -> None:
    missing = set(schema.keys()) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")


def _validate_column_types(
    df: pd.DataFrame,
    schema: Mapping[str, Type],
) -> None:
    for column, expected_type in schema.items():
        series = df[column]

        if expected_type == "datetime64[ns]":
            if not pd.api.types.is_datetime64_any_dtype(series):
                raise TypeError(
                    f"Column '{column}' must be datetime64, "
                    f"got {series.dtype}"
                )
            continue

        invalid_mask = ~series.map(
            lambda x: isinstance(x, expected_type) or pd.isna(x)
        )

        if invalid_mask.any():
            bad_count = int(invalid_mask.sum())
            raise TypeError(
                f"Column '{column}' contains {bad_count} invalid values "
                f"(expected {expected_type})"
            )
