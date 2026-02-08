"""
Macro Schema Validation (RAW)
STEP 4 – Exogenous Event Data
"""

import pandas as pd

from src.pipelines.macro.schema import MACRO_SCHEMA_RAW


def validate_macro_schema(df: pd.DataFrame) -> None:
    """
    Validate raw macroeconomic data against the declared schema.

    This function enforces:
    - required columns
    - datetime dtype correctness

    It does NOT:
    - coerce types
    - fill missing values
    - normalize data
    """

    # ---------------------------------------------------------------
    # Required columns
    # ---------------------------------------------------------------

    missing_columns = set(MACRO_SCHEMA_RAW.keys()) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing macro columns: {sorted(missing_columns)}")

    # ---------------------------------------------------------------
    # Datetime columns
    # ---------------------------------------------------------------

    for column, expected_dtype in MACRO_SCHEMA_RAW.items():
        if expected_dtype == "datetime64[ns]":
            if not pd.api.types.is_datetime64_any_dtype(df[column]):
                raise TypeError(
                    f"Column '{column}' must be datetime64 dtype"
                )
