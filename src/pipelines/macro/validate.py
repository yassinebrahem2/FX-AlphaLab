"""
Macro Schema Validation (RAW)
STEP 4 – Exogenous Event Data
"""

import pandas as pd

from src.pipelines.macro.schema import MACRO_SCHEMA_RAW


# Fields that are allowed to be missing at RAW stage
# (source-dependent, e.g. FRED vs economic calendars)
OPTIONAL_RAW_COLUMNS = {
    "scheduled_datetime_utc",
    "previous_value",
    "revision_value",
}


def validate_macro_schema(df: pd.DataFrame) -> None:
    """
    Validate raw macroeconomic data against the declared schema.

    Enforces:
    - presence of required core columns
    - datetime dtype correctness where columns exist

    Allows:
    - source-specific optional fields to be absent

    Does NOT:
    - coerce types
    - fill missing values
    - normalize data
    """

    expected_columns = set(MACRO_SCHEMA_RAW.keys())
    present_columns = set(df.columns)

    # ---------------------------------------------------------------
    # Required columns (excluding optional RAW fields)
    # ---------------------------------------------------------------

    missing_columns = expected_columns - present_columns - OPTIONAL_RAW_COLUMNS
    if missing_columns:
        raise ValueError(
            f"Missing macro columns: {sorted(missing_columns)}"
        )

    # ---------------------------------------------------------------
    # Datetime columns (only if present)
    # ---------------------------------------------------------------

    for column, expected_dtype in MACRO_SCHEMA_RAW.items():
        if column not in df.columns:
            continue

        if expected_dtype == "datetime64[ns]":
            if not pd.api.types.is_datetime64_any_dtype(df[column]):
                raise TypeError(
                    f"Column '{column}' must be datetime64 dtype"
                )
