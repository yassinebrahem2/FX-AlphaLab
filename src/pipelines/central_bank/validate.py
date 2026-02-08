"""
Central Bank Document Schema Validation (RAW)
STEP 5 – Policy Source Data
"""

import pandas as pd

from src.pipelines.central_bank.schema import CB_SCHEMA_RAW


# Fields that may be missing at RAW stage
OPTIONAL_RAW_COLUMNS = {
    "publication_datetime_utc",
    "raw_text",
}


def validate_cb_schema(df: pd.DataFrame) -> None:
    """
    Validate raw central bank documents against the declared schema.

    Enforces:
    - required core columns
    - datetime dtype correctness where present

    Allows:
    - source-dependent missing fields (RAW reality)

    Does NOT:
    - coerce types
    - infer values
    - clean text
    """

    expected_columns = set(CB_SCHEMA_RAW.keys())
    present_columns = set(df.columns)

    # ---------------------------------------------------------------
    # Required columns (excluding optional RAW fields)
    # ---------------------------------------------------------------

    missing_columns = expected_columns - present_columns - OPTIONAL_RAW_COLUMNS
    if missing_columns:
        raise ValueError(
            f"Missing CB columns: {sorted(missing_columns)}"
        )

    # ---------------------------------------------------------------
    # Datetime columns (only if present)
    # ---------------------------------------------------------------

    for column, expected_dtype in CB_SCHEMA_RAW.items():
        if column not in df.columns:
            continue

        if expected_dtype == "datetime64[ns]":
            if not pd.api.types.is_datetime64_any_dtype(df[column]):
                raise TypeError(
                    f"Column '{column}' must be datetime64 dtype"
                )
