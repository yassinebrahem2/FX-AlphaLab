"""
Raw → Clean Macro Transformation
STEP 4 – Exogenous Event Data
"""

import pandas as pd

from src.pipelines.macro.validate import validate_macro_schema


def raw_to_clean_macro(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform minimal, non-destructive normalization on raw macro data.

    This function:
    - validates schema
    - enforces deterministic ordering

    It does NOT:
    - infer meaning
    - fill missing values
    - transform units
    - modify timestamps
    """
    df = df.copy()

    # ---------------------------------------------------------------
    # Schema validation (raw contract)
    # ---------------------------------------------------------------
    validate_macro_schema(df)

    # ---------------------------------------------------------------
    # Deterministic ordering
    # ---------------------------------------------------------------
    df = df.sort_values("release_datetime_utc").reset_index(drop=True)

    return df
