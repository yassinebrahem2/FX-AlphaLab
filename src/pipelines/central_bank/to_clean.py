"""
Central Bank Raw → Clean Transformation
STEP 5 – Policy Source Data
"""

import pandas as pd

from src.pipelines.central_bank.validate import validate_cb_schema


def raw_to_clean_cb(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw central bank documents into clean, validated form.

    Responsibilities:
    - enforce schema validity
    - sort deterministically
    - preserve raw facts exactly

    Does NOT:
    - attach metadata
    - infer values
    - modify document content
    """

    if df.empty:
        return df.copy()

    df_clean = df.copy()

    # ---------------------------------------------------------------
    # Deterministic ordering (when publication time exists)
    # ---------------------------------------------------------------

    if "publication_datetime_utc" in df_clean.columns:
        df_clean = df_clean.sort_values(
            by="publication_datetime_utc",
            na_position="last"
        ).reset_index(drop=True)

    # ---------------------------------------------------------------
    # Schema validation
    # ---------------------------------------------------------------

    validate_cb_schema(df_clean)

    return df_clean
