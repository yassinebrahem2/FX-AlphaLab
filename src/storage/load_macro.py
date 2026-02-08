"""
Macro Data Loader
STEP 4 – Exogenous Event Data
"""

import pandas as pd

from src.storage.db import get_connection


def load_macro_dataframe(df: pd.DataFrame) -> None:
    """
    Load clean macro data into the macro_data table.

    Inserts are idempotent by primary key (event_id).
    """
    if df.empty:
        return

    conn = get_connection()

    try:
        df.to_sql(
            "macro_data",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )
        conn.commit()
    finally:
        conn.close()
