"""
Central Bank Document Loader
STEP 5 – Policy Source Data
"""

import pandas as pd

from src.storage.db import get_connection


def load_central_bank_documents(df: pd.DataFrame) -> None:
    """
    Load raw central bank documents into persistent storage.

    Inserts are idempotent by primary key (document_id).
    """

    if df.empty:
        return

    conn = get_connection()

    try:
        df.to_sql(
            "central_bank_documents",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )
        conn.commit()
    finally:
        conn.close()
