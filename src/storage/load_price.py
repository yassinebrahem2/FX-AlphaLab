"""
Load Clean Price Data into Database
Week 4 – Foundation & Data
"""

import pandas as pd
from src.storage.db import get_connection


def load_price_dataframe(df: pd.DataFrame):
    """
    Load clean price dataframe into SQLite.
    Assumes schema has already been validated.
    """

    conn = get_connection()

    df.to_sql(
        "price_ohlc",
        conn,
        if_exists="append",
        index=False,
    )

    conn.close()
