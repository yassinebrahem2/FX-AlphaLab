"""
SQLite Database Connection
Week 4 – Foundation & Data
"""

import sqlite3
from pathlib import Path
from typing import Iterator

from src.storage.schema import PRICE_TABLE_SQL


# -----------------------------
# Configuration
# -----------------------------

DB_PATH = Path("data/fx_data.db")


# -----------------------------
# Connection Handling
# -----------------------------

def get_connection() -> sqlite3.Connection:
    """
    Create and return a SQLite database connection.

    Notes:
    - Uses file-based SQLite database
    - Caller is responsible for closing the connection
    """

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    return sqlite3.connect(
        DB_PATH,
        detect_types=sqlite3.PARSE_DECLTYPES,
        check_same_thread=False,
    )


# -----------------------------
# Database Initialisation
# -----------------------------

def initialize_database() -> None:
    """
    Initialise database schema if it does not exist.
    """

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(PRICE_TABLE_SQL)
        conn.commit()
