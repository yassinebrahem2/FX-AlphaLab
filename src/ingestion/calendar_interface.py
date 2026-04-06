from pathlib import Path

import pandas as pd

PUBLISHED_DIR = Path("data/published/calendar")
STATE_SNAPSHOT_PATH = PUBLISHED_DIR / "calendar_state_snapshot_latest.csv"
EVENTS_PATH = PUBLISHED_DIR / "calendar_events_latest.csv"


def load_calendar_state_snapshot() -> pd.DataFrame:
    """Load the published calendar state snapshot."""
    if not STATE_SNAPSHOT_PATH.exists():
        raise FileNotFoundError(f"Published state snapshot not found: {STATE_SNAPSHOT_PATH}")
    return pd.read_csv(STATE_SNAPSHOT_PATH)


def get_currency_calendar_state(currency: str) -> pd.DataFrame:
    """Get calendar state for a specific currency."""
    df = load_calendar_state_snapshot()
    normalized_currency = currency.strip().upper()
    filtered = df[df["currency"] == normalized_currency]
    return filtered


def load_scored_calendar_events() -> pd.DataFrame:
    """Load the published scored calendar events."""
    if not EVENTS_PATH.exists():
        raise FileNotFoundError(f"Published scored events not found: {EVENTS_PATH}")
    return pd.read_csv(EVENTS_PATH)
