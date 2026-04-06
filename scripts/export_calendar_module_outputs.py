from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path("data/processed")
PUBLISH_DIR = Path("data/published/calendar")
SIGNALS_PATH = PROCESSED_DIR / "calendar_currency_signals.csv"
STATE_SNAPSHOT_PATH = PROCESSED_DIR / "calendar_state_snapshot.csv"

SCORED_REQUIRED_COLUMNS = ["date", "currency", "impact", "final_score", "score_available"]

SIGNALS_REQUIRED_COLUMNS = [
    "date",
    "currency",
    "event_count",
    "high_impact_count",
    "net_macro_score",
    "dominant_direction",
]

STATE_SNAPSHOT_REQUIRED_COLUMNS = [
    "snapshot_time_utc",
    "currency",
    "recent_net_macro_score",
    "recent_event_count",
    "recent_high_impact_count",
    "dominant_direction",
    "time_to_next_event_hours",
    "time_to_next_high_impact_event_hours",
    "events_next_4h",
    "events_next_24h",
    "high_impact_events_next_24h",
    "future_release_count",
    "calendar_pressure_label",
    "macro_bias_label",
    "calendar_state_label",
]


def find_latest_scored_calendar_file(processed_dir: Path) -> Path | None:
    candidates = [
        path
        for path in processed_dir.rglob("*.csv")
        if "calendar" in path.stem.lower() and "scored" in path.stem.lower()
    ]

    if not candidates:
        return None

    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name.lower()))


def load_csv_safe(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        print(f"File not found: {path}")
        return None

    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        print(f"Empty file: {path}")
        return None

    if df.empty:
        print(f"Empty file: {path}")
        return None

    return df


def get_currencies(df: pd.DataFrame) -> list[str]:
    if "currency" not in df.columns:
        return []
    return sorted(
        set(curr for curr in df["currency"].dropna().astype(str).str.strip().unique() if curr)
    )


def get_snapshot_time(df: pd.DataFrame) -> str:
    if "snapshot_time_utc" not in df.columns or df.empty:
        return "N/A"
    return str(df["snapshot_time_utc"].iloc[0])


def validate_schema(path: Path, df: pd.DataFrame, required_columns: list[str]) -> bool:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        print(f"Schema validation failed for {path}")
        print(f"  Missing columns: {', '.join(missing)}")
        return False
    return True


def publish_datasets(
    scored: pd.DataFrame,
    signals: pd.DataFrame,
    state_snapshot: pd.DataFrame,
) -> None:
    PUBLISH_DIR.mkdir(parents=True, exist_ok=True)

    files = [
        (scored, "calendar_events_latest.csv"),
        (signals, "calendar_currency_signals_latest.csv"),
        (state_snapshot, "calendar_state_snapshot_latest.csv"),
    ]

    print("Published outputs:")
    for df, filename in files:
        output_path = PUBLISH_DIR / filename
        df.to_csv(output_path, index=False)
        currencies = get_currencies(df)
        currencies_text = ", ".join(currencies) if currencies else "N/A"
        print(f"  {output_path}")
        print(f"    Rows: {len(df)}, Currencies: {currencies_text}")


def main() -> int:
    scored_path = find_latest_scored_calendar_file(PROCESSED_DIR)
    if scored_path is None:
        print("No scored calendar file found in data/processed")
        return 1

    scored = load_csv_safe(scored_path)
    if scored is None:
        return 1

    signals = load_csv_safe(SIGNALS_PATH)
    if signals is None:
        return 1

    state_snapshot = load_csv_safe(STATE_SNAPSHOT_PATH)
    if state_snapshot is None:
        return 1

    if not validate_schema(scored_path, scored, SCORED_REQUIRED_COLUMNS):
        return 1
    if not validate_schema(SIGNALS_PATH, signals, SIGNALS_REQUIRED_COLUMNS):
        return 1
    if not validate_schema(STATE_SNAPSHOT_PATH, state_snapshot, STATE_SNAPSHOT_REQUIRED_COLUMNS):
        return 1

    print("Schema validation passed for all datasets")
    print()

    scored_currencies = get_currencies(scored)
    signals_currencies = get_currencies(signals)
    snapshot_currencies = get_currencies(state_snapshot)

    scored_currencies_text = ", ".join(scored_currencies) if scored_currencies else "N/A"
    signals_currencies_text = ", ".join(signals_currencies) if signals_currencies else "N/A"
    snapshot_currencies_text = ", ".join(snapshot_currencies) if snapshot_currencies else "N/A"

    snapshot_time = get_snapshot_time(state_snapshot)

    print(f"Scored events file: {scored_path}")
    print(f"  Rows: {len(scored)}")
    print(f"  Currencies: {scored_currencies_text}")
    print()
    print(f"Signals file: {SIGNALS_PATH}")
    print(f"  Rows: {len(signals)}")
    print(f"  Currencies: {signals_currencies_text}")
    print()
    print(f"State snapshot file: {STATE_SNAPSHOT_PATH}")
    print(f"  Rows: {len(state_snapshot)}")
    print(f"  Currencies: {snapshot_currencies_text}")
    print(f"  Snapshot time: {snapshot_time}")

    print()
    publish_datasets(scored, signals, state_snapshot)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
