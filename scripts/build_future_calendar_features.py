from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path("data/processed")
OUTPUT_PATH = Path("data/processed/calendar_future_features.csv")
REQUIRED_COLUMNS = ["timestamp_utc", "currency", "impact", "event_type", "score_available"]


def find_latest_scored_calendar_file(processed_dir: Path) -> Path | None:
    candidates = [
        path
        for path in processed_dir.rglob("*.csv")
        if "calendar" in path.stem.lower() and "scored" in path.stem.lower()
    ]

    if not candidates:
        return None

    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name.lower()))


def load_calendar_file(path: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        print(f"Empty file: {path}")
        return None

    if df.empty:
        print(f"Empty file: {path}")
        return None

    return df


def validate_required_columns(df: pd.DataFrame) -> list[str]:
    return [column for column in REQUIRED_COLUMNS if column not in df.columns]


def normalize_text(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower()


def format_time_range(df: pd.DataFrame) -> str:
    timestamps = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True).dropna()
    if timestamps.empty:
        return "N/A"
    return f"{timestamps.min().isoformat()} -> {timestamps.max().isoformat()}"


def hours_until(later: pd.Series, now_utc: pd.Timestamp) -> pd.Series:
    return (later - now_utc).dt.total_seconds() / 3600.0


def min_positive_hours(series: pd.Series) -> float | None:
    positive = series[series > 0]
    return float(positive.min()) if not positive.empty else None


def get_calendar_pressure_label(row: pd.Series) -> str:
    if row["high_impact_events_next_24h"] >= 2 or row["events_next_4h"] >= 2:
        return "high_upcoming_risk"
    if row["high_impact_events_next_24h"] == 1 or row["events_next_24h"] >= 2:
        return "moderate_upcoming_risk"
    return "quiet_calendar"


def main() -> int:
    input_path = find_latest_scored_calendar_file(PROCESSED_DIR)
    if input_path is None:
        print("No scored calendar file found in data/processed")
        return 1

    df = load_calendar_file(input_path)
    if df is None:
        return 1

    missing_columns = validate_required_columns(df)
    if missing_columns:
        print(f"Missing required columns in {input_path}: {', '.join(missing_columns)}")
        return 1

    df = df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
    df["currency"] = df["currency"].astype(str).str.strip()
    df["impact"] = normalize_text(df["impact"])
    df["event_type"] = normalize_text(df["event_type"])

    now_utc = pd.Timestamp.now(tz="UTC")
    future_rows = df.loc[df["timestamp_utc"] > now_utc].copy()

    print(f"Input file: {input_path}")
    print(f"Total rows loaded: {len(df)}")
    print(f"Future events kept: {len(future_rows)}")

    currencies = sorted(
        currency for currency in future_rows["currency"].dropna().unique() if currency
    )
    currencies_text = ", ".join(currencies) if currencies else "N/A"
    print(f"Currencies present: {currencies_text}")
    print(f"Time range: {format_time_range(future_rows) if not future_rows.empty else 'N/A'}")
    print(f"future_release events: {(future_rows['event_type'] == 'future_release').sum()}")

    if future_rows.empty:
        print("No future events found")
        return 0

    future_rows["hours_to_event"] = hours_until(future_rows["timestamp_utc"], now_utc)

    feature_rows = []
    for currency, group in future_rows.groupby("currency", dropna=False):
        high_group = group[(group["impact"] == "high") & (group["hours_to_event"] > 0)]
        feature_rows.append(
            {
                "snapshot_time_utc": now_utc.isoformat(),
                "currency": currency,
                "time_to_next_event_hours": min_positive_hours(group["hours_to_event"]),
                "time_to_next_high_impact_event_hours": min_positive_hours(
                    high_group["hours_to_event"]
                ),
                "events_next_4h": int((group["hours_to_event"] <= 4).sum()),
                "events_next_24h": int((group["hours_to_event"] <= 24).sum()),
                "high_impact_events_next_24h": int(
                    ((group["impact"] == "high") & (group["hours_to_event"] <= 24)).sum()
                ),
                "future_release_count": int((group["event_type"] == "future_release").sum()),
            }
        )

    features = pd.DataFrame(feature_rows)

    if features.empty:
        print("No feature rows to save")
        return 0

    features["calendar_pressure_label"] = features.apply(get_calendar_pressure_label, axis=1)

    output_columns = [
        "snapshot_time_utc",
        "currency",
        "time_to_next_event_hours",
        "time_to_next_high_impact_event_hours",
        "events_next_4h",
        "events_next_24h",
        "high_impact_events_next_24h",
        "future_release_count",
        "calendar_pressure_label",
    ]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    features.loc[:, output_columns].to_csv(OUTPUT_PATH, index=False)

    print("Preview:")
    for _, row in features.head(10).iterrows():
        next_event = row["time_to_next_event_hours"]
        next_high = row["time_to_next_high_impact_event_hours"]
        next_high_text = "N/A" if pd.isna(next_high) else f"{float(next_high):.1f}h"
        print(
            f"{row['currency']} | next event in {float(next_event):.1f}h | "
            f"next high-impact in {next_high_text} | {int(row['events_next_24h'])} events next 24h"
        )

    print(f"Saved future features to {OUTPUT_PATH}")
    print(f"Feature rows: {len(features)}")
    for _, row in features.head(5).iterrows():
        next_high = row["time_to_next_high_impact_event_hours"]
        next_high_text = (
            "no high-impact event soon"
            if pd.isna(next_high)
            else f"next high-impact in {float(next_high):.1f}h"
        )
        print(
            f"{row['currency']} | {next_high_text} | {row['events_next_24h']} events next 24h | "
            f"{row['calendar_pressure_label']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
