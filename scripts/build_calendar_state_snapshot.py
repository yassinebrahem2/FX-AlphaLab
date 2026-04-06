from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path("data/processed")
SIGNALS_PATH = PROCESSED_DIR / "calendar_currency_signals.csv"
FEATURES_PATH = PROCESSED_DIR / "calendar_future_features.csv"
OUTPUT_PATH = PROCESSED_DIR / "calendar_state_snapshot.csv"

SIGNALS_REQUIRED_COLUMNS = [
    "date",
    "currency",
    "event_count",
    "high_impact_count",
    "net_macro_score",
    "dominant_direction",
]

FEATURES_REQUIRED_COLUMNS = [
    "snapshot_time_utc",
    "currency",
    "time_to_next_event_hours",
    "time_to_next_high_impact_event_hours",
    "events_next_4h",
    "events_next_24h",
    "high_impact_events_next_24h",
    "calendar_pressure_label",
]


def load_csv_safe(path: Path, required_columns: list[str]) -> pd.DataFrame | None:
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

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        print(f"Missing columns in {path}: {', '.join(missing)}")
        return None

    return df


def get_latest_signals_per_currency(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["currency"] = df["currency"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values("date")
    return df.drop_duplicates(subset=["currency"], keep="last")


def normalize_currency(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["currency"] = df["currency"].astype(str).str.strip()
    return df


def get_macro_bias_label(value: float | None) -> str:
    if pd.isna(value):
        return "neutral_macro_bias"
    if value > 0:
        return "positive_macro_bias"
    if value < 0:
        return "negative_macro_bias"
    return "neutral_macro_bias"


def get_calendar_state_label(macro_bias: str, pressure: str) -> str:
    pressure_map = {
        "high_upcoming_risk": "high_risk",
        "moderate_upcoming_risk": "moderate_risk",
        "quiet_calendar": "quiet_calendar",
    }
    mapped_pressure = (
        pressure_map.get(str(pressure).strip(), "unknown_risk")
        if pd.notna(pressure)
        else "unknown_risk"
    )
    return f"{macro_bias}_{mapped_pressure}"


def main() -> int:
    missing_files = []
    if not SIGNALS_PATH.exists():
        missing_files.append(str(SIGNALS_PATH))
    if not FEATURES_PATH.exists():
        missing_files.append(str(FEATURES_PATH))

    if missing_files:
        print(f"Missing required file(s): {', '.join(missing_files)}")
        return 1

    signals = load_csv_safe(SIGNALS_PATH, SIGNALS_REQUIRED_COLUMNS)
    features = load_csv_safe(FEATURES_PATH, FEATURES_REQUIRED_COLUMNS)

    if signals is None or features is None:
        return 1

    signals = get_latest_signals_per_currency(signals)
    features = normalize_currency(features)

    signals_currencies = sorted(set(signals["currency"].dropna().unique()))
    features_currencies = sorted(set(features["currency"].dropna().unique()))

    signals_currencies_text = ", ".join(signals_currencies) if signals_currencies else "N/A"
    features_currencies_text = ", ".join(features_currencies) if features_currencies else "N/A"

    latest_date = pd.to_datetime(signals["date"], errors="coerce").max()
    latest_date_text = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A"

    snapshot_time = features["snapshot_time_utc"].iloc[0] if not features.empty else "N/A"

    print(f"Signals file: {SIGNALS_PATH}")
    print(f"  Rows loaded: {len(signals)}")
    print(f"  Currencies: {signals_currencies_text}")
    print(f"  Latest date: {latest_date_text}")
    print()
    print(f"Features file: {FEATURES_PATH}")
    print(f"  Rows loaded: {len(features)}")
    print(f"  Currencies: {features_currencies_text}")
    print(f"  Snapshot time: {snapshot_time}")

    if not signals_currencies or not features_currencies:
        print("No currencies found in one or both files")
        return 0

    signals_rename = {
        "net_macro_score": "recent_net_macro_score",
        "event_count": "recent_event_count",
        "high_impact_count": "recent_high_impact_count",
    }
    signals_kept = signals[
        ["currency"] + list(signals_rename.keys()) + ["dominant_direction"]
    ].rename(columns=signals_rename)

    merged = features.merge(signals_kept, on="currency", how="left")

    if merged.empty:
        print("No rows after merge")
        return 0

    merged["macro_bias_label"] = merged["recent_net_macro_score"].apply(get_macro_bias_label)
    merged["calendar_state_label"] = merged.apply(
        lambda row: get_calendar_state_label(
            row["macro_bias_label"], row["calendar_pressure_label"]
        ),
        axis=1,
    )

    print()
    print("Merged snapshot preview:")
    for _, row in merged.head(10).iterrows():
        recent_net = row.get("recent_net_macro_score")
        recent_net_text = "N/A" if pd.isna(recent_net) else f"{float(recent_net):+.2f}"
        next_high = row.get("time_to_next_high_impact_event_hours")
        next_high_text = "N/A" if pd.isna(next_high) else f"{float(next_high):.1f}h"
        print(
            f"{row['currency']} | recent_net={recent_net_text} | "
            f"next_high_impact={next_high_text} | macro_bias={row['macro_bias_label']} | "
            f"state={row['calendar_state_label']}"
        )

    output_columns = [
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

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.loc[:, output_columns].to_csv(OUTPUT_PATH, index=False)

    print()
    print(f"Saved calendar state snapshot to {OUTPUT_PATH}")
    print(f"Snapshot rows: {len(merged)}")
    for _, row in merged.head(5).iterrows():
        recent_net = row.get("recent_net_macro_score")
        recent_net_text = "N/A" if pd.isna(recent_net) else f"{float(recent_net):+.2f}"
        print(
            f"{row['currency']} | recent_net={recent_net_text} | "
            f"bias={row['macro_bias_label']} | pressure={row['calendar_pressure_label']} | "
            f"state={row['calendar_state_label']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
