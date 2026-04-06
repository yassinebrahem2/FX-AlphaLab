from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path("data/processed")
OUTPUT_PATH = Path("data/processed/calendar_currency_signals.csv")
REQUIRED_COLUMNS = ["date", "currency", "impact", "final_score", "score_available"]


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


def is_score_available(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def format_date_range(df: pd.DataFrame) -> str:
    dates = pd.to_datetime(df["date"], errors="coerce")
    dates = dates.dropna()
    if dates.empty:
        return "N/A"
    return f"{dates.min().date()} to {dates.max().date()}"


def format_score(value: float) -> str:
    return f"{value:+.2f}"


def normalize_text(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower()


def get_dominant_direction(value: float) -> str:
    if value > 0:
        return "positive"
    if value < 0:
        return "negative"
    return "neutral"


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

    scored_rows = df.loc[is_score_available(df["score_available"])].copy()

    currencies = sorted(
        currency
        for currency in scored_rows["currency"].dropna().astype(str).str.strip().unique()
        if currency
    )
    currencies_text = ", ".join(currencies) if currencies else "N/A"
    date_range_text = format_date_range(scored_rows) if not scored_rows.empty else "N/A"

    print(f"Input file: {input_path}")
    print(f"Total rows loaded: {len(df)}")
    print(f"Valid scored rows kept: {len(scored_rows)}")
    print(f"Currencies present: {currencies_text}")
    print(f"Date range: {date_range_text}")

    if scored_rows.empty:
        print("No valid scoreable rows found")
        return 0

    scored_rows["final_score"] = pd.to_numeric(scored_rows["final_score"], errors="coerce")
    scored_rows = scored_rows.dropna(subset=["final_score"]).copy()

    if scored_rows.empty:
        print("No valid numeric scored rows remain after parsing final_score")
        return 0

    scored_rows["date"] = pd.to_datetime(scored_rows["date"], errors="coerce")
    scored_rows["currency"] = scored_rows["currency"].astype(str).str.strip()
    scored_rows["impact_normalized"] = normalize_text(scored_rows["impact"])
    scored_rows = scored_rows.dropna(subset=["date"])
    scored_rows = scored_rows[scored_rows["currency"] != ""].copy()
    scored_rows["date"] = scored_rows["date"].dt.strftime("%Y-%m-%d")

    aggregated = (
        scored_rows.groupby(["date", "currency"], as_index=False)
        .agg(
            event_count=("final_score", "size"),
            net_macro_score=("final_score", "sum"),
            high_impact_count=("impact_normalized", lambda values: (values == "high").sum()),
            positive_score_sum=("final_score", lambda values: values[values > 0].sum()),
            negative_score_sum=("final_score", lambda values: values[values < 0].sum()),
            mean_final_score=("final_score", "mean"),
            max_abs_score=("final_score", lambda values: values.abs().max()),
        )
        .sort_values(["date", "currency"])
    )

    aggregated["dominant_direction"] = aggregated["net_macro_score"].map(get_dominant_direction)

    if aggregated.empty:
        print("No aggregated rows to save")
        return 0

    output_columns = [
        "date",
        "currency",
        "event_count",
        "high_impact_count",
        "positive_score_sum",
        "negative_score_sum",
        "net_macro_score",
        "mean_final_score",
        "max_abs_score",
        "dominant_direction",
    ]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    aggregated.loc[:, output_columns].to_csv(OUTPUT_PATH, index=False)

    print(f"Saved aggregated signals to {OUTPUT_PATH}")
    print(f"Aggregated rows: {len(aggregated)}")
    for _, row in aggregated.head(5).iterrows():
        print(
            f"{row['currency']} | {row['date']} | {int(row['event_count'])} events | "
            f"net={format_score(float(row['net_macro_score']))} | direction={row['dominant_direction']}"
        )

    print("Preview:")
    for _, row in aggregated.head(10).iterrows():
        print(
            f"{row['currency']} | {row['date']} | {int(row['event_count'])} events | "
            f"net={format_score(float(row['net_macro_score']))} | direction={row['dominant_direction']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
