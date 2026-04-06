from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path("data/processed")
PUBLISHED_DIR = Path("data/published/calendar")
REQUIRED_COLUMNS = ["currency", "event_name", "event_type", "score_available", "score_reason"]
PUBLISHED_FILES = {
    "calendar_events_latest.csv": ["currency", "event_name", "score_available"],
    "calendar_currency_signals_latest.csv": ["currency", "net_macro_score", "dominant_direction"],
    "calendar_state_snapshot_latest.csv": ["currency", "macro_bias_label", "calendar_state_label"],
    "calendar_macro_context_latest.csv": ["currency", "macro_bias_label", "calendar_state_label"],
}


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
    return [col for col in REQUIRED_COLUMNS if col not in df.columns]


def is_score_available(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def normalize_currency_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper()


def main() -> int:
    input_path = find_latest_scored_calendar_file(PROCESSED_DIR)
    if input_path is None:
        print("No scored calendar file found in data/processed")
        return 1

    df = load_csv_safe(input_path)
    if df is None:
        return 1

    missing_columns = validate_required_columns(df)
    if missing_columns:
        print(f"Missing required columns in {input_path}: {', '.join(missing_columns)}")
        return 1

    report_df = df.copy()
    report_df["currency"] = report_df["currency"].astype(str).str.strip().str.upper()

    scored_mask = is_score_available(report_df["score_available"])
    scored_rows = int(scored_mask.sum())
    total_rows = len(report_df)
    unscored_rows = total_rows - scored_rows
    coverage_pct = (scored_rows / total_rows * 100.0) if total_rows else 0.0

    currencies = sorted(
        curr for curr in report_df["currency"].dropna().unique() if str(curr).strip()
    )
    currencies_text = ", ".join(currencies) if currencies else "N/A"

    event_type_counts = report_df["event_type"].astype(str).value_counts(dropna=False)
    score_reason_counts = report_df["score_reason"].astype(str).value_counts(dropna=False)
    top_event_names = report_df["event_name"].astype(str).value_counts(dropna=False).head(10)

    print(f"Input file: {input_path}")
    print(f"Total rows: {total_rows}")
    print(f"Scored rows count: {scored_rows}")
    print(f"Unscored rows count: {unscored_rows}")
    print(f"Scoring coverage percentage: {coverage_pct:.2f}%")
    print(f"Distinct currencies seen: {currencies_text}")

    print("\nEvent type counts:")
    for key, count in event_type_counts.items():
        print(f"- {key}: {int(count)}")

    print("\nScore reason counts:")
    for key, count in score_reason_counts.items():
        print(f"- {key}: {int(count)}")

    print("\nTop 10 event names by frequency:")
    for key, count in top_event_names.items():
        print(f"- {key}: {int(count)}")

    unscored_mask = ~scored_mask
    unscored_df = report_df.loc[unscored_mask].copy()

    print("\nScoreability by event_type:")
    scoreability = (
        report_df.assign(scored=scored_mask)
        .groupby("event_type", dropna=False)
        .agg(total_rows=("event_type", "size"), scored_rows=("scored", "sum"))
    )
    scoreability["coverage_pct"] = scoreability["scored_rows"] / scoreability["total_rows"] * 100.0
    for event_type, row in scoreability.sort_index().iterrows():
        print(
            f"- {event_type}: total={int(row['total_rows'])}, scored={int(row['scored_rows'])}, "
            f"coverage={float(row['coverage_pct']):.2f}%"
        )

    print("\nTop unscored event names:")
    if unscored_df.empty:
        print("- no unscored rows")
    else:
        top_unscored = unscored_df["event_name"].astype(str).value_counts(dropna=False).head(10)
        for key, count in top_unscored.items():
            print(f"- {key}: {int(count)}")

    print("\nUnscored rows by currency:")
    if unscored_df.empty:
        print("- no unscored rows")
    else:
        unscored_currency_counts = unscored_df["currency"].astype(str).value_counts(dropna=False)
        for key, count in unscored_currency_counts.items():
            print(f"- {key}: {int(count)}")

    print("\nNon-scoreable reason groups:")
    if unscored_df.empty:
        print("- no unscored rows")
    else:
        unscored_reason_counts = unscored_df["score_reason"].astype(str).value_counts(dropna=False)
        for key, count in unscored_reason_counts.items():
            print(f"- {key}: {int(count)}")

    print("\nCalibration summary:")
    if "final_score" not in report_df.columns:
        print("- final_score column not present")
    else:
        numeric_scores = pd.to_numeric(
            report_df.loc[scored_mask, "final_score"], errors="coerce"
        ).dropna()
        if numeric_scores.empty:
            print("- no scored numeric rows")
        else:
            print("Net score distribution summary:")
            print(f"- count: {int(numeric_scores.count())}")
            print(f"- mean: {numeric_scores.mean():.4f}")
            print(f"- median: {numeric_scores.median():.4f}")
            print(f"- min: {numeric_scores.min():.4f}")
            print(f"- max: {numeric_scores.max():.4f}")
            print(f"- 25th percentile: {numeric_scores.quantile(0.25):.4f}")
            print(f"- 75th percentile: {numeric_scores.quantile(0.75):.4f}")

            abs_scores = numeric_scores.abs()
            print("\nSuggested neutral-zone baseline:")
            print(f"- median absolute score: {abs_scores.median():.4f}")
            print(f"- 25th percentile absolute score: {abs_scores.quantile(0.25):.4f}")
            print(f"- suggested neutral threshold: {abs_scores.quantile(0.25):.4f}")

    future_release_df = report_df.loc[
        report_df["event_type"].astype(str) == "future_release"
    ].copy()
    print("\nUpcoming-event pressure baseline:")
    if future_release_df.empty:
        print("- no future_release rows")
    else:
        print("Count by currency:")
        future_currency_counts = (
            future_release_df["currency"].astype(str).value_counts(dropna=False)
        )
        for key, count in future_currency_counts.items():
            print(f"- {key}: {int(count)}")

        print("Count by impact:")
        future_impact_counts = (
            future_release_df["impact"].astype(str).value_counts(dropna=False)
            if "impact" in future_release_df.columns
            else pd.Series(dtype=int)
        )
        if future_impact_counts.empty:
            print("- no impact column available")
        else:
            for key, count in future_impact_counts.items():
                print(f"- {key}: {int(count)}")

        print("Top future event names:")
        future_event_names = (
            future_release_df["event_name"].astype(str).value_counts(dropna=False).head(10)
        )
        for key, count in future_event_names.items():
            print(f"- {key}: {int(count)}")

    print("\nPublished output consistency checks:")
    published_data = {}
    for filename, required_columns in PUBLISHED_FILES.items():
        path = PUBLISHED_DIR / filename
        if not path.exists():
            print(f"- {filename}: missing")
            continue

        try:
            published_df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            print(f"- {filename}: empty")
            continue

        if published_df.empty:
            print(f"- {filename}: empty")
            continue

        published_data[filename] = published_df
        missing_cols = [col for col in required_columns if col not in published_df.columns]
        currencies = (
            normalize_currency_series(published_df["currency"]).dropna().unique()
            if "currency" in published_df.columns
            else []
        )
        currencies_text = ", ".join(sorted(currencies)) if len(currencies) else "N/A"
        dup_currency_count = 0
        if "currency" in published_df.columns:
            dup_currency_count = int(
                normalize_currency_series(published_df["currency"]).duplicated().sum()
            )

        print(f"- {filename}: present")
        print(f"  rows: {len(published_df)}")
        print(
            f"  required columns: {'ok' if not missing_cols else 'missing ' + ', '.join(missing_cols)}"
        )
        print(f"  currencies: {currencies_text}")
        print(f"  duplicate currencies: {dup_currency_count}")

        unexpected_currency_mask = (
            ~normalize_currency_series(published_df["currency"]).str.fullmatch(r"[A-Z]{3}")
            if "currency" in published_df.columns
            else pd.Series(dtype=bool)
        )
        if not unexpected_currency_mask.empty:
            unexpected_values = sorted(
                normalize_currency_series(published_df.loc[unexpected_currency_mask, "currency"])
                .dropna()
                .unique()
            )
            if unexpected_values:
                print(f"  unexpected currencies: {', '.join(unexpected_values)}")
            else:
                print("  unexpected currencies: none")
        else:
            print("  unexpected currencies: none")

    print("\nPublished file presence summary:")
    for filename in PUBLISHED_FILES:
        status = "exists" if filename in published_data else "missing"
        print(f"- {filename}: {status}")

    print("\nCurrency overlap checks:")
    signals_df = published_data.get("calendar_currency_signals_latest.csv")
    state_df = published_data.get("calendar_state_snapshot_latest.csv")
    macro_df = published_data.get("calendar_macro_context_latest.csv")

    def currency_set(df: pd.DataFrame | None) -> set[str]:
        if df is None or "currency" not in df.columns:
            return set()
        return set(normalize_currency_series(df["currency"]).dropna().unique())

    signals_currencies = currency_set(signals_df)
    state_currencies = currency_set(state_df)
    macro_currencies = currency_set(macro_df)

    if signals_df is None or state_df is None:
        print("- currency_signals vs state_snapshot: unavailable")
    else:
        overlap = sorted(signals_currencies & state_currencies)
        print(
            f"- currency_signals vs state_snapshot: overlap={len(overlap)} | {' ,'.join(overlap) if overlap else 'none'}"
        )

    if state_df is None or macro_df is None:
        print("- state_snapshot vs macro_context: unavailable")
    else:
        overlap = sorted(state_currencies & macro_currencies)
        print(
            f"- state_snapshot vs macro_context: overlap={len(overlap)} | {' ,'.join(overlap) if overlap else 'none'}"
        )

    print("\nOne-row-per-currency checks:")
    for filename in ["calendar_state_snapshot_latest.csv", "calendar_macro_context_latest.csv"]:
        df_pub = published_data.get(filename)
        if df_pub is None or "currency" not in df_pub.columns:
            print(f"- {filename}: unavailable")
            continue
        normalized = normalize_currency_series(df_pub["currency"])
        at_most_one = not normalized.duplicated().any()
        print(f"- {filename}: at_most_one_row_per_currency={at_most_one}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
