from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path("data/processed")
SUMMARY_OUTPUT_PATH = PROCESSED_DIR / "calendar_validation_summary.csv"
BY_CURRENCY_OUTPUT_PATH = PROCESSED_DIR / "calendar_validation_by_currency.csv"
BY_EVENT_TYPE_OUTPUT_PATH = PROCESSED_DIR / "calendar_validation_by_event_type.csv"
CALIBRATION_COMPARISON_OUTPUT_PATH = (
    PROCESSED_DIR / "calendar_validation_calibration_comparison.csv"
)
REQUIRED_COLUMNS = ["currency", "event_name", "impact", "final_score", "score_available"]
OPTIONAL_REACTION_COLUMNS = ["reaction_1h", "reaction_4h"]


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


def is_score_available(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def direction_from_value(value: object) -> str | None:
    if pd.isna(value):
        return None

    numeric_value = float(value)
    if numeric_value > 0:
        return "positive"
    if numeric_value < 0:
        return "negative"
    return None


def match_rate_for_group(group_df: pd.DataFrame, reaction_column: str) -> tuple[int, float | None]:
    if reaction_column not in group_df.columns:
        return 0, None

    horizon_df = group_df.loc[group_df[reaction_column].notna()].copy()
    horizon_df["observed_direction"] = horizon_df[reaction_column].apply(direction_from_value)
    horizon_df = horizon_df[horizon_df["observed_direction"].notna()]
    if horizon_df.empty:
        return 0, None

    match_rate = (
        horizon_df["expected_direction"] == horizon_df["observed_direction"]
    ).mean() * 100.0
    return len(horizon_df), float(match_rate)


def directional_pass_metrics(df: pd.DataFrame, score_column: str) -> dict[str, int | float | None]:
    directional_df = df.loc[df[score_column] != 0].copy()
    directional_df["expected_direction"] = directional_df[score_column].apply(direction_from_value)
    directional_df = directional_df[directional_df["expected_direction"].notna()]

    usable_1h, match_1h = match_rate_for_group(directional_df, "reaction_1h")
    usable_4h, match_4h = match_rate_for_group(directional_df, "reaction_4h")

    return {
        "direction_1h_usable_rows": usable_1h,
        "direction_1h_match_rate": match_1h,
        "direction_4h_usable_rows": usable_4h,
        "direction_4h_match_rate": match_4h,
        "directional_rows": int(len(directional_df)),
    }


def main() -> int:
    input_path = find_latest_scored_calendar_file(PROCESSED_DIR)
    if input_path is None:
        print("No scored calendar file found in data/processed")
        return 1

    df = load_csv_safe(input_path)
    if df is None:
        return 1

    missing_required = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_required:
        print(f"Missing required columns in {input_path}: {', '.join(missing_required)}")
        return 1

    missing_optional = [column for column in OPTIONAL_REACTION_COLUMNS if column not in df.columns]
    if missing_optional:
        print(f"Optional reaction columns missing in {input_path}: {', '.join(missing_optional)}")

    report_df = df.copy()
    report_df["currency"] = report_df["currency"].astype(str).str.strip().str.upper()
    report_df["final_score"] = pd.to_numeric(report_df["final_score"], errors="coerce")

    if "filtered_score" not in report_df.columns:
        print("Missing required column for filtered validation: filtered_score")
        print("Exiting safely without running filtered validation pass.")
        return 0
    report_df["filtered_score"] = pd.to_numeric(report_df["filtered_score"], errors="coerce")

    scored_mask = is_score_available(report_df["score_available"])
    usable_mask = scored_mask & report_df["final_score"].notna()
    usable_df = report_df.loc[usable_mask].copy()

    total_rows = len(report_df)
    scored_usable_rows = len(usable_df)
    reaction_1h_rows = (
        int(report_df["reaction_1h"].notna().sum()) if "reaction_1h" in report_df.columns else 0
    )
    reaction_4h_rows = (
        int(report_df["reaction_4h"].notna().sum()) if "reaction_4h" in report_df.columns else 0
    )

    currencies = sorted(
        curr for curr in usable_df["currency"].dropna().unique() if str(curr).strip()
    )
    currencies_text = ", ".join(currencies) if currencies else "N/A"

    if "event_type" in report_df.columns:
        event_types = sorted(
            value
            for value in report_df["event_type"].astype(str).dropna().unique()
            if str(value).strip()
        )
        event_types_text = ", ".join(event_types) if event_types else "N/A"
    else:
        event_types_text = "N/A"

    impact_counts = report_df["impact"].astype(str).value_counts(dropna=False)

    print(f"Input file: {input_path}")
    print(f"Total rows: {total_rows}")
    print(f"Scored usable rows: {scored_usable_rows}")
    print(f"Rows with reaction_1h available: {reaction_1h_rows}")
    print(f"Rows with reaction_4h available: {reaction_4h_rows}")
    print(f"Currencies present: {currencies_text}")
    print(f"Event types present: {event_types_text}")
    print("Impact distribution:")
    for key, count in impact_counts.items():
        print(f"- {key}: {int(count)}")

    directional_df = usable_df.loc[usable_df["final_score"] != 0].copy()
    directional_df["expected_direction"] = directional_df["final_score"].apply(direction_from_value)
    strong_score_threshold = (
        directional_df["final_score"].abs().median() if not directional_df.empty else float("nan")
    )

    original_metrics = directional_pass_metrics(usable_df, "final_score")
    filtered_metrics = directional_pass_metrics(usable_df, "filtered_score")
    rows_removed_by_filtering = int(
        original_metrics["directional_rows"] - filtered_metrics["directional_rows"]
    )

    delta_1h = None
    if (
        original_metrics["direction_1h_match_rate"] is not None
        and filtered_metrics["direction_1h_match_rate"] is not None
    ):
        delta_1h = float(filtered_metrics["direction_1h_match_rate"]) - float(
            original_metrics["direction_1h_match_rate"]
        )

    delta_4h = None
    if (
        original_metrics["direction_4h_match_rate"] is not None
        and filtered_metrics["direction_4h_match_rate"] is not None
    ):
        delta_4h = float(filtered_metrics["direction_4h_match_rate"]) - float(
            original_metrics["direction_4h_match_rate"]
        )

    print("\nOriginal directional pass (final_score):")
    print("- primary horizon: 1h")
    print("- secondary horizon: 4h")
    print(f"- usable directional rows for 1h: {original_metrics['direction_1h_usable_rows']}")
    print(
        "- direction match rate for 1h: "
        + (
            "N/A"
            if original_metrics["direction_1h_match_rate"] is None
            else f"{float(original_metrics['direction_1h_match_rate']):.2f}%"
        )
    )
    print(f"- usable directional rows for 4h: {original_metrics['direction_4h_usable_rows']}")
    print(
        "- direction match rate for 4h: "
        + (
            "N/A"
            if original_metrics["direction_4h_match_rate"] is None
            else f"{float(original_metrics['direction_4h_match_rate']):.2f}%"
        )
    )

    print("\nFiltered directional pass (filtered_score, excluding zeros):")
    print("- primary horizon: 1h")
    print("- secondary horizon: 4h")
    print(f"- usable directional rows for 1h: {filtered_metrics['direction_1h_usable_rows']}")
    print(
        "- direction match rate for 1h: "
        + (
            "N/A"
            if filtered_metrics["direction_1h_match_rate"] is None
            else f"{float(filtered_metrics['direction_1h_match_rate']):.2f}%"
        )
    )
    print(f"- usable directional rows for 4h: {filtered_metrics['direction_4h_usable_rows']}")
    print(
        "- direction match rate for 4h: "
        + (
            "N/A"
            if filtered_metrics["direction_4h_match_rate"] is None
            else f"{float(filtered_metrics['direction_4h_match_rate']):.2f}%"
        )
    )

    print("\nComparison:")
    print(f"- delta match rate 1h: {'N/A' if delta_1h is None else f'{delta_1h:+.2f} pp'}")
    print(f"- delta match rate 4h: {'N/A' if delta_4h is None else f'{delta_4h:+.2f} pp'}")
    print(f"- rows removed by low-signal filtering: {rows_removed_by_filtering}")

    comparison_rows = [
        {
            "signal_version": "original",
            "horizon": "1h",
            "usable_rows": int(original_metrics["direction_1h_usable_rows"]),
            "match_rate": original_metrics["direction_1h_match_rate"],
        },
        {
            "signal_version": "original",
            "horizon": "4h",
            "usable_rows": int(original_metrics["direction_4h_usable_rows"]),
            "match_rate": original_metrics["direction_4h_match_rate"],
        },
        {
            "signal_version": "filtered",
            "horizon": "1h",
            "usable_rows": int(filtered_metrics["direction_1h_usable_rows"]),
            "match_rate": filtered_metrics["direction_1h_match_rate"],
        },
        {
            "signal_version": "filtered",
            "horizon": "4h",
            "usable_rows": int(filtered_metrics["direction_4h_usable_rows"]),
            "match_rate": filtered_metrics["direction_4h_match_rate"],
        },
    ]
    comparison_df = pd.DataFrame(
        comparison_rows,
        columns=["signal_version", "horizon", "usable_rows", "match_rate"],
    )

    if (
        original_metrics["direction_1h_match_rate"] is not None
        and filtered_metrics["direction_1h_match_rate"] is not None
        and float(filtered_metrics["direction_1h_match_rate"])
        > float(original_metrics["direction_1h_match_rate"])
    ):
        print("Calibration improved 1h signal")
    else:
        print("Calibration did not improve 1h signal")

    print("\nDirectional validation:")
    direction_summary = {
        "direction_1h_usable_rows": 0,
        "direction_1h_match_rate": None,
        "direction_4h_usable_rows": 0,
        "direction_4h_match_rate": None,
    }
    for horizon in ["1h", "4h"]:
        reaction_column = f"reaction_{horizon}"
        if reaction_column not in report_df.columns:
            print(f"- usable directional rows for {horizon}: 0")
            print(f"- direction match rate for {horizon}: N/A")
            continue

        usable_count, match_rate = match_rate_for_group(directional_df, reaction_column)
        direction_summary[f"direction_{horizon}_usable_rows"] = usable_count
        direction_summary[f"direction_{horizon}_match_rate"] = match_rate
        if usable_count == 0 or match_rate is None:
            print(f"- usable directional rows for {horizon}: 0")
            print(f"- direction match rate for {horizon}: N/A")
        else:
            print(f"- usable directional rows for {horizon}: {usable_count}")
            print(f"- direction match rate for {horizon}: {match_rate:.2f}%")

    print("\nDirectional validation by currency:")
    for currency, currency_df in directional_df.groupby("currency", dropna=False):
        currency_df = currency_df.copy()
        print(f"- {currency}: count={len(currency_df)}")
        for horizon in ["1h", "4h"]:
            reaction_column = f"reaction_{horizon}"
            if reaction_column not in currency_df.columns:
                print(f"  {horizon} match rate: N/A")
                continue

            usable_count, match_rate = match_rate_for_group(currency_df, reaction_column)
            if usable_count == 0 or match_rate is None:
                print(f"  {horizon} match rate: N/A")
            else:
                print(f"  {horizon} match rate: {match_rate:.2f}%")

    print("\nSegmented validation summaries:")
    if directional_df.empty:
        print("- no usable directional rows")
    else:

        def print_segment_summary(title: str, group_col: str) -> None:
            print(f"\n{title}:")
            grouped = directional_df.groupby(group_col, dropna=False)
            if grouped.ngroups == 0:
                print("- no usable directional rows")
                return

            rows = []
            for group_value, group_df in grouped:
                row = {group_col: group_value, "count": len(group_df)}
                for horizon in ["1h", "4h"]:
                    reaction_column = f"reaction_{horizon}"
                    usable_count, match_rate = match_rate_for_group(group_df, reaction_column)
                    row[f"{horizon}_rate"] = match_rate if usable_count else None
                rows.append(row)

            for row in rows:
                print(f"- {row[group_col]}: count={row['count']}")
                for horizon in ["1h", "4h"]:
                    rate = row[f"{horizon}_rate"]
                    print(f"  {horizon} match rate: {'N/A' if rate is None else f'{rate:.2f}%'}")

        print("By event_name (top 10 by usable row count):")
        event_name_rows = []
        for event_name, event_df in directional_df.groupby("event_name", dropna=False):
            event_name_rows.append((event_name, len(event_df), event_df))
        event_name_rows = sorted(event_name_rows, key=lambda item: item[1], reverse=True)[:10]
        if not event_name_rows:
            print("- no usable directional rows")
        else:
            for event_name, count, event_df in event_name_rows:
                print(f"- {event_name}: count={count}")
                for horizon in ["1h", "4h"]:
                    reaction_column = f"reaction_{horizon}"
                    usable_count, match_rate = match_rate_for_group(event_df, reaction_column)
                    if usable_count == 0 or match_rate is None:
                        print(f"  {horizon} match rate: N/A")
                    else:
                        print(f"  {horizon} match rate: {match_rate:.2f}%")

        print("\nBy impact:")
        print_segment_summary("Impact groups", "impact")

        directional_df["score_strength_bucket"] = (
            directional_df["final_score"]
            .abs()
            .apply(lambda value: "weak" if value < strong_score_threshold else "strong")
        )
        print("\nBy score strength bucket:")
        print_segment_summary("Score strength buckets", "score_strength_bucket")

    if directional_df.empty:
        print("- no usable directional rows")

    summary_df = pd.DataFrame(
        [
            {
                "total_rows": total_rows,
                "scored_usable_rows": scored_usable_rows,
                "reaction_1h_available_rows": reaction_1h_rows,
                "reaction_4h_available_rows": reaction_4h_rows,
                "direction_1h_usable_rows": direction_summary["direction_1h_usable_rows"],
                "direction_1h_match_rate": direction_summary["direction_1h_match_rate"],
                "direction_4h_usable_rows": direction_summary["direction_4h_usable_rows"],
                "direction_4h_match_rate": direction_summary["direction_4h_match_rate"],
                "strong_score_threshold": strong_score_threshold,
            }
        ]
    )
    by_currency_df = []
    for currency, currency_df in directional_df.groupby("currency", dropna=False):
        row = {"currency": currency}
        for horizon in ["1h", "4h"]:
            usable_count, match_rate = match_rate_for_group(currency_df, f"reaction_{horizon}")
            row[f"usable_rows_{horizon}"] = usable_count
            row[f"match_rate_{horizon}"] = match_rate
        by_currency_df.append(row)
    by_currency_df = pd.DataFrame(
        by_currency_df,
        columns=["currency", "usable_rows_1h", "match_rate_1h", "usable_rows_4h", "match_rate_4h"],
    )

    event_type_segment_col = "event_type" if "event_type" in report_df.columns else "impact"
    if event_type_segment_col == "impact":
        print("\nEvent type not available; using impact as fallback for segment export.")
    by_event_type_df = []
    for segment, segment_df in directional_df.groupby(event_type_segment_col, dropna=False):
        row = {"segment": segment}
        for horizon in ["1h", "4h"]:
            usable_count, match_rate = match_rate_for_group(segment_df, f"reaction_{horizon}")
            row[f"usable_rows_{horizon}"] = usable_count
            row[f"match_rate_{horizon}"] = match_rate
        by_event_type_df.append(row)
    by_event_type_df = pd.DataFrame(
        by_event_type_df,
        columns=["segment", "usable_rows_1h", "match_rate_1h", "usable_rows_4h", "match_rate_4h"],
    )

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(SUMMARY_OUTPUT_PATH, index=False)
    by_currency_df.to_csv(BY_CURRENCY_OUTPUT_PATH, index=False)
    by_event_type_df.to_csv(BY_EVENT_TYPE_OUTPUT_PATH, index=False)
    comparison_df.to_csv(CALIBRATION_COMPARISON_OUTPUT_PATH, index=False)

    print("\nSaved validation outputs:")
    print(f"- {SUMMARY_OUTPUT_PATH}")
    print(f"- {BY_CURRENCY_OUTPUT_PATH}")
    print(f"- {BY_EVENT_TYPE_OUTPUT_PATH}")
    print(f"- {CALIBRATION_COMPARISON_OUTPUT_PATH}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
