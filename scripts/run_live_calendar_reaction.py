import argparse
from pathlib import Path

import pandas as pd

OUTPUT_PATH = Path("data/processed/calendar_reaction_live.csv")
ENRICHED_OUTPUT_PATH = Path("data/processed/calendar_live_scored_reactions.csv")
PAIR_MAP = {
    "USD": "EURUSD",
    "EUR": "EURUSD",
    "GBP": "GBPUSD",
    "JPY": "USDJPY",
    "CHF": "USDCHF",
    "ES": "EURUSD",
    "DE": "EURUSD",
    "FR": "EURUSD",
    "IT": "EURUSD",
    "EU": "EURUSD",
}
TARGET_PAIRS = {"EURUSD", "GBPUSD", "USDCHF", "USDJPY"}


def load_latest_scored_calendar(scored_file: Path | None = None) -> pd.DataFrame:
    if scored_file is not None:
        if not scored_file.exists():
            raise FileNotFoundError(f"Scored calendar CSV not found: {scored_file}")
        selected = scored_file
    else:
        candidates = sorted(
            [p for p in Path("data/processed").glob("*.csv") if "scored" in p.stem.lower()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise FileNotFoundError("No scored calendar CSV found in data/processed")
        selected = candidates[0]

    df = pd.read_csv(selected)
    if "score_available" not in df.columns:
        raise ValueError("score_available column missing from scored calendar CSV")

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc"]).reset_index(drop=True)
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build live calendar reaction dataset")
    parser.add_argument(
        "--scored-file",
        type=Path,
        default=None,
        help="Explicit scored calendar CSV path (avoids latest-by-mtime drift)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PATH,
        help="Output CSV path for reaction rows",
    )
    parser.add_argument(
        "--enriched-output",
        type=Path,
        default=ENRICHED_OUTPUT_PATH,
        help="Output CSV path for reaction-enriched scored calendar",
    )
    return parser.parse_args()


def load_prices(pair: str) -> tuple[pd.DataFrame | None, int, int]:
    # Find all MT5 files for this pair and combine available history
    mt5_dir = Path("data/raw/mt5")
    candidates = sorted(
        [p for p in mt5_dir.glob(f"mt5_{pair}_H1_*.csv")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None, 0, 0

    required_columns = {"time", "close"}
    frames: list[pd.DataFrame] = []
    total_rows = 0

    for path in candidates:
        prices = pd.read_csv(path)
        total_rows += len(prices)
        if not required_columns.issubset(prices.columns):
            missing = required_columns - set(prices.columns)
            raise ValueError(f"Missing required price columns in {path.name}: {missing}")

        prices["timestamp_utc"] = pd.to_datetime(
            prices["time"], unit="s", utc=True, errors="coerce"
        )
        prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
        prices = prices.dropna(subset=["timestamp_utc", "close"])
        frames.append(prices[["timestamp_utc", "close"]])

    if not frames:
        return None, len(candidates), total_rows

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("timestamp_utc")
    combined = combined.drop_duplicates(subset=["timestamp_utc"], keep="last").reset_index(
        drop=True
    )
    return combined, len(candidates), total_rows


def get_last_price_at_or_before(prices: pd.DataFrame, ts: pd.Timestamp) -> float | None:
    subset = prices[prices["timestamp_utc"] <= ts]
    if subset.empty:
        return None
    return float(subset.iloc[-1]["close"])


def get_last_price_before(prices: pd.DataFrame, ts: pd.Timestamp) -> float | None:
    subset = prices[prices["timestamp_utc"] < ts]
    if subset.empty:
        return None
    return float(subset.iloc[-1]["close"])


def get_price_closest_to_event(prices: pd.DataFrame, ts: pd.Timestamp) -> float | None:
    if prices.empty:
        return None
    idx = (prices["timestamp_utc"] - ts).abs().idxmin()
    return float(prices.loc[idx, "close"])


def safe_return(price_now: float | None, price_future: float | None) -> float | None:
    if price_now is None or price_future is None or price_now == 0:
        return None
    return (price_future - price_now) / price_now


def explain_reaction(final_score: float | None, return_1h: float | None) -> str:
    if final_score is None or return_1h is None:
        return "No clear signal (missing data)"
    if final_score > 0 and return_1h < 0:
        return "Positive macro surprise with expected move"
    if final_score < 0 and return_1h > 0:
        return "Negative macro surprise with expected move"
    if final_score > 0 and return_1h > 0:
        return "Positive surprise but divergent move"
    if final_score < 0 and return_1h < 0:
        return "Negative surprise but divergent move"
    return "Neutral or weak reaction"


def get_expected_direction(final_score: float | None) -> str | None:
    """Return expected price direction based on macro score: UP/DOWN/NEUTRAL."""
    if final_score is None:
        return None
    if final_score > 0.01:
        return "UP"
    elif final_score < -0.01:
        return "DOWN"
    else:
        return "NEUTRAL"


def get_observed_direction(return_1h: float | None) -> str | None:
    """Return observed price direction in 1h window: UP/DOWN/NEUTRAL."""
    if return_1h is None:
        return None
    if return_1h > 0.0005:  # > 0.05% threshold
        return "UP"
    elif return_1h < -0.0005:  # < -0.05% threshold
        return "DOWN"
    else:
        return "NEUTRAL"


def direction_match(expected: str | None, observed: str | None) -> bool | None:
    """Check if expected and observed directions align (excluding NEUTRAL mismatches).

    Returns:
        True: Expected and observed directions match
        False: Expected and observed directions diverge
        None: Missing data or NEUTRAL signal (not enough information for comparison)

    None is expected when:
    - final_score is missing (expected_direction cannot be computed)
    - return_1h is missing (observed_direction cannot be computed)
    - Either direction is NEUTRAL (no clear signal)
    - Price data insufficient for the time window
    """
    if expected is None or observed is None:
        return None
    if expected == "NEUTRAL" or observed == "NEUTRAL":
        return None
    return expected == observed


def main() -> None:
    args = parse_args()
    events = load_latest_scored_calendar(args.scored_file)

    scoreable = events[events["score_available"].fillna(False).astype(bool)].copy()
    if scoreable.empty:
        raise ValueError("No scoreable events found")

    scoreable["currency_norm"] = scoreable["currency"].astype(str).str.upper().str.strip()
    scoreable["pair"] = scoreable["currency_norm"].map(PAIR_MAP)
    scoreable = scoreable.dropna(subset=["pair"]).reset_index(drop=True)
    scoreable = scoreable[scoreable["pair"].isin(TARGET_PAIRS)].reset_index(drop=True)
    if scoreable.empty:
        raise ValueError("No scoreable events with supported currency mapping")

    pair_prices: dict[str, pd.DataFrame | None] = {}
    unique_pairs = sorted(scoreable["pair"].unique())
    for pair in unique_pairs:
        prices, file_count, total_rows = load_prices(pair)
        pair_prices[pair] = prices
        print(f"MT5 coverage {pair}: files={file_count}, rows={total_rows}")

    results = []

    for idx, event in scoreable.iterrows():
        ts = event["timestamp_utc"]
        pair = str(event["pair"])

        prices = pair_prices.get(pair)
        if prices is None or prices.empty:
            print(f"SKIP: {event['event_name']} ({pair}) - no MT5 file found")
            continue

        max_price_ts = prices["timestamp_utc"].max()

        has_bar_before = (prices["timestamp_utc"] < ts).any()
        has_bar_at_or_after = (prices["timestamp_utc"] >= ts).any()
        has_through_4h = max_price_ts >= (ts + pd.Timedelta(hours=4))

        # For live data, require bars before/at event but allow partial post-event window
        if not (has_bar_before and has_bar_at_or_after):
            print(
                f"SKIP: {event['event_name']} ({pair}) - insufficient price coverage before/at event"
            )
            continue

        if not has_through_4h:
            print(
                f"WARN: {event['event_name']} ({pair}) - incomplete 4h window (have up to {pd.Timestamp(max_price_ts).isoformat()})"
            )

        before_subset = prices[prices["timestamp_utc"] < ts]
        matched_price_timestamp_before = (
            pd.Timestamp(before_subset.iloc[-1]["timestamp_utc"]).isoformat()
            if not before_subset.empty
            else None
        )

        idx_at_event = (prices["timestamp_utc"] - ts).abs().idxmin() if not prices.empty else None
        matched_price_timestamp_at_event = (
            pd.Timestamp(prices.loc[idx_at_event, "timestamp_utc"]).isoformat()
            if idx_at_event is not None
            else None
        )

        one_h_subset = prices[prices["timestamp_utc"] <= ts + pd.Timedelta(hours=1)]
        matched_price_timestamp_1h = (
            pd.Timestamp(one_h_subset.iloc[-1]["timestamp_utc"]).isoformat()
            if not one_h_subset.empty
            else None
        )

        four_h_subset = prices[prices["timestamp_utc"] <= ts + pd.Timedelta(hours=4)]
        matched_price_timestamp_4h = (
            pd.Timestamp(four_h_subset.iloc[-1]["timestamp_utc"]).isoformat()
            if not four_h_subset.empty
            else None
        )

        price_before_event = get_last_price_before(prices, ts)
        price_at_event = get_price_closest_to_event(prices, ts)
        price_1h_after = get_last_price_at_or_before(prices, ts + pd.Timedelta(hours=1))
        price_4h_after = get_last_price_at_or_before(prices, ts + pd.Timedelta(hours=4))

        return_1h = safe_return(price_at_event, price_1h_after)
        return_4h = safe_return(price_at_event, price_4h_after)

        final_score = pd.to_numeric(pd.Series([event.get("final_score")]), errors="coerce").iloc[0]
        final_score_val = float(final_score) if pd.notna(final_score) else None

        matched_timestamps = [
            matched_price_timestamp_before,
            matched_price_timestamp_at_event,
            matched_price_timestamp_1h,
            matched_price_timestamp_4h,
        ]
        all_matched_identical = all(ts_val is not None for ts_val in matched_timestamps) and (
            len(set(matched_timestamps)) == 1
        )
        if all_matched_identical:
            reaction_explanation = "No measurable reaction at current price resolution"
        else:
            reaction_explanation = explain_reaction(final_score_val, return_1h)

        # Calculate direction fields (1h and 4h)
        expected_direction = get_expected_direction(final_score_val)
        observed_direction_1h = get_observed_direction(return_1h)
        direction_match_1h = direction_match(expected_direction, observed_direction_1h)

        observed_direction_4h = get_observed_direction(return_4h)
        direction_match_4h = direction_match(expected_direction, observed_direction_4h)

        result_row = {
            "event_id": event.get("event_id"),
            "timestamp_utc": pd.Timestamp(ts).isoformat(),
            "event_name": event.get("event_name"),
            "currency": event.get("currency_norm"),
            "actual": event.get("actual"),
            "forecast": event.get("forecast"),
            "final_score": final_score_val,
            "score_available": event.get("score_available"),
            "filtered_score": event.get("filtered_score"),
            "event_type": event.get("event_type"),
            "impact": event.get("impact"),
            "pair": pair,
            "price_before_event": price_before_event,
            "matched_price_timestamp_before": matched_price_timestamp_before,
            "price_at_event": price_at_event,
            "matched_price_timestamp_at_event": matched_price_timestamp_at_event,
            "price_1h_after": price_1h_after,
            "matched_price_timestamp_1h": matched_price_timestamp_1h,
            "price_4h_after": price_4h_after,
            "matched_price_timestamp_4h": matched_price_timestamp_4h,
            "return_1h": return_1h,
            "return_4h": return_4h,
            "reaction_1h": return_1h,
            "reaction_4h": return_4h,
            "reaction_explanation": reaction_explanation,
            "expected_direction": expected_direction,
            "observed_direction_1h": observed_direction_1h,
            "observed_direction_4h": observed_direction_4h,
            "direction_match_1h": direction_match_1h,
            "direction_match_4h": direction_match_4h,
        }
        results.append(result_row)

    if not results:
        raise ValueError("No valid reaction data generated for any events")

    result_df = pd.DataFrame(results)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    usable_1h_labels = int(result_df["direction_match_1h"].notna().sum())
    usable_4h_labels = int(result_df["direction_match_4h"].notna().sum())
    print(f"Result rows: {len(result_df)}")
    print("Rows per pair:")
    for pair, count in result_df["pair"].value_counts().sort_index().items():
        print(f"- {pair}: {int(count)}")
    print(f"Usable 1h labels: {usable_1h_labels}")
    print(f"Usable 4h labels: {usable_4h_labels}")

    # Export complete reaction data with all direction fields
    result_df.to_csv(args.output, index=False)

    # Remove old reaction columns from events before merging fresh data
    old_reaction_cols = [
        col
        for col in events.columns
        if col.endswith("_x")
        or col.endswith("_y")
        or col
        in [
            "reaction_1h",
            "reaction_4h",
            "reaction_explanation",
            "expected_direction",
            "observed_direction",
            "direction_match",
        ]
    ]
    events_clean = events.drop(columns=old_reaction_cols, errors="ignore")

    enriched_df = events_clean.merge(
        result_df[
            [
                "event_id",
                "reaction_1h",
                "reaction_4h",
                "reaction_explanation",
                "expected_direction",
                "observed_direction_1h",
                "observed_direction_4h",
                "direction_match_1h",
                "direction_match_4h",
            ]
        ],
        on="event_id",
        how="left",
    )
    args.enriched_output.parent.mkdir(parents=True, exist_ok=True)
    enriched_df.to_csv(args.enriched_output, index=False)

    print(f"Saved {len(result_df)} live reaction rows to {args.output}")
    print(f"Saved reaction-enriched scored calendar to {args.enriched_output}")


if __name__ == "__main__":
    main()
