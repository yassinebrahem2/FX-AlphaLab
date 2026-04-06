from pathlib import Path

import pandas as pd

OUTPUT_PATH = Path("data/processed/calendar_reaction_live.csv")
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


def load_latest_scored_calendar() -> pd.DataFrame:
    candidates = sorted(
        [p for p in Path("data/processed").glob("*.csv") if "scored" in p.stem.lower()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError("No scored calendar CSV found in data/processed")

    df = pd.read_csv(candidates[0])
    if "score_available" not in df.columns:
        raise ValueError("score_available column missing from scored calendar CSV")

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc"]).reset_index(drop=True)
    return df


def load_prices(pair: str) -> pd.DataFrame:
    price_path = Path(f"data/raw/mt5/mt5_{pair}_H1_20260406.csv")
    if not price_path.exists():
        raise FileNotFoundError(f"Price file not found: {price_path}")

    prices = pd.read_csv(price_path)
    required_columns = {"time", "close"}
    if not required_columns.issubset(prices.columns):
        missing = required_columns - set(prices.columns)
        raise ValueError(f"Missing required price columns: {missing}")

    # Convert Unix timestamp (time column) to UTC datetime
    prices["timestamp_utc"] = pd.to_datetime(prices["time"], unit="s", utc=True, errors="coerce")
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices.dropna(subset=["timestamp_utc", "close"])
    prices = prices.sort_values("timestamp_utc").reset_index(drop=True)
    return prices


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


def main() -> None:
    events = load_latest_scored_calendar()

    scoreable = events[events["score_available"].fillna(False).astype(bool)].copy()
    if scoreable.empty:
        raise ValueError("No scoreable events found")

    scoreable["currency_norm"] = scoreable["currency"].astype(str).str.upper().str.strip()
    scoreable["pair"] = scoreable["currency_norm"].map(PAIR_MAP)
    scoreable = scoreable.dropna(subset=["pair"]).reset_index(drop=True)
    if scoreable.empty:
        raise ValueError("No scoreable events with supported currency mapping")

    event = scoreable.iloc[0]
    ts = event["timestamp_utc"]
    pair = str(event["pair"])

    prices = load_prices(pair)
    min_price_ts = prices["timestamp_utc"].min()
    max_price_ts = prices["timestamp_utc"].max()
    print(f"event_timestamp_utc={pd.Timestamp(ts).isoformat()}")
    print(f"min_price_timestamp_utc={pd.Timestamp(min_price_ts).isoformat()}")
    print(f"max_price_timestamp_utc={pd.Timestamp(max_price_ts).isoformat()}")

    has_bar_before = (prices["timestamp_utc"] < ts).any()
    has_bar_at_or_after = (prices["timestamp_utc"] >= ts).any()
    has_through_4h = max_price_ts >= (ts + pd.Timedelta(hours=4))

    # For live data, require bars before/at event but allow partial post-event window
    if not (has_bar_before and has_bar_at_or_after):
        raise ValueError("Insufficient price coverage for event window")

    if not has_through_4h:
        print(
            f"WARNING: Incomplete 4h window (have up to {pd.Timestamp(max_price_ts).isoformat()}, need {(ts + pd.Timedelta(hours=4)).isoformat()})"
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

    result = pd.DataFrame(
        [
            {
                "event_id": event.get("event_id"),
                "timestamp_utc": pd.Timestamp(ts).isoformat(),
                "event_name": event.get("event_name"),
                "currency": event.get("currency_norm"),
                "final_score": final_score_val,
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
                "reaction_explanation": reaction_explanation,
            }
        ]
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved live reaction row to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
