from pathlib import Path

import pandas as pd


EVENTS_PATH = Path("data/processed/calendar_fixture_scored.csv")
PRICES_PATH = Path("tests/fixtures/calendar/sample_eurusd_prices.csv")
OUTPUT_PATH = Path("data/processed/calendar_reaction_demo.csv")


def load_events(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    return df


def load_prices(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    return df.sort_values(["pair", "timestamp_utc"]).reset_index(drop=True)


def get_last_price_at_or_before(prices: pd.DataFrame, ts: pd.Timestamp) -> float | None:
    subset = prices[prices["timestamp_utc"] <= ts]
    if subset.empty:
        return None
    return float(subset.iloc[-1]["close"])


def get_last_price_at_or_before_in_window(
    prices: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> float | None:
    subset = prices[
        (prices["timestamp_utc"] >= start_ts) & (prices["timestamp_utc"] <= end_ts)
    ]
    if subset.empty:
        return None
    return float(subset.iloc[-1]["close"])


def get_window_stats(
    prices: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> tuple[float | None, float | None]:
    subset = prices[
        (prices["timestamp_utc"] >= start_ts) & (prices["timestamp_utc"] <= end_ts)
    ]
    if subset.empty:
        return None, None
    return float(subset["close"].max()), float(subset["close"].min())


def safe_return(price_now: float | None, price_future: float | None) -> float | None:
    if price_now is None or price_future is None or price_now == 0:
        return None
    return (price_future - price_now) / price_now


def explain_reaction(final_score: float | None, return_1h: float | None) -> str:
    if final_score is None or return_1h is None:
        return "No clear signal (missing data)"

    if final_score > 0 and return_1h < 0:
        return "Positive macro surprise -> currency strengthened -> pair moved down (expected)"
    elif final_score < 0 and return_1h > 0:
        return "Negative macro surprise -> currency weakened -> pair moved up (expected)"
    elif final_score > 0 and return_1h > 0:
        return "Positive surprise but price moved up -> possible market divergence"
    elif final_score < 0 and return_1h < 0:
        return "Negative surprise but price moved down -> possible market divergence"
    else:
        return "Neutral or weak reaction"


def map_event_to_pair(currency: str) -> str | None:
    mapping = {
        "USD": "EURUSD",
        "EUR": "EURUSD",
        "GBP": "EURUSD",
        "JPY": "EURUSD",
    }
    return mapping.get(currency)


def main() -> None:
    events = load_events(EVENTS_PATH)
    prices = load_prices(PRICES_PATH)

    results = []

    for _, event in events.iterrows():
        ts = event["timestamp_utc"]
        currency = event["currency"]
        pair = map_event_to_pair(currency)

        if pair is None:
            continue

        pair_prices = prices[prices["pair"] == pair].copy()
        if pair_prices.empty:
            continue

        one_hour_before = ts - pd.Timedelta(hours=1)
        one_hour_after = ts + pd.Timedelta(hours=1)
        four_hours_after = ts + pd.Timedelta(hours=4)

        price_before_event = get_last_price_at_or_before(pair_prices, one_hour_before)
        price_at_event = get_last_price_at_or_before(pair_prices, ts)
        price_1h_after = get_last_price_at_or_before(pair_prices, one_hour_after)
        price_4h_after = get_last_price_at_or_before(pair_prices, four_hours_after)

        max_price_4h, min_price_4h = get_window_stats(pair_prices, ts, four_hours_after)

        return_1h = safe_return(price_at_event, price_1h_after)
        return_4h = safe_return(price_at_event, price_4h_after)

        max_move_up_4h = safe_return(price_at_event, max_price_4h)
        max_move_down_4h = safe_return(price_at_event, min_price_4h)

        results.append(
            {
                "event_id": event["event_id"],
                "timestamp_utc": ts.isoformat(),
                "currency": currency,
                "event_name": event["event_name"],
                "final_score": event.get("final_score"),
                "affected_pairs": event.get("affected_pairs"),
                "reaction_explanation": explain_reaction(event.get("final_score"), return_1h),
                "pair": pair,
                "price_before_event": price_before_event,
                "price_at_event": price_at_event,
                "price_1h_after": price_1h_after,
                "price_4h_after": price_4h_after,
                "return_1h": return_1h,
                "return_4h": return_4h,
                "max_price_4h": max_price_4h,
                "min_price_4h": min_price_4h,
                "max_move_up_4h": max_move_up_4h,
                "max_move_down_4h": max_move_down_4h,
            }
        )

    reaction_df = pd.DataFrame(results)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    reaction_df.to_csv(OUTPUT_PATH, index=False)

    print(f"Loaded {len(events)} scored events")
    print(f"Loaded {len(prices)} price rows")
    print(f"Computed reactions for {len(reaction_df)} event-pair rows")
    print("Saved demo results to data/processed/calendar_reaction_demo.csv")

    if not reaction_df.empty:
        example = reaction_df.iloc[0]

        print("\n--- Example Interpretation ---")
        print(f"Event: {example['event_name']} ({example['currency']})")

        if pd.notna(example["final_score"]):
            print(f"Score: {float(example['final_score']):.4f}")
        else:
            print("Score: N/A")

        if pd.notna(example["return_1h"]):
            print(f"Return 1h: {float(example['return_1h']):.5f}")
        else:
            print("Return 1h: N/A")

        print(f"Explanation: {example['reaction_explanation']}")

if __name__ == "__main__":
    main()