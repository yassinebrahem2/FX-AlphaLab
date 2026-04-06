from pathlib import Path

import pandas as pd

from src.ingestion.calendar_interface import (
    get_currency_calendar_state,
    load_calendar_state_snapshot,
)

MACRO_CONTEXT_OUTPUT_PATH = Path("data/published/calendar/calendar_macro_context_latest.csv")


def format_net(value: object) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):+.2f}"


def format_next_high(value: object) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):.1f}h"


def main() -> int:
    snapshot = load_calendar_state_snapshot()
    if snapshot.empty:
        print("Snapshot load check failed: calendar state snapshot is empty")
        return 1
    print(f"Snapshot load check passed: {len(snapshot)} rows")

    for currency in ["USD", "EUR", "GBP", "JPY"]:
        state = get_currency_calendar_state(currency)

        if state.empty:
            print(f"{currency} | no data")
            continue

        row = state.iloc[0]
        print(
            f"{currency} | net={format_net(row.get('recent_net_macro_score'))} | "
            f"bias={row.get('macro_bias_label', 'N/A')} | "
            f"pressure={row.get('calendar_pressure_label', 'N/A')} | "
            f"state={row.get('calendar_state_label', 'N/A')} | "
            f"next_high={format_next_high(row.get('time_to_next_high_impact_event_hours'))}"
        )

    print("\nCase-insensitive currency input checks:")
    usd_upper = get_currency_calendar_state("USD")
    usd_lower = get_currency_calendar_state("usd")
    usd_mixed = get_currency_calendar_state("Usd")
    lower_match = usd_lower.equals(usd_upper)
    mixed_match = usd_mixed.equals(usd_upper)
    print(f"usd matches USD: {lower_match}")
    print(f"Usd matches USD: {mixed_match}")

    missing = get_currency_calendar_state("XXX")
    if missing.empty:
        print("XXX | no data")
    else:
        print("XXX | unexpected data found")

    output_columns = [
        "currency",
        "macro_bias_label",
        "calendar_pressure_label",
        "calendar_state_label",
        "recent_net_macro_score",
        "time_to_next_high_impact_event_hours",
    ]
    macro_context = snapshot.loc[:, output_columns].copy()
    macro_context["currency"] = macro_context["currency"].astype(str).str.strip().str.upper()

    MACRO_CONTEXT_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    macro_context.to_csv(MACRO_CONTEXT_OUTPUT_PATH, index=False)

    currencies = sorted(
        curr for curr in macro_context["currency"].dropna().unique() if str(curr).strip()
    )
    currencies_text = ", ".join(currencies) if currencies else "N/A"
    print(f"\nSaved macro context to: {MACRO_CONTEXT_OUTPUT_PATH}")
    print(f"Rows written: {len(macro_context)}")
    print(f"Currencies included: {currencies_text}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
