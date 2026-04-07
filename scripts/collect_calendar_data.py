import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# ONLY import what you need
from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector
from src.ingestion.collectors.tradingeconomics_collector import (
    TradingEconomicsCalendarCollector,
)
from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor

TARGET_CURRENCIES = {"EUR", "GBP", "CHF", "JPY"}
DEFAULT_LIVE_LOOKBACK_DAYS = 90

RAW_OUTPUT_DIR = Path("data/raw/calendar")


def _empty_calendar_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "time",
            "currency",
            "event",
            "impact",
            "forecast",
            "previous",
            "actual",
            "event_url",
            "source_url",
            "scraped_at",
            "source",
        ]
    )


def _ensure_calendar_columns(df: pd.DataFrame) -> pd.DataFrame:
    canonical_cols = [
        "date",
        "time",
        "currency",
        "event",
        "impact",
        "forecast",
        "previous",
        "actual",
        "event_url",
        "source_url",
        "scraped_at",
        "source",
    ]
    if df.empty:
        return _empty_calendar_frame()

    normalized = df.copy()
    for col in canonical_cols:
        if col not in normalized.columns:
            normalized[col] = pd.NA

    return normalized


def _save_raw_calendar(df: pd.DataFrame, source: str) -> Path:
    RAW_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = RAW_OUTPUT_DIR / f"{source}_{timestamp}.csv"
    df.to_csv(output_path, index=False)
    return output_path


def _collect_forexfactory(start_date: datetime | None, end_date: datetime | None) -> pd.DataFrame:
    collector = ForexFactoryCalendarCollector()
    data = collector.collect(start_date=start_date, end_date=end_date)
    if "calendar" not in data:
        return _empty_calendar_frame()
    return _ensure_calendar_columns(data["calendar"])


def _collect_tradingeconomics(
    start_date: datetime | None,
    end_date: datetime | None,
) -> pd.DataFrame:
    collector = TradingEconomicsCalendarCollector()
    data = collector.collect(start_date=start_date, end_date=end_date)
    if "calendar" not in data:
        return _empty_calendar_frame()
    return _ensure_calendar_columns(data["calendar"])


def _collect_combined(start_date: datetime | None, end_date: datetime | None) -> pd.DataFrame:
    frames = []
    ff_df = _collect_forexfactory(start_date, end_date)
    te_df = _collect_tradingeconomics(start_date, end_date)

    if not ff_df.empty:
        frames.append(ff_df)
    if not te_df.empty:
        frames.append(te_df)

    if not frames:
        return _empty_calendar_frame()

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = add_source_matching_annotations(combined)[0]
    return _ensure_calendar_columns(combined)


def collect_calendar_source(
    source: str,
    start_date: datetime | None,
    end_date: datetime | None,
) -> pd.DataFrame:
    source = source.lower().strip()

    if source in {"live", "forexfactory"}:
        df = _collect_forexfactory(start_date, end_date)
    elif source == "tradingeconomics":
        df = _collect_tradingeconomics(start_date, end_date)
    elif source == "combined":
        df = _collect_combined(start_date, end_date)
    else:
        raise ValueError(f"Unsupported source: {source}")

    if "currency" in df.columns:
        df["currency"] = df["currency"].astype(str).str.upper().str.strip()
        df = df[df["currency"].isin(TARGET_CURRENCIES)].reset_index(drop=True)

    return df


def load_fred_secondary_events(
    start_date: datetime | None,
    end_date: datetime | None,
) -> pd.DataFrame | None:
    """Load existing FRED macro rows and map to calendar-like schema."""
    fred_path = Path("data/raw/macro/fred_macro.csv")
    if not fred_path.exists():
        print("Secondary source unavailable: data/raw/macro/fred_macro.csv not found")
        return None

    try:
        fred_df = pd.read_csv(fred_path)
    except Exception as exc:
        print(f"Secondary source unavailable: failed to read FRED data ({exc})")
        return None

    required_cols = ["release_datetime_utc", "currency", "indicator", "actual_value"]
    missing = [col for col in required_cols if col not in fred_df.columns]
    if missing:
        print(f"Secondary source unavailable: missing FRED columns: {', '.join(missing)}")
        return None

    release_dt = pd.to_datetime(fred_df["release_datetime_utc"], errors="coerce", utc=True)
    if start_date is not None:
        release_dt = release_dt[release_dt >= pd.Timestamp(start_date, tz="UTC")]
        fred_df = fred_df.loc[release_dt.index]
        release_dt = pd.to_datetime(fred_df["release_datetime_utc"], errors="coerce", utc=True)
    if end_date is not None:
        end_bound = (
            pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        )
        release_dt = release_dt[release_dt <= end_bound]
        fred_df = fred_df.loc[release_dt.index]
        release_dt = pd.to_datetime(fred_df["release_datetime_utc"], errors="coerce", utc=True)

    if fred_df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "time",
                "currency",
                "event",
                "impact",
                "forecast",
                "previous",
                "actual",
                "event_url",
                "source_url",
                "scraped_at",
                "source",
            ]
        )

    fred_events = pd.DataFrame(
        {
            "date": release_dt.dt.strftime("%Y-%m-%d"),
            "time": release_dt.dt.strftime("%H:%M"),
            "currency": fred_df["currency"].astype(str),
            "event": fred_df["indicator"].astype(str),
            "impact": "low",
            "forecast": pd.NA,
            "previous": pd.NA,
            "actual": pd.to_numeric(fred_df["actual_value"], errors="coerce"),
            "event_url": pd.NA,
            "source_url": "https://fred.stlouisfed.org",
            "scraped_at": datetime.utcnow().isoformat(),
            "source": "fred",
        }
    )

    return fred_events


def _normalize_event_name(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _names_partially_match(name_a: str, name_b: str) -> bool:
    if not name_a or not name_b:
        return False
    return name_a in name_b or name_b in name_a


def add_source_matching_annotations(combined_df: pd.DataFrame) -> tuple[pd.DataFrame, int, float]:
    """Annotate rows with source_count and source_agreement using lightweight matching."""
    annotated = combined_df.copy()
    if annotated.empty:
        annotated["source_count"] = 1
        annotated["source_agreement"] = False
        return annotated, 0, 0.0

    annotated["source_count"] = 1
    annotated["source_agreement"] = False

    timestamps = pd.to_datetime(
        annotated["date"].astype(str).str.strip() + " " + annotated["time"].astype(str).str.strip(),
        errors="coerce",
        utc=True,
    )
    currencies = annotated["currency"].astype(str).str.strip().str.upper()
    names = annotated["event"].apply(_normalize_event_name)
    sources = annotated["source"].astype(str).str.strip().str.lower()

    matched_indices: set[int] = set()
    matched_pairs = 0
    delta = pd.Timedelta(hours=2)

    for i in range(len(annotated)):
        ts_i = timestamps.iloc[i]
        if pd.isna(ts_i):
            continue
        ccy_i = currencies.iloc[i]
        name_i = names.iloc[i]
        source_i = sources.iloc[i]

        for j in range(i + 1, len(annotated)):
            ts_j = timestamps.iloc[j]
            if pd.isna(ts_j):
                continue

            if source_i == sources.iloc[j]:
                continue
            if ccy_i != currencies.iloc[j]:
                continue
            if abs(ts_i - ts_j) > delta:
                continue
            if not _names_partially_match(name_i, names.iloc[j]):
                continue

            matched_indices.add(i)
            matched_indices.add(j)
            matched_pairs += 1

    if matched_indices:
        idx_list = list(matched_indices)
        annotated.loc[idx_list, "source_count"] = 2
        annotated.loc[idx_list, "source_agreement"] = True

    multi_source_pct = (len(matched_indices) / len(annotated) * 100.0) if len(annotated) else 0.0
    return annotated, matched_pairs, multi_source_pct


def run_source(source: str, start_date=None, end_date=None):
    if start_date is None and end_date is None:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=DEFAULT_LIVE_LOOKBACK_DAYS)

    combined_df = collect_calendar_source(source=source, start_date=start_date, end_date=end_date)

    if combined_df.empty:
        print("No data collected")
        return None

    if source == "combined" and {"source_count", "source_agreement"}.issubset(combined_df.columns):
        matched_events = int(combined_df["source_agreement"].fillna(False).sum())
        multi_source_pct = matched_events / len(combined_df) * 100.0 if len(combined_df) else 0.0
    else:
        matched_events = 0
        multi_source_pct = 0.0

    total_rows = len(combined_df)

    print(f"Source: {source}")
    print(f"Total rows: {total_rows}")
    if start_date and end_date:
        print(f"Collection date window: {start_date.date()} to {end_date.date()}")
    print("Source value counts:")
    source_counts = combined_df["source"].astype(str).value_counts(dropna=False)
    for source_name, count in source_counts.items():
        print(f"- {source_name}: {int(count)}")
    if "currency" in combined_df.columns:
        print("Rows by currency:")
        for ccy, cnt in combined_df["currency"].value_counts(dropna=False).items():
            print(f"- {ccy}: {int(cnt)}")
    print(f"Matched events across sources: {matched_events}")
    print(f"% of events with multiple sources: {multi_source_pct:.2f}%")

    output_path = _save_raw_calendar(combined_df, source)
    print(f"Saved raw calendar data to {output_path}")
    print(f"Collected {len(combined_df)} rows")
    if len(combined_df) == 0:
        raise ValueError("No target-currency events collected (EUR/GBP/CHF/JPY)")
    return combined_df


def run_live(start_date=None, end_date=None):
    return run_source("forexfactory", start_date=start_date, end_date=end_date)


def run_fixture(input_path):
    df = pd.read_csv(input_path)
    print(f"Loaded fixture with {len(df)} rows")
    return df


def run_preprocess(df, output_path: Path | None = None):
    processor = CalendarPreprocessor()
    result = processor.preprocess(df)
    processed = result["events"]

    if "score_available" in processed.columns:
        scored_count = int(processed["score_available"].fillna(False).sum())
    else:
        scored_count = 0

    if output_path is None:
        output_path = Path("data/processed/calendar_fixture_scored.csv")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    processed.to_csv(output_path, index=False)
    print(f"Loaded {len(df)} rows")
    print(f"Processed {len(processed)} unique events")
    print(f"Scored {scored_count} events")
    print(f"Saved scored calendar data to {output_path}")

    if "timestamp_utc" in processed.columns:
        ts = pd.to_datetime(processed["timestamp_utc"], errors="coerce", utc=True).dropna()
        ts_min = ts.min() if not ts.empty else "n/a"
        ts_max = ts.max() if not ts.empty else "n/a"
    else:
        ts_min = "n/a"
        ts_max = "n/a"

    print(f"Scored file: {output_path}")
    print(f"Total scored rows: {len(processed)}")
    print(f"score_available true: {scored_count}")
    if "currency" in processed.columns:
        print("Rows by currency:")
        for ccy, cnt in (
            processed["currency"]
            .astype(str)
            .str.upper()
            .str.strip()
            .value_counts(dropna=False)
            .items()
        ):
            print(f"- {ccy}: {int(cnt)}")
    print(f"timestamp_utc min: {ts_min}")
    print(f"timestamp_utc max: {ts_max}")

    return processed


def build_scored_output_path(
    source: str, start_date: datetime | None, end_date: datetime | None
) -> Path:
    if source == "fixture":
        return Path("data/processed/calendar_fixture_scored.csv")

    if source in {"live", "forexfactory"}:
        start_part = (
            start_date.strftime("%Y%m%d") if start_date else datetime.now().strftime("%Y%m%d")
        )
        end_part = end_date.strftime("%Y%m%d") if end_date else start_part
        return Path(f"data/processed/calendar_live_scored_{start_part}_{end_part}.csv")

    start_part = start_date.strftime("%Y%m%d") if start_date else datetime.now().strftime("%Y%m%d")
    end_part = end_date.strftime("%Y%m%d") if end_date else start_part
    return Path(f"data/processed/calendar_{source}_scored_{start_part}_{end_part}.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        choices=["forexfactory", "tradingeconomics", "combined", "fixture", "live"],
        default="forexfactory",
    )
    parser.add_argument("--input", type=str, help="Path to fixture CSV")
    parser.add_argument("--start", type=str, help="Start date for live mode (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date for live mode (YYYY-MM-DD)")
    parser.add_argument("--preprocess", action="store_true")

    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d") if args.start else None
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else start_date

    if args.source == "fixture":
        if not args.input:
            raise ValueError("Fixture mode requires --input")
        df = run_fixture(args.input)
    else:
        df = run_source(args.source, start_date=start_date, end_date=end_date)

    if df is None:
        return

    if args.preprocess:
        output_path = build_scored_output_path(args.source, start_date, end_date)
        run_preprocess(df, output_path=output_path)


if __name__ == "__main__":
    main()
