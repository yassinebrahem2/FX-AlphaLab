"""Compare calendar source quality for Module C.

Loads the latest raw calendar CSVs for ForexFactory and Trading Economics,
runs them through the existing CalendarPreprocessor, and reports coverage and
scoreability metrics.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor

TARGET_CURRENCIES = {"EUR", "GBP", "CHF", "JPY"}
DEFAULT_RAW_DIR = Path("data/raw/calendar")
DEFAULT_OUTPUT_PATH = Path("data/processed/calendar_source_comparison.csv")


def _find_latest_csv(prefix: str, raw_dir: Path) -> Path | None:
    candidates = sorted(raw_dir.glob(f"{prefix}_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _load_calendar_frame(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "currency" in df.columns:
        df["currency"] = df["currency"].astype(str).str.upper().str.strip()
    return df


def _score_source(df: pd.DataFrame) -> pd.DataFrame:
    processor = CalendarPreprocessor(input_dir=Path("data/raw/calendar"), output_dir=Path("data/processed/events"))
    result = processor.preprocess(df=df)
    return result.get("events", pd.DataFrame())


def _summarize_source(name: str, raw_path: Path) -> dict[str, object]:
    raw_df = _load_calendar_frame(raw_path)
    target_mask = raw_df["currency"].isin(TARGET_CURRENCIES) if "currency" in raw_df.columns else pd.Series(False, index=raw_df.index)

    processed = _score_source(raw_df)
    scored_rows = int(processed["final_score"].notna().sum()) if "final_score" in processed.columns else 0
    score_available = int(processed["score_available"].fillna(False).sum()) if "score_available" in processed.columns else 0
    validated_1h = int(processed["direction_match_1h"].notna().sum()) if "direction_match_1h" in processed.columns else 0
    validated_4h = int(processed["direction_match_4h"].notna().sum()) if "direction_match_4h" in processed.columns else 0

    return {
        "source": name,
        "raw_rows": len(raw_df),
        "target_currency_rows": int(target_mask.sum()),
        "missing_actual_rows": int(raw_df["actual"].isna().sum()) if "actual" in raw_df.columns else 0,
        "missing_forecast_rows": int(raw_df["forecast"].isna().sum()) if "forecast" in raw_df.columns else 0,
        "processed_rows": len(processed),
        "scored_rows": scored_rows,
        "scoreable_rows": score_available,
        "validated_1h_rows": validated_1h,
        "validated_4h_rows": validated_4h,
        "score_rate": (scored_rows / len(processed) * 100.0) if len(processed) else 0.0,
        "scoreable_rate": (score_available / len(processed) * 100.0) if len(processed) else 0.0,
        "target_coverage_rate": (int(target_mask.sum()) / len(raw_df) * 100.0) if len(raw_df) else 0.0,
    }


def _latest_or_explicit(prefix: str, raw_dir: Path, explicit: str | None) -> Path | None:
    if explicit:
        path = Path(explicit)
        return path if path.exists() else None
    return _find_latest_csv(prefix, raw_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare Forex calendar sources for Module C")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--forexfactory", type=str, help="Explicit ForexFactory raw CSV path")
    parser.add_argument("--tradingeconomics", type=str, help="Explicit Trading Economics raw CSV path")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    ff_path = _latest_or_explicit("forexfactory", args.raw_dir, args.forexfactory)
    te_path = _latest_or_explicit("tradingeconomics", args.raw_dir, args.tradingeconomics)

    if ff_path is None and te_path is None:
        raise FileNotFoundError("No raw calendar CSVs found for comparison")

    rows: list[dict[str, object]] = []
    if ff_path is not None:
        rows.append(_summarize_source("forexfactory", ff_path))
    if te_path is not None:
        rows.append(_summarize_source("tradingeconomics", te_path))

    report = pd.DataFrame(rows)
    print("\nCALENDAR SOURCE COMPARISON\n")
    print(report.to_string(index=False))

    if len(report) >= 2:
        ff = report.loc[report["source"] == "forexfactory"].iloc[0]
        te = report.loc[report["source"] == "tradingeconomics"].iloc[0]
        if te["scored_rows"] > ff["scored_rows"] and te["target_currency_rows"] >= ff["target_currency_rows"]:
            verdict = "replacement"
        elif te["scored_rows"] >= ff["scored_rows"] * 0.8:
            verdict = "complement"
        else:
            verdict = "fallback"
        print(f"\nVerdict: Trading Economics is best used as {verdict} for Module C.")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.output, index=False)
    print(f"Saved report to {args.output}")


if __name__ == "__main__":
    main()