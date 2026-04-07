from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


PROCESSED_DIR = Path("data/processed")
RAW_DIR = Path("data/raw/calendar")

SUMMARY_PATH = PROCESSED_DIR / "calendar_validation_summary.csv"
BY_CURRENCY_PATH = PROCESSED_DIR / "calendar_validation_by_currency.csv"
BY_EVENT_TYPE_PATH = PROCESSED_DIR / "calendar_validation_by_event_type.csv"
BY_IMPACT_PATH = PROCESSED_DIR / "calendar_validation_by_impact.csv"
BY_SCORE_STRENGTH_PATH = PROCESSED_DIR / "calendar_validation_by_score_strength.csv"
LOSS_BREAKDOWN_PATH = PROCESSED_DIR / "calendar_validation_data_loss_breakdown.csv"


def _latest(path_glob: list[Path]) -> Path:
    if not path_glob:
        raise FileNotFoundError("No matching files found")
    return max(path_glob, key=lambda p: (p.stat().st_mtime, p.name.lower()))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recompute calendar validation metrics")
    parser.add_argument(
        "--raw-file",
        type=Path,
        default=None,
        help="Explicit raw ForexFactory CSV path (avoids latest-by-mtime drift)",
    )
    parser.add_argument(
        "--scored-file",
        type=Path,
        default=None,
        help="Explicit scored calendar CSV path",
    )
    parser.add_argument(
        "--reaction-file",
        type=Path,
        default=None,
        help="Explicit reaction CSV path",
    )
    return parser.parse_args()


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def _accuracy(df: pd.DataFrame, col: str) -> tuple[int, int, float | None]:
    if col not in df.columns:
        return 0, 0, None
    usable = df[col].dropna()
    if usable.empty:
        return 0, 0, None
    hits = int((usable == True).sum())
    total = int(len(usable))
    return hits, total, (hits / total) * 100.0


def _segment_accuracy(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    grouped = df.groupby(group_col, dropna=False, observed=False)
    for value, g in grouped:
        h1_hits, h1_total, h1_acc = _accuracy(g, "direction_match_1h")
        h4_hits, h4_total, h4_acc = _accuracy(g, "direction_match_4h")
        rows.append(
            {
                group_col: value,
                "rows": int(len(g)),
                "usable_1h": h1_total,
                "hits_1h": h1_hits,
                "accuracy_1h": h1_acc,
                "usable_4h": h4_total,
                "hits_4h": h4_hits,
                "accuracy_4h": h4_acc,
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    if args.raw_file is not None:
        raw_file = args.raw_file
        if not raw_file.exists():
            raise FileNotFoundError(f"Missing raw file: {raw_file}")
    else:
        raw_file = _latest(list(RAW_DIR.glob("forexfactory_*.csv")))

    if args.scored_file is not None:
        scored_file = args.scored_file
        if not scored_file.exists():
            raise FileNotFoundError(f"Missing scored file: {scored_file}")
    else:
        scored_candidates = [
            p
            for p in PROCESSED_DIR.glob("calendar_live_scored_*.csv")
            if "reactions" not in p.name.lower()
        ]
        scored_file = _latest(scored_candidates)

    reaction_file = args.reaction_file or (PROCESSED_DIR / "calendar_reaction_live.csv")
    if not reaction_file.exists():
        raise FileNotFoundError(f"Missing reaction file: {reaction_file}")

    raw_df = pd.read_csv(raw_file)
    scored_df = pd.read_csv(scored_file)
    reaction_df = pd.read_csv(reaction_file)

    for col in ["currency", "event_name", "event_type", "impact"]:
        if col in reaction_df.columns:
            reaction_df[col] = reaction_df[col].astype(str).str.strip()

    scored_mask = _bool_series(scored_df["score_available"]) if "score_available" in scored_df.columns else pd.Series(False, index=scored_df.index)

    total_events = int(len(scored_df))
    scoreable_events = int(scored_mask.sum())
    coverage_pct = (scoreable_events / total_events * 100.0) if total_events else 0.0

    h1_hits, h1_usable, h1_acc = _accuracy(reaction_df, "direction_match_1h")
    h4_hits, h4_usable, h4_acc = _accuracy(reaction_df, "direction_match_4h")

    summary_df = pd.DataFrame(
        [
            {
                "raw_file": str(raw_file),
                "scored_file": str(scored_file),
                "reaction_file": str(reaction_file),
                "raw_events": int(len(raw_df)),
                "total_events": total_events,
                "scoreable_events": scoreable_events,
                "coverage_pct": coverage_pct,
                "validated_1h_rows": h1_usable,
                "hits_1h": h1_hits,
                "accuracy_1h": h1_acc,
                "validated_4h_rows": h4_usable,
                "hits_4h": h4_hits,
                "accuracy_4h": h4_acc,
            }
        ]
    )
    summary_df.to_csv(SUMMARY_PATH, index=False)

    by_currency = _segment_accuracy(reaction_df, "currency") if "currency" in reaction_df.columns else pd.DataFrame()
    by_event_type = _segment_accuracy(reaction_df, "event_type") if "event_type" in reaction_df.columns else pd.DataFrame()
    by_impact = _segment_accuracy(reaction_df, "impact") if "impact" in reaction_df.columns else pd.DataFrame()

    by_currency.to_csv(BY_CURRENCY_PATH, index=False)
    by_event_type.to_csv(BY_EVENT_TYPE_PATH, index=False)
    by_impact.to_csv(BY_IMPACT_PATH, index=False)

    score_strength_df = reaction_df.copy()
    score_strength_df["abs_final_score"] = pd.to_numeric(
        score_strength_df.get("final_score"), errors="coerce"
    ).abs()
    non_null_strength = score_strength_df["abs_final_score"].dropna()
    if len(non_null_strength) >= 3:
        score_strength_df["score_strength"] = pd.qcut(
            score_strength_df["abs_final_score"],
            q=3,
            labels=["low", "medium", "high"],
            duplicates="drop",
        )
    else:
        score_strength_df["score_strength"] = pd.NA

    by_score_strength = _segment_accuracy(score_strength_df, "score_strength")
    by_score_strength.to_csv(BY_SCORE_STRENGTH_PATH, index=False)

    validated_rows = int(
        reaction_df["direction_match_1h"].notna().sum() if "direction_match_1h" in reaction_df.columns else 0
    )
    scoreable_df = scored_df.loc[scored_mask].copy()
    target_currency_rows = (
        int(
            scoreable_df["currency"]
            .astype(str)
            .str.upper()
            .isin({"EUR", "GBP", "CHF", "JPY"})
            .sum()
        )
        if "currency" in scoreable_df.columns
        else 0
    )

    loss_breakdown = pd.DataFrame(
        [
            {"stage": "raw", "rows": int(len(raw_df))},
            {"stage": "scoreable", "rows": scoreable_events},
            {"stage": "target_currency", "rows": target_currency_rows},
            {"stage": "validated_1h", "rows": validated_rows},
        ]
    )
    loss_breakdown["drop_from_previous"] = loss_breakdown["rows"].shift(1) - loss_breakdown["rows"]
    loss_breakdown["drop_from_previous"] = loss_breakdown["drop_from_previous"].fillna(0).astype(int)
    loss_breakdown["retention_pct"] = (
        loss_breakdown["rows"] / loss_breakdown["rows"].shift(1) * 100.0
    )
    loss_breakdown.loc[0, "retention_pct"] = 100.0
    loss_breakdown.to_csv(LOSS_BREAKDOWN_PATH, index=False)

    print(
        f"Collected {len(raw_df)} raw events -> {scoreable_events} scoreable -> "
        f"{target_currency_rows} target-currency"
    )
    print(
        f"1h accuracy: {'N/A' if h1_acc is None else f'{h1_acc:.2f}%'} "
        f"({h1_hits}/{h1_usable})"
    )
    print(
        f"4h accuracy: {'N/A' if h4_acc is None else f'{h4_acc:.2f}%'} "
        f"({h4_hits}/{h4_usable})"
    )
    print(f"Saved: {SUMMARY_PATH}")
    print(f"Saved: {BY_CURRENCY_PATH}")
    print(f"Saved: {BY_EVENT_TYPE_PATH}")
    print(f"Saved: {BY_IMPACT_PATH}")
    print(f"Saved: {BY_SCORE_STRENGTH_PATH}")
    print(f"Saved: {LOSS_BREAKDOWN_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
