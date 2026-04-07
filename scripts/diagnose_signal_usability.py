from pathlib import Path

import pandas as pd

DATA_PATH = Path("data/processed/calendar_reaction_live.csv")
MT5_DIR = Path("data/raw/mt5")

TARGET_PAIRS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF",
    "USDJPY": "USD/JPY",
}

OBS_THRESHOLD = 0.001
EXPECTED_THRESHOLD = 0.1
NEAR_ZERO_THRESHOLD = 0.01


def normalize_pair(value: object) -> str | None:
    if pd.isna(value):
        return None
    s = str(value).upper().replace("/", "").strip()
    return s if s else None


def count_directions(series: pd.Series) -> dict[str, int]:
    s = series.astype("string").str.upper().fillna("<MISSING>")
    return {
        "UP": int((s == "UP").sum()),
        "DOWN": int((s == "DOWN").sum()),
        "NEUTRAL": int((s == "NEUTRAL").sum()),
        "MISSING": int((s == "<MISSING>").sum()),
    }


def describe_numeric(df: pd.DataFrame, col: str) -> str:
    if col not in df.columns:
        return "column missing"
    s = pd.to_numeric(df[col], errors="coerce")
    if s.notna().sum() == 0:
        return "all missing/non-numeric"
    d = s.describe()
    return (
        f"count={int(d['count'])}, mean={d['mean']:.6f}, std={d['std']:.6f}, "
        f"min={d['min']:.6f}, 25%={d['25%']:.6f}, 50%={d['50%']:.6f}, "
        f"75%={d['75%']:.6f}, max={d['max']:.6f}"
    )


def pair_has_mt5(pair: str) -> bool:
    return any(MT5_DIR.glob(f"mt5_{pair}_H1_*.csv"))


def safe_col(df: pd.DataFrame, primary: str, fallback: str | None = None) -> str | None:
    if primary in df.columns:
        return primary
    if fallback and fallback in df.columns:
        return fallback
    return None


def main() -> None:
    print("=" * 90)
    print("SIGNAL USABILITY DIAGNOSIS (TARGET PAIRS ONLY)")
    print("=" * 90)

    if not DATA_PATH.exists():
        print(f"File missing: {DATA_PATH}")
        return

    df = pd.read_csv(DATA_PATH)
    if df.empty:
        print("Input dataset is empty.")
        return

    if "pair" not in df.columns:
        print("Required column missing: pair")
        return

    df = df.copy()
    df["pair_norm"] = df["pair"].map(normalize_pair)

    target_norm = set(TARGET_PAIRS.keys())
    target_df = df[df["pair_norm"].isin(target_norm)].copy()

    print("A) TARGET-PAIR FILTER")
    print("-" * 90)
    print(f"Rows before filter: {len(df)}")
    print(f"Rows after filter : {len(target_df)}")
    pair_counts = target_df["pair_norm"].value_counts().reindex(sorted(target_norm), fill_value=0)
    for p in sorted(target_norm):
        print(f"  {TARGET_PAIRS[p]}: {int(pair_counts[p])}")

    if target_df.empty:
        print("\nNo rows for target pairs. Main bottleneck: data volume / pair coverage.")
        return

    expected_col = safe_col(target_df, "expected_direction")
    obs_1h_col = safe_col(target_df, "observed_direction_1h", "observed_direction")
    obs_4h_col = safe_col(target_df, "observed_direction_4h")
    match_1h_col = safe_col(target_df, "direction_match_1h", "direction_match")
    match_4h_col = safe_col(target_df, "direction_match_4h")

    print("\nB) EXPECTED DIRECTION DISTRIBUTION")
    print("-" * 90)
    if expected_col:
        c = count_directions(target_df[expected_col])
        print(c)
    else:
        print("expected_direction column missing")

    print("\nC) OBSERVED DIRECTION DISTRIBUTION")
    print("-" * 90)
    if obs_1h_col:
        c1 = count_directions(target_df[obs_1h_col])
        print(f"1h: {c1}")
    else:
        print("1h: observed direction column missing")

    if obs_4h_col:
        c4 = count_directions(target_df[obs_4h_col])
        print(f"4h: {c4}")
    else:
        print("4h: observed_direction_4h column missing")

    print("\nD) RETURN DISTRIBUTION")
    print("-" * 90)
    print(f"return_1h: {describe_numeric(target_df, 'return_1h')}")
    print(f"return_4h: {describe_numeric(target_df, 'return_4h')}")
    for col in ["return_1h", "return_4h"]:
        if col in target_df.columns:
            s = pd.to_numeric(target_df[col], errors="coerce")
            missing = int(s.isna().sum())
            below = int((s.abs() <= OBS_THRESHOLD).fillna(False).sum())
            print(
                f"{col}: missing={missing}, abs(return)<={OBS_THRESHOLD} (NEUTRAL-prone)={below}"
            )

    print("\nE) FINAL SCORE DISTRIBUTION")
    print("-" * 90)
    print(f"final_score: {describe_numeric(target_df, 'final_score')}")
    if "final_score" in target_df.columns:
        fs = pd.to_numeric(target_df["final_score"], errors="coerce").abs()
        strong = int((fs > EXPECTED_THRESHOLD).fillna(False).sum())
        weak = int(((fs > NEAR_ZERO_THRESHOLD) & (fs <= EXPECTED_THRESHOLD)).fillna(False).sum())
        near_zero = int((fs <= NEAR_ZERO_THRESHOLD).fillna(False).sum())
        missing_fs = int(fs.isna().sum())
        print(
            f"strong(|score|>{EXPECTED_THRESHOLD})={strong}, "
            f"weak({NEAR_ZERO_THRESHOLD}<|score|<={EXPECTED_THRESHOLD})={weak}, "
            f"near-zero(|score|<={NEAR_ZERO_THRESHOLD})={near_zero}, missing={missing_fs}"
        )

    print("\nF) USABILITY BREAKDOWN (TARGET PAIRS)")
    print("-" * 90)

    # MT5 coverage by pair
    mt5_availability = {p: pair_has_mt5(p) for p in sorted(target_norm)}
    for p in sorted(target_norm):
        print(f"MT5 file for {TARGET_PAIRS[p]} ({p}): {'yes' if mt5_availability[p] else 'no'}")

    # Row-level failure counters (non-exclusive)
    fail = {
        "expected_direction == NEUTRAL": 0,
        "observed_direction_1h == NEUTRAL": 0,
        "observed_direction_4h == NEUTRAL": 0,
        "missing return_1h": 0,
        "missing return_4h": 0,
        "missing price anchors (1h anchors)": 0,
        "missing price anchors (4h anchors)": 0,
        "missing MT5 data for pair": 0,
        "score too weak (|final_score|<=0.01)": 0,
        "usable_1h": 0,
        "usable_4h": 0,
    }

    for _, r in target_df.iterrows():
        pair = normalize_pair(r.get("pair_norm"))

        exp = str(r.get(expected_col)).upper() if expected_col and pd.notna(r.get(expected_col)) else None
        o1 = str(r.get(obs_1h_col)).upper() if obs_1h_col and pd.notna(r.get(obs_1h_col)) else None
        o4 = str(r.get(obs_4h_col)).upper() if obs_4h_col and pd.notna(r.get(obs_4h_col)) else None

        ret1 = pd.to_numeric(pd.Series([r.get("return_1h")]), errors="coerce").iloc[0]
        ret4 = pd.to_numeric(pd.Series([r.get("return_4h")]), errors="coerce").iloc[0]
        fs = pd.to_numeric(pd.Series([r.get("final_score")]), errors="coerce").abs().iloc[0]

        if exp == "NEUTRAL":
            fail["expected_direction == NEUTRAL"] += 1
        if o1 == "NEUTRAL":
            fail["observed_direction_1h == NEUTRAL"] += 1
        if o4 == "NEUTRAL":
            fail["observed_direction_4h == NEUTRAL"] += 1
        if pd.isna(ret1):
            fail["missing return_1h"] += 1
        if pd.isna(ret4):
            fail["missing return_4h"] += 1

        missing_1h_anchor = any(
            pd.isna(r.get(c)) for c in ["price_before_event", "price_at_event", "price_1h_after"]
        )
        missing_4h_anchor = any(
            pd.isna(r.get(c)) for c in ["price_before_event", "price_at_event", "price_4h_after"]
        )
        if missing_1h_anchor:
            fail["missing price anchors (1h anchors)"] += 1
        if missing_4h_anchor:
            fail["missing price anchors (4h anchors)"] += 1

        if pair not in mt5_availability or not mt5_availability[pair]:
            fail["missing MT5 data for pair"] += 1

        if pd.notna(fs) and fs <= NEAR_ZERO_THRESHOLD:
            fail["score too weak (|final_score|<=0.01)"] += 1

        # Usable labels under current matching logic
        usable_1h = exp in {"UP", "DOWN"} and o1 in {"UP", "DOWN"}
        usable_4h = exp in {"UP", "DOWN"} and o4 in {"UP", "DOWN"}
        if usable_1h:
            fail["usable_1h"] += 1
        if usable_4h:
            fail["usable_4h"] += 1

    for k, v in fail.items():
        print(f"{k}: {v}")

    print("\nG) DATA VOLUME CHECK BY PAIR")
    print("-" * 90)
    for p in sorted(target_norm):
        sub = target_df[target_df["pair_norm"] == p]
        total = len(sub)
        if total == 0:
            print(f"{TARGET_PAIRS[p]}: total=0, usable_1h=0 (0.0%), usable_4h=0 (0.0%)")
            continue

        exp = (
            sub[expected_col].astype("string").str.upper() if expected_col else pd.Series([None] * total)
        )
        o1 = sub[obs_1h_col].astype("string").str.upper() if obs_1h_col else pd.Series([None] * total)
        o4 = sub[obs_4h_col].astype("string").str.upper() if obs_4h_col else pd.Series([None] * total)

        usable_1h = int(((exp.isin(["UP", "DOWN"])) & (o1.isin(["UP", "DOWN"]))).sum())
        usable_4h = int(((exp.isin(["UP", "DOWN"])) & (o4.isin(["UP", "DOWN"]))).sum())

        pct_1h = (usable_1h / total) * 100 if total else 0.0
        pct_4h = (usable_4h / total) * 100 if total else 0.0

        print(
            f"{TARGET_PAIRS[p]}: total={total}, usable_1h={usable_1h} ({pct_1h:.1f}%), "
            f"usable_4h={usable_4h} ({pct_4h:.1f}%)"
        )

    print("\nH) FINAL DIAGNOSIS SUMMARY")
    print("-" * 90)

    reasons = {
        "expected NEUTRAL": fail["expected_direction == NEUTRAL"],
        "observed 1h NEUTRAL": fail["observed_direction_1h == NEUTRAL"],
        "observed 4h NEUTRAL": fail["observed_direction_4h == NEUTRAL"],
        "missing returns": fail["missing return_1h"] + fail["missing return_4h"],
        "missing anchors": fail["missing price anchors (1h anchors)"] + fail["missing price anchors (4h anchors)"],
        "missing MT5 pair coverage": fail["missing MT5 data for pair"],
        "near-zero score": fail["score too weak (|final_score|<=0.01)"],
    }
    top3 = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:3]

    print("Top 3 reasons signals are unusable:")
    for i, (name, val) in enumerate(top3, start=1):
        print(f"{i}. {name}: {val}")

    total_rows = len(target_df)
    usable_any = max(fail["usable_1h"], fail["usable_4h"])

    print("\nMain bottleneck assessment:")
    if total_rows < 50:
        print("- Primary: data volume (sample is too small for robust usability)")
    if reasons["missing MT5 pair coverage"] > 0:
        print("- Primary/secondary: missing MT5 coverage for one or more target pairs")
    if reasons["expected NEUTRAL"] + reasons["observed 1h NEUTRAL"] > total_rows:
        print("- Secondary: thresholding creates many NEUTRAL labels")
    if reasons["near-zero score"] > 0:
        print("- Secondary: weak final_score values reduce expected directional signals")

    print("\nRecommendation:")
    print("- First: improve data volume (more days + more events)")
    print("- First: collect MT5 files for GBPUSD, USDJPY, USDCHF to widen pair coverage")
    if usable_any == 0:
        print("- Consider threshold adjustments only after data volume improves")
    else:
        print("- Threshold adjustments are optional and should be tested after volume improvement")

    print("=" * 90)


if __name__ == "__main__":
    main()
