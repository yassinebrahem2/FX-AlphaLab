from pathlib import Path

import pandas as pd

DATA_PATH = Path("data/processed/calendar_reaction_live.csv")

SCORE_THRESHOLDS = [0.1, 0.05, 0.02, 0.01]
RETURN_THRESHOLDS = [0.001, 0.0005, 0.0002, 0.0001]


def classify_expected(score: float, threshold: float) -> str:
    if pd.isna(score):
        return "MISSING"
    if score > threshold:
        return "UP"
    if score < -threshold:
        return "DOWN"
    return "NEUTRAL"


def classify_observed(ret: float, threshold: float) -> str:
    if pd.isna(ret):
        return "MISSING"
    if ret > threshold:
        return "UP"
    if ret < -threshold:
        return "DOWN"
    return "NEUTRAL"


def direction_counts(series: pd.Series) -> dict[str, int]:
    s = series.astype("string")
    return {
        "UP": int((s == "UP").sum()),
        "DOWN": int((s == "DOWN").sum()),
        "NEUTRAL": int((s == "NEUTRAL").sum()),
        "MISSING": int((s == "MISSING").sum()),
    }


def usable_mask(expected: pd.Series, observed: pd.Series) -> pd.Series:
    return expected.isin(["UP", "DOWN"]) & observed.isin(["UP", "DOWN"])


def main() -> None:
    print("=" * 100)
    print("THRESHOLD SENSITIVITY EXPERIMENT")
    print("=" * 100)

    if not DATA_PATH.exists():
        print(f"Missing dataset: {DATA_PATH}")
        return

    df = pd.read_csv(DATA_PATH)
    if df.empty:
        print("Dataset is empty.")
        return

    required = ["final_score", "return_1h", "return_4h"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"Missing required columns: {missing}")
        return

    n = len(df)
    print(f"Rows analyzed: {n}")
    print()

    baseline_score_thr = 0.1
    baseline_ret_thr = 0.001

    baseline_expected = df["final_score"].apply(lambda x: classify_expected(x, baseline_score_thr))
    baseline_obs_1h = df["return_1h"].apply(lambda x: classify_observed(x, baseline_ret_thr))
    baseline_obs_4h = df["return_4h"].apply(lambda x: classify_observed(x, baseline_ret_thr))

    baseline_neutral_expected = int((baseline_expected == "NEUTRAL").sum())
    baseline_neutral_obs_1h = int((baseline_obs_1h == "NEUTRAL").sum())
    baseline_neutral_obs_4h = int((baseline_obs_4h == "NEUTRAL").sum())

    rows = []
    details = {}

    for s_thr in SCORE_THRESHOLDS:
        for r_thr in RETURN_THRESHOLDS:
            expected = df["final_score"].apply(lambda x: classify_expected(x, s_thr))
            obs_1h = df["return_1h"].apply(lambda x: classify_observed(x, r_thr))
            obs_4h = df["return_4h"].apply(lambda x: classify_observed(x, r_thr))

            usable_1h = int(usable_mask(expected, obs_1h).sum())
            usable_4h = int(usable_mask(expected, obs_4h).sum())
            usable_any = int((usable_mask(expected, obs_1h) | usable_mask(expected, obs_4h)).sum())
            pct_usable = (usable_any / n) * 100 if n else 0.0

            exp_counts = direction_counts(expected)
            obs1_counts = direction_counts(obs_1h)
            obs4_counts = direction_counts(obs_4h)

            exp_neutral_to_direction = baseline_neutral_expected - exp_counts["NEUTRAL"]
            obs1_neutral_to_direction = baseline_neutral_obs_1h - obs1_counts["NEUTRAL"]
            obs4_neutral_to_direction = baseline_neutral_obs_4h - obs4_counts["NEUTRAL"]

            key = (s_thr, r_thr)
            details[key] = {
                "expected_counts": exp_counts,
                "observed_1h_counts": obs1_counts,
                "observed_4h_counts": obs4_counts,
                "exp_neutral_to_direction": exp_neutral_to_direction,
                "obs1_neutral_to_direction": obs1_neutral_to_direction,
                "obs4_neutral_to_direction": obs4_neutral_to_direction,
                "usable_any": usable_any,
            }

            rows.append(
                {
                    "threshold_score": s_thr,
                    "threshold_return": r_thr,
                    "usable_1h": usable_1h,
                    "usable_4h": usable_4h,
                    "%usable": round(pct_usable, 1),
                    "usable_any": usable_any,
                }
            )

    result = pd.DataFrame(rows).sort_values(
        by=["usable_any", "usable_4h", "usable_1h", "%usable"], ascending=False
    )

    print("TABLE: threshold_score | threshold_return | usable_1h | usable_4h | %usable")
    print("-" * 100)
    for _, r in result.iterrows():
        print(
            f"{r['threshold_score']:>14.4f} | {r['threshold_return']:>16.4f} | "
            f"{int(r['usable_1h']):>9d} | {int(r['usable_4h']):>9d} | {float(r['%usable']):>7.1f}%"
        )

    best = result.iloc[0]
    best_key = (float(best["threshold_score"]), float(best["threshold_return"]))
    best_details = details[best_key]

    print()
    print("NEUTRAL -> UP/DOWN changes vs baseline (score=0.1, return=0.001)")
    print("-" * 100)
    print(
        f"Best combo score={best_key[0]:.4f}, return={best_key[1]:.4f}: "
        f"expected={best_details['exp_neutral_to_direction']}, "
        f"observed_1h={best_details['obs1_neutral_to_direction']}, "
        f"observed_4h={best_details['obs4_neutral_to_direction']}"
    )

    print()
    print("Best combo distributions")
    print("-" * 100)
    print(f"Expected: {best_details['expected_counts']}")
    print(f"Observed 1h: {best_details['observed_1h_counts']}")
    print(f"Observed 4h: {best_details['observed_4h_counts']}")
    print(
        f"Signal usable at all: {'YES' if best_details['usable_any'] > 0 else 'NO'} "
        f"(usable_any={best_details['usable_any']}/{n})"
    )
    print("=" * 100)


if __name__ == "__main__":
    main()
