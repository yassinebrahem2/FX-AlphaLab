"""IC analysis: GDELT raw Bronze signals vs FX next-day returns."""

import glob
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

print("=" * 70)
print("GDELT IC ANALYSIS — Signal vs FX Next-Day Returns")
print("=" * 70)

# ── 1. Load all GDELT Bronze JSONL ──────────────────────────────────────
print("\n[1] Loading GDELT Bronze JSONL files...")
jsonl_files = sorted(glob.glob("data/raw/news/gdelt/aggregated_*.jsonl"))
print(f"    Found {len(jsonl_files)} files")

records = []
for fp in jsonl_files:
    with open(fp, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                records.append(rec)
            except Exception:
                pass

print(f"    Total records loaded: {len(records):,}")

# ── 2. Build article-level DataFrame ────────────────────────────────────
print("\n[2] Parsing records...")


def strip_theme_offset(t):
    parts = t.rsplit(",", 1)
    if len(parts) == 2 and parts[1].strip().isdigit():
        return parts[0].strip()
    return t.strip()


def has_currency(themes_raw, currency_tokens):
    if not themes_raw:
        return False
    themes_clean = {strip_theme_offset(t).upper() for t in themes_raw}
    return any(any(ct in t for ct in currency_tokens) for t in themes_clean)


EUR_TOKENS = ["EUR", "EURO"]
USD_TOKENS = ["USD", "DOLLAR"]
GBP_TOKENS = ["GBP", "POUND", "BRITISHPOUND", "STERLING"]
JPY_TOKENS = ["JPY", "YEN", "JAPANESEYEN"]

rows = []
for rec in records:
    ts_raw = rec.get("timestamp_published") or rec.get("timestamp_collected")
    if not ts_raw:
        continue
    try:
        dt = pd.to_datetime(ts_raw, utc=True)
    except Exception:
        continue

    tone = rec.get("tone")
    if tone is None:
        continue

    themes = rec.get("themes", []) or []

    rows.append(
        {
            "date": dt.date(),
            "tone": float(tone),
            "has_eur": has_currency(themes, EUR_TOKENS),
            "has_usd": has_currency(themes, USD_TOKENS),
            "has_gbp": has_currency(themes, GBP_TOKENS),
            "has_jpy": has_currency(themes, JPY_TOKENS),
        }
    )

df = pd.DataFrame(rows)
print(f"    Valid records: {len(df):,}")
print(f"    Date range: {df['date'].min()} to {df['date'].max()}")

# ── 3. Compute daily aggregated signals ─────────────────────────────────
print("\n[3] Computing daily signals...")


def daily_signals(sub, prefix, min_articles=3):
    sub_nonzero = sub[sub["tone"] != 0.0]
    cnt = sub.groupby("date").size().rename(f"{prefix}_count")
    tone_mean = sub_nonzero.groupby("date")["tone"].mean().rename(f"{prefix}_tone_mean")

    daily = pd.concat([cnt, tone_mean], axis=1).reset_index()
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date").set_index("date")

    roll_mean = daily[f"{prefix}_tone_mean"].rolling(30, min_periods=10).mean()
    roll_std = daily[f"{prefix}_tone_mean"].rolling(30, min_periods=10).std().replace(0, np.nan)
    daily[f"{prefix}_tone_z30"] = (daily[f"{prefix}_tone_mean"] - roll_mean) / roll_std

    cnt_mean = daily[f"{prefix}_count"].rolling(30, min_periods=10).mean()
    cnt_std = daily[f"{prefix}_count"].rolling(30, min_periods=10).std().replace(0, np.nan)
    daily[f"{prefix}_count_z30"] = (daily[f"{prefix}_count"] - cnt_mean) / cnt_std

    # Mask days with too few articles
    mask = daily[f"{prefix}_count"] < min_articles
    daily.loc[mask, f"{prefix}_tone_mean"] = np.nan
    daily.loc[mask, f"{prefix}_tone_z30"] = np.nan

    return daily


global_sig = daily_signals(df, "global")
eur_only = df[df["has_eur"] & ~df["has_usd"]]
usd_only = df[df["has_usd"] & ~df["has_eur"]]
gbp_df = df[df["has_gbp"]]
jpy_df = df[df["has_jpy"]]

eur_sig = daily_signals(eur_only, "eur")
usd_sig = daily_signals(usd_only, "usd")
gbp_sig = daily_signals(gbp_df, "gbp", min_articles=1)
jpy_sig = daily_signals(jpy_df, "jpy", min_articles=1)

print(
    f"    Global obs: {len(global_sig)}, median articles/day: {global_sig['global_count'].median():.0f}"
)
print(f"    EUR-only obs: {len(eur_sig)}, median articles/day: {eur_sig['eur_count'].median():.0f}")
print(f"    USD-only obs: {len(usd_sig)}, median articles/day: {usd_sig['usd_count'].median():.0f}")
print(f"    GBP obs: {len(gbp_sig)}, median articles/day: {gbp_sig['gbp_count'].median():.0f}")
print(f"    JPY obs: {len(jpy_sig)}, median articles/day: {jpy_sig['jpy_count'].median():.0f}")

all_signals = global_sig.join([eur_sig, usd_sig, gbp_sig, jpy_sig], how="outer")

# ── 4. Load OHLCV D1 price data ─────────────────────────────────────────
print("\n[4] Loading OHLCV D1 parquet files...")

pairs_files = {
    "EURUSD": "data/processed/ohlcv/ohlcv_EURUSDm_D1_2021-01-03_2025-12-30.parquet",
    "GBPUSD": "data/processed/ohlcv/ohlcv_GBPUSDm_D1_2021-01-03_2025-12-30.parquet",
    "USDJPY": "data/processed/ohlcv/ohlcv_USDJPYm_D1_2021-01-03_2025-12-30.parquet",
    "USDCHF": "data/processed/ohlcv/ohlcv_USDCHFm_D1_2021-01-03_2025-12-30.parquet",
}

price_data = {}
for pair, fp in pairs_files.items():
    if not Path(fp).exists():
        print(f"    MISSING: {fp}")
        continue
    pdf = pd.read_parquet(fp)
    pdf["timestamp_utc"] = pd.to_datetime(pdf["timestamp_utc"], utc=True)
    pdf = pdf.sort_values("timestamp_utc")
    pdf["date"] = pd.to_datetime(pdf["timestamp_utc"].dt.date)
    pdf = pdf.set_index("date")
    pdf["log_ret"] = np.log(pdf["close"] / pdf["close"].shift(1))
    pdf["log_ret_fwd1"] = pdf["log_ret"].shift(-1)
    pdf["abs_ret_fwd1"] = pdf["log_ret_fwd1"].abs()
    price_data[pair] = pdf[["log_ret_fwd1", "abs_ret_fwd1"]]
    print(f"    {pair}: {len(pdf)} days")

# ── 5. IC computation ────────────────────────────────────────────────────
print("\n[5] Spearman IC results:")
print()


def compute_ic(signal_series, target_series, min_obs=50):
    merged = pd.concat([signal_series, target_series], axis=1).dropna()
    if len(merged) < min_obs:
        return None, None, len(merged)
    ic, pval = stats.spearmanr(merged.iloc[:, 0], merged.iloc[:, 1])
    return ic, pval, len(merged)


primary_mapping = {
    "EURUSD": [
        "global_tone_mean",
        "global_tone_z30",
        "global_count_z30",
        "eur_tone_mean",
        "eur_tone_z30",
    ],
    "GBPUSD": [
        "global_tone_mean",
        "global_tone_z30",
        "global_count_z30",
        "gbp_tone_mean",
        "gbp_tone_z30",
    ],
    "USDJPY": [
        "global_tone_mean",
        "global_tone_z30",
        "global_count_z30",
        "jpy_tone_mean",
        "jpy_tone_z30",
    ],
    "USDCHF": ["global_tone_mean", "global_tone_z30", "global_count_z30"],
}

print(f"  {'Signal':<25} {'Pair':<8} {'Target':<14} {'IC':>7} {'p-val':>8} {'N':>6}  Sig")
print("  " + "-" * 76)

results = []
for pair, sigs in primary_mapping.items():
    if pair not in price_data:
        continue
    prices = price_data[pair]
    for sig_col in sigs:
        if sig_col not in all_signals.columns:
            continue
        sig = all_signals[sig_col]
        for target_name, target_col in [
            ("ret_fwd1", "log_ret_fwd1"),
            ("abret_fwd1", "abs_ret_fwd1"),
        ]:
            target = prices[target_col]
            ic, pval, n = compute_ic(sig, target)
            if ic is None:
                continue
            sig_marker = "**" if pval < 0.01 else "*" if pval < 0.05 else "." if pval < 0.10 else ""
            print(
                f"  {sig_col:<25} {pair:<8} {target_name:<14} {ic:>7.4f} {pval:>8.4f} {n:>6}  {sig_marker}"
            )
            results.append(
                {
                    "signal": sig_col,
                    "pair": pair,
                    "target": target_name,
                    "ic": ic,
                    "pval": pval,
                    "n": n,
                }
            )

# ── 6. Summary ───────────────────────────────────────────────────────────
print()
print("=" * 70)
print("SUMMARY")
print("=" * 70)
results_df = pd.DataFrame(results)

dir_df = results_df[results_df["target"] == "ret_fwd1"]
vol_df = results_df[results_df["target"] == "abret_fwd1"]

print(f"\nTotal tests: {len(results_df)}")
print(f"Significant p<0.05: {(results_df['pval']<0.05).sum()} / {len(results_df)}")

print(
    f"\nDirectional (ret_fwd1): mean|IC|={dir_df['ic'].abs().mean():.4f}, max|IC|={dir_df['ic'].abs().max():.4f}"
)
sig_dir = dir_df[dir_df["pval"] < 0.05]
if len(sig_dir):
    for _, r in sig_dir.iterrows():
        print(f"  SIG: {r['signal']:<25} {r['pair']:<8} IC={r['ic']:+.4f} p={r['pval']:.4f}")
else:
    print("  No directional tests significant at p<0.05")

print(
    f"\nVolatility (abret_fwd1): mean|IC|={vol_df['ic'].abs().mean():.4f}, max|IC|={vol_df['ic'].abs().max():.4f}"
)
sig_vol = vol_df[vol_df["pval"] < 0.05]
if len(sig_vol):
    for _, r in sig_vol.iterrows():
        print(f"  SIG: {r['signal']:<25} {r['pair']:<8} IC={r['ic']:+.4f} p={r['pval']:.4f}")
else:
    print("  No volatility tests significant at p<0.05")

# Decile check for best overall signal
best = results_df.loc[results_df["ic"].abs().idxmax()]
print(f"\nDecile monotonicity for best: {best['signal']} -> {best['pair']} ({best['target']})")
sig_s = all_signals[best["signal"]].dropna()
tgt_col = "log_ret_fwd1" if best["target"] == "ret_fwd1" else "abs_ret_fwd1"
tgt_s = price_data[best["pair"]][tgt_col]
merged = pd.concat([sig_s, tgt_s], axis=1).dropna()
merged.columns = ["signal", "target"]
merged["decile"] = pd.qcut(merged["signal"], q=10, labels=False, duplicates="drop")
dec_means = merged.groupby("decile")["target"].mean()
for d, v in dec_means.items():
    bar = "+" * min(int(abs(v) * 2000), 30) if v > 0 else "-" * min(int(abs(v) * 2000), 30)
    print(f"  D{int(d)+1:02d}: {v:+.6f}  {bar}")

print("\nDone.")
