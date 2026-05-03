"""Corrective test: geo IC vs training-consistent trailing vol target — deep diagnostic."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF"]
PAIRS_M = [p + "m" for p in PAIRS]

df = pd.read_parquet(ROOT / "data/processed/coordinator/signals_aligned.parquet")
df["date"] = pd.to_datetime(df["date"]).dt.date
df["year"] = pd.to_datetime(df["date"]).dt.year

# Compute trailing_vol_10d
trailing_vols: dict = {}
for pair, pair_m in zip(PAIRS, PAIRS_M):
    files = sorted((ROOT / "data/processed/ohlcv").glob(f"ohlcv_{pair_m}_D1_2021*.parquet"))
    df_p = pd.read_parquet(files[-1], columns=["timestamp_utc", "close"])
    df_p["timestamp_utc"] = pd.to_datetime(df_p["timestamp_utc"], utc=True)
    df_p = df_p.sort_values("timestamp_utc").drop_duplicates("timestamp_utc")
    prices = df_p.set_index("timestamp_utc")["close"]
    ret = prices.pct_change()
    tv = ret.rolling(10).std()
    tv.index = tv.index.normalize()
    trailing_vols[pair] = {idx.date(): float(v) for idx, v in tv.items() if pd.notna(v)}

df["trailing_vol_10d"] = [trailing_vols[row["pair"]].get(row["date"]) for _, row in df.iterrows()]

# Overall IC
print("=" * 55)
print("IC vs trailing_vol_10d (CORRECT target) — full period")
print("=" * 55)
ics = []
for pair in PAIRS:
    sub = df[df["pair"] == pair].dropna(subset=["geo_bilateral_risk", "trailing_vol_10d"])
    r, p = stats.spearmanr(sub["geo_bilateral_risk"], sub["trailing_vol_10d"])
    ics.append(r)
    sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else ""
    print(f"  {pair}: {r:+.4f}  p={p:.5f} n={len(sub)} {sig}")
print(f"  Mean: {sum(ics)/len(ics):+.4f}  (docs claim: ~+0.2012)")

# IC by year for EURUSD (best performer)
print("\nIC by year — EURUSD:")
sub_eu = df[df["pair"] == "EURUSD"].dropna(subset=["geo_bilateral_risk", "trailing_vol_10d"])
for yr in sorted(sub_eu["year"].unique()):
    yr_sub = sub_eu[sub_eu["year"] == yr]
    r, p = stats.spearmanr(yr_sub["geo_bilateral_risk"], yr_sub["trailing_vol_10d"])
    print(f"  {yr}: IC={r:+.4f}  n={len(yr_sub)}")

# Test-set only (2025) — matches notebook 18 test split
print("\nIC vs trailing_vol_10d — 2025 ONLY (notebook 18 test set):")
df_2025 = df[df["year"] == 2025]
ics_2025 = []
for pair in PAIRS:
    sub = df_2025[df_2025["pair"] == pair].dropna(subset=["geo_bilateral_risk", "trailing_vol_10d"])
    r, p = stats.spearmanr(sub["geo_bilateral_risk"], sub["trailing_vol_10d"])
    ics_2025.append(r)
    print(f"  {pair}: IC={r:+.4f}  n={len(sub)}")
print(f"  Mean 2025: {sum(ics_2025)/len(ics_2025):+.4f}  (docs claim: +0.2012)")

# Check if geo_bilateral_risk actually varies (zone collapse diagnostic)
print("\nGeo bilateral_risk_score variance check:")
sub_eu2 = df[(df["pair"] == "EURUSD")].dropna(subset=["geo_bilateral_risk"])
print(
    f"  EURUSD bilateral_risk: mean={sub_eu2['geo_bilateral_risk'].mean():.4f} std={sub_eu2['geo_bilateral_risk'].std():.4f}"
)
print(
    f"  GBPUSD bilateral_risk: same as EURUSD? {(df[df['pair']=='GBPUSD']['geo_bilateral_risk'].values == df[df['pair']=='EURUSD']['geo_bilateral_risk'].values).mean():.2%}"
)

# Check zone features directly for a sample date
print("\nZone features for 2025-06-01 (sample diagnostic):")
zf = pd.read_parquet(ROOT / "data/processed/geopolitical/zone_features_daily.parquet")
zf["date"] = pd.to_datetime(zf["date"]).dt.date
import datetime  # noqa: E402

sample = zf[zf["date"] == datetime.date(2025, 6, 1)]
if not sample.empty:
    row = sample.iloc[0]
    for zone in ["usd", "eur", "gbp", "jpy", "chf"]:
        feats = [
            round(row.get(f"{zone}_{f}", 0), 3)
            for f in ["log_count", "goldstein", "conflict_frac", "avg_tone", "log_mentions"]
        ]
        print(f"  {zone.upper()}: {feats}")
else:
    print("  No data for 2025-06-01; trying 2025-05-30:")
    closest = zf[zf["date"] <= datetime.date(2025, 6, 1)].tail(1)
    if not closest.empty:
        row = closest.iloc[0]
        print(f"  Last available date: {row['date']}")
        for zone in ["usd", "eur", "gbp", "jpy", "chf"]:
            feats = [
                round(row.get(f"{zone}_{f}", 0), 3)
                for f in ["log_count", "goldstein", "conflict_frac", "avg_tone", "log_mentions"]
            ]
            print(f"  {zone.upper()}: {feats}")
