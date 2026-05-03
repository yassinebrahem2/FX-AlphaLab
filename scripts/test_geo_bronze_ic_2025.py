"""
Discriminating test: replicate notebook 18's Bronze-direct feature computation,
run GAT ensemble, compute IC vs trailing_vol_10d on 2025 only.

If IC recovers to ~+0.18-0.20 → zone_features_daily.parquet is the culprit (wrong source).
If IC stays at ~+0.05 → deeper bug (weights, BatchNorm, z-score implementation).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import datetime
import json

import numpy as np
import pandas as pd
import torch
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = ROOT / "data/raw/gdelt_events"

from src.agents.geopolitical.agent import ADJ_FULL, _GATZoneRiskV2  # noqa: E402
from src.agents.geopolitical.signal import ZONE_COUNTRIES, ZONE_NODES  # noqa: E402

PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF"]
# base_zone_idx, quote_zone_idx per pair — from config pair_node_idx
PAIR_NODE_IDX = {"EURUSD": [1, 0], "GBPUSD": [2, 0], "USDJPY": [0, 3], "USDCHF": [0, 4]}
PAIRS_M = [p + "m" for p in PAIRS]
ROLL_WIN = 30
MIN_HIST = 10

# ── 1. Build Bronze-direct features for 2025 dates ───────────────────────────
print("Building Bronze-direct zone features for 2025...")


def compute_zone_features_from_bronze(date: datetime.date) -> np.ndarray | None:
    """Replicate notebook 18's inline feature computation from Bronze."""
    path = BRONZE_DIR / str(date.year) / f"{date.month:02d}" / f"{date.strftime('%Y%m%d')}.parquet"
    if not path.exists():
        return None

    df = pd.read_parquet(path)

    # Detect column names (Bronze uses CamelCase)
    col_map = {c.lower(): c for c in df.columns}
    a1 = col_map.get("actor1countrycode")
    a2 = col_map.get("actor2countrycode")
    gs = col_map.get("goldsteinscale")
    qc = col_map.get("quadclass")
    tone = col_map.get("avgtone")
    nm = col_map.get("nummentions")

    if not all([a1, a2, gs, tone]):
        return None

    df[gs] = pd.to_numeric(df[gs], errors="coerce")
    df[tone] = pd.to_numeric(df[tone], errors="coerce")
    if qc:
        df[qc] = pd.to_numeric(df[qc], errors="coerce")
    if nm:
        df[nm] = pd.to_numeric(df[nm], errors="coerce").fillna(0)

    node_feats = np.zeros((len(ZONE_NODES), 5), dtype=np.float32)
    for z_idx, zone in enumerate(ZONE_NODES):
        countries = ZONE_COUNTRIES[zone]
        mask = df[a1].isin(countries) | df[a2].isin(countries)
        sub = df[mask]
        if len(sub) > 0:
            node_feats[z_idx, 0] = np.log1p(len(sub))
            node_feats[z_idx, 1] = float(sub[gs].mean() or 0.0)
            node_feats[z_idx, 2] = float(sub[qc].isin([3.0, 4.0]).mean()) if qc else 0.0
            node_feats[z_idx, 3] = float(sub[tone].mean() or 0.0)
            node_feats[z_idx, 4] = float(np.log1p(sub[nm].sum())) if nm else 0.0

    return node_feats.astype(np.float64)


# Build features for 2021-12 to 2025-12 (need history for rolling z-score in early 2025)
all_dates = pd.date_range("2021-12-01", "2025-12-31", freq="D")
bronze_features: list[tuple[datetime.date, np.ndarray]] = []

for ts in all_dates:
    d = ts.date()
    feats = compute_zone_features_from_bronze(d)
    if feats is not None:
        bronze_features.append((d, feats))

print(f"  Bronze features built: {len(bronze_features)} days")
bronze_dates = [r[0] for r in bronze_features]
bronze_array = np.stack([r[1] for r in bronze_features], axis=0)  # [T, 5, 5]

# ── 2. Load GAT model weights ─────────────────────────────────────────────────
config_path = ROOT / "models/production/gdelt_gat_config.json"
states_path = ROOT / "models/production/gdelt_gat_final_states.pt"

with open(config_path) as f:
    config = json.load(f)

states = torch.load(states_path, weights_only=True, map_location="cpu")
models = []
for state in states:
    m = _GATZoneRiskV2(
        in_features=int(config.get("in_features", 5)),
        hidden=int(config.get("hidden", 8)),
        n_heads=int(config.get("n_heads", 4)),
        dropout=float(config.get("dropout", 0.2)),
    )
    m.load_state_dict(state)
    m.eval()
    models.append(m)

adj = ADJ_FULL.to(dtype=torch.float32)
print(f"  Loaded {len(models)} GAT models")

# ── 3. Run inference on 2025 business days ────────────────────────────────────
print("Running inference on 2025 business days...")


def ensemble_zone_risk(z_features: np.ndarray) -> np.ndarray:
    x = torch.tensor(z_features, dtype=torch.float32).unsqueeze(0)
    outputs = []
    with torch.no_grad():
        for m in models:
            outputs.append(m(x, adj).squeeze(0).cpu().numpy())
    return np.mean(np.stack(outputs, axis=0), axis=0)


business_days_2025 = pd.bdate_range("2025-01-01", "2025-12-31", tz="UTC")
date_to_idx = {d: i for i, d in enumerate(bronze_dates)}

results = []
n_skipped = 0

for ts in business_days_2025:
    signal_date = ts.date()
    gdelt_date = (ts - pd.Timedelta(days=1)).date()

    idx = date_to_idx.get(gdelt_date)
    if idx is None:
        n_skipped += 1
        continue

    # Rolling z-score: last ROLL_WIN rows BEFORE gdelt_date
    hist_slice = bronze_array[max(0, idx - ROLL_WIN) : idx]  # up to 30 rows before current
    today_feats = bronze_array[idx]

    if hist_slice.shape[0] >= MIN_HIST:
        mu = hist_slice.mean(axis=0)
        sigma = hist_slice.std(axis=0).clip(min=1e-8)
        z_feats = (today_feats - mu) / sigma
    else:
        z_feats = np.zeros_like(today_feats)

    zone_risk = ensemble_zone_risk(z_feats)

    for pair in PAIRS:
        base_idx, quote_idx = PAIR_NODE_IDX[pair]
        bilateral = float(zone_risk[base_idx] + zone_risk[quote_idx])
        results.append({"date": signal_date, "pair": pair, "geo_bronze_bilateral": bilateral})

print(f"  Computed {len(results)} rows, skipped {n_skipped} dates")

results_df = pd.DataFrame(results)

# ── 4. Load trailing_vol_10d for 2025 ─────────────────────────────────────────
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

results_df["trailing_vol_10d"] = [
    trailing_vols[row["pair"]].get(row["date"]) for _, row in results_df.iterrows()
]

# ── 5. Compute IC ─────────────────────────────────────────────────────────────
print("\nIC results — Bronze-direct features, 2025 only:")
print(f"{'Pair':<10} {'IC (Bronze)':>12} {'IC (parquet)':>14}")
print("-" * 40)

# Load signal table for comparison
sig_df = pd.read_parquet(ROOT / "data/processed/coordinator/signals_aligned.parquet")
sig_df["date"] = pd.to_datetime(sig_df["date"]).dt.date
sig_df["year"] = pd.to_datetime(sig_df["date"]).dt.year
sig_2025 = sig_df[sig_df["year"] == 2025]

# Add trailing vol to sig_2025
sig_2025 = sig_2025.copy()
sig_2025["trailing_vol_10d"] = [
    trailing_vols[row["pair"]].get(row["date"]) for _, row in sig_2025.iterrows()
]

ics_bronze = []
for pair in PAIRS:
    sub_b = results_df[results_df["pair"] == pair].dropna(
        subset=["geo_bronze_bilateral", "trailing_vol_10d"]
    )
    sub_p = sig_2025[sig_2025["pair"] == pair].dropna(
        subset=["geo_bilateral_risk", "trailing_vol_10d"]
    )

    r_b, _ = stats.spearmanr(sub_b["geo_bronze_bilateral"], sub_b["trailing_vol_10d"])
    r_p, _ = stats.spearmanr(sub_p["geo_bilateral_risk"], sub_p["trailing_vol_10d"])
    ics_bronze.append(r_b)
    print(f"{pair:<10} {r_b:>+12.4f} {r_p:>+14.4f}")

print(f"\n{'Mean':<10} {sum(ics_bronze)/len(ics_bronze):>+12.4f}")
print("Expected from docs: ~+0.2012")

# Cross-check: how similar are Bronze-computed bilateral scores vs parquet-based?
print("\nCross-check: correlation between Bronze bilateral vs parquet bilateral for EURUSD 2025:")
merged = results_df[results_df["pair"] == "EURUSD"].merge(
    sig_2025[sig_2025["pair"] == "EURUSD"][["date", "geo_bilateral_risk"]], on="date", how="inner"
)
if len(merged) > 10:
    r, _ = stats.spearmanr(merged["geo_bronze_bilateral"], merged["geo_bilateral_risk"])
    print(f"  Spearman(bronze_bilateral, parquet_bilateral) = {r:.4f}")
    print(
        f"  Bronze bilateral: mean={merged['geo_bronze_bilateral'].mean():.4f} std={merged['geo_bronze_bilateral'].std():.4f}"
    )
    print(
        f"  Parquet bilateral: mean={merged['geo_bilateral_risk'].mean():.4f} std={merged['geo_bilateral_risk'].std():.4f}"
    )
