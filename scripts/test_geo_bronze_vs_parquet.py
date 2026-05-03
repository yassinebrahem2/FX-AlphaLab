"""Compare zone features computed from Bronze vs zone_features_daily.parquet."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import datetime

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = ROOT / "data/raw/gdelt_events"

from src.agents.geopolitical.signal import ZONE_COUNTRIES, ZONE_NODES

zf = pd.read_parquet(ROOT / "data/processed/geopolitical/zone_features_daily.parquet")
zf["date"] = pd.to_datetime(zf["date"]).dt.date

TEST_DATES = [
    datetime.date(2023, 1, 10),
    datetime.date(2024, 3, 15),
    datetime.date(2025, 1, 15),
]

print("Comparing Bronze-computed features vs zone_features_daily.parquet:")

for test_d in TEST_DATES:
    bronze_path = (
        BRONZE_DIR
        / str(test_d.year)
        / f"{test_d.month:02d}"
        / f"{test_d.strftime('%Y%m%d')}.parquet"
    )
    zf_row = zf[zf["date"] == test_d]

    if not bronze_path.exists():
        print(f"\n{test_d}: Bronze file not found at {bronze_path}")
        continue
    if zf_row.empty:
        print(f"\n{test_d}: No zone_features row")
        continue

    df_b = pd.read_parquet(bronze_path)
    r = zf_row.iloc[0]

    print(f"\n{test_d}  (Bronze rows: {len(df_b)}):")
    print(
        f"  {'Zone':<6} {'B_logcnt':>10} {'P_logcnt':>10}  {'B_gs':>8} {'P_gs':>8}  {'B_tone':>8} {'P_tone':>8}  match"
    )

    # Check Bronze column names
    cols = df_b.columns.tolist()
    actor1_col = next(
        (c for c in cols if c.lower() in ("actor1countrycode", "actor1_country_code")), None
    )
    actor2_col = next(
        (c for c in cols if c.lower() in ("actor2countrycode", "actor2_country_code")), None
    )
    gs_col = next((c for c in cols if c.lower() in ("goldsteinscale", "goldstein_scale")), None)
    qc_col = next((c for c in cols if c.lower() in ("quadclass", "quad_class")), None)
    tone_col = next((c for c in cols if c.lower() in ("avgtone", "avg_tone")), None)
    nm_col = next((c for c in cols if c.lower() in ("nummentions", "num_mentions")), None)

    if not all([actor1_col, actor2_col, gs_col, tone_col]):
        print(f"  Column detection failed. Cols: {cols[:10]}")
        continue

    df_b[gs_col] = pd.to_numeric(df_b[gs_col], errors="coerce")
    df_b[tone_col] = pd.to_numeric(df_b[tone_col], errors="coerce")
    if nm_col:
        df_b[nm_col] = pd.to_numeric(df_b[nm_col], errors="coerce").fillna(0)
    if qc_col:
        df_b[qc_col] = pd.to_numeric(df_b[qc_col], errors="coerce")

    for zone in ZONE_NODES:
        countries = ZONE_COUNTRIES[zone]
        mask = df_b[actor1_col].isin(countries) | df_b[actor2_col].isin(countries)
        z_df = df_b[mask]

        b_logcnt = round(float(np.log1p(len(z_df))), 4)
        b_gs = round(float(z_df[gs_col].mean()) if len(z_df) > 0 else 0.0, 4)
        b_tone = round(float(z_df[tone_col].mean()) if len(z_df) > 0 else 0.0, 4)

        zk = zone.lower()
        p_logcnt = round(float(r.get(f"{zk}_log_count", 0)), 4)
        p_gs = round(float(r.get(f"{zk}_goldstein", 0)), 4)
        p_tone = round(float(r.get(f"{zk}_avg_tone", 0)), 4)

        logcnt_ok = abs(b_logcnt - p_logcnt) < 0.02
        gs_ok = abs(b_gs - p_gs) < 0.05 or (b_logcnt < 0.01 and p_logcnt < 0.01)
        match = "OK" if (logcnt_ok and gs_ok) else "MISMATCH"
        print(
            f"  {zone:<6} {b_logcnt:>10.4f} {p_logcnt:>10.4f}  {b_gs:>8.4f} {p_gs:>8.4f}  {b_tone:>8.4f} {p_tone:>8.4f}  {match}"
        )
