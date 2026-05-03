"""Audit: investigate zone_features coverage vs Silver, and check IC by subperiod."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import datetime

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

# ── 1. zone_features coverage for 2022-2025 specifically ─────────────────────
zf = pd.read_parquet(ROOT / "data/processed/geopolitical/zone_features_daily.parquet")
zf["date"] = pd.to_datetime(zf["date"]).dt.date

zf_2022_2025 = zf[
    (zf["date"] >= datetime.date(2022, 1, 1)) & (zf["date"] <= datetime.date(2025, 12, 31))
]
print(f"zone_features rows in 2022-2025: {len(zf_2022_2025)}")

all_calendar = pd.date_range("2022-01-01", "2025-12-31", freq="D")
zf_dates_2022 = set(zf_2022_2025["date"])
missing_2022_2025 = [d.date() for d in all_calendar if d.date() not in zf_dates_2022]
print(f"Missing calendar days in 2022-2025: {len(missing_2022_2025)}")
if missing_2022_2025[:10]:
    print(f"  Sample: {missing_2022_2025[:10]}")

# ── 2. How many days in 2022 have fewer than 10 days of history? ──────────────
print("\nHistory depth for early 2022 dates (rolling window in agent.py):")
zf_sorted = zf.sort_values("date").reset_index(drop=True)
zf_dates_sorted = zf_sorted["date"].values

# Simulate _history_window for Jan 2022 - check history depth
test_gdelt_dates = [
    datetime.date(2022, 1, d) for d in range(3, 32) if datetime.date(2022, 1, d) in zf_dates_2022
]
for d in test_gdelt_dates[:10]:
    hist = zf_sorted[zf_sorted["date"] < d].tail(30)
    print(
        f"  history for gdelt_date={d}: {len(hist)} rows (oldest: {hist['date'].values[0] if len(hist)>0 else 'none'})"
    )

# ── 3. Check if pre-2022 data is in the history window for early 2022 ─────────
print("\nFor first 2022 signal dates, is pre-2022 data in the 30-day rolling window?")
for d in test_gdelt_dates[:5]:
    hist = zf_sorted[zf_sorted["date"] < d].tail(30)
    pre_2022 = hist[hist["date"] < datetime.date(2022, 1, 1)]
    print(f"  gdelt_date={d}: {len(pre_2022)} pre-2022 rows in history window")

# ── 4. Check Silver coverage ──────────────────────────────────────────────────
silver_dir = ROOT / "data/processed/gdelt_events"
print("\nSilver file coverage (which months exist):")
silver_files = sorted(silver_dir.glob("year=*/month=*/gdelt_events_cleaned.parquet"))
if silver_files:
    for f in silver_files:
        yr = f.parent.parent.name.replace("year=", "")
        mo = f.parent.name.replace("month=", "")
        df_s = pd.read_parquet(f, columns=["event_date"])
        print(f"  {yr}-{mo}: {len(df_s)} event rows")
else:
    print("  No Silver files found at", silver_dir)

# ── 5. Compare features for a date where both Silver and zone_features exist ──
print("\nDirect feature comparison (Silver vs zone_features_daily) for a 2024 date:")
silver_2024 = silver_dir / "year=2024" / "month=01" / "gdelt_events_cleaned.parquet"
if silver_2024.exists():
    df_s = pd.read_parquet(silver_2024)
    df_s["event_date_d"] = pd.to_datetime(df_s["event_date"], utc=True).dt.date
    test_d = datetime.date(2024, 1, 10)
    df_day = df_s[df_s["event_date_d"] == test_d]
    zf_row = zf[zf["date"] == test_d]

    from src.agents.geopolitical.signal import ZONE_COUNTRIES, ZONE_NODES

    print(f"  Silver events on {test_d}: {len(df_day)}")
    if not zf_row.empty:
        r = zf_row.iloc[0]
        for zone in ZONE_NODES:
            countries = ZONE_COUNTRIES[zone]
            z_df = df_day[
                df_day["actor1_country_code"].isin(countries)
                | df_day["actor2_country_code"].isin(countries)
            ]
            s_log_count = round(float(np.log1p(len(z_df))), 4)
            p_log_count = round(float(r.get(f"{zone.lower()}_log_count", 0)), 4)
            match = "OK" if abs(s_log_count - p_log_count) < 0.01 else "MISMATCH"
            print(
                f"  {zone}: Silver log_count={s_log_count} | parquet log_count={p_log_count} → {match}"
            )
    else:
        print(f"  No zone_features row for {test_d}")
else:
    print("  Silver 2024-01 not found; trying 2023-01")
    silver_2023 = silver_dir / "year=2023" / "month=01" / "gdelt_events_cleaned.parquet"
    if silver_2023.exists():
        df_s = pd.read_parquet(silver_2023)
        df_s["event_date_d"] = pd.to_datetime(df_s["event_date"], utc=True).dt.date
        test_d = datetime.date(2023, 1, 10)
        df_day = df_s[df_s["event_date_d"] == test_d]
        zf_row = zf[zf["date"] == test_d]
        from src.agents.geopolitical.signal import ZONE_COUNTRIES, ZONE_NODES

        print(f"  Silver events on {test_d}: {len(df_day)}")
        if not zf_row.empty:
            r = zf_row.iloc[0]
            for zone in ZONE_NODES:
                countries = ZONE_COUNTRIES[zone]
                z_df = df_day[
                    df_day["actor1_country_code"].isin(countries)
                    | df_day["actor2_country_code"].isin(countries)
                ]
                s_lc = round(float(np.log1p(len(z_df))), 4)
                p_lc = round(float(r.get(f"{zone.lower()}_log_count", 0)), 4)
                match = "OK" if abs(s_lc - p_lc) < 0.01 else "MISMATCH"
                print(f"  {zone}: Silver log_count={s_lc} | parquet log_count={p_lc} → {match}")
    else:
        print("  No Silver files found for comparison")
