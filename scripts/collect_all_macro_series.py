"""Collect ALL macro series required by the macro analysis notebook.

Sources:
    FRED  : DFF, CPIAUCSL, PCEPI, GDPC1, UNRATE, PAYEMS, BOPGSTB, NETFI
    ECB   : ECB_DFR, ECB_MRR  (ECB SDMX FM dataflow)
    ECB   : HICP_EA            (ECB SDMX ICP dataflow, index INX, 2015=100)
    BIS   : BOEBANKRATE        (BIS WS_CBPOL SDMX XML, daily UK rate)

    Note: NAPM (ISM Mfg PMI) and NAPMNMI (ISM Svc PMI) are ISM-proprietary
    and not available from FRED's free API tier.  They are omitted; the
    notebook already guards for None and shows a descriptive placeholder.

Output:
    data/processed/macro/macro_all_2021-01-01_2025-12-31.csv
"""

import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

# ── project root on sys.path ──────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingestion.collectors.ecb_collector import ECBCollector
from src.ingestion.collectors.fred_collector import FREDCollector
from src.shared.config import Config

START = datetime(2021, 1, 1)
END = datetime(2025, 12, 31)

# Silver schema columns
SILVER_COLS = ["timestamp_utc", "series_id", "value", "source", "frequency", "units"]

# ── FRED series (FRED_id → Silver series_id; None = same) ────────────────────
FRED_SERIES: dict[str, str | None] = {
    "DFF": None,  # Fed Funds Rate         (Daily)
    "CPIAUCSL": None,  # US CPI All-Items        (Monthly)
    "PCEPI": None,  # US PCE Price Index      (Monthly)
    "GDPC1": None,  # US Real GDP             (Quarterly)
    "UNRATE": None,  # US Unemployment Rate    (Monthly)
    "PAYEMS": None,  # Nonfarm Payrolls        (Monthly)
    "BOPGSTB": None,  # US Trade Balance        (Monthly)
    "NETFI": None,  # Net Capital Flows       (Quarterly)
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _fred_to_silver(raw: pd.DataFrame, silver_sid: str) -> pd.DataFrame:
    """Convert a FRED Bronze DataFrame to Silver schema."""
    df = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(raw["date"], utc=True),
            "series_id": silver_sid,
            "value": pd.to_numeric(raw["value"], errors="coerce"),
            "source": raw["source"],
            "frequency": raw["frequency"],
            "units": raw["units"],
        }
    ).dropna(subset=["value"])
    return df[SILVER_COLS]


# ─────────────────────────────────────────────────────────────────────────────
# Collectors
# ─────────────────────────────────────────────────────────────────────────────


def collect_fred(collector: FREDCollector) -> list[pd.DataFrame]:
    """Fetch all FRED series listed in FRED_SERIES."""
    frames: list[pd.DataFrame] = []
    for fred_sid, silver_sid in FRED_SERIES.items():
        effective_sid = silver_sid if silver_sid else fred_sid
        try:
            raw = collector.get_series(fred_sid, start_date=START, end_date=END, use_cache=True)
            if raw.empty:
                print(f"  * {fred_sid:20s}: empty response from FRED — skipping")
                continue
            df = _fred_to_silver(raw, effective_sid)
            frames.append(df)
            print(f"  OK {fred_sid:20s} -> {effective_sid:15s}  {len(df):>5,} rows")
        except Exception as exc:
            print(f"  ERR {fred_sid:20s}: {exc}")
    return frames


def collect_ecb_rates(ecb: ECBCollector) -> list[pd.DataFrame]:
    """Collect ECB policy rates (DFR, MRR) using ECBCollector.collect_policy_rates().

    ECB SDMX returns columns including TIME_PERIOD and OBS_VALUE.
    PROVIDER_FM_ID identifies the specific rate: DFR = Deposit, MRR_FR = Main Rate.

    The ECB API only records rate-change dates.  Rates were held at -0.50% (DFR)
    and 0.00% (MRR) from pre-2021 to 2022-07-27.  A synthetic start-of-window
    observation is prepended so the carry-differential chart starts at 2021-01-01.
    """
    frames: list[pd.DataFrame] = []

    # Known ECB rates prior to first change in our window (2022-07-27):
    # DFR = -0.50% since June 2014, MRR = 0.00% since March 2016.
    PRE_HIKE_RATES = {
        "DFR": ("ECB_DFR", -0.50),
        "MRR_FR": ("ECB_MRR", 0.00),
    }

    try:
        raw = ecb.collect_policy_rates(START, END)
        if raw.empty:
            print("  * ECB rates: no data returned")
            return frames

        rate_map = {
            "DFR": ("ECB_DFR", "Deposit Facility Rate", "B", "Percent"),
            "MRR_FR": ("ECB_MRR", "Main Refinancing Operations", "B", "Percent"),
        }
        for ecb_code, (silver_sid, _, freq, units) in rate_map.items():
            subset = (
                raw[raw["PROVIDER_FM_ID"] == ecb_code].copy()
                if "PROVIDER_FM_ID" in raw.columns
                else pd.DataFrame()
            )
            if subset.empty:
                print(f"  * ECB {ecb_code}: no rows (PROVIDER_FM_ID={ecb_code} not found)")
                continue

            # DATE column: ECB uses TIME_PERIOD (not OBS_PERIOD)
            date_col = next(
                (c for c in ("TIME_PERIOD", "OBS_PERIOD", "date") if c in subset.columns), None
            )
            value_col = next((c for c in ("OBS_VALUE", "value") if c in subset.columns), None)

            if date_col is None or value_col is None:
                print(
                    f"  ERR ECB {ecb_code}: cannot find date/value columns in {subset.columns.tolist()}"
                )
                continue

            df = pd.DataFrame(
                {
                    "timestamp_utc": pd.to_datetime(subset[date_col], utc=True),
                    "series_id": silver_sid,
                    "value": pd.to_numeric(subset[value_col], errors="coerce"),
                    "source": "ecb",
                    "frequency": freq,
                    "units": units,
                }
            ).dropna(subset=["value"])

            # Prepend a start-of-window observation if first record is after START
            first_ts = df["timestamp_utc"].min()
            if pd.Timestamp(START, tz="UTC") < first_ts:
                pre_hike_val = PRE_HIKE_RATES[ecb_code][1]
                head_row = pd.DataFrame(
                    [
                        {
                            "timestamp_utc": pd.Timestamp(START, tz="UTC"),
                            "series_id": silver_sid,
                            "value": pre_hike_val,
                            "source": "ecb_synthetic",  # mark as synthetic fill
                            "frequency": freq,
                            "units": units,
                        }
                    ]
                )
                df = pd.concat([head_row, df], ignore_index=True)

            frames.append(df[SILVER_COLS])
            print(f"  OK ECB {ecb_code:10s} -> {silver_sid:15s}  {len(df):>5,} rows")

    except Exception as exc:
        print(f"  ERR ECB rates: {exc}")
    return frames


def collect_hicp_ea() -> list[pd.DataFrame]:
    """Fetch Euro Area HICP index from ECB SDMX ICP dataflow.

    Series: ICP/M.U2.N.000000.4.INX
    - M = Monthly, U2 = Euro area, N = Unadjusted
    - 000000 = All items, 4 = Eurostat, INX = Index 2015=100
    """
    url = (
        "https://data-api.ecb.europa.eu/service/data/ICP/M.U2.N.000000.4.INX"
        f"?startPeriod={START.strftime('%Y-%m')}&endPeriod={END.strftime('%Y-%m')}"
        "&format=csvdata"
    )
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        raw = pd.read_csv(StringIO(r.text))
        if raw.empty or "OBS_VALUE" not in raw.columns or "TIME_PERIOD" not in raw.columns:
            print("  * HICP_EA: empty or unexpected schema")
            return []

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.to_datetime(raw["TIME_PERIOD"], utc=True),
                "series_id": "HICP_EA",
                "value": pd.to_numeric(raw["OBS_VALUE"], errors="coerce"),
                "source": "ecb",
                "frequency": "M",
                "units": "Index 2015=100",
            }
        ).dropna(subset=["value"])
        print(f"  OK HICP_EA (ECB ICP/INX)  -> HICP_EA          {len(df):>5,} rows")
        return [df[SILVER_COLS]]
    except Exception as exc:
        print(f"  ERR HICP_EA: {exc}")
        return []


def collect_boe_rate() -> list[pd.DataFrame]:
    """Fetch BoE Bank Rate from BIS SDMX WS_CBPOL daily dataflow.

    Source: BIS Statistics API — Central Bank Policy Rates (daily), GB series.
    URL: https://stats.bis.org/api/v1/data/BIS,WS_CBPOL,1.0/D.GB
    Returns XML (SDMX 2.1 StructureSpecificData); parsed with ElementTree.
    """
    url = (
        "https://stats.bis.org/api/v1/data/BIS,WS_CBPOL,1.0/D.GB"
        f"?startPeriod={START.strftime('%Y-%m-%d')}&endPeriod={END.strftime('%Y-%m-%d')}"
    )
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.content)

        records: list[dict] = []
        for elem in root.iter():
            attribs = elem.attrib
            if "TIME_PERIOD" in attribs and "OBS_VALUE" in attribs:
                records.append({"date": attribs["TIME_PERIOD"], "value": attribs["OBS_VALUE"]})

        if not records:
            print("  * BOEBANKRATE (BIS): no observations in XML")
            return []

        df = (
            pd.DataFrame(
                {
                    "timestamp_utc": pd.to_datetime([r["date"] for r in records], utc=True),
                    "series_id": "BOEBANKRATE",
                    "value": pd.to_numeric([r["value"] for r in records], errors="coerce"),
                    "source": "bis",
                    "frequency": "D",
                    "units": "Percent",
                }
            )
            .dropna(subset=["value"])
            .sort_values("timestamp_utc")
        )
        print(f"  OK BOEBANKRATE (BIS SDMX) -> BOEBANKRATE      {len(df):>5,} rows")
        return [df[SILVER_COLS]]
    except Exception as exc:
        print(f"  ERR BOEBANKRATE: {exc}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    out_dir = Config.DATA_DIR / "processed" / "macro"
    out_dir.mkdir(parents=True, exist_ok=True)

    fred = FREDCollector()
    ecb = ECBCollector()
    all_frames: list[pd.DataFrame] = []

    print("\n-- FRED collection -------------------------------------------------")
    all_frames.extend(collect_fred(fred))

    print("\n-- ECB policy rates (DFR, MRR) ------------------------------------")
    all_frames.extend(collect_ecb_rates(ecb))

    print("\n-- ECB HICP index (HICP_EA) ----------------------------------------")
    all_frames.extend(collect_hicp_ea())

    print("\n-- BIS BoE Bank Rate (BOEBANKRATE) ---------------------------------")
    all_frames.extend(collect_boe_rate())

    if not all_frames:
        print("\nERR: No data collected — aborting")
        sys.exit(1)

    consolidated = (
        pd.concat(all_frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp_utc", "series_id"])
        .sort_values("timestamp_utc")
        .reset_index(drop=True)
    )

    start_str = START.strftime("%Y-%m-%d")
    end_str = END.strftime("%Y-%m-%d")
    out_file = out_dir / f"macro_all_{start_str}_{end_str}.csv"
    consolidated.to_csv(out_file, index=False)

    print(f"\n{'='*65}")
    print(f"Consolidated Silver file: {out_file.name}")
    print(f"  Total rows : {len(consolidated):,}")
    print(f"  Series     : {consolidated['series_id'].nunique()}")
    print("\nPer-series row counts:")
    for sid, cnt in consolidated.groupby("series_id").size().sort_index().items():
        print(f"  {sid:20s}: {cnt:>5,}")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
