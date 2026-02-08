"""
Macro Data Pipeline Runner (RAW → CLEAN → DB)
STEP 4 – Exogenous Event Data
"""

from pathlib import Path
import json
import os
from datetime import datetime, timezone

from src.pipelines.macro.fetch_calendar import attach_metadata
from src.pipelines.macro.fred_fetcher import FredFetcher
from src.pipelines.macro.to_clean import raw_to_clean_macro
from src.storage.db import initialize_database
from src.storage.load_macro import load_macro_dataframe


# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

RAW_DIR = Path("data/raw/macro")
CLEAN_DIR = Path("data/clean/macro")
MANIFEST_DIR = Path("data/manifests")


# -------------------------------------------------------------------
# Pipeline
# -------------------------------------------------------------------

def run() -> None:
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise RuntimeError("FRED_API_KEY environment variable not set")

    initialize_database()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------
    # Fetch (RAW)
    # ---------------------------------------------------------------

    fetcher = FredFetcher(api_key=api_key)
    df_raw = fetcher.fetch()
    df_raw = attach_metadata(df_raw, source="FRED")

    raw_path = RAW_DIR / "fred_macro.csv"
    df_raw.to_csv(raw_path, index=False)

    # ---------------------------------------------------------------
    # Raw → Clean
    # ---------------------------------------------------------------

    df_clean = raw_to_clean_macro(df_raw)

    clean_path = CLEAN_DIR / "fred_macro.csv"
    df_clean.to_csv(clean_path, index=False)

    # ---------------------------------------------------------------
    # Persist
    # ---------------------------------------------------------------

    load_macro_dataframe(df_clean)

    # ---------------------------------------------------------------
    # Manifest
    # ---------------------------------------------------------------

    run_time_utc = datetime.now(timezone.utc).isoformat()

    manifest = {
        "run_time_utc": run_time_utc,
        "pipeline": "macro",
        "source": "FRED",
        "rows_loaded": len(df_clean),
        "raw_file": str(raw_path),
        "clean_file": str(clean_path),
    }

    manifest_path = MANIFEST_DIR / f"macro_run_{run_time_utc.replace(':', '-')}.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print("Macro pipeline completed successfully")


if __name__ == "__main__":
    run()

