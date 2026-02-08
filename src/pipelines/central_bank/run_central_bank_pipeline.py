"""
Central Bank Document Pipeline Runner (RAW → CLEAN → DB)
STEP 5 – Policy Source Data
"""

from pathlib import Path
import json
from datetime import datetime, timezone

from src.pipelines.central_bank.fetch_rss import fetch_rss_documents
from src.pipelines.central_bank.to_clean import raw_to_clean_cb
from src.storage.db import initialize_database
from src.storage.load_central_bank import load_central_bank_documents


# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

RAW_DIR = Path("data/raw/central_bank")
CLEAN_DIR = Path("data/clean/central_bank")
MANIFEST_DIR = Path("data/manifests")


# -------------------------------------------------------------------
# Pipeline
# -------------------------------------------------------------------

def run() -> None:
    initialize_database()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------
    # Fetch (RAW)
    # ---------------------------------------------------------------

    df_raw = fetch_rss_documents()

    raw_path = RAW_DIR / "central_bank_raw.csv"
    df_raw.to_csv(raw_path, index=False)

    # ---------------------------------------------------------------
    # Raw → Clean
    # ---------------------------------------------------------------

    df_clean = raw_to_clean_cb(df_raw)

    clean_path = CLEAN_DIR / "central_bank_clean.csv"
    df_clean.to_csv(clean_path, index=False)

    # ---------------------------------------------------------------
    # Persist
    # ---------------------------------------------------------------

    load_central_bank_documents(df_clean)

    # ---------------------------------------------------------------
    # Manifest
    # ---------------------------------------------------------------

    run_time_utc = datetime.now(timezone.utc).isoformat()

    manifest = {
        "run_time_utc": run_time_utc,
        "pipeline": "central_bank_documents",
        "rows_loaded": len(df_clean),
        "institutions": sorted(df_clean["institution"].unique().tolist())
        if not df_clean.empty else [],
        "raw_file": str(raw_path),
        "clean_file": str(clean_path),
    }

    manifest_path = MANIFEST_DIR / f"cb_run_{run_time_utc.replace(':', '-')}.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print("Central bank document pipeline completed successfully")


if __name__ == "__main__":
    run()
