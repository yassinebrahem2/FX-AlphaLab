"""
Price Data Pipeline Runner
Week 4 – Foundation & Data
"""
from src.storage.db import initialize_database
from src.storage.load_price import load_price_dataframe

from pathlib import Path
from datetime import datetime, timezone
import json

from src.common.config import FX_PAIRS
from src.pipelines.price.mt5_fetch import (
    fetch_raw_ohlc,
    standardize_raw_ohlc,
    add_metadata,
)
from src.pipelines.price.validate import validate_schema
from src.pipelines.price.quality_checks import (
    check_missing_values,
    check_monotonic_time,
)
from src.pipelines.price.to_clean import raw_to_clean


# -----------------------------
# Configuration
# -----------------------------

TIMEFRAME = "H1"

RAW_DIR = Path("data/raw/price")
CLEAN_DIR = Path("data/clean/price")
MANIFEST_DIR = Path("data/manifests")


# -----------------------------
# Pipeline Runner
# -----------------------------

def run() -> None:
    run_time_utc = datetime.now(timezone.utc)
    run_id = run_time_utc.isoformat().replace(":", "-")

    manifest = {
        "run_time_utc": run_time_utc.isoformat(),
        "timeframe": TIMEFRAME,
        "pairs": {},
    }

    # Ensure directories exist
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    initialize_database()

    for pair in FX_PAIRS:
        print(f"Fetching {pair} {TIMEFRAME}")

        # -----------------------------
        # Ingestion & Standardisation
        # -----------------------------

        df_raw = fetch_raw_ohlc(pair, TIMEFRAME)
        df_std = standardize_raw_ohlc(df_raw)
        df_final = add_metadata(df_std, pair, TIMEFRAME)

        # -----------------------------
        # Validation & Quality Gates (Raw)
        # -----------------------------

        validate_schema(df_final)
        check_monotonic_time(df_final)

        quality_report = check_missing_values(df_final)

        # -----------------------------
        # Persist Raw
        # -----------------------------

        raw_path = RAW_DIR / f"{pair}_{TIMEFRAME}.csv"
        df_final.to_csv(raw_path, index=False)

        # -----------------------------
        # Convert to Clean
        # -----------------------------

        df_clean = raw_to_clean(df_final)

        clean_path = CLEAN_DIR / f"{pair}_{TIMEFRAME}.csv"
        df_clean.to_csv(clean_path, index=False)
        load_price_dataframe(df_clean)
        # -----------------------------
        # Manifest Update
        # -----------------------------

        manifest["pairs"][pair] = {
            "rows": len(df_final),
            "raw_file": str(raw_path),
            "clean_file": str(clean_path),
            "quality": quality_report,
        }

    # -----------------------------
    # Persist Manifest
    # -----------------------------

    manifest_path = MANIFEST_DIR / f"price_run_{run_id}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print("Price pipeline completed")
    print(f"Manifest written to {manifest_path}")


if __name__ == "__main__":
    run()

