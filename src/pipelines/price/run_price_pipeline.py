"""
Price Data Pipeline Runner
Week 4 – Foundation & Data
"""

from pathlib import Path
from datetime import datetime, timezone
import json

from src.common.config import FX_PAIRS
from src.pipelines.price.mt5_fetch import (
    fetch_raw_ohlc,
    standardize_raw_ohlc,
    add_metadata,
)


# -----------------------------
# Configuration
# -----------------------------

TIMEFRAME = "H1"

RAW_DIR = Path("data/raw/price")
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

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

    for pair in FX_PAIRS:
        print(f"Fetching {pair} {TIMEFRAME}")

        df = fetch_raw_ohlc(pair, TIMEFRAME)
        df = standardize_raw_ohlc(df)
        df = add_metadata(df, pair, TIMEFRAME)

        file_path = RAW_DIR / f"{pair}_{TIMEFRAME}.csv"
        df.to_csv(file_path, index=False)

        manifest["pairs"][pair] = {
            "rows": len(df),
            "file": str(file_path),
        }

    manifest_path = MANIFEST_DIR / f"price_run_{run_id}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print("Price pipeline completed")
    print(f"Manifest written to {manifest_path}")


if __name__ == "__main__":
    run()
