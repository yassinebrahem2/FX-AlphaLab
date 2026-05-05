from pathlib import Path

import pandas as pd

ohlcv_dir = Path("data/processed/ohlcv")

# Test glob match
candidates = sorted(ohlcv_dir.glob("ohlcv_EURUSD*_D1_*.parquet"))
print("Glob candidates:", candidates)

# Test reading
for p in candidates:
    df = pd.read_parquet(p)
    print(f"\n{p.name}")
    print("  columns:", list(df.columns))
    print("  index name:", df.index.name)
    print("  shape:", df.shape)
    print("  dtypes:", df.dtypes.to_dict())
    if len(df) > 0:
        print(
            "  first ts:",
            (
                df.index[0]
                if df.index.name == "timestamp_utc"
                else df["timestamp_utc"].iloc[0] if "timestamp_utc" in df.columns else "N/A"
            ),
        )
        print(
            "  last ts:",
            (
                df.index[-1]
                if df.index.name == "timestamp_utc"
                else df["timestamp_utc"].iloc[-1] if "timestamp_utc" in df.columns else "N/A"
            ),
        )
