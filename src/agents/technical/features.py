# src/agents/technical/features.py
# ── Imports ────────────────────────────────────────────────────────────────
from pathlib import Path

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller

# ── Constants ──────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "processed" / "ohlcv"
PAIRS = ["EURUSDm", "GBPUSDm", "USDJPYm", "USDCHFm"]
TIMEFRAMES = ["D1", "H4", "H1"]
MISSING = set()


# ── File discovery ─────────────────────────────────────────────────────────
def find_parquet(pair: str, tf: str) -> Path:
    """
    Scans DATA_DIR for a parquet file matching pair and timeframe.
    Handles naming pattern: ohlcv_{pair}_{tf}_{start}_{end}.parquet
    Tries without 'm' suffix first, then falls back to with 'm' suffix.
    """
    base = pair[:-1] if pair.endswith("m") else pair
    matches = list(DATA_DIR.glob(f"ohlcv_{base}_{tf}_*.parquet"))
    if not matches:
        matches = list(DATA_DIR.glob(f"ohlcv_{base}m_{tf}_*.parquet"))
    if len(matches) == 0:
        raise FileNotFoundError(f"No parquet found for {pair} {tf} in {DATA_DIR}")
    if len(matches) > 1:
        raise ValueError(f"Multiple parquets found for {pair} {tf}: {matches}")
    return matches[0]


# ── Data loading ───────────────────────────────────────────────────────────
def load_pair(pair: str, tf: str) -> pd.DataFrame:
    """
    Load a single OHLCV parquet using glob pattern matching.
    Returns DataFrame with DatetimeIndex, lowercase columns.
    """
    path = find_parquet(pair, tf)
    df = pd.read_parquet(path, engine="pyarrow")
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df.columns = df.columns.str.lower()
    assert {"open", "high", "low", "close", "volume"}.issubset(
        df.columns
    ), f"Missing OHLCV columns in {path.name}"
    return df


# ── Feature engineering ────────────────────────────────────────────────────
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all technical indicators.
    Citations:
        Log returns    — Hamilton (1994)
        RSI, MACD, Mom — Öztürk (2025) [33b]
        BB, EMA        — Islam & Kapinchev (2024) [TA]
        ATR            — Wilder (1978)
    Returns copy with new columns.
    """
    df = df.copy()
    c = df["close"]

    # Log returns (Hamilton 1994)
    df["log_ret"] = np.log(c / c.shift(1))
    df["log_ret_sq"] = df["log_ret"] ** 2

    # RSI (14)
    delta = c.diff()
    gain = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(span=14, adjust=False).mean()
    df["rsi"] = 100 - 100 / (1 + gain / loss.replace(0, np.nan))

    # MACD (12, 26, 9)
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    # Bollinger Bands (20, 2σ)
    sma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    df["bb_upper"] = sma20 + 2 * std20
    df["bb_lower"] = sma20 - 2 * std20
    df["bb_pct"] = (c - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])

    # ATR (14)
    hl = df["high"] - df["low"]
    hpc = (df["high"] - c.shift(1)).abs()
    lpc = (df["low"] - c.shift(1)).abs()
    tr = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    df["atr"] = tr.ewm(span=14, adjust=False).mean()
    df["atr_pct_rank"] = df["atr"].rolling(252).rank(pct=True)

    # EMAs
    for span in [20, 50, 200]:
        df[f"ema_{span}"] = c.ewm(span=span, adjust=False).mean()

    # Momentum
    df["mom_5"] = c.pct_change(5)
    df["mom_20"] = c.pct_change(20)

    # Target (next-bar direction)
    df["target"] = (df["log_ret"].shift(-1) > 0).astype(int)

    return df.dropna()


# ── Feature names ──────────────────────────────────────────────────────────
def get_feature_names() -> list[str]:
    """Returns list of all feature column names excluding OHLCV and target."""
    return [
        "log_ret",
        "log_ret_sq",
        "rsi",
        "macd",
        "macd_signal",
        "macd_hist",
        "bb_upper",
        "bb_lower",
        "bb_pct",
        "atr",
        "ema_20",
        "ema_50",
        "ema_200",
        "mom_5",
        "mom_20",
    ]


def volatility_regime_label(atr_pct_rank: float, threshold: float = 0.6) -> str:
    """Returns 'high' if ATR percentile rank exceeds threshold, else 'low'."""
    return "high" if atr_pct_rank > threshold else "low"


# ── Stationarity test ──────────────────────────────────────────────────────
def adf_test(series: pd.Series, name: str) -> dict:
    """Augmented Dickey-Fuller test. Returns stat, p-value, verdict."""
    stat, p, lags, nobs, crit = adfuller(series.dropna(), maxlag=5, autolag=None)
    result = {
        "series": name,
        "stat": round(stat, 4),
        "p_value": round(p, 4),
        "stationary": p < 0.05,
    }
    label = "✓ stationary" if p < 0.05 else "✗ unit root"
    print(f"  ADF {name}".ljust(40) + f"stat={stat:7.3f}  p={p:.4f}  {label}")
    return result
