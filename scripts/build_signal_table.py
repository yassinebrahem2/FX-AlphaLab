"""Build the aligned signal table for the coordinator.

All 4 agents x 4 pairs x business days 2022-01-01 to 2025-12-31.
Output: data/processed/coordinator/signals_aligned.parquet
"""

from __future__ import annotations

import sys
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from src.agents.geopolitical.agent import GeopoliticalAgent  # noqa: E402
from src.agents.macro.agent import MacroAgent  # noqa: E402
from src.agents.macro.calendar_node import CalendarEventsNode  # noqa: E402
from src.agents.sentiment.agent import SentimentAgent  # noqa: E402
from src.agents.sentiment.gdelt_node import GDELTSignalNode  # noqa: E402
from src.agents.sentiment.google_trends_node import GoogleTrendsSignalNode  # noqa: E402
from src.agents.sentiment.reddit_node import RedditSignalNode  # noqa: E402
from src.agents.sentiment.stocktwits_node import StocktwitsSignalNode  # noqa: E402
from src.agents.technical.agent import TechnicalAgent, fuse_timeframe_signals  # noqa: E402

PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF"]
PAIRS_M = [p + "m" for p in PAIRS]
TIMEFRAMES = ["D1", "H4", "H1"]
START = datetime(2022, 1, 1)
END = datetime(2025, 12, 31)
OUT = ROOT / "data" / "processed" / "coordinator" / "signals_aligned.parquet"

# ── Load agents ────────────────────────────────────────────────────────────

print("Loading technical agents...")
tech_agents: dict[str, dict[str, TechnicalAgent]] = {}
for pair_m in PAIRS_M:
    tech_agents[pair_m] = {}
    for tf in TIMEFRAMES:
        path = ROOT / "models" / "technical" / f"{pair_m}_{tf}.pkl"
        tech_agents[pair_m][tf] = TechnicalAgent.load(path)
print("  ok")

print("Loading geopolitical agent...")
geo_agent = GeopoliticalAgent()
geo_agent.load()
print("  ok")

print("Loading macro agent...")
calendar_node = CalendarEventsNode()
macro_agent = MacroAgent(calendar_node=calendar_node)
macro_agent.load()
print("  ok")

print("Loading sentiment agent...")
sent_agent = SentimentAgent(
    stocktwits_node=StocktwitsSignalNode(
        checkpoint_path=ROOT / "data/processed/sentiment/source=stockwits/labels_checkpoint.jsonl"
    ),
    reddit_node=RedditSignalNode(
        checkpoint_path=ROOT
        / "data/processed/sentiment/source=reddit/reddit_labels_checkpoint.jsonl"
    ),
    gdelt_node=GDELTSignalNode(silver_dir=ROOT / "data/processed/sentiment/source=gdelt"),
    gtrends_node=GoogleTrendsSignalNode(
        silver_dir=ROOT / "data/processed/sentiment/source=google_trends"
    ),
)
print("  ok")

# ── Pre-compute sentiment batch ────────────────────────────────────────────

print("Computing sentiment batch 2022-01 to 2025-12...")
sent_df = sent_agent.compute_batch(START, END)

# Verify all key columns are present and not entirely null
REQUIRED_SENT_COLS = [
    "usdjpy_stocktwits_vol_signal",
    "gdelt_tone_zscore",
    "gdelt_attention_zscore",
    "macro_attention_zscore",
    "composite_stress_flag",
]
missing_cols = [c for c in REQUIRED_SENT_COLS if c not in sent_df.columns]
if missing_cols:
    raise RuntimeError(f"Sentiment batch missing columns: {missing_cols}")

all_null_cols = [c for c in REQUIRED_SENT_COLS if sent_df[c].isnull().all()]
if all_null_cols:
    raise RuntimeError(f"Sentiment batch columns are entirely null (node failed): {all_null_cols}")

print(f"  {len(sent_df)} rows ok")
for col in REQUIRED_SENT_COLS:
    pct = sent_df[col].notna().mean() * 100
    print(f"    {col}: {pct:.1f}% non-null")


def _norm_date(ts) -> datetime.date:
    return pd.Timestamp(ts).normalize().to_pydatetime().date()


sent_lookup: dict[datetime.date, pd.Series] = {
    _norm_date(row["timestamp_utc"]): row for _, row in sent_df.iterrows()
}

# ── Load OHLCV and compute forward return labels (vectorized) ──────────────

print("Loading OHLCV D1...")
fwd_returns: dict[str, dict[datetime.date, dict]] = {}
for pair_m in PAIRS_M:
    files = sorted((ROOT / "data/processed/ohlcv").glob(f"ohlcv_{pair_m}_D1_2021*.parquet"))
    if not files:
        raise FileNotFoundError(f"No D1 parquet for {pair_m}")
    df = pd.read_parquet(files[-1], columns=["timestamp_utc", "close"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.sort_values("timestamp_utc").drop_duplicates("timestamp_utc")
    prices = df.set_index("timestamp_utc")["close"]

    ret = prices.pct_change()
    fr = pd.DataFrame(
        {
            "fwd_ret_1d": ret.shift(-1),
            "fwd_ret_5d": prices.shift(-5) / prices - 1,
            "fwd_vol_3d": ret.rolling(3).std().shift(-3),
            "fwd_vol_5d": ret.rolling(5).std().shift(-5),
            "fwd_vol_10d": ret.rolling(10).std().shift(-10),
        }
    )
    fr.index = fr.index.normalize()
    fwd_returns[pair_m] = {
        idx.to_pydatetime().date(): {c: (None if pd.isna(v) else float(v)) for c, v in row.items()}
        for idx, row in fr.iterrows()
    }
    print(f"  {pair_m}: {len(fr)} rows")

# ── Build signal table ─────────────────────────────────────────────────────

print("Building signal table...")
business_days = pd.bdate_range(start=START, end=END, tz="UTC")
rows: list[dict] = []
n_total = len(business_days) * len(PAIRS)
n_done = 0

for date in business_days:
    dk = _norm_date(date)
    sent_row = sent_lookup.get(dk)

    for pair, pair_m in zip(PAIRS, PAIRS_M):
        row: dict = {"date": dk, "pair": pair}

        # Technical — must not fail
        tf_sigs = {tf: tech_agents[pair_m][tf].predict(pair_m, date) for tf in TIMEFRAMES}
        fused = fuse_timeframe_signals(tf_sigs)
        row["tech_direction"] = fused.direction
        row["tech_confidence"] = fused.confidence
        row["tech_vol_regime"] = fused.volatility_regime

        # Geopolitical — must not fail
        geo_sig = geo_agent.predict(pair, date)
        row["geo_bilateral_risk"] = float(geo_sig.bilateral_risk_score)
        row["geo_risk_regime"] = geo_sig.risk_regime

        # Macro — KeyError is the only legitimate missing-data exception;
        # anything else is a real bug and should propagate.
        try:
            msig = macro_agent.predict(pair_m, date)
            row["macro_direction"] = msig.module_c_direction
            row["macro_confidence"] = float(msig.macro_confidence)
            row["macro_carry_score"] = float(msig.carry_signal_score)
            row["macro_regime_score"] = float(msig.regime_context_score)
            row["macro_fundamental_score"] = float(msig.fundamental_mispricing_score)
            row["macro_surprise_score"] = float(msig.macro_surprise_score)
            row["macro_bias_score"] = float(msig.macro_bias_score)
            row["macro_dominant_driver"] = msig.dominant_driver
        except KeyError as exc:
            raise RuntimeError(
                f"Macro signal missing for {pair} on {dk} — backfill incomplete? {exc}"
            ) from exc

        # Sentiment — must be present for every business day
        if sent_row is None:
            raise RuntimeError(
                f"No sentiment row for {dk} — sentiment batch did not cover this date"
            )
        row["usdjpy_stocktwits_vol_signal"] = sent_row.get("usdjpy_stocktwits_vol_signal")
        row["gdelt_tone_zscore"] = sent_row.get("gdelt_tone_zscore")
        row["gdelt_attention_zscore"] = sent_row.get("gdelt_attention_zscore")
        row["macro_attention_zscore"] = sent_row.get("macro_attention_zscore")
        row["composite_stress_flag"] = bool(sent_row.get("composite_stress_flag", False))

        # Forward return labels — null at the tail of the window is expected
        fwd = fwd_returns[pair_m].get(dk, {})
        row["fwd_ret_1d"] = fwd.get("fwd_ret_1d")
        row["fwd_ret_5d"] = fwd.get("fwd_ret_5d")
        row["fwd_vol_3d"] = fwd.get("fwd_vol_3d")
        row["fwd_vol_5d"] = fwd.get("fwd_vol_5d")
        row["fwd_vol_10d"] = fwd.get("fwd_vol_10d")

        rows.append(row)
        n_done += 1

    if n_done % 400 == 0:
        print(f"  {n_done}/{n_total} ({100*n_done//n_total}%)")

print(f"  {n_done}/{n_total} done")

# ── Save ───────────────────────────────────────────────────────────────────

out_df = pd.DataFrame(rows).sort_values(["date", "pair"]).reset_index(drop=True)
OUT.parent.mkdir(parents=True, exist_ok=True)
out_df.to_parquet(OUT, engine="pyarrow", index=False)

print(f"\nSaved {len(out_df)} rows to {OUT}")
print(f"Columns ({len(out_df.columns)}): {list(out_df.columns)}")
print(f"Date range: {out_df['date'].min()} to {out_df['date'].max()}")
print("Null rates (non-zero only):")
null_rates = (out_df.isnull().sum() / len(out_df) * 100).round(1)
print(null_rates[null_rates > 0].to_string())
