from collections import Counter
from pathlib import Path

import pandas as pd

from src.agents.coordinator.backtest import Backtester, _day_open
from src.agents.coordinator.calibration import CalibrationParams
from src.agents.coordinator.predict import Coordinator

_SIGNALS_PATH = Path("data/processed/coordinator/signals_aligned.parquet")
_OHLCV_DIR = Path("data/processed/ohlcv")

_NB04_CAL = CalibrationParams(
    usdjpy_conf_p75=0.088708,
    geo_slope=0.002405,
    geo_intercept=0.004057,
    st_slope=0.004930,
    st_intercept=0.006403,
    baseline_vol=0.004524,
)


def _load_h1(pair: str):
    files = sorted(_OHLCV_DIR.glob(f"ohlcv_{pair}m_H1_*.parquet")) or sorted(
        _OHLCV_DIR.glob(f"ohlcv_{pair}_H1_*.parquet")
    )
    if not files:
        return None
    frames = []
    for f in files:
        frame = pd.read_parquet(f)
        if "timestamp_utc" in frame.columns:
            frame = frame.set_index("timestamp_utc")
        frame.index = pd.to_datetime(frame.index, utc=True)
        frame.columns = frame.columns.str.lower()
        frames.append(frame)
    out = pd.concat(frames).sort_index()
    out = out[~out.index.duplicated(keep="first")]
    return out[["open", "high", "low", "close"]]


h1 = {p: _load_h1(p) for p in ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]}

signals_df = pd.read_parquet(_SIGNALS_PATH)
signals_df["date"] = pd.to_datetime(signals_df["date"])

coord = Coordinator(calibration=_NB04_CAL)
backtester = Backtester(coordinator=coord, bars=h1)
res = backtester.run(signals_df)

print("n_trades", res.n_trades)
print("top_pick_distribution", res.top_pick_distribution)
reasons = Counter(t.exit_reason for t in res.closed_trades)
print("reasons", reasons)

# print first 10 closed trades
for t in res.closed_trades[:10]:
    print(t)

# Diagnostic: list dates where a trade signal existed but no entry was opened
reports = coord.build_batch(signals_df)
sorted_dates = sorted(reports)
skipped = []
for i, current_date in enumerate(sorted_dates):
    report = reports[current_date]
    if report.overall_action == "trade" and report.top_pick is not None:
        pair = report.top_pick
        if i + 1 >= len(sorted_dates):
            skipped.append((current_date, pair, "no_next_date"))
            continue
        next_date = sorted_dates[i + 1]
        bars = h1.get(pair)
        if bars is None:
            skipped.append((current_date, pair, "no_bars"))
            continue
        entry_px = _day_open(bars, next_date)
        if entry_px is None:
            skipped.append((current_date, pair, "no_entry_px"))

print("\nSkipped entries (date, pair, reason):")
for s in skipped[:50]:
    print(s)
print("Total skipped:", len(skipped))

# Additional diagnostics: candidate trade signals vs actual opened trades
candidate_dates = []
invalid_action = []
for d in sorted_dates:
    r = reports[d]
    if r.overall_action == "trade" and r.top_pick is not None:
        candidate_dates.append((d, r.top_pick))
        pa = next((a for a in r.pairs if a.pair == r.top_pick), None)
        if pa is None or pa.suggested_action not in ("LONG", "SHORT"):
            invalid_action.append((d, r.top_pick, getattr(pa, "suggested_action", None)))

print("\nCandidate trade signals count:", len(candidate_dates))
print("Invalid suggested actions count:", len(invalid_action))
if invalid_action:
    print("Examples of invalid actions:", invalid_action[:10])
