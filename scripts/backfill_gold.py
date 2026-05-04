"""Backfill the Gold layer (PostgreSQL) from the Silver signals table.

Usage:
    python scripts/backfill_gold.py [--backtest-run LABEL] [--dry-run]

Steps:
    1. Load signals_aligned.parquet (strips fwd_* columns).
    2. Insert all rows into agent_signals.
    3. Run Coordinator.build_batch() → insert coordinator_signals + coordinator_reports.
    4. Run Backtester against H1 OHLCV → insert trade_log.

The script is idempotent: duplicate (date, pair) rows are silently skipped.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.coordinator.backtest import Backtester
from src.agents.coordinator.calibration import load_calibration
from src.agents.coordinator.predict import Coordinator
from src.shared.db.storage import (
    insert_agent_signals,
    insert_coordinator_report,
    insert_coordinator_signals,
    insert_trade_log,
)

_SIGNALS_PATH = ROOT / "data" / "processed" / "coordinator" / "signals_aligned.parquet"
_OHLCV_DIR = ROOT / "data" / "processed" / "ohlcv"
_PAIRS = ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]
_FWD_COLS = {"fwd_ret_1d", "fwd_ret_5d", "fwd_vol_3d", "fwd_vol_5d", "fwd_vol_10d"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backfill_gold")


def _load_h1(pair: str) -> pd.DataFrame | None:
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


def _row_to_agent_signal_dict(row: pd.Series) -> dict:
    composite = row.get("composite_stress_flag")
    return {
        "date": pd.Timestamp(row["date"]).date(),
        "pair": str(row["pair"]),
        "tech_direction": (
            int(row["tech_direction"]) if pd.notna(row.get("tech_direction")) else None
        ),
        "tech_confidence": (
            float(row["tech_confidence"]) if pd.notna(row.get("tech_confidence")) else None
        ),
        "tech_vol_regime": row.get("tech_vol_regime"),
        "geo_bilateral_risk": (
            float(row["geo_bilateral_risk"]) if pd.notna(row.get("geo_bilateral_risk")) else None
        ),
        "geo_risk_regime": row.get("geo_risk_regime"),
        "macro_direction": row.get("macro_direction"),
        "macro_confidence": (
            float(row["macro_confidence"]) if pd.notna(row.get("macro_confidence")) else None
        ),
        "macro_carry_score": (
            float(row["macro_carry_score"]) if pd.notna(row.get("macro_carry_score")) else None
        ),
        "macro_regime_score": (
            float(row["macro_regime_score"]) if pd.notna(row.get("macro_regime_score")) else None
        ),
        "macro_fundamental_score": (
            float(row["macro_fundamental_score"])
            if pd.notna(row.get("macro_fundamental_score"))
            else None
        ),
        "macro_surprise_score": (
            float(row["macro_surprise_score"])
            if pd.notna(row.get("macro_surprise_score"))
            else None
        ),
        "macro_bias_score": (
            float(row["macro_bias_score"]) if pd.notna(row.get("macro_bias_score")) else None
        ),
        "macro_dominant_driver": row.get("macro_dominant_driver"),
        "usdjpy_stocktwits_vol_signal": (
            float(row["usdjpy_stocktwits_vol_signal"])
            if pd.notna(row.get("usdjpy_stocktwits_vol_signal"))
            else None
        ),
        "gdelt_tone_zscore": (
            float(row["gdelt_tone_zscore"]) if pd.notna(row.get("gdelt_tone_zscore")) else None
        ),
        "gdelt_attention_zscore": (
            float(row["gdelt_attention_zscore"])
            if pd.notna(row.get("gdelt_attention_zscore"))
            else None
        ),
        "macro_attention_zscore": (
            float(row["macro_attention_zscore"])
            if pd.notna(row.get("macro_attention_zscore"))
            else None
        ),
        "composite_stress_flag": bool(composite) if composite is not None else None,
    }


def backfill(backtest_run: str = "backtest_nb05_v1", dry_run: bool = False) -> None:
    if not _SIGNALS_PATH.exists():
        logger.error("signals_aligned.parquet not found at %s", _SIGNALS_PATH)
        sys.exit(1)

    # ── Load signals (strip fwd_* leakage) ──────────────────────────────
    signals_raw = pd.read_parquet(_SIGNALS_PATH)
    signals_raw["date"] = pd.to_datetime(signals_raw["date"])
    signals_df = signals_raw.drop(columns=[c for c in signals_raw.columns if c in _FWD_COLS])
    logger.info(
        "Loaded signals_aligned: %d rows × %d cols (%s → %s)",
        len(signals_df),
        len(signals_df.columns),
        signals_df["date"].min().date(),
        signals_df["date"].max().date(),
    )

    # ── Step 1: agent_signals ────────────────────────────────────────────
    logger.info("Step 1: inserting agent_signals …")
    agent_rows = [_row_to_agent_signal_dict(row) for _, row in signals_df.iterrows()]
    if not dry_run:
        n_agent = insert_agent_signals(agent_rows)
        logger.info("  inserted %d / %d agent_signal rows", n_agent, len(agent_rows))
    else:
        logger.info("  [dry-run] would insert %d agent_signal rows", len(agent_rows))

    # ── Step 2: coordinator_signals + coordinator_reports ────────────────
    logger.info("Step 2: running Coordinator.build_batch() …")
    cal = load_calibration()
    coordinator = Coordinator(calibration=cal)
    reports = coordinator.build_batch(signals_df)
    logger.info("  built %d CoordinatorReports", len(reports))

    coord_signal_rows: list[dict] = []
    coord_report_rows: list[dict] = []

    for date_ts, report in reports.items():
        date_val = date_ts.date()
        for pa in report.pairs:
            coord_signal_rows.append(
                {
                    "date": date_val,
                    "pair": pa.pair,
                    "vol_signal": pa.vol_signal_norm,
                    "vol_source": pa.vol_source,
                    "direction": pa.direction,
                    "direction_source": pa.direction_source,
                    "direction_horizon": pa.direction_horizon,
                    "direction_ic": pa.direction_ic,
                    "confidence_tier": pa.confidence_tier,
                    "flat_reason": None,
                    "regime": pa.regime,
                    "suggested_action": pa.suggested_action,
                    "conviction_score": pa.conviction_score,
                    "position_size_pct": pa.position_size_pct,
                    "sl_pct": pa.sl_pct,
                    "tp_pct": pa.tp_pct,
                    "risk_reward_ratio": pa.risk_reward_ratio,
                    "estimated_vol_3d": pa.estimated_vol_3d,
                    "is_top_pick": pa.pair == report.top_pick,
                    "overall_action": report.overall_action,
                }
            )
        coord_report_rows.append(
            {
                "date": date_val,
                "top_pick": report.top_pick,
                "overall_action": report.overall_action,
                "hold_reason": report.hold_reason,
                "global_regime": report.global_regime,
                "narrative_context": report.narrative_context,
            }
        )

    if not dry_run:
        n_coord_sig = insert_coordinator_signals(coord_signal_rows)
        logger.info(
            "  inserted %d / %d coordinator_signal rows", n_coord_sig, len(coord_signal_rows)
        )
        n_coord_rep = sum(1 for r in coord_report_rows if insert_coordinator_report(r))
        logger.info(
            "  inserted %d / %d coordinator_report rows", n_coord_rep, len(coord_report_rows)
        )
    else:
        logger.info(
            "  [dry-run] would insert %d coordinator_signal rows + %d coordinator_report rows",
            len(coord_signal_rows),
            len(coord_report_rows),
        )

    # ── Step 3: trade_log from backtester ─────────────────────────────────
    logger.info("Step 3: loading H1 bars and running backtester …")
    h1 = {p: _load_h1(p) for p in _PAIRS}
    missing = [p for p, bars in h1.items() if bars is None]
    if missing:
        logger.warning("  H1 bars missing for: %s — trade_log step skipped", missing)
        return

    backtester = Backtester(coordinator=coordinator, bars=h1)
    result = backtester.run(signals_df)
    logger.info(
        "  backtest complete: %d trades | Sharpe=%.3f | max_dd=%.2f%% | PF=%.2f",
        result.n_trades,
        result.sharpe,
        result.max_drawdown * 100,
        result.profit_factor,
    )

    trade_rows = [
        {
            "backtest_run": backtest_run,
            "pair": t.pair,
            "direction": t.direction,
            "entry_date": t.entry_date.date() if hasattr(t.entry_date, "date") else t.entry_date,
            "entry_price": t.entry_price,
            "exit_date": t.exit_date.date() if hasattr(t.exit_date, "date") else t.exit_date,
            "exit_price": t.exit_price,
            "exit_reason": t.exit_reason,
            "hold_days": t.hold_days,
            "position_pct": t.position_pct,
            "cost_pct": t.cost_pct,
            "gross_pnl_pct": t.gross_pnl_pct,
            "net_pnl_pct": t.net_pnl_pct,
            "equity_delta": t.equity_delta,
        }
        for t in result.closed_trades
    ]

    if not dry_run:
        n_trades = insert_trade_log(trade_rows, backtest_run=backtest_run)
        logger.info("  inserted %d / %d trade_log rows", n_trades, len(trade_rows))
    else:
        logger.info("  [dry-run] would insert %d trade_log rows", len(trade_rows))

    logger.info("Backfill complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Gold layer from Silver signals")
    parser.add_argument(
        "--backtest-run",
        default="backtest_nb05_v1",
        help="Label for trade_log.backtest_run (default: backtest_nb05_v1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be inserted without touching the DB",
    )
    args = parser.parse_args()
    backfill(backtest_run=args.backtest_run, dry_run=args.dry_run)
