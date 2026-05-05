from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.agents.coordinator.predict import Coordinator
from src.agents.coordinator.signal import ClosedTrade, CoordinatorReport, OpenTrade

PIP_SIZE: dict[str, float] = {
    "EURUSD": 0.0001,
    "GBPUSD": 0.0001,
    "USDCHF": 0.0001,
    "USDJPY": 0.01,
}
_COST_PIPS = 2.0  # round-trip spread in pips


@dataclass(frozen=True)
class BacktestResult:
    closed_trades: list[ClosedTrade]
    equity_curve: pd.Series  # date-indexed equity (normalized to 1.0)
    sharpe: float  # annualised (252 trading days)
    max_drawdown: float  # as a fraction (negative)
    profit_factor: float
    win_rate: float
    n_trades: int
    top_pick_distribution: dict[str, float]  # pair → fraction of trade days


def _day_open(bars: pd.DataFrame, date: pd.Timestamp) -> float | None:
    day = date.tz_localize("UTC") if date.tzinfo is None else date.normalize()
    window = bars[(bars.index >= day) & (bars.index < day + pd.Timedelta(days=1))]
    if not window.empty:
        return float(window.iloc[0]["open"])
    # Fallback: return first available bar on or after `day` (handles market holidays / missing day slices)
    later = bars[bars.index >= day]
    if not later.empty:
        return float(later.iloc[0]["open"])
    return None


def _day_close(bars: pd.DataFrame, date: pd.Timestamp) -> float | None:
    day = date.tz_localize("UTC") if date.tzinfo is None else date.normalize()
    window = bars[(bars.index >= day) & (bars.index < day + pd.Timedelta(days=1))]
    return float(window.iloc[-1]["close"]) if not window.empty else None


def _resolve_sl_tp(
    trade: OpenTrade,
    bars: pd.DataFrame,
    from_dt: pd.Timestamp,
    to_dt: pd.Timestamp,
) -> tuple[str | None, float | None]:
    """Walk H1 bars and return the first SL/TP hit, or (None, None) if neither hit."""
    window = bars[(bars.index > from_dt) & (bars.index <= to_dt)]
    for _, bar in window.iterrows():
        lo, hi = float(bar["low"]), float(bar["high"])
        if trade.direction == "LONG":
            if lo <= trade.sl_price and hi >= trade.tp_price:
                return "SL", trade.sl_price  # worst case: SL resolves before TP
            if lo <= trade.sl_price:
                return "SL", trade.sl_price
            if hi >= trade.tp_price:
                return "TP", trade.tp_price
        else:
            if hi >= trade.sl_price and lo <= trade.tp_price:
                return "SL", trade.sl_price
            if hi >= trade.sl_price:
                return "SL", trade.sl_price
            if lo <= trade.tp_price:
                return "TP", trade.tp_price
    return None, None


def _close(
    trade: OpenTrade,
    exit_date: pd.Timestamp,
    exit_price: float,
    reason: str,
    hold_days: int,
) -> ClosedTrade:
    sign = 1 if trade.direction == "LONG" else -1
    gross = sign * (exit_price - trade.entry_price) / trade.entry_price * 100
    net = gross - trade.cost_pct
    delta = net / 100.0 * trade.position_pct / 100.0
    return ClosedTrade(
        pair=trade.pair,
        direction=trade.direction,
        entry_date=trade.entry_date,
        entry_price=trade.entry_price,
        exit_date=exit_date,
        exit_price=exit_price,
        exit_reason=reason,
        hold_days=hold_days,
        position_pct=trade.position_pct,
        cost_pct=trade.cost_pct,
        gross_pnl_pct=round(gross, 6),
        net_pnl_pct=round(net, 6),
        equity_delta=round(delta, 8),
    )


class Backtester:
    """Daily-cadence backtester consuming CoordinatorReport signals and H1 OHLCV bars.

    Execution model:
      - Signal for date T is generated at end of T (after NY close).
      - Entry at T+1 first H1 open (avoids T's close look-ahead).
      - SL/TP resolution: H1 bars on T+1.
      - If neither hit: hold if signal continues, else FLIP at T+1 close.
      - Costs: 2 pips round-trip per new entry.
      - Only top_pick pair, one position at a time.
    """

    def __init__(
        self,
        coordinator: Coordinator,
        bars: dict[str, pd.DataFrame],
        cost_pips: float = _COST_PIPS,
    ) -> None:
        self._coordinator = coordinator
        self._bars = bars
        self._cost_pips = cost_pips

    def run(self, signals_df: pd.DataFrame) -> BacktestResult:
        """Run backtest on the full signals table.

        Args:
            signals_df: The signals_aligned DataFrame.  fwd_* columns are stripped
                        defensively inside Coordinator.build_batch().
        """
        reports = self._coordinator.build_batch(signals_df)
        sorted_dates = sorted(reports)

        equity = 1.0
        equity_curve: list[tuple[pd.Timestamp, float]] = []
        closed_trades: list[ClosedTrade] = []
        open_trades: dict[str, OpenTrade] = {}
        open_idx_map: dict[str, int] = {}

        for i, current_date in enumerate(sorted_dates):
            report: CoordinatorReport = reports[current_date]

            # Check each open trade for SL/TP or FLIP conditions
            current_date_utc = (
                current_date.tz_localize("UTC") if current_date.tzinfo is None else current_date
            )
            day_end = current_date_utc + pd.Timedelta(hours=23, minutes=59)
            day_start_excl = current_date_utc - pd.Timedelta(seconds=1)

            for pair, ot in list(open_trades.items()):
                pair_bars = self._bars.get(ot.pair)
                if pair_bars is not None:
                    reason, exit_px = _resolve_sl_tp(ot, pair_bars, day_start_excl, day_end)
                else:
                    reason, exit_px = None, None

                if reason is not None and exit_px is not None:
                    idx = open_idx_map.get(pair, 0)
                    ct = _close(ot, current_date, exit_px, reason, i - idx)
                    closed_trades.append(ct)
                    equity *= 1.0 + ct.equity_delta
                    del open_trades[pair]
                    del open_idx_map[pair]
                else:
                    new_top = report.top_pick
                    new_dir: str | None = None
                    if new_top:
                        for pa in report.pairs:
                            if pa.pair == new_top:
                                new_dir = pa.suggested_action
                                break
                    signal_continues = (
                        report.overall_action == "trade"
                        and new_top == ot.pair
                        and new_dir == ot.direction
                    )
                    if not signal_continues:
                        pair_bars2 = self._bars.get(ot.pair)
                        eod_px = (
                            _day_close(pair_bars2, current_date) if pair_bars2 is not None else None
                        )
                        if eod_px is None:
                            eod_px = ot.entry_price
                        idx = open_idx_map.get(pair, 0)
                        ct = _close(ot, current_date, eod_px, "FLIP", i - idx)
                        closed_trades.append(ct)
                        equity *= 1.0 + ct.equity_delta
                        del open_trades[pair]
                        del open_idx_map[pair]

            if report.overall_action == "trade" and report.top_pick is not None:
                pair = report.top_pick
                pa = next((a for a in report.pairs if a.pair == pair), None)
                # Only open a new trade if this pair doesn't already have an open position
                if (
                    pa is not None
                    and pa.suggested_action in ("LONG", "SHORT")
                    and pair not in open_trades
                ):
                    next_bars = self._bars.get(pair)
                    entry_px: float | None = None
                    if next_bars is not None and i + 1 < len(sorted_dates):
                        next_date = sorted_dates[i + 1]
                        entry_px = _day_open(next_bars, next_date)

                    if entry_px is not None:
                        pip = PIP_SIZE.get(pair, 0.0001)
                        cost_pct = self._cost_pips * pip / entry_px * 100
                        sign = 1 if pa.suggested_action == "LONG" else -1
                        ot = OpenTrade(
                            pair=pair,
                            direction=pa.suggested_action,
                            entry_date=sorted_dates[i + 1],
                            entry_price=entry_px,
                            sl_price=entry_px * (1.0 - sign * pa.sl_pct / 100.0),
                            tp_price=entry_px * (1.0 + sign * pa.tp_pct / 100.0),
                            position_pct=pa.position_size_pct,
                            cost_pct=cost_pct,
                        )
                        open_trades[pair] = ot
                        open_idx_map[pair] = i + 1

            equity_curve.append((current_date, equity))

        # Close any remaining open trades at the final date
        if open_trades:
            last_date = sorted_dates[-1]
            for pair, ot in list(open_trades.items()):
                pair_bars = self._bars.get(ot.pair)
                last_px = _day_close(pair_bars, last_date) if pair_bars is not None else None
                idx = open_idx_map.get(pair, 0)
                if last_px is not None:
                    ct = _close(ot, last_date, last_px, "EOD_HOLD", len(sorted_dates) - 1 - idx)
                    closed_trades.append(ct)
                    equity *= 1.0 + ct.equity_delta

        eq_series = pd.Series(
            [e for _, e in equity_curve],
            index=[d for d, _ in equity_curve],
            name="equity",
        )
        daily_ret = eq_series.pct_change().fillna(0.0)
        sharpe = (
            float(np.sqrt(252) * daily_ret.mean() / daily_ret.std()) if daily_ret.std() > 0 else 0.0
        )
        peak = eq_series.cummax()
        max_dd = float(((eq_series - peak) / peak).min())

        n = len(closed_trades)
        n_win = sum(1 for t in closed_trades if t.net_pnl_pct > 0)
        win_rate = n_win / n if n > 0 else 0.0

        wins = [t.net_pnl_pct for t in closed_trades if t.net_pnl_pct > 0]
        losses = [abs(t.net_pnl_pct) for t in closed_trades if t.net_pnl_pct <= 0]
        gross_win = sum(wins)
        gross_loss = sum(losses)
        profit_factor = gross_win / gross_loss if gross_loss > 0 else float("inf")

        from collections import Counter  # noqa: PLC0415

        top_picks = [t.pair for t in closed_trades]
        counts = Counter(top_picks)
        top_pick_dist = {pair: counts[pair] / n for pair in counts} if n > 0 else {}

        return BacktestResult(
            closed_trades=closed_trades,
            equity_curve=eq_series,
            sharpe=round(sharpe, 3),
            max_drawdown=round(max_dd, 4),
            profit_factor=round(profit_factor, 3),
            win_rate=round(win_rate, 4),
            n_trades=n,
            top_pick_distribution=top_pick_dist,
        )
