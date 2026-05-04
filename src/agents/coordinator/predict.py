from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.agents.coordinator.calibration import CalibrationParams, load_calibration
from src.agents.coordinator.signal import CoordinatorReport, CoordinatorSignal, PairAnalysis

# ── Per-pair macro signal configuration (IC and tier from NB02/NB03 walk-forward) ──────
_PAIR_MACRO_CONFIG: dict[str, tuple[str, str, float]] = {
    "GBPUSD": ("medium", "5d", 0.115),
    "EURUSD": ("low", "5d", 0.060),
    "USDCHF": ("low", "5d", 0.033),
    "USDJPY": ("low", "5d", 0.058),
}

# USDJPY tech-gate: validated accuracy = 58.6% on gated days; edge = accuracy − 0.5
_TECH_EDGE = 0.086

_TIER_WEIGHTS: dict[str, float] = {"high": 1.0, "medium": 0.6, "low": 0.3, "none": 0.0}
_BASE_POSITION: dict[str, float] = {"high": 2.0, "medium": 1.0, "low": 0.5, "none": 0.0}
_SL_MULT = 1.5
_TP_MULT = 2.5
_REGIME_DISCOUNT = 0.75  # position scale-down in high_attention regime
_REGIME_THRESHOLD = 1.0  # macro_attention_zscore threshold for high_attention

# HOLD gate thresholds
_HOLD_MIN_CONVICTION = 0.02
_HOLD_REGIME_CONVICTION_FLOOR = 0.05

# Leakage guard: these columns must never enter the coordinator
_FWD_COLS = frozenset(["fwd_ret_1d", "fwd_ret_5d", "fwd_vol_3d", "fwd_vol_5d", "fwd_vol_10d"])


def _coordinator_signal(row: pd.Series, cal: CalibrationParams) -> CoordinatorSignal:
    pair = str(row["pair"])
    date = pd.Timestamp(row["date"])

    stocktwits_val = row.get("usdjpy_stocktwits_vol_signal")
    if pair == "USDJPY" and pd.notna(stocktwits_val):
        vol_signal: float | None = float(stocktwits_val)
        vol_source = "stocktwits"
    else:
        geo_val = row.get("geo_bilateral_risk")
        vol_signal = float(geo_val) if pd.notna(geo_val) else None
        vol_source = "geo" if vol_signal is not None else "none"

    flat_reason: str | None = None

    if pair == "USDJPY":
        conf = row.get("tech_confidence")
        if pd.notna(conf) and float(conf) >= cal.usdjpy_conf_p75:
            direction: int | None = int(row["tech_direction"])
            direction_source = "tech_usdjpy"
            direction_horizon, direction_ic, confidence_tier = "1d", None, "high"
        else:
            macro_dir = row.get("macro_direction")
            if pd.notna(macro_dir):
                direction = 1 if macro_dir == "up" else 0
                tier, horizon, ic_val = _PAIR_MACRO_CONFIG["USDJPY"]
                direction_source = "macro_usdjpy"
                direction_horizon, direction_ic, confidence_tier = horizon, ic_val, tier
            else:
                direction, direction_source = None, "flat"
                direction_horizon, direction_ic, confidence_tier = "none", None, "none"
                flat_reason = "macro_direction unavailable"
    else:
        macro_dir = row.get("macro_direction")
        if pd.notna(macro_dir):
            direction = 1 if macro_dir == "up" else 0
            tier, horizon, ic_val = _PAIR_MACRO_CONFIG[pair]
            direction_source = f"macro_{pair.lower()}"
            direction_horizon, direction_ic, confidence_tier = horizon, ic_val, tier
        else:
            direction, direction_source = None, "flat"
            direction_horizon, direction_ic, confidence_tier = "none", None, "none"
            flat_reason = "macro_direction unavailable"

    macro_z = row.get("macro_attention_zscore")
    if pd.isna(macro_z):
        regime = "unknown"
    elif float(macro_z) > _REGIME_THRESHOLD:
        regime = "high_attention"
    else:
        regime = "normal"

    return CoordinatorSignal(
        pair=pair,
        date=date,
        vol_signal=vol_signal,
        vol_source=vol_source,
        direction=direction,
        direction_source=direction_source,
        direction_horizon=direction_horizon,
        direction_ic=direction_ic,
        confidence_tier=confidence_tier,
        flat_reason=flat_reason,
        regime=regime,
    )


def _estimate_vol(vol_signal_norm: float | None, vol_source: str, cal: CalibrationParams) -> float:
    if vol_signal_norm is None:
        return cal.baseline_vol
    if vol_source == "geo":
        return max(0.001, cal.geo_intercept + cal.geo_slope * vol_signal_norm)
    if vol_source == "stocktwits":
        return max(0.001, cal.st_intercept + cal.st_slope * vol_signal_norm)
    return cal.baseline_vol


def _compute_sl_tp(estimated_vol: float, direction_horizon: str) -> tuple[float, float]:
    if direction_horizon == "1d":
        expected_move = estimated_vol
    elif direction_horizon == "5d":
        expected_move = estimated_vol * (5**0.5)
    else:
        expected_move = estimated_vol * 2.0
    sl = round(_SL_MULT * expected_move * 100, 4)
    tp = round(_TP_MULT * expected_move * 100, 4)
    return sl, tp


def _build_pair_analysis(
    sig: CoordinatorSignal, row: pd.Series, cal: CalibrationParams
) -> PairAnalysis:
    vol_norm = (
        -sig.vol_signal
        if sig.vol_source == "stocktwits" and sig.vol_signal is not None
        else sig.vol_signal
    )
    est_vol = _estimate_vol(vol_norm, sig.vol_source, cal)

    if sig.direction == 1:
        direction_label, suggested_action = f"LONG {sig.pair}", "LONG"
    elif sig.direction == 0:
        direction_label, suggested_action = f"SHORT {sig.pair}", "SHORT"
    else:
        direction_label, suggested_action = f"FLAT {sig.pair}", "FLAT"

    weight = _TIER_WEIGHTS.get(sig.confidence_tier, 0.0)
    edge = (
        _TECH_EDGE
        if sig.direction_source == "tech_usdjpy"
        else (abs(sig.direction_ic) if sig.direction_ic is not None else 0.0)
    )
    conv = round(weight * edge, 4)

    pos_pct = _BASE_POSITION.get(sig.confidence_tier, 0.0)
    if sig.direction is not None and sig.regime == "high_attention":
        pos_pct *= _REGIME_DISCOUNT
    if sig.direction is None:
        pos_pct = 0.0
    pos_pct = round(pos_pct, 4)

    sl, tp = _compute_sl_tp(est_vol, sig.direction_horizon)
    rr = round(tp / sl, 2) if sl > 0 else 0.0

    raw: dict = {
        "tech_direction": row.get("tech_direction"),
        "tech_confidence": row.get("tech_confidence"),
        "tech_vol_regime": row.get("tech_vol_regime"),
        "geo_bilateral_risk": row.get("geo_bilateral_risk"),
        "geo_risk_regime": row.get("geo_risk_regime"),
        "macro_direction": row.get("macro_direction"),
        "macro_confidence": row.get("macro_confidence"),
        "macro_dominant_driver": row.get("macro_dominant_driver"),
        "macro_carry_score": row.get("macro_carry_score"),
        "macro_fundamental_score": row.get("macro_fundamental_score"),
        "macro_regime_score": row.get("macro_regime_score"),
        "macro_surprise_score": row.get("macro_surprise_score"),
        "macro_bias_score": row.get("macro_bias_score"),
        "gdelt_tone_zscore": row.get("gdelt_tone_zscore"),
        "macro_attention_zscore": row.get("macro_attention_zscore"),
        "composite_stress_flag": row.get("composite_stress_flag"),
    }
    if sig.pair == "USDJPY":
        raw["usdjpy_stocktwits_vol_signal"] = row.get("usdjpy_stocktwits_vol_signal")

    return PairAnalysis(
        pair=sig.pair,
        direction=sig.direction,
        direction_label=direction_label,
        confidence_tier=sig.confidence_tier,
        direction_source=sig.direction_source,
        direction_horizon=sig.direction_horizon,
        direction_ic=sig.direction_ic,
        vol_signal_norm=vol_norm,
        vol_source=sig.vol_source,
        estimated_vol_3d=est_vol,
        regime=sig.regime,
        suggested_action=suggested_action,
        conviction_score=conv,
        position_size_pct=pos_pct,
        sl_pct=sl,
        tp_pct=tp,
        risk_reward_ratio=rr,
        flat_reason=sig.flat_reason,
        raw_signals=raw,
    )


def _sanitize_for_json(obj: object) -> object:
    """Recursively replace float NaN/Inf with None so the tree is valid JSON for PostgreSQL JSONB."""
    if isinstance(obj, float) and (obj != obj or obj == float("inf") or obj == float("-inf")):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


def _build_narrative_context(
    analyses: list[PairAnalysis],
    date: pd.Timestamp,
    overall_action: str,
    hold_reason: str | None,
    global_regime: str,
) -> dict:
    ranked = sorted(analyses, key=lambda a: a.conviction_score, reverse=True)
    top = ranked[0] if overall_action == "trade" and ranked else None

    ctx: dict = {
        "date": str(date.date()),
        "overall_action": overall_action,
        "hold_reason": hold_reason,
        "global_regime": global_regime,
        "top_pick": top.pair if top else None,
    }

    if top:
        ctx["top_recommendation"] = {
            "pair": top.pair,
            "action": top.suggested_action,
            "confidence": top.confidence_tier,
            "position_size_pct": top.position_size_pct,
            "sl_pct": top.sl_pct,
            "tp_pct": top.tp_pct,
            "risk_reward": top.risk_reward_ratio,
            "horizon": top.direction_horizon,
            "signal_source": top.direction_source,
            "direction_ic": top.direction_ic,
            "estimated_vol_pct": round(top.estimated_vol_3d * 100, 3),
            "vol_source": top.vol_source,
        }

    ctx["all_pairs"] = [
        {
            "pair": a.pair,
            "action": a.suggested_action,
            "confidence": a.confidence_tier,
            "conviction": a.conviction_score,
            "horizon": a.direction_horizon,
            "estimated_vol_pct": round(a.estimated_vol_3d * 100, 3),
            "vol_source": a.vol_source,
            "regime": a.regime,
            "macro_driver": a.raw_signals.get("macro_dominant_driver"),
            "geo_risk_regime": a.raw_signals.get("geo_risk_regime"),
            "macro_attention_z": a.raw_signals.get("macro_attention_zscore"),
            "gdelt_tone_z": a.raw_signals.get("gdelt_tone_zscore"),
            "composite_stress": a.raw_signals.get("composite_stress_flag"),
        }
        for a in ranked
    ]
    return _sanitize_for_json(ctx)  # type: ignore[return-value]


class Coordinator:
    """Deterministic signal coordinator: produces CoordinatorReport from aligned agent signals."""

    def __init__(
        self,
        calibration: CalibrationParams | None = None,
        calibration_path: Path | None = None,
    ) -> None:
        if calibration is not None:
            self._cal = calibration
        else:
            self._cal = load_calibration(
                calibration_path
                or Path(__file__).resolve().parents[3]
                / "models"
                / "production"
                / "coordinator_calibration.json"
            )

    @property
    def calibration(self) -> CalibrationParams:
        return self._cal

    def build_report(self, date_rows: pd.DataFrame) -> CoordinatorReport:
        """Build CoordinatorReport for a single date from aligned signal rows.

        Strips any fwd_* columns defensively before processing.
        """
        safe = date_rows.drop(columns=[c for c in date_rows.columns if c in _FWD_COLS])
        date = pd.Timestamp(safe["date"].iloc[0])

        analyses: list[PairAnalysis] = []
        for _, row in safe.iterrows():
            sig = _coordinator_signal(row, self._cal)
            analyses.append(_build_pair_analysis(sig, row, self._cal))

        global_regime = analyses[0].regime if analyses else "unknown"
        max_conv = max((a.conviction_score for a in analyses), default=0.0)

        if max_conv < _HOLD_MIN_CONVICTION or (
            global_regime == "high_attention" and max_conv < _HOLD_REGIME_CONVICTION_FLOOR
        ):
            overall_action = "hold"
            hold_reason: str | None = f"Max conviction {max_conv:.4f} below threshold"
        else:
            overall_action = "hold" if not analyses else "trade"
            hold_reason = None

        tradeable = [a for a in analyses if a.direction is not None]
        ranked_pairs = [
            a.pair for a in sorted(tradeable, key=lambda a: a.conviction_score, reverse=True)
        ]
        top_pick = ranked_pairs[0] if ranked_pairs and overall_action == "trade" else None

        ctx = _build_narrative_context(analyses, date, overall_action, hold_reason, global_regime)

        return CoordinatorReport(
            date=date,
            pairs=analyses,
            ranked_pairs=ranked_pairs,
            top_pick=top_pick,
            overall_action=overall_action,
            hold_reason=hold_reason,
            global_regime=global_regime,
            narrative_context=ctx,
        )

    def build_batch(self, signals_df: pd.DataFrame) -> dict[pd.Timestamp, CoordinatorReport]:
        """Build one CoordinatorReport per date from the full signals table."""
        safe = signals_df.drop(columns=[c for c in signals_df.columns if c in _FWD_COLS])
        reports: dict[pd.Timestamp, CoordinatorReport] = {}
        for date_val in sorted(safe["date"].unique()):
            date_ts = pd.Timestamp(date_val)
            rows = safe[safe["date"] == date_val]
            reports[date_ts] = self.build_report(rows)
        return reports
