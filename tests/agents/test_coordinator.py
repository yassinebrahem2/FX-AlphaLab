"""Tests for src.agents.coordinator.

Unit tests use synthetic fixtures.
The integration regression test pins NB05 backtest numbers and requires
data/processed/coordinator/signals_aligned.parquet + data/processed/ohlcv/*.parquet.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pandas as pd
import pytest

from src.agents.coordinator.calibration import CalibrationParams, load_calibration
from src.agents.coordinator.predict import Coordinator
from src.agents.coordinator.prompt import build_llm_prompt
from src.agents.coordinator.signal import CoordinatorReport

# ── Calibration ─────────────────────────────────────────────────────────────

_NB04_CAL = CalibrationParams(
    usdjpy_conf_p75=0.088708,
    geo_slope=0.002405,
    geo_intercept=0.004057,
    st_slope=0.004930,
    st_intercept=0.006403,
    baseline_vol=0.004524,
)

PAIRS = ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _make_signal_row(
    pair: str,
    day: str = "2024-01-15",
    macro_direction: str = "up",
    tech_direction: int = 1,
    tech_confidence: float = 0.05,
    geo_bilateral_risk: float = 1.5,
    macro_attention_zscore: float = 0.2,
    composite_stress_flag: bool = False,
) -> dict:
    return {
        "date": pd.Timestamp(day),
        "pair": pair,
        "tech_direction": tech_direction,
        "tech_confidence": tech_confidence,
        "tech_vol_regime": "normal",
        "geo_bilateral_risk": geo_bilateral_risk,
        "geo_risk_regime": "low",
        "macro_direction": macro_direction,
        "macro_confidence": 0.6,
        "macro_carry_score": 0.1,
        "macro_regime_score": 0.05,
        "macro_fundamental_score": 0.2,
        "macro_surprise_score": 0.0,
        "macro_bias_score": 0.05,
        "macro_dominant_driver": "fundamental_mispricing_score",
        "usdjpy_stocktwits_vol_signal": None,
        "gdelt_tone_zscore": -0.5,
        "gdelt_attention_zscore": 0.3,
        "macro_attention_zscore": macro_attention_zscore,
        "composite_stress_flag": composite_stress_flag,
    }


def _make_date_rows(day: str = "2024-01-15", **kwargs) -> pd.DataFrame:
    return pd.DataFrame([_make_signal_row(p, day, **kwargs) for p in PAIRS])


@pytest.fixture
def coordinator() -> Coordinator:
    return Coordinator(calibration=_NB04_CAL)


@pytest.fixture
def normal_report(coordinator: Coordinator) -> CoordinatorReport:
    return coordinator.build_report(_make_date_rows())


# ── Coordinator.build_report: structure ─────────────────────────────────────


def test_report_has_all_pairs(normal_report: CoordinatorReport) -> None:
    assert {pa.pair for pa in normal_report.pairs} == set(PAIRS)


def test_report_overall_action_trade(normal_report: CoordinatorReport) -> None:
    assert normal_report.overall_action == "trade"


def test_report_top_pick_is_gbpusd(normal_report: CoordinatorReport) -> None:
    assert normal_report.top_pick == "GBPUSD"


def test_ranked_pairs_sorted_by_conviction(normal_report: CoordinatorReport) -> None:
    conv_map = {pa.pair: pa.conviction_score for pa in normal_report.pairs}
    ranked_convictions = [conv_map[p] for p in normal_report.ranked_pairs]
    assert ranked_convictions == sorted(ranked_convictions, reverse=True)


def test_narrative_context_keys(normal_report: CoordinatorReport) -> None:
    ctx = normal_report.narrative_context
    assert "date" in ctx
    assert "overall_action" in ctx
    assert "top_recommendation" in ctx
    assert "all_pairs" in ctx


def test_narrative_context_all_pairs_count(normal_report: CoordinatorReport) -> None:
    assert len(normal_report.narrative_context["all_pairs"]) == len(PAIRS)


# ── Conviction and position sizing ──────────────────────────────────────────


def test_gbpusd_conviction_is_medium_tier(normal_report: CoordinatorReport) -> None:
    gbp = next(pa for pa in normal_report.pairs if pa.pair == "GBPUSD")
    assert gbp.confidence_tier == "medium"
    assert abs(gbp.conviction_score - round(0.6 * 0.115, 4)) < 1e-6


def test_position_size_gbpusd_normal_regime(normal_report: CoordinatorReport) -> None:
    gbp = next(pa for pa in normal_report.pairs if pa.pair == "GBPUSD")
    assert gbp.position_size_pct == pytest.approx(1.0, abs=1e-6)


def test_position_size_discounted_in_high_attention(coordinator: Coordinator) -> None:
    rows = _make_date_rows(macro_attention_zscore=1.5)
    report = coordinator.build_report(rows)
    gbp = next(pa for pa in report.pairs if pa.pair == "GBPUSD")
    assert gbp.position_size_pct == pytest.approx(0.75, abs=1e-4)


def test_risk_reward_is_always_1_67(normal_report: CoordinatorReport) -> None:
    for pa in normal_report.pairs:
        if pa.sl_pct > 0:
            assert abs(pa.risk_reward_ratio - round(pa.tp_pct / pa.sl_pct, 2)) < 1e-4


# ── USDJPY tech-gate ─────────────────────────────────────────────────────────


def test_usdjpy_uses_tech_when_above_p75(coordinator: Coordinator) -> None:
    rows = _make_date_rows(tech_confidence=0.2)  # above p75=0.0887
    report = coordinator.build_report(rows)
    usdjpy = next(pa for pa in report.pairs if pa.pair == "USDJPY")
    assert usdjpy.direction_source == "tech_usdjpy"
    assert usdjpy.confidence_tier == "high"


def test_usdjpy_falls_back_to_macro_below_p75(normal_report: CoordinatorReport) -> None:
    usdjpy = next(pa for pa in normal_report.pairs if pa.pair == "USDJPY")
    assert usdjpy.direction_source == "macro_usdjpy"
    assert usdjpy.confidence_tier == "low"


# ── HOLD gate ────────────────────────────────────────────────────────────────


def test_hold_gate_fires_when_no_macro_direction(coordinator: Coordinator) -> None:
    rows = pd.DataFrame(
        [{**_make_signal_row(p), "macro_direction": None, "tech_confidence": 0.0} for p in PAIRS]
    )
    report = coordinator.build_report(rows)
    assert report.overall_action == "hold"
    assert report.top_pick is None
    assert report.hold_reason is not None


# ── Leakage guard ────────────────────────────────────────────────────────────


def test_fwd_cols_stripped_silently(coordinator: Coordinator) -> None:
    rows = _make_date_rows()
    rows["fwd_ret_1d"] = 0.01
    rows["fwd_vol_3d"] = 0.005
    # Must not raise and report must be identical to one built without fwd_ cols
    report_with = coordinator.build_report(rows)
    report_without = coordinator.build_report(_make_date_rows())
    assert report_with.top_pick == report_without.top_pick
    assert report_with.overall_action == report_without.overall_action


# ── Vol estimation ───────────────────────────────────────────────────────────


def test_vol_source_geo_for_non_usdjpy(normal_report: CoordinatorReport) -> None:
    for pa in normal_report.pairs:
        if pa.pair != "USDJPY":
            assert pa.vol_source == "geo"


def test_estimated_vol_uses_geo_ols(coordinator: Coordinator) -> None:
    rows = _make_date_rows(geo_bilateral_risk=2.0)
    report = coordinator.build_report(rows)
    gbp = next(pa for pa in report.pairs if pa.pair == "GBPUSD")
    expected = max(0.001, _NB04_CAL.geo_intercept + _NB04_CAL.geo_slope * 2.0)
    assert abs(gbp.estimated_vol_3d - expected) < 1e-8


def test_stocktwits_vol_used_for_usdjpy(coordinator: Coordinator) -> None:
    rows = _make_date_rows()
    rows.loc[rows["pair"] == "USDJPY", "usdjpy_stocktwits_vol_signal"] = 0.5
    report = coordinator.build_report(rows)
    usdjpy = next(pa for pa in report.pairs if pa.pair == "USDJPY")
    assert usdjpy.vol_source == "stocktwits"
    expected_norm = -0.5  # inverted
    expected_vol = max(0.001, _NB04_CAL.st_intercept + _NB04_CAL.st_slope * expected_norm)
    assert abs(usdjpy.estimated_vol_3d - expected_vol) < 1e-8


# ── Prompt rendering ─────────────────────────────────────────────────────────


def test_llm_prompt_contains_top_pick(normal_report: CoordinatorReport) -> None:
    prompt = build_llm_prompt(normal_report)
    assert "GBPUSD" in prompt
    assert "LONG" in prompt or "SHORT" in prompt


def test_llm_prompt_hold_path(coordinator: Coordinator) -> None:
    rows = pd.DataFrame(
        [{**_make_signal_row(p), "macro_direction": None, "tech_confidence": 0.0} for p in PAIRS]
    )
    report = coordinator.build_report(rows)
    prompt = build_llm_prompt(report)
    assert "HOLD" in prompt.upper()


# ── build_batch ───────────────────────────────────────────────────────────────


def test_build_batch_returns_one_report_per_date(coordinator: Coordinator) -> None:
    rows1 = _make_date_rows("2024-01-15")
    rows2 = _make_date_rows("2024-01-16")
    all_rows = pd.concat([rows1, rows2], ignore_index=True)
    batch = coordinator.build_batch(all_rows)
    assert len(batch) == 2
    assert pd.Timestamp("2024-01-15") in batch
    assert pd.Timestamp("2024-01-16") in batch


# ── Calibration JSON round-trip ───────────────────────────────────────────────


def test_load_calibration_from_json(tmp_path: Path) -> None:
    data = {
        "usdjpy_conf_p75": 0.088708,
        "geo_slope": 0.002405,
        "geo_intercept": 0.004057,
        "st_slope": 0.004930,
        "st_intercept": 0.006403,
        "baseline_vol": 0.004524,
    }
    p = tmp_path / "cal.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    cal = load_calibration(p)
    assert abs(cal.usdjpy_conf_p75 - 0.088708) < 1e-6
    assert abs(cal.geo_slope - 0.002405) < 1e-6


def test_load_calibration_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_calibration(tmp_path / "nonexistent.json")


# ── JSON safety (NaN sanitization) ──────────────────────────────────────────


def test_narrative_context_is_json_serializable_with_nan_inputs(coordinator: Coordinator) -> None:
    """narrative_context must survive json.dumps(allow_nan=False) even when raw inputs contain NaN."""
    rows = _make_date_rows()
    # Inject NaN into every nullable float column
    for col in ["gdelt_tone_zscore", "macro_attention_zscore", "geo_bilateral_risk"]:
        rows[col] = float("nan")
    report = coordinator.build_report(rows)
    # Must not raise — PostgreSQL JSONB rejects NaN tokens
    serialized = json.dumps(report.narrative_context, allow_nan=False)
    parsed = json.loads(serialized)
    for entry in parsed["all_pairs"]:
        assert entry["macro_attention_z"] is None
        assert entry["gdelt_tone_z"] is None


# ── Integration regression (requires real data files) ────────────────────────

_SIGNALS_PATH = Path("data/processed/coordinator/signals_aligned.parquet")
_OHLCV_DIR = Path("data/processed/ohlcv")


@pytest.mark.integration
def test_nb05_backtest_regression() -> None:
    """Pins the NB05 backtest numbers: 150 trades, Sharpe=1.077, max_dd=-0.13%, PF=1.48."""
    if not _SIGNALS_PATH.exists():
        pytest.skip("signals_aligned.parquet not available")

    from src.agents.coordinator.backtest import Backtester

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

    h1 = {p: _load_h1(p) for p in ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]}
    if any(v is None for v in h1.values()):
        pytest.skip("H1 OHLCV files not available")

    signals_df = pd.read_parquet(_SIGNALS_PATH)
    signals_df["date"] = pd.to_datetime(signals_df["date"])

    coordinator = Coordinator(calibration=_NB04_CAL)
    backtester = Backtester(coordinator=coordinator, bars=h1)
    result = backtester.run(signals_df)

    assert result.n_trades == 150, f"Expected 150 trades, got {result.n_trades}"
    assert abs(result.sharpe - 1.077) < 0.01, f"Sharpe {result.sharpe} != 1.077"
    assert abs(result.max_drawdown - (-0.0013)) < 0.0002, f"Max DD {result.max_drawdown} != -0.0013"
    assert abs(result.profit_factor - 1.48) < 0.02, f"Profit factor {result.profit_factor} != 1.48"

    # Top pick distribution at trade level (NB05: 76 GBPUSD, 74 USDJPY out of 150)
    gbp_frac = result.top_pick_distribution.get("GBPUSD", 0.0)
    usdjpy_frac = result.top_pick_distribution.get("USDJPY", 0.0)
    assert abs(gbp_frac - 76 / 150) < 0.01, f"GBPUSD trade fraction {gbp_frac:.4f} unexpected"
    assert abs(usdjpy_frac - 74 / 150) < 0.01, f"USDJPY trade fraction {usdjpy_frac:.4f} unexpected"

    # Exit reason breakdown: SL=57, TP=51, FLIP=42
    reasons = Counter(t.exit_reason for t in result.closed_trades)
    assert reasons["SL"] == 57, f"SL count {reasons['SL']} != 57"
    assert reasons["TP"] == 51, f"TP count {reasons['TP']} != 51"
    assert reasons["FLIP"] == 42, f"FLIP count {reasons['FLIP']} != 42"
