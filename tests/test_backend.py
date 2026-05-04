"""Unit tests for src.backend routers — schema, 404 paths, inference trigger."""

from __future__ import annotations

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.backend.dependencies import get_db
from src.backend.main import app

_DATE = datetime.date(2025, 1, 10)


# ── DB session mock factory ────────────────────────────────────────────────────


def _make_db_override(first=None, all_rows=None, all_rows_by_model=None):
    """Return a get_db dependency override that yields a mock SQLAlchemy session.

    all_rows_by_model: dict mapping model class → list of rows, for endpoints
    that call db.query() on multiple models (e.g. signals).
    """

    def _override():
        session = MagicMock()

        def _query(model=None, *args, **kwargs):
            q = MagicMock()
            rows = (all_rows_by_model or {}).get(model, all_rows or [])
            q.order_by.return_value = q
            q.filter.return_value = q
            q.first.return_value = first
            q.all.return_value = rows
            return q

        session.query.side_effect = _query
        yield session

    return _override


def _report_row(**kwargs):
    defaults = dict(
        date=_DATE,
        top_pick="GBPUSD",
        overall_action="trade",
        hold_reason=None,
        global_regime="normal",
        narrative_context={"date": "2025-01-10", "overall_action": "trade"},
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _agent_row(pair: str = "EURUSD"):
    return SimpleNamespace(
        date=_DATE,
        pair=pair,
        tech_direction=1,
        tech_confidence=0.2,
        tech_vol_regime="normal",
        geo_bilateral_risk=0.4,
        geo_risk_regime="low",
        macro_direction="up",
        macro_confidence=0.7,
        macro_carry_score=0.1,
        macro_regime_score=0.05,
        macro_fundamental_score=0.2,
        macro_surprise_score=0.0,
        macro_bias_score=0.05,
        macro_dominant_driver="fundamental_mispricing_score",
        usdjpy_stocktwits_vol_signal=None,
        gdelt_tone_zscore=-0.5,
        gdelt_attention_zscore=0.4,
        macro_attention_zscore=0.2,
        composite_stress_flag=False,
    )


def _coord_row(pair: str = "EURUSD"):
    return SimpleNamespace(
        date=_DATE,
        pair=pair,
        vol_signal=0.4,
        vol_source="geo",
        direction=1,
        direction_source="macro_eurusd",
        direction_horizon="5d",
        direction_ic=0.06,
        confidence_tier="low",
        flat_reason=None,
        regime="normal",
        suggested_action="LONG",
        conviction_score=0.018,
        position_size_pct=0.5,
        sl_pct=1.374,
        tp_pct=2.290,
        risk_reward_ratio=1.67,
        estimated_vol_3d=0.0045,
        is_top_pick=False,
        overall_action="trade",
    )


# ── /reports/latest ────────────────────────────────────────────────────────────


def test_reports_latest_returns_200():
    app.dependency_overrides[get_db] = _make_db_override(first=_report_row())
    try:
        with TestClient(app) as client:
            resp = client.get("/reports/latest")
        assert resp.status_code == 200
        body = resp.json()
        assert body["overall_action"] == "trade"
        assert body["top_pick"] == "GBPUSD"
        assert body["date"] == str(_DATE)
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_reports_latest_returns_404_when_no_rows():
    app.dependency_overrides[get_db] = _make_db_override(first=None)
    try:
        with TestClient(app) as client:
            resp = client.get("/reports/latest")
        assert resp.status_code == 404
    finally:
        app.dependency_overrides.pop(get_db, None)


# ── /reports/{date} ────────────────────────────────────────────────────────────


def test_reports_by_date_returns_200():
    app.dependency_overrides[get_db] = _make_db_override(first=_report_row())
    try:
        with TestClient(app) as client:
            resp = client.get(f"/reports/{_DATE}")
        assert resp.status_code == 200
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_reports_by_date_returns_404_for_missing():
    app.dependency_overrides[get_db] = _make_db_override(first=None)
    try:
        with TestClient(app) as client:
            resp = client.get("/reports/2099-01-01")
        assert resp.status_code == 404
        assert "2099-01-01" in resp.json()["detail"]
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_reports_by_date_returns_422_for_malformed():
    with TestClient(app) as client:
        resp = client.get("/reports/not-a-date")
    assert resp.status_code == 422


# ── /signals/{date} ────────────────────────────────────────────────────────────


def test_signals_by_date_returns_correct_counts():
    from src.shared.db.models import AgentSignal, CoordinatorSignalRow

    pairs = ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]
    app.dependency_overrides[get_db] = _make_db_override(
        all_rows_by_model={
            AgentSignal: [_agent_row(p) for p in pairs],
            CoordinatorSignalRow: [_coord_row(p) for p in pairs],
        }
    )
    try:
        with TestClient(app) as client:
            resp = client.get(f"/signals/{_DATE}")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["agent_signals"]) == 4
        assert len(body["coordinator_signals"]) == 4
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_signals_by_date_returns_404_when_empty():
    from src.shared.db.models import AgentSignal, CoordinatorSignalRow

    app.dependency_overrides[get_db] = _make_db_override(
        all_rows_by_model={AgentSignal: [], CoordinatorSignalRow: []}
    )
    try:
        with TestClient(app) as client:
            resp = client.get("/signals/2099-01-01")
        assert resp.status_code == 404
    finally:
        app.dependency_overrides.pop(get_db, None)


# ── /trades ────────────────────────────────────────────────────────────────────


def _trade_row():
    return SimpleNamespace(
        id=1,
        backtest_run="backtest_nb05_v1",
        pair="GBPUSD",
        direction="LONG",
        entry_date=_DATE,
        entry_price=1.2850,
        exit_date=datetime.date(2025, 1, 15),
        exit_price=1.2900,
        exit_reason="TP",
        hold_days=5,
        position_pct=1.0,
        cost_pct=0.0001,
        gross_pnl_pct=0.39,
        net_pnl_pct=0.38,
        equity_delta=0.0038,
    )


def test_trades_returns_list():
    app.dependency_overrides[get_db] = _make_db_override(all_rows=[_trade_row()])
    try:
        with TestClient(app) as client:
            resp = client.get("/trades")
        assert resp.status_code == 200
        assert len(resp.json()) == 1
        assert resp.json()[0]["pair"] == "GBPUSD"
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_trades_returns_empty_list():
    app.dependency_overrides[get_db] = _make_db_override(all_rows=[])
    try:
        with TestClient(app) as client:
            resp = client.get("/trades")
        assert resp.status_code == 200
        assert resp.json() == []
    finally:
        app.dependency_overrides.pop(get_db, None)


# ── POST /inference/run ────────────────────────────────────────────────────────


def test_inference_run_returns_202():
    with patch("src.backend.routers.inference._run_pipeline") as mock_run:
        with TestClient(app) as client:
            resp = client.post("/inference/run", json={"date": "2025-01-10", "dry_run": True})
        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "started"
        assert body["target_date"] == "2025-01-10"
        mock_run.assert_called_once_with(datetime.date(2025, 1, 10), True)


def test_inference_run_defaults_to_yesterday_when_no_date():
    with patch("src.backend.routers.inference._run_pipeline"):
        with TestClient(app) as client:
            resp = client.post("/inference/run", json={})
        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "started"
        assert body["target_date"] != ""
