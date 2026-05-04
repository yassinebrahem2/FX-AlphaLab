from __future__ import annotations

import importlib
import sys
import types
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.shared.config import Config


@pytest.fixture
def client_and_mocks():
    """Create TestClient with patched startup dependencies."""
    from fastapi import APIRouter

    from src.shared.config.sources import InferenceConfig, SourceConfig, SourcesConfig

    sys.modules.pop("src.backend.routers", None)
    sys.modules.pop("src.backend.routers.ohlcv", None)
    real_ohlcv = importlib.import_module("src.backend.routers.ohlcv")

    sys.modules.pop("src.backend.main", None)
    routers_mod = types.ModuleType("src.backend.routers")
    routers_mod.inference = types.SimpleNamespace(router=APIRouter())
    routers_mod.reports = types.SimpleNamespace(router=APIRouter())
    routers_mod.signals = types.SimpleNamespace(router=APIRouter())
    routers_mod.trades = types.SimpleNamespace(router=APIRouter())
    routers_mod.ohlcv = real_ohlcv
    sys.modules["src.backend.routers"] = routers_mod

    backend_main = importlib.import_module("src.backend.main")

    sources = {
        "gdelt_events": SourceConfig(
            enabled=True,
            interval_hours=24.0,
            min_silver_days=None,
            fetch_from=None,
            description="GDELT events",
        )
    }
    sources_config = SourcesConfig(
        sources=sources, inference=InferenceConfig(schedule_cron="0 0 * * *", dry_run=True)
    )

    orchestrator_mock = MagicMock()
    orchestrator_mock.config = sources_config

    scheduler_mock = MagicMock()
    scheduler_mock.start = MagicMock()
    scheduler_mock.shutdown = MagicMock()

    with ExitStack() as stack:
        stack.enter_context(
            patch("src.backend.main.load_sources_config", return_value=sources_config)
        )
        stack.enter_context(
            patch("src.backend.main.CollectionOrchestrator", return_value=orchestrator_mock)
        )
        stack.enter_context(patch("src.backend.main.SchedulerService", return_value=scheduler_mock))
        client = stack.enter_context(TestClient(backend_main.app))
        yield {
            "client": client,
            "orchestrator_mock": orchestrator_mock,
            "scheduler_mock": scheduler_mock,
        }


def _fixed_datetime(value: datetime):
    class FixedDateTime(datetime):
        @classmethod
        def utcnow(cls):
            return value

    return FixedDateTime


def _make_1min_frame(start: datetime, periods: int = 60) -> pd.DataFrame:
    index = pd.date_range(start, periods=periods, freq="min", tz="UTC")
    prices = pd.Series(range(periods), dtype="float64") / 1000 + 1.1
    return pd.DataFrame(
        {
            "timestamp_utc": index,
            "open": prices,
            "high": prices + 0.0002,
            "low": prices - 0.0002,
            "close": prices,
            "volume": pd.Series([1.0] * periods, dtype="float64"),
        }
    )


def _bronze_file(base_dir: Path, instrument: str, yyyymmdd: str) -> Path:
    path = base_dir / instrument / yyyymmdd[:4] / yyyymmdd[4:6] / f"{yyyymmdd}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    return path


def test_ohlcv_returns_bars(client_and_mocks, tmp_path: Path):
    client = client_and_mocks["client"]
    with patch.object(Config, "DATA_DIR", tmp_path):
        bronze_root = tmp_path / "raw" / "dukascopy"
        _bronze_file(bronze_root, "EURUSD", "20260501")
        fixed_now = datetime(2026, 5, 4, 12, 0, 0)

        with (
            patch("src.backend.routers.ohlcv.datetime", _fixed_datetime(fixed_now)),
            patch(
                "src.backend.routers.ohlcv.pd.read_parquet",
                return_value=_make_1min_frame(fixed_now),
            ),
        ):
            resp = client.get("/ohlcv/EURUSD?tf=H1&days=30")

    assert resp.status_code == 200
    body = resp.json()
    assert body
    assert pd.Timestamp(body[0]["timestamp_utc"]).tz is not None
    assert set(body[0]) == {"timestamp_utc", "open", "high", "low", "close", "volume"}


def test_ohlcv_invalid_instrument(client_and_mocks):
    client = client_and_mocks["client"]
    resp = client.get("/ohlcv/AUDUSD?tf=H1&days=30")
    assert resp.status_code == 422
    assert "Invalid instrument" in resp.json()["detail"]


def test_ohlcv_invalid_tf(client_and_mocks):
    client = client_and_mocks["client"]
    resp = client.get("/ohlcv/EURUSD?tf=M5&days=30")
    assert resp.status_code == 422
    assert "Invalid timeframe" in resp.json()["detail"]


def test_ohlcv_no_bronze_dir(client_and_mocks, tmp_path: Path):
    client = client_and_mocks["client"]
    with patch.object(Config, "DATA_DIR", tmp_path):
        resp = client.get("/ohlcv/EURUSD?tf=H1&days=30")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "No Bronze data for EURUSD"


def test_ohlcv_empty_data_in_range(client_and_mocks, tmp_path: Path):
    client = client_and_mocks["client"]
    with patch.object(Config, "DATA_DIR", tmp_path):
        bronze_root = tmp_path / "raw" / "dukascopy"
        _bronze_file(bronze_root, "EURUSD", "20260301")
        fixed_now = datetime(2026, 5, 4, 12, 0, 0)

        with patch("src.backend.routers.ohlcv.datetime", _fixed_datetime(fixed_now)):
            resp = client.get("/ohlcv/EURUSD?tf=H1&days=30")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "No data for EURUSD in last 30 days"


def test_ohlcv_m15_days_capped(client_and_mocks, tmp_path: Path):
    client = client_and_mocks["client"]
    with patch.object(Config, "DATA_DIR", tmp_path):
        bronze_root = tmp_path / "raw" / "dukascopy"
        recent = _bronze_file(bronze_root, "EURUSD", "20260205")
        stale = _bronze_file(bronze_root, "EURUSD", "20260101")

        fixed_now = datetime(2026, 5, 4, 12, 0, 0)
        read_calls: list[Path] = []

        def _read_parquet(path: Path):
            read_calls.append(path)
            return _make_1min_frame(fixed_now)

        with (
            patch("src.backend.routers.ohlcv.datetime", _fixed_datetime(fixed_now)),
            patch("src.backend.routers.ohlcv.pd.read_parquet", side_effect=_read_parquet),
        ):
            resp = client.get("/ohlcv/EURUSD?tf=M15&days=200")

    assert resp.status_code == 200
    assert recent in read_calls
    assert stale not in read_calls


def test_ohlcv_default_params(client_and_mocks, tmp_path: Path):
    client = client_and_mocks["client"]
    with patch.object(Config, "DATA_DIR", tmp_path):
        bronze_root = tmp_path / "raw" / "dukascopy"
        within_window = _bronze_file(bronze_root, "EURUSD", "20260420")
        outside_window = _bronze_file(bronze_root, "EURUSD", "20260301")

        fixed_now = datetime(2026, 5, 4, 12, 0, 0)
        seen_paths: list[Path] = []

        def _read_parquet(path: Path):
            seen_paths.append(path)
            return _make_1min_frame(fixed_now)

        with (
            patch("src.backend.routers.ohlcv.datetime", _fixed_datetime(fixed_now)),
            patch("src.backend.routers.ohlcv.pd.read_parquet", side_effect=_read_parquet),
        ):
            resp = client.get("/ohlcv/EURUSD")

    assert resp.status_code == 200
    assert within_window in seen_paths
    assert outside_window not in seen_paths
