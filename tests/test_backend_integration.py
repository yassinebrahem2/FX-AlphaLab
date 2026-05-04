from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client_and_mocks():
    """Create TestClient with patched startup dependencies and provide mocks."""
    # Avoid importing src.ingestion package (collectors import optional deps like bs4)
    from dataclasses import dataclass

    from src.shared.config.sources import InferenceConfig, SourceConfig, SourcesConfig

    @dataclass
    class CollectionResult:
        source_id: str
        rows_written: int
        backfill_performed: bool
        error: str | None = None

    # Prevent importing heavy router modules (they require DB/sqlalchemy)
    import sys
    import types

    from fastapi import APIRouter

    routers_mod = types.ModuleType("src.backend.routers")
    routers_mod.inference = types.SimpleNamespace(router=APIRouter())
    routers_mod.ohlcv = types.SimpleNamespace(router=APIRouter())
    routers_mod.reports = types.SimpleNamespace(router=APIRouter())
    routers_mod.signals = types.SimpleNamespace(router=APIRouter())
    routers_mod.trades = types.SimpleNamespace(router=APIRouter())
    sys.modules["src.backend.routers"] = routers_mod

    # Provide lightweight dummy scheduler module to avoid importing apscheduler
    scheduler_mod = types.ModuleType("src.backend.scheduler")
    scheduler_mod.SchedulerService = MagicMock()
    sys.modules["src.backend.scheduler"] = scheduler_mod

    # Provide lightweight dummy orchestrator module to avoid importing collectors
    orchestrator_mod = types.ModuleType("src.ingestion.orchestrator")
    orchestrator_mod.CollectionOrchestrator = MagicMock()
    sys.modules["src.ingestion.orchestrator"] = orchestrator_mod

    from src.backend import main as backend_main

    # Build minimal sources config: one enabled (gdelt_events), one disabled (dukascopy_d1)
    sources = {
        "gdelt_events": SourceConfig(
            enabled=True,
            interval_hours=24.0,
            min_silver_days=None,
            fetch_from=None,
            description="GDELT events",
        ),
        "dukascopy_d1": SourceConfig(
            enabled=False,
            interval_hours=24.0,
            min_silver_days=None,
            fetch_from=None,
            description="Dukascopy daily",
        ),
    }

    sources_config = SourcesConfig(
        sources=sources, inference=InferenceConfig(schedule_cron="0 0 * * *", dry_run=True)
    )

    # Orchestrator mock
    orchestrator_mock = MagicMock()
    orchestrator_mock.config = sources_config

    def run_source_side_effect(sid: str):
        if sid == "gdelt_events":
            return CollectionResult(
                source_id="gdelt_events", rows_written=100, backfill_performed=True, error=None
            )
        if sid == "dukascopy_d1":
            return CollectionResult(
                source_id="dukascopy_d1",
                rows_written=0,
                backfill_performed=False,
                error="not yet integrated",
            )
        return CollectionResult(
            source_id=sid, rows_written=0, backfill_performed=False, error=f"Unknown source: {sid}"
        )

    orchestrator_mock.run_source.side_effect = run_source_side_effect

    # Scheduler mock
    scheduler_mock = MagicMock()
    scheduler_mock.start = MagicMock()
    scheduler_mock.shutdown = MagicMock()

    # Patch targets used by src.backend.main
    with (
        patch(
            "src.backend.main.load_sources_config", return_value=sources_config
        ) as load_cfg_patch,
        patch(
            "src.backend.main.CollectionOrchestrator", return_value=orchestrator_mock
        ) as orchestrator_class_patch,
        patch(
            "src.backend.main.SchedulerService", return_value=scheduler_mock
        ) as scheduler_class_patch,
    ):
        # Create TestClient which will execute lifespan (startup/shutdown)
        with TestClient(backend_main.app) as client:
            yield {
                "client": client,
                "orchestrator_mock": orchestrator_mock,
                "scheduler_mock": scheduler_mock,
                "load_cfg_patch": load_cfg_patch,
                "orchestrator_class_patch": orchestrator_class_patch,
                "scheduler_class_patch": scheduler_class_patch,
            }


def test_health_returns_ok(client_and_mocks):
    client = client_and_mocks["client"]
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_trigger_known_source_success(client_and_mocks):
    client = client_and_mocks["client"]
    # dukascopy_d1 is present but returns an error string
    r = client.post("/admin/trigger/dukascopy_d1")
    assert r.status_code == 500
    assert "not yet integrated" in r.json()["detail"]


def test_trigger_success_when_no_error(client_and_mocks):
    client = client_and_mocks["client"]
    r = client.post("/admin/trigger/gdelt_events")
    assert r.status_code == 200
    body = r.json()
    assert body["source_id"] == "gdelt_events"
    assert body["rows_written"] == 100
    assert body["backfill_performed"] is True
    assert body["error"] is None


def test_trigger_unknown_source(client_and_mocks):
    client = client_and_mocks["client"]
    r = client.post("/admin/trigger/nonexistent")
    assert r.status_code == 422
    assert "Unknown source" in r.json()["detail"]


def test_scheduler_started_on_startup(client_and_mocks):
    scheduler_mock = client_and_mocks["scheduler_mock"]
    # start should have been called once during lifespan startup
    assert scheduler_mock.start.call_count == 1


def test_scheduler_shutdown_on_app_teardown():
    # For this test we create a fresh set of patches to capture shutdown call on exit
    from dataclasses import dataclass
    from unittest.mock import patch

    from src.shared.config.sources import InferenceConfig, SourceConfig, SourcesConfig

    @dataclass
    class CollectionResult:
        source_id: str
        rows_written: int
        backfill_performed: bool
        error: str | None = None

    from fastapi.testclient import TestClient

    from src.backend import main as backend_main

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
    orchestrator_mock.run_source.return_value = CollectionResult(
        source_id="gdelt_events", rows_written=0, backfill_performed=False, error=None
    )

    scheduler_mock = MagicMock()
    scheduler_mock.start = MagicMock()
    scheduler_mock.shutdown = MagicMock()

    with (
        patch("src.backend.main.load_sources_config", return_value=sources_config),
        patch("src.backend.main.CollectionOrchestrator", return_value=orchestrator_mock),
        patch("src.backend.main.SchedulerService", return_value=scheduler_mock),
    ):
        with TestClient(backend_main.app):
            pass

    # After exiting TestClient context, shutdown should have been called
    assert scheduler_mock.shutdown.call_count == 1
