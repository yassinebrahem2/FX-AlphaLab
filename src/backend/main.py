"""FastAPI application entry point for FX-AlphaLab backend.

Usage:
    uvicorn src.backend.main:app --reload --port 8000
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from src.backend.routers import inference, ohlcv, reports, signals, trades
from src.backend.scheduler import SchedulerService
from src.backend.schemas.admin import TriggerResult
from src.ingestion.orchestrator import CollectionOrchestrator
from src.shared.config import Config
from src.shared.config.sources import load_sources_config

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown hooks.

    Startup:
    - Load sources configuration from config/sources.yaml
    - Instantiate CollectionOrchestrator
    - Create and start SchedulerService

    Shutdown:
    - Gracefully shut down the scheduler
    """
    # ── Startup ─────────────────────────────────────────────────────────────
    logger.info("Starting FX-AlphaLab backend")

    # Load sources configuration
    config_path = Config.ROOT_DIR / "config" / "sources.yaml"
    try:
        sources_config = load_sources_config(config_path)
        logger.info(f"Loaded sources config from {config_path}")
    except Exception as e:
        logger.error(f"Failed to load sources config: {e}")
        raise

    # Create collection orchestrator
    try:
        orchestrator = CollectionOrchestrator(config=sources_config, root=Config.ROOT_DIR)
        logger.info("Instantiated CollectionOrchestrator")
    except Exception as e:
        logger.error(f"Failed to instantiate CollectionOrchestrator: {e}")
        raise

    # Create and start scheduler
    try:
        scheduler = SchedulerService(config=sources_config, orchestrator=orchestrator)
        scheduler.start()
        logger.info("Scheduler service started")
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        raise

    # Store services on app.state for tests and routes to access
    app.state.orchestrator = orchestrator
    app.state.scheduler = scheduler

    yield

    # ── Shutdown ────────────────────────────────────────────────────────────
    logger.info("Shutting down FX-AlphaLab backend")
    if hasattr(app.state, "scheduler") and app.state.scheduler is not None:
        try:
            app.state.scheduler.shutdown()
            logger.info("Scheduler service shut down")
        except Exception:
            logger.exception("Error while shutting down scheduler")
    # Close orchestrator if present
    if hasattr(app.state, "orchestrator") and app.state.orchestrator is not None:
        try:
            if hasattr(app.state.orchestrator, "close"):
                app.state.orchestrator.close()
                logger.info("Orchestrator closed")
        except Exception:
            logger.exception("Error while closing orchestrator")


app = FastAPI(
    title="FX-AlphaLab API",
    description="Multi-agent FX signal pipeline — coordinator reports, signals, and trades.",
    version="0.1.0",
    lifespan=lifespan,
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(reports.router)
app.include_router(signals.router)
app.include_router(trades.router)
app.include_router(inference.router)
app.include_router(ohlcv.router)


@app.post("/admin/trigger/{source_id}", tags=["admin"], response_model=TriggerResult)
def admin_trigger(source_id: str):
    """Admin endpoint to trigger a collection for a given source_id.

    The orchestrator instance is stored on `app.state.orchestrator`.
    """
    if not hasattr(app.state, "orchestrator") or app.state.orchestrator is None:
        raise HTTPException(status_code=500, detail="Orchestrator not available")

    orchestrator = app.state.orchestrator

    # Run the source and obtain a CollectionResult
    result = orchestrator.run_source(source_id)

    # If the source is unknown according to configured sources, return 422
    if source_id not in getattr(orchestrator, "config", {}).sources:
        # pass through orchestrator error when available
        raise HTTPException(status_code=422, detail=result.error or "Unknown source")

    # If orchestrator returned an error, return 500
    if result.error is not None:
        raise HTTPException(status_code=500, detail=result.error)

    return TriggerResult(
        source_id=result.source_id,
        rows_written=result.rows_written,
        backfill_performed=result.backfill_performed,
        error=result.error,
    )


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}
