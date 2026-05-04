"""FastAPI application entry point for FX-AlphaLab backend.

Usage:
    uvicorn src.backend.main:app --reload --port 8000
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.backend.routers import inference, reports, signals, trades
from src.backend.scheduler import SchedulerService
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

    # Store scheduler in app state for shutdown access
    app.scheduler = scheduler

    yield

    # ── Shutdown ────────────────────────────────────────────────────────────
    logger.info("Shutting down FX-AlphaLab backend")
    if hasattr(app, "scheduler"):
        app.scheduler.shutdown()
        logger.info("Scheduler service shut down")


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


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}
