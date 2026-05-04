"""Router for triggering the inference pipeline."""

from __future__ import annotations

import datetime

from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel

router = APIRouter(prefix="/inference", tags=["inference"])


class InferenceRunRequest(BaseModel):
    date: datetime.date | None = None
    dry_run: bool = False


class InferenceRunResponse(BaseModel):
    status: str
    target_date: str


def _run_pipeline(date: datetime.date | None, dry_run: bool) -> None:
    import pandas as pd

    from src.inference.pipeline import InferencePipeline

    target = pd.Timestamp(date, tz="UTC") if date is not None else None
    InferencePipeline().run(target_date=target, dry_run=dry_run)


@router.post("/run", response_model=InferenceRunResponse, status_code=202)
def trigger_inference(
    body: InferenceRunRequest,
    background_tasks: BackgroundTasks,
) -> InferenceRunResponse:
    """Trigger the inference pipeline as a background task.

    Returns 202 immediately. Poll GET /reports/{date} to detect completion.
    Defaults to yesterday UTC when date is omitted.
    """
    import pandas as pd

    target_date = body.date or (pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=1)).date()

    background_tasks.add_task(_run_pipeline, body.date, body.dry_run)

    return InferenceRunResponse(
        status="started",
        target_date=str(target_date),
    )
