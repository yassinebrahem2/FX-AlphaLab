"""Router for agent_signals and coordinator_signals endpoints."""

from __future__ import annotations

import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.backend.dependencies import get_db
from src.backend.schemas.signals import (
    AgentSignalResponse,
    CoordinatorSignalResponse,
    DateSignalsResponse,
)
from src.shared.db.models import AgentSignal, CoordinatorSignalRow

router = APIRouter(prefix="/signals", tags=["signals"])


@router.get("/{date}", response_model=DateSignalsResponse)
def get_signals_by_date(
    date: datetime.date,
    db: Session = Depends(get_db),
) -> DateSignalsResponse:
    """Return agent_signals and coordinator_signals for all pairs on a given date."""
    agent_rows = db.query(AgentSignal).filter(AgentSignal.date == date).all()
    coord_rows = db.query(CoordinatorSignalRow).filter(CoordinatorSignalRow.date == date).all()
    if not agent_rows and not coord_rows:
        raise HTTPException(status_code=404, detail=f"No signals for {date}")
    return DateSignalsResponse(
        date=date,
        agent_signals=[AgentSignalResponse.model_validate(r) for r in agent_rows],
        coordinator_signals=[CoordinatorSignalResponse.model_validate(r) for r in coord_rows],
    )
