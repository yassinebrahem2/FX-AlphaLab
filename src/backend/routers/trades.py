"""Router for trade_log endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.backend.dependencies import get_db
from src.backend.schemas.trades import TradeLogResponse
from src.shared.db.models import TradeLogRow

router = APIRouter(prefix="/trades", tags=["trades"])


@router.get("", response_model=list[TradeLogResponse])
def list_trades(
    backtest_run: str | None = None,
    db: Session = Depends(get_db),
) -> list[TradeLogRow]:
    """List closed trades, optionally filtered by backtest_run label."""
    q = db.query(TradeLogRow).order_by(TradeLogRow.entry_date.desc())
    if backtest_run is not None:
        q = q.filter(TradeLogRow.backtest_run == backtest_run)
    return q.all()
