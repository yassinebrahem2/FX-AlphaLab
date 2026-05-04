"""Router for coordinator_reports endpoints."""

from __future__ import annotations

import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.backend.dependencies import get_db
from src.backend.schemas.reports import CoordinatorReportResponse
from src.shared.db.models import CoordinatorReportRow

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("/latest", response_model=CoordinatorReportResponse)
def get_latest_report(db: Session = Depends(get_db)) -> CoordinatorReportRow:
    """Return the most recent CoordinatorReport by date."""
    row = db.query(CoordinatorReportRow).order_by(CoordinatorReportRow.date.desc()).first()
    if row is None:
        raise HTTPException(status_code=404, detail="No reports found")
    return row


@router.get("/{date}", response_model=CoordinatorReportResponse)
def get_report_by_date(
    date: datetime.date,
    db: Session = Depends(get_db),
) -> CoordinatorReportRow:
    """Return the CoordinatorReport for a specific date (YYYY-MM-DD)."""
    row = db.query(CoordinatorReportRow).filter(CoordinatorReportRow.date == date).first()
    if row is None:
        raise HTTPException(status_code=404, detail=f"No report for {date}")
    return row
