"""Pydantic response schemas for coordinator_reports."""

from __future__ import annotations

import datetime

from pydantic import BaseModel


class CoordinatorReportResponse(BaseModel):
    date: datetime.date
    top_pick: str | None
    overall_action: str
    hold_reason: str | None
    global_regime: str | None
    narrative_context: dict | None

    model_config = {"from_attributes": True}
