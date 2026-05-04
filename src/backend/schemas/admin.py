"""Admin response schemas."""

from __future__ import annotations

from pydantic import BaseModel


class TriggerResult(BaseModel):
    source_id: str
    rows_written: int
    backfill_performed: bool
    error: str | None = None

    model_config = {"from_attributes": True}
