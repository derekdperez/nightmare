"""Pydantic schema for task-attempt execution rows."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class TaskAttemptSchema(BaseModel):
    """Validate one attempt for a workflow task claim/run cycle."""

    model_config = ConfigDict(extra="forbid")

    task_id: str = Field(min_length=1, max_length=200)
    attempt_number: int = Field(ge=1)
    worker_id: str = Field(min_length=1, max_length=200)
    status: Literal["running", "succeeded", "failed", "canceled"]
    started_at_utc: datetime | None = None
    completed_at_utc: datetime | None = None
