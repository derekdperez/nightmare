"""Pydantic schema for immutable workflow run records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class WorkflowRunSchema(BaseModel):
    """Validate workflow-run creation payloads and stored run rows."""

    model_config = ConfigDict(extra="forbid")

    workflow_run_id: str = Field(min_length=1, max_length=200)
    workflow_id: str = Field(min_length=1, max_length=200)
    root_domain: str = Field(min_length=1, max_length=253)
    status: Literal["queued", "running", "completed", "failed", "canceled"] = "queued"
    trigger_source: str = Field(default="manual", max_length=80)
    input_json: dict[str, Any] = Field(default_factory=dict)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
