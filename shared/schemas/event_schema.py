"""Pydantic schema for immutable coordinator event records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class EventSchema(BaseModel):
    """Validate one append-only event emitted by coordinators, workers, plugins, or UI actions."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = Field(min_length=1, max_length=200)
    aggregate_key: str = Field(min_length=1, max_length=500)
    schema_version: int = Field(default=1, ge=1)
    source: str = Field(default="", max_length=120)
    severity: Literal["debug", "info", "warning", "error", "critical"] = "info"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    message: str = Field(default="", max_length=1000)
    workflow_id: str = ""
    workflow_run_id: str = ""
    task_id: str = ""
    step_id: str = ""
    plugin_id: str = ""
    worker_id: str = ""
    target_id: str = ""
    correlation_id: str = ""
    causation_id: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = Field(default="", max_length=500)
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
