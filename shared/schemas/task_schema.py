"""Pydantic schema for coordinator task queue rows."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TaskSchema(BaseModel):
    """Validate a schedulable or claimable workflow step task."""

    model_config = ConfigDict(extra="forbid")

    workflow_id: str = Field(default="default", min_length=1, max_length=200)
    root_domain: str = Field(min_length=1, max_length=253)
    stage: str = Field(min_length=1, max_length=200)
    status: Literal["created", "pending", "ready", "running", "completed", "failed", "paused", "canceled"] = "pending"
    worker_id: str = ""
    attempt_count: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=4, ge=1)
    lease_expires_at: datetime | None = None
    checkpoint: dict[str, Any] = Field(default_factory=dict)
    progress: dict[str, Any] = Field(default_factory=dict)

    @field_validator("root_domain", "stage")
    @classmethod
    def normalize_tokens(cls, value: str) -> str:
        """Normalize claim keys to avoid duplicate queue rows with different casing."""
        return str(value or "").strip().lower()
