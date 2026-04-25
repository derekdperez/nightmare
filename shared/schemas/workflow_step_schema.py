"""Pydantic schema for a single workflow step definition."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class WorkflowStepSchema(BaseModel):
    """Validate one coordinator-driven workflow step before it is persisted or scheduled."""

    model_config = ConfigDict(extra="forbid")

    step_key: str = Field(min_length=1, max_length=200)
    display_name: str = Field(default="", max_length=300)
    plugin_key: str = Field(min_length=1, max_length=200)
    ordinal: int = Field(ge=1)
    enabled: bool = True
    continue_on_error: bool = False
    retry_failed: bool = True
    retry_limit: int = Field(default=3, ge=0)
    max_attempts: int | None = Field(default=None, ge=1)
    timeout_seconds: int = Field(default=0, ge=0)
    input_bindings: dict[str, Any] = Field(default_factory=dict)
    config_json: dict[str, Any] = Field(default_factory=dict)
    preconditions_json: dict[str, Any] = Field(default_factory=dict)
    outputs_json: dict[str, Any] = Field(default_factory=dict)

    @field_validator("step_key", "plugin_key")
    @classmethod
    def normalize_keys(cls, value: str) -> str:
        """Normalize step/plugin keys to the coordinator's lower-case token format."""
        return str(value or "").strip().lower().replace("-", "_")
