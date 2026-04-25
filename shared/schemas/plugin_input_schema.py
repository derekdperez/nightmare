"""Pydantic schema for plugin input envelopes."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PluginInputSchema(BaseModel):
    """Validate payloads passed into plugin execution handlers."""

    model_config = ConfigDict(extra="allow")

    workflow_id: str = Field(default="", max_length=200)
    workflow_run_id: str = Field(default="", max_length=200)
    step_id: str = Field(default="", max_length=200)
    task_id: str = Field(default="", max_length=200)
    root_domain: str = Field(default="", max_length=253)
    input: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
