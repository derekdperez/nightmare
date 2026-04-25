"""Pydantic schema for plugin output envelopes."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class PluginOutputSchema(BaseModel):
    """Validate plugin execution outputs before persistence/event emission."""

    model_config = ConfigDict(extra="allow")

    status: Literal["completed", "failed", "canceled"]
    exit_code: int = 0
    error: str = ""
    checkpoint: dict[str, Any] = Field(default_factory=dict)
    progress: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[dict[str, Any] | str] = Field(default_factory=list)
