"""Pydantic schema for plugin manifest contracts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PluginManifestSchema(BaseModel):
    """Validate plugin manifest metadata loaded by workflow/plugin registries."""

    model_config = ConfigDict(extra="allow")

    name: str = Field(min_length=1, max_length=200)
    display_name: str = Field(default="", max_length=300)
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="general", max_length=120)
    contract_version: str = Field(default="1.0.0", max_length=50)
    handler: str = Field(min_length=1, max_length=200)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    input_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)
