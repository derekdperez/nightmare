#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shared.schemas import PluginManifestSchema


@dataclass(frozen=True)
class PluginContract:
    name: str
    display_name: str
    description: str
    category: str
    contract_version: str
    handler: str
    config_schema: dict[str, Any]
    manifest_path: Path


class WorkflowPluginRegistry:
    def __init__(self, plugins_dir: Path) -> None:
        self.plugins_dir = plugins_dir

    def list(self) -> list[PluginContract]:
        if not self.plugins_dir.exists():
            return []
        plugins: list[PluginContract] = []
        for path in sorted(self.plugins_dir.glob("*.json")):
            raw = json.loads(path.read_text(encoding="utf-8"))
            manifest = PluginManifestSchema.model_validate(raw)
            plugins.append(
                PluginContract(
                    name=str(manifest.name or "").strip(),
                    display_name=str(manifest.display_name or manifest.name or "").strip(),
                    description=str(manifest.description or "").strip(),
                    category=str(manifest.category or "general").strip(),
                    contract_version=str(manifest.contract_version or "1.0.0").strip(),
                    handler=str(manifest.handler or "legacy_step_adapter").strip(),
                    config_schema=dict(manifest.config_schema or {}),
                    manifest_path=path,
                )
            )
        return plugins

    def map_by_name(self) -> dict[str, PluginContract]:
        return {plugin.name: plugin for plugin in self.list() if plugin.name}

    def create(self, *, name: str, display_name: str, description: str, category: str, handler: str) -> Path:
        self.plugins_dir.mkdir(parents=True, exist_ok=True)
        path = self.plugins_dir / f"{name}.json"
        payload = {
            "schema_version": "1.0.0",
            "name": name,
            "display_name": display_name,
            "description": description,
            "category": category,
            "contract_version": "1.0.0",
            "handler": handler,
            "config_schema": {
                "type": "object",
                "additionalProperties": True,
            },
        }
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return path


class WorkflowComposer:
    @staticmethod
    def compose(*, workflow_type: str, plugin_names: list[str]) -> dict[str, Any]:
        return {
            "workflow-type": workflow_type,
            "variables": [],
            "steps": [{"type": name, "name": f"{name}-{idx}"} for idx, name in enumerate(plugin_names, start=1)],
        }
