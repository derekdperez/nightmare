from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from plugins.auth0r_plugin import Auth0rPlugin
from plugins.base import CoordinatorPlugin
from plugins.extractor_plugin import ExtractorPlugin
from plugins.fozzy_plugin import FozzyPlugin
from plugins.nightmare_artifact_gate_plugin import NightmareArtifactGatePlugin
from plugins.recon.extractor_high_value_plugin import ReconExtractorHighValuePlugin
from plugins.recon.spider.ai_plugin import ReconSpiderAiPlugin
from plugins.recon.spider.script_links_plugin import ReconSpiderScriptLinksPlugin
from plugins.recon.spider.source_tags_plugin import ReconSpiderSourceTagsPlugin
from plugins.recon.spider.wordlist_plugin import ReconSpiderWordlistPlugin
from plugins.recon.subdomain_enumeration_plugin import ReconSubdomainEnumerationPlugin
from plugins.recon.subdomain_takeover_plugin import ReconSubdomainTakeoverPlugin


_PLUGIN_FACTORIES: dict[str, Callable[[], CoordinatorPlugin]] = {
    "auth0r": Auth0rPlugin,
    "extractor": ExtractorPlugin,
    "fozzy": FozzyPlugin,
    "recon_subdomain_enumeration": ReconSubdomainEnumerationPlugin,
    "recon_subdomain_takeover": ReconSubdomainTakeoverPlugin,
    "recon_spider_source_tags": ReconSpiderSourceTagsPlugin,
    "recon_spider_script_links": ReconSpiderScriptLinksPlugin,
    "recon_spider_wordlist": ReconSpiderWordlistPlugin,
    "recon_spider_ai": ReconSpiderAiPlugin,
    "recon_extractor_high_value": ReconExtractorHighValuePlugin,
}


def _json_schema_for_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"type": "boolean", "default": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"type": "integer", "default": value}
    if isinstance(value, float):
        return {"type": "number", "default": value}
    if isinstance(value, list):
        item_schema = _json_schema_for_value(value[0]) if value else {}
        return {"type": "array", "items": item_schema, "default": value}
    if isinstance(value, dict):
        return {
            "type": "object",
            "additionalProperties": True,
            "default": value,
            "properties": {str(k): _json_schema_for_value(v) for k, v in value.items()},
        }
    return {"type": "string", "default": "" if value is None else str(value)}


def _builtin_workflow_parameter_schemas() -> dict[str, dict[str, Any]]:
    """Infer plugin config schemas from shipped workflow files.

    This keeps the workflow builder useful even when a plugin class only exposes a
    generic runtime contract.
    """
    schemas: dict[str, dict[str, Any]] = {}
    workflow_dir = Path(__file__).resolve().parents[1] / "workflows"
    for path in workflow_dir.glob("*.workflow.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        for item in payload.get("plugins") or []:
            if not isinstance(item, dict):
                continue
            key = str(item.get("plugin_name") or item.get("handler") or item.get("name") or "").strip().lower()
            params = item.get("parameters") if isinstance(item.get("parameters"), dict) else {}
            if not key or not isinstance(params, dict):
                continue
            schema = schemas.setdefault(key, {"type": "object", "additionalProperties": True, "properties": {}})
            props = schema.setdefault("properties", {})
            for name, value in params.items():
                props.setdefault(str(name), _json_schema_for_value(value))
    return schemas


def resolve_plugin(plugin_name: str) -> CoordinatorPlugin | None:
    normalized = str(plugin_name or "").strip().lower()
    if not normalized:
        return None
    if normalized.startswith("nightmare_"):
        return NightmareArtifactGatePlugin()
    factory = _PLUGIN_FACTORIES.get(normalized)
    if not factory:
        return None
    return factory()


def list_registered_plugins() -> list[str]:
    """Return plugin keys available to coordinator workers."""
    return sorted(_PLUGIN_FACTORIES.keys())


def list_plugin_contracts() -> list[dict[str, object]]:
    """Return lightweight built-in plugin contracts for DB bootstrap/UI authoring.

    Contract rows can later be edited in the database without changing the runtime
    registry. The registry remains the source for executable Python classes.
    """
    contracts: list[dict[str, object]] = []
    inferred_config_schemas = _builtin_workflow_parameter_schemas()
    for key, factory in sorted(_PLUGIN_FACTORIES.items()):
        cls = factory
        module = getattr(cls, "__module__", "")
        name = getattr(cls, "__name__", "")
        plugin = factory()
        try:
            stage_contract = plugin.contract()
        except Exception:
            stage_contract = None
        contracts.append(
            {
                "plugin_key": key,
                "display_name": key.replace("_", " ").title(),
                "description": f"Coordinator plugin implemented by {module}.{name}.",
                "category": key.split("_", 1)[0] if "_" in key else "general",
                "python_module": module,
                "python_class": name,
                "contract_version": "1.0.0",
                "input_schema": {
                    "type": "object",
                    "additionalProperties": True,
                    "properties": {
                        "root_domain": {"type": "string"},
                    },
                },
                "output_schema": {
                    "type": "object",
                    "additionalProperties": True,
                    "properties": {
                        "artifacts": {"type": "array", "items": {"type": "string"}},
                    },
                },
                "config_schema": inferred_config_schemas.get(key) or {
                    "type": "object",
                    "additionalProperties": True,
                },
                "ui_schema": {},
                "examples": [],
                "tags": [key.split("_", 1)[0]] if "_" in key else [],
                "enabled": True,
                "source_path": (str(module).replace(".", "/") + ".py") if module else "",
                "input_artifacts": list(getattr(stage_contract, "input_artifacts", ()) or ()),
                "output_artifacts": list(getattr(stage_contract, "output_artifacts", ()) or ()),
            }
        )
    return contracts
