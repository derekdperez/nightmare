from __future__ import annotations

from typing import Any, Callable
import json
from pathlib import Path

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


_PLUGIN_FACTORIES: dict[str, Callable[[], CoordinatorPlugin]] = {
    "auth0r": Auth0rPlugin,
    "extractor": ExtractorPlugin,
    "fozzy": FozzyPlugin,
    "recon_subdomain_enumeration": ReconSubdomainEnumerationPlugin,
    "recon_spider_source_tags": ReconSpiderSourceTagsPlugin,
    "recon_spider_script_links": ReconSpiderScriptLinksPlugin,
    "recon_spider_wordlist": ReconSpiderWordlistPlugin,
    "recon_spider_ai": ReconSpiderAiPlugin,
    "recon_extractor_high_value": ReconExtractorHighValuePlugin,
}


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


def _json_type_for(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "string"


def _schema_from_example(value: Any, *, title: str = "") -> dict[str, Any]:
    kind = _json_type_for(value)
    schema: dict[str, Any] = {"type": kind}
    if title:
        schema["title"] = title
    if kind == "object":
        schema["additionalProperties"] = True
        schema["properties"] = {
            str(k): _schema_from_example(v, title=str(k).replace("_", " ").title())
            for k, v in sorted((value or {}).items())
        }
    elif kind == "array":
        first = value[0] if isinstance(value, list) and value else ""
        schema["items"] = _schema_from_example(first) if value else {"type": "string"}
    else:
        schema["default"] = value
    return schema


def _object_schema_from_example(example: dict[str, Any] | None, *, title: str, description: str = "") -> dict[str, Any]:
    example = example if isinstance(example, dict) else {}
    schema = _schema_from_example(example, title=title)
    schema.setdefault("type", "object")
    schema.setdefault("additionalProperties", True)
    schema.setdefault("properties", {})
    if description:
        schema["description"] = description
    return schema


def _load_workflow_plugin_examples() -> dict[str, dict[str, Any]]:
    """Infer plugin authoring contracts from committed workflow JSON files.

    The runtime plugins do not currently expose rich config schemas. The shipped
    workflow files are the best available source of truth for expected parameter,
    input, output, and precondition shapes, so the builder uses them to create
    practical defaults instead of blank JSON objects.
    """
    examples: dict[str, dict[str, Any]] = {}
    root = Path(__file__).resolve().parents[1]
    for path in sorted((root / "workflows").glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for item in data.get("plugins") or []:
            if not isinstance(item, dict):
                continue
            key = str(item.get("plugin_name") or item.get("name") or "").strip().lower()
            if not key or key in examples:
                continue
            examples[key] = item
    return examples


def list_plugin_contracts() -> list[dict[str, object]]:
    """Return built-in plugin contracts for DB bootstrap/UI authoring.

    Contract rows can later be edited in the database without changing the runtime
    registry. The registry remains the source for executable Python classes.
    """
    workflow_examples = _load_workflow_plugin_examples()
    contracts: list[dict[str, object]] = []
    for key, factory in sorted(_PLUGIN_FACTORIES.items()):
        cls = factory
        module = getattr(cls, "__module__", "")
        name = getattr(cls, "__name__", "")
        plugin = factory()
        try:
            stage_contract = plugin.contract()
        except Exception:
            stage_contract = None
        example = workflow_examples.get(key, {})
        input_artifacts = list(getattr(stage_contract, "input_artifacts", ()) or ())
        output_artifacts = list(getattr(stage_contract, "output_artifacts", ()) or ())
        if not input_artifacts:
            input_artifacts = list((example.get("inputs") or {}).get("artifacts_all") or [])
        if not output_artifacts:
            output_artifacts = list((example.get("outputs") or {}).get("artifacts") or [])

        parameters = example.get("parameters") if isinstance(example.get("parameters"), dict) else {}
        preconditions = example.get("preconditions") if isinstance(example.get("preconditions"), dict) else {}
        inputs = example.get("inputs") if isinstance(example.get("inputs"), dict) else {}
        outputs = example.get("outputs") if isinstance(example.get("outputs"), dict) else {"artifacts": output_artifacts}

        config_schema = _object_schema_from_example(
            parameters,
            title=f"{key} configuration",
            description="Configuration parameters used when this workflow step runs.",
        )
        input_schema = _object_schema_from_example(
            {"root_domain": "example.com", **inputs},
            title=f"{key} inputs",
            description="Runtime inputs and prerequisite artifacts expected by this plugin.",
        )
        output_schema = _object_schema_from_example(
            outputs,
            title=f"{key} outputs",
            description="Artifacts and structured outputs this plugin is expected to create.",
        )
        precondition_schema = _object_schema_from_example(
            preconditions,
            title=f"{key} preconditions",
            description="Conditions that must be satisfied before this step becomes ready.",
        )

        contracts.append(
            {
                "plugin_key": key,
                "display_name": str(example.get("display_name") or key.replace("_", " ").title()),
                "description": str(example.get("description") or f"Coordinator plugin implemented by {module}.{name}."),
                "category": key.split("_", 1)[0] if "_" in key else "general",
                "python_module": module,
                "python_class": name,
                "contract_version": "1.0.0",
                "input_schema": input_schema,
                "output_schema": output_schema,
                "config_schema": config_schema,
                "ui_schema": {
                    "preconditions_schema": precondition_schema,
                    "default_preconditions": preconditions,
                    "default_inputs": inputs,
                    "default_outputs": outputs,
                    "schema_source": "workflow_json" if example else "runtime_default",
                },
                "examples": [
                    {
                        "name": str(example.get("name") or key),
                        "config_json": parameters,
                        "preconditions_json": preconditions,
                        "outputs_json": outputs,
                    }
                ] if example else [],
                "tags": [key.split("_", 1)[0]] if "_" in key else [],
                "enabled": bool(example.get("enabled", True)),
                "source_path": (str(module).replace(".", "/") + ".py") if module else "",
                "input_artifacts": input_artifacts,
                "output_artifacts": output_artifacts,
            }
        )
    return contracts

