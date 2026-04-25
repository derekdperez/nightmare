"""Adapters for TaiLOR-style workflow specs used by the coordinator APIs."""

from __future__ import annotations

import re
from typing import Any

_TEMPLATE_TOKEN_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}")


def _normalized_token(value: Any, *, default: str = "") -> str:
    """Normalize arbitrary names to a stable lowercase token."""
    text = str(value or "").strip().lower().replace(" ", "_").replace("-", "_")
    return text or default


def _coerce_bool(value: Any, default: bool = False) -> bool:
    """Convert user payload values to booleans for workflow flags."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _coerce_int(value: Any, default: int = 0) -> int:
    """Convert user payload values to integers with a safe default."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _deep_render_templates(value: Any, context: dict[str, Any]) -> Any:
    """Recursively expand simple {{token}} placeholders in strings."""
    if isinstance(value, str):
        def _replace(match: re.Match[str]) -> str:
            key = str(match.group(1) or "").strip()
            resolved = context.get(key)
            return "" if resolved is None else str(resolved)

        return _TEMPLATE_TOKEN_RE.sub(_replace, value)
    if isinstance(value, list):
        return [_deep_render_templates(item, context) for item in value]
    if isinstance(value, dict):
        return {str(k): _deep_render_templates(v, context) for k, v in value.items()}
    return value


def _tailor_variable_context(payload: dict[str, Any], runtime_input: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build TaiLOR variable context from declared variables and runtime input."""
    context: dict[str, Any] = {}
    for key, value in (runtime_input or {}).items():
        context[str(key)] = value
    variables = payload.get("variables")
    if not isinstance(variables, list):
        return context
    for item in variables:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        raw_value = item.get("value")
        context[name] = _deep_render_templates(raw_value, context)
    return context


def is_tailor_workflow_spec(payload: dict[str, Any]) -> bool:
    """Return True when payload follows TaiLOR's variables/steps shape."""
    if not isinstance(payload, dict):
        return False
    if isinstance(payload.get("steps"), list):
        if "workflow-type" in payload or "variables" in payload:
            return True
        if any(isinstance(item, dict) and "type" in item for item in payload["steps"]):
            return True
    return False


def scheduler_plugins_from_tailor_steps(
    steps: list[dict[str, Any]],
    *,
    default_retry_limit: int = 3,
    template_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Convert TaiLOR steps into scheduler plugin records."""
    name_to_plugin: dict[str, str] = {}
    for item in steps or []:
        if not isinstance(item, dict):
            continue
        step_name = _normalized_token(item.get("name"), default="")
        step_type = _normalized_token(item.get("type"), default="")
        if step_name and step_type:
            name_to_plugin[step_name] = step_type

    plugins: list[dict[str, Any]] = []
    for idx, item in enumerate(steps or [], start=1):
        if not isinstance(item, dict):
            continue
        step_type = _normalized_token(item.get("type"), default="")
        if not step_type:
            continue
        retry_limit = _coerce_int(item.get("retry_limit"), default_retry_limit)
        retry_limit = max(0, retry_limit)
        plugin_name = _normalized_token(item.get("plugin_name") or item.get("name") or step_type, default=step_type)
        parameters = {
            k: v
            for k, v in item.items()
            if k
            not in {
                "type",
                "name",
                "plugin_name",
                "preconditions",
                "prerequisites",
                "inputs",
                "outputs",
                "enabled",
                "retry_failed",
                "retry_limit",
                "max_attempts",
                "resume_mode",
                "depends_on",
                "dependsOn",
            }
        }
        if template_context:
            parameters = _deep_render_templates(parameters, template_context)
        raw_preconditions = item.get("preconditions", item.get("prerequisites"))
        preconditions = dict(raw_preconditions or {}) if isinstance(raw_preconditions, dict) else {}
        depends_on_raw = item.get("depends_on", item.get("dependsOn"))
        depends_on = depends_on_raw if isinstance(depends_on_raw, list) else []
        requires_plugins_all = preconditions.get("requires_plugins_all")
        if not isinstance(requires_plugins_all, list):
            requires_plugins_all = []
        for dep in depends_on:
            dep_name = _normalized_token(dep, default="")
            if not dep_name:
                continue
            dep_plugin = name_to_plugin.get(dep_name) or dep_name
            if dep_plugin not in requires_plugins_all:
                requires_plugins_all.append(dep_plugin)
        if requires_plugins_all:
            preconditions["requires_plugins_all"] = requires_plugins_all

        plugins.append(
            {
                "name": plugin_name,
                "plugin_name": step_type,
                "handler": str(item.get("handler") or step_type),
                "display_name": str(item.get("display_name") or plugin_name.replace("_", " ").title()),
                "description": str(item.get("description") or ""),
                "enabled": _coerce_bool(item.get("enabled"), True),
                "resume_mode": str(item.get("resume_mode") or "exact"),
                "preconditions": preconditions,
                "inputs": item.get("inputs") if isinstance(item.get("inputs"), dict) else {},
                "outputs": item.get("outputs") if isinstance(item.get("outputs"), dict) else {},
                "retry_failed": _coerce_bool(item.get("retry_failed"), True),
                "retry_limit": retry_limit,
                "max_attempts": max(1, _coerce_int(item.get("max_attempts"), retry_limit + 1)),
                "ordinal": _coerce_int(item.get("ordinal"), idx),
                "parameters": parameters,
            }
        )
    return plugins


def normalize_workflow_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize workflow payloads so runtime always has a `plugins` list."""
    if not isinstance(payload, dict):
        return {}
    normalized = dict(payload)
    if isinstance(normalized.get("plugins"), list):
        normalized["plugins"] = [item for item in normalized["plugins"] if isinstance(item, dict)]
        return normalized
    if is_tailor_workflow_spec(normalized):
        default_retry = max(0, _coerce_int(normalized.get("retry_limit"), 3))
        template_context = _tailor_variable_context(normalized, runtime_input=None)
        normalized["plugins"] = scheduler_plugins_from_tailor_steps(
            [item for item in (normalized.get("steps") or []) if isinstance(item, dict)],
            default_retry_limit=default_retry,
            template_context=template_context,
        )
    else:
        normalized["plugins"] = []
    return normalized


def resolve_workflow_runtime_payload(payload: dict[str, Any], runtime_input: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a runtime-ready workflow payload with resolved TaiLOR variables/templates."""
    normalized = normalize_workflow_payload(payload)
    if not is_tailor_workflow_spec(normalized):
        return normalized
    context = _tailor_variable_context(normalized, runtime_input=runtime_input)
    default_retry = max(0, _coerce_int(normalized.get("retry_limit"), 3))
    normalized["plugins"] = scheduler_plugins_from_tailor_steps(
        [item for item in (normalized.get("steps") or []) if isinstance(item, dict)],
        default_retry_limit=default_retry,
        template_context=context,
    )
    ui_schema = dict(normalized.get("ui_schema") or {}) if isinstance(normalized.get("ui_schema"), dict) else {}
    ui_schema["workflow_type"] = str(normalized.get("workflow-type") or normalized.get("workflow_type") or "single_run")
    ui_schema["tailor_runtime_variables"] = context
    normalized["ui_schema"] = ui_schema
    return normalized
