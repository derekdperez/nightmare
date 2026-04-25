from __future__ import annotations

from typing import Any

from workflow_app.tailor_adapter import is_tailor_workflow_spec, normalize_workflow_payload


def validate_workflow_definition(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not str(payload.get("workflow_key") or payload.get("name") or "").strip():
        errors.append("workflow_key or name is required")
    normalized = normalize_workflow_payload(payload)
    steps = payload.get("steps")
    plugins = normalized.get("plugins")
    has_steps = isinstance(steps, list) and bool(steps)
    has_plugins = isinstance(plugins, list) and bool(plugins)
    if not has_steps and not has_plugins:
        errors.append("at least one workflow step/plugin is required")
    seen: set[str] = set()
    iterate = plugins if has_plugins else (steps or [])
    for idx, step in enumerate(iterate, start=1):
        if not isinstance(step, dict):
            errors.append(f"step {idx} must be an object")
            continue
        if is_tailor_workflow_spec(payload):
            key = str(step.get("name") or step.get("type") or f"step-{idx}")
            plugin_key = str(step.get("type") or "").strip()
        else:
            key = str(step.get("step_key") or step.get("name") or f"step-{idx}")
            plugin_key = str(step.get("plugin_key") or step.get("plugin_name") or step.get("type") or "").strip()
        if key in seen:
            errors.append(f"duplicate step_key: {key}")
        seen.add(key)
        if not plugin_key:
            errors.append(f"step {idx} is missing plugin_key")
    return errors
