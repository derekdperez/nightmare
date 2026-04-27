"""System plugin that executes mapped legacy workflow actions."""

from __future__ import annotations

import inspect
from typing import Any

from plugins.base import CoordinatorPlugin, PluginExecutionContext, RunResult
from plugins.system.legacy_actions import LEGACY_ACTIONS


class LegacyStepAdapterPlugin(CoordinatorPlugin):
    """Dispatch legacy workflow step names to system action functions."""

    def __init__(self, action_name: str) -> None:
        self.plugin_name = str(action_name or "").strip().lower()

    def run(self, context: PluginExecutionContext) -> RunResult:
        action_name = str(context.plugin_name or self.plugin_name or "").strip().lower()
        action = LEGACY_ACTIONS.get(action_name)
        if action is None:
            return 1, f"unsupported legacy action: {action_name}"
        params = context.coordinator._workflow_stage_parameters(action_name)
        kwargs = dict(params or {}) if isinstance(params, dict) else {}

        checkpoint = context.entry.get("checkpoint") if isinstance(context.entry, dict) else {}
        checkpoint = checkpoint if isinstance(checkpoint, dict) else {}
        task_config: dict[str, Any] = {}
        for key in ("resolved_config_json", "resolved_config", "parameters", "config_json", "config"):
            value = checkpoint.get(key)
            if isinstance(value, dict):
                task_config.update(value)
        if task_config:
            # Per-run DB/builder config wins over static file-catalog parameters.
            kwargs.update(task_config)

        # Add common runtime context for actions that need task metadata.
        kwargs.setdefault("root_domain", context.root_domain)
        kwargs.setdefault("workflow_id", context.workflow_id)
        kwargs.setdefault("worker_id", context.worker_id)
        kwargs.setdefault("entry", context.entry)

        try:
            signature = inspect.signature(action)
            accepted = set(signature.parameters.keys())
            filtered = {k: v for k, v in kwargs.items() if k in accepted}
            result = action(**filtered)
            output_payload = result if isinstance(result, dict) else {"result": result}
            if isinstance(context.entry, dict):
                context.entry["plugin_output"] = output_payload
            context.coordinator.logger.info(
                "legacy_system_action_executed",
                worker_id=context.worker_id,
                workflow_id=context.workflow_id,
                root_domain=context.root_domain,
                stage=action_name,
                output=output_payload,
            )
            return 0, ""
        except Exception as exc:
            return 1, str(exc)
