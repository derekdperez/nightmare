from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Literal


RunResult = tuple[int, str]


@dataclass(frozen=True)
class ArtifactRequirement:
    artifact_type: str
    required: bool = True
    logical_role: str = ""
    shard_selector: str = ""


@dataclass(frozen=True)
class ArtifactDeclaration:
    artifact_type: str
    logical_role: str = ""
    retention_class: str = "derived_rebuildable"
    media_type: str = "application/octet-stream"
    manifest: bool = False


@dataclass(frozen=True)
class StageContract:
    plugin_name: str
    input_artifacts: tuple[str, ...] = ()
    output_artifacts: tuple[str, ...] = ()
    input_requirements: tuple[ArtifactRequirement, ...] = ()
    output_declarations: tuple[ArtifactDeclaration, ...] = ()
    checkpoint_schema: dict[str, Any] = field(default_factory=dict)
    resource_class: str = "default"
    access_mode: Literal["read", "write"] = "write"
    concurrency_group: str = ""
    max_parallelism: int = 1
    shard_key_strategy: str = ""
    summary_fields: tuple[str, ...] = (
        "match_count",
        "anomaly_count",
        "request_count",
        "error_count",
        "warning_count",
        "latest_status",
        "started_at",
        "finished_at",
        "duration_ms",
    )


@dataclass(frozen=True)
class PluginExecutionContext:
    coordinator: Any
    worker_id: str
    root_domain: str
    workflow_id: str
    plugin_name: str
    entry: dict[str, Any]


@dataclass(frozen=True)
class PluginRuntimeHooks:
    prepare_inputs: Callable[[PluginExecutionContext], dict[str, Any]] | None = None
    emit_outputs: Callable[[PluginExecutionContext, dict[str, Any]], None] | None = None
    flush_checkpoint: Callable[[PluginExecutionContext, dict[str, Any]], None] | None = None


class CoordinatorPlugin:
    plugin_name = ""

    def contract(self) -> StageContract:
        name = str(self.plugin_name or "").strip().lower()
        return StageContract(
            plugin_name=name,
            resource_class="default",
            access_mode="write",
            concurrency_group=name,
            max_parallelism=1,
        )

    def prepare_inputs(self, context: PluginExecutionContext) -> dict[str, Any]:
        return {}

    def emit_outputs(self, context: PluginExecutionContext, outputs: dict[str, Any]) -> None:
        return None

    def flush_checkpoint(self, context: PluginExecutionContext, checkpoint: dict[str, Any]) -> None:
        context.coordinator.client.update_stage_progress(
            context.root_domain,
            context.plugin_name,
            context.worker_id,
            workflow_id=context.workflow_id,
            checkpoint=checkpoint,
            progress={"last_milestone": "checkpoint"},
        )

    def describe_resources(self) -> dict[str, Any]:
        contract = self.contract()
        return {
            "resource_class": contract.resource_class,
            "access_mode": contract.access_mode,
            "concurrency_group": contract.concurrency_group,
            "max_parallelism": contract.max_parallelism,
            "shard_key_strategy": contract.shard_key_strategy,
        }

    def run(self, context: PluginExecutionContext) -> RunResult:
        raise NotImplementedError
