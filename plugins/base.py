from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


RunResult = tuple[int, str]


@dataclass(frozen=True)
class StageContract:
    plugin_name: str
    input_artifacts: tuple[str, ...] = ()
    output_artifacts: tuple[str, ...] = ()
    checkpoint_schema: dict[str, Any] = field(default_factory=dict)
    resource_class: str = "default"
    access_mode: str = "write"
    concurrency_group: str = ""
    max_parallelism: int = 1


@dataclass(frozen=True)
class PluginExecutionContext:
    coordinator: Any
    worker_id: str
    root_domain: str
    workflow_id: str
    plugin_name: str
    entry: dict[str, Any]


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

    def run(self, context: PluginExecutionContext) -> RunResult:
        raise NotImplementedError
