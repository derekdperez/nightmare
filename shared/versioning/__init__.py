from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

Migrator = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass
class SchemaRegistry:
    _current: dict[str, int] = field(default_factory=dict)
    _migrators: dict[tuple[str, int], Migrator] = field(default_factory=dict)

    def register(self, schema_name: str, version: int, migrator: Migrator | None = None) -> None:
        key = str(schema_name).strip()
        self._current[key] = max(int(version), int(self._current.get(key, 0)))
        if migrator is not None:
            self._migrators[(key, int(version))] = migrator

    def current_version(self, schema_name: str) -> int:
        return int(self._current.get(str(schema_name).strip(), 1) or 1)

    def migrate(self, schema_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        key = str(schema_name).strip()
        out = dict(payload or {})
        version = int(out.get("schema_version") or 1)
        current = self.current_version(key)
        if version > current:
            raise ValueError(f"Unsupported future schema version {version} for {key}")
        while version < current:
            next_version = version + 1
            migrator = self._migrators.get((key, next_version))
            if migrator is None:
                out["schema_version"] = next_version
            else:
                out = dict(migrator(out) or out)
                out["schema_version"] = next_version
            version = next_version
        return out


registry = SchemaRegistry()
for _name in (
    "artifact_envelope",
    "artifact_metadata",
    "summary_envelope",
    "event_record",
    "cache_record",
    "suppression_rule",
    "triage_record",
    "risk_score",
):
    registry.register(_name, 1)
