from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class ArtifactMetadata:
    artifact_id: str
    artifact_type: str
    root_domain: str
    sha256: str
    size_bytes: int
    storage_backend: str
    storage_uri: str
    schema_version: int = 1
    media_type: str = "application/octet-stream"
    encoding: str = "binary"
    compression: str = "identity"
    run_id: str = ""
    stage_id: str = ""
    worker_id: str = ""
    created_at: str = field(default_factory=_iso_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SummaryEnvelope:
    stage: str
    status: str
    schema_version: int = 1
    run_id: str = ""
    session_id: str = ""
    root_domain: str = ""
    started_at: str = ""
    completed_at: str = ""
    duration_seconds: float = 0.0
    counts: dict[str, int] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    input_signatures: dict[str, str] = field(default_factory=dict)
    output_artifacts: dict[str, str] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EventRecord:
    event_type: str
    aggregate_key: str
    schema_version: int = 1
    event_id: str = ""
    created_at: str = field(default_factory=_iso_now)
    idempotency_key: str = ""
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SuppressionRule:
    suppression_id: str
    scope_type: str
    matcher: dict[str, Any]
    reason_type: str
    rationale: str = ""
    expires_at: str | None = None
    created_at: str = field(default_factory=_iso_now)
    created_by: str = ""
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class TriageRecord:
    finding_fingerprint: str
    status: str
    schema_version: int = 1
    owner: str = ""
    severity: str = ""
    note: str = ""
    reviewed_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RiskScorecard:
    root_domain: str
    score: float
    schema_version: int = 1
    contributing_factors: dict[str, float] = field(default_factory=dict)
    calculated_at: str = field(default_factory=_iso_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
