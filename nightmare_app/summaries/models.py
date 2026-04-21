from __future__ import annotations

from typing import Any

from shared.models import SummaryEnvelope
from shared.versioning import registry


def build_stage_summary(*, stage: str, status: str, root_domain: str = "", run_id: str = "", duration_seconds: float = 0.0, counts: dict[str, int] | None = None, metrics: dict[str, float] | None = None, input_signatures: dict[str, str] | None = None, output_artifacts: dict[str, str] | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    summary = SummaryEnvelope(
        stage=str(stage or ""),
        status=str(status or ""),
        schema_version=registry.current_version("summary_envelope"),
        root_domain=str(root_domain or ""),
        run_id=str(run_id or ""),
        duration_seconds=float(duration_seconds or 0.0),
        counts=dict(counts or {}),
        metrics=dict(metrics or {}),
        input_signatures=dict(input_signatures or {}),
        output_artifacts=dict(output_artifacts or {}),
        extra=dict(extra or {}),
    )
    return summary.to_dict()
