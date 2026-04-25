from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from app_platform.server.fastapi_app import create_app


class _StoreReadinessOk:
    def readiness_probe(self) -> dict[str, Any]:
        return {
            "ok": True,
            "ready": True,
            "generated_at_utc": "2026-01-01T00:00:00Z",
            "database": {"reachable": True, "ping_ms": 1.0, "error": ""},
            "schema": {"columns": {}, "indexes": {}, "tables": {}},
            "issues": [],
        }


class _StoreReadinessNotReady:
    def readiness_probe(self) -> dict[str, Any]:
        return {
            "ok": False,
            "ready": False,
            "generated_at_utc": "2026-01-01T00:00:00Z",
            "database": {"reachable": True, "ping_ms": 2.0, "error": ""},
            "schema": {"columns": {}, "indexes": {}, "tables": {}},
            "issues": ["missing column public.coordinator_artifacts.workflow_run_id"],
        }


def test_readiness_returns_200_when_probe_ready() -> None:
    app = create_app(coordinator_store=_StoreReadinessOk(), coordinator_api_token="")
    client = TestClient(app)
    response = client.get("/api/coord/readiness")
    assert response.status_code == 200
    body = response.json()
    assert body.get("ready") is True
    assert body.get("database", {}).get("reachable") is True


def test_readiness_returns_503_when_probe_not_ready() -> None:
    app = create_app(coordinator_store=_StoreReadinessNotReady(), coordinator_api_token="")
    client = TestClient(app)
    response = client.get("/api/coord/readiness")
    assert response.status_code == 503
    body = response.json()
    assert body.get("ready") is False
    assert body.get("issues")
