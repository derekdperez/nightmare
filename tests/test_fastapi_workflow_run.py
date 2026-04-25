from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from fastapi.testclient import TestClient

from server_app.fastapi_app import create_app


class _StoreRunOk:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    @contextmanager
    def workflow_stage_task_scope(self, workflow_id: str):
        _ = workflow_id
        yield

    def schedule_stage(self, root_domain: str, plugin_name: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append((str(root_domain), str(plugin_name), str(kwargs.get("workflow_id") or "")))
        return {"ok": True, "scheduled": True, "reason": "inserted_ready", "status": "ready"}

    def workflow_scheduler_domains(self, *, limit: int = 2000) -> dict[str, Any]:
        return {"root_domains": [], "limit": limit, "count": 0}

    def workflow_scheduler_snapshot(self, *, limit: int = 2000) -> dict[str, Any]:
        return {"domains": [], "limit": limit}

    def count_stage_tasks(self, *, workflow_id: str = "", root_domains: list[str] | None = None, plugins: list[str] | None = None) -> int:
        _ = (workflow_id, root_domains, plugins)
        return len(self.calls)

    def record_system_event(self, event_type: str, event_key: str, payload: dict[str, Any]) -> None:
        _ = (event_type, event_key, payload)

    def recent_stage_task_events(self, *, workflow_id: str = "", limit: int = 20) -> list[dict[str, Any]]:
        _ = (workflow_id, limit)
        return []


class _StoreRunMismatch(_StoreRunOk):
    def count_stage_tasks(self, *, workflow_id: str = "", root_domains: list[str] | None = None, plugins: list[str] | None = None) -> int:
        _ = (workflow_id, root_domains, plugins)
        return 0


def test_fastapi_workflow_run_persists_stage_rows() -> None:
    store = _StoreRunOk()
    app = create_app(coordinator_store=store, coordinator_api_token="")
    client = TestClient(app)

    response = client.post(
        "/api/coord/workflow/run",
        json={
            "workflow_id": "run-recon",
            "root_domains": ["example.com"],
            "plugins": ["recon_subdomain_enumeration"],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert int(payload["counts"]["scheduled"]) == 1
    assert int(payload["persisted_stage_task_rows"]) == 1
    assert ("example.com", "recon_subdomain_enumeration", "run-recon") in store.calls


def test_fastapi_workflow_run_fails_when_persistence_check_misses_rows() -> None:
    store = _StoreRunMismatch()
    app = create_app(coordinator_store=store, coordinator_api_token="")
    client = TestClient(app)

    response = client.post(
        "/api/coord/workflow/run",
        json={
            "workflow_id": "run-recon",
            "root_domains": ["example.com"],
            "plugins": ["recon_subdomain_enumeration"],
        },
    )
    assert response.status_code == 500
    payload = response.json()
    detail = payload.get("detail")
    if isinstance(detail, dict):
        assert "no rows were persisted" in str(detail.get("error") or "").lower()
    else:
        assert "no rows were persisted" in str(detail or "").lower()
