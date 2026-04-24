from __future__ import annotations

import pytest

from workflow_app import store as workflow_store


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        _ = (sql, params)


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor()

    def commit(self):
        return None


class _FakeStore:
    def __init__(self, *, persisted_rows: int):
        self.persisted_rows = int(persisted_rows)

    def _connect(self):
        return _FakeConnection()

    def refresh_stage_task_readiness(self, *, root_domain: str, workflow_id: str, limit: int = 5000):
        _ = (root_domain, workflow_id, limit)

    def count_stage_tasks(self, *, workflow_id: str = "", root_domains: list[str] | None = None, plugins: list[str] | None = None) -> int:
        _ = (workflow_id, root_domains, plugins)
        return int(self.persisted_rows)


def _workflow_definition() -> dict[str, object]:
    return {
        "id": "wf-1",
        "workflow_key": "run-recon",
        "version": 1,
        "steps": [
            {
                "id": "step-1",
                "step_key": "recon-subdomain-enumeration",
                "plugin_key": "recon_subdomain_enumeration",
                "ordinal": 1,
                "max_attempts": 1,
                "retry_failed": False,
                "config_json": {},
                "preconditions_json": {},
            }
        ],
    }


def test_create_workflow_run_requires_root_domain(monkeypatch):
    monkeypatch.setattr(workflow_store, "ensure_workflow_schema", lambda store: None)
    monkeypatch.setattr(workflow_store, "get_workflow_definition", lambda store, workflow_key: _workflow_definition())

    store = _FakeStore(persisted_rows=1)
    with pytest.raises(ValueError, match="root_domain is required"):
        workflow_store.create_workflow_run(
            store,
            {"workflow_key": "run-recon", "root_domain": ""},
            actor="test",
        )


def test_create_workflow_run_raises_when_no_stage_rows_persist(monkeypatch):
    monkeypatch.setattr(workflow_store, "ensure_workflow_schema", lambda store: None)
    monkeypatch.setattr(workflow_store, "get_workflow_definition", lambda store, workflow_key: _workflow_definition())

    store = _FakeStore(persisted_rows=0)
    with pytest.raises(RuntimeError, match="no rows were persisted"):
        workflow_store.create_workflow_run(
            store,
            {"workflow_key": "run-recon", "root_domain": "example.com"},
            actor="test",
        )


def test_create_workflow_run_returns_persisted_stage_task_count(monkeypatch):
    monkeypatch.setattr(workflow_store, "ensure_workflow_schema", lambda store: None)
    monkeypatch.setattr(workflow_store, "get_workflow_definition", lambda store, workflow_key: _workflow_definition())

    store = _FakeStore(persisted_rows=3)
    result = workflow_store.create_workflow_run(
        store,
        {"workflow_key": "run-recon", "root_domain": "example.com"},
        actor="test",
    )
    assert result["workflow_key"] == "run-recon"
    assert result["root_domain"] == "example.com"
    assert int(result["persisted_stage_task_rows"]) == 3
