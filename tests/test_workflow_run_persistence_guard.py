from __future__ import annotations

import pytest

from app_platform.workflow import store as workflow_store


class _FakeCursor:
    def __init__(self):
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((" ".join(str(sql).split()), tuple(params) if params is not None else None))


class _FakeConnection:
    def __init__(self):
        self.cursor_instance = _FakeCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        return None


class _FakeStore:
    def __init__(self, *, persisted_rows: int):
        self.persisted_rows = int(persisted_rows)
        self.connection = _FakeConnection()

    def _connect(self):
        return self.connection

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


def test_create_workflow_run_inserts_no_prerequisite_steps_as_ready(monkeypatch):
    monkeypatch.setattr(workflow_store, "ensure_workflow_schema", lambda store: None)
    monkeypatch.setattr(workflow_store, "get_workflow_definition", lambda store, workflow_key: _workflow_definition())

    store = _FakeStore(persisted_rows=1)
    workflow_store.create_workflow_run(
        store,
        {"workflow_key": "run-recon", "root_domain": "example.com"},
        actor="test",
    )

    workflow_step_params = [
        params
        for sql, params in store.connection.cursor_instance.executed
        if sql.startswith("INSERT INTO workflow_step_runs(")
    ]
    coordinator_stage_params = [
        params
        for sql, params in store.connection.cursor_instance.executed
        if sql.startswith("INSERT INTO coordinator_stage_tasks(")
    ]

    assert workflow_step_params
    assert workflow_step_params[0][7] == "ready"
    assert workflow_step_params[0][8] == ""

    assert coordinator_stage_params
    assert coordinator_stage_params[0][3] == "ready"
    assert coordinator_stage_params[0][5] == ""


def test_create_workflow_run_iteration_mode_fans_out_stage_rows(monkeypatch):
    monkeypatch.setattr(workflow_store, "ensure_workflow_schema", lambda store: None)
    definition = _workflow_definition()
    definition["ui_schema"] = {"workflow_type": "iteration_over_list_file"}
    monkeypatch.setattr(workflow_store, "get_workflow_definition", lambda store, workflow_key: definition)

    store = _FakeStore(persisted_rows=1)
    workflow_store.create_workflow_run(
        store,
        {
            "workflow_key": "run-recon",
            "root_domain": "example.com",
            "input": {"iteration_items": ["one", "two"]},
        },
        actor="test",
    )

    coordinator_stage_params = [
        params
        for sql, params in store.connection.cursor_instance.executed
        if sql.startswith("INSERT INTO coordinator_stage_tasks(")
    ]
    workflow_step_params = [
        params
        for sql, params in store.connection.cursor_instance.executed
        if sql.startswith("INSERT INTO workflow_step_runs(")
    ]
    assert len(coordinator_stage_params) == 2
    assert len(workflow_step_params) == 2
    workflow_ids = [str(params[0]) for params in coordinator_stage_params]
    assert all(".iter." in value for value in workflow_ids)
