from __future__ import annotations

from typing import Any

from server_app.store import CoordinatorStore


class _PendingStageCursor:
    def __init__(self) -> None:
        self.status = "pending"
        self.updated_statuses: list[str] = []
        self._last_query = ""

    def execute(self, query: str, params: Any = None) -> None:
        self._last_query = " ".join(str(query or "").split()).lower()
        if self._last_query.startswith("update coordinator_stage_tasks set status"):
            self.status = str(params[0])
            self.updated_statuses.append(self.status)

    def fetchone(self):
        if "select status, attempt_count from coordinator_stage_tasks" in self._last_query:
            return (self.status, 0)
        return None

    def fetchall(self):
        return []

    def __enter__(self) -> "_PendingStageCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _PendingStageConn:
    def __init__(self) -> None:
        self.cursor_obj = _PendingStageCursor()
        self.committed = False

    def cursor(self) -> _PendingStageCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> "_PendingStageConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_schedule_stage_promotes_pending_task_to_ready_when_prerequisites_are_met() -> None:
    conn = _PendingStageConn()
    store = CoordinatorStore.__new__(CoordinatorStore)
    store._connect = lambda: conn  # type: ignore[attr-defined]
    store._load_workflow_stage_preconditions = lambda *_args, **_kwargs: {}  # type: ignore[attr-defined]

    result = CoordinatorStore.schedule_stage(
        store,
        "example.com",
        "recon_subdomain_enumeration",
        workflow_id="run-recon",
    )

    assert result["ok"] is True
    assert result["status"] == "ready"
    assert result["scheduled"] is True
    assert result["reason"] == "prerequisites_satisfied_ready"
    assert conn.cursor_obj.updated_statuses == ["ready"]
    assert conn.committed is True


def test_subdomain_enumeration_is_always_treated_as_bootstrap_ready() -> None:
    store = CoordinatorStore.__new__(CoordinatorStore)
    store._normalize_workflow_token = lambda value, default="": str(value or default).strip().lower()  # type: ignore[attr-defined]

    prereq = CoordinatorStore._load_workflow_stage_preconditions(
        store,
        "run-recon",
        "recon_subdomain_enumeration",
    )

    assert prereq == {}
