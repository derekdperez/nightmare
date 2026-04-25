from __future__ import annotations

from workflow_app.store import get_workflow_definition, save_workflow_definition, workflow_payload_from_scheduler_file


def test_save_workflow_definition_archives_removed_steps_without_deleting():
    class FakeCursor:
        def __init__(self):
            self._fetchone = None
            self._fetchall = []
            self.queries: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            self.queries.append(compact)
            if "SELECT to_jsonb(w.*) FROM workflow_definitions w WHERE workflow_key=%s" in compact:
                self._fetchone = ({"id": "wf-1", "workflow_key": "run-recon"},)
                return
            if "RETURNING id::text, version;" in compact and "INSERT INTO workflow_definitions" in compact:
                self._fetchone = ("wf-1", 1)
                return
            if "SELECT id::text, step_key, ordinal, COALESCE(is_archived, FALSE)" in compact:
                self._fetchall = [
                    ("592d110e-a359-4846-9ccb-ab619b37e126", "legacy-step", 1, False),
                    ("7e559f63-4e1b-4f4a-a8a6-9ed9f89f2d7e", "remove-me", 2, False),
                ]
                return

        def fetchone(self):
            return self._fetchone

        def fetchall(self):
            return self._fetchall

    class FakeConnection:
        def __init__(self):
            self.cursor_instance = FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return self.cursor_instance

        def commit(self):
            return None

    conn = FakeConnection()

    class FakeStore:
        def _connect(self):
            return conn

    payload = {
        "workflow_key": "run-recon",
        "name": "Run Recon",
        "steps": [
            {
                "step_key": "legacy-step",
                "display_name": "Legacy Step",
                "plugin_key": "recon_spider_source_tags",
                "ordinal": 1,
                "enabled": True,
                "config_json": {},
                "preconditions_json": {},
            }
        ],
    }
    result = save_workflow_definition(FakeStore(), payload, actor="test")
    assert result["workflow_key"] == "run-recon"
    assert result["id"] == "wf-1"

    joined = "\n".join(conn.cursor_instance.queries)
    assert "DELETE FROM workflow_steps WHERE workflow_definition_id=%s" not in joined
    assert "UPDATE workflow_steps SET is_archived = TRUE" in joined
    assert "INSERT INTO workflow_steps(" in joined
    assert "ON CONFLICT (id) DO UPDATE SET" in joined


def test_get_workflow_definition_excludes_archived_steps():
    class FakeCursor:
        def __init__(self):
            self._fetchone = None
            self._fetchall = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if compact.startswith("CREATE TABLE IF NOT EXISTS") or compact.startswith("ALTER TABLE ") or compact.startswith("UPDATE workflow_steps SET max_attempts = 4") or compact.startswith("UPDATE workflow_step_runs SET max_attempts = 4"):
                return
            if "FROM workflow_definitions WHERE workflow_key=%s" in compact:
                self._fetchone = ("wf-1", "run-recon", 1, "Run Recon", "", "draft", "manual", {}, {}, [])
                return
            if "FROM workflow_steps" in compact and "workflow_definition_id=%s" in compact:
                assert "COALESCE(is_archived, FALSE) = FALSE" in compact
                self._fetchall = [
                    (
                        "step-1",
                        "active-step",
                        "Active Step",
                        "recon_spider_source_tags",
                        1,
                        True,
                        False,
                        False,
                        4,
                        0,
                        {},
                        {},
                        {},
                        {},
                    )
                ]
                return
            raise AssertionError(f"Unexpected SQL in test: {compact}")

        def fetchone(self):
            return self._fetchone

        def fetchall(self):
            return self._fetchall

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    class FakeStore:
        def _connect(self):
            return FakeConnection()

    workflow = get_workflow_definition(FakeStore(), "run-recon")
    assert workflow is not None
    assert workflow["workflow_key"] == "run-recon"
    assert len(workflow["steps"]) == 1
    assert workflow["steps"][0]["step_key"] == "active-step"


def test_scheduler_workflow_uses_default_retry_limit_without_required_attempts(tmp_path):
    workflow_path = tmp_path / "sample.workflow.json"
    workflow_path.write_text(
        """{
          "workflow_id": "run-recon",
          "retry_limit": 3,
          "plugins": [
            {
              "name": "recon_subdomain_enumeration",
              "plugin_name": "recon_subdomain_enumeration",
              "enabled": true,
              "preconditions": {},
              "retry_failed": true
            }
          ]
        }""",
        encoding="utf-8",
    )

    payload = workflow_payload_from_scheduler_file(workflow_path)
    step = payload["steps"][0]
    assert step["retry_limit"] == 3
    assert step["max_attempts"] == 4
