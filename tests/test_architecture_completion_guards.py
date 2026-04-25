from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_runtime_scheduler_no_longer_uses_whole_fleet_snapshot() -> None:
    coordinator = _read("coordinator.py")
    assert "get_workflow_snapshot(limit=5000)" not in coordinator
    assert "_workflow_reschedule_queue" in coordinator
    assert "mode=\"domain_event_queue\"" in coordinator


def test_resource_lock_claim_path_is_primary() -> None:
    store = _read("app_platform/server/store.py")
    assert "coordinator_resource_leases" in store
    assert "def try_claim_stage_with_resources" in store
    assert "return self.try_claim_stage_with_resources" in store
    assert "pg_advisory_xact_lock" in store


def test_eventstream_and_zip_handoff_removed_from_runtime() -> None:
    store = _read("app_platform/server/store.py")
    coordinator = _read("coordinator.py")
    assert "EventStream" not in store
    assert "_upload_zip_artifact" not in coordinator
    assert "_download_zip_artifact" not in coordinator
    assert "_upload_directory_manifest_artifact" in coordinator
    assert "_materialize_manifest_artifact" in coordinator


def test_content_addressed_manifest_tables_and_api_exist() -> None:
    store = _read("app_platform/server/store.py")
    api = _read("app_platform/server/fastapi_app.py")
    runtime = _read("app_platform/coordinator_runtime/runtime.py")
    assert "coordinator_artifact_objects" in store
    assert "coordinator_artifact_manifest_entries" in store
    assert "list_artifact_manifest_entries" in store
    assert "/api/coord/artifact/manifest-entries" in api
    assert "def list_artifact_manifest_entries" in runtime


def test_plugin_contract_has_runtime_resource_and_artifact_metadata() -> None:
    base = _read("plugins/base.py")
    assert "ArtifactRequirement" in base
    assert "ArtifactDeclaration" in base
    assert "shard_key_strategy" in base
    assert "describe_resources" in base
