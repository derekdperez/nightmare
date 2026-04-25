from pathlib import Path

from app_platform.coordinator_runtime.runtime import load_config
from app_platform.server.store import CoordinatorStore


def test_new_module_files_exist():
    assert Path("app_platform/server/store.py").is_file()
    assert Path("app_platform/coordinator_runtime/runtime.py").is_file()


def test_coordinator_runtime_exports():
    assert callable(load_config)
    assert CoordinatorStore.__name__ == "CoordinatorStore"


def test_server_uses_external_coordinator_store():
    server_source = Path("server.py").read_text(encoding="utf-8")
    assert "from app_platform.server.store import CoordinatorStore" in server_source
    assert "class CoordinatorStore" not in server_source
    assert "from reporting.server_pages import" in server_source
    assert "render_dashboard_html" in server_source
    assert "render_workers_html" in server_source
    assert "render_database_html" in server_source
    assert "render_crawl_progress_html" in server_source
    assert "render_extractor_matches_html" in server_source
    assert "render_fuzzing_html" in server_source
    assert "render_docker_status_html" in server_source
    assert "render_view_logs_html" in server_source
    assert "render_discovered_targets_html" in server_source
    assert "render_workflows_html" in server_source
    assert "def _render_dashboard_html" not in server_source
    assert "def _render_workers_html" not in server_source
    assert "def _render_database_html" not in server_source
    assert "def _render_crawl_progress_html" not in server_source
    assert "def _render_extractor_matches_html" not in server_source
    assert "def _render_fuzzing_html" not in server_source


def test_store_includes_database_status_method():
    assert callable(getattr(CoordinatorStore, "database_status", None))
