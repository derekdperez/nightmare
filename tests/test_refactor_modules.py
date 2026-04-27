from pathlib import Path

from shared.runtime_common.config import ClientSettings, CoordinatorSettings, ServerSettings, normalize_server_base_url
from reporting.server_pages import (
    render_crawl_progress_html,
    render_dashboard_html,
    render_database_html,
    render_docker_status_html,
    render_discovered_targets_html,
    render_extractor_matches_html,
    render_fuzzing_html,
    render_http_requests_html,
    render_view_logs_html,
    render_workflows_html,
    render_workers_html,
)


def test_normalize_server_base_url():
    assert normalize_server_base_url("example.com") == "https://example.com"
    assert normalize_server_base_url("https://example.com/") == "https://example.com"


def test_server_settings_validation():
    settings = ServerSettings.model_validate({"host": "0.0.0.0", "http_port": 8080, "https_port": 0})
    assert settings.host == "0.0.0.0"
    assert settings.http_port == 8080
    assert settings.https_port == 0


def test_coordinator_settings_validation():
    settings = CoordinatorSettings.model_validate({"server_base_url": "coord.example.com", "lease_seconds": 5})
    assert settings.server_base_url == "https://coord.example.com"
    assert settings.lease_seconds == 30


def test_coordinator_settings_insecure_tls_parses_string():
    settings = CoordinatorSettings.model_validate({"server_base_url": "coord.example.com", "insecure_tls": "true"})
    assert settings.insecure_tls is True


def test_client_settings_validation():
    settings = ClientSettings.model_validate({"server_base_url": "coord.example.com", "api_token": " abc "})
    assert settings.server_base_url == "https://coord.example.com"
    assert settings.api_token == "abc"


def test_server_template_renders():
    html = render_dashboard_html()
    assert "<!doctype html>" in html.lower()
    assert "Nightmare Live Dashboard" in html
    assert 'href="/dashboard"' in html
    assert "perfStats" in html
    assert "EventSource" in html
    assert "subscribeToEventStream" in html
    assert 'href="/database"' not in html
    assert 'href="/crawl-progress"' not in html
    assert '/api/summary' in html
    assert "showLoadError" in html


def test_worker_template_renders_database_link():
    html = render_workers_html()
    assert "<!doctype html>" in html.lower()
    assert "Worker Control Center" in html
    assert 'href="/dashboard"' in html
    assert 'href="/workflows"' in html
    assert 'href="/crawl-progress"' not in html
    assert 'href="/database"' not in html
    assert 'encodeURIComponent(l.relative).replace(/%2F/g, "/")' in html
    assert "nightmare_coord_token" in html


def test_workflows_template_renders():
    html = render_workflows_html()
    assert "<!doctype html>" in html.lower()
    assert "Workflow Monitor" in html
    assert "Workflow Timeline" not in html
    assert 'href="/workers"' in html
    assert 'href="/dashboard"' in html
    assert 'href="/database"' not in html
    assert "/api/coord/workflow-snapshot" in html
    assert "/api/coord/stage/control" in html
    assert 'data-action="run"' in html
    assert 'data-action="pause"' in html
    assert 'data-action="delete"' in html
    assert "nightmare_coord_token" in html


def test_database_template_renders():
    html = render_database_html()
    assert "<!doctype html>" in html.lower()
    assert "Database Status" in html
    assert 'href="/workers"' in html
    assert 'href="/http-requests"' in html
    assert 'href="/workers"' in html
    assert 'href="/dashboard"' in html
    assert 'href="/crawl-progress"' not in html
    assert "/api/coord/database-status" in html
    assert "nightmare_coord_token" in html


def test_crawl_progress_template_renders():
    html = render_crawl_progress_html()
    assert "<!doctype html>" in html.lower()
    assert "Crawl Progress" in html
    assert 'href="/workers"' in html
    assert 'href="/http-requests"' in html
    assert 'href="/workers"' in html
    assert 'href="/dashboard"' in html
    assert 'href="/database"' not in html
    assert "/api/coord/crawl-progress" in html
    assert "nightmare_coord_token" in html
    assert 'href="/fuzzing"' not in html


def test_extractor_matches_template_renders():
    html = render_extractor_matches_html()
    assert "<!doctype html>" in html.lower()
    assert "Extractor Matches" in html
    assert 'href="/workers"' in html
    assert 'href="/http-requests"' in html
    assert 'href="/workers"' in html
    assert 'href="/dashboard"' in html
    assert 'href="/database"' not in html
    assert 'href="/crawl-progress"' not in html
    assert 'href="/fuzzing"' not in html
    assert "/api/coord/extractor-matches/domains" in html
    assert "/api/coord/extractor-matches" in html
    assert "/api/coord/extractor-patterns" in html
    assert "/api/coord/extractor-patterns/save" in html
    assert "loadPatternsBtn" in html
    assert "savePatternsBtn" in html
    assert "hideZeroDomains" in html
    assert "nightmare_coord_token" in html


def test_fuzzing_template_renders():
    html = render_fuzzing_html()
    assert "<!doctype html>" in html.lower()
    assert "Fuzzing" in html
    assert 'href="/workers"' in html
    assert 'href="/http-requests"' in html
    assert 'href="/workers"' in html
    assert 'href="/dashboard"' in html
    assert 'href="/database"' not in html
    assert 'href="/crawl-progress"' not in html
    assert 'href="/extractor-matches"' not in html
    assert "/api/coord/fuzzing/domains" in html
    assert "/api/coord/fuzzing?" in html
    assert "/api/coord/ui-preferences" in html
    assert "NightmareGridControls" in html
    assert "fuzzing_findings_table_v1" in html
    assert "nightmare_coord_token" in html


def test_docker_status_template_renders():
    html = render_docker_status_html()
    assert "<!doctype html>" in html.lower()
    assert "Docker Status" in html
    assert 'href="/view-logs"' in html
    assert 'href="/dashboard"' in html
    assert "/api/coord/docker-status" in html
    assert "nightmare_coord_token" in html


def test_view_logs_template_renders():
    html = render_view_logs_html()
    assert "<!doctype html>" in html.lower()
    assert "View Logs" in html
    assert 'href="/docker-status"' not in html
    assert 'href="/dashboard"' in html
    assert "/api/coord/log-sources" in html
    assert "/api/coord/log-events" in html
    assert "/api/coord/log-download" in html
    assert "nightmare_coord_token" in html


def test_discovered_targets_template_renders():
    html = render_discovered_targets_html()
    assert "Discovered Targets" in html
    assert '/api/coord/discovered-targets' in html


def test_http_requests_template_renders():
    html = render_http_requests_html()
    assert "<!doctype html>" in html.lower()
    assert "HTTP Requests" in html
    assert "/api/coord/http-requests" in html
    assert "Double-click any clipped cell to select full content" in html
    assert "max-width: 15vw" in html
