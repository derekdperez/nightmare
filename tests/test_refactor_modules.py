from pathlib import Path

from nightmare_shared.config import ClientSettings, CoordinatorSettings, ServerSettings, normalize_server_base_url
from reporting.server_pages import render_dashboard_html, render_database_html, render_workers_html


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
    assert 'href="/database"' in html
    assert '/api/summary' in html
    assert "showLoadError" in html


def test_worker_template_renders_database_link():
    html = render_workers_html()
    assert "<!doctype html>" in html.lower()
    assert "Worker Control Center" in html
    assert 'href="/database"' in html
    assert 'encodeURIComponent(l.relative).replace(/%2F/g, "/")' in html
    assert "nightmare_coord_token" in html


def test_database_template_renders():
    html = render_database_html()
    assert "<!doctype html>" in html.lower()
    assert "Database Status" in html
    assert 'href="/dashboard"' in html
    assert 'href="/workers"' in html
    assert "/api/coord/database-status" in html
    assert "nightmare_coord_token" in html
