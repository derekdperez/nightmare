#!/usr/bin/env python3
"""Server-facing HTML page helpers."""

from __future__ import annotations

from nightmare_shared.templating import render_template


def render_dashboard_html() -> str:
    return render_template("server_dashboard.html.j2")


def render_workers_html() -> str:
    return render_template("worker_control.html.j2")


def render_operations_html() -> str:
    return render_template("operations.html.j2")


def render_database_html() -> str:
    return render_template("database_status.html.j2")


def render_crawl_progress_html() -> str:
    return render_template("crawl_progress.html.j2")


def render_extractor_matches_html() -> str:
    return render_template("extractor_matches.html.j2")


def render_fuzzing_html() -> str:
    return render_template("fuzzing.html.j2")


def render_docker_status_html() -> str:
    return render_template("docker_status.html.j2")


def render_view_logs_html() -> str:
    return render_template("view_logs.html.j2")


def render_errors_html() -> str:
    return render_template("errors.html.j2")


def render_auth0r_html() -> str:
    return render_template("auth0r.html.j2")


def render_discovered_targets_html() -> str:
    return render_template("discovered_targets.html.j2")


def render_discovered_files_html() -> str:
    return render_template("discovered_files.html.j2")


def render_http_requests_html() -> str:
    return render_template("http_requests.html.j2")


def render_events_html() -> str:
    return render_template("events.html.j2")


def render_workflows_html() -> str:
    return render_template("workflows.html.j2")


def render_discovered_target_response_html() -> str:
    return render_template("discovered_target_response.html.j2")


def render_configurations_html() -> str:
    return render_template("configurations.html.j2")


def render_workflow_definitions_html() -> str:
    return render_template("workflow_definitions.html.j2")


def render_workflow_runs_html() -> str:
    return render_template("workflow_runs.html.j2")


def render_plugin_definitions_html() -> str:
    return render_template("plugin_definitions.html.j2")
