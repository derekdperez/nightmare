#!/usr/bin/env python3
"""Server-facing HTML page helpers."""

from __future__ import annotations

from nightmare_shared.templating import render_template


def render_dashboard_html() -> str:
    return render_template("server_dashboard.html.j2")


def render_workers_html() -> str:
    return render_template("worker_control.html.j2")


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


def render_discovered_targets_html() -> str:
    return render_template("discovered_targets.html.j2")


def render_discovered_files_html() -> str:
    return render_template("discovered_files.html.j2")


def render_auth0r_html() -> str:
    return render_template("auth0r.html.j2")
