#!/usr/bin/env python3
"""Small web server for Nightmare/Fozzy reporting and live run overview.

Usage:
    python server.py
    python server.py --host 0.0.0.0 --http-port 80 --output-root output
    python server.py --config server.json
    python server.py --reset-coordinator --reset-confirm RESET_COORDINATOR_DATA --database-url ...

Public master report:
- When ``output_root/all_domains.results_summary.html`` exists, ``GET /`` serves that file.
- Regenerate on the server (same tree as ``output_root``) via authenticated POST::

    curl -X POST -H "Authorization: Bearer YOUR_TOKEN" http://HOST/api/regenerate-master-report

  Token resolution: ``master_report_regen_token`` in config, else env ``MASTER_REPORT_REGEN_TOKEN``,
  else ``coordinator_api_token`` (must be non-empty; the endpoint is disabled if none are set).

The web UI provides:
- aggregate counts (domains discovered, completed/running/pending/failed),
- per-domain status rows with links to generated HTML reports and JSON artifacts,
- master-report links when available,
- recent log tail snippets.
"""

from __future__ import annotations

import argparse
import base64
import copy
import glob
import html
import io
import json
import mimetypes
import os
import re
import shlex
import shutil
import ssl
import subprocess
import sys
import time
import threading
import zipfile
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, quote, unquote, urlparse

from output_cleanup import clear_output_root_children
from nightmare_shared.templating import render_template
from reporting.server_pages import (
    render_auth0r_html,
    render_crawl_progress_html,
    render_dashboard_html,
    render_database_html,
    render_discovered_files_html,
    render_discovered_target_response_html,
    render_discovered_targets_html,
    render_docker_status_html,
    render_errors_html,
    render_events_html,
    render_extractor_matches_html,
    render_fuzzing_html,
    render_http_requests_html,
    render_view_logs_html,
    render_workflows_html,
    render_workers_html,
    render_operations_html,
    render_workflow_definitions_html,
    render_workflow_runs_html,
    render_plugin_definitions_html,
)
from server_app.store import CoordinatorStore
from auth0r.profile_store import Auth0rProfileStore
from logging_app.store import LogStore

from workflow_app.store import (
    create_workflow_run,
    get_workflow_definition,
    get_workflow_run,
    list_plugin_definitions,
    list_workflow_definitions,
    list_workflow_runs,
    publish_workflow_definition,
    save_workflow_definition,
    seed_builtin_plugins,
    seed_builtin_workflows,
    load_builtin_workflow_definition,
    upsert_plugin_definition,
)

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = BASE_DIR / "output"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 443
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "server.json"
DEFAULT_COORDINATOR_LEASE_SECONDS = 120
RESET_COORDINATOR_CONFIRM = "RESET_COORDINATOR_DATA"
DEFAULT_EXTRACTOR_MATCH_LIMIT = 250
MAX_EXTRACTOR_MATCH_LIMIT = 2000
MAX_EXTRACTOR_CACHE_DOMAINS = 64
EXTRACTOR_CACHE_TTL_SECONDS = 900
DEFAULT_FUZZING_RESULT_LIMIT = 250
MAX_FUZZING_RESULT_LIMIT = 2000
MAX_FUZZING_CACHE_DOMAINS = 64
FUZZING_CACHE_TTL_SECONDS = 900
DEFAULT_VIEW_LOG_LINES = 300
MAX_VIEW_LOG_LINES = 2000
MAX_VIEW_LOG_FILE_SOURCES = 1000
MAX_LOG_DOWNLOAD_BYTES = 25 * 1024 * 1024
VIEW_LOG_SOURCES_CACHE_TTL_SECONDS = 30
PAGE_DATA_CACHE_TTL_SECONDS = 30
PAGE_DATA_CACHE_MAX_ENTRIES = 512
PAGE_CACHE_WARM_INTERVAL_SECONDS = 20
CRAWL_PROGRESS_PAGE_CACHE_TTL_SECONDS = 8
DISCOVERED_TARGETS_PAGE_CACHE_TTL_SECONDS = 30
DISCOVERED_TARGET_SITEMAP_PAGE_CACHE_TTL_SECONDS = 45
DISCOVERED_FILES_PAGE_CACHE_TTL_SECONDS = 45
HTTP_REQUESTS_PAGE_CACHE_TTL_SECONDS = 30
WORKFLOW_SNAPSHOT_PAGE_CACHE_TTL_SECONDS = 12
EST_TZ = timezone(timedelta(hours=-5), name="EST")
WORKFLOW_FILE_SUFFIX = ".workflow.json"
WORKFLOW_FILE_GLOB = f"*{WORKFLOW_FILE_SUFFIX}"
WORKFLOW_INTERFACE_ROUTE_PREFIX = "/workflow-interfaces"
WORKFLOW_INTERFACE_TYPES = ("control", "results")


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
    except Exception:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _resolve_config_path(raw_path: str | None) -> Path:
    value = str(raw_path or "").strip() or "server.json"
    p = Path(value).expanduser()
    if p.is_absolute():
        return p.resolve()
    if p.parts and p.parts[0].lower() == "config":
        return (BASE_DIR / p).resolve()
    return (BASE_DIR / "config" / p).resolve()


def _normalize_workflow_id(value: Any, *, default: str = "") -> str:
    raw = str(value or "").strip().lower()
    safe = re.sub(r"[^a-z0-9._-]+", "-", raw).strip("-")
    if safe:
        return safe
    fallback = str(default or "").strip().lower()
    return re.sub(r"[^a-z0-9._-]+", "-", fallback).strip("-")


def _default_workflow_path_for_id(workflow_id: str) -> Path:
    safe_id = _normalize_workflow_id(workflow_id)
    return (BASE_DIR / "workflows" / f"{safe_id}{WORKFLOW_FILE_SUFFIX}").resolve()


def _iter_workflow_paths() -> list[Path]:
    workflows_dir = BASE_DIR / "workflows"
    if not workflows_dir.is_dir():
        return []
    return sorted(path.resolve() for path in workflows_dir.glob(WORKFLOW_FILE_GLOB) if path.is_file())


def _workflow_id_from_path(path: Path) -> str:
    name = str(path.name or "")
    lowered = name.lower()
    if lowered.endswith(WORKFLOW_FILE_SUFFIX):
        return _normalize_workflow_id(name[:-len(WORKFLOW_FILE_SUFFIX)])
    return _normalize_workflow_id(path.stem)


def _load_workflow_payload(path: Path) -> dict[str, Any]:
    payload = _read_json_dict(path)
    if not payload:
        return {}
    workflow_id = _normalize_workflow_id(payload.get("workflow_id"), default=_workflow_id_from_path(path))
    payload["workflow_id"] = workflow_id or _workflow_id_from_path(path)
    plugins = payload.get("plugins")
    payload["plugins"] = [item for item in plugins if isinstance(item, dict)] if isinstance(plugins, list) else []
    return payload


def _workflow_index_payload() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in _iter_workflow_paths():
        payload = _load_workflow_payload(path)
        workflow_id = _normalize_workflow_id(payload.get("workflow_id"), default=_workflow_id_from_path(path))
        if not workflow_id:
            continue
        out.append(
            {
                "workflow_id": workflow_id,
                "description": str(payload.get("description") or "").strip(),
                "plugin_count": len(payload.get("plugins") if isinstance(payload.get("plugins"), list) else []),
                "path_rel": _to_repo_relative_path(path),
            }
        )
    out.sort(key=lambda item: str(item.get("workflow_id") or ""))
    return out


def _resolve_workflow_path(workflow_id: str) -> Path | None:
    safe_id = _normalize_workflow_id(workflow_id)
    if not safe_id:
        return None
    direct = _default_workflow_path_for_id(safe_id)
    if direct.is_file():
        return direct
    for path in _iter_workflow_paths():
        payload = _load_workflow_payload(path)
        payload_id = _normalize_workflow_id(payload.get("workflow_id"), default=_workflow_id_from_path(path))
        if payload_id and payload_id == safe_id:
            return path
    return None



def _workflow_catalog_payload() -> dict[str, dict[str, Any]]:
    catalog: dict[str, dict[str, Any]] = {}
    for path in _iter_workflow_paths():
        payload = _load_workflow_payload(path)
        workflow_id = _normalize_workflow_id(payload.get("workflow_id"), default=_workflow_id_from_path(path))
        if workflow_id:
            catalog[workflow_id] = payload
    return catalog


def _normalize_workflow_interface_route(value: Any, *, workflow_id: str, interface_type: str) -> str:
    raw = str(value or "").strip()
    default = f"{WORKFLOW_INTERFACE_ROUTE_PREFIX}/{workflow_id}/{interface_type}"
    if not raw:
        return default
    candidate = raw.split("?", 1)[0].split("#", 1)[0].strip()
    if not candidate:
        return default
    if not candidate.startswith("/"):
        candidate = f"/{candidate}"
    parts = [part for part in candidate.split("/") if part]
    return "/" + "/".join(parts) if parts else default


def _normalize_workflow_interface_template(value: Any) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    if not raw:
        return ""
    if raw.startswith("/"):
        return ""
    parts = [part for part in raw.split("/") if part]
    if not parts:
        return ""
    if any(part == ".." for part in parts):
        return ""
    normalized = "/".join(parts)
    if not normalized.endswith(".j2"):
        return ""
    template_path = (BASE_DIR / "templates" / normalized).resolve()
    templates_root = (BASE_DIR / "templates").resolve()
    try:
        template_path.relative_to(templates_root)
    except ValueError:
        return ""
    if not template_path.is_file():
        return ""
    return normalized


def _workflow_interface_catalog_payload() -> dict[str, Any]:
    routes: dict[str, dict[str, Any]] = {}
    for path in _iter_workflow_paths():
        payload = _load_workflow_payload(path)
        workflow_id = _normalize_workflow_id(payload.get("workflow_id"), default=_workflow_id_from_path(path))
        if not workflow_id:
            continue
        raw_interfaces = payload.get("interfaces")
        if not isinstance(raw_interfaces, dict):
            continue
        ordered_keys: list[str] = []
        for key in WORKFLOW_INTERFACE_TYPES:
            if key in raw_interfaces:
                ordered_keys.append(key)
        for key in sorted(raw_interfaces.keys()):
            if key not in ordered_keys:
                ordered_keys.append(key)
        for interface_type in ordered_keys:
            item = raw_interfaces.get(interface_type)
            if isinstance(item, str):
                item = {"template": item}
            if not isinstance(item, dict):
                continue
            if not bool(item.get("enabled", True)):
                continue
            template_name = _normalize_workflow_interface_template(
                item.get("template", item.get("page", item.get("html_page", "")))
            )
            if not template_name:
                continue
            safe_type = _normalize_workflow_id(interface_type, default="page") or "page"
            route = _normalize_workflow_interface_route(
                item.get("route", item.get("path", "")),
                workflow_id=workflow_id,
                interface_type=safe_type,
            )
            title = str(item.get("title") or f"{workflow_id} {safe_type.title()}").strip() or f"{workflow_id} {safe_type.title()}"
            nav_label = str(item.get("nav_label") or title).strip() or title
            description = str(item.get("description") or "").strip()
            entry = {
                "workflow_id": workflow_id,
                "interface_type": safe_type,
                "title": title,
                "nav_label": nav_label,
                "description": description,
                "route": route,
                "template": template_name,
                "path_rel": _to_repo_relative_path(path) or str(path),
            }
            # Last writer wins for duplicate routes so aliases/legacy workflow files
            # do not generate duplicate navigation links.
            routes[route] = entry
    interfaces = list(routes.values())
    interfaces.sort(key=lambda row: (str(row.get("workflow_id") or ""), str(row.get("interface_type") or "")))
    return {"generated_at_utc": _iso_now(), "interfaces": interfaces, "routes": routes}


def _decode_json_bytes(payload: bytes) -> Any:
    raw = bytes(payload or b"")
    if not raw:
        return {}
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        return {}


def _extract_recon_subdomains(payload: Any) -> list[str]:
    items: set[str] = set()
    if isinstance(payload, dict):
        subdomains = payload.get("subdomains")
        if isinstance(subdomains, list):
            for item in subdomains:
                text = str(item or "").strip().lower()
                if text:
                    items.add(text)
        entries = payload.get("entries")
        if isinstance(entries, list):
            for row in entries:
                if not isinstance(row, dict):
                    continue
                text = str(row.get("subdomain") or "").strip().lower()
                if text:
                    items.add(text)
    elif isinstance(payload, list):
        for item in payload:
            text = str(item or "").strip().lower()
            if text:
                items.add(text)
    return sorted(items)


def _extract_recon_takeover_count(payload: Any) -> int:
    if not isinstance(payload, dict):
        return 0
    for key in (
        "potential_takeover_count",
        "potential_takeovers_count",
        "takeover_count",
        "findings_count",
        "match_count",
        "anomaly_count",
    ):
        value = _safe_int(payload.get(key), -1)
        if value >= 0:
            return value
    for key in ("potential_takeovers", "findings", "matches", "anomalies"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return len(rows)
    return 0


def _extract_recon_high_value_count(payload: Any) -> int:
    if not isinstance(payload, dict):
        return 0
    for key in ("match_count", "high_value_count", "findings_count", "result_count"):
        value = _safe_int(payload.get(key), -1)
        if value >= 0:
            return value
    rows = payload.get("rows")
    if isinstance(rows, list):
        return len(rows)
    return 0


def _extract_request_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("request_count", "total_requests", "count"):
            value = _safe_int(payload.get(key), -1)
            if value >= 0:
                return value
        for key in ("requests", "rows", "items"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return len(rows)
    return 0


def _workflow_running_tasks_summary(
    snapshot: dict[str, Any],
    *,
    workflow_id: str,
    root_domains: Optional[set[str]] = None,
    plugins: Optional[set[str]] = None,
) -> dict[str, Any]:
    running_count = 0
    workers: set[str] = set()
    domains = snapshot.get("domains") if isinstance(snapshot.get("domains"), list) else []
    for domain in domains:
        if not isinstance(domain, dict):
            continue
        root_domain = str(domain.get("root_domain") or "").strip().lower()
        if not root_domain:
            continue
        if root_domains and root_domain not in root_domains:
            continue
        plugin_tasks = domain.get("plugin_tasks") if isinstance(domain.get("plugin_tasks"), dict) else {}
        workflow_tasks = plugin_tasks.get(workflow_id) if isinstance(plugin_tasks.get(workflow_id), dict) else {}
        for plugin_name, row in workflow_tasks.items():
            if not isinstance(row, dict):
                continue
            safe_plugin = str(plugin_name or "").strip().lower()
            if plugins and safe_plugin not in plugins:
                continue
            if str(row.get("status") or "").strip().lower() != "running":
                continue
            running_count += 1
            worker_id = str(row.get("worker_id") or "").strip()
            if worker_id:
                workers.add(worker_id)
    return {"running_tasks": running_count, "running_workers": sorted(workers)}


def _workflow_stage_prerequisites_satisfied(
    workflow_entry: dict[str, Any],
    *,
    domain_row: dict[str, Any],
    workflow_id: str,
) -> bool:
    artifacts = {
        str(item or "").strip().lower()
        for item in (domain_row.get("artifact_types") if isinstance(domain_row.get("artifact_types"), list) else [])
        if str(item or "").strip()
    }
    plugin_tasks_all = domain_row.get("plugin_tasks") if isinstance(domain_row.get("plugin_tasks"), dict) else {}
    workflow_tasks = plugin_tasks_all.get(workflow_id) if isinstance(plugin_tasks_all.get(workflow_id), dict) else {}
    target_counts = domain_row.get("targets") if isinstance(domain_row.get("targets"), dict) else {}
    prereq = workflow_entry.get("preconditions", workflow_entry.get("prerequisites", {}))
    prereq = prereq if isinstance(prereq, dict) else {}

    required_all = {str(item or "").strip().lower() for item in (prereq.get("artifacts_all") or []) if str(item or "").strip()}
    required_any = {str(item or "").strip().lower() for item in (prereq.get("artifacts_any") or []) if str(item or "").strip()}
    required_plugins_all = {
        str(item or "").strip().lower()
        for item in (prereq.get("requires_plugins_all") or prereq.get("plugins_all") or [])
        if str(item or "").strip()
    }
    required_plugins_any = {
        str(item or "").strip().lower()
        for item in (prereq.get("requires_plugins_any") or prereq.get("plugins_any") or [])
        if str(item or "").strip()
    }
    required_target_statuses = {
        str(item or "").strip().lower()
        for item in (prereq.get("target_statuses") or [])
        if str(item or "").strip()
    }
    require_target_completed = bool(prereq.get("require_target_completed", False))

    def _task_status(name: str) -> str:
        row = workflow_tasks.get(name) if isinstance(workflow_tasks.get(name), dict) else {}
        return str(row.get("status") or "").strip().lower()

    pending_targets = _safe_int(target_counts.get("pending", 0), 0)
    running_targets = _safe_int(target_counts.get("running", 0), 0)
    completed_targets = _safe_int(target_counts.get("completed", 0), 0)
    failed_targets = _safe_int(target_counts.get("failed", 0), 0)
    if running_targets > 0:
        current_target_status = "running"
    elif pending_targets > 0:
        current_target_status = "pending"
    elif failed_targets > 0 and completed_targets <= 0:
        current_target_status = "failed"
    elif completed_targets > 0:
        current_target_status = "completed"
    else:
        current_target_status = "unknown"

    if required_all and not required_all.issubset(artifacts):
        return False
    if required_any and artifacts.isdisjoint(required_any):
        return False
    if required_plugins_all and any(_task_status(item) != "completed" for item in required_plugins_all):
        return False
    if required_plugins_any and not any(_task_status(item) == "completed" for item in required_plugins_any):
        return False
    if required_target_statuses and current_target_status not in required_target_statuses:
        return False
    if require_target_completed and completed_targets <= 0:
        return False
    return True

def _default_server_config() -> dict[str, Any]:
    return {
        "host": DEFAULT_HOST,
        "port": DEFAULT_PORT,
        "output_root": "output",
        "database_url": "",
        "log_database_url": "",
        "coordinator_api_token": "",
        "master_report_regen_token": "",
    }


def _merged_value(cli_value: Any, cfg: dict[str, Any], key: str, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if key in cfg:
        value = cfg[key]
        if value is not None and value != "":
            return value
    return default


def _urls_point_to_same_database(left: str, right: str) -> bool:
    lhs = str(left or "").strip()
    rhs = str(right or "").strip()
    if not lhs or not rhs:
        return False
    if lhs == rhs:
        return True
    try:
        l = urlparse(lhs)
        r = urlparse(rhs)
    except Exception:
        return False
    if not l.scheme or not r.scheme:
        return False
    return (
        l.scheme.lower() == r.scheme.lower()
        and (l.hostname or "").lower() == (r.hostname or "").lower()
        and int(l.port or 0) == int(r.port or 0)
        and (l.path or "").strip("/") == (r.path or "").strip("/")
        and (l.username or "") == (r.username or "")
    )


def _find_all_domains_report_html(output_root: Path) -> Path | None:
    candidate = output_root / "all_domains.results_summary.html"
    if candidate.is_file():
        return candidate
    return None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _max_iso_datetime(*values: Any) -> str:
    latest: datetime | None = None
    for item in values:
        parsed = _parse_iso_datetime(item)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest.isoformat() if latest is not None else ""


def _latest_worker_log_line_from_links(logs: list[dict[str, Any]]) -> dict[str, str]:
    newest_path: Path | None = None
    newest_mtime = -1.0
    for item in list(logs or []):
        if not isinstance(item, dict):
            continue
        rel = str(item.get("relative") or "").strip()
        if not rel:
            continue
        candidate = (BASE_DIR / rel).resolve()
        if not candidate.is_file():
            continue
        try:
            stat = candidate.stat()
        except Exception:
            continue
        if stat.st_mtime > newest_mtime:
            newest_mtime = float(stat.st_mtime)
            newest_path = candidate
    if newest_path is None:
        return {"message": "", "event_time_utc": ""}
    message = ""
    try:
        tail = _read_tail_lines(newest_path, lines=5)
        for line in reversed(tail.splitlines()):
            cleaned = str(line or "").strip()
            if cleaned:
                message = cleaned
                break
    except Exception:
        message = ""
    event_time_utc = ""
    try:
        event_time_utc = datetime.fromtimestamp(newest_mtime, tz=timezone.utc).isoformat()
    except Exception:
        event_time_utc = ""
    return {"message": message, "event_time_utc": event_time_utc}


def _enrich_worker_snapshot_with_live_details(
    snapshot: dict[str, Any],
    *,
    log_store: LogStore | None = None,
) -> dict[str, Any]:
    workers = snapshot.get("workers")
    if not isinstance(workers, list):
        return snapshot
    worker_ids = [str(worker.get("worker_id") or "").strip() for worker in workers if isinstance(worker, dict)]
    log_map: dict[str, dict[str, Any]] = {}
    if log_store is not None:
        try:
            if hasattr(log_store, "latest_events_by_worker_ids"):
                log_map = log_store.latest_events_by_worker_ids(worker_ids)
            else:
                log_map = log_store.latest_events_by_source_ids(worker_ids)
        except Exception:
            log_map = {}
    for worker in workers:
        if not isinstance(worker, dict):
            continue
        worker_id = str(worker.get("worker_id") or "").strip()
        log_info = log_map.get(worker_id, {})
        last_log_message = str(log_info.get("description") or "").strip() or str(log_info.get("raw_line") or "").strip()
        last_log_message_at_utc = str(log_info.get("event_time_utc") or "").strip()
        if not last_log_message:
            file_log_info = _latest_worker_log_line_from_links(worker.get("logs") if isinstance(worker.get("logs"), list) else [])
            last_log_message = str(file_log_info.get("message") or "").strip()
            last_log_message_at_utc = str(file_log_info.get("event_time_utc") or "").strip()
        if not last_log_message:
            last_log_message = str(worker.get("last_event_emitted") or "").strip()
            last_log_message_at_utc = str(worker.get("last_event_emitted_at_utc") or "").strip()
        worker["last_log_message"] = last_log_message
        worker["last_log_message_at_utc"] = last_log_message_at_utc
        if not str(worker.get("last_action_performed") or "").strip() or str(worker.get("last_action_performed") or "").strip().lower() == "unknown":
            if last_log_message:
                worker["last_action_performed"] = last_log_message
        worker["last_seen_time_at_utc"] = _max_iso_datetime(
            worker.get("last_seen_time_at_utc"),
            worker.get("last_heartbeat_at_utc"),
            worker.get("last_event_emitted_at_utc"),
            worker.get("last_log_message_at_utc"),
        )
        worker["last_run_time_at_utc"] = str(worker.get("last_seen_time_at_utc") or "")
    return snapshot


def _collect_master_reports(output_root: Path) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    candidates = [
        output_root / "all_domains.results_summary.html",
        output_root / "all_domains.results_summary.json",
    ]
    for path in candidates:
        if path.is_file():
            reports.append(
                {
                    "name": path.name,
                    "path": str(path.resolve()),
                    "relative": path.resolve().relative_to(BASE_DIR).as_posix()
                    if str(path.resolve()).startswith(str(BASE_DIR.resolve()))
                    else path.resolve().as_posix(),
                }
            )
    return reports


def _to_repo_relative_path(path: Path) -> str | None:
    try:
        return path.resolve().relative_to(BASE_DIR.resolve()).as_posix()
    except ValueError:
        return None


def _discover_fozzy_summary_for_domain(domain_dir: Path, domain: str) -> Path | None:
    direct = domain_dir / "fozzy-output" / domain / f"{domain}.fozzy.summary.json"
    if direct.is_file():
        return direct
    for candidate in domain_dir.glob(f"**/{domain}.fozzy.summary.json"):
        if candidate.is_file():
            return candidate
    return None


def _discover_fozzy_results_html_for_domain(domain_dir: Path, domain: str) -> Path | None:
    direct = domain_dir / "fozzy-output" / domain / "results" / f"{domain}.results_summary.html"
    if direct.is_file():
        return direct
    for candidate in domain_dir.glob("**/*.results_summary.html"):
        if candidate.is_file() and candidate.name.lower() == f"{domain}.results_summary.html":
            return candidate
    return None


def _discover_nightmare_report_html(domain_dir: Path) -> Path | None:
    report = domain_dir / "report.html"
    return report if report.is_file() else None


def _discover_domain_log_files(domain_dir: Path, domain: str) -> list[Path]:
    out: list[Path] = []
    patterns = [
        f"{domain}_nightmare.log",
        f"{domain}_scrapy.log",
        "*.fozzy.log",
        "*.log",
    ]
    seen: set[str] = set()
    for pattern in patterns:
        for path in domain_dir.glob(pattern):
            if not path.is_file():
                continue
            key = str(path.resolve()).lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(path.resolve())
    for path in domain_dir.glob("**/*.log"):
        if not path.is_file():
            continue
        key = str(path.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(path.resolve())
    return sorted(out, key=lambda p: p.name.lower())




def _normalize_and_validate_relative_path(root: Path, rel: str) -> Optional[Path]:
    """Resolve a user-provided relative path under root, rejecting traversal/absolute paths."""
    try:
        root_resolved = Path(root).resolve()
        original = str(rel or "").strip()
        if not original or original.startswith(("/", "\\")):
            return None
        raw = original.replace("\\", "/")
        if raw.startswith("../") or "/../" in raw or raw == "..":
            return None
        candidate = (root_resolved / raw).resolve()
        try:
            candidate.relative_to(root_resolved)
        except ValueError:
            return None
        return candidate
    except Exception:
        return None

def _read_tail_lines(path: Path, *, lines: int = DEFAULT_VIEW_LOG_LINES) -> str:
    line_count = max(1, int(lines or DEFAULT_VIEW_LOG_LINES))
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            cursor = handle.tell()
            if cursor <= 0:
                return ""
            newline_target = line_count + 1
            chunk_size = 64 * 1024
            newline_count = 0
            chunks: list[bytes] = []
            while cursor > 0 and newline_count < newline_target:
                read_size = min(chunk_size, cursor)
                cursor -= read_size
                handle.seek(cursor, os.SEEK_SET)
                chunk = handle.read(read_size)
                if not chunk:
                    break
                chunks.append(chunk)
                newline_count += chunk.count(b"\n")
        data = b"".join(reversed(chunks))
        text = data.decode("utf-8", errors="replace")
    except Exception:
        return ""
    rows = text.splitlines()
    if len(rows) <= line_count:
        return "\n".join(rows)
    return "\n".join(rows[-line_count:])


def _run_command(command: list[str], *, cwd: Optional[Path] = None, timeout_seconds: int = 20) -> tuple[bool, str, str, int]:
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd) if cwd is not None else None,
            capture_output=True,
            text=True,
            timeout=max(1, int(timeout_seconds)),
            env={**os.environ, "PYTHONUTF8": "1"},
            check=False,
        )
    except FileNotFoundError as exc:
        return False, "", str(exc), 127
    except subprocess.TimeoutExpired as exc:
        out = str(exc.stdout or "")
        err = str(exc.stderr or "")
        return False, out, f"command timeout after {timeout_seconds}s: {err}".strip(), 124
    except Exception as exc:
        return False, "", str(exc), 1
    return completed.returncode == 0, str(completed.stdout or ""), str(completed.stderr or ""), int(completed.returncode or 0)


def _compose_command_prefixes() -> list[list[str]]:
    prefixes: list[list[str]] = []
    has_docker = shutil.which("docker") is not None
    has_docker_compose = shutil.which("docker-compose") is not None
    if has_docker:
        prefixes.append(["docker", "compose"])
    if has_docker_compose:
        prefixes.append(["docker-compose"])
    return prefixes


def _run_aws_json(command: list[str], *, timeout_seconds: int = 60) -> tuple[bool, dict[str, Any], str]:
    ok, stdout, stderr, exit_code = _run_command(command, timeout_seconds=timeout_seconds)
    if not ok:
        return False, {}, stderr.strip() or f"aws command failed ({exit_code})"
    raw = str(stdout or "").strip() or "{}"
    try:
        parsed = json.loads(raw)
    except Exception as exc:
        return False, {}, f"aws returned non-JSON output: {raw[:300]} ({exc})"
    return True, (parsed if isinstance(parsed, dict) else {}), ""


def _run_aws_text(command: list[str], *, timeout_seconds: int = 60) -> tuple[bool, str, str]:
    ok, stdout, stderr, exit_code = _run_command(command, timeout_seconds=timeout_seconds)
    if not ok:
        return False, "", stderr.strip() or f"aws command failed ({exit_code})"
    return True, str(stdout or "").strip(), ""


def _extract_json_payload_from_text(raw: str) -> Any:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    for open_char, close_char in [("[", "]"), ("{", "}")]:
        start = text.find(open_char)
        end = text.rfind(close_char)
        if start < 0 or end <= start:
            continue
        candidate = text[start : end + 1]
        try:
            return json.loads(candidate)
        except Exception:
            continue
    return None


def _parse_compose_ps_json(raw: str) -> list[dict[str, Any]]:
    parsed = _extract_json_payload_from_text(raw)
    entries: list[Any]
    if isinstance(parsed, list):
        entries = parsed
    elif isinstance(parsed, dict):
        entries = [parsed]
    else:
        return []
    out: list[dict[str, Any]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "name": str(item.get("Name", "") or item.get("name", "") or ""),
                "service": str(item.get("Service", "") or item.get("service", "") or ""),
                "state": str(item.get("State", "") or item.get("state", "") or ""),
                "status": str(item.get("Status", "") or item.get("status", "") or ""),
                "image": str(item.get("Image", "") or item.get("image", "") or ""),
                "publishers": item.get("Publishers", item.get("publishers", [])),
            }
        )
    return out


def _load_docker_containers() -> dict[str, Any]:
    command_attempts: list[list[str]] = [["docker", "ps", "--no-trunc", "--format", "{{json .}}"]]
    error_messages: list[str] = []
    stdout = ""
    used_cmd: list[str] = command_attempts[0]
    for cmd in command_attempts:
        ok, candidate_stdout, stderr, exit_code = _run_command(cmd)
        if ok:
            stdout = candidate_stdout
            used_cmd = cmd
            break
        msg = stderr.strip() or f"docker ps failed with exit code {exit_code}"
        error_messages.append(f"{' '.join(cmd)} -> {msg}")
    else:
        return {
            "ok": False,
            "command": used_cmd,
            "error": " | ".join(error_messages) if error_messages else "docker ps failed",
            "containers": [],
        }
    containers: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            row = json.loads(raw)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        name = str(row.get("Names", "") or "")
        image = str(row.get("Image", "") or "")
        lowered = f"{name} {image}".lower()
        containers.append(
            {
                "id": str(row.get("ID", "") or ""),
                "name": name,
                "image": image,
                "command": str(row.get("Command", "") or ""),
                "created_at": str(row.get("CreatedAt", "") or ""),
                "running_for": str(row.get("RunningFor", "") or ""),
                "status": str(row.get("Status", "") or ""),
                "ports": str(row.get("Ports", "") or ""),
                "is_application": "nightmare" in lowered or "deploy-" in lowered,
            }
        )
    containers.sort(key=lambda item: str(item.get("name", "")).lower())
    return {
        "ok": True,
        "command": used_cmd,
        "error": "",
        "containers": containers,
    }


def _candidate_compose_specs(app_root: Path) -> list[tuple[Path, Optional[Path]]]:
    deploy_dir = app_root / "deploy"
    out: list[tuple[Path, Optional[Path]]] = []
    if not deploy_dir.is_dir():
        return out
    env_file = (deploy_dir / ".env").resolve() if (deploy_dir / ".env").is_file() else None
    for name in ("docker-compose.central.yml", "docker-compose.local.yml", "docker-compose.worker.yml", "docker-compose.log-store.yml"):
        compose_file = (deploy_dir / name).resolve()
        if compose_file.is_file():
            out.append((compose_file, env_file))
    return out


def _run_compose_ps(compose_file: Path, env_file: Optional[Path]) -> dict[str, Any]:
    variants: list[list[str]] = []
    compose_file_arg = str(compose_file)
    for prefix in _compose_command_prefixes():
        if env_file is not None:
            variants.append([*prefix, "-f", compose_file_arg, "--env-file", str(env_file), "ps", "--format", "json"])
        variants.append([*prefix, "-f", compose_file_arg, "ps", "--format", "json"])
    if not variants:
        return {
            "ok": False,
            "command": [],
            "compose_file": str(compose_file),
            "env_file": str(env_file) if env_file is not None else None,
            "rows": [],
            "error": "docker compose commands are not available in this runtime",
        }
    errors: list[str] = []
    for cmd in variants:
        ok, stdout, stderr, exit_code = _run_command(cmd, cwd=compose_file.parent)
        if not ok:
            msg = stderr.strip() or f"exit={exit_code}"
            errors.append(f"{' '.join(cmd)} -> {msg}")
            continue
        rows = _parse_compose_ps_json(stdout)
        rows.sort(key=lambda item: str(item.get("name", "")).lower())
        return {
            "ok": True,
            "command": cmd,
            "compose_file": str(compose_file),
            "env_file": str(env_file) if env_file is not None else None,
            "rows": rows,
            "error": "",
        }
    return {
        "ok": False,
        "command": variants[0] if variants else [],
        "compose_file": str(compose_file),
        "env_file": str(env_file) if env_file is not None else None,
        "rows": [],
        "error": " | ".join(errors) if errors else "docker-compose ps failed",
    }


def _worker_ssm_settings_from_env() -> dict[str, Any]:
    return {
        "enabled": shutil.which("aws") is not None,
        "region": str(os.getenv("AWS_REGION", "us-east-1") or "us-east-1").strip(),
        "target_key": str(os.getenv("COORDINATOR_WORKER_SSM_TARGET_KEY", os.getenv("SSM_TARGET_KEY", "tag:Name")) or "tag:Name").strip(),
        "target_values": str(
            os.getenv(
                "COORDINATOR_WORKER_SSM_TARGET_VALUES",
                os.getenv("SSM_TARGET_VALUES", "nightmare-worker*"),
            )
            or "nightmare-worker*"
        ).strip(),
        "timeout_seconds": max(
            15,
            _safe_int(
                os.getenv(
                    "COORDINATOR_WORKER_SSM_TIMEOUT_SECONDS",
                    os.getenv("SSM_TIMEOUT_SECONDS", "60"),
                ),
                60,
            ),
        ),
    }


def _run_ssm_shell(
    command_text: str,
    *,
    settings: dict[str, Any],
    instance_ids: Optional[list[str]] = None,
) -> dict[str, Any]:
    if not settings.get("enabled"):
        return {"ok": False, "error": "aws CLI is not available on this host", "invocations": []}
    region = str(settings.get("region", "us-east-1") or "us-east-1").strip()
    timeout_seconds = max(15, _safe_int(settings.get("timeout_seconds", 60), 60))
    send_cmd = [
        "aws",
        "ssm",
        "send-command",
        "--document-name",
        "AWS-RunShellScript",
        "--parameters",
        json.dumps({"commands": [str(command_text or "").strip()]}, ensure_ascii=False, separators=(",", ":")),
        "--comment",
        "nightmare server docker/log status",
        "--region",
        region,
        "--query",
        "Command.CommandId",
        "--output",
        "text",
    ]
    instance_list = [str(x or "").strip() for x in (instance_ids or []) if str(x or "").strip()]
    if instance_list:
        send_cmd.extend(["--instance-ids", *instance_list])
    else:
        target_key = str(settings.get("target_key", "tag:Name") or "tag:Name").strip()
        target_values = str(settings.get("target_values", "nightmare-worker*") or "nightmare-worker*").strip()
        send_cmd.extend(["--targets", f"Key={target_key},Values={target_values}"])
    ok, command_id, error = _run_aws_text(send_cmd, timeout_seconds=30)
    if not ok or not command_id:
        return {"ok": False, "error": error or "failed to start SSM command", "invocations": []}

    list_cmd = [
        "aws",
        "ssm",
        "list-command-invocations",
        "--command-id",
        str(command_id),
        "--details",
        "--region",
        region,
        "--output",
        "json",
    ]
    terminal = {
        "Success",
        "Cancelled",
        "TimedOut",
        "Failed",
        "Cancelling",
        "Undeliverable",
        "Terminated",
        "InvalidPlatform",
        "AccessDenied",
        "Delivery Timed Out",
        "Execution Timed Out",
    }
    invocations: list[dict[str, Any]] = []
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        list_ok, payload, list_error = _run_aws_json(list_cmd, timeout_seconds=30)
        if not list_ok:
            return {
                "ok": False,
                "error": list_error or "failed to list SSM invocations",
                "command_id": command_id,
                "invocations": [],
            }
        rows = payload.get("CommandInvocations", [])
        invocations = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        if invocations and all(str(row.get("Status", "")) in terminal for row in invocations):
            break
        time.sleep(2.0)
    return {
        "ok": True,
        "error": "",
        "command_id": command_id,
        "invocations": invocations,
    }


def _parse_docker_log_sections(raw: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current_name = ""
    for line in str(raw or "").splitlines():
        if line.startswith("###BEGIN_CONTAINER_LOG:"):
            current_name = line.split(":", 1)[1].strip()
            sections.setdefault(current_name, [])
            continue
        if line.startswith("###END_CONTAINER_LOG:"):
            current_name = ""
            continue
        if current_name:
            sections.setdefault(current_name, []).append(line)
    return {name: "\n".join(lines).strip() for name, lines in sections.items()}


def _parse_docker_ps_json_lines(raw: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in str(raw or "").splitlines():
        text = str(line or "").strip()
        if not text or not text.startswith("{") or not text.endswith("}"):
            continue
        try:
            row = json.loads(text)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        name = str(row.get("Names", "") or "")
        image = str(row.get("Image", "") or "")
        lowered = f"{name} {image}".lower()
        out.append(
            {
                "id": str(row.get("ID", "") or ""),
                "name": name,
                "image": image,
                "command": str(row.get("Command", "") or ""),
                "created_at": str(row.get("CreatedAt", "") or ""),
                "running_for": str(row.get("RunningFor", "") or ""),
                "status": str(row.get("Status", "") or ""),
                "ports": str(row.get("Ports", "") or ""),
                "is_application": "nightmare" in lowered or "deploy-" in lowered,
            }
        )
    return out


def _list_worker_vm_docker_containers(*, log_lines: int = 0, timeout_seconds_override: Optional[int] = None) -> dict[str, Any]:
    settings = dict(_worker_ssm_settings_from_env())
    if timeout_seconds_override is not None:
        settings["timeout_seconds"] = max(10, int(timeout_seconds_override))
    ps_cmd = (
        "if docker compose version >/dev/null 2>&1; then COMPOSE_CMD='docker compose'; "
        "elif command -v docker-compose >/dev/null 2>&1; then COMPOSE_CMD='docker-compose'; "
        "else echo '[]'; exit 0; fi; "
        "cd /opt/nightmare/deploy && $COMPOSE_CMD -f docker-compose.worker.yml --env-file .env ps --format json || echo '[]'"
    )
    ps_result = _run_ssm_shell(ps_cmd, settings=settings)
    if not ps_result.get("ok"):
        return {
            "ok": False,
            "error": ps_result.get("error", "SSM worker query failed"),
            "command_id": ps_result.get("command_id"),
            "instances": [],
            "containers": [],
        }
    instances: list[dict[str, Any]] = []
    containers: list[dict[str, Any]] = []
    for inv in ps_result.get("invocations", []):
        if not isinstance(inv, dict):
            continue
        instance_id = str(inv.get("InstanceId", "") or "").strip()
        status = str(inv.get("Status", "") or "").strip()
        output = ""
        plugin_rows = inv.get("CommandPlugins", [])
        if isinstance(plugin_rows, list) and plugin_rows and isinstance(plugin_rows[0], dict):
            output = str(plugin_rows[0].get("Output", "") or "")
        rows = _parse_compose_ps_json(output)
        instances.append(
            {
                "instance_id": instance_id,
                "status": status,
                "container_count": len(rows),
            }
        )
        for row in rows:
            containers.append(
                {
                    **row,
                    "host_type": "worker_vm",
                    "host_label": instance_id,
                    "instance_id": instance_id,
                    "is_application": True,
                }
            )
    if log_lines > 0 and containers:
        log_cmd = (
            f"for n in $(docker ps --format '{{{{.Names}}}}'); do "
            f"echo \"###BEGIN_CONTAINER_LOG:$n\"; "
            f"docker logs --tail {max(1, int(log_lines))} \"$n\" 2>&1 || true; "
            f"echo \"###END_CONTAINER_LOG:$n\"; done"
        )
        log_result = _run_ssm_shell(log_cmd, settings=settings)
        if log_result.get("ok"):
            logs_by_instance: dict[str, dict[str, str]] = {}
            for inv in log_result.get("invocations", []):
                if not isinstance(inv, dict):
                    continue
                instance_id = str(inv.get("InstanceId", "") or "").strip()
                output = ""
                plugin_rows = inv.get("CommandPlugins", [])
                if isinstance(plugin_rows, list) and plugin_rows and isinstance(plugin_rows[0], dict):
                    output = str(plugin_rows[0].get("Output", "") or "")
                logs_by_instance[instance_id] = _parse_docker_log_sections(output)
            for row in containers:
                iid = str(row.get("instance_id", "") or "")
                name = str(row.get("name", "") or "")
                row["log_tail"] = str(logs_by_instance.get(iid, {}).get(name, "") or "")
    return {
        "ok": True,
        "error": "",
        "command_id": ps_result.get("command_id"),
        "instances": instances,
        "containers": containers,
        "settings": {
            "region": settings.get("region"),
            "target_key": settings.get("target_key"),
            "target_values": settings.get("target_values"),
        },
    }


def _list_remote_ec2_docker_containers(*, log_lines: int = 0, timeout_seconds_override: Optional[int] = None) -> dict[str, Any]:
    settings = dict(_worker_ssm_settings_from_env())
    if timeout_seconds_override is not None:
        settings["timeout_seconds"] = max(10, int(timeout_seconds_override))
    ec2_payload = _list_ec2_fleet_instances()
    if not ec2_payload.get("ok"):
        return {"ok": False, "error": str(ec2_payload.get("error", "EC2 discovery failed") or "EC2 discovery failed"), "instances": [], "containers": []}
    instances = ec2_payload.get("instances", []) if isinstance(ec2_payload.get("instances"), list) else []
    instance_ids = [str(item.get("instance_id", "") or "").strip() for item in instances if isinstance(item, dict) and str(item.get("instance_id", "") or "").strip()]
    if not instance_ids:
        return {"ok": True, "error": "", "instances": [], "containers": []}

    instance_name_map: dict[str, str] = {}
    for item in instances:
        if not isinstance(item, dict):
            continue
        iid = str(item.get("instance_id", "") or "").strip()
        if not iid:
            continue
        instance_name_map[iid] = str(item.get("name", "") or "").strip()

    ps_cmd = "docker ps --no-trunc --format '{{json .}}' || true"
    ps_result = _run_ssm_shell(ps_cmd, settings=settings, instance_ids=instance_ids)
    if not ps_result.get("ok"):
        return {"ok": False, "error": ps_result.get("error", "SSM docker ps failed"), "instances": [], "containers": []}

    instance_rows: list[dict[str, Any]] = []
    containers: list[dict[str, Any]] = []
    for inv in ps_result.get("invocations", []):
        if not isinstance(inv, dict):
            continue
        instance_id = str(inv.get("InstanceId", "") or "").strip()
        status = str(inv.get("Status", "") or "").strip()
        output = ""
        plugin_rows = inv.get("CommandPlugins", [])
        if isinstance(plugin_rows, list) and plugin_rows and isinstance(plugin_rows[0], dict):
            output = str(plugin_rows[0].get("Output", "") or "")
        rows = _parse_docker_ps_json_lines(output)
        instance_rows.append({"instance_id": instance_id, "status": status, "container_count": len(rows)})
        for row in rows:
            containers.append(
                {
                    **row,
                    "host_type": "remote_vm",
                    "host_label": instance_name_map.get(instance_id) or instance_id,
                    "instance_id": instance_id,
                }
            )

    if log_lines > 0 and containers:
        log_cmd = (
            f"for n in $(docker ps --format '{{{{.Names}}}}'); do "
            f"echo \"###BEGIN_CONTAINER_LOG:$n\"; "
            f"docker logs --tail {max(1, int(log_lines))} \"$n\" 2>&1 || true; "
            f"echo \"###END_CONTAINER_LOG:$n\"; done"
        )
        log_result = _run_ssm_shell(log_cmd, settings=settings, instance_ids=instance_ids)
        if log_result.get("ok"):
            logs_by_instance: dict[str, dict[str, str]] = {}
            for inv in log_result.get("invocations", []):
                if not isinstance(inv, dict):
                    continue
                instance_id = str(inv.get("InstanceId", "") or "").strip()
                output = ""
                plugin_rows = inv.get("CommandPlugins", [])
                if isinstance(plugin_rows, list) and plugin_rows and isinstance(plugin_rows[0], dict):
                    output = str(plugin_rows[0].get("Output", "") or "")
                logs_by_instance[instance_id] = _parse_docker_log_sections(output)
            for row in containers:
                iid = str(row.get("instance_id", "") or "")
                name = str(row.get("name", "") or "")
                row["log_tail"] = str(logs_by_instance.get(iid, {}).get(name, "") or "")

    return {"ok": True, "error": "", "instances": instance_rows, "containers": containers}


def _ec2_log_settings_from_env() -> dict[str, Any]:
    return {
        "enabled": shutil.which("aws") is not None,
        "region": str(os.getenv("AWS_REGION", "us-east-1") or "us-east-1").strip(),
        "filter_key": str(
            os.getenv(
                "COORDINATOR_LOG_EC2_FILTER_KEY",
                os.getenv("COORDINATOR_WORKER_SSM_TARGET_KEY", "tag:Name"),
            )
            or "tag:Name"
        ).strip(),
        "filter_values": str(
            os.getenv(
                "COORDINATOR_LOG_EC2_FILTER_VALUES",
                "nightmare-worker*,nightmare-central*,nightmare-log-db*",
            )
            or "nightmare-worker*,nightmare-central*,nightmare-log-db*"
        ).strip(),
    }


def _list_ec2_fleet_instances() -> dict[str, Any]:
    settings = _ec2_log_settings_from_env()
    if not settings.get("enabled"):
        return {"ok": False, "error": "aws CLI is not available on this host", "instances": []}
    region = str(settings.get("region", "us-east-1") or "us-east-1").strip()
    filter_key = str(settings.get("filter_key", "tag:Name") or "tag:Name").strip()
    filter_values = str(settings.get("filter_values", "nightmare-worker*,nightmare-central*") or "").strip()
    cmd = [
        "aws",
        "ec2",
        "describe-instances",
        "--region",
        region,
        "--filters",
        "Name=instance-state-name,Values=pending,running,stopping,stopped",
        f"Name={filter_key},Values={filter_values}",
        "--output",
        "json",
    ]
    ok, payload, error = _run_aws_json(cmd, timeout_seconds=30)
    if not ok:
        return {"ok": False, "error": error, "instances": []}
    instances: list[dict[str, Any]] = []
    for reservation in payload.get("Reservations", []) if isinstance(payload.get("Reservations"), list) else []:
        if not isinstance(reservation, dict):
            continue
        for item in reservation.get("Instances", []) if isinstance(reservation.get("Instances"), list) else []:
            if not isinstance(item, dict):
                continue
            tags = item.get("Tags", []) if isinstance(item.get("Tags"), list) else []
            name = ""
            for tag in tags:
                if isinstance(tag, dict) and str(tag.get("Key", "")).strip() == "Name":
                    name = str(tag.get("Value", "") or "").strip()
                    break
            instances.append(
                {
                    "instance_id": str(item.get("InstanceId", "") or "").strip(),
                    "state": str((item.get("State") or {}).get("Name", "") if isinstance(item.get("State"), dict) else ""),
                    "name": name,
                    "private_ip": str(item.get("PrivateIpAddress", "") or ""),
                    "public_ip": str(item.get("PublicIpAddress", "") or ""),
                }
            )
    instances = [row for row in instances if row.get("instance_id")]
    instances.sort(key=lambda row: (str(row.get("name", "")).lower(), str(row.get("instance_id", "")).lower()))
    return {"ok": True, "error": "", "instances": instances}


def _read_ec2_console_output(instance_id: str) -> tuple[bool, str, str]:
    iid = str(instance_id or "").strip()
    if not iid:
        return False, "", "instance_id is required"
    settings = _ec2_log_settings_from_env()
    if not settings.get("enabled"):
        return False, "", "aws CLI is not available on this host"
    region = str(settings.get("region", "us-east-1") or "us-east-1").strip()
    cmd = [
        "aws",
        "ec2",
        "get-console-output",
        "--instance-id",
        iid,
        "--latest",
        "--region",
        region,
        "--output",
        "text",
    ]
    ok, stdout, error = _run_aws_text(cmd, timeout_seconds=30)
    if not ok:
        return False, "", error
    return True, str(stdout or ""), ""


def _format_est_datetime(dt: datetime) -> str:
    try:
        return dt.astimezone(EST_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _format_est_time(dt: datetime) -> str:
    try:
        return dt.astimezone(EST_TZ).strftime("%H:%M:%S")
    except Exception:
        return ""


def _parse_timestamp(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_severity(value: Any) -> str:
    sev = str(value or "").strip().lower()
    if sev in {"debug", "info", "warning", "error", "critical"}:
        return sev
    if sev in {"warn"}:
        return "warning"
    if sev in {"fatal"}:
        return "critical"
    return "info"


def _parse_log_events_from_text(text: str, *, source: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    source_id = str(source.get("source_id", "") or "")
    source_type = str(source.get("source_type", "") or "unknown")
    machine = str(
        source.get("instance_id")
        or source.get("host_label")
        or source.get("label")
        or source_id
        or "unknown"
    )
    for raw_line in str(text or "").splitlines():
        line = str(raw_line or "")
        if not line.strip():
            continue
        payload: dict[str, Any] = {}
        json_line = line.strip()
        if json_line.startswith("{") and json_line.endswith("}"):
            try:
                parsed = json.loads(json_line)
                if isinstance(parsed, dict):
                    payload = parsed
            except Exception:
                payload = {}
        ts = (
            _parse_timestamp(payload.get("timestamp"))
            or _parse_timestamp(payload.get("time"))
            or _parse_timestamp(payload.get("ts"))
            or datetime.now(timezone.utc)
        )
        severity = _normalize_severity(payload.get("level") or payload.get("severity") or payload.get("log_level"))
        description = str(
            payload.get("event")
            or payload.get("message")
            or payload.get("msg")
            or payload.get("description")
            or line
        )
        out.append(
            {
                "event_time_utc": ts.isoformat(),
                "event_time_est": _format_est_datetime(ts),
                "severity": severity,
                "description": description,
                "machine": str(payload.get("machine") or payload.get("host") or payload.get("worker_id") or machine),
                "source_id": source_id,
                "source_type": source_type,
                "program_name": str(payload.get("program_name") or payload.get("program") or ""),
                "component_name": str(payload.get("component_name") or payload.get("component") or ""),
                "class_name": str(payload.get("class_name") or payload.get("class") or ""),
                "function_name": str(payload.get("function_name") or payload.get("function") or ""),
                "exception_type": str(payload.get("exception_type") or payload.get("exc_type") or ""),
                "stacktrace": str(payload.get("stacktrace") or payload.get("exc_info") or ""),
                "raw_line": line,
            }
        )
    return out


def _filter_and_sort_log_events(
    events: list[dict[str, Any]],
    *,
    search: str = "",
    severity: str = "",
    machine: str = "",
    sort_key: str = "event_time_utc",
    sort_dir: str = "desc",
    offset: int = 0,
    limit: int = 500,
) -> dict[str, Any]:
    needle = str(search or "").strip().lower()
    sev = _normalize_severity(severity) if str(severity or "").strip() else ""
    machine_q = str(machine or "").strip().lower()
    filtered: list[dict[str, Any]] = []
    for item in events:
        if not isinstance(item, dict):
            continue
        if sev and str(item.get("severity", "") or "").lower() != sev:
            continue
        if machine_q and machine_q not in str(item.get("machine", "") or "").lower():
            continue
        if needle:
            hay = " ".join(
                [
                    str(item.get("event_time_est", "") or ""),
                    str(item.get("severity", "") or ""),
                    str(item.get("description", "") or ""),
                    str(item.get("machine", "") or ""),
                    str(item.get("raw_line", "") or ""),
                ]
            ).lower()
            if needle not in hay:
                continue
        filtered.append(item)
    total = len(filtered)
    key = sort_key if sort_key in {"event_time_utc", "severity", "machine", "description"} else "event_time_utc"
    reverse = str(sort_dir or "").strip().lower() != "asc"
    filtered.sort(key=lambda item: str(item.get(key, "") or "").lower(), reverse=reverse)
    start = max(0, int(offset or 0))
    end = start + max(1, int(limit or 500))
    return {"total": total, "events": filtered[start:end], "offset": start, "limit": max(1, int(limit or 500))}


def _collect_docker_status(app_root: Path, *, include_logs: bool = False, log_lines: int = 120) -> dict[str, Any]:
    docker = _load_docker_containers()
    compose_specs = _candidate_compose_specs(app_root)
    compose: list[dict[str, Any]] = [_run_compose_ps(compose_file, env_file) for compose_file, env_file in compose_specs]
    local_containers = docker.get("containers", []) if isinstance(docker.get("containers"), list) else []
    for row in local_containers:
        row["host_type"] = "central_server"
        row["host_label"] = "central_server"
    if include_logs:
        for row in local_containers:
            name = str(row.get("name", "") or "").strip()
            ok, tail, error = _read_docker_log_tail(name, max(1, int(log_lines)), app_root=app_root)
            row["log_tail"] = tail if ok else ""
            if not ok:
                row["log_error"] = error

    workers = _list_worker_vm_docker_containers(log_lines=(max(1, int(log_lines)) if include_logs else 0))
    worker_containers = workers.get("containers", []) if isinstance(workers.get("containers"), list) else []
    all_containers = [*local_containers, *worker_containers]
    app_containers = [item for item in all_containers if bool(item.get("is_application"))]
    ec2_instances = _list_ec2_fleet_instances()
    return {
        "generated_at_utc": _iso_now(),
        "docker": docker,
        "compose": compose,
        "workers": workers,
        "ec2_instances": ec2_instances,
        "all_containers": all_containers,
        "summary": {
            "container_count": len(all_containers),
            "application_container_count": len(app_containers),
            "central_container_count": len(local_containers),
            "worker_vm_container_count": len(worker_containers),
            "compose_files_checked": len(compose),
            "compose_files_ok": sum(1 for item in compose if item.get("ok")),
            "worker_vm_count": len(workers.get("instances", [])) if isinstance(workers.get("instances"), list) else 0,
        },
        "application_containers": app_containers,
    }


def _iter_log_files_limited(root: Path, *, max_depth: int, max_files: int) -> list[Path]:
    resolved_root = root.resolve()
    if not resolved_root.is_dir():
        return []
    out: list[Path] = []
    root_depth = len(resolved_root.parts)
    for dirpath, _dirnames, filenames in os.walk(resolved_root):
        current_path = Path(dirpath)
        current_depth = len(current_path.parts) - root_depth
        if current_depth > max_depth:
            continue
        for filename in filenames:
            if not str(filename or "").lower().endswith(".log"):
                continue
            full_path = (current_path / filename).resolve()
            if not full_path.is_file():
                continue
            out.append(full_path)
            if len(out) >= max_files:
                return out
    return out


def _discover_view_log_file_sources(app_root: Path, output_root: Path) -> list[dict[str, Any]]:
    candidates: list[Path] = []
    seen: set[str] = set()
    app_root_resolved = app_root.resolve()
    output_root_resolved = output_root.resolve()
    roots_with_depth = [
        (output_root_resolved, 8),
        (app_root_resolved / "output", 8),
        (app_root_resolved / "logs", 8),
        (app_root_resolved / "deploy", 6),
        (app_root_resolved, 4),
    ]
    for root, max_depth in roots_with_depth:
        if len(candidates) >= MAX_VIEW_LOG_FILE_SOURCES:
            break
        remaining = max(1, MAX_VIEW_LOG_FILE_SOURCES - len(candidates))
        for path in _iter_log_files_limited(root, max_depth=max_depth, max_files=remaining):
            key = str(path).lower()
            if key in seen:
                continue
            seen.add(key)
            candidates.append(path)
            if len(candidates) >= MAX_VIEW_LOG_FILE_SOURCES:
                break
    candidates.sort(key=lambda p: str(p).lower())
    out: list[dict[str, Any]] = []
    for path in candidates:
        rel = _to_repo_relative_path(path)
        if not rel:
            continue
        out.append(
            {
                "source_id": f"file:{rel}",
                "source_type": "file",
                "system": "filesystem",
                "label": path.name,
                "relative_path": rel,
                "size_bytes": int(path.stat().st_size if path.exists() else 0),
                "updated_at_utc": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat() if path.exists() else "",
            }
        )
    return out


def _discover_view_log_docker_sources() -> list[dict[str, Any]]:
    if shutil.which("docker") is None and not _compose_command_prefixes():
        return []
    docker = _load_docker_containers()
    rows = docker.get("containers", []) if isinstance(docker.get("containers"), list) else []
    out: list[dict[str, Any]] = []
    for row in rows:
        container_name = str(row.get("name", "") or "").strip()
        if not container_name:
            continue
        out.append(
            {
                "source_id": f"docker:{container_name}",
                "source_type": "docker",
                "system": "docker",
                "label": container_name,
                "container_name": container_name,
                "status": str(row.get("status", "") or ""),
                "image": str(row.get("image", "") or ""),
                "is_application": bool(row.get("is_application")),
            }
        )
    out.sort(key=lambda item: str(item.get("label", "")).lower())
    return out


def _discover_view_log_remote_docker_sources() -> list[dict[str, Any]]:
    remote = _list_remote_ec2_docker_containers(log_lines=0, timeout_seconds_override=20)
    rows = remote.get("containers", []) if isinstance(remote.get("containers"), list) else []
    out: list[dict[str, Any]] = []
    for row in rows:
        container_name = str(row.get("name", "") or "").strip()
        instance_id = str(row.get("instance_id", "") or "").strip()
        if not container_name or not instance_id:
            continue
        out.append(
            {
                "source_id": f"ssm:{instance_id}:docker:{container_name}",
                "source_type": "docker",
                "system": "remote_vm",
                "label": f"{str(row.get('host_label', '') or instance_id)}:{container_name}",
                "container_name": container_name,
                "instance_id": instance_id,
                "status": str(row.get("status", "") or ""),
                "image": str(row.get("image", "") or ""),
                "is_application": bool(row.get("is_application", False)),
            }
        )
    out.sort(key=lambda item: str(item.get("label", "")).lower())
    return out


def _discover_view_log_ec2_console_sources() -> list[dict[str, Any]]:
    payload = _list_ec2_fleet_instances()
    rows = payload.get("instances", []) if isinstance(payload.get("instances"), list) else []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        instance_id = str(row.get("instance_id", "") or "").strip()
        if not instance_id:
            continue
        label = str(row.get("name", "") or "").strip() or instance_id
        state = str(row.get("state", "") or "").strip()
        out.append(
            {
                "source_id": f"ec2-console:{instance_id}",
                "source_type": "ec2_console",
                "system": "aws_ec2",
                "label": f"{label} ({instance_id})",
                "instance_id": instance_id,
                "status": state,
                "is_application": True,
            }
        )
    out.sort(key=lambda item: str(item.get("label", "")).lower())
    return out


_view_log_sources_cache_lock = threading.Lock()
_view_log_sources_cache: dict[str, Any] = {
    "expires_at_epoch": 0.0,
    "cache_key": "",
    "sources": [],
}


def _collect_view_log_sources(app_root: Path, output_root: Path) -> list[dict[str, Any]]:
    sources = (
        _discover_view_log_docker_sources()
        + _discover_view_log_remote_docker_sources()
        + _discover_view_log_ec2_console_sources()
        + _discover_view_log_file_sources(app_root, output_root)
    )
    if shutil.which("docker") is not None or _compose_command_prefixes():
        must_have = [
            ("nightmare-coordinator-server", True),
            ("nightmare-postgres", True),
            ("nightmare-log-postgres", False),
        ]
        existing_ids = {str(item.get("source_id", "") or "").strip() for item in sources if isinstance(item, dict)}
        for container_name, app_flag in must_have:
            sid = f"docker:{container_name}"
            if sid in existing_ids:
                continue
            sources.append(
                {
                    "source_id": sid,
                    "source_type": "docker",
                    "system": "docker",
                    "label": container_name,
                    "container_name": container_name,
                    "status": "unknown",
                    "image": "",
                    "is_application": app_flag,
                }
            )
    sources.sort(key=lambda item: (str(item.get("source_type", "")), str(item.get("label", "")).lower()))
    return sources


def _collect_view_log_sources_cached(
    app_root: Path,
    output_root: Path,
    *,
    force_refresh: bool = False,
    cache_ttl_seconds: int = VIEW_LOG_SOURCES_CACHE_TTL_SECONDS,
) -> list[dict[str, Any]]:
    now = time.time()
    resolved_app = str(app_root.resolve())
    resolved_output = str(output_root.resolve())
    cache_key = f"{resolved_app}|{resolved_output}"
    ttl = max(1, int(cache_ttl_seconds or VIEW_LOG_SOURCES_CACHE_TTL_SECONDS))
    if not force_refresh:
        with _view_log_sources_cache_lock:
            current_key = str(_view_log_sources_cache.get("cache_key", "") or "")
            expires_at = float(_view_log_sources_cache.get("expires_at_epoch", 0.0) or 0.0)
            cached_sources = _view_log_sources_cache.get("sources")
            if (
                current_key == cache_key
                and now < expires_at
                and isinstance(cached_sources, list)
            ):
                return [dict(item) for item in cached_sources if isinstance(item, dict)]
    sources = _collect_view_log_sources(app_root, output_root)
    with _view_log_sources_cache_lock:
        _view_log_sources_cache["cache_key"] = cache_key
        _view_log_sources_cache["expires_at_epoch"] = now + ttl
        _view_log_sources_cache["sources"] = [dict(item) for item in sources if isinstance(item, dict)]
    return sources


def _diagnose_view_log_sources(
    app_root: Path,
    output_root: Path,
    *,
    lines: int = 20,
    force_refresh: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    safe_lines = max(1, min(200, int(lines or 20)))
    safe_limit = max(1, min(500, int(limit or 200)))
    sources = _collect_view_log_sources_cached(app_root, output_root, force_refresh=force_refresh)
    diagnostics: list[dict[str, Any]] = []
    ok_count = 0
    error_count = 0
    for source in sources[:safe_limit]:
        checked_at = datetime.now(timezone.utc)
        source_id = str(source.get("source_id", "") or "")
        label = str(source.get("label", "") or source_id)
        source_type = str(source.get("source_type", "") or "")
        system = str(source.get("system", "") or "")
        ok, text, error = _read_log_source_text(source, lines=safe_lines, full=False, app_root=app_root)
        if ok:
            ok_count += 1
        else:
            error_count += 1
        line_count = len(str(text or "").splitlines()) if ok else 0
        preview = ""
        if ok and text:
            preview = "\n".join(str(text).splitlines()[:3])[:280]
        diagnostics.append(
            {
                "source_id": source_id,
                "label": label,
                "source_type": source_type,
                "system": system,
                "checked_at_utc": checked_at.isoformat(),
                "checked_at_est_time": _format_est_time(checked_at),
                "instance_id": str(source.get("instance_id", "") or ""),
                "container_name": str(source.get("container_name", "") or ""),
                "ok": bool(ok),
                "status": "ok" if ok else "error",
                "error": str(error or ""),
                "line_count": int(line_count),
                "preview": preview,
            }
        )
    diagnostics.sort(key=lambda row: (0 if not row.get("ok") else 1, str(row.get("label", "")).lower()))
    return {
        "generated_at_utc": _iso_now(),
        "total_sources": len(sources),
        "diagnosed_sources": len(diagnostics),
        "ok_sources": ok_count,
        "error_sources": error_count,
        "rows": diagnostics,
    }


def _resolve_view_log_source(source_id: str, app_root: Path, output_root: Path) -> Optional[dict[str, Any]]:
    sid = str(source_id or "").strip()
    if not sid:
        return None
    if sid.startswith("ssm:") and ":docker:" in sid:
        parts = sid.split(":", 3)
        if len(parts) == 4:
            instance_id = str(parts[1] or "").strip()
            container_name = str(parts[3] or "").strip()
            if instance_id and container_name:
                return {
                    "source_id": sid,
                    "source_type": "docker",
                    "system": "worker_vm",
                    "label": f"{instance_id}:{container_name}",
                    "container_name": container_name,
                    "instance_id": instance_id,
                    "is_application": True,
                }
    if sid.startswith("ec2-console:"):
        instance_id = sid.split(":", 1)[1].strip()
        if instance_id:
            return {
                "source_id": sid,
                "source_type": "ec2_console",
                "system": "aws_ec2",
                "label": instance_id,
                "instance_id": instance_id,
            }
    for source in _collect_view_log_sources_cached(app_root, output_root):
        if str(source.get("source_id", "")).strip() == sid:
            return source
    return None


def _compose_log_candidates_for_container(app_root: Path, container_name: str) -> list[tuple[Path, Optional[Path], str]]:
    name = str(container_name or "").strip()
    if not name:
        return []
    normalized = name.lower()
    spec_map: dict[str, tuple[Path, Optional[Path]]] = {
        compose_file.name.lower(): (compose_file, env_file)
        for compose_file, env_file in _candidate_compose_specs(app_root)
    }
    out: list[tuple[Path, Optional[Path], str]] = []

    def add(file_name: str, service: str) -> None:
        spec = spec_map.get(file_name.lower())
        if spec is not None:
            out.append((spec[0], spec[1], service))

    if normalized == "nightmare-coordinator-server":
        add("docker-compose.central.yml", "server")
        add("docker-compose.local.yml", "server")
    elif normalized == "nightmare-postgres":
        add("docker-compose.central.yml", "postgres")
        add("docker-compose.local.yml", "postgres")
    elif normalized == "nightmare-log-postgres":
        add("docker-compose.log-store.yml", "log_postgres")

    if out:
        return out

    guessed_service = normalized
    default_name_match = re.match(r"^[^-]+-([a-z0-9_]+)-\d+$", normalized)
    if default_name_match:
        guessed_service = str(default_name_match.group(1) or normalized).strip() or normalized
    for compose_file, env_file in _candidate_compose_specs(app_root):
        out.append((compose_file, env_file, guessed_service))
    return out


def _run_compose_logs_for_service(
    compose_file: Path,
    env_file: Optional[Path],
    service: str,
    *,
    lines: int,
    full: bool,
) -> tuple[bool, str, str]:
    variants: list[list[str]] = []
    for prefix in _compose_command_prefixes():
        base = [*prefix, "-f", str(compose_file)]
        cmd_without_env = [*base, "logs", "--no-color"]
        if not full:
            cmd_without_env.extend(["--tail", str(max(1, int(lines or DEFAULT_VIEW_LOG_LINES)))])
        cmd_without_env.append(service)
        variants.append(cmd_without_env)

        if env_file is not None:
            cmd_with_env = [*base, "--env-file", str(env_file), "logs", "--no-color"]
            if not full:
                cmd_with_env.extend(["--tail", str(max(1, int(lines or DEFAULT_VIEW_LOG_LINES)))])
            cmd_with_env.append(service)
            variants.insert(0, cmd_with_env)

    if not variants:
        return False, "", "docker compose commands are not available in this runtime"

    errors: list[str] = []
    timeout_seconds = 60 if full else 30
    for cmd in variants:
        ok, stdout, stderr, exit_code = _run_command(cmd, cwd=compose_file.parent, timeout_seconds=timeout_seconds)
        if not ok:
            msg = stderr.strip() or f"exit={exit_code}"
            errors.append(f"{' '.join(cmd)} -> {msg}")
            continue
        text = "\n".join([part for part in [stdout.strip(), stderr.strip()] if part]).strip()
        return True, text, ""
    return False, "", " | ".join(errors) if errors else "compose logs command failed"


def _read_docker_log_tail(container_name: str, lines: int, *, app_root: Optional[Path] = None) -> tuple[bool, str, str]:
    container = str(container_name or "").strip()
    cmd = ["docker", "logs", "--tail", str(max(1, int(lines or DEFAULT_VIEW_LOG_LINES))), container]
    ok, stdout, stderr, exit_code = _run_command(cmd, timeout_seconds=30)
    if ok:
        # docker logs often writes output to stderr.
        text = "\n".join([part for part in [stdout.strip(), stderr.strip()] if part]).strip()
        return True, text, ""
    docker_error = stderr.strip() or f"docker logs failed with exit code {exit_code}"
    errors: list[str] = [docker_error]

    root = (app_root or BASE_DIR).resolve()
    for compose_file, env_file, service in _compose_log_candidates_for_container(root, container):
        c_ok, c_text, c_error = _run_compose_logs_for_service(
            compose_file,
            env_file,
            service,
            lines=lines,
            full=False,
        )
        if c_ok:
            return True, c_text, ""
        if c_error:
            errors.append(c_error)
    return False, "", " | ".join([e for e in errors if str(e).strip()])


def _read_docker_log_full(container_name: str, *, app_root: Optional[Path] = None) -> tuple[bool, str, str]:
    container = str(container_name or "").strip()
    cmd = ["docker", "logs", container]
    ok, stdout, stderr, exit_code = _run_command(cmd, timeout_seconds=60)
    if ok:
        text = "\n".join([part for part in [stdout.strip(), stderr.strip()] if part]).strip()
        return True, text, ""
    docker_error = stderr.strip() or f"docker logs failed with exit code {exit_code}"
    errors: list[str] = [docker_error]

    root = (app_root or BASE_DIR).resolve()
    for compose_file, env_file, service in _compose_log_candidates_for_container(root, container):
        c_ok, c_text, c_error = _run_compose_logs_for_service(
            compose_file,
            env_file,
            service,
            lines=DEFAULT_VIEW_LOG_LINES,
            full=True,
        )
        if c_ok:
            return True, c_text, ""
        if c_error:
            errors.append(c_error)
    return False, "", " | ".join([e for e in errors if str(e).strip()])


def _read_worker_vm_docker_log_tail(instance_id: str, container_name: str, lines: int) -> tuple[bool, str, str]:
    iid = str(instance_id or "").strip()
    name = str(container_name or "").strip()
    if not iid or not name:
        return False, "", "instance_id and container_name are required"
    settings = _worker_ssm_settings_from_env()
    log_cmd = f"docker logs --tail {max(1, int(lines or DEFAULT_VIEW_LOG_LINES))} {shlex.quote(name)} 2>&1 || true"
    result = _run_ssm_shell(log_cmd, settings=settings, instance_ids=[iid])
    if not result.get("ok"):
        return False, "", str(result.get("error", "SSM log command failed") or "SSM log command failed")
    invocations = result.get("invocations", [])
    if not isinstance(invocations, list) or not invocations:
        return False, "", "no SSM invocation result received"
    inv = invocations[0] if isinstance(invocations[0], dict) else {}
    plugin_rows = inv.get("CommandPlugins", [])
    output = ""
    if isinstance(plugin_rows, list) and plugin_rows and isinstance(plugin_rows[0], dict):
        output = str(plugin_rows[0].get("Output", "") or "")
    return True, output.strip(), ""


def _read_worker_vm_docker_log_full(instance_id: str, container_name: str) -> tuple[bool, str, str]:
    iid = str(instance_id or "").strip()
    name = str(container_name or "").strip()
    if not iid or not name:
        return False, "", "instance_id and container_name are required"
    settings = _worker_ssm_settings_from_env()
    log_cmd = f"docker logs {shlex.quote(name)} 2>&1 || true"
    result = _run_ssm_shell(log_cmd, settings=settings, instance_ids=[iid])
    if not result.get("ok"):
        return False, "", str(result.get("error", "SSM log command failed") or "SSM log command failed")
    invocations = result.get("invocations", [])
    if not isinstance(invocations, list) or not invocations:
        return False, "", "no SSM invocation result received"
    inv = invocations[0] if isinstance(invocations[0], dict) else {}
    plugin_rows = inv.get("CommandPlugins", [])
    output = ""
    if isinstance(plugin_rows, list) and plugin_rows and isinstance(plugin_rows[0], dict):
        output = str(plugin_rows[0].get("Output", "") or "")
    return True, output.strip(), ""


def _read_log_source_text(
    source: dict[str, Any],
    *,
    lines: int,
    full: bool = False,
    app_root: Optional[Path] = None,
) -> tuple[bool, str, str]:
    source_type = str(source.get("source_type", "") or "").strip().lower()
    if source_type == "docker":
        system = str(source.get("system", "") or "").strip().lower()
        container_name = str(source.get("container_name", "") or "").strip()
        if system in {"worker_vm", "remote_vm"}:
            instance_id = str(source.get("instance_id", "") or "").strip()
            if full:
                return _read_worker_vm_docker_log_full(instance_id, container_name)
            return _read_worker_vm_docker_log_tail(instance_id, container_name, lines)
        if full:
            return _read_docker_log_full(container_name, app_root=app_root)
        return _read_docker_log_tail(container_name, lines, app_root=app_root)
    if source_type == "ec2_console":
        instance_id = str(source.get("instance_id", "") or "").strip()
        ok, text, error = _read_ec2_console_output(instance_id)
        if not ok:
            return False, "", error
        if full:
            return True, text, ""
        return True, "\n".join(str(text or "").splitlines()[-max(1, int(lines or DEFAULT_VIEW_LOG_LINES)):]), ""
    rel = str(source.get("relative_path", "") or "")
    root = app_root or BASE_DIR
    resolved = _normalize_and_validate_relative_path(root, rel)
    if resolved is None or not resolved.is_file():
        return False, "", "file log source no longer exists"
    if full:
        try:
            with resolved.open("rb") as handle:
                data = handle.read()
            return True, data.decode("utf-8", errors="replace"), ""
        except Exception as exc:
            return False, "", str(exc)
    return True, _read_tail_lines(resolved, lines=lines), ""


def collect_dashboard_data(output_root: Path, coordinator_store: CoordinatorStore | None = None) -> dict[str, Any]:
    output_root = output_root.resolve()
    domains: list[dict[str, Any]] = []

    for domain_dir in sorted(output_root.iterdir(), key=lambda p: p.name.lower()) if output_root.is_dir() else []:
        if not domain_dir.is_dir() or domain_dir.name.startswith("."):
            continue
        domain = domain_dir.name.strip().lower()
        if not domain:
            continue

        inv_path = domain_dir / f"{domain}_url_inventory.json"
        req_path = domain_dir / "requests.json"
        params_path = domain_dir / f"{domain}.parameters.json"
        nightmare_report = _discover_nightmare_report_html(domain_dir)
        fozzy_summary = _discover_fozzy_summary_for_domain(domain_dir, domain)
        fozzy_results_html = _discover_fozzy_results_html_for_domain(domain_dir, domain)
        log_files = _discover_domain_log_files(domain_dir, domain)

        inv = _read_json_dict(inv_path) if inv_path.is_file() else {}
        req = _read_json_dict(req_path) if req_path.is_file() else {}
        foz = _read_json_dict(fozzy_summary) if fozzy_summary and fozzy_summary.is_file() else {}

        unique_urls = _safe_int(inv.get("total_urls_effective", inv.get("total_urls", 0)))
        requested_urls = _safe_int(req.get("total_requested_urls", 0))
        interrupted = bool(foz.get("interrupted", False))
        anomaly_count = _safe_int((foz.get("totals") or {}).get("anomalies", 0)) if isinstance(foz.get("totals"), dict) else 0
        reflection_count = _safe_int((foz.get("totals") or {}).get("reflections", 0)) if isinstance(foz.get("totals"), dict) else 0

        status = "discovered"
        if params_path.is_file():
            status = "parameterized"
        if fozzy_summary and fozzy_summary.is_file():
            status = "fozzy_complete" if not interrupted else "fozzy_interrupted"

        domains.append(
            {
                "domain": domain,
                "status": status,
                "unique_urls": unique_urls,
                "requested_urls": requested_urls,
                "anomalies": anomaly_count,
                "reflections": reflection_count,
                "paths": {
                    "domain_dir": str(domain_dir.resolve()),
                    "url_inventory": str(inv_path.resolve()) if inv_path.is_file() else None,
                    "requests": str(req_path.resolve()) if req_path.is_file() else None,
                    "parameters": str(params_path.resolve()) if params_path.is_file() else None,
                    "nightmare_report_html": str(nightmare_report.resolve()) if nightmare_report else None,
                    "fozzy_summary_json": str(fozzy_summary.resolve()) if fozzy_summary else None,
                    "fozzy_results_html": str(fozzy_results_html.resolve()) if fozzy_results_html else None,
                },
                "paths_rel": {
                    "domain_dir": _to_repo_relative_path(domain_dir),
                    "url_inventory": _to_repo_relative_path(inv_path) if inv_path.is_file() else None,
                    "requests": _to_repo_relative_path(req_path) if req_path.is_file() else None,
                    "parameters": _to_repo_relative_path(params_path) if params_path.is_file() else None,
                    "nightmare_report_html": _to_repo_relative_path(nightmare_report) if nightmare_report else None,
                    "fozzy_summary_json": _to_repo_relative_path(fozzy_summary) if fozzy_summary else None,
                    "fozzy_results_html": _to_repo_relative_path(fozzy_results_html) if fozzy_results_html else None,
                },
                "logs": [str(p) for p in log_files],
                "latest_log_tail": _read_tail_lines(log_files[-1], lines=120) if log_files else "",
            }
        )

    coordinator_counts: dict[str, Any] = {}
    if coordinator_store is not None:
        try:
            progress = coordinator_store.crawl_progress_snapshot(limit=2000)
            coordinator_counts = progress.get("counts", {}) if isinstance(progress.get("counts"), dict) else {}
            existing = {str(item.get("domain", "")).strip().lower(): item for item in domains}
            for item in progress.get("domains", []):
                if not isinstance(item, dict):
                    continue
                domain = str(item.get("root_domain", "")).strip().lower()
                if not domain:
                    continue
                discovered = _safe_int(item.get("discovered_urls_count", 0))
                visited = _safe_int(item.get("visited_urls_count", 0))
                phase = str(item.get("phase", "") or "").strip().lower() or "discovered"
                if domain in existing:
                    row = existing[domain]
                    row["status"] = phase
                    row["unique_urls"] = max(_safe_int(row.get("unique_urls", 0)), discovered)
                    row["requested_urls"] = max(_safe_int(row.get("requested_urls", 0)), visited)
                    continue
                domains.append(
                    {
                        "domain": domain,
                        "status": phase,
                        "unique_urls": discovered,
                        "requested_urls": visited,
                        "anomalies": 0,
                        "reflections": 0,
                        "paths": {
                            "domain_dir": None,
                            "url_inventory": None,
                            "requests": None,
                            "parameters": None,
                            "nightmare_report_html": None,
                            "fozzy_summary_json": None,
                            "fozzy_results_html": None,
                        },
                        "paths_rel": {
                            "domain_dir": None,
                            "url_inventory": None,
                            "requests": None,
                            "parameters": None,
                            "nightmare_report_html": None,
                            "fozzy_summary_json": None,
                            "fozzy_results_html": None,
                        },
                        "logs": [],
                        "latest_log_tail": "",
                    }
                )
        except Exception:
            coordinator_counts = {}

    domains = sorted(domains, key=lambda item: str(item.get("domain", "")).lower())

    batch_state = _read_json_dict(output_root / "batch_state" / "batch_run_state.json")
    batch_counts: dict[str, int] = {}
    entries = batch_state.get("entries", [])
    if isinstance(entries, list):
        for item in entries:
            if not isinstance(item, dict):
                continue
            st = str(item.get("status", "unknown"))
            batch_counts[st] = batch_counts.get(st, 0) + 1

    totals = {
        "domains": len(domains),
        "fozzy_complete": sum(1 for d in domains if d["status"] == "fozzy_complete"),
        "fozzy_interrupted": sum(1 for d in domains if d["status"] == "fozzy_interrupted"),
        "parameterized": sum(1 for d in domains if d["status"] == "parameterized"),
        "discovered": sum(1 for d in domains if d["status"] == "discovered"),
        "running": sum(1 for d in domains if str(d.get("status", "")).endswith("_running") or d.get("status") == "running"),
        "pending": sum(1 for d in domains if str(d.get("status", "")).endswith("_pending") or d.get("status") == "pending"),
        "completed": sum(1 for d in domains if d.get("status") in {"completed", "fozzy_complete"}),
        "failed": sum(1 for d in domains if d.get("status") in {"failed", "fozzy_interrupted", "errored"}),
        "batch_counts": batch_counts,
        "coordinator_counts": coordinator_counts,
    }

    return {
        "generated_at_utc": _iso_now(),
        "output_root": str(output_root),
        "totals": totals,
        "master_reports": _collect_master_reports(output_root),
        "domains": domains,
    }


class _ExtractorMatchesCache:
    def __init__(self, *, max_domains: int = MAX_EXTRACTOR_CACHE_DOMAINS, ttl_seconds: int = EXTRACTOR_CACHE_TTL_SECONDS):
        self._max_domains = max(8, int(max_domains or MAX_EXTRACTOR_CACHE_DOMAINS))
        self._ttl_seconds = max(60, int(ttl_seconds or EXTRACTOR_CACHE_TTL_SECONDS))
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def get(self, root_domain: str, content_sha256: str) -> Optional[dict[str, Any]]:
        key = str(root_domain or "").strip().lower()
        sha = str(content_sha256 or "").strip().lower()
        now = time.time()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if str(entry.get("content_sha256", "")).strip().lower() != sha:
                self._entries.pop(key, None)
                return None
            loaded_at = float(entry.get("loaded_at_epoch", 0.0) or 0.0)
            if now - loaded_at > self._ttl_seconds:
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return entry

    def set(
        self,
        root_domain: str,
        content_sha256: str,
        *,
        updated_at_utc: Optional[str],
        match_count: int,
        rows: list[dict[str, Any]],
        files: list[dict[str, Any]],
    ) -> None:
        key = str(root_domain or "").strip().lower()
        if not key:
            return
        entry = {
            "root_domain": key,
            "content_sha256": str(content_sha256 or "").strip().lower(),
            "updated_at_utc": str(updated_at_utc or "") or None,
            "match_count": int(match_count or 0),
            "max_importance_score": max((_safe_int(item.get("importance_score", 0), 0) for item in rows if isinstance(item, dict)), default=0),
            "rows": rows,
            "files": files,
            "loaded_at_epoch": time.time(),
        }
        with self._lock:
            self._entries[key] = entry
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_domains:
                self._entries.popitem(last=False)

    def get_match_count(self, root_domain: str, content_sha256: str) -> Optional[int]:
        entry = self.get(root_domain, content_sha256)
        if entry is None:
            return None
        try:
            return int(entry.get("match_count", 0) or 0)
        except Exception:
            return None

    def get_domain_stats(self, root_domain: str, content_sha256: str) -> Optional[dict[str, int]]:
        entry = self.get(root_domain, content_sha256)
        if entry is None:
            return None
        return {
            "match_count": _safe_int(entry.get("match_count", 0), 0),
            "max_importance_score": _safe_int(entry.get("max_importance_score", 0), 0),
        }


class _FozzySummaryCache:
    def __init__(self, *, max_domains: int = MAX_FUZZING_CACHE_DOMAINS, ttl_seconds: int = FUZZING_CACHE_TTL_SECONDS):
        self._max_domains = max(8, int(max_domains or MAX_FUZZING_CACHE_DOMAINS))
        self._ttl_seconds = max(60, int(ttl_seconds or FUZZING_CACHE_TTL_SECONDS))
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def get(self, root_domain: str, summary_sha256: str) -> Optional[dict[str, Any]]:
        key = str(root_domain or "").strip().lower()
        sha = str(summary_sha256 or "").strip().lower()
        now = time.time()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if str(entry.get("summary_sha256", "")).strip().lower() != sha:
                self._entries.pop(key, None)
                return None
            loaded_at = float(entry.get("loaded_at_epoch", 0.0) or 0.0)
            if now - loaded_at > self._ttl_seconds:
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return entry

    def set(
        self,
        root_domain: str,
        summary_sha256: str,
        *,
        updated_at_utc: Optional[str],
        rows: list[dict[str, Any]],
        totals: dict[str, int],
    ) -> None:
        key = str(root_domain or "").strip().lower()
        if not key:
            return
        entry = {
            "root_domain": key,
            "summary_sha256": str(summary_sha256 or "").strip().lower(),
            "updated_at_utc": str(updated_at_utc or "") or None,
            "rows": rows,
            "totals": totals,
            "loaded_at_epoch": time.time(),
        }
        with self._lock:
            self._entries[key] = entry
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_domains:
                self._entries.popitem(last=False)


class _ZipFileIndexCache:
    def __init__(self, *, max_domains: int = MAX_FUZZING_CACHE_DOMAINS, ttl_seconds: int = FUZZING_CACHE_TTL_SECONDS):
        self._max_domains = max(8, int(max_domains or MAX_FUZZING_CACHE_DOMAINS))
        self._ttl_seconds = max(60, int(ttl_seconds or FUZZING_CACHE_TTL_SECONDS))
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def get(self, root_domain: str, content_sha256: str) -> Optional[dict[str, Any]]:
        key = str(root_domain or "").strip().lower()
        sha = str(content_sha256 or "").strip().lower()
        now = time.time()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if str(entry.get("content_sha256", "")).strip().lower() != sha:
                self._entries.pop(key, None)
                return None
            loaded_at = float(entry.get("loaded_at_epoch", 0.0) or 0.0)
            if now - loaded_at > self._ttl_seconds:
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return entry

    def set(self, root_domain: str, content_sha256: str, *, files: list[dict[str, Any]], normalized_names: dict[str, str]) -> None:
        key = str(root_domain or "").strip().lower()
        if not key:
            return
        entry = {
            "root_domain": key,
            "content_sha256": str(content_sha256 or "").strip().lower(),
            "files": files,
            "normalized_names": normalized_names,
            "loaded_at_epoch": time.time(),
        }
        with self._lock:
            self._entries[key] = entry
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_domains:
                self._entries.popitem(last=False)


def _normalize_zip_member_path(raw: str) -> str:
    text = str(raw or "").replace("\\", "/").strip().lstrip("/")
    if not text or text.startswith("../") or "/../" in text:
        return ""
    return text


def _to_preview_text(value: Any, *, max_len: int = 220) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def _fuzzing_preview(value: Any, *, max_len: int = 600) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def _fuzzing_extract_request_content(payload: dict[str, Any], response_payload: dict[str, Any] | None = None) -> str:
    requested_url = str(payload.get("requested_url", "") or "")
    http_method = str(
        payload.get("http_method")
        or ((response_payload or {}).get("http_method") if isinstance(response_payload, dict) else "")
        or "GET"
    ).strip().upper() or "GET"
    mutated_parameter = str(payload.get("mutated_parameter", "") or "")
    mutated_value = str(payload.get("mutated_value", "") or "")
    if mutated_parameter:
        if mutated_value:
            return _fuzzing_preview(f"{http_method} {requested_url} [{mutated_parameter}={mutated_value}]")
        return _fuzzing_preview(f"{http_method} {requested_url} [{mutated_parameter}]")
    return _fuzzing_preview(f"{http_method} {requested_url}")


def _build_fozzy_result_details_map(data: bytes) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    raw = bytes(data or b"")
    if not raw:
        return out
    try:
        with zipfile.ZipFile(io.BytesIO(raw), mode="r") as zf:
            for name in zf.namelist():
                member = _normalize_zip_member_path(name)
                if not member or not member.lower().endswith(".json"):
                    continue
                basename = Path(member).name
                if not basename:
                    continue
                try:
                    payload = json.loads(zf.read(name).decode("utf-8", errors="replace"))
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                baseline = payload.get("baseline") if isinstance(payload.get("baseline"), dict) else {}
                fuzzing = payload.get("anomaly") if isinstance(payload.get("anomaly"), dict) else {}
                if not fuzzing and isinstance(payload.get("response"), dict):
                    fuzzing = payload.get("response") or {}
                reflected_text = str(payload.get("mutated_value", "") or "")
                out[member] = {
                    "baseline_request_content": _fuzzing_extract_request_content(payload, baseline),
                    "fuzzing_request_content": _fuzzing_extract_request_content(payload, fuzzing),
                    "baseline_response_content": _fuzzing_preview(baseline.get("body_preview", "")),
                    "fuzzing_response_content": _fuzzing_preview(fuzzing.get("body_preview", "")),
                    "baseline_status": _safe_int(baseline.get("status", 0), 0),
                    "new_status": _safe_int(fuzzing.get("status", 0), 0),
                    "baseline_size": _safe_int(baseline.get("size", 0), 0),
                    "new_size": _safe_int(fuzzing.get("size", 0), 0),
                    "baseline_duration_ms": _safe_int(baseline.get("elapsed_ms", 0), 0),
                    "fuzzing_duration_ms": _safe_int(fuzzing.get("elapsed_ms", 0), 0),
                    "reflected_text": reflected_text,
                }
                if basename not in out:
                    out[basename] = out[member]
    except Exception:
        return {}
    return out


def _enrich_fuzzing_rows_from_result_details(rows: list[dict[str, Any]], details: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows or not details:
        return rows
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        result_file = str(row.get("result_file", "") or "")
        file_key = _normalize_zip_member_path(result_file)
        basename = Path(file_key or result_file).name
        item = details.get(file_key) or details.get(basename)
        if item is None:
            out_rows.append(row)
            continue
        merged = dict(row)
        merged["baseline_request_content"] = _fuzzing_preview(
            item.get("baseline_request_content") or merged.get("baseline_request_content", "")
        )
        merged["fuzzing_request_content"] = _fuzzing_preview(
            item.get("fuzzing_request_content") or merged.get("fuzzing_request_content", "")
        )
        merged["baseline_response_content"] = _fuzzing_preview(
            item.get("baseline_response_content") or merged.get("baseline_response_content", "")
        )
        merged["fuzzing_response_content"] = _fuzzing_preview(
            item.get("fuzzing_response_content") or merged.get("fuzzing_response_content", "")
        )
        baseline_status = _safe_int(item.get("baseline_status", merged.get("baseline_status", 0)), 0)
        new_status = _safe_int(item.get("new_status", merged.get("new_status", 0)), 0)
        baseline_size = _safe_int(item.get("baseline_size", merged.get("baseline_size", 0)), 0)
        new_size = _safe_int(item.get("new_size", merged.get("new_size", 0)), 0)
        baseline_duration_ms = _safe_int(item.get("baseline_duration_ms", merged.get("baseline_duration_ms", 0)), 0)
        fuzzing_duration_ms = _safe_int(item.get("fuzzing_duration_ms", merged.get("fuzzing_duration_ms", 0)), 0)
        merged["baseline_status"] = baseline_status
        merged["new_status"] = new_status
        merged["baseline_size"] = baseline_size
        merged["new_size"] = new_size
        merged["size_difference"] = new_size - baseline_size
        merged["status_difference"] = new_status - baseline_status
        merged["baseline_duration_ms"] = baseline_duration_ms
        merged["fuzzing_duration_ms"] = fuzzing_duration_ms
        merged["duration_difference_ms"] = fuzzing_duration_ms - baseline_duration_ms
        if str(merged.get("result_type", "") or "") == "reflection":
            reflected = str(item.get("reflected_text", "") or "").strip()
            if reflected:
                merged["anomaly_note"] = _fuzzing_preview(f"reflection_detected: {reflected}")
        out_rows.append(merged)
    return out_rows


def _top_extractor_filters(rows: list[dict[str, Any]], *, top_n: int = 10) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("filter_name", "") or "").strip()
        if not name:
            name = "(unnamed)"
        counts[name] = counts.get(name, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0].lower()))
    out: list[dict[str, Any]] = []
    for name, count in ranked[: max(1, int(top_n or 10))]:
        out.append({"filter_name": name, "match_count": int(count)})
    return out



def _resolve_extractor_rule_paths(app_root: Path) -> tuple[Path, Path]:
    config_path = (app_root / "config" / "extractor.json").resolve()
    cfg: dict[str, Any] = {}
    try:
        cfg = _read_json_dict(config_path)
    except Exception:
        cfg = {}
    rule_rel = str(cfg.get("wordlist", "resources/wordlists/extractor_list.txt") or "resources/wordlists/extractor_list.txt").strip()
    js_rel = str(cfg.get("javascript_wordlist", "resources/wordlists/javascript_extractor_list.txt") or "resources/wordlists/javascript_extractor_list.txt").strip()
    return (app_root / rule_rel).resolve(), (app_root / js_rel).resolve()


def _load_extractor_rule_file(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        parsed = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    out: list[dict[str, Any]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        out.append({
            "name": str(item.get("name", "") or "").strip(),
            "scope": str(item.get("scope", "request_response") or "request_response").strip(),
            "regex": str(item.get("regex", "") or ""),
            "description": str(item.get("description", "") or ""),
            "output_filename": str(item.get("output_filename", "") or ""),
            "importance_score": _safe_int(item.get("importance_score", 0), 0),
        })
    return out


def _write_extractor_rule_file(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "") or "").strip()
        regex_value = str(item.get("regex", "") or "")
        if not name or not regex_value:
            continue
        clean.append({
            "name": name,
            "scope": str(item.get("scope", "request_response") or "request_response").strip() or "request_response",
            "regex": regex_value,
            "description": str(item.get("description", "") or ""),
            "output_filename": str(item.get("output_filename", "") or ""),
            "importance_score": _safe_int(item.get("importance_score", 0), 0),
        })
    path.write_text(json.dumps(clean, indent=2) + "\n", encoding="utf-8")

def _decode_json_artifact(content: bytes, content_encoding: str) -> dict[str, Any]:
    data = bytes(content or b"")
    if not data:
        return {}
    encoding = str(content_encoding or "identity").strip().lower()
    try:
        payload = data
        if encoding == "zip":
            with zipfile.ZipFile(io.BytesIO(data), mode="r") as zf:
                names = [name for name in zf.namelist() if name.lower().endswith(".json")]
                if not names:
                    return {}
                payload = zf.read(names[0])
        parsed = json.loads(payload.decode("utf-8", errors="replace"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _flatten_fozzy_summary_rows(root_domain: str, summary: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    groups = summary.get("groups") if isinstance(summary.get("groups"), list) else []
    totals = summary.get("totals") if isinstance(summary.get("totals"), dict) else {}
    for group in groups:
        if not isinstance(group, dict):
            continue
        host = str(group.get("host", "") or "")
        path = str(group.get("path", "") or "")
        for entry in group.get("anomaly_entries", []) if isinstance(group.get("anomaly_entries"), list) else []:
            if not isinstance(entry, dict):
                continue
            baseline_status = _safe_int(entry.get("baseline_status", 0), 0)
            new_status = _safe_int(entry.get("new_status", 0), 0)
            baseline_size = _safe_int(entry.get("baseline_size", 0), 0)
            new_size = _safe_int(entry.get("new_size", 0), 0)
            size_difference = _safe_int(entry.get("size_difference", new_size - baseline_size), new_size - baseline_size)
            rows.append(
                {
                    "root_domain": root_domain,
                    "result_type": "anomaly",
                    "anomaly_type": str(entry.get("anomaly_type", "") or ""),
                    "anomaly_note": str(entry.get("anomaly_note", "") or ""),
                    "url": str(entry.get("url", "") or ""),
                    "host": str(entry.get("host", "") or host),
                    "path": str(entry.get("path", "") or path),
                    "mutated_parameter": str(entry.get("mutated_parameter", "") or ""),
                    "mutated_value": str(entry.get("mutated_value", "") or ""),
                    "baseline_status": baseline_status,
                    "new_status": new_status,
                    "baseline_size": baseline_size,
                    "new_size": new_size,
                    "size_difference": size_difference,
                    "status_difference": new_status - baseline_status,
                    "baseline_duration_ms": _safe_int(entry.get("baseline_duration_ms", 0), 0),
                    "fuzzing_duration_ms": _safe_int(entry.get("fuzzing_duration_ms", 0), 0),
                    "duration_difference_ms": _safe_int(
                        entry.get(
                            "duration_difference_ms",
                            _safe_int(entry.get("fuzzing_duration_ms", 0), 0)
                            - _safe_int(entry.get("baseline_duration_ms", 0), 0),
                        ),
                        0,
                    ),
                    "baseline_request_content": str(entry.get("baseline_request_content", "") or ""),
                    "fuzzing_request_content": str(entry.get("fuzzing_request_content", "") or ""),
                    "baseline_response_content": str(entry.get("baseline_response_content", "") or ""),
                    "fuzzing_response_content": str(entry.get("fuzzing_response_content", "") or ""),
                    "result_file": str(entry.get("result_file", "") or ""),
                }
            )
        for entry in group.get("reflection_entries", []) if isinstance(group.get("reflection_entries"), list) else []:
            if not isinstance(entry, dict):
                continue
            mutated_value = str(entry.get("mutated_value", "") or "")
            reflection_note = "reflection_detected"
            if mutated_value:
                reflection_note = f"reflection_detected: {mutated_value}"
            baseline_status = _safe_int(entry.get("baseline_status", 0), 0)
            new_status = _safe_int(entry.get("new_status", 0), 0)
            baseline_size = _safe_int(entry.get("baseline_size", 0), 0)
            new_size = _safe_int(entry.get("new_size", 0), 0)
            size_difference = _safe_int(entry.get("size_difference", new_size - baseline_size), new_size - baseline_size)
            rows.append(
                {
                    "root_domain": root_domain,
                    "result_type": "reflection",
                    "anomaly_type": "",
                    "anomaly_note": reflection_note,
                    "url": str(entry.get("url", "") or ""),
                    "host": str(entry.get("host", "") or host),
                    "path": str(entry.get("path", "") or path),
                    "mutated_parameter": str(entry.get("mutated_parameter", "") or ""),
                    "mutated_value": mutated_value,
                    "baseline_status": baseline_status,
                    "new_status": new_status,
                    "baseline_size": baseline_size,
                    "new_size": new_size,
                    "size_difference": size_difference,
                    "status_difference": new_status - baseline_status,
                    "baseline_duration_ms": _safe_int(entry.get("baseline_duration_ms", 0), 0),
                    "fuzzing_duration_ms": _safe_int(entry.get("fuzzing_duration_ms", 0), 0),
                    "duration_difference_ms": _safe_int(
                        entry.get(
                            "duration_difference_ms",
                            _safe_int(entry.get("fuzzing_duration_ms", 0), 0)
                            - _safe_int(entry.get("baseline_duration_ms", 0), 0),
                        ),
                        0,
                    ),
                    "baseline_request_content": str(entry.get("baseline_request_content", "") or ""),
                    "fuzzing_request_content": str(entry.get("fuzzing_request_content", "") or ""),
                    "baseline_response_content": str(entry.get("baseline_response_content", "") or ""),
                    "fuzzing_response_content": str(entry.get("fuzzing_response_content", "") or ""),
                    "result_file": str(entry.get("result_file", "") or ""),
                }
            )
    normalized_totals = {
        "groups": max(0, _safe_int(totals.get("groups", len(groups)), len(groups))),
        "baseline_requests": max(0, _safe_int(totals.get("baseline_requests", 0), 0)),
        "fuzz_requests": max(0, _safe_int(totals.get("fuzz_requests", 0), 0)),
        "anomalies": max(0, _safe_int(totals.get("anomalies", 0), 0)),
        "reflections": max(0, _safe_int(totals.get("reflections", 0), 0)),
    }
    return rows, normalized_totals


EXTRACTOR_MATCH_FILTER_KEYS = {
    "root_domain",
    "filter_name",
    "importance_score",
    "scope",
    "response_side",
    "source_http_status",
    "url",
    "source_request_url",
    "match_preview",
    "match_file",
    "regex",
    "result_type",
    "result_file",
    "generated_at_utc",
}
EXTRACTOR_MATCH_NUMERIC_KEYS = {"importance_score", "source_http_status"}
EXTRACTOR_MATCH_DEFAULT_SORT_KEY = "importance_score"

FUZZING_RESULT_FILTER_KEYS = {
    "root_domain",
    "result_type",
    "anomaly_type",
    "anomaly_note",
    "url",
    "baseline_request_content",
    "baseline_response_content",
    "fuzzing_request_content",
    "fuzzing_response_content",
    "host",
    "path",
    "mutated_parameter",
    "mutated_value",
    "baseline_status",
    "new_status",
    "baseline_size",
    "new_size",
    "baseline_duration_ms",
    "fuzzing_duration_ms",
    "status_difference",
    "size_difference",
    "duration_difference_ms",
    "result_file",
}
FUZZING_RESULT_NUMERIC_KEYS = {
    "baseline_status",
    "new_status",
    "baseline_size",
    "new_size",
    "baseline_duration_ms",
    "fuzzing_duration_ms",
    "status_difference",
    "size_difference",
    "duration_difference_ms",
}
FUZZING_RESULT_DEFAULT_SORT_KEY = "size_difference"
DISCOVERED_TARGET_DOMAIN_SORT_KEYS = {"root_domain", "saved_at_utc", "discovered_urls_count", "status"}
DISCOVERED_TARGET_DOMAIN_NUMERIC_KEYS = {"discovered_urls_count"}
DISCOVERED_TARGET_SITEMAP_SORT_KEYS = {
    "url",
    "subdomain",
    "path",
    "inbound_count",
    "outbound_count",
    "exists_status",
    "response_status_code",
    "response_elapsed_ms",
    "response_size_bytes",
    "crawl_status_code",
    "existence_status_code",
    "captured_at_utc",
}
DISCOVERED_TARGET_SITEMAP_NUMERIC_KEYS = {
    "inbound_count",
    "outbound_count",
    "response_status_code",
    "response_elapsed_ms",
    "response_size_bytes",
    "crawl_status_code",
    "existence_status_code",
}
DISCOVERED_TARGET_SITEMAP_DEFAULT_SORT_KEY = "url"


def _extractor_row_search_blob(row: dict[str, Any]) -> str:
    parts = [
        row.get("root_domain"),
        row.get("filter_name"),
        row.get("scope"),
        row.get("response_side"),
        row.get("regex"),
        row.get("url"),
        row.get("source_request_url"),
        row.get("result_file"),
        row.get("matched_text"),
        row.get("match_file"),
    ]
    return " ".join(str(item or "") for item in parts).lower()


def _apply_extractor_row_query(
    rows: list[dict[str, Any]],
    *,
    search_text: str,
    column_filters: dict[str, str],
    sort_key: str,
    sort_dir: str,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    q = str(search_text or "").strip().lower()
    effective_filters: dict[str, str] = {}
    for key, value in (column_filters or {}).items():
        col = str(key or "").strip()
        if col not in EXTRACTOR_MATCH_FILTER_KEYS:
            continue
        text = str(value or "").strip().lower()
        if text:
            effective_filters[col] = text

    filtered: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if q and q not in _extractor_row_search_blob(row):
            continue
        failed = False
        for col, needle in effective_filters.items():
            value = row.get(col)
            if col in EXTRACTOR_MATCH_NUMERIC_KEYS:
                hay = str(_safe_int(value, 0))
            else:
                hay = str(value or "")
            if needle not in hay.lower():
                failed = True
                break
        if not failed:
            filtered.append(row)

    safe_sort_key = str(sort_key or EXTRACTOR_MATCH_DEFAULT_SORT_KEY).strip()
    if safe_sort_key not in EXTRACTOR_MATCH_FILTER_KEYS:
        safe_sort_key = EXTRACTOR_MATCH_DEFAULT_SORT_KEY
    safe_sort_dir = "asc" if str(sort_dir or "").strip().lower() == "asc" else "desc"
    reverse = safe_sort_dir == "desc"

    def _sort_tuple(item: dict[str, Any]) -> tuple[int, Any]:
        value = item.get(safe_sort_key)
        if safe_sort_key in EXTRACTOR_MATCH_NUMERIC_KEYS:
            return (0, _safe_int(value, 0))
        return (1, str(value or "").lower())

    filtered.sort(key=_sort_tuple, reverse=reverse)
    total_rows = len(filtered)
    safe_offset = max(0, int(offset or 0))
    safe_limit = max(1, min(MAX_EXTRACTOR_MATCH_LIMIT, int(limit or DEFAULT_EXTRACTOR_MATCH_LIMIT)))
    page_rows = filtered[safe_offset : safe_offset + safe_limit]
    next_offset = safe_offset + safe_limit if (safe_offset + safe_limit) < total_rows else None
    prev_offset = max(0, safe_offset - safe_limit) if safe_offset > 0 else None

    return {
        "rows": page_rows,
        "filtered_rows_for_stats": filtered,
        "total_rows": total_rows,
        "offset": safe_offset,
        "limit": safe_limit,
        "next_offset": next_offset,
        "prev_offset": prev_offset,
        "has_more": next_offset is not None,
        "sort_key": safe_sort_key,
        "sort_dir": safe_sort_dir,
        "column_filters": effective_filters,
    }


def _fuzzing_row_search_blob(row: dict[str, Any]) -> str:
    parts = [
        row.get("root_domain"),
        row.get("result_type"),
        row.get("anomaly_type"),
        row.get("anomaly_note"),
        row.get("url"),
        row.get("baseline_request_content"),
        row.get("baseline_response_content"),
        row.get("fuzzing_request_content"),
        row.get("fuzzing_response_content"),
        row.get("host"),
        row.get("path"),
        row.get("mutated_parameter"),
        row.get("mutated_value"),
        row.get("result_file"),
    ]
    return " ".join(str(item or "") for item in parts).lower()


def _apply_fuzzing_row_query(
    rows: list[dict[str, Any]],
    *,
    search_text: str,
    column_filters: dict[str, str],
    sort_key: str,
    sort_dir: str,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    q = str(search_text or "").strip().lower()
    effective_filters: dict[str, str] = {}
    for key, value in (column_filters or {}).items():
        col = str(key or "").strip()
        if col not in FUZZING_RESULT_FILTER_KEYS:
            continue
        text = str(value or "").strip().lower()
        if text:
            effective_filters[col] = text

    filtered: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if q and q not in _fuzzing_row_search_blob(row):
            continue
        failed = False
        for col, needle in effective_filters.items():
            value = row.get(col)
            if col in FUZZING_RESULT_NUMERIC_KEYS:
                hay = str(_safe_int(value, 0))
            else:
                hay = str(value or "")
            if needle not in hay.lower():
                failed = True
                break
        if not failed:
            filtered.append(row)

    safe_sort_key = str(sort_key or FUZZING_RESULT_DEFAULT_SORT_KEY).strip()
    if safe_sort_key not in FUZZING_RESULT_FILTER_KEYS:
        safe_sort_key = FUZZING_RESULT_DEFAULT_SORT_KEY
    safe_sort_dir = "asc" if str(sort_dir or "").strip().lower() == "asc" else "desc"
    reverse = safe_sort_dir == "desc"

    def _sort_tuple(item: dict[str, Any]) -> tuple[int, Any]:
        value = item.get(safe_sort_key)
        if safe_sort_key in FUZZING_RESULT_NUMERIC_KEYS:
            return (0, _safe_int(value, 0))
        return (1, str(value or "").lower())

    filtered.sort(key=_sort_tuple, reverse=reverse)
    total_rows = len(filtered)
    safe_offset = max(0, int(offset or 0))
    safe_limit = max(1, min(MAX_FUZZING_RESULT_LIMIT, int(limit or DEFAULT_FUZZING_RESULT_LIMIT)))
    page_rows = filtered[safe_offset : safe_offset + safe_limit]
    next_offset = safe_offset + safe_limit if (safe_offset + safe_limit) < total_rows else None
    prev_offset = max(0, safe_offset - safe_limit) if safe_offset > 0 else None

    return {
        "rows": page_rows,
        "filtered_rows_for_stats": filtered,
        "total_rows": total_rows,
        "offset": safe_offset,
        "limit": safe_limit,
        "next_offset": next_offset,
        "prev_offset": prev_offset,
        "has_more": next_offset is not None,
        "sort_key": safe_sort_key,
        "sort_dir": safe_sort_dir,
        "column_filters": effective_filters,
    }


def _discovered_target_subdomain_for_row(row: dict[str, Any], root_domain: str) -> str:
    existing = str(row.get("subdomain") or "").strip()
    if existing:
        return existing
    url_text = str(row.get("url") or "").strip()
    host = str(urlparse(url_text).hostname or "").strip().lower()
    rd = str(root_domain or "").strip().lower()
    if not host:
        return ""
    if rd and host == rd:
        return "@root"
    if rd and host.endswith(f".{rd}"):
        return host[: -(len(rd) + 1)] or "@root"
    return host


def _discovered_target_sitemap_search_blob(row: dict[str, Any]) -> str:
    parts = [
        row.get("url"),
        row.get("subdomain"),
        row.get("path"),
        row.get("exists_status"),
        row.get("exists_reason"),
        row.get("request_method"),
        row.get("response_content_type"),
        row.get("content_type"),
        " ".join(str(v or "") for v in (row.get("discovered_via") or [])),
        " ".join(str(v or "") for v in (row.get("discovered_from") or row.get("parents") or [])),
    ]
    return " ".join(str(part or "") for part in parts).lower()


def _apply_discovered_target_domain_query(
    rows: list[dict[str, Any]],
    *,
    search_text: str,
    sort_key: str,
    sort_dir: str,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    q = str(search_text or "").strip().lower()
    safe_sort_key = str(sort_key or "saved_at_utc").strip().lower()
    if safe_sort_key not in DISCOVERED_TARGET_DOMAIN_SORT_KEYS:
        safe_sort_key = "saved_at_utc"
    descending = str(sort_dir or "desc").strip().lower() != "asc"
    filtered: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if q:
            blob = " ".join(
                [
                    str(row.get("root_domain") or ""),
                    str(row.get("start_url") or ""),
                    str(row.get("status") or ""),
                    str(row.get("saved_at_utc") or ""),
                ]
            ).lower()
            if q not in blob:
                continue
        filtered.append(row)

    def _sort_value(item: dict[str, Any]) -> Any:
        value = item.get(safe_sort_key)
        if safe_sort_key in DISCOVERED_TARGET_DOMAIN_NUMERIC_KEYS:
            return _safe_int(value, 0)
        return str(value or "").lower()

    filtered.sort(key=_sort_value, reverse=descending)
    total_rows = len(filtered)
    safe_limit = max(1, min(2000, int(limit or 200)))
    safe_offset = max(0, int(offset or 0))
    page_rows = filtered[safe_offset : safe_offset + safe_limit]
    next_offset = safe_offset + safe_limit if (safe_offset + safe_limit) < total_rows else None
    prev_offset = max(0, safe_offset - safe_limit) if safe_offset > 0 else None
    return {
        "rows": page_rows,
        "total_rows": total_rows,
        "offset": safe_offset,
        "limit": safe_limit,
        "next_offset": next_offset,
        "prev_offset": prev_offset,
        "has_more": next_offset is not None,
        "sort_key": safe_sort_key,
        "sort_dir": "desc" if descending else "asc",
    }


def _apply_discovered_target_sitemap_query(
    rows: list[dict[str, Any]],
    *,
    root_domain: str,
    search_text: str,
    subdomain: str,
    sort_key: str,
    sort_dir: str,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    q = str(search_text or "").strip().lower()
    subdomain_filter = str(subdomain or "").strip().lower()
    safe_sort_key = str(sort_key or DISCOVERED_TARGET_SITEMAP_DEFAULT_SORT_KEY).strip().lower()
    if safe_sort_key not in DISCOVERED_TARGET_SITEMAP_SORT_KEYS:
        safe_sort_key = DISCOVERED_TARGET_SITEMAP_DEFAULT_SORT_KEY
    descending = str(sort_dir or "asc").strip().lower() == "desc"

    with_subdomains: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clone = dict(row)
        clone["subdomain"] = _discovered_target_subdomain_for_row(clone, root_domain)
        with_subdomains.append(clone)

    filtered_for_search: list[dict[str, Any]] = []
    for row in with_subdomains:
        if q and q not in _discovered_target_sitemap_search_blob(row):
            continue
        filtered_for_search.append(row)

    subdomains = sorted(
        {
            str(row.get("subdomain") or "").strip()
            for row in filtered_for_search
            if str(row.get("subdomain") or "").strip()
        },
        key=lambda value: (value != "@root", value.lower()),
    )

    filtered: list[dict[str, Any]] = []
    for row in filtered_for_search:
        row_sub = str(row.get("subdomain") or "").strip().lower()
        if subdomain_filter and subdomain_filter not in {"*", "all"} and row_sub != subdomain_filter:
            continue
        filtered.append(row)

    def _sort_value(item: dict[str, Any]) -> Any:
        value = item.get(safe_sort_key)
        if safe_sort_key in DISCOVERED_TARGET_SITEMAP_NUMERIC_KEYS:
            return _safe_int(value, 0)
        return str(value or "").lower()

    filtered.sort(key=_sort_value, reverse=descending)
    total_rows = len(filtered)
    safe_limit = max(1, min(2000, int(limit or 200)))
    safe_offset = max(0, int(offset or 0))
    page_rows = filtered[safe_offset : safe_offset + safe_limit]
    next_offset = safe_offset + safe_limit if (safe_offset + safe_limit) < total_rows else None
    prev_offset = max(0, safe_offset - safe_limit) if safe_offset > 0 else None
    return {
        "rows": page_rows,
        "total_rows": total_rows,
        "offset": safe_offset,
        "limit": safe_limit,
        "next_offset": next_offset,
        "prev_offset": prev_offset,
        "has_more": next_offset is not None,
        "sort_key": safe_sort_key,
        "sort_dir": "desc" if descending else "asc",
        "subdomains": subdomains,
    }


def _normalize_page_cache_mode(raw: Any) -> str:
    mode = str(raw or "").strip().lower()
    if mode in {"refresh", "live", "bypass", "force"}:
        return "refresh"
    return "prefer"


def _build_page_cache_key(page_name: str, key_parts: dict[str, Any]) -> str:
    safe_name = str(page_name or "").strip().lower() or "page"
    try:
        normalized = json.dumps(key_parts, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        normalized = str(key_parts)
    return f"{safe_name}:{normalized}"


def _iso_from_epoch(epoch_seconds: Any) -> str | None:
    try:
        value = float(epoch_seconds or 0.0)
    except Exception:
        value = 0.0
    if value <= 0.0:
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _attach_page_cache_metadata(
    payload: dict[str, Any],
    *,
    page_name: str,
    cache_key: str,
    cache_mode: str,
    source: str,
    stale: bool,
    cached_at_epoch: float,
    expires_at_epoch: float,
    error_message: str = "",
) -> dict[str, Any]:
    out = dict(payload) if isinstance(payload, dict) else {}
    out["page_cache"] = {
        "page": str(page_name or "").strip().lower(),
        "key": str(cache_key or ""),
        "mode": str(cache_mode or "prefer"),
        "source": str(source or "live"),
        "stale": bool(stale),
        "cached_at_utc": _iso_from_epoch(cached_at_epoch),
        "expires_at_utc": _iso_from_epoch(expires_at_epoch),
        "error": str(error_message or ""),
    }
    return out


class _PageDataCache:
    def __init__(self, *, max_entries: int = PAGE_DATA_CACHE_MAX_ENTRIES, ttl_seconds: int = PAGE_DATA_CACHE_TTL_SECONDS):
        self._max_entries = max(64, int(max_entries or PAGE_DATA_CACHE_MAX_ENTRIES))
        self._ttl_seconds = max(1, int(ttl_seconds or PAGE_DATA_CACHE_TTL_SECONDS))
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def _prune_locked(self, now: float) -> None:
        stale_keys = [
            key
            for key, entry in self._entries.items()
            if now >= float(entry.get("expires_at_epoch", 0.0) or 0.0)
        ]
        for key in stale_keys:
            self._entries.pop(key, None)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def get(self, key: str, *, allow_stale: bool = False) -> Optional[dict[str, Any]]:
        safe_key = str(key or "").strip()
        if not safe_key:
            return None
        now = time.time()
        with self._lock:
            entry = self._entries.get(safe_key)
            if entry is None:
                return None
            expires_at = float(entry.get("expires_at_epoch", 0.0) or 0.0)
            stale = now >= expires_at
            if stale and not allow_stale:
                self._entries.pop(safe_key, None)
                return None
            self._entries.move_to_end(safe_key)
            return {
                "key": safe_key,
                "payload": copy.deepcopy(entry.get("payload", {})),
                "cached_at_epoch": float(entry.get("cached_at_epoch", 0.0) or 0.0),
                "expires_at_epoch": expires_at,
                "stale": stale,
            }

    def set(self, key: str, payload: dict[str, Any], *, ttl_seconds: Optional[int] = None) -> dict[str, Any]:
        safe_key = str(key or "").strip()
        if not safe_key:
            return {"key": "", "payload": {}, "cached_at_epoch": 0.0, "expires_at_epoch": 0.0, "stale": False}
        now = time.time()
        ttl = max(1, int(ttl_seconds or self._ttl_seconds))
        entry = {
            "payload": copy.deepcopy(payload if isinstance(payload, dict) else {}),
            "cached_at_epoch": now,
            "expires_at_epoch": now + ttl,
        }
        with self._lock:
            self._entries[safe_key] = entry
            self._entries.move_to_end(safe_key)
            self._prune_locked(now)
        return {
            "key": safe_key,
            "payload": copy.deepcopy(entry.get("payload", {})),
            "cached_at_epoch": float(entry.get("cached_at_epoch", 0.0) or 0.0),
            "expires_at_epoch": float(entry.get("expires_at_epoch", 0.0) or 0.0),
            "stale": False,
        }


def _build_crawl_progress_payload(store: CoordinatorStore, *, limit: int) -> dict[str, Any]:
    payload = store.crawl_progress_snapshot(limit=limit)
    if isinstance(payload, dict):
        return payload
    return {"generated_at_utc": _iso_now(), "counts": {}, "domains": []}


def _build_discovered_target_domains_payload(
    store: CoordinatorStore,
    *,
    limit: int,
    offset: int,
    search_text: str,
    sort_key: str,
    sort_dir: str,
) -> dict[str, Any]:
    fetch_limit = max(limit + offset, limit)
    rows = store.list_discovered_target_domains(limit=min(20000, fetch_limit), q=search_text)
    query_result = _apply_discovered_target_domain_query(
        rows,
        search_text=search_text,
        sort_key=sort_key,
        sort_dir=sort_dir,
        offset=offset,
        limit=limit,
    )
    page_rows = query_result.get("rows", [])
    return {
        "generated_at_utc": _iso_now(),
        "rows": page_rows,
        "domains": page_rows,
        "total_domains": int(query_result.get("total_rows", 0) or 0),
        "offset": int(query_result.get("offset", 0) or 0),
        "limit": int(query_result.get("limit", limit) or limit),
        "next_offset": query_result.get("next_offset"),
        "prev_offset": query_result.get("prev_offset"),
        "has_more": bool(query_result.get("has_more", False)),
        "sort_key": str(query_result.get("sort_key", "saved_at_utc") or "saved_at_utc"),
        "sort_dir": str(query_result.get("sort_dir", "desc") or "desc"),
        "search": search_text,
    }


def _build_discovered_target_sitemap_payload(
    store: CoordinatorStore,
    *,
    root_domain: str,
    limit: int,
    offset: int,
    search_text: str,
    subdomain: str,
    sort_key: str,
    sort_dir: str,
    include_details: bool,
) -> dict[str, Any]:
    sitemap = store.get_discovered_target_sitemap(root_domain)
    pages: list[dict[str, Any]]
    if isinstance(sitemap, dict):
        pages = sitemap.get("pages", []) if isinstance(sitemap.get("pages"), list) else []
        sitemap_payload = sitemap
    else:
        pages = sitemap if isinstance(sitemap, list) else []
        sitemap_payload = {
            "root_domain": root_domain,
            "start_url": "",
            "page_count": len(pages),
            "pages": pages,
        }
    query_result = _apply_discovered_target_sitemap_query(
        pages,
        root_domain=root_domain,
        search_text=search_text,
        subdomain=subdomain,
        sort_key=sort_key,
        sort_dir=sort_dir,
        offset=offset,
        limit=limit,
    )
    page_rows = query_result.get("rows", [])
    if include_details and page_rows:
        try:
            page_rows = store.enrich_discovered_target_sitemap_rows(
                root_domain=root_domain,
                rows=list(page_rows),
            )
        except Exception:
            pass
    return {
        "generated_at_utc": _iso_now(),
        "root_domain": root_domain,
        "rows": page_rows,
        "pages": page_rows,
        "all_rows_count": len(pages),
        "total_rows": int(query_result.get("total_rows", 0) or 0),
        "offset": int(query_result.get("offset", 0) or 0),
        "limit": int(query_result.get("limit", limit) or limit),
        "next_offset": query_result.get("next_offset"),
        "prev_offset": query_result.get("prev_offset"),
        "has_more": bool(query_result.get("has_more", False)),
        "sort_key": str(query_result.get("sort_key", DISCOVERED_TARGET_SITEMAP_DEFAULT_SORT_KEY) or DISCOVERED_TARGET_SITEMAP_DEFAULT_SORT_KEY),
        "sort_dir": str(query_result.get("sort_dir", "asc") or "asc"),
        "subdomains": query_result.get("subdomains", []),
        "search": search_text,
        "subdomain": subdomain,
        "include_details": include_details,
        "sitemap": sitemap_payload,
    }


def _filter_discovered_rows_by_text(
    rows: list[dict[str, Any]],
    *,
    search_text: str,
    keys: list[str],
) -> list[dict[str, Any]]:
    q = str(search_text or "").strip().lower()
    if not q:
        return rows
    out: list[dict[str, Any]] = []
    for row in rows:
        blob = " ".join(str(row.get(key) or "") for key in keys).lower()
        if q in blob:
            out.append(row)
    return out


def _build_discovered_files_payload(store: CoordinatorStore, *, limit: int, search_text: str) -> dict[str, Any]:
    rows = store.list_discovered_files(limit=limit)
    rows = _filter_discovered_rows_by_text(
        rows,
        search_text=search_text,
        keys=["root_domain", "source_url", "artifact_type"],
    )
    return {"generated_at_utc": _iso_now(), "rows": rows, "files": rows}


def _build_high_value_files_payload(store: CoordinatorStore, *, limit: int, search_text: str) -> dict[str, Any]:
    rows = store.list_high_value_files(limit=limit)
    rows = _filter_discovered_rows_by_text(
        rows,
        search_text=search_text,
        keys=["root_domain", "source_url", "saved_relative"],
    )
    return {"generated_at_utc": _iso_now(), "rows": rows, "files": rows}


def _build_http_requests_payload(
    store: CoordinatorStore,
    *,
    limit: int,
    offset: int,
    search_text: str,
    root_domain: str,
) -> dict[str, Any]:
    fetch_limit = 50000
    rows = store.list_http_requests(
        limit=fetch_limit,
        q=search_text,
        root_domain=root_domain,
    )
    total_rows = len(rows)
    page_rows = rows[offset : offset + limit]
    next_offset = (offset + limit) if (offset + limit) < total_rows else None
    prev_offset = (offset - limit) if offset > 0 else None
    if isinstance(prev_offset, int) and prev_offset < 0:
        prev_offset = 0
    return {
        "generated_at_utc": _iso_now(),
        "rows": page_rows,
        "total_rows": total_rows,
        "offset": offset,
        "limit": limit,
        "next_offset": next_offset,
        "prev_offset": prev_offset,
        "has_more": bool(next_offset is not None),
        "search": search_text,
        "root_domain": root_domain,
    }


def _start_default_page_cache_warmer(server: ThreadingHTTPServer, *, coordinator_store: CoordinatorStore | None) -> None:
    if coordinator_store is None:
        return
    cache = getattr(server, "page_data_cache", None)
    if cache is None:
        return
    stop_event = threading.Event()
    server.page_cache_warm_stop = stop_event  # type: ignore[attr-defined]

    def _warm_defaults_once() -> None:
        def _ensure_cached(
            *,
            cache_key: str,
            ttl_seconds: int,
            loader: Callable[[], dict[str, Any]],
        ) -> dict[str, Any]:
            cached = cache.get(cache_key)
            if cached is not None and isinstance(cached.get("payload"), dict):
                return cached.get("payload", {})
            payload = loader()
            cache.set(cache_key, payload, ttl_seconds=ttl_seconds)
            return payload

        crawl_key = _build_page_cache_key("crawl_progress", {"limit": 2000})
        _ensure_cached(
            cache_key=crawl_key,
            ttl_seconds=CRAWL_PROGRESS_PAGE_CACHE_TTL_SECONDS,
            loader=lambda: _build_crawl_progress_payload(coordinator_store, limit=2000),
        )

        domain_defaults = {
            "q": "",
            "offset": 0,
            "limit": 100,
            "sort_key": "saved_at_utc",
            "sort_dir": "desc",
        }
        discovered_targets_key = _build_page_cache_key("discovered_targets", domain_defaults)
        discovered_targets_payload = _ensure_cached(
            cache_key=discovered_targets_key,
            ttl_seconds=DISCOVERED_TARGETS_PAGE_CACHE_TTL_SECONDS,
            loader=lambda: _build_discovered_target_domains_payload(
                coordinator_store,
                limit=100,
                offset=0,
                search_text="",
                sort_key="saved_at_utc",
                sort_dir="desc",
            ),
        )

        files_defaults = {"limit": 5000, "q": ""}
        discovered_files_key = _build_page_cache_key("discovered_files", files_defaults)
        _ensure_cached(
            cache_key=discovered_files_key,
            ttl_seconds=DISCOVERED_FILES_PAGE_CACHE_TTL_SECONDS,
            loader=lambda: _build_discovered_files_payload(coordinator_store, limit=5000, search_text=""),
        )

        high_value_files_key = _build_page_cache_key("high_value_files", files_defaults)
        _ensure_cached(
            cache_key=high_value_files_key,
            ttl_seconds=DISCOVERED_FILES_PAGE_CACHE_TTL_SECONDS,
            loader=lambda: _build_high_value_files_payload(coordinator_store, limit=5000, search_text=""),
        )

        # Intentionally skip HTTP request warming here.
        # Building this cache requires reconstructing request traces across many
        # domains and can overwhelm the DB under large fleets; keep it on-demand.

        warmed_domains = discovered_targets_payload.get("rows", []) if isinstance(discovered_targets_payload, dict) else []
        if isinstance(warmed_domains, list):
            for row in warmed_domains[:3]:
                root_domain = str((row or {}).get("root_domain", "") or "").strip().lower()
                if not root_domain:
                    continue
                sitemap_defaults = {
                    "root_domain": root_domain,
                    "q": "",
                    "subdomain": "",
                    "offset": 0,
                    "limit": 200,
                    "sort_key": DISCOVERED_TARGET_SITEMAP_DEFAULT_SORT_KEY,
                    "sort_dir": "asc",
                    "include_details": True,
                }
                sitemap_key = _build_page_cache_key("discovered_target_sitemap", sitemap_defaults)
                _ensure_cached(
                    cache_key=sitemap_key,
                    ttl_seconds=DISCOVERED_TARGET_SITEMAP_PAGE_CACHE_TTL_SECONDS,
                    loader=lambda rd=root_domain: _build_discovered_target_sitemap_payload(
                        coordinator_store,
                        root_domain=rd,
                        limit=200,
                        offset=0,
                        search_text="",
                        subdomain="",
                        sort_key=DISCOVERED_TARGET_SITEMAP_DEFAULT_SORT_KEY,
                        sort_dir="asc",
                        include_details=True,
                    ),
                )

    def _loop() -> None:
        while not stop_event.is_set():
            try:
                _warm_defaults_once()
            except Exception:
                # Keep periodic warming best-effort and non-fatal.
                pass
            if stop_event.wait(max(5, int(PAGE_CACHE_WARM_INTERVAL_SECONDS or 20))):
                break

    threading.Thread(target=_loop, daemon=True, name="nightmare-page-cache-warmer").start()


def _count_matches_in_zip_bytes(data: bytes) -> int:
    stats = _extractor_match_stats_from_zip_bytes(data)
    return int(stats.get("match_count", 0) or 0)


def _extractor_match_stats_from_zip_bytes(data: bytes) -> dict[str, int]:
    try:
        with zipfile.ZipFile(io.BytesIO(data), mode="r") as zf:
            total = 0
            max_importance_score = 0
            for info in zf.infolist():
                if info.is_dir():
                    continue
                name = _normalize_zip_member_path(info.filename).lower()
                if not name.endswith(".json"):
                    continue
                if name.split("/")[-1].startswith("m_"):
                    total += 1
                    try:
                        payload = zf.read(info)
                        parsed = json.loads(payload.decode("utf-8", errors="replace"))
                    except Exception:
                        parsed = {}
                    if isinstance(parsed, dict):
                        max_importance_score = max(max_importance_score, _safe_int(parsed.get("importance_score", 0), 0))
            return {"match_count": total, "max_importance_score": max_importance_score}
    except Exception:
        return {"match_count": 0, "max_importance_score": 0}


def _load_extractor_match_rows_from_zip(
    *,
    root_domain: str,
    data: bytes,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    with zipfile.ZipFile(io.BytesIO(data), mode="r") as zf:
        infos = sorted(zf.infolist(), key=lambda item: item.filename.lower())
        for info in infos:
            if info.is_dir():
                continue
            member_path = _normalize_zip_member_path(info.filename)
            if not member_path:
                continue
            files.append(
                {
                    "path": member_path,
                    "size": int(info.file_size or 0),
                    "compressed_size": int(info.compress_size or 0),
                }
            )
            if not member_path.lower().endswith(".json"):
                continue
            name = member_path.split("/")[-1].lower()
            if not name.startswith("m_"):
                continue
            try:
                payload = zf.read(info)
                parsed = json.loads(payload.decode("utf-8", errors="replace"))
            except Exception:
                continue
            if not isinstance(parsed, dict):
                continue
            matched_text = str(parsed.get("matched_text", "") or "")
            rows.append(
                {
                    "root_domain": root_domain,
                    "match_file": member_path,
                    "filter_name": str(parsed.get("filter_name", "") or ""),
                    "importance_score": _safe_int(parsed.get("importance_score", 0), 0),
                    "scope": str(parsed.get("scope", "") or ""),
                    "response_side": str(parsed.get("response_side", "") or ""),
                    "regex": str(parsed.get("regex", "") or ""),
                    "url": str(parsed.get("url", "") or ""),
                    "source_request_url": str(parsed.get("source_request_url", "") or ""),
                    "source_http_status": _safe_int(parsed.get("source_http_status", 0), 0),
                    "result_type": str(parsed.get("result_type", "") or ""),
                    "result_file": str(parsed.get("result_file", "") or ""),
                    "matched_text": matched_text,
                    "match_preview": _to_preview_text(matched_text),
                    "generated_at_utc": str(parsed.get("generated_at_utc", "") or ""),
                }
            )
    return rows, files


def _is_client_disconnect_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, TimeoutError)):
        return True
    if isinstance(exc, OSError):
        errno_value = getattr(exc, "errno", None)
        if errno_value in {32, 54, 104, 110}:
            return True
    if isinstance(exc, ssl.SSLError):
        msg = str(exc).lower()
        if "eof occurred in violation of protocol" in msg:
            return True
        if "wrong version number" in msg:
            return True
        if "tlsv1 alert" in msg:
            return True
    return False


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "NightmareServer/1.0"

    @property
    def app_root(self) -> Path:
        return self.server.app_root  # type: ignore[attr-defined]

    @property
    def output_root(self) -> Path:
        return self.server.output_root  # type: ignore[attr-defined]

    @property
    def coordinator_store(self) -> CoordinatorStore | None:
        return getattr(self.server, "coordinator_store", None)  # type: ignore[attr-defined]

    @property
    def log_store(self) -> LogStore | None:
        return getattr(self.server, "log_store", None)  # type: ignore[attr-defined]

    @property
    def extractor_matches_cache(self) -> _ExtractorMatchesCache:
        return getattr(self.server, "extractor_matches_cache")  # type: ignore[attr-defined]

    @property
    def fuzzing_summary_cache(self) -> _FozzySummaryCache:
        return getattr(self.server, "fuzzing_summary_cache")  # type: ignore[attr-defined]

    @property
    def fuzzing_zip_index_cache(self) -> _ZipFileIndexCache:
        return getattr(self.server, "fuzzing_zip_index_cache")  # type: ignore[attr-defined]

    @property
    def page_data_cache(self) -> _PageDataCache:
        return getattr(self.server, "page_data_cache")  # type: ignore[attr-defined]

    @property
    def coordinator_token(self) -> str:
        return str(getattr(self.server, "coordinator_token", "") or "")  # type: ignore[attr-defined]

    @property
    def master_report_regen_token(self) -> str:
        return str(getattr(self.server, "master_report_regen_token", "") or "").strip()  # type: ignore[attr-defined]

    def _get_workflow_interface_catalog(self) -> dict[str, Any]:
        cached = getattr(self.server, "workflow_interface_catalog", None)  # type: ignore[attr-defined]
        if isinstance(cached, dict) and isinstance(cached.get("interfaces"), list) and isinstance(cached.get("routes"), dict):
            return cached
        fresh = _workflow_interface_catalog_payload()
        setattr(self.server, "workflow_interface_catalog", fresh)  # type: ignore[attr-defined]
        return fresh

    def _refresh_workflow_interface_catalog(self) -> dict[str, Any]:
        fresh = _workflow_interface_catalog_payload()
        setattr(self.server, "workflow_interface_catalog", fresh)  # type: ignore[attr-defined]
        return fresh

    def _render_workflow_interface_page(self, interface_entry: dict[str, Any]) -> str:
        workflow_id = str(interface_entry.get("workflow_id") or "").strip().lower()
        workflow_path = _resolve_workflow_path(workflow_id) if workflow_id else None
        workflow_payload = _load_workflow_payload(workflow_path) if isinstance(workflow_path, Path) else {}
        return render_template(
            str(interface_entry.get("template") or ""),
            workflow_id=workflow_id,
            workflow=workflow_payload,
            workflow_interface=interface_entry,
        )

    def log_message(self, format: str, *args: Any) -> None:
        # Keep server output concise.
        message = format % args
        sys.stdout.write("[server] " + message + "\n")
        try:
            if self.log_store is not None:
                now_utc = datetime.now(timezone.utc)
                self.log_store.insert_events(
                    [
                        {
                            "event_time_utc": now_utc.isoformat(),
                            "event_time_est": _format_est_datetime(now_utc),
                            "severity": "info",
                            "description": message,
                            "machine": "central_server",
                            "source_id": "server:http",
                            "source_type": "server_http",
                            "raw_line": message,
                        }
                    ]
                )
        except Exception:
            pass

    def handle(self) -> None:
        try:
            super().handle()
        except Exception as exc:
            # Public HTTP(S) listeners receive frequent scanner/disconnect traffic;
            # avoid noisy tracebacks for expected connection aborts.
            if _is_client_disconnect_error(exc):
                return
            raise

    def _is_same_origin_value(self, value: str) -> bool:
        raw = str(value or "").strip()
        if not raw:
            return True
        try:
            parsed = urlparse(raw)
        except Exception:
            return False
        host = str(self.headers.get("Host", "") or "").strip().lower()
        origin_host = str(parsed.netloc or "").strip().lower()
        return bool(host and origin_host == host)

    def _send_cors_headers(self) -> None:
        origin = str(self.headers.get("Origin", "") or "").strip()
        if origin and self._is_same_origin_value(origin):
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Coordinator-Token, X-Master-Report-Token")

    def _reject_cross_site_post(self) -> bool:
        origin = str(self.headers.get("Origin", "") or "").strip()
        referer = str(self.headers.get("Referer", "") or "").strip()
        if origin and not self._is_same_origin_value(origin):
            self._write_json({"error": "cross-site POST rejected"}, status=403)
            return True
        if not origin and referer and not self._is_same_origin_value(referer):
            self._write_json({"error": "cross-site POST rejected"}, status=403)
            return True
        return False

    def _write_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self._send_cors_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _resolve_cached_page_payload(
        self,
        *,
        page_name: str,
        key_parts: dict[str, Any],
        ttl_seconds: int,
        cache_mode: str,
        loader: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        mode = _normalize_page_cache_mode(cache_mode)
        cache_key = _build_page_cache_key(page_name, key_parts)
        if mode == "prefer":
            cached = self.page_data_cache.get(cache_key)
            if cached is not None:
                return _attach_page_cache_metadata(
                    cached.get("payload", {}),
                    page_name=page_name,
                    cache_key=cache_key,
                    cache_mode=mode,
                    source="cache",
                    stale=bool(cached.get("stale", False)),
                    cached_at_epoch=float(cached.get("cached_at_epoch", 0.0) or 0.0),
                    expires_at_epoch=float(cached.get("expires_at_epoch", 0.0) or 0.0),
                )
        try:
            live_payload = loader()
        except Exception as exc:
            stale = self.page_data_cache.get(cache_key, allow_stale=True)
            if stale is None:
                raise
            self.log_message(
                "page cache fallback for %s (mode=%s): %r",
                str(page_name or "").strip().lower(),
                mode,
                exc,
            )
            return _attach_page_cache_metadata(
                stale.get("payload", {}),
                page_name=page_name,
                cache_key=cache_key,
                cache_mode=mode,
                source="stale_cache",
                stale=True,
                cached_at_epoch=float(stale.get("cached_at_epoch", 0.0) or 0.0),
                expires_at_epoch=float(stale.get("expires_at_epoch", 0.0) or 0.0),
                error_message=str(exc),
            )
        stored = self.page_data_cache.set(cache_key, live_payload, ttl_seconds=ttl_seconds)
        return _attach_page_cache_metadata(
            live_payload,
            page_name=page_name,
            cache_key=cache_key,
            cache_mode=mode,
            source="live",
            stale=False,
            cached_at_epoch=float(stored.get("cached_at_epoch", 0.0) or 0.0),
            expires_at_epoch=float(stored.get("expires_at_epoch", 0.0) or 0.0),
        )

    def _write_text(self, text: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> None:
        body = text.encode("utf-8", errors="replace")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self._send_cors_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_bytes(
        self,
        payload: bytes,
        *,
        status: int = 200,
        content_type: str = "application/octet-stream",
        download_name: Optional[str] = None,
    ) -> None:
        data = bytes(payload or b"")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self._send_cors_headers()
        if download_name:
            safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(download_name))
            self.send_header("Content-Disposition", f'attachment; filename="{safe_name}"')
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_static_file(self, file_path: Path) -> None:
        if not file_path.is_file():
            self._write_text("Not found", status=404)
            return
        content_type, _ = mimetypes.guess_type(str(file_path))
        if not content_type:
            content_type = "application/octet-stream"
        try:
            size = file_path.stat().st_size
            fh = file_path.open("rb")
        except OSError:
            self._write_text("Read error", status=500)
            return
        with fh:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self._send_cors_headers()
            self.send_header("Content-Length", str(size))
            self.end_headers()
            shutil.copyfileobj(fh, self.wfile, length=1024 * 1024)

    def _read_json_body(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0") or 0)
        except Exception:
            length = 0
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            parsed = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _is_coordinator_authorized(self) -> bool:
        token = self.coordinator_token.strip()
        if not token:
            return True
        authz = str(self.headers.get("Authorization", "") or "").strip()
        x_token = str(self.headers.get("X-Coordinator-Token", "") or "").strip()
        if authz.lower().startswith("bearer "):
            candidate = authz[7:].strip()
            if candidate == token:
                return True
        if x_token == token:
            return True
        cookie_token = self._read_cookie("nightmare_coord_token").strip()
        if cookie_token == token:
            return True
        return False

    def _read_cookie(self, name: str) -> str:
        cookie_header = str(self.headers.get("Cookie", "") or "")
        if not cookie_header:
            return ""
        for raw_part in cookie_header.split(";"):
            part = raw_part.strip()
            if not part or "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key.strip() != name:
                continue
            decoded = unquote(value.strip())
            if len(decoded) >= 2 and decoded[0] == decoded[-1] == '"':
                return decoded[1:-1]
            return decoded
        return ""

    def _is_master_report_regen_authorized(self) -> bool:
        expected = self.master_report_regen_token
        if not expected:
            return False
        authz = str(self.headers.get("Authorization", "") or "").strip()
        if authz.lower().startswith("bearer "):
            if authz[7:].strip() == expected:
                return True
        x_coord = str(self.headers.get("X-Coordinator-Token", "") or "").strip()
        x_master = str(self.headers.get("X-Master-Report-Token", "") or "").strip()
        return x_coord == expected or x_master == expected

    def _handle_regenerate_master_report(self) -> None:
        if not self.master_report_regen_token:
            self._write_json(
                {
                    "error": (
                        "master report regeneration is not configured "
                        "(set master_report_regen_token, MASTER_REPORT_REGEN_TOKEN, or coordinator_api_token)"
                    ),
                },
                status=503,
            )
            return
        if not self._is_master_report_regen_authorized():
            self._write_json({"error": "unauthorized"}, status=401)
            return
        lock = getattr(self.server, "_master_regen_lock", None)
        acquired = False
        if lock is not None:
            acquired = lock.acquire(blocking=False)
            if not acquired:
                self._write_json({"error": "regeneration already in progress"}, status=409)
                return
        try:
            fozzy = self.app_root / "fozzy.py"
            if not fozzy.is_file():
                self._write_json({"error": "fozzy.py not found next to server.py"}, status=500)
                return
            out_root = self.output_root.resolve()
            if not out_root.is_dir():
                self._write_json({"error": f"output_root is not a directory: {out_root}"}, status=500)
                return
            cmd = [sys.executable, str(fozzy), "--generate-master-report", str(out_root)]
            env = {**os.environ, "PYTHONUTF8": "1"}
            try:
                proc = subprocess.run(
                    cmd,
                    cwd=str(self.app_root),
                    capture_output=True,
                    text=True,
                    timeout=3600,
                    env=env,
                )
            except subprocess.TimeoutExpired:
                self._write_json({"error": "fozzy.py timed out after 3600s"}, status=504)
                return
            except OSError as exc:
                self._write_json({"error": f"failed to run fozzy.py: {exc}"}, status=500)
                return
            if proc.returncode != 0:
                tail = (proc.stderr or "")[-8000:]
                self._write_json({"ok": False, "exit_code": proc.returncode, "stderr_tail": tail}, status=500)
                return
            json_path = out_root / "all_domains.results_summary.json"
            html_path = out_root / "all_domains.results_summary.html"
            self.log_message("regenerated master report under %s", str(out_root))
            self._write_json(
                {
                    "ok": True,
                    "output_root": str(out_root),
                    "json_path": str(json_path) if json_path.is_file() else None,
                    "html_path": str(html_path) if html_path.is_file() else None,
                    "stdout_tail": (proc.stdout or "")[-4000:],
                }
            )
        finally:
            if acquired and lock is not None:
                lock.release()

    def do_OPTIONS(self) -> None:  # noqa: N802
        if not self._is_same_origin_value(str(self.headers.get("Origin", "") or "")):
            self.send_response(403)
            self.end_headers()
            return
        self.send_response(204)
        self._send_cors_headers()
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            self._write_text(render_workers_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/all-domains-report":
            all_domains_html = _find_all_domains_report_html(self.output_root)
            if all_domains_html is None:
                self._write_text("Not found", status=404)
                return
            self._serve_static_file(all_domains_html)
            return
        if path == "/dashboard":
            self._write_text("Dashboard page removed. Use /workers.", status=404)
            return
        if path == "/operations":
            self._write_text("Operations page removed.", status=404)
            return
        if path == "/workers":
            self._write_text(render_workers_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/http-requests":
            self._write_text(render_http_requests_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/database":
            self._write_text(render_database_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/events":
            self._write_text("Events page removed.", status=404)
            return
        if path == "/workflows":
            self._write_text(render_workflows_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/workflow-definitions":
            self._write_text(render_workflow_definitions_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/workflow-runs":
            self._write_text("Workflow Runs page removed.", status=404)
            return
        if path == "/plugin-definitions":
            self._write_text("Plugins page removed.", status=404)
            return
        if path == "/crawl-progress":
            self._write_text("Crawl Progress moved to Recon Results.", status=404)
            return
        if path == "/extractor-matches":
            self._write_text("Extractor Matches moved to Recon Results.", status=404)
            return
        if path == "/fuzzing":
            self._write_text("Fuzzing page removed.", status=404)
            return
        if path == "/docker-status":
            self._write_text(render_docker_status_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/view-logs":
            self._write_text(render_view_logs_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/errors":
            self._write_text("Errors page removed.", status=404)
            return
        if path == "/discovered-targets":
            self._write_text("Discovered Targets moved to Recon Results.", status=404)
            return
        if path == "/discovered-target-response":
            self._write_text(render_discovered_target_response_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/discovered-files":
            self._write_text("Discovered Files moved to Recon Results.", status=404)
            return
        if path == "/auth0r":
            self._write_text("Auth0r page removed.", status=404)
            return
        interface_catalog = self._get_workflow_interface_catalog()
        interface_route_map = interface_catalog.get("routes") if isinstance(interface_catalog.get("routes"), dict) else {}
        interface_entry = interface_route_map.get(path) if isinstance(interface_route_map, dict) else None
        if isinstance(interface_entry, dict):
            try:
                page_html = self._render_workflow_interface_page(interface_entry)
            except Exception as exc:
                self._write_text(f"Workflow interface render failed: {exc}", status=500)
                return
            self._write_text(page_html, content_type="text/html; charset=utf-8")
            return

        if path == "/api/coord/http-requests":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            limit = max(1, min(5000, _safe_int((query.get("limit") or [500])[0], 500)))
            offset = max(0, _safe_int((query.get("offset") or [0])[0], 0))
            search_text = str((query.get("q") or [""])[0] or "").strip()
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            cache_mode = str((query.get("cache_mode") or ["prefer"])[0] or "prefer").strip().lower()
            cache_key_parts = {
                "limit": limit,
                "offset": offset,
                "q": search_text,
                "root_domain": root_domain,
            }
            try:
                payload = self._resolve_cached_page_payload(
                    page_name="http_requests",
                    key_parts=cache_key_parts,
                    ttl_seconds=HTTP_REQUESTS_PAGE_CACHE_TTL_SECONDS,
                    cache_mode=cache_mode,
                    loader=lambda: _build_http_requests_payload(
                        self.coordinator_store,  # type: ignore[arg-type]
                        limit=limit,
                        offset=offset,
                        search_text=search_text,
                        root_domain=root_domain,
                    ),
                )
            except Exception as exc:
                self.log_message("list_http_requests failed: %r", exc)
                self._write_json({"error": "http requests query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/discovered-targets":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            limit = max(1, min(2000, _safe_int((query.get("limit") or [200])[0], 200)))
            offset = max(0, _safe_int((query.get("offset") or [0])[0], 0))
            search_text = str((query.get("q") or [""])[0] or "").strip()
            sort_key = str((query.get("sort_key") or ["saved_at_utc"])[0] or "saved_at_utc").strip().lower()
            sort_dir = str((query.get("sort_dir") or ["desc"])[0] or "desc").strip().lower()
            cache_mode = str((query.get("cache_mode") or ["prefer"])[0] or "prefer").strip().lower()
            cache_key_parts = {
                "q": search_text,
                "offset": offset,
                "limit": limit,
                "sort_key": sort_key,
                "sort_dir": sort_dir,
            }
            try:
                payload = self._resolve_cached_page_payload(
                    page_name="discovered_targets",
                    key_parts=cache_key_parts,
                    ttl_seconds=DISCOVERED_TARGETS_PAGE_CACHE_TTL_SECONDS,
                    cache_mode=cache_mode,
                    loader=lambda: _build_discovered_target_domains_payload(
                        self.coordinator_store,  # type: ignore[arg-type]
                        limit=limit,
                        offset=offset,
                        search_text=search_text,
                        sort_key=sort_key,
                        sort_dir=sort_dir,
                    ),
                )
            except Exception as exc:
                self.log_message("list_discovered_target_domains failed: %r", exc)
                self._write_json({"error": "discovered targets query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/discovered-target-sitemap":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            if not root_domain:
                self._write_json({"error": "root_domain is required"}, status=400)
                return
            limit = max(1, min(2000, _safe_int((query.get("limit") or [200])[0], 200)))
            offset = max(0, _safe_int((query.get("offset") or [0])[0], 0))
            search_text = str((query.get("q") or [""])[0] or "").strip()
            subdomain = str((query.get("subdomain") or [""])[0] or "").strip()
            sort_key = str((query.get("sort_key") or [DISCOVERED_TARGET_SITEMAP_DEFAULT_SORT_KEY])[0] or DISCOVERED_TARGET_SITEMAP_DEFAULT_SORT_KEY).strip()
            sort_dir = str((query.get("sort_dir") or ["asc"])[0] or "asc").strip()
            include_details = str((query.get("include_details") or ["1"])[0] or "1").strip().lower() in {"1", "true", "yes", "on"}
            cache_mode = str((query.get("cache_mode") or ["prefer"])[0] or "prefer").strip().lower()
            cache_key_parts = {
                "root_domain": root_domain,
                "q": search_text,
                "subdomain": subdomain,
                "offset": offset,
                "limit": limit,
                "sort_key": sort_key,
                "sort_dir": sort_dir,
                "include_details": include_details,
            }
            try:
                payload = self._resolve_cached_page_payload(
                    page_name="discovered_target_sitemap",
                    key_parts=cache_key_parts,
                    ttl_seconds=DISCOVERED_TARGET_SITEMAP_PAGE_CACHE_TTL_SECONDS,
                    cache_mode=cache_mode,
                    loader=lambda: _build_discovered_target_sitemap_payload(
                        self.coordinator_store,  # type: ignore[arg-type]
                        root_domain=root_domain,
                        limit=limit,
                        offset=offset,
                        search_text=search_text,
                        subdomain=subdomain,
                        sort_key=sort_key,
                        sort_dir=sort_dir,
                        include_details=include_details,
                    ),
                )
            except Exception as exc:
                self.log_message("get_discovered_target_sitemap failed: %r", exc)
                self._write_json({"error": "discovered target sitemap query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/discovered-target-response":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            url = str((query.get("url") or [""])[0] or "").strip()
            if not root_domain or not url:
                self._write_json({"error": "root_domain and url are required"}, status=400)
                return
            try:
                payload = self.coordinator_store.get_discovered_target_response(root_domain, url)
            except Exception as exc:
                self.log_message("get_discovered_target_response failed: %r", exc)
                self._write_json({"error": "discovered target response query failed", "detail": str(exc)}, status=500)
                return
            if not payload.get("found"):
                self._write_json(payload, status=404)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/discovered-target-download":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            url = str((query.get("url") or [""])[0] or "").strip()
            part = str((query.get("part") or ["evidence_json"])[0] or "evidence_json").strip().lower()
            if not root_domain or not url:
                self._write_json({"error": "root_domain and url are required"}, status=400)
                return
            try:
                payload = self.coordinator_store.get_discovered_target_response(root_domain, url)
            except Exception as exc:
                self.log_message("get_discovered_target_response failed (download): %r", exc)
                self._write_json({"error": "discovered target response query failed", "detail": str(exc)}, status=500)
                return
            if not payload.get("found"):
                self._write_json(payload, status=404)
                return
            url_token = base64.urlsafe_b64encode(url.encode("utf-8")).decode("ascii").strip("=").lower()[:20] or "url"
            if part == "evidence_json":
                self._write_bytes(
                    json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                    content_type="application/json; charset=utf-8",
                    download_name=f"{root_domain}.{url_token}.evidence.json",
                )
                return
            if part == "request_json":
                request_payload = payload.get("request") if isinstance(payload.get("request"), dict) else {}
                self._write_bytes(
                    json.dumps(request_payload, ensure_ascii=False, indent=2).encode("utf-8"),
                    content_type="application/json; charset=utf-8",
                    download_name=f"{root_domain}.{url_token}.request.json",
                )
                return
            if part == "response_json":
                response_payload = payload.get("response") if isinstance(payload.get("response"), dict) else {}
                self._write_bytes(
                    json.dumps(response_payload, ensure_ascii=False, indent=2).encode("utf-8"),
                    content_type="application/json; charset=utf-8",
                    download_name=f"{root_domain}.{url_token}.response.json",
                )
                return
            if part in {"request_body", "response_body"}:
                container_key = "request" if part == "request_body" else "response"
                container = payload.get(container_key) if isinstance(payload.get(container_key), dict) else {}
                body_b64 = str(container.get("body_base64") or "")
                if not body_b64:
                    self._write_json({"error": f"{container_key} body is not available"}, status=404)
                    return
                try:
                    body = base64.b64decode(body_b64)
                except Exception:
                    self._write_json({"error": f"{container_key} body is not valid base64"}, status=500)
                    return
                content_type = "application/octet-stream"
                if part == "response_body":
                    headers = container.get("headers") if isinstance(container.get("headers"), dict) else {}
                    for key, value in headers.items():
                        if str(key or "").strip().lower() == "content-type":
                            content_type = str(value or "").strip() or content_type
                            break
                extension = ".bin"
                lowered = content_type.lower()
                if "json" in lowered:
                    extension = ".json"
                elif "html" in lowered:
                    extension = ".html"
                elif "text/" in lowered:
                    extension = ".txt"
                self._write_bytes(
                    body,
                    content_type=content_type,
                    download_name=f"{root_domain}.{url_token}.{part}{extension}",
                )
                return
            self._write_json({"error": "invalid part", "supported_parts": ["evidence_json", "request_json", "response_json", "request_body", "response_body"]}, status=400)
            return
        if path == "/api/coord/discovered-files":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            limit = _safe_int((query.get("limit") or [5000])[0], 5000)
            search_text = str((query.get("q") or [""])[0] or "").strip().lower()
            cache_mode = str((query.get("cache_mode") or ["prefer"])[0] or "prefer").strip().lower()
            cache_key_parts = {"limit": limit, "q": search_text}
            try:
                payload = self._resolve_cached_page_payload(
                    page_name="discovered_files",
                    key_parts=cache_key_parts,
                    ttl_seconds=DISCOVERED_FILES_PAGE_CACHE_TTL_SECONDS,
                    cache_mode=cache_mode,
                    loader=lambda: _build_discovered_files_payload(
                        self.coordinator_store,  # type: ignore[arg-type]
                        limit=limit,
                        search_text=search_text,
                    ),
                )
            except Exception as exc:
                self.log_message("list_discovered_files failed: %r", exc)
                self._write_json({"error": "discovered files query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/high-value-files":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            limit = _safe_int((query.get("limit") or [5000])[0], 5000)
            search_text = str((query.get("q") or [""])[0] or "").strip().lower()
            cache_mode = str((query.get("cache_mode") or ["prefer"])[0] or "prefer").strip().lower()
            cache_key_parts = {"limit": limit, "q": search_text}
            try:
                payload = self._resolve_cached_page_payload(
                    page_name="high_value_files",
                    key_parts=cache_key_parts,
                    ttl_seconds=DISCOVERED_FILES_PAGE_CACHE_TTL_SECONDS,
                    cache_mode=cache_mode,
                    loader=lambda: _build_high_value_files_payload(
                        self.coordinator_store,  # type: ignore[arg-type]
                        limit=limit,
                        search_text=search_text,
                    ),
                )
            except Exception as exc:
                self.log_message("list_high_value_files failed: %r", exc)
                self._write_json({"error": "high value files query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/discovered-files/download":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            artifact_type = str((query.get("artifact_type") or [""])[0] or "").strip().lower()
            artifact = self.coordinator_store.get_artifact(root_domain, artifact_type) if root_domain and artifact_type else None
            if artifact is None:
                self._write_json({"error": "artifact not found"}, status=404)
                return
            data = bytes(artifact.get("content", b"") or b"")
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", f'attachment; filename="{root_domain}.{artifact_type}.bin"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if path == "/api/coord/high-value-files/download":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            saved_relative = str((query.get("saved_relative") or [""])[0] or "").strip()
            artifact = self.coordinator_store.get_artifact(root_domain, "nightmare_high_value_zip") if root_domain else None
            if artifact is None:
                self._write_json({"error": "artifact not found"}, status=404)
                return
            data = bytes(artifact.get("content", b"") or b"")
            try:
                with zipfile.ZipFile(io.BytesIO(data), mode="r") as zf:
                    payload = zf.read(saved_relative)
            except Exception as exc:
                self._write_json({"error": "file not found", "detail": str(exc)}, status=404)
                return
            name = Path(saved_relative).name or "download.bin"
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", f'attachment; filename="{name}"')
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if path == "/api/coord/auth0r/overview":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            completed_only = str((query.get("completed_only") or ["0"])[0] or "0").strip().lower() in {"1","true","yes","on"}
            limit = _safe_int((query.get("limit") or [5000])[0], 5000)
            try:
                payload = self.coordinator_store.auth0r_overview(completed_only=completed_only, limit=limit)
            except Exception as exc:
                self.log_message("auth0r_overview failed: %r", exc)
                self._write_json({"error": "auth0r overview query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/summary":
            payload = collect_dashboard_data(self.output_root, self.coordinator_store)
            self._write_json(payload)
            return
        if path == "/api/errors":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            if self.log_store is None:
                self._write_json({"error": "structured log store is unavailable"}, status=503)
                return
            search = str((query.get("search") or [""])[0] or "").strip()
            machine = str((query.get("machine") or [""])[0] or "").strip()
            program_name = str((query.get("program_name") or [""])[0] or "").strip()
            component_name = str((query.get("component_name") or [""])[0] or "").strip()
            exception_type = str((query.get("exception_type") or [""])[0] or "").strip()
            limit = max(1, min(5000, _safe_int((query.get("limit") or [250])[0], 250)))
            offset = max(0, _safe_int((query.get("offset") or [0])[0], 0))
            sort_dir = str((query.get("sort_dir") or ["desc"])[0] or "desc").strip().lower()
            try:
                payload = self.log_store.query_error_events(
                    search=search,
                    machine=machine,
                    program_name=program_name,
                    component_name=component_name,
                    exception_type=exception_type,
                    limit=limit,
                    offset=offset,
                    sort_dir=sort_dir,
                )
            except Exception as exc:
                self.log_message("query_error_events failed: %r", exc)
                self._write_json({"error": "error event query failed", "detail": str(exc)}, status=500)
                return
            payload.update({"generated_at_utc": _iso_now(), "offset": offset, "limit": limit})
            self._write_json(payload)
            return
        if path == "/api/log-tail":
            rel = (query.get("path") or [""])[0]
            resolved = _normalize_and_validate_relative_path(self.app_root, unquote(rel))
            if resolved is None or not resolved.is_file():
                self._write_json({"error": "invalid log path"}, status=400)
                return
            self._write_json({"path": str(resolved), "tail": _read_tail_lines(resolved, lines=DEFAULT_VIEW_LOG_LINES)})
            return
        if path == "/api/plugin-definitions":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            try:
                seed_builtin_plugins(self.coordinator_store)
                self._write_json({"items": list_plugin_definitions(self.coordinator_store)})
            except Exception as exc:
                self._write_json({"error": "plugin definition query failed", "detail": str(exc)}, status=500)
            return
        if path == "/api/workflow-definitions":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            try:
                seed_builtin_plugins(self.coordinator_store)
                seed_builtin_workflows(self.coordinator_store)
                self._write_json({"items": list_workflow_definitions(self.coordinator_store)})
            except Exception as exc:
                self._write_json({"error": "workflow definition query failed", "detail": str(exc)}, status=500)
            return
        if path.startswith("/api/workflow-definitions/builtin/"):
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            key = unquote(path[len("/api/workflow-definitions/builtin/"):]).strip("/") or "run-recon"
            try:
                self._write_json({"item": load_builtin_workflow_definition(key)})
            except Exception as exc:
                self._write_json({"error": "built-in workflow import preview failed", "detail": str(exc)}, status=500)
            return
        if path.startswith("/api/workflow-definitions/"):
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            key = unquote(path[len("/api/workflow-definitions/"):]).strip("/")
            try:
                item = get_workflow_definition(self.coordinator_store, key)
            except Exception as exc:
                self._write_json({"error": "workflow definition query failed", "detail": str(exc)}, status=500)
                return
            if item is None:
                self._write_json({"error": "workflow definition not found"}, status=404)
                return
            self._write_json({"item": item})
            return
        if path == "/api/workflow-runs":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            limit = max(1, min(500, _safe_int((query.get("limit") or [100])[0], 100)))
            try:
                self._write_json({"items": list_workflow_runs(self.coordinator_store, limit=limit)})
            except Exception as exc:
                self._write_json({"error": "workflow run query failed", "detail": str(exc)}, status=500)
            return
        if path.startswith("/api/workflow-runs/"):
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            run_id = unquote(path[len("/api/workflow-runs/"):]).strip("/")
            try:
                item = get_workflow_run(self.coordinator_store, run_id)
            except Exception as exc:
                self._write_json({"error": "workflow run query failed", "detail": str(exc)}, status=500)
                return
            if item is None:
                self._write_json({"error": "workflow run not found"}, status=404)
                return
            self._write_json({"item": item})
            return
        if path == "/api/coord/database-status":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            try:
                payload = self.coordinator_store.database_status()
            except Exception as exc:
                self.log_message("database_status failed: %r", exc)
                self._write_json(
                    {
                        "error": "database status query failed",
                        "detail": str(exc),
                    },
                    status=500,
                )
                return
            self._write_json(payload)
            return
        if path == "/api/coord/docker-status":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            include_logs = str((query.get("include_logs") or ["1"])[0] or "1").strip().lower() in {"1", "true", "yes"}
            log_lines = max(1, min(MAX_VIEW_LOG_LINES, _safe_int((query.get("log_lines") or [120])[0], 120)))
            payload = _collect_docker_status(self.app_root, include_logs=include_logs, log_lines=log_lines)
            self._write_json(payload)
            return

        if path == "/api/coord/auth0r/profiles":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503); return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401); return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            auth0r_store = getattr(self.server, "auth0r_store", None)
            if auth0r_store is None:
                self._write_json({"error": "auth0r store unavailable"}, status=503); return
            self._write_json(auth0r_store.get_domain_profiles(root_domain))
            return
        if path == "/api/coord/auth0r/results":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503); return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401); return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            auth0r_store = getattr(self.server, "auth0r_store", None)
            if auth0r_store is None:
                self._write_json({"error": "auth0r store unavailable"}, status=503); return
            self._write_json(auth0r_store.get_domain_results(root_domain))
            return
        if path == "/api/coord/extractor-patterns":
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401); return
            rules_path, js_rules_path = _resolve_extractor_rule_paths(self.app_root)
            self._write_json({
                "generated_at_utc": _iso_now(),
                "rules_path": _to_repo_relative_path(rules_path) or str(rules_path),
                "javascript_rules_path": _to_repo_relative_path(js_rules_path) or str(js_rules_path),
                "rules": _load_extractor_rule_file(rules_path),
                "javascript_rules": _load_extractor_rule_file(js_rules_path),
            })
            return
        if path == "/api/coord/log-sources":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            force_refresh = str((query.get("force_refresh") or ["0"])[0] or "0").strip().lower() in {"1", "true", "yes"}
            sources = _collect_view_log_sources_cached(self.app_root, self.output_root, force_refresh=force_refresh)
            self._write_json(
                {
                    "generated_at_utc": _iso_now(),
                    "total_sources": len(sources),
                    "sources": sources,
                }
            )
            return
        if path == "/api/coord/log-source-diagnostics":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            force_refresh = str((query.get("force_refresh") or ["0"])[0] or "0").strip().lower() in {"1", "true", "yes"}
            lines = max(1, min(200, _safe_int((query.get("lines") or [20])[0], 20)))
            limit = max(1, min(500, _safe_int((query.get("limit") or [200])[0], 200)))
            payload = _diagnose_view_log_sources(
                self.app_root,
                self.output_root,
                lines=lines,
                force_refresh=force_refresh,
                limit=limit,
            )
            self._write_json(payload)
            return
        if path == "/api/coord/log-tail":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            source_id = str((query.get("source_id") or [""])[0] or "").strip()
            lines = max(
                1,
                min(
                    MAX_VIEW_LOG_LINES,
                    _safe_int((query.get("lines") or [DEFAULT_VIEW_LOG_LINES])[0], DEFAULT_VIEW_LOG_LINES),
                ),
            )
            source = _resolve_view_log_source(source_id, self.app_root, self.output_root)
            if source is None:
                self._write_json({"error": "unknown source_id"}, status=404)
                return
            ok, tail, error = _read_log_source_text(source, lines=lines, full=False, app_root=self.app_root)
            if not ok:
                self._write_json({"error": "log read failed", "detail": error, "source": source}, status=500)
                return
            self._write_json(
                {
                    "generated_at_utc": _iso_now(),
                    "source": source,
                    "lines_requested": lines,
                    "tail": tail,
                }
            )
            return
        if path == "/api/coord/log-events":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            source_id = str((query.get("source_id") or [""])[0] or "").strip()
            lines = max(1, min(MAX_VIEW_LOG_LINES, _safe_int((query.get("lines") or [DEFAULT_VIEW_LOG_LINES])[0], DEFAULT_VIEW_LOG_LINES)))
            search = str((query.get("search") or [""])[0] or "").strip()
            severity = str((query.get("severity") or [""])[0] or "").strip()
            machine = str((query.get("machine") or [""])[0] or "").strip()
            sort_key = str((query.get("sort_key") or ["event_time_utc"])[0] or "event_time_utc").strip()
            sort_dir = str((query.get("sort_dir") or ["desc"])[0] or "desc").strip().lower()
            limit = max(1, min(5000, _safe_int((query.get("limit") or [500])[0], 500)))
            offset = max(0, _safe_int((query.get("offset") or [0])[0], 0))
            if self.log_store is not None:
                try:
                    payload = self.log_store.query_events(
                        source_id=source_id,
                        search=search,
                        severity=severity,
                        machine=machine,
                        limit=limit,
                        offset=offset,
                        sort_dir=sort_dir,
                    )
                    db_total = int(payload.get("total", 0) or 0)
                    if db_total > 0:
                        payload.update({"generated_at_utc": _iso_now(), "source_id": source_id or "__all__", "from_log_db": True})
                        self._write_json(payload)
                        return
                except Exception as exc:
                    self.log_message("log_store query failed: %r", exc)
            sources: list[dict[str, Any]] = []
            if source_id and source_id != "__all__":
                src = _resolve_view_log_source(source_id, self.app_root, self.output_root)
                if src is not None:
                    sources = [src]
            else:
                sources = _collect_view_log_sources_cached(self.app_root, self.output_root)
            events: list[dict[str, Any]] = []
            for source in sources:
                ok, text, error = _read_log_source_text(source, lines=lines, full=bool(source_id and source_id != "__all__"), app_root=self.app_root)
                if not ok:
                    events.append(
                        {
                            "event_time_utc": datetime.now(timezone.utc).isoformat(),
                            "event_time_est": _format_est_datetime(datetime.now(timezone.utc)),
                            "severity": "error",
                            "description": f"log source read failed: {error}",
                            "machine": str(source.get("instance_id") or source.get("label") or "unknown"),
                            "source_id": str(source.get("source_id", "") or ""),
                            "source_type": str(source.get("source_type", "") or ""),
                            "raw_line": str(error or ""),
                        }
                    )
                    continue
                events.extend(_parse_log_events_from_text(text, source=source))
            if self.log_store is not None:
                try:
                    self.log_store.insert_events(events)
                except Exception as exc:
                    self.log_message("log_store insert failed: %r", exc)
            filtered = _filter_and_sort_log_events(
                events,
                search=search,
                severity=severity,
                machine=machine,
                sort_key=sort_key,
                sort_dir=sort_dir,
                offset=offset,
                limit=limit,
            )
            self._write_json(
                {
                    "generated_at_utc": _iso_now(),
                    "source_id": source_id or "__all__",
                    "from_log_db": False,
                    **filtered,
                }
            )
            return
        if path == "/api/coord/log-download":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            source_id = str((query.get("source_id") or [""])[0] or "").strip()
            if not source_id:
                self._write_json({"error": "source_id is required"}, status=400)
                return
            source = _resolve_view_log_source(source_id, self.app_root, self.output_root)
            if source is None:
                self._write_json({"error": "unknown source_id"}, status=404)
                return
            ok, text, error = _read_log_source_text(source, lines=MAX_VIEW_LOG_LINES, full=True, app_root=self.app_root)
            if not ok:
                self._write_json({"error": "log read failed", "detail": error, "source": source}, status=500)
                return
            safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(source_id))
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"{safe_name}.log", str(text or ""))
            self._write_bytes(
                zip_buffer.getvalue(),
                content_type="application/zip",
                download_name=f"{safe_name}.zip",
            )
            return

        if path == "/api/coord/events":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            search = str((query.get("search") or [""])[0] or "").strip()
            event_type = str((query.get("event_type") or [""])[0] or "").strip()
            aggregate_key = str((query.get("aggregate_key") or [""])[0] or "").strip()
            source = str((query.get("source") or [""])[0] or "").strip()
            sort_dir = str((query.get("sort_dir") or ["desc"])[0] or "desc").strip().lower()
            limit = max(1, min(5000, _safe_int((query.get("limit") or [250])[0], 250)))
            offset = max(0, _safe_int((query.get("offset") or [0])[0], 0))
            try:
                payload = self.coordinator_store.list_events(
                    limit=limit,
                    offset=offset,
                    search=search,
                    event_type=event_type,
                    aggregate_key=aggregate_key,
                    source=source,
                    sort_dir=sort_dir,
                )
            except Exception as exc:
                self.log_message("list_events failed: %r", exc)
                self._write_json({"error": "event query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return

        if path == "/api/coord/state":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            self._write_json(self.coordinator_store.status_summary())
            return
        if path == "/api/coord/workers":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            stale_after_seconds = _safe_int(
                (query.get("stale_after_seconds") or [DEFAULT_COORDINATOR_LEASE_SECONDS])[0],
                DEFAULT_COORDINATOR_LEASE_SECONDS,
            )
            try:
                payload = self.coordinator_store.worker_statuses(stale_after_seconds=stale_after_seconds)
            except Exception as exc:
                self.log_message("worker_statuses failed: %r", exc)
                self._write_json({"error": "worker status query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/worker-control":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            stale_after_seconds = _safe_int(
                (query.get("stale_after_seconds") or [DEFAULT_COORDINATOR_LEASE_SECONDS])[0],
                DEFAULT_COORDINATOR_LEASE_SECONDS,
            )
            try:
                snapshot = self.coordinator_store.worker_control_snapshot(stale_after_seconds=stale_after_seconds)
            except Exception as exc:
                self.log_message("worker_control_snapshot failed: %r", exc)
                self._write_json({"error": "worker control query failed", "detail": str(exc)}, status=500)
                return
            for worker in snapshot.get("workers", []):
                worker_id = str(worker.get("worker_id", "") or "").strip()
                worker["logs"] = self._discover_worker_log_links(worker_id)
                worker["config"] = self._resolve_worker_config_rel(worker_id)
            snapshot = _enrich_worker_snapshot_with_live_details(snapshot, log_store=self.log_store)
            self._write_json(snapshot)
            return
        if path == "/api/coord/crawl-progress":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            limit = _safe_int((query.get("limit") or [2000])[0], 2000)
            cache_mode = str((query.get("cache_mode") or ["prefer"])[0] or "prefer").strip().lower()
            try:
                payload = self._resolve_cached_page_payload(
                    page_name="crawl_progress",
                    key_parts={"limit": limit},
                    ttl_seconds=CRAWL_PROGRESS_PAGE_CACHE_TTL_SECONDS,
                    cache_mode=cache_mode,
                    loader=lambda: _build_crawl_progress_payload(
                        self.coordinator_store,  # type: ignore[arg-type]
                        limit=limit,
                    ),
                )
            except Exception as exc:
                self.log_message("crawl_progress_snapshot failed: %r", exc)
                self._write_json({"error": "crawl progress query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/extractor-matches/domains":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            limit = _safe_int((query.get("limit") or [20000])[0], 20000)
            search_text = str((query.get("q") or [""])[0] or "").strip().lower()
            try:
                rows = self.coordinator_store.list_extractor_match_domains(limit=limit)
            except Exception as exc:
                self.log_message("list_extractor_match_domains failed: %r", exc)
                self._write_json({"error": "extractor domain list failed", "detail": str(exc)}, status=500)
                return
            out: list[dict[str, Any]] = []
            fallback_budget = 200
            for item in rows:
                root_domain = str(item.get("root_domain", "") or "").strip().lower()
                if not root_domain:
                    continue
                if search_text and search_text not in root_domain:
                    continue
                content_sha256 = str(item.get("content_sha256", "") or "")
                match_count = item.get("summary_match_count")
                max_importance_score: Optional[int] = None
                cached_stats = self.extractor_matches_cache.get_domain_stats(root_domain, content_sha256)
                if cached_stats is not None:
                    if match_count is None:
                        match_count = cached_stats.get("match_count")
                    max_importance_score = cached_stats.get("max_importance_score")
                if (match_count is None or max_importance_score is None) and fallback_budget > 0:
                    try:
                        artifact = self.coordinator_store.get_artifact(root_domain, "extractor_matches_zip")
                    except Exception:
                        artifact = None
                    if artifact is not None:
                        fallback_budget -= 1
                        stats = _extractor_match_stats_from_zip_bytes(bytes(artifact.get("content", b"") or b""))
                        if match_count is None:
                            match_count = stats.get("match_count")
                        if max_importance_score is None:
                            max_importance_score = stats.get("max_importance_score")
                out.append(
                    {
                        "root_domain": root_domain,
                        "match_count": int(match_count or 0),
                        "max_importance_score": int(max_importance_score or 0),
                        "content_size_bytes": int(item.get("content_size_bytes", 0) or 0),
                        "updated_at_utc": item.get("updated_at_utc"),
                        "source_worker": item.get("source_worker"),
                        "cached": cached_stats is not None,
                    }
                )
            out.sort(
                key=lambda row: (
                    -_safe_int(row.get("max_importance_score", 0), 0),
                    -_safe_int(row.get("match_count", 0), 0),
                    str(row.get("root_domain", "")).lower(),
                )
            )
            self._write_json(
                {
                    "generated_at_utc": _iso_now(),
                    "total_domains": len(out),
                    "domains": out,
                }
            )
            return
        if path == "/api/coord/extractor-matches":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            search_text = str((query.get("q") or [""])[0] or "").strip().lower()
            limit = max(
                1,
                min(
                    MAX_EXTRACTOR_MATCH_LIMIT,
                    _safe_int((query.get("limit") or [DEFAULT_EXTRACTOR_MATCH_LIMIT])[0], DEFAULT_EXTRACTOR_MATCH_LIMIT),
                ),
            )
            offset = max(0, _safe_int((query.get("offset") or [0])[0], 0))
            sort_key = str((query.get("sort_key") or [EXTRACTOR_MATCH_DEFAULT_SORT_KEY])[0] or EXTRACTOR_MATCH_DEFAULT_SORT_KEY).strip()
            sort_dir = str((query.get("sort_dir") or ["desc"])[0] or "desc").strip().lower()
            column_filters: dict[str, str] = {}
            for key in sorted(EXTRACTOR_MATCH_FILTER_KEYS):
                raw = str((query.get(f"f_{key}") or [""])[0] or "").strip()
                if raw:
                    column_filters[key] = raw

            domains_to_load: list[str] = []
            if root_domain and root_domain != "__all__":
                domains_to_load = [root_domain]
            else:
                domain_rows = self.coordinator_store.list_extractor_match_domains(limit=20000)
                domains_to_load = [str(item.get("root_domain", "")).strip().lower() for item in domain_rows if str(item.get("root_domain", "")).strip()]

            all_rows: list[dict[str, Any]] = []
            files_by_domain: dict[str, list[dict[str, Any]]] = {}
            cache_stats = {"hits": 0, "misses": 0}
            for domain in domains_to_load:
                try:
                    loaded = self._load_extractor_matches_domain(domain)
                except Exception as exc:
                    self.log_message("extractor matches load failed for %s: %r", domain, exc)
                    continue
                if loaded is None:
                    continue
                if loaded.get("cached"):
                    cache_stats["hits"] += 1
                else:
                    cache_stats["misses"] += 1
                rows = loaded.get("rows", [])
                if isinstance(rows, list):
                    all_rows.extend(rows)
                files = loaded.get("files", [])
                if isinstance(files, list):
                    files_by_domain[domain] = files

            query_result = _apply_extractor_row_query(
                all_rows,
                search_text=search_text,
                column_filters=column_filters,
                sort_key=sort_key,
                sort_dir=sort_dir,
                offset=offset,
                limit=limit,
            )
            filtered_rows = query_result.get("rows", [])
            filtered_rows_for_stats = query_result.get("filtered_rows_for_stats", [])
            total_rows = int(query_result.get("total_rows", 0) or 0)
            top_filters = _top_extractor_filters(
                filtered_rows_for_stats if isinstance(filtered_rows_for_stats, list) else filtered_rows,
                top_n=10,
            )
            self._write_json(
                {
                    "generated_at_utc": _iso_now(),
                    "root_domain": root_domain or "__all__",
                    "search": search_text,
                    "total_rows": total_rows,
                    "rows_returned": len(filtered_rows),
                    "rows_limited": total_rows > len(filtered_rows),
                    "offset": int(query_result.get("offset", 0) or 0),
                    "limit": int(query_result.get("limit", limit) or limit),
                    "next_offset": query_result.get("next_offset"),
                    "prev_offset": query_result.get("prev_offset"),
                    "has_more": bool(query_result.get("has_more", False)),
                    "sort_key": str(query_result.get("sort_key", EXTRACTOR_MATCH_DEFAULT_SORT_KEY) or EXTRACTOR_MATCH_DEFAULT_SORT_KEY),
                    "sort_dir": str(query_result.get("sort_dir", "desc") or "desc"),
                    "column_filters": query_result.get("column_filters", {}),
                    "cache": cache_stats,
                    "top_filters": top_filters,
                    "rows": filtered_rows,
                    "files_by_domain": files_by_domain if root_domain and root_domain != "__all__" else {},
                }
            )
            return
        if path == "/api/coord/extractor-matches/download":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            if not root_domain:
                self._write_json({"error": "root_domain is required"}, status=400)
                return
            artifact = self.coordinator_store.get_artifact(root_domain, "extractor_matches_zip")
            if artifact is None:
                self._write_json({"error": "extractor_matches_zip not found"}, status=404)
                return
            self._write_bytes(
                bytes(artifact.get("content", b"") or b""),
                content_type="application/zip",
                download_name=f"{root_domain}.extractor.matches.zip",
            )
            return
        if path == "/api/coord/extractor-matches/file":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            member_path = _normalize_zip_member_path(str((query.get("path") or [""])[0] or ""))
            as_download = str((query.get("download") or ["0"])[0] or "0").strip() in {"1", "true", "yes"}
            if not root_domain or not member_path:
                self._write_json({"error": "root_domain and path are required"}, status=400)
                return
            artifact = self.coordinator_store.get_artifact(root_domain, "extractor_matches_zip")
            if artifact is None:
                self._write_json({"error": "extractor_matches_zip not found"}, status=404)
                return
            data = bytes(artifact.get("content", b"") or b"")
            try:
                with zipfile.ZipFile(io.BytesIO(data), mode="r") as zf:
                    names = {_normalize_zip_member_path(name): name for name in zf.namelist()}
                    real_name = names.get(member_path)
                    if not real_name:
                        self._write_json({"error": "file not found in zip"}, status=404)
                        return
                    payload = zf.read(real_name)
            except Exception as exc:
                self._write_json({"error": "failed to read zip file", "detail": str(exc)}, status=500)
                return
            content_type = "application/octet-stream"
            if member_path.lower().endswith(".json"):
                content_type = "application/json; charset=utf-8"
            elif member_path.lower().endswith(".txt"):
                content_type = "text/plain; charset=utf-8"
            self._write_bytes(
                payload,
                content_type=content_type,
                download_name=(Path(member_path).name if as_download else None),
            )
            return
        if path == "/api/coord/fuzzing/domains":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            limit = _safe_int((query.get("limit") or [5000])[0], 5000)
            search_text = str((query.get("q") or [""])[0] or "").strip().lower()
            try:
                rows = self.coordinator_store.list_fozzy_summary_domains(limit=limit)
            except Exception as exc:
                self.log_message("list_fozzy_summary_domains failed: %r", exc)
                self._write_json({"error": "fuzzing domain list failed", "detail": str(exc)}, status=500)
                return
            out: list[dict[str, Any]] = []
            for item in rows:
                root_domain = str(item.get("root_domain", "") or "").strip().lower()
                if not root_domain:
                    continue
                if search_text and search_text not in root_domain:
                    continue
                totals = item.get("totals") if isinstance(item.get("totals"), dict) else {}
                anomalies = _safe_int(totals.get("anomalies", 0), 0)
                reflections = _safe_int(totals.get("reflections", 0), 0)
                out.append(
                    {
                        "root_domain": root_domain,
                        "anomalies": anomalies,
                        "reflections": reflections,
                        "findings": anomalies + reflections,
                        "groups": _safe_int(totals.get("groups", 0), 0),
                        "baseline_requests": _safe_int(totals.get("baseline_requests", 0), 0),
                        "fuzz_requests": _safe_int(totals.get("fuzz_requests", 0), 0),
                        "summary_updated_at_utc": item.get("summary_updated_at_utc"),
                        "has_results_zip": bool(item.get("has_results_zip", False)),
                    }
                )
            self._write_json(
                {
                    "generated_at_utc": _iso_now(),
                    "total_domains": len(out),
                    "domains": out,
                }
            )
            return
        if path == "/api/coord/fuzzing":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            search_text = str((query.get("q") or [""])[0] or "").strip().lower()
            limit = max(
                1,
                min(
                    MAX_FUZZING_RESULT_LIMIT,
                    _safe_int((query.get("limit") or [DEFAULT_FUZZING_RESULT_LIMIT])[0], DEFAULT_FUZZING_RESULT_LIMIT),
                ),
            )
            offset = max(0, _safe_int((query.get("offset") or [0])[0], 0))
            sort_key = str((query.get("sort_key") or [FUZZING_RESULT_DEFAULT_SORT_KEY])[0] or FUZZING_RESULT_DEFAULT_SORT_KEY).strip()
            sort_dir = str((query.get("sort_dir") or ["desc"])[0] or "desc").strip().lower()
            column_filters: dict[str, str] = {}
            for key in sorted(FUZZING_RESULT_FILTER_KEYS):
                raw = str((query.get(f"f_{key}") or [""])[0] or "").strip()
                if raw:
                    column_filters[key] = raw

            domains_to_load: list[str] = []
            if root_domain and root_domain != "__all__":
                domains_to_load = [root_domain]
            else:
                domain_rows = self.coordinator_store.list_fozzy_summary_domains(limit=20000)
                domains_to_load = [str(item.get("root_domain", "")).strip().lower() for item in domain_rows if str(item.get("root_domain", "")).strip()]

            all_rows: list[dict[str, Any]] = []
            cache_stats = {"hits": 0, "misses": 0}
            domain_totals: dict[str, dict[str, int]] = {}
            for domain in domains_to_load:
                try:
                    loaded = self._load_fuzzing_domain_summary(domain)
                except Exception as exc:
                    self.log_message("fuzzing summary load failed for %s: %r", domain, exc)
                    continue
                if loaded is None:
                    continue
                if loaded.get("cached"):
                    cache_stats["hits"] += 1
                else:
                    cache_stats["misses"] += 1
                rows = loaded.get("rows", [])
                if isinstance(rows, list):
                    all_rows.extend(rows)
                totals = loaded.get("totals") if isinstance(loaded.get("totals"), dict) else {}
                domain_totals[domain] = {
                    "anomalies": _safe_int(totals.get("anomalies", 0), 0),
                    "reflections": _safe_int(totals.get("reflections", 0), 0),
                    "groups": _safe_int(totals.get("groups", 0), 0),
                    "baseline_requests": _safe_int(totals.get("baseline_requests", 0), 0),
                    "fuzz_requests": _safe_int(totals.get("fuzz_requests", 0), 0),
                }

            query_result = _apply_fuzzing_row_query(
                all_rows,
                search_text=search_text,
                column_filters=column_filters,
                sort_key=sort_key,
                sort_dir=sort_dir,
                offset=offset,
                limit=limit,
            )
            page_rows = query_result.get("rows", [])
            filtered_rows_for_stats = query_result.get("filtered_rows_for_stats", [])
            filtered_rows_list = filtered_rows_for_stats if isinstance(filtered_rows_for_stats, list) else page_rows
            total_rows = int(query_result.get("total_rows", 0) or 0)
            filtered_anomalies = sum(1 for row in filtered_rows_list if str((row or {}).get("result_type", "")) == "anomaly")
            filtered_reflections = sum(1 for row in filtered_rows_list if str((row or {}).get("result_type", "")) == "reflection")

            files: list[dict[str, Any]] = []
            if root_domain and root_domain != "__all__":
                zip_index = self._load_fuzzing_zip_index(root_domain)
                if zip_index is not None:
                    files = zip_index.get("files", []) if isinstance(zip_index.get("files"), list) else []

            self._write_json(
                {
                    "generated_at_utc": _iso_now(),
                    "root_domain": root_domain or "__all__",
                    "search": search_text,
                    "total_rows": total_rows,
                    "rows_returned": len(page_rows),
                    "rows_limited": total_rows > len(page_rows),
                    "offset": int(query_result.get("offset", 0) or 0),
                    "limit": int(query_result.get("limit", limit) or limit),
                    "next_offset": query_result.get("next_offset"),
                    "prev_offset": query_result.get("prev_offset"),
                    "has_more": bool(query_result.get("has_more", False)),
                    "sort_key": str(query_result.get("sort_key", FUZZING_RESULT_DEFAULT_SORT_KEY) or FUZZING_RESULT_DEFAULT_SORT_KEY),
                    "sort_dir": str(query_result.get("sort_dir", "desc") or "desc"),
                    "column_filters": query_result.get("column_filters", {}),
                    "cache": cache_stats,
                    "counts": {
                        "filtered_anomalies": filtered_anomalies,
                        "filtered_reflections": filtered_reflections,
                        "filtered_findings": filtered_anomalies + filtered_reflections,
                    },
                    "domain_totals": domain_totals,
                    "rows": page_rows,
                    "files": files if root_domain and root_domain != "__all__" else [],
                }
            )
            return
        if path == "/api/coord/fuzzing/download":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            artifact_type = str((query.get("artifact_type") or ["fozzy_results_zip"])[0] or "fozzy_results_zip").strip().lower()
            if not root_domain:
                self._write_json({"error": "root_domain is required"}, status=400)
                return
            if artifact_type not in {"fozzy_results_zip", "fozzy_summary_json"}:
                self._write_json({"error": "artifact_type must be fozzy_results_zip or fozzy_summary_json"}, status=400)
                return
            artifact = self.coordinator_store.get_artifact(root_domain, artifact_type)
            if artifact is None:
                self._write_json({"error": f"{artifact_type} not found"}, status=404)
                return
            if artifact_type == "fozzy_results_zip":
                content_type = "application/zip"
                download_name = f"{root_domain}.fozzy.results.zip"
            else:
                content_type = "application/json; charset=utf-8"
                download_name = f"{root_domain}.fozzy.summary.json"
            self._write_bytes(bytes(artifact.get("content", b"") or b""), content_type=content_type, download_name=download_name)
            return
        if path == "/api/coord/fuzzing/file":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            member_path = _normalize_zip_member_path(str((query.get("path") or [""])[0] or ""))
            as_download = str((query.get("download") or ["0"])[0] or "0").strip().lower() in {"1", "true", "yes"}
            if not root_domain or not member_path:
                self._write_json({"error": "root_domain and path are required"}, status=400)
                return
            zip_index = self._load_fuzzing_zip_index(root_domain)
            if zip_index is None:
                self._write_json({"error": "fozzy_results_zip not found"}, status=404)
                return
            normalized_names = zip_index.get("normalized_names") if isinstance(zip_index.get("normalized_names"), dict) else {}
            normalized_names = normalized_names if isinstance(normalized_names, dict) else {}
            real_name = str(normalized_names.get(member_path, "") or "")
            if not real_name and normalized_names:
                member_lower = member_path.lower()
                base_lower = Path(member_path).name.lower()
                exact_basename_matches = [
                    str(original or "")
                    for norm, original in normalized_names.items()
                    if Path(str(norm or "")).name.lower() == base_lower
                ]
                if len(exact_basename_matches) == 1:
                    real_name = exact_basename_matches[0]
                if not real_name:
                    suffix = f"/{member_lower}"
                    suffix_matches = [
                        str(original or "")
                        for norm, original in normalized_names.items()
                        if str(norm or "").lower().endswith(suffix)
                    ]
                    if len(suffix_matches) == 1:
                        real_name = suffix_matches[0]
            if not real_name:
                self._write_json({"error": "file not found in zip"}, status=404)
                return
            artifact = self.coordinator_store.get_artifact(root_domain, "fozzy_results_zip")
            if artifact is None:
                self._write_json({"error": "fozzy_results_zip not found"}, status=404)
                return
            payload = b""
            try:
                with zipfile.ZipFile(io.BytesIO(bytes(artifact.get("content", b"") or b"")), mode="r") as zf:
                    payload = zf.read(real_name)
            except Exception as exc:
                self._write_json({"error": "failed to read zip file", "detail": str(exc)}, status=500)
                return
            content_type = "application/octet-stream"
            lowered = member_path.lower()
            if lowered.endswith(".json"):
                content_type = "application/json; charset=utf-8"
            elif lowered.endswith(".txt") or lowered.endswith(".log"):
                content_type = "text/plain; charset=utf-8"
            elif lowered.endswith(".html") or lowered.endswith(".htm"):
                content_type = "text/html; charset=utf-8"
            self._write_bytes(payload, content_type=content_type, download_name=(Path(member_path).name if as_download else None))
            return
        if path == "/api/coord/worker-config":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            worker_id = str((query.get("worker_id") or [""])[0] or "").strip()
            config_path = self._resolve_worker_config_path(worker_id)
            if config_path is None:
                self._write_json({"error": "worker config path not available"}, status=404)
                return
            if not config_path.exists():
                self._write_json(
                    {
                        "found": False,
                        "worker_id": worker_id,
                        "config_path_rel": _to_repo_relative_path(config_path),
                        "content": "",
                    }
                )
                return
            self._write_json(
                {
                    "found": True,
                    "worker_id": worker_id,
                    "config_path_rel": _to_repo_relative_path(config_path),
                    "content": config_path.read_text(encoding="utf-8"),
                }
            )
            return
        if path == "/api/coord/fleet-settings":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            self._write_json(self.coordinator_store.get_fleet_settings())
            return
        if path == "/api/coord/ui-preferences":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            page = str((query.get("page") or [""])[0] or "").strip().lower()
            pref_key = str((query.get("key") or [""])[0] or "").strip().lower()
            if not page or not pref_key:
                self._write_json({"error": "page and key are required"}, status=400)
                return
            try:
                payload = self.coordinator_store.get_ui_preference(page=page, pref_key=pref_key)
            except Exception as exc:
                self.log_message("get_ui_preference failed: %r", exc)
                self._write_json({"error": "ui preference query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/session":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            if not root_domain:
                self._write_json({"error": "root_domain is required"}, status=400)
                return
            payload = self.coordinator_store.load_session(root_domain)
            if payload is None:
                self._write_json({"found": False, "root_domain": root_domain})
                return
            self._write_json({"found": True, "session": payload})
            return
        if path == "/api/coord/artifact":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            artifact_type = str((query.get("artifact_type") or [""])[0] or "").strip().lower()
            if not root_domain or not artifact_type:
                self._write_json({"error": "root_domain and artifact_type are required"}, status=400)
                return
            artifact = self.coordinator_store.get_artifact(root_domain, artifact_type)
            if artifact is None:
                self._write_json({"found": False, "root_domain": root_domain, "artifact_type": artifact_type})
                return
            self._write_json(
                {
                    "found": True,
                    "artifact": {
                        "root_domain": artifact["root_domain"],
                        "artifact_type": artifact["artifact_type"],
                        "source_worker": artifact["source_worker"],
                        "content_encoding": artifact["content_encoding"],
                        "content_sha256": artifact["content_sha256"],
                        "content_size_bytes": artifact["content_size_bytes"],
                        "updated_at_utc": artifact["updated_at_utc"],
                        "content_base64": base64.b64encode(bytes(artifact["content"])).decode("ascii"),
                    },
                }
            )
            return
        if path == "/api/coord/artifacts":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            if not root_domain:
                self._write_json({"error": "root_domain is required"}, status=400)
                return
            self._write_json({"root_domain": root_domain, "artifacts": self.coordinator_store.list_artifacts(root_domain)})
            return
        if path == "/api/coord/workflow-interfaces":
            catalog = self._get_workflow_interface_catalog()
            self._write_json(
                {
                    "ok": True,
                    "generated_at_utc": str(catalog.get("generated_at_utc") or _iso_now()),
                    "interfaces": catalog.get("interfaces") if isinstance(catalog.get("interfaces"), list) else [],
                }
            )
            return
        if path == "/api/coord/workflow-interface/recon/domains":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            workflow_id = _normalize_workflow_id((query.get("workflow_id") or ["run-recon"])[0], default="run-recon")
            limit = max(1, min(10000, _safe_int((query.get("limit") or [5000])[0], 5000)))
            search_text = str((query.get("q") or [""])[0] or "").strip().lower()
            snapshot = self.coordinator_store.workflow_scheduler_snapshot(limit=max(limit, 5000))
            snapshot_domains = snapshot.get("domains") if isinstance(snapshot.get("domains"), list) else []
            snapshot_map: dict[str, dict[str, Any]] = {}
            for domain in snapshot_domains:
                if not isinstance(domain, dict):
                    continue
                root_domain = str(domain.get("root_domain") or "").strip().lower()
                if root_domain:
                    snapshot_map[root_domain] = domain
            discovered_domains = self.coordinator_store.list_discovered_target_domains(limit=limit, q=search_text)
            rows: list[dict[str, Any]] = []
            discovered_domain_set: set[str] = set()

            def _append_domain_row(root_domain: str, *, discovered_domain: dict[str, Any] | None = None) -> None:
                if not root_domain:
                    return
                if search_text and search_text not in root_domain:
                    return
                domain = snapshot_map.get(root_domain, {})
                plugin_tasks_all = domain.get("plugin_tasks") if isinstance(domain.get("plugin_tasks"), dict) else {}
                workflow_tasks = plugin_tasks_all.get(workflow_id) if isinstance(plugin_tasks_all.get(workflow_id), dict) else {}
                running_count = 0
                pending_count = 0
                completed_count = 0
                failed_count = 0
                for item in workflow_tasks.values():
                    if not isinstance(item, dict):
                        continue
                    status = str(item.get("status") or "").strip().lower()
                    if status == "running":
                        running_count += 1
                    elif status in {"pending", "ready", "paused"}:
                        pending_count += 1
                    elif status == "completed":
                        completed_count += 1
                    elif status == "failed":
                        failed_count += 1
                discovered_urls_count = int(domain.get("discovered_urls_count") or 0)
                frontier_count = int(domain.get("frontier_count") or 0)
                status_text = str(domain.get("status") or "")
                if isinstance(discovered_domain, dict):
                    discovered_urls_count = max(discovered_urls_count, int(discovered_domain.get("discovered_urls_count") or 0))
                    status_text = str(discovered_domain.get("status") or status_text)
                rows.append(
                    {
                        "root_domain": root_domain,
                        "status": status_text,
                        "discovered_urls_count": discovered_urls_count,
                        "frontier_count": frontier_count,
                        "pending_targets": int((domain.get("targets") or {}).get("pending", 0) if isinstance(domain.get("targets"), dict) else 0),
                        "running_targets": int((domain.get("targets") or {}).get("running", 0) if isinstance(domain.get("targets"), dict) else 0),
                        "workflow_pending_tasks": pending_count,
                        "workflow_running_tasks": running_count,
                        "workflow_completed_tasks": completed_count,
                        "workflow_failed_tasks": failed_count,
                    }
                )

            for discovered_domain in discovered_domains:
                if not isinstance(discovered_domain, dict):
                    continue
                root_domain = str(discovered_domain.get("root_domain") or "").strip().lower()
                if not root_domain:
                    continue
                discovered_domain_set.add(root_domain)
                _append_domain_row(root_domain, discovered_domain=discovered_domain)

            for root_domain in sorted(snapshot_map.keys()):
                if root_domain in discovered_domain_set:
                    continue
                _append_domain_row(root_domain)
            rows.sort(key=lambda item: str(item.get("root_domain") or ""))
            self._write_json(
                {
                    "ok": True,
                    "workflow_id": workflow_id,
                    "count": len(rows),
                    "rows": rows,
                }
            )
            return
        if path == "/api/coord/workflow-interface/recon/subdomains":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            root_domain = str((query.get("root_domain") or [""])[0] or "").strip().lower()
            if not root_domain:
                self._write_json({"error": "root_domain is required"}, status=400)
                return
            artifact = self.coordinator_store.get_artifact(root_domain, "recon_subdomains_json", include_content=True)
            payload = _decode_json_bytes(bytes(artifact.get("content") or b"")) if isinstance(artifact, dict) else {}
            subdomains = _extract_recon_subdomains(payload)
            self._write_json({"ok": True, "root_domain": root_domain, "count": len(subdomains), "subdomains": subdomains})
            return
        if path == "/api/coord/workflow-interface/recon/results":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            workflow_id = _normalize_workflow_id((query.get("workflow_id") or ["run-recon"])[0], default="run-recon")
            limit = max(1, min(2000, _safe_int((query.get("limit") or [250])[0], 250)))
            search_text = str((query.get("q") or [""])[0] or "").strip().lower()
            snapshot = self.coordinator_store.workflow_scheduler_snapshot(limit=max(limit, 500))
            domains = snapshot.get("domains") if isinstance(snapshot.get("domains"), list) else []
            rows: list[dict[str, Any]] = []
            for domain in domains:
                if not isinstance(domain, dict):
                    continue
                root_domain = str(domain.get("root_domain") or "").strip().lower()
                if not root_domain:
                    continue
                if search_text and search_text not in root_domain:
                    continue
                artifacts = domain.get("artifacts") if isinstance(domain.get("artifacts"), list) else []
                artifact_map: dict[str, dict[str, Any]] = {}
                for item in artifacts:
                    if not isinstance(item, dict):
                        continue
                    artifact_type = str(item.get("artifact_type") or "").strip().lower()
                    if artifact_type:
                        artifact_map[artifact_type] = item

                subdomain_count = 0
                if "recon_subdomains_json" in artifact_map:
                    artifact = self.coordinator_store.get_artifact(root_domain, "recon_subdomains_json", include_content=True)
                    payload = _decode_json_bytes(bytes(artifact.get("content") or b"")) if isinstance(artifact, dict) else {}
                    subdomain_count = len(_extract_recon_subdomains(payload))

                requests_count = int((artifact_map.get("nightmare_requests_json") or {}).get("summary_request_count") or 0)
                if requests_count <= 0 and "nightmare_requests_json" in artifact_map:
                    artifact = self.coordinator_store.get_artifact(root_domain, "nightmare_requests_json", include_content=True)
                    payload = _decode_json_bytes(bytes(artifact.get("content") or b"")) if isinstance(artifact, dict) else {}
                    requests_count = _extract_request_count(payload)

                takeover_count = int((artifact_map.get("recon_subdomain_takeover_summary_json") or {}).get("summary_anomaly_count") or 0)
                if takeover_count <= 0 and "recon_subdomain_takeover_summary_json" in artifact_map:
                    artifact = self.coordinator_store.get_artifact(root_domain, "recon_subdomain_takeover_summary_json", include_content=True)
                    payload = _decode_json_bytes(bytes(artifact.get("content") or b"")) if isinstance(artifact, dict) else {}
                    takeover_count = _extract_recon_takeover_count(payload)

                high_value_count = int((artifact_map.get("recon_extractor_high_value_summary_json") or {}).get("summary_match_count") or 0)
                if high_value_count <= 0 and "recon_extractor_high_value_summary_json" in artifact_map:
                    artifact = self.coordinator_store.get_artifact(root_domain, "recon_extractor_high_value_summary_json", include_content=True)
                    payload = _decode_json_bytes(bytes(artifact.get("content") or b"")) if isinstance(artifact, dict) else {}
                    high_value_count = _extract_recon_high_value_count(payload)

                plugin_tasks_all = domain.get("plugin_tasks") if isinstance(domain.get("plugin_tasks"), dict) else {}
                workflow_tasks = plugin_tasks_all.get(workflow_id) if isinstance(plugin_tasks_all.get(workflow_id), dict) else {}
                running_task_count = 0
                for task_row in workflow_tasks.values():
                    if not isinstance(task_row, dict):
                        continue
                    if str(task_row.get("status") or "").strip().lower() == "running":
                        running_task_count += 1

                rows.append(
                    {
                        "root_domain": root_domain,
                        "subdomain_count": max(0, int(subdomain_count)),
                        "discovered_url_count": int(domain.get("discovered_urls_count") or 0),
                        "request_count": max(0, int(requests_count)),
                        "urls_in_queue_count": int(domain.get("frontier_count") or 0),
                        "takeover_count": max(0, int(takeover_count)),
                        "high_value_count": max(0, int(high_value_count)),
                        "running_task_count": running_task_count,
                    }
                )
                if len(rows) >= limit:
                    break
            rows.sort(key=lambda item: str(item.get("root_domain") or ""))
            self._write_json(
                {
                    "ok": True,
                    "workflow_id": workflow_id,
                    "count": len(rows),
                    "rows": rows,
                }
            )
            return
        if path == "/api/coord/workflow-config":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            requested_id = _normalize_workflow_id((query.get("workflow_id") or [""])[0], default="run-recon")
            workflow_path = _resolve_workflow_path(requested_id)
            if workflow_path is None:
                self._write_json(
                    {
                        "error": f"workflow not found: {requested_id or 'unknown'}",
                        "available_workflows": _workflow_index_payload(),
                    },
                    status=404,
                )
                return
            workflow = _load_workflow_payload(workflow_path)
            if not workflow:
                self._write_json({"error": f"workflow config is empty or invalid: {workflow_path.name}"}, status=500)
                return
            self._write_json(
                {
                    "ok": True,
                    "workflow_id": str(workflow.get("workflow_id") or requested_id),
                    "path_rel": _to_repo_relative_path(workflow_path),
                    "workflow": workflow,
                    "available_workflows": _workflow_index_payload(),
                }
            )
            return
        if path == "/api/coord/workflow-snapshot":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            limit = _safe_int((query.get("limit") or ["2000"])[0], 2000)
            cache_mode = str((query.get("cache_mode") or ["prefer"])[0] or "prefer").strip().lower()
            try:
                payload = self._resolve_cached_page_payload(
                    page_name="workflow_snapshot",
                    key_parts={"limit": limit},
                    ttl_seconds=WORKFLOW_SNAPSHOT_PAGE_CACHE_TTL_SECONDS,
                    cache_mode=cache_mode,
                    loader=lambda: self.coordinator_store.workflow_scheduler_snapshot(limit=limit),  # type: ignore[union-attr]
                )
            except Exception as exc:
                self.log_message("workflow_scheduler_snapshot failed: %r", exc)
                self._write_json({"error": "workflow snapshot query failed", "detail": str(exc)}, status=500)
                return
            self._write_json(payload)
            return
        if path == "/api/coord/worker-log-download":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            worker_id = str((query.get("worker_id") or [""])[0] or "").strip()
            if not worker_id:
                self._write_json({"error": "worker_id is required"}, status=400)
                return
            if not self._is_safe_worker_id(worker_id):
                self._write_json({"error": "invalid worker_id"}, status=400)
                return
            links = self._discover_worker_log_links(worker_id)
            if not links:
                self._write_json({"error": "no log files found for worker", "worker_id": worker_id}, status=404)
                return
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                for idx, link in enumerate(links, start=1):
                    rel = str(link.get("relative", "") or "").strip()
                    if not rel:
                        continue
                    resolved = _normalize_and_validate_relative_path(self.app_root, rel)
                    if resolved is None or not resolved.is_file():
                        continue
                    try:
                        data = resolved.read_bytes()
                    except Exception:
                        continue
                    if not data:
                        continue
                    entry_name = f"{idx:02d}_{Path(rel).name}"
                    zf.writestr(entry_name, data)
            payload = zip_buffer.getvalue()
            if not payload:
                self._write_json({"error": "no readable log content found for worker", "worker_id": worker_id}, status=404)
                return
            safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", worker_id)
            self._write_bytes(
                payload,
                content_type="application/zip",
                download_name=f"{safe_name}.worker.logs.zip",
            )
            return
        if path.startswith("/files/"):
            rel = unquote(path[len("/files/"):])
            resolved = _normalize_and_validate_relative_path(self.output_root, rel)
            if resolved is None:
                self._write_text("Invalid path", status=400)
                return
            self._serve_static_file(resolved)
            return
        # Allow report-adjacent relative fetches from '/' served report pages, e.g.
        # /all_domains.results_summary.json loaded by all_domains.results_summary.html.
        if path not in {"/favicon.ico"} and not path.startswith("/api/"):
            rel = unquote(path.lstrip("/"))
            resolved = _normalize_and_validate_relative_path(self.output_root, rel)
            if resolved is not None and resolved.is_file():
                self._serve_static_file(resolved)
                return
        self._write_text("Not found", status=404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if self._reject_cross_site_post():
            return
        body = self._read_json_body()

        if path == "/api/regenerate-master-report":
            self._handle_regenerate_master_report()
            return

        if path in {"/api/plugin-definitions", "/api/workflow-definitions", "/api/workflow-runs"} or path.startswith("/api/workflow-definitions/"):
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            actor = str(self.headers.get("X-Actor", "") or "web").strip() or "web"
            try:
                if path == "/api/plugin-definitions":
                    item = upsert_plugin_definition(self.coordinator_store, body, actor=actor)
                    self._write_json({"ok": True, "item": item})
                    return
                if path == "/api/workflow-definitions":
                    item = save_workflow_definition(self.coordinator_store, body, actor=actor)
                    self._write_json({"ok": True, "item": item})
                    return
                if path == "/api/workflow-runs":
                    item = create_workflow_run(self.coordinator_store, body, actor=actor)
                    self._write_json({"ok": True, "item": item})
                    return
                suffix = path[len("/api/workflow-definitions/"):].strip("/")
                if suffix.startswith("import-builtin/"):
                    key = unquote(suffix[len("import-builtin/"):].strip("/") or "run-recon")
                    payload = load_builtin_workflow_definition(key)
                    if body.get("workflow_key"):
                        payload["workflow_key"] = str(body.get("workflow_key") or payload["workflow_key"])
                    if body.get("status"):
                        payload["status"] = str(body.get("status") or payload["status"])
                    item = save_workflow_definition(self.coordinator_store, payload, actor=actor)
                    self._write_json({"ok": True, "item": item, "workflow": payload})
                    return
                if suffix.endswith("/publish"):
                    key = unquote(suffix[: -len("/publish")].strip("/"))
                    item = publish_workflow_definition(self.coordinator_store, key, actor=actor)
                    self._write_json({"ok": True, "item": item})
                    return
            except KeyError as exc:
                self._write_json({"error": str(exc)}, status=404)
                return
            except ValueError as exc:
                self._write_json({"error": str(exc)}, status=400)
                return
            except Exception as exc:
                self._write_json({"error": "workflow API request failed", "detail": str(exc)}, status=500)
                return

        if not path.startswith("/api/coord/"):
            self._write_json({"error": "not found"}, status=404)
            return
        if self.coordinator_store is None:
            self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
            return
        if not self._is_coordinator_authorized():
            self._write_json({"error": "unauthorized"}, status=401)
            return

        def _parse_status_filters(payload: dict[str, Any]) -> list[str]:
            out: list[str] = []
            raw_list = payload.get("statuses")
            if isinstance(raw_list, list):
                candidates = [str(item or "").strip().lower() for item in raw_list]
            else:
                single = str(payload.get("status", "") or "").strip().lower()
                candidates = [single] if single else []
            expanded: list[str] = []
            for item in candidates:
                if not item:
                    continue
                expanded.extend([part.strip().lower() for part in item.split(",") if part.strip()])
            for item in expanded:
                mapped = "failed" if item in {"errored", "error"} else item
                if mapped not in {"pending", "ready", "running", "completed", "failed", "paused"}:
                    continue
                if mapped not in out:
                    out.append(mapped)
            return out

        if path == "/api/coord/register-targets":
            targets_payload = body.get("targets", [])
            targets: list[str] = []
            if isinstance(targets_payload, list):
                targets = [str(item) for item in targets_payload if str(item or "").strip()]
            elif isinstance(targets_payload, str):
                targets = [line for line in str(targets_payload).splitlines() if line.strip()]
            replace_raw = body.get("replace_existing", False)
            replace_existing = False
            if isinstance(replace_raw, bool):
                replace_existing = replace_raw
            elif isinstance(replace_raw, (int, float)):
                replace_existing = bool(replace_raw)
            elif isinstance(replace_raw, str):
                replace_existing = replace_raw.strip().lower() in {"1", "true", "yes", "on"}
            result = self.coordinator_store.register_targets(targets, replace_existing=replace_existing)
            self._write_json({"ok": True, **result})
            return

        if path == "/api/coord/claim":
            worker_id = str(body.get("worker_id", "") or "").strip()
            lease_seconds = _safe_int(body.get("lease_seconds", DEFAULT_COORDINATOR_LEASE_SECONDS), DEFAULT_COORDINATOR_LEASE_SECONDS)
            item = self.coordinator_store.claim_target(worker_id, lease_seconds)
            self._write_json({"ok": True, "entry": item})
            return

        if path == "/api/coord/heartbeat":
            entry_id = str(body.get("entry_id", "") or "").strip()
            worker_id = str(body.get("worker_id", "") or "").strip()
            lease_seconds = _safe_int(body.get("lease_seconds", DEFAULT_COORDINATOR_LEASE_SECONDS), DEFAULT_COORDINATOR_LEASE_SECONDS)
            ok = self.coordinator_store.heartbeat(entry_id, worker_id, lease_seconds)
            self._write_json({"ok": ok})
            return


        if path == "/api/coord/extractor-patterns/save":
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401); return
            rules = body.get("rules", [])
            javascript_rules = body.get("javascript_rules", None)
            rules_path, js_rules_path = _resolve_extractor_rule_paths(self.app_root)
            if not isinstance(rules, list):
                self._write_json({"error": "rules must be a list"}, status=400); return
            _write_extractor_rule_file(rules_path, rules)
            if isinstance(javascript_rules, list):
                _write_extractor_rule_file(js_rules_path, javascript_rules)
            self._write_json({"ok": True, "rules_path": _to_repo_relative_path(rules_path) or str(rules_path)})
            return
        if path == "/api/coord/auth0r/profile/save":
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401); return
            auth0r_store = getattr(self.server, "auth0r_store", None)
            if auth0r_store is None:
                self._write_json({"error": "auth0r store unavailable"}, status=503); return
            profile_id = auth0r_store.upsert_profile(
                profile_id=body.get("profile_id"),
                root_domain=str(body.get("root_domain","") or "").strip().lower(),
                profile_label=str(body.get("profile_label","") or "").strip() or "Default",
                enabled=bool(body.get("enabled", True)),
                allowed_hosts=body.get("allowed_hosts", []),
                default_headers=body.get("default_headers", {}),
                replay_policy=body.get("replay_policy", {}),
            )
            self._write_json({"ok": True, "profile_id": profile_id}); return
        if path == "/api/coord/auth0r/profile/delete":
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401); return
            auth0r_store = getattr(self.server, "auth0r_store", None)
            if auth0r_store is None:
                self._write_json({"error": "auth0r store unavailable"}, status=503); return
            self._write_json({"ok": auth0r_store.delete_profile(str(body.get("profile_id","") or "").strip())}); return
        if path == "/api/coord/auth0r/identity/save":
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401); return
            auth0r_store = getattr(self.server, "auth0r_store", None)
            if auth0r_store is None:
                self._write_json({"error": "auth0r store unavailable"}, status=503); return
            identity_id = auth0r_store.upsert_identity(
                identity_id=body.get("identity_id"),
                profile_id=str(body.get("profile_id","") or "").strip(),
                identity_label=str(body.get("identity_label","") or "").strip() or "default",
                role_label=str(body.get("role_label","") or "").strip(),
                tenant_label=str(body.get("tenant_label","") or "").strip(),
                login_strategy=str(body.get("login_strategy","cookie_import") or "cookie_import"),
                username=str(body.get("username","") or ""),
                password=str(body.get("password","") or ""),
                login_url=str(body.get("login_url","") or ""),
                login_method=str(body.get("login_method","POST") or "POST"),
                login_username_field=str(body.get("login_username_field","username") or "username"),
                login_password_field=str(body.get("login_password_field","password") or "password"),
                login_extra_fields=body.get("login_extra_fields", {}),
                authenticated_probe_url=str(body.get("authenticated_probe_url","") or ""),
                logout_url=str(body.get("logout_url","") or ""),
                custom_headers=body.get("custom_headers", {}),
                success_markers=body.get("success_markers", []),
                denial_markers=body.get("denial_markers", []),
                imported_cookies=body.get("imported_cookies", []),
                enabled=bool(body.get("enabled", True)),
            )
            self._write_json({"ok": True, "identity_id": identity_id}); return
        if path == "/api/coord/auth0r/identity/delete":
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401); return
            auth0r_store = getattr(self.server, "auth0r_store", None)
            if auth0r_store is None:
                self._write_json({"error": "auth0r store unavailable"}, status=503); return
            self._write_json({"ok": auth0r_store.delete_identity(str(body.get("identity_id","") or "").strip())}); return
        if path == "/api/coord/complete":
            entry_id = str(body.get("entry_id", "") or "").strip()
            worker_id = str(body.get("worker_id", "") or "").strip()
            exit_code = _safe_int(body.get("exit_code", 0), 0)
            error = str(body.get("error", "") or "")
            ok = self.coordinator_store.finish(entry_id, worker_id, exit_code=exit_code, error=error)
            self._write_json({"ok": ok})
            return

        if path == "/api/coord/session":
            session_payload = body.get("session", body)
            if not isinstance(session_payload, dict):
                self._write_json({"error": "session payload must be an object"}, status=400)
                return
            root_domain = str(session_payload.get("root_domain", "") or "").strip().lower()
            start_url = str(session_payload.get("start_url", "") or "").strip()
            max_pages = _safe_int(session_payload.get("max_pages", 0), 0)
            saved_at_utc = str(session_payload.get("saved_at_utc", "") or "").strip() or None
            ok = self.coordinator_store.save_session(
                root_domain=root_domain,
                start_url=start_url,
                max_pages=max_pages,
                payload=session_payload,
                saved_at_utc=saved_at_utc,
            )
            self._write_json({"ok": bool(ok)})
            return

        if path == "/api/coord/stage/enqueue":
            root_domain = str(body.get("root_domain", "") or "").strip().lower()
            stage = str(body.get("stage", "") or "").strip().lower()
            workflow_id = str(body.get("workflow_id", "default") or "default").strip().lower() or "default"
            worker_id = str(body.get("worker_id", "") or "").strip()
            reason = str(body.get("reason", "") or "").strip()
            allow_retry_failed = bool(body.get("allow_retry_failed", False))
            max_attempts = _safe_int(body.get("max_attempts", 0), 0)
            checkpoint = body.get("checkpoint") if isinstance(body.get("checkpoint"), dict) else None
            progress = body.get("progress") if isinstance(body.get("progress"), dict) else None
            progress_artifact_type = str(body.get("progress_artifact_type", "") or "").strip().lower()
            resume_mode = str(body.get("resume_mode", "exact") or "exact").strip().lower() or "exact"
            result = self.coordinator_store.schedule_stage(
                root_domain,
                stage,
                workflow_id=workflow_id,
                worker_id=worker_id,
                reason=reason,
                allow_retry_failed=allow_retry_failed,
                max_attempts=max_attempts,
                checkpoint=checkpoint,
                progress=progress,
                progress_artifact_type=progress_artifact_type,
                resume_mode=resume_mode,
            )
            self._write_json(result)
            return

        if path == "/api/coord/stage/claim":
            worker_id = str(body.get("worker_id", "") or "").strip()
            stage = str(body.get("stage", "") or "").strip().lower()
            # Workflow is metadata only for tasks; workers claim globally.
            workflow_id = ""
            lease_seconds = _safe_int(body.get("lease_seconds", DEFAULT_COORDINATOR_LEASE_SECONDS), DEFAULT_COORDINATOR_LEASE_SECONDS)
            plugin_allowlist_raw = body.get("plugin_allowlist")
            plugin_allowlist = (
                [str(item or "").strip().lower() for item in plugin_allowlist_raw if str(item or "").strip()]
                if isinstance(plugin_allowlist_raw, list)
                else None
            )
            if stage:
                item = self.coordinator_store.claim_stage(stage, worker_id, lease_seconds, workflow_id=workflow_id)
            else:
                item = self.coordinator_store.claim_next_stage(
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                    workflow_id=workflow_id,
                    plugin_allowlist=plugin_allowlist,
                )
            self._write_json({"ok": True, "entry": item})
            return


        if path == "/api/coord/stage/claim-next":
            worker_id = str(body.get("worker_id", "") or "").strip()
            lease_seconds = _safe_int(body.get("lease_seconds", DEFAULT_COORDINATOR_LEASE_SECONDS), DEFAULT_COORDINATOR_LEASE_SECONDS)
            plugin_allowlist_raw = body.get("plugin_allowlist")
            plugin_allowlist = (
                [str(item or "").strip().lower() for item in plugin_allowlist_raw if str(item or "").strip()]
                if isinstance(plugin_allowlist_raw, list)
                else None
            )
            entry = self.coordinator_store.claim_next_stage(
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                workflow_id="",
                plugin_allowlist=plugin_allowlist,
            )
            self._write_json({"ok": True, "entry": entry})
            return

        if path == "/api/coord/stage/heartbeat":
            worker_id = str(body.get("worker_id", "") or "").strip()
            root_domain = str(body.get("root_domain", "") or "").strip().lower()
            stage = str(body.get("stage", "") or "").strip().lower()
            workflow_id = str(body.get("workflow_id", "default") or "default").strip().lower() or "default"
            checkpoint = body.get("checkpoint") if isinstance(body.get("checkpoint"), dict) else None
            progress = body.get("progress") if isinstance(body.get("progress"), dict) else None
            progress_artifact_type = str(body.get("progress_artifact_type", "") or "").strip().lower()
            lease_seconds = _safe_int(body.get("lease_seconds", DEFAULT_COORDINATOR_LEASE_SECONDS), DEFAULT_COORDINATOR_LEASE_SECONDS)
            ok = self.coordinator_store.heartbeat_stage_with_workflow(
                root_domain=root_domain,
                stage=stage,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                workflow_id=workflow_id,
                checkpoint=checkpoint,
                progress=progress,
                progress_artifact_type=progress_artifact_type,
            )
            self._write_json({"ok": bool(ok)})
            return

        if path == "/api/coord/stage/progress":
            worker_id = str(body.get("worker_id", "") or "").strip()
            root_domain = str(body.get("root_domain", "") or "").strip().lower()
            stage = str(body.get("stage", "") or "").strip().lower()
            workflow_id = str(body.get("workflow_id", "default") or "default").strip().lower() or "default"
            checkpoint = body.get("checkpoint") if isinstance(body.get("checkpoint"), dict) else None
            progress = body.get("progress") if isinstance(body.get("progress"), dict) else None
            progress_artifact_type = str(body.get("progress_artifact_type", "") or "").strip().lower()
            ok = self.coordinator_store.update_stage_progress(
                root_domain=root_domain,
                stage=stage,
                worker_id=worker_id,
                workflow_id=workflow_id,
                checkpoint=checkpoint,
                progress=progress,
                progress_artifact_type=progress_artifact_type,
            )
            self._write_json({"ok": bool(ok)})
            return

        if path == "/api/coord/stage/complete":
            worker_id = str(body.get("worker_id", "") or "").strip()
            root_domain = str(body.get("root_domain", "") or "").strip().lower()
            stage = str(body.get("stage", "") or "").strip().lower()
            workflow_id = str(body.get("workflow_id", "default") or "default").strip().lower() or "default"
            exit_code = _safe_int(body.get("exit_code", 0), 0)
            error = str(body.get("error", "") or "")
            checkpoint = body.get("checkpoint") if isinstance(body.get("checkpoint"), dict) else None
            progress = body.get("progress") if isinstance(body.get("progress"), dict) else None
            progress_artifact_type = str(body.get("progress_artifact_type", "") or "").strip().lower()
            resume_mode = str(body.get("resume_mode", "") or "").strip().lower()
            ok = self.coordinator_store.complete_stage(
                root_domain,
                stage,
                worker_id,
                workflow_id=workflow_id,
                exit_code=exit_code,
                error=error,
                checkpoint=checkpoint,
                progress=progress,
                progress_artifact_type=progress_artifact_type,
                resume_mode=resume_mode,
            )
            self._write_json({"ok": bool(ok)})
            return

        if path == "/api/coord/stage/reset":
            workflow_id = str(body.get("workflow_id", "") or "").strip().lower()
            root_domains_raw = body.get("root_domains")
            if isinstance(root_domains_raw, list):
                root_domains = [str(item or "").strip().lower() for item in root_domains_raw if str(item or "").strip()]
            else:
                root_domain_single = str(body.get("root_domain", "") or "").strip().lower()
                root_domains = [root_domain_single] if root_domain_single else []
            plugins_raw = body.get("plugins")
            if isinstance(plugins_raw, list):
                plugins = [str(item or "").strip().lower() for item in plugins_raw if str(item or "").strip()]
            else:
                plugin_single = str(body.get("plugin", "") or "").strip().lower()
                plugins = [plugin_single] if plugin_single else []
            statuses = _parse_status_filters(body)
            hard_delete = bool(body.get("hard_delete", False))
            result = self.coordinator_store.reset_stage_tasks(
                workflow_id=workflow_id,
                root_domains=root_domains,
                plugins=plugins,
                statuses=statuses,
                hard_delete=hard_delete,
            )
            self._write_json(result)
            return

        if path == "/api/coord/stage/control":
            workflow_id = str(body.get("workflow_id", "default") or "default").strip().lower() or "default"
            root_domain = str(body.get("root_domain", "") or "").strip().lower()
            stage = str(body.get("stage", body.get("plugin", "")) or "").strip().lower()
            action = str(body.get("action", "") or "").strip().lower()
            if not root_domain:
                self._write_json({"error": "root_domain is required"}, status=400)
                return
            if not stage:
                self._write_json({"error": "stage/plugin is required"}, status=400)
                return
            if action not in {"delete", "pause", "run"}:
                self._write_json({"error": "action must be one of: delete, pause, run"}, status=400)
                return
            result = self.coordinator_store.control_stage_task(
                workflow_id=workflow_id,
                root_domain=root_domain,
                stage=stage,
                action=action,
            )
            status = 200 if bool(result.get("ok", False)) else 404
            self._write_json(result, status=status)
            return

        if path == "/api/coord/targets/reset":
            root_domains_raw = body.get("root_domains")
            if isinstance(root_domains_raw, list):
                root_domains = [str(item or "").strip().lower() for item in root_domains_raw if str(item or "").strip()]
            else:
                root_domain_single = str(body.get("root_domain", "") or "").strip().lower()
                root_domains = [root_domain_single] if root_domain_single else []
            statuses = _parse_status_filters(body)
            hard_delete = bool(body.get("hard_delete", False))
            result = self.coordinator_store.reset_targets(
                root_domains=root_domains,
                statuses=statuses,
                hard_delete=hard_delete,
            )
            self._write_json(result)
            return

        if path == "/api/coord/tasks/reset":
            workflow_id = str(body.get("workflow_id", "") or "").strip().lower()
            root_domains_raw = body.get("root_domains")
            if isinstance(root_domains_raw, list):
                root_domains = [str(item or "").strip().lower() for item in root_domains_raw if str(item or "").strip()]
            else:
                root_domain_single = str(body.get("root_domain", "") or "").strip().lower()
                root_domains = [root_domain_single] if root_domain_single else []
            plugins_raw = body.get("plugins")
            if isinstance(plugins_raw, list):
                plugins = [str(item or "").strip().lower() for item in plugins_raw if str(item or "").strip()]
            else:
                plugin_single = str(body.get("plugin", "") or "").strip().lower()
                plugins = [plugin_single] if plugin_single else []
            statuses = _parse_status_filters(body)
            hard_delete = bool(body.get("hard_delete", False))

            scope_raw = body.get("scopes")
            if isinstance(scope_raw, list):
                scope_values = [str(item or "").strip().lower() for item in scope_raw if str(item or "").strip()]
            else:
                scope_single = str(body.get("scope", "") or "").strip().lower()
                scope_values = [scope_single] if scope_single else ["stage_tasks"]
            scopes: list[str] = []
            for scope in scope_values:
                if scope in {"all", "*"}:
                    scopes = ["stage_tasks", "targets"]
                    break
                if scope in {"stage", "stages", "stage_task", "stage_tasks"} and "stage_tasks" not in scopes:
                    scopes.append("stage_tasks")
                if scope in {"target", "targets", "target_queue", "coordinator_targets"} and "targets" not in scopes:
                    scopes.append("targets")
            if not scopes:
                scopes = ["stage_tasks"]

            stage_result: dict[str, Any] | None = None
            target_result: dict[str, Any] | None = None
            if "stage_tasks" in scopes:
                stage_result = self.coordinator_store.reset_stage_tasks(
                    workflow_id=workflow_id,
                    root_domains=root_domains,
                    plugins=plugins,
                    statuses=statuses,
                    hard_delete=hard_delete,
                )
            if "targets" in scopes:
                target_result = self.coordinator_store.reset_targets(
                    root_domains=root_domains,
                    statuses=statuses,
                    hard_delete=hard_delete,
                )
            total_affected = int((stage_result or {}).get("affected_rows") or 0) + int((target_result or {}).get("affected_rows") or 0)
            self._write_json(
                {
                    "ok": True,
                    "scopes": scopes,
                    "workflow_id": workflow_id,
                    "root_domains": root_domains,
                    "plugins": plugins,
                    "statuses": statuses,
                    "hard_delete": hard_delete,
                    "total_affected_rows": total_affected,
                    "stage_tasks": stage_result,
                    "targets": target_result,
                }
            )
            return

        if path == "/api/coord/workflow-config":
            raw_workflow = body.get("workflow", body)
            if not isinstance(raw_workflow, dict):
                self._write_json({"error": "workflow payload must be an object"}, status=400)
                return
            workflow_payload = dict(raw_workflow)
            workflow_id = _normalize_workflow_id(
                workflow_payload.get("workflow_id", body.get("workflow_id", "")),
                default="run-recon",
            )
            if not workflow_id:
                self._write_json({"error": "workflow_id is required"}, status=400)
                return
            plugins_raw = workflow_payload.get("plugins")
            if not isinstance(plugins_raw, list):
                self._write_json({"error": "workflow.plugins must be an array"}, status=400)
                return
            sanitized_plugins: list[dict[str, Any]] = []
            for idx, item in enumerate(plugins_raw):
                if not isinstance(item, dict):
                    self._write_json({"error": f"workflow.plugins[{idx}] must be an object"}, status=400)
                    return
                plugin = dict(item)
                plugin_name = str(plugin.get("name") or plugin.get("plugin_name") or plugin.get("stage") or "").strip().lower()
                if not plugin_name:
                    self._write_json({"error": f"workflow.plugins[{idx}] is missing name/plugin_name/stage"}, status=400)
                    return
                plugin["name"] = plugin_name
                parameters = plugin.get("parameters")
                if parameters is None:
                    plugin["parameters"] = {}
                elif not isinstance(parameters, dict):
                    self._write_json({"error": f"workflow.plugins[{idx}].parameters must be an object"}, status=400)
                    return
                preconditions = plugin.get("preconditions")
                if preconditions is not None and not isinstance(preconditions, dict):
                    self._write_json({"error": f"workflow.plugins[{idx}].preconditions must be an object"}, status=400)
                    return
                sanitized_plugins.append(plugin)
            workflow_payload["workflow_id"] = workflow_id
            workflow_payload["plugins"] = sanitized_plugins
            if not str(workflow_payload.get("workflow_type") or "").strip():
                workflow_payload["workflow_type"] = "coordinator_plugin_scheduler"
            if not str(workflow_payload.get("schema_version") or "").strip():
                workflow_payload["schema_version"] = "2.0.0"
            workflow_path = _resolve_workflow_path(workflow_id) or _default_workflow_path_for_id(workflow_id)
            workflow_path.parent.mkdir(parents=True, exist_ok=True)
            workflow_path.write_text(json.dumps(workflow_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            interface_catalog = self._refresh_workflow_interface_catalog()
            self.coordinator_store.record_system_event(
                "workflow.config.updated",
                f"workflow:{workflow_id}",
                {
                    "source": "server.api.workflow_config",
                    "workflow_id": workflow_id,
                    "plugin_count": len(sanitized_plugins),
                    "path_rel": _to_repo_relative_path(workflow_path),
                },
            )
            self._write_json(
                {
                    "ok": True,
                    "workflow_id": workflow_id,
                    "path_rel": _to_repo_relative_path(workflow_path),
                    "workflow": workflow_payload,
                    "available_workflows": _workflow_index_payload(),
                    "available_interfaces": interface_catalog.get("interfaces") if isinstance(interface_catalog.get("interfaces"), list) else [],
                }
            )
            return

        if path == "/api/coord/workflow/run":
            workflow_id = _normalize_workflow_id(body.get("workflow_id"), default="run-recon")
            workflow_path = _resolve_workflow_path(workflow_id)
            if workflow_path is None:
                self._write_json(
                    {
                        "error": f"workflow not found: {workflow_id or 'unknown'}",
                        "available_workflows": _workflow_index_payload(),
                    },
                    status=404,
                )
                return
            workflow = _load_workflow_payload(workflow_path)
            if not workflow:
                self._write_json({"error": f"workflow config is empty or invalid: {workflow_path.name}"}, status=500)
                return
            plugins = [item for item in (workflow.get("plugins") if isinstance(workflow.get("plugins"), list) else []) if isinstance(item, dict)]
            selected_plugins_raw = body.get("plugins")
            selected_plugins: set[str] = set()
            if isinstance(selected_plugins_raw, str):
                selected_plugins = {
                    str(item or "").strip().lower()
                    for item in selected_plugins_raw.split(",")
                    if str(item or "").strip()
                }
            elif isinstance(selected_plugins_raw, list):
                selected_plugins = {
                    str(item or "").strip().lower()
                    for item in selected_plugins_raw
                    if str(item or "").strip()
                }
            runnable_plugins = [plugin for plugin in plugins if bool(plugin.get("enabled", True))]
            if selected_plugins:
                runnable_plugins = [
                    plugin
                    for plugin in runnable_plugins
                    if str(plugin.get("name") or plugin.get("plugin_name") or plugin.get("stage") or "").strip().lower() in selected_plugins
                ]
            starter_plugins = list(runnable_plugins)
            runnable_plugin_names = sorted(
                {
                    str(item.get("name") or item.get("plugin_name") or item.get("stage") or "").strip().lower()
                    for item in starter_plugins
                    if str(item.get("name") or item.get("plugin_name") or item.get("stage") or "").strip()
                }
            )
            if not runnable_plugins:
                self._write_json({"error": "workflow has no enabled plugins"}, status=400)
                return
            plugin_parameter_overrides_raw = body.get("plugin_parameter_overrides")
            plugin_parameter_overrides: dict[str, dict[str, Any]] = {}
            if isinstance(plugin_parameter_overrides_raw, dict):
                for plugin_name, params in plugin_parameter_overrides_raw.items():
                    safe_name = str(plugin_name or "").strip().lower()
                    if not safe_name or not isinstance(params, dict):
                        continue
                    plugin_parameter_overrides[safe_name] = dict(params)
            saved_parameter_overrides = False
            if plugin_parameter_overrides and bool(body.get("persist_parameter_overrides", True)):
                changed = False
                for plugin in plugins:
                    plugin_name = str(plugin.get("name") or plugin.get("plugin_name") or plugin.get("stage") or "").strip().lower()
                    if not plugin_name:
                        continue
                    override = plugin_parameter_overrides.get(plugin_name)
                    if not isinstance(override, dict):
                        continue
                    current_params = dict(plugin.get("parameters") or {}) if isinstance(plugin.get("parameters"), dict) else {}
                    merged_params = {**current_params, **override}
                    if merged_params != current_params:
                        plugin["parameters"] = merged_params
                        changed = True
                if changed:
                    workflow["plugins"] = plugins
                    workflow_path.write_text(json.dumps(workflow, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                    self._refresh_workflow_interface_catalog()
                    saved_parameter_overrides = True
            root_domains_raw = body.get("root_domains")
            root_domains: list[str] = []
            if isinstance(root_domains_raw, list):
                root_domains = sorted({str(item or "").strip().lower() for item in root_domains_raw if str(item or "").strip()})
            if not root_domains:
                domain_limit = max(1, min(20000, _safe_int(body.get("domain_limit", 5000), 5000)))
                snapshot = self.coordinator_store.workflow_scheduler_snapshot(limit=domain_limit)
                root_domains = sorted(
                    {
                        str(item.get("root_domain") or "").strip().lower()
                        for item in (snapshot.get("domains") if isinstance(snapshot.get("domains"), list) else [])
                        if str(item.get("root_domain") or "").strip()
                    }
                )
            if not root_domains:
                self._write_json({"error": "no domains available to run workflow"}, status=400)
                return
            enqueue_reason = str(body.get("reason", "") or "").strip() or f"manual_run:{workflow_id}"
            allow_retry_failed = bool(body.get("allow_retry_failed", False))
            rows: list[dict[str, Any]] = []
            counts = {"scheduled": 0, "already_pending": 0, "already_running": 0, "already_completed": 0, "failed": 0, "other": 0}
            retry_counts = {"scheduled": 0, "already_pending": 0, "already_running": 0, "already_completed": 0, "failed": 0, "other": 0}
            requeue_attempted = False
            for root_domain in root_domains:
                for plugin in runnable_plugins:
                    plugin_name = str(plugin.get("name") or plugin.get("plugin_name") or plugin.get("stage") or "").strip().lower()
                    if not plugin_name:
                        continue
                    resume_mode = str(plugin.get("resume_mode") or "exact").strip().lower() or "exact"
                    max_attempts = max(0, _safe_int(plugin.get("max_attempts", 0), 0))
                    result = self.coordinator_store.schedule_stage(
                        root_domain,
                        plugin_name,
                        workflow_id=workflow_id,
                        reason=enqueue_reason,
                        allow_retry_failed=allow_retry_failed,
                        max_attempts=max_attempts,
                        checkpoint={"schema_version": 1, "resume_mode": resume_mode, "state": "queued"},
                        progress={"status": "queued", "plugin_name": plugin_name},
                        progress_artifact_type=f"workflow_progress_{plugin_name}",
                        resume_mode=resume_mode,
                    )
                    reason = str(result.get("reason") or "")
                    if bool(result.get("scheduled")):
                        counts["scheduled"] += 1
                    elif reason == "already_pending":
                        counts["already_pending"] += 1
                    elif reason == "already_running":
                        counts["already_running"] += 1
                    elif reason == "already_completed":
                        counts["already_completed"] += 1
                    elif reason.startswith("unsupported_status_") or reason in {"retry_not_allowed", "max_attempts_reached"}:
                        counts["failed"] += 1
                    else:
                        counts["other"] += 1
                    rows.append(
                        {
                            "root_domain": root_domain,
                            "plugin_name": plugin_name,
                            "scheduled": bool(result.get("scheduled")),
                            "reason": reason,
                            "status": str(result.get("status") or ""),
                        }
                    )
                self.coordinator_store.record_system_event(
                "workflow.run.enqueued",
                f"workflow:{workflow_id}",
                {
                    "source": "server.api.workflow_run",
                    "workflow_id": workflow_id,
                    "domains_count": len(root_domains),
                    "starter_plugins": [
                        str(item.get("name") or item.get("plugin_name") or item.get("stage") or "").strip().lower()
                        for item in starter_plugins
                    ],
                    "counts": counts,
                    "selected_plugins": sorted(selected_plugins),
                    "saved_parameter_overrides": saved_parameter_overrides,
                },
            )
            persisted_stage_task_rows = self.coordinator_store.count_stage_tasks(
                workflow_id=workflow_id,
                root_domains=root_domains,
                plugins=runnable_plugin_names,
            )
            if counts["scheduled"] > 0 and persisted_stage_task_rows <= 0:
                requeue_attempted = True
                for root_domain in root_domains:
                    for plugin in runnable_plugins:
                        plugin_name = str(plugin.get("name") or plugin.get("plugin_name") or plugin.get("stage") or "").strip().lower()
                        if not plugin_name:
                            continue
                        resume_mode = str(plugin.get("resume_mode") or "exact").strip().lower() or "exact"
                        max_attempts = max(0, _safe_int(plugin.get("max_attempts", 0), 0))
                        retry_result = self.coordinator_store.schedule_stage(
                            root_domain,
                            plugin_name,
                            workflow_id=workflow_id,
                            reason=f"{enqueue_reason}:retry_after_zero_persist",
                            allow_retry_failed=allow_retry_failed,
                            max_attempts=max_attempts,
                            checkpoint={"schema_version": 1, "resume_mode": resume_mode, "state": "queued"},
                            progress={"status": "queued", "plugin_name": plugin_name},
                            progress_artifact_type=f"workflow_progress_{plugin_name}",
                            resume_mode=resume_mode,
                        )
                        retry_reason = str(retry_result.get("reason") or "")
                        if bool(retry_result.get("scheduled")):
                            retry_counts["scheduled"] += 1
                        elif retry_reason == "already_pending":
                            retry_counts["already_pending"] += 1
                        elif retry_reason == "already_running":
                            retry_counts["already_running"] += 1
                        elif retry_reason == "already_completed":
                            retry_counts["already_completed"] += 1
                        elif retry_reason.startswith("unsupported_status_") or retry_reason in {"retry_not_allowed", "max_attempts_reached"}:
                            retry_counts["failed"] += 1
                        else:
                            retry_counts["other"] += 1
                persisted_stage_task_rows = self.coordinator_store.count_stage_tasks(
                    workflow_id=workflow_id,
                    root_domains=root_domains,
                    plugins=runnable_plugin_names,
                )
            if counts["scheduled"] > 0 and persisted_stage_task_rows <= 0:
                self._write_json(
                    {
                        "error": "workflow run reported scheduled tasks but no rows were persisted to coordinator_stage_tasks",
                        "workflow_id": workflow_id,
                        "domains_count": len(root_domains),
                        "selected_plugins": sorted(selected_plugins),
                        "requeue_attempted": requeue_attempted,
                        "requeue_counts": retry_counts,
                    },
                    status=500,
                )
                return
            reload_workers_queued: list[str] = []
            if saved_parameter_overrides and bool(body.get("reload_workers", True)):
                worker_snapshot = self.coordinator_store.worker_statuses(stale_after_seconds=DEFAULT_COORDINATOR_LEASE_SECONDS)
                workers = worker_snapshot.get("workers") if isinstance(worker_snapshot.get("workers"), list) else []
                for row in workers:
                    if not isinstance(row, dict):
                        continue
                    worker_id = str(row.get("worker_id") or "").strip()
                    if not worker_id or not self._is_workflow_runtime_worker(worker_id):
                        continue
                    if self.coordinator_store.queue_worker_command(
                        worker_id,
                        "reload",
                        payload={"source": "workflow_run_overrides", "workflow_id": workflow_id},
                    ):
                        reload_workers_queued.append(worker_id)
            self._write_json(
                {
                    "ok": True,
                    "workflow_id": workflow_id,
                    "domains_count": len(root_domains),
                    "domains": root_domains,
                    "selected_plugins": sorted(selected_plugins),
                    "starter_plugins": runnable_plugin_names,
                    "saved_parameter_overrides": saved_parameter_overrides,
                    "reload_workers_queued": reload_workers_queued,
                    "counts": counts,
                    "persisted_stage_task_rows": int(persisted_stage_task_rows),
                    "requeue_attempted": requeue_attempted,
                    "requeue_counts": retry_counts,
                    "results": rows[:1000],
                    "results_truncated": max(0, len(rows) - 1000),
                }
            )
            return

        if path == "/api/coord/workflow/clear-generated-tasks":
            workflow_id = _normalize_workflow_id(body.get("workflow_id"), default="run-recon")
            plugin_values = body.get("plugins")
            selected_plugins: set[str] = set()
            if isinstance(plugin_values, str):
                selected_plugins = {
                    str(item or "").strip().lower()
                    for item in plugin_values.split(",")
                    if str(item or "").strip()
                }
            elif isinstance(plugin_values, list):
                selected_plugins = {
                    str(item or "").strip().lower()
                    for item in plugin_values
                    if str(item or "").strip()
                }
            root_domains_raw = body.get("root_domains")
            root_domains = (
                sorted({str(item or "").strip().lower() for item in root_domains_raw if str(item or "").strip()})
                if isinstance(root_domains_raw, list)
                else []
            )
            hard_delete = bool(body.get("hard_delete", True))
            stop_running_workers = bool(body.get("stop_running_workers", False))
            snapshot_limit = max(1000, min(20000, _safe_int(body.get("snapshot_limit", 5000), 5000)))
            snapshot = self.coordinator_store.workflow_scheduler_snapshot(limit=snapshot_limit)
            root_domain_filter = set(root_domains) if root_domains else None
            plugin_filter = set(selected_plugins) if selected_plugins else None
            running = _workflow_running_tasks_summary(
                snapshot,
                workflow_id=workflow_id,
                root_domains=root_domain_filter,
                plugins=plugin_filter,
            )
            result = self.coordinator_store.reset_stage_tasks(
                workflow_id=workflow_id,
                root_domains=root_domains,
                plugins=sorted(selected_plugins),
                statuses=["pending", "ready", "paused", "completed", "failed"],
                hard_delete=hard_delete,
            )
            stop_commands_queued: list[str] = []
            if stop_running_workers and running["running_workers"]:
                for worker_id in running["running_workers"]:
                    if self.coordinator_store.queue_worker_command(
                        worker_id,
                        "stop",
                        payload={"source": "workflow_clear_tasks", "workflow_id": workflow_id},
                    ):
                        stop_commands_queued.append(worker_id)
            warning = ""
            if int(running["running_tasks"] or 0) > 0:
                warning = (
                    "Not all tasks were deleted because some matching tasks are currently running. "
                    "Stop those workers, then clear again to remove running tasks."
                )
            self._write_json(
                {
                    "ok": True,
                    "workflow_id": workflow_id,
                    "root_domains": root_domains,
                    "plugins": sorted(selected_plugins),
                    "hard_delete": hard_delete,
                    "result": result,
                    "running_tasks_remaining": int(running["running_tasks"] or 0),
                    "running_workers": list(running["running_workers"] or []),
                    "warning": warning,
                    "stop_commands_queued": stop_commands_queued,
                }
            )
            return

        if path == "/api/coord/workflow/clear-data":
            workflow_id = _normalize_workflow_id(body.get("workflow_id"), default="run-recon")
            workflow_path = _resolve_workflow_path(workflow_id)
            if workflow_path is None:
                self._write_json({"error": f"workflow not found: {workflow_id or 'unknown'}"}, status=404)
                return
            workflow = _load_workflow_payload(workflow_path)
            if not workflow:
                self._write_json({"error": f"workflow config is empty or invalid: {workflow_path.name}"}, status=500)
                return
            root_domains_raw = body.get("root_domains")
            root_domains = (
                sorted({str(item or "").strip().lower() for item in root_domains_raw if str(item or "").strip()})
                if isinstance(root_domains_raw, list)
                else []
            )
            if not root_domains:
                one_domain = str(body.get("root_domain") or "").strip().lower()
                if one_domain:
                    root_domains = [one_domain]
            if not root_domains:
                self._write_json({"error": "root_domain or root_domains is required"}, status=400)
                return
            artifact_types: set[str] = set()
            for plugin in (workflow.get("plugins") if isinstance(workflow.get("plugins"), list) else []):
                if not isinstance(plugin, dict):
                    continue
                outputs = plugin.get("outputs") if isinstance(plugin.get("outputs"), dict) else {}
                output_artifacts = outputs.get("artifacts") if isinstance(outputs.get("artifacts"), list) else []
                for artifact in output_artifacts:
                    text = str(artifact or "").strip().lower()
                    if text:
                        artifact_types.add(text)
            per_domain: list[dict[str, Any]] = []
            total_deleted_artifacts = 0
            total_deleted_sessions = 0
            for root_domain in root_domains:
                deleted_artifacts = self.coordinator_store.delete_artifacts(
                    root_domain=root_domain,
                    artifact_types=sorted(artifact_types),
                )
                deleted_sessions = self.coordinator_store.delete_sessions(root_domains=[root_domain])
                total_deleted_artifacts += int(deleted_artifacts or 0)
                total_deleted_sessions += int((deleted_sessions or {}).get("affected_rows") or 0)
                per_domain.append(
                    {
                        "root_domain": root_domain,
                        "deleted_artifacts": int(deleted_artifacts or 0),
                        "deleted_sessions": int((deleted_sessions or {}).get("affected_rows") or 0),
                    }
                )
            self._write_json(
                {
                    "ok": True,
                    "workflow_id": workflow_id,
                    "artifact_types": sorted(artifact_types),
                    "domains": per_domain,
                    "deleted_artifacts_total": total_deleted_artifacts,
                    "deleted_sessions_total": total_deleted_sessions,
                }
            )
            return

        if path == "/api/coord/workflow/mode":
            mode = str(body.get("mode", "workflow_only") or "workflow_only").strip().lower()
            if mode not in {"workflow_only", "all_running"}:
                self._write_json({"error": "mode must be one of: workflow_only, all_running"}, status=400)
                return
            snapshot = self.coordinator_store.worker_statuses(stale_after_seconds=DEFAULT_COORDINATOR_LEASE_SECONDS)
            workers = snapshot.get("workers") if isinstance(snapshot.get("workers"), list) else []
            workflow_workers: list[str] = []
            non_workflow_workers: list[str] = []
            for row in workers:
                if not isinstance(row, dict):
                    continue
                worker_id = str(row.get("worker_id") or "").strip()
                if not worker_id:
                    continue
                if self._is_workflow_runtime_worker(worker_id):
                    workflow_workers.append(worker_id)
                else:
                    non_workflow_workers.append(worker_id)
            changed: list[dict[str, Any]] = []
            if mode == "workflow_only":
                for worker_id in workflow_workers:
                    if self.coordinator_store.queue_worker_command(worker_id, "start", payload={"source": "workflow_mode", "mode": mode}):
                        changed.append({"worker_id": worker_id, "command": "start"})
                for worker_id in non_workflow_workers:
                    if self.coordinator_store.queue_worker_command(worker_id, "pause", payload={"source": "workflow_mode", "mode": mode}):
                        changed.append({"worker_id": worker_id, "command": "pause"})
            else:
                for worker_id in workflow_workers + non_workflow_workers:
                    if self.coordinator_store.queue_worker_command(worker_id, "start", payload={"source": "workflow_mode", "mode": mode}):
                        changed.append({"worker_id": worker_id, "command": "start"})
            self.coordinator_store.record_system_event(
                "workflow.mode.updated",
                "workflow:mode",
                {
                    "source": "server.api.workflow_mode",
                    "mode": mode,
                    "workflow_workers": workflow_workers,
                    "non_workflow_workers": non_workflow_workers,
                    "commands_queued": len(changed),
                },
            )
            self._write_json(
                {
                    "ok": True,
                    "mode": mode,
                    "workflow_workers": workflow_workers,
                    "non_workflow_workers": non_workflow_workers,
                    "commands_queued": changed,
                }
            )
            return

        if path == "/api/coord/workflow/reload":
            snapshot = self.coordinator_store.worker_statuses(stale_after_seconds=DEFAULT_COORDINATOR_LEASE_SECONDS)
            workers = snapshot.get("workers") if isinstance(snapshot.get("workers"), list) else []
            queued: list[str] = []
            for row in workers:
                if not isinstance(row, dict):
                    continue
                worker_id = str(row.get("worker_id") or "").strip()
                if not worker_id or not self._is_workflow_runtime_worker(worker_id):
                    continue
                if self.coordinator_store.queue_worker_command(worker_id, "reload", payload={"source": "workflow_reload"}):
                    queued.append(worker_id)
            self.coordinator_store.record_system_event(
                "workflow.config.reload_requested",
                "workflow:reload",
                {
                    "source": "server.api.workflow_reload",
                    "workers": queued,
                    "queued_count": len(queued),
                },
            )
            self._write_json({"ok": True, "workers_queued": queued, "queued_count": len(queued)})
            return

        if path == "/api/coord/artifact":
            root_domain = str(body.get("root_domain", "") or "").strip().lower()
            artifact_type = str(body.get("artifact_type", "") or "").strip().lower()
            source_worker = str(body.get("source_worker", "") or "").strip()
            content_encoding = str(body.get("content_encoding", "identity") or "identity").strip()
            content_base64 = str(body.get("content_base64", "") or "")
            if not root_domain or not artifact_type or not content_base64:
                self._write_json({"error": "root_domain, artifact_type, content_base64 are required"}, status=400)
                return
            try:
                content = base64.b64decode(content_base64.encode("ascii"), validate=True)
            except Exception:
                self._write_json({"error": "content_base64 is invalid"}, status=400)
                return
            ok = self.coordinator_store.upload_artifact(
                root_domain=root_domain,
                artifact_type=artifact_type,
                content=content,
                source_worker=source_worker,
                content_encoding=content_encoding,
            )
            self._write_json({"ok": bool(ok), "size": len(content)})
            return

        if path == "/api/coord/reset":
            confirm = str(body.get("confirm", "") or "").strip()
            if confirm != RESET_COORDINATOR_CONFIRM:
                self._write_json(
                    {
                        "error": "invalid confirm",
                        "expected_confirm": RESET_COORDINATOR_CONFIRM,
                    },
                    status=400,
                )
                return
            clear_output = bool(body.get("clear_output", False))
            signal_workers_clear_disk = bool(body.get("signal_workers_clear_disk", False))
            result = self.coordinator_store.reset_coordinator_tables()
            if clear_output:
                result["output_clear"] = clear_output_root_children(self.output_root)
            if signal_workers_clear_disk:
                result["fleet_signal"] = self.coordinator_store.bump_output_clear_generation()
            self._write_json({"ok": True, **result})
            return

        if path == "/api/coord/workers/command":
            command = str(body.get("command", "") or "").strip().lower()
            worker_ids_raw = body.get("worker_ids", [])
            if not isinstance(worker_ids_raw, list):
                self._write_json({"error": "worker_ids must be an array"}, status=400)
                return
            worker_ids = [str(item or "").strip() for item in worker_ids_raw if str(item or "").strip()]
            if command not in {"start", "pause", "stop", "reload"}:
                self._write_json({"error": "command must be one of: start, pause, stop, reload"}, status=400)
                return
            if not worker_ids:
                self._write_json({"error": "at least one worker_id is required"}, status=400)
                return
            queued = 0
            for worker_id in worker_ids:
                if self.coordinator_store.queue_worker_command(worker_id, command, payload={"source": "worker-control-ui"}):
                    queued += 1
            self._write_json({"ok": True, "queued": queued, "command": command, "worker_ids": worker_ids})
            return

        if path == "/api/coord/worker-command/claim":
            worker_id = str(body.get("worker_id", "") or "").strip()
            worker_state = str(body.get("worker_state", "idle") or "idle").strip().lower()
            if not worker_id:
                self._write_json({"error": "worker_id is required"}, status=400)
                return
            command = self.coordinator_store.claim_worker_command(worker_id, worker_state=worker_state)
            self._write_json({"ok": True, "command": command})
            return

        if path == "/api/coord/worker-command/complete":
            worker_id = str(body.get("worker_id", "") or "").strip()
            command_id = _safe_int(body.get("command_id", 0), 0)
            success = bool(body.get("success", False))
            error = str(body.get("error", "") or "")
            if not worker_id or command_id <= 0:
                self._write_json({"error": "worker_id and command_id are required"}, status=400)
                return
            ok = self.coordinator_store.complete_worker_command(
                worker_id,
                command_id,
                success=success,
                error=error,
            )
            self._write_json({"ok": bool(ok)})
            return

        if path == "/api/coord/worker-config":
            worker_id = str(body.get("worker_id", "") or "").strip()
            content = str(body.get("content", "") or "")
            config_path = self._resolve_worker_config_path(worker_id)
            if config_path is None:
                self._write_json({"error": "worker config path not available"}, status=404)
                return
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(content, encoding="utf-8")
            self._write_json(
                {
                    "ok": True,
                    "worker_id": worker_id,
                    "config_path_rel": _to_repo_relative_path(config_path),
                    "bytes_written": len(content.encode("utf-8")),
                }
            )
            return

        if path == "/api/coord/fleet-signal-clear-output":
            self._write_json({"ok": True, **self.coordinator_store.bump_output_clear_generation()})
            return

        if path == "/api/coord/ui-preferences":
            page = str(body.get("page", "") or "").strip().lower()
            pref_key = str(body.get("key", "") or "").strip().lower()
            pref_value = body.get("value", {})
            if not page or not pref_key:
                self._write_json({"error": "page and key are required"}, status=400)
                return
            if not isinstance(pref_value, dict):
                self._write_json({"error": "value must be an object"}, status=400)
                return
            try:
                payload = self.coordinator_store.set_ui_preference(page=page, pref_key=pref_key, pref_value=pref_value)
            except Exception as exc:
                self.log_message("set_ui_preference failed: %r", exc)
                self._write_json({"error": "ui preference update failed", "detail": str(exc)}, status=500)
                return
            self._write_json({"ok": True, **payload})
            return
        if path == "/api/coord/errors/ingest":
            if self.log_store is None:
                self._write_json({"error": "structured log store is unavailable"}, status=503)
                return
            payload_events = body.get("events")
            if isinstance(payload_events, list):
                events = [item for item in payload_events if isinstance(item, dict)]
            elif isinstance(body, dict):
                events = [body]
            else:
                self._write_json({"error": "invalid payload"}, status=400)
                return
            if not events:
                self._write_json({"error": "at least one event is required"}, status=400)
                return
            try:
                inserted = int(self.log_store.insert_events(events))
            except Exception as exc:
                self.log_message("errors_ingest failed: %r", exc)
                self._write_json({"error": "error ingest failed", "detail": str(exc)}, status=500)
                return
            self._write_json({"ok": True, "inserted": inserted, "received": len(events)})
            return

        self._write_json({"error": "not found"}, status=404)

    def _is_safe_worker_id(self, worker_id: str) -> bool:
        return bool(re.fullmatch(r"[A-Za-z0-9._-]{1,80}", str(worker_id or "").strip()))

    @staticmethod
    def _is_workflow_runtime_worker(worker_id: str) -> bool:
        text = str(worker_id or "").strip().lower()
        return "-plugin-" in text or "-scheduler-" in text

    def _resolve_worker_config_path(self, worker_id: str) -> Path | None:
        wid = str(worker_id or "").strip()
        if not self._is_safe_worker_id(wid):
            return None
        candidates = [
            self.app_root / "config" / "workers" / f"{wid}.json",
            self.app_root / "config" / f"{wid}.json",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate.resolve()
        return candidates[0].resolve()

    def _resolve_worker_config_rel(self, worker_id: str) -> str | None:
        path = self._resolve_worker_config_path(worker_id)
        return _to_repo_relative_path(path) if path is not None else None

    def _discover_worker_log_links(self, worker_id: str) -> list[dict[str, str]]:
        wid = str(worker_id or "").strip()
        if not self._is_safe_worker_id(wid):
            return []
        patterns = [
            str(self.output_root.resolve() / f"**/*{wid}*.log"),
            str(self.app_root / f"**/*{wid}*.log"),
        ]
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        for pattern in patterns:
            for raw_path in glob.iglob(pattern, recursive=True):
                path = Path(raw_path)
                if not path.is_file():
                    continue
                rel = _to_repo_relative_path(path)
                if not rel:
                    continue
                key = rel.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append({"label": path.name, "relative": rel})
                if len(out) >= 8:
                    return out
        return out

    def _load_extractor_matches_domain(self, root_domain: str) -> Optional[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        if not rd or self.coordinator_store is None:
            return None
        artifact = self.coordinator_store.get_artifact(rd, "extractor_matches_zip")
        if artifact is None:
            return None
        content_sha256 = str(artifact.get("content_sha256", "") or "")
        cached = self.extractor_matches_cache.get(rd, content_sha256)
        if cached is not None:
            return {
                **cached,
                "cached": True,
                "updated_at_utc": artifact.get("updated_at_utc"),
                "content_size_bytes": int(artifact.get("content_size_bytes", 0) or 0),
            }
        content = bytes(artifact.get("content", b"") or b"")
        try:
            rows, files = _load_extractor_match_rows_from_zip(root_domain=rd, data=content)
        except Exception as exc:
            raise RuntimeError(f"failed to parse extractor zip for {rd}: {exc}") from exc
        self.extractor_matches_cache.set(
            rd,
            content_sha256,
            updated_at_utc=str(artifact.get("updated_at_utc", "") or "") or None,
            match_count=len(rows),
            rows=rows,
            files=files,
        )
        return {
            "root_domain": rd,
            "content_sha256": content_sha256,
            "updated_at_utc": artifact.get("updated_at_utc"),
            "content_size_bytes": int(artifact.get("content_size_bytes", 0) or 0),
            "match_count": len(rows),
            "rows": rows,
            "files": files,
            "cached": False,
        }

    def _load_fuzzing_domain_summary(self, root_domain: str) -> Optional[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        if not rd or self.coordinator_store is None:
            return None
        artifact = self.coordinator_store.get_artifact(rd, "fozzy_summary_json")
        if artifact is None:
            return None
        summary_sha256 = str(artifact.get("content_sha256", "") or "")
        cached = self.fuzzing_summary_cache.get(rd, summary_sha256)
        if cached is not None:
            return {
                **cached,
                "cached": True,
                "updated_at_utc": artifact.get("updated_at_utc"),
                "summary_content_size_bytes": int(artifact.get("content_size_bytes", 0) or 0),
            }
        summary = _decode_json_artifact(
            bytes(artifact.get("content", b"") or b""),
            str(artifact.get("content_encoding", "identity") or "identity"),
        )
        rows, totals = _flatten_fozzy_summary_rows(rd, summary)
        # Enrich with per-result baseline/fuzz content and timing from fozzy_results_zip when available.
        try:
            results_artifact = self.coordinator_store.get_artifact(rd, "fozzy_results_zip")
        except Exception:
            results_artifact = None
        if results_artifact is not None:
            details = _build_fozzy_result_details_map(bytes(results_artifact.get("content", b"") or b""))
            rows = _enrich_fuzzing_rows_from_result_details(rows, details)
        self.fuzzing_summary_cache.set(
            rd,
            summary_sha256,
            updated_at_utc=str(artifact.get("updated_at_utc", "") or "") or None,
            rows=rows,
            totals=totals,
        )
        return {
            "root_domain": rd,
            "summary_sha256": summary_sha256,
            "updated_at_utc": artifact.get("updated_at_utc"),
            "summary_content_size_bytes": int(artifact.get("content_size_bytes", 0) or 0),
            "rows": rows,
            "totals": totals,
            "cached": False,
        }

    def _load_fuzzing_zip_index(self, root_domain: str) -> Optional[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        if not rd or self.coordinator_store is None:
            return None
        artifact = self.coordinator_store.get_artifact(rd, "fozzy_results_zip")
        if artifact is None:
            return None
        content_sha256 = str(artifact.get("content_sha256", "") or "")
        cached = self.fuzzing_zip_index_cache.get(rd, content_sha256)
        if cached is not None:
            return cached
        files: list[dict[str, Any]] = []
        normalized_names: dict[str, str] = {}
        try:
            with zipfile.ZipFile(io.BytesIO(bytes(artifact.get("content", b"") or b"")), mode="r") as zf:
                infos = sorted(zf.infolist(), key=lambda item: item.filename.lower())
                for info in infos:
                    if info.is_dir():
                        continue
                    member_path = _normalize_zip_member_path(info.filename)
                    if not member_path:
                        continue
                    files.append(
                        {
                            "path": member_path,
                            "size": int(info.file_size or 0),
                            "compressed_size": int(info.compress_size or 0),
                        }
                    )
                    normalized_names[member_path] = info.filename
        except Exception:
            files = []
            normalized_names = {}
        self.fuzzing_zip_index_cache.set(rd, content_sha256, files=files, normalized_names=normalized_names)
        return self.fuzzing_zip_index_cache.get(rd, content_sha256)

    @staticmethod
    def _matches_row_search_text(row: dict[str, Any]) -> str:
        parts = [
            row.get("root_domain"),
            row.get("filter_name"),
            row.get("scope"),
            row.get("response_side"),
            row.get("regex"),
            row.get("url"),
            row.get("source_request_url"),
            row.get("result_file"),
            row.get("matched_text"),
            row.get("match_file"),
        ]
        return " ".join(str(item or "") for item in parts).lower()

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Nightmare/Fozzy report web UI server.")
    p.add_argument(
        "--config",
        default="server.json",
        help="Server config path (default: config/server.json). Relative paths resolve under config/.",
    )
    p.add_argument("--host", default=None, help=f"Bind host (default: {DEFAULT_HOST})")
    p.add_argument("--port", type=int, default=None, help=f"Bind port (default: {DEFAULT_PORT})")
    p.add_argument("--http-port", type=int, default=None, help="HTTP bind port (default: 80)")
    p.add_argument("--https-port", type=int, default=None, help="HTTPS bind port (default: 443)")
    p.add_argument("--cert-file", default=None, help="TLS certificate file for HTTPS listener")
    p.add_argument("--key-file", default=None, help="TLS private key file for HTTPS listener")
    p.add_argument("--database-url", default=None, help="Postgres DATABASE_URL for coordinator mode")
    p.add_argument("--log-database-url", default=None, help="Dedicated Postgres URL for log/reporting data (must be separate from coordinator DB)")
    p.add_argument("--coordinator-api-token", default=None, help="Bearer token required for /api/coord/* endpoints")
    p.add_argument(
        "--reset-coordinator",
        action="store_true",
        help="Truncate coordinator Postgres tables and exit (requires --database-url and matching --reset-confirm)",
    )
    p.add_argument(
        "--reset-confirm",
        default="",
        help=f"Must be {RESET_COORDINATOR_CONFIRM} when using --reset-coordinator",
    )
    p.add_argument(
        "--clear-output",
        action="store_true",
        help="With --reset-coordinator, also remove direct children under the configured output root",
    )
    p.add_argument(
        "--signal-workers-clear-disk",
        action="store_true",
        help="With --reset-coordinator, bump fleet output_clear_generation so workers clear their local output",
    )
    p.add_argument(
        "--output-root",
        default=None,
        help=f"Output root to summarize (default: {DEFAULT_OUTPUT_ROOT})",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config_path = _resolve_config_path(args.config)
    file_cfg = _read_json_dict(config_path)
    merged_cfg = {**_default_server_config(), **file_cfg}

    output_root_raw = str(_merged_value(args.output_root, merged_cfg, "output_root", "output"))
    output_root = Path(output_root_raw).expanduser()
    if not output_root.is_absolute():
        output_root = (BASE_DIR / output_root).resolve()
    host = str(_merged_value(args.host, merged_cfg, "host", DEFAULT_HOST) or DEFAULT_HOST).strip() or DEFAULT_HOST
    legacy_port = int(_merged_value(args.port, merged_cfg, "port", DEFAULT_PORT) or DEFAULT_PORT)
    http_port = int(_merged_value(args.http_port, merged_cfg, "http_port", 80) or 80)
    https_port = int(_merged_value(args.https_port, merged_cfg, "https_port", 443) or 443)
    cert_file_raw = str(_merged_value(args.cert_file, merged_cfg, "cert_file", "") or "").strip()
    key_file_raw = str(_merged_value(args.key_file, merged_cfg, "key_file", "") or "").strip()
    database_url = str(
        _merged_value(
            args.database_url,
            merged_cfg,
            "database_url",
            os.getenv("DATABASE_URL", ""),
        )
        or ""
    ).strip()
    log_database_url = str(
        _merged_value(
            args.log_database_url,
            merged_cfg,
            "log_database_url",
            os.getenv("LOG_DATABASE_URL", ""),
        )
        or ""
    ).strip()
    coordinator_token = str(
        _merged_value(
            args.coordinator_api_token,
            merged_cfg,
            "coordinator_api_token",
            os.getenv("COORDINATOR_API_TOKEN", ""),
        )
        or ""
    ).strip()
    master_report_regen_token = str(merged_cfg.get("master_report_regen_token") or "").strip()
    if not master_report_regen_token:
        master_report_regen_token = os.getenv("MASTER_REPORT_REGEN_TOKEN", "").strip()
    if not master_report_regen_token:
        master_report_regen_token = coordinator_token

    if args.http_port is None and args.https_port is None and args.port is not None:
        http_port = legacy_port
        https_port = 0
    if http_port and (http_port < 1 or http_port > 65535):
        raise ValueError("http_port must be in range 1..65535")
    if https_port and (https_port < 1 or https_port > 65535):
        raise ValueError("https_port must be in range 1..65535")

    if args.reset_coordinator:
        confirm = str(args.reset_confirm or "").strip()
        if confirm != RESET_COORDINATOR_CONFIRM:
            print(
                f"[server] --reset-coordinator requires --reset-confirm {RESET_COORDINATOR_CONFIRM}",
                file=sys.stderr,
                flush=True,
            )
            return 2
        if not database_url:
            print(
                "[server] --reset-coordinator requires database_url (config, DATABASE_URL, or --database-url)",
                file=sys.stderr,
                flush=True,
            )
            return 2
        store = CoordinatorStore(database_url)
        result = store.reset_coordinator_tables()
        if args.clear_output:
            result["output_clear"] = clear_output_root_children(output_root)
        if args.signal_workers_clear_disk:
            result["fleet_signal"] = store.bump_output_clear_generation()
        print(json.dumps(result, indent=2), flush=True)
        return 0

    if not database_url:
        print(
            "[server] startup requires database_url (config, DATABASE_URL, or --database-url)",
            file=sys.stderr,
            flush=True,
        )
        return 2
    if not log_database_url:
        print(
            "[server] startup requires log_database_url (config, LOG_DATABASE_URL, or --log-database-url)",
            file=sys.stderr,
            flush=True,
        )
        return 2
    if _urls_point_to_same_database(database_url, log_database_url):
        print(
            "[server] log_database_url must point to a separate database/server from database_url",
            file=sys.stderr,
            flush=True,
        )
        return 2

    coordinator_store: CoordinatorStore | None = CoordinatorStore(database_url)
    try:
        log_store: LogStore | None = LogStore(log_database_url)
    except Exception as exc:
        print(
            f"[server] failed to initialize required log store at log_database_url: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 2
    auth0r_store: Auth0rProfileStore | None = None
    try:
        auth0r_store = Auth0rProfileStore(database_url)
    except Exception as exc:
        print(
            f"[server] auth0r store unavailable; auth0r profile/result APIs will return 503: {exc}",
            file=sys.stderr,
            flush=True,
        )

    def _prepare_server(port_value: int) -> ThreadingHTTPServer:
        srv = ThreadingHTTPServer((host, port_value), DashboardHandler)
        srv.app_root = BASE_DIR  # type: ignore[attr-defined]
        srv.output_root = output_root  # type: ignore[attr-defined]
        srv.coordinator_store = coordinator_store  # type: ignore[attr-defined]
        srv.coordinator_token = coordinator_token  # type: ignore[attr-defined]
        srv.master_report_regen_token = master_report_regen_token  # type: ignore[attr-defined]
        srv._master_regen_lock = threading.Lock()  # type: ignore[attr-defined]
        srv.page_data_cache = _PageDataCache()  # type: ignore[attr-defined]
        srv.extractor_matches_cache = _ExtractorMatchesCache()  # type: ignore[attr-defined]
        srv.fuzzing_summary_cache = _FozzySummaryCache()  # type: ignore[attr-defined]
        srv.fuzzing_zip_index_cache = _ZipFileIndexCache()  # type: ignore[attr-defined]
        srv.log_store = log_store  # type: ignore[attr-defined]
        srv.auth0r_store = auth0r_store  # type: ignore[attr-defined]
        srv.workflow_interface_catalog = _workflow_interface_catalog_payload()  # type: ignore[attr-defined]
        _start_default_page_cache_warmer(srv, coordinator_store=coordinator_store)
        return srv

    servers: list[tuple[str, ThreadingHTTPServer]] = []
    if http_port:
        servers.append(("http", _prepare_server(http_port)))
    if https_port:
        cert_file = Path(cert_file_raw).expanduser().resolve() if cert_file_raw else None
        key_file = Path(key_file_raw).expanduser().resolve() if key_file_raw else None
        if cert_file is None or key_file is None or not cert_file.is_file() or not key_file.is_file():
            print(
                "[server] https listener disabled: provide valid cert_file/key_file for port 443",
                flush=True,
            )
        else:
            https_server = _prepare_server(https_port)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(certfile=str(cert_file), keyfile=str(key_file))
            https_server.socket = context.wrap_socket(https_server.socket, server_side=True)
            servers.append(("https", https_server))

    if not servers:
        raise RuntimeError("No active listeners configured. Enable http_port and/or https_port with valid TLS files.")

    print("[server] starting coordinator UI server", flush=True)
    print(f"[server] config={config_path}", flush=True)
    print(f"[server] app_root={BASE_DIR}", flush=True)
    print(f"[server] output_root={output_root}", flush=True)
    if coordinator_store is not None:
        print("[server] coordinator mode enabled (Postgres backend)", flush=True)
    if log_store is not None:
        print("[server] structured log store enabled (dedicated logging/reporting DB)", flush=True)
    all_domains_html = _find_all_domains_report_html(output_root)
    if all_domains_html is not None:
        print(f"[server] default route / serving all-domains report: {all_domains_html}", flush=True)
    else:
        print("[server] default route / serving workers (all_domains.results_summary.html not found)", flush=True)
    for scheme, srv in servers:
        bound = srv.server_address[1]
        print(f"[server] {scheme} listening on {scheme}://{host}:{bound}", flush=True)
    print(f"[server] workers route: http://{host}:{http_port or legacy_port}/workers", flush=True)

    threads: list[threading.Thread] = []
    for _scheme, srv in servers:
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        threads.append(t)
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n[server] interrupt received, shutting down.", flush=True)
    finally:
        for _scheme, srv in servers:
            stop_event = getattr(srv, "page_cache_warm_stop", None)
            if isinstance(stop_event, threading.Event):
                stop_event.set()
            try:
                srv.shutdown()
            except Exception:
                pass
            srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
