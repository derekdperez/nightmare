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

The dashboard provides:
- aggregate counts (domains discovered, completed/running/pending/failed),
- per-domain status rows with links to generated HTML reports and JSON artifacts,
- master-report links when available,
- recent log tail snippets.
"""

from __future__ import annotations

import argparse
import base64
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
from typing import Any, Optional
from urllib.parse import parse_qs, quote, unquote, urlparse

from output_cleanup import clear_output_root_children
from reporting.server_pages import (
    render_crawl_progress_html,
    render_dashboard_html,
    render_database_html,
    render_docker_status_html,
    render_extractor_matches_html,
    render_fuzzing_html,
    render_view_logs_html,
    render_workers_html,
)
from server_app.store import CoordinatorStore
from logging_app.store import LogStore

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = BASE_DIR / "output"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 443
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "server.json"
MAX_LOG_TAIL_BYTES = 64 * 1024
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
EST_TZ = timezone(timedelta(hours=-5), name="EST")


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


def _read_tail_text(path: Path, max_bytes: int = MAX_LOG_TAIL_BYTES) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes), os.SEEK_SET)
            data = handle.read()
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def _read_tail_lines(path: Path, *, lines: int = DEFAULT_VIEW_LOG_LINES, max_bytes: int = MAX_LOG_TAIL_BYTES) -> str:
    text = _read_tail_text(path, max_bytes=max_bytes)
    if not text:
        return ""
    line_count = max(1, int(lines or DEFAULT_VIEW_LOG_LINES))
    return "\n".join(text.splitlines()[-line_count:])


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
    if not prefixes:
        # Keep deterministic fallback order for better error reporting.
        prefixes = [["docker", "compose"], ["docker-compose"]]
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
    for name in ("docker-compose.central.yml", "docker-compose.local.yml", "docker-compose.worker.yml"):
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
                "nightmare-worker*,nightmare-central*",
            )
            or "nightmare-worker*,nightmare-central*"
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
        line = str(raw_line or "").strip()
        if not line:
            continue
        payload: dict[str, Any] = {}
        if line.startswith("{") and line.endswith("}"):
            try:
                parsed = json.loads(line)
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
            ok, tail, error = _read_docker_log_tail(name, max(1, int(log_lines)))
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
        (output_root_resolved, 6),
        (app_root_resolved / "output", 6),
        (app_root_resolved / "logs", 4),
        (app_root_resolved / "deploy", 3),
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
    worker = _list_worker_vm_docker_containers(log_lines=0, timeout_seconds_override=15)
    rows = worker.get("containers", []) if isinstance(worker.get("containers"), list) else []
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
                "system": "worker_vm",
                "label": f"{instance_id}:{container_name}",
                "container_name": container_name,
                "instance_id": instance_id,
                "status": str(row.get("status", "") or ""),
                "image": str(row.get("image", "") or ""),
                "is_application": True,
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
    sources.sort(key=lambda item: (str(item.get("source_type", "")), str(item.get("label", "")).lower()))
    if not sources:
        sources = [
            {
                "source_id": "docker:nightmare-coordinator-server",
                "source_type": "docker",
                "system": "docker",
                "label": "nightmare-coordinator-server",
                "container_name": "nightmare-coordinator-server",
                "status": "unknown",
                "image": "",
                "is_application": True,
            },
            {
                "source_id": "docker:nightmare-postgres",
                "source_type": "docker",
                "system": "docker",
                "label": "nightmare-postgres",
                "container_name": "nightmare-postgres",
                "status": "unknown",
                "image": "",
                "is_application": True,
            },
        ]
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


def _read_docker_log_tail(container_name: str, lines: int) -> tuple[bool, str, str]:
    cmd = ["docker", "logs", "--tail", str(max(1, int(lines or DEFAULT_VIEW_LOG_LINES))), str(container_name or "").strip()]
    ok, stdout, stderr, exit_code = _run_command(cmd, timeout_seconds=30)
    if ok:
        # docker logs often writes output to stderr.
        text = "\n".join([part for part in [stdout.strip(), stderr.strip()] if part]).strip()
        return True, text, ""
    error = stderr.strip() or f"docker logs failed with exit code {exit_code}"
    return False, "", error


def _read_docker_log_full(container_name: str) -> tuple[bool, str, str]:
    cmd = ["docker", "logs", str(container_name or "").strip()]
    ok, stdout, stderr, exit_code = _run_command(cmd, timeout_seconds=60)
    if ok:
        text = "\n".join([part for part in [stdout.strip(), stderr.strip()] if part]).strip()
        return True, text[:MAX_LOG_DOWNLOAD_BYTES], ""
    error = stderr.strip() or f"docker logs failed with exit code {exit_code}"
    return False, "", error


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
    log_cmd = f"docker logs {shlex.quote(name)} 2>&1 | head -c {MAX_LOG_DOWNLOAD_BYTES}"
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
        if system == "worker_vm":
            instance_id = str(source.get("instance_id", "") or "").strip()
            if full:
                return _read_worker_vm_docker_log_full(instance_id, container_name)
            return _read_worker_vm_docker_log_tail(instance_id, container_name, lines)
        if full:
            return _read_docker_log_full(container_name)
        return _read_docker_log_tail(container_name, lines)
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
                data = handle.read(MAX_LOG_DOWNLOAD_BYTES)
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
                "latest_log_tail": _read_tail_text(log_files[-1]) if log_files else "",
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
    def coordinator_token(self) -> str:
        return str(getattr(self.server, "coordinator_token", "") or "")  # type: ignore[attr-defined]

    @property
    def master_report_regen_token(self) -> str:
        return str(getattr(self.server, "master_report_regen_token", "") or "").strip()  # type: ignore[attr-defined]

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

    def _write_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_text(self, text: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> None:
        body = text.encode("utf-8", errors="replace")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
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
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
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
            data = file_path.read_bytes()
        except OSError:
            self._write_text("Read error", status=500)
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

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
        return cookie_token == token

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
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            all_domains_html = _find_all_domains_report_html(self.output_root)
            if all_domains_html is not None:
                self._serve_static_file(all_domains_html)
            else:
                self._write_text(render_dashboard_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/dashboard":
            self._write_text(render_dashboard_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/workers":
            self._write_text(render_workers_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/database":
            self._write_text(render_database_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/crawl-progress":
            self._write_text(render_crawl_progress_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/extractor-matches":
            self._write_text(render_extractor_matches_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/fuzzing":
            self._write_text(render_fuzzing_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/docker-status":
            self._write_text(render_docker_status_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/view-logs":
            self._write_text(render_view_logs_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/api/summary":
            payload = collect_dashboard_data(self.output_root, self.coordinator_store)
            self._write_json(payload)
            return
        if path == "/api/log-tail":
            rel = (query.get("path") or [""])[0]
            resolved = _normalize_and_validate_relative_path(self.app_root, unquote(rel))
            if resolved is None or not resolved.is_file():
                self._write_json({"error": "invalid log path"}, status=400)
                return
            self._write_json({"path": str(resolved), "tail": _read_tail_text(resolved)})
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
                sources = _collect_view_log_sources_cached(self.app_root, self.output_root)[:20]
            events: list[dict[str, Any]] = []
            for source in sources:
                ok, text, error = _read_log_source_text(source, lines=lines, full=False, app_root=self.app_root)
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
            try:
                payload = self.coordinator_store.crawl_progress_snapshot(limit=limit)
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
        if path.startswith("/files/"):
            rel = unquote(path[len("/files/"):])
            resolved = _normalize_and_validate_relative_path(self.app_root, rel)
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
        body = self._read_json_body()

        if path == "/api/regenerate-master-report":
            self._handle_regenerate_master_report()
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

        if path == "/api/coord/register-targets":
            targets_payload = body.get("targets", [])
            targets: list[str] = []
            if isinstance(targets_payload, list):
                targets = [str(item) for item in targets_payload if str(item or "").strip()]
            elif isinstance(targets_payload, str):
                targets = [line for line in str(targets_payload).splitlines() if line.strip()]
            result = self.coordinator_store.register_targets(targets)
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
            ok = self.coordinator_store.enqueue_stage(root_domain, stage)
            self._write_json({"ok": bool(ok)})
            return

        if path == "/api/coord/stage/claim":
            worker_id = str(body.get("worker_id", "") or "").strip()
            stage = str(body.get("stage", "") or "").strip().lower()
            lease_seconds = _safe_int(body.get("lease_seconds", DEFAULT_COORDINATOR_LEASE_SECONDS), DEFAULT_COORDINATOR_LEASE_SECONDS)
            item = self.coordinator_store.claim_stage(stage, worker_id, lease_seconds)
            self._write_json({"ok": True, "entry": item})
            return

        if path == "/api/coord/stage/heartbeat":
            worker_id = str(body.get("worker_id", "") or "").strip()
            root_domain = str(body.get("root_domain", "") or "").strip().lower()
            stage = str(body.get("stage", "") or "").strip().lower()
            lease_seconds = _safe_int(body.get("lease_seconds", DEFAULT_COORDINATOR_LEASE_SECONDS), DEFAULT_COORDINATOR_LEASE_SECONDS)
            ok = self.coordinator_store.heartbeat_stage(root_domain, stage, worker_id, lease_seconds)
            self._write_json({"ok": bool(ok)})
            return

        if path == "/api/coord/stage/complete":
            worker_id = str(body.get("worker_id", "") or "").strip()
            root_domain = str(body.get("root_domain", "") or "").strip().lower()
            stage = str(body.get("stage", "") or "").strip().lower()
            exit_code = _safe_int(body.get("exit_code", 0), 0)
            error = str(body.get("error", "") or "")
            ok = self.coordinator_store.complete_stage(
                root_domain,
                stage,
                worker_id,
                exit_code=exit_code,
                error=error,
            )
            self._write_json({"ok": bool(ok)})
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
            if command not in {"start", "pause", "stop"}:
                self._write_json({"error": "command must be one of: start, pause, stop"}, status=400)
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

        self._write_json({"error": "not found"}, status=404)

    def _is_safe_worker_id(self, worker_id: str) -> bool:
        return bool(re.fullmatch(r"[A-Za-z0-9._-]{1,80}", str(worker_id or "").strip()))

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
    p = argparse.ArgumentParser(description="Nightmare/Fozzy report web dashboard server.")
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

    def _prepare_server(port_value: int) -> ThreadingHTTPServer:
        srv = ThreadingHTTPServer((host, port_value), DashboardHandler)
        srv.app_root = BASE_DIR  # type: ignore[attr-defined]
        srv.output_root = output_root  # type: ignore[attr-defined]
        srv.coordinator_store = coordinator_store  # type: ignore[attr-defined]
        srv.coordinator_token = coordinator_token  # type: ignore[attr-defined]
        srv.master_report_regen_token = master_report_regen_token  # type: ignore[attr-defined]
        srv._master_regen_lock = threading.Lock()  # type: ignore[attr-defined]
        srv.extractor_matches_cache = _ExtractorMatchesCache()  # type: ignore[attr-defined]
        srv.fuzzing_summary_cache = _FozzySummaryCache()  # type: ignore[attr-defined]
        srv.fuzzing_zip_index_cache = _ZipFileIndexCache()  # type: ignore[attr-defined]
        srv.log_store = log_store  # type: ignore[attr-defined]
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

    print("[server] starting dashboard/coordinator server", flush=True)
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
        print("[server] default route / serving dashboard (all_domains.results_summary.html not found)", flush=True)
    for scheme, srv in servers:
        bound = srv.server_address[1]
        print(f"[server] {scheme} listening on {scheme}://{host}:{bound}", flush=True)
    print(f"[server] dashboard route: http://{host}:{http_port or legacy_port}/dashboard", flush=True)

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
            try:
                srv.shutdown()
            except Exception:
                pass
            srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



