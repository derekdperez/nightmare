#!/usr/bin/env python3
"""
TaiLOR companion CLI in Python.

Purpose:
- Reuse TaiLOR repository structure (task-workflows.json, prompts/, tailor-state.db)
- Create and manage agents/channels in the shared SQLite schema used by TaiLOR
- Monitor Gmail inboxes and run configurable AI-assisted email workflows
- Automate supported browser-backed messaging flows such as FetLife
- Provide development utilities for bulk file-to-AI operations

Primarily uses the Python stdlib. Some optional features also require `selenium` and `beautifulsoup4`.
"""

from __future__ import annotations

import argparse
import atexit
import ast
import base64
import contextlib
import copy
import csv
import datetime as dt
import difflib
import email.utils
import fnmatch
import hashlib
import random
import html
import http.server
import io
import json
import os
import re
import shutil
import sqlite3
import socket
import subprocess
import tempfile
import threading
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import webbrowser
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator
from workflow_engine import WorkflowComposer, WorkflowPluginRegistry

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
except ImportError:
    webdriver = None
    ChromeOptions = None
    ChromeService = None
    By = None
    EC = None
    WebDriverWait = None

UTC = dt.timezone.utc
TRACE_LEVEL = 1
_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_PREFER_IPV4_ENABLED = False
_NETWORK_LOG_LOCK = threading.Lock()
NETWORK_LOG_PATH = Path(os.getenv("TAILOR_NETWORK_LOG_PATH", "artifacts/network-api.ndjson"))
API_USAGE_DIR = Path(os.getenv("TAILOR_API_USAGE_DIR", "api-usage"))
USER_NOTIFICATIONS_PATH = Path(os.getenv("TAILOR_USER_NOTIFICATIONS_PATH", "artifacts/user-notifications.ndjson"))
EXEC_LOGS_DIR = Path(os.getenv("TAILOR_EXEC_LOGS_DIR", "exec-logs"))
ENGINE_LOG_DIR = Path(os.getenv("TAILOR_ENGINE_LOG_DIR", "logs/engine"))
WORKFLOW_PLUGINS_DIR = Path(os.getenv("TAILOR_WORKFLOW_PLUGINS_DIR", "scripts/workflow_plugins"))
PLUGIN_SCHEMA_PATH = Path("schemas/workflow-plugin.schema.json")
DEFAULT_AI_CHAT_TIMEOUT = int(os.getenv("TAILOR_AI_CHAT_TIMEOUT", "600"))
APPSETTINGS_RELATIVE_PATH = Path("config") / "appsettings.json"
APPSETTINGS_LOCAL_RELATIVE_PATH = Path("config") / "appsettings.local.json"
LEGACY_APPSETTINGS_RELATIVE_PATH = Path("src") / "TaiLOR.Cli.Host" / "appsettings.json"
LEGACY_APPSETTINGS_LOCAL_RELATIVE_PATH = Path("src") / "TaiLOR.Cli.Host" / "appsettings.local.json"
_EXEC_LOG_CONTEXT = threading.local()
_ENGINE_WORKFLOW_DEV_API = threading.local()
FETLIFE_TEMP_PROFILE_DIR = Path(os.getenv("LOCALAPPDATA", "")) / "Temp" / "tailor-fetlife-chrome"


# ----------------------------
# Utilities
# ----------------------------

def now_iso() -> str:
    return dt.datetime.now(tz=UTC).isoformat()


def new_id() -> str:
    return uuid.uuid4().hex


def _normalize_chat_completion_content(raw: Any) -> str:
    """Turn ``choices[].message.content`` into plain text (string or multimodal blocks)."""
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        parts: list[str] = []
        for item in raw:
            if isinstance(item, dict):
                if item.get("type") == "text" and isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif isinstance(item.get("text"), str):
                    parts.append(item["text"])
            elif isinstance(item, str):
                parts.append(item)
        return "".join(parts)
    return str(raw)


def _chat_api_token_limit_fields(model: str, max_tokens: int) -> dict[str, Any]:
    """OpenAI-compatible chat completions: older stacks expect ``max_tokens``, newer models ``max_completion_tokens``."""
    m = (model or "").strip().lower()
    if any(
        token in m
        for token in (
            "gpt-5",
            "gpt-4.1",
            "o1-preview",
            "o1-mini",
            "o1-pro",
            "o3-",
            "o3",
            "o4-",
            "chatgpt-4o",
        )
    ):
        return {"max_completion_tokens": max_tokens}
    return {"max_tokens": max_tokens}


def _parse_json_from_model_text(text: str, *, label: str = "model output") -> Any:
    """Parse JSON from model text; allow markdown fences and trailing prose."""
    raw = (text or "").strip()
    if not raw:
        raise RuntimeError(
            f"Cannot parse JSON from empty {label}. The model returned no text. "
            "Check input size, max_tokens, and logs/engine dev_api_response for the raw API payload."
        )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    bom_stripped = raw.lstrip("\ufeff")
    if bom_stripped != raw:
        try:
            return json.loads(bom_stripped)
        except json.JSONDecodeError:
            raw = bom_stripped
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw, re.IGNORECASE)
    if fence:
        inner = fence.group(1).strip()
        try:
            return json.loads(inner)
        except json.JSONDecodeError:
            raw = inner
    for opener, closer in (("{", "}"), ("[", "]")):
        start = raw.find(opener)
        if start < 0:
            continue
        depth = 0
        for i in range(start, len(raw)):
            ch = raw[i]
            if ch == opener:
                depth += 1
            elif ch == closer:
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(raw[start : i + 1])
                    except json.JSONDecodeError:
                        break
    raise RuntimeError(
        f"{label} is not valid JSON (after fence/brace extraction). First 400 chars: {raw[:400]!r}"
    )


def set_trace_level(level: int) -> None:
    global TRACE_LEVEL
    TRACE_LEVEL = max(0, int(level))


def trace_level() -> int:
    return TRACE_LEVEL


def trace(minimum: int, message: str) -> None:
    if TRACE_LEVEL >= minimum:
        print(message, flush=True)


def trace_excerpt(value: Any, limit: int = 280) -> str:
    text = _context_value_to_text(value) if "_context_value_to_text" in globals() else str(value)
    text = text.replace("\r", " ").replace("\n", "\\n")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _console_safe_text(value: Any) -> str:
    text = str(value or "")
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        return text.encode(encoding, errors="replace").decode(encoding, errors="replace")
    except Exception:
        return text.encode("utf-8", errors="replace").decode("utf-8", errors="replace")


def _slugify_name(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", str(value or "").strip().lower())
    return re.sub(r"-+", "-", text).strip("-") or "plugin"


def _redact_network_text(text: str) -> str:
    if not text:
        return text
    redacted = text
    redacted = re.sub(r'("Authorization"\s*:\s*")([^"]+)(")', r'\1<redacted>\3', redacted, flags=re.IGNORECASE)
    redacted = re.sub(r'("?(?:access_token|refresh_token|client_secret|code)"?\s*[:=]\s*")([^"]+)(")', r'\1<redacted>\3', redacted, flags=re.IGNORECASE)
    redacted = re.sub(r'((?:^|[?&])(?:access_token|refresh_token|client_secret|code)=)([^&\s]+)', r'\1<redacted>', redacted, flags=re.IGNORECASE)
    redacted = re.sub(r"(Authorization:\s*Bearer\s+)([^\s]+)", r"\1<redacted>", redacted, flags=re.IGNORECASE)
    return redacted


def _redact_headers_for_log(headers: dict[str, Any]) -> dict[str, Any]:
    safe_headers = dict(headers)
    for key in list(safe_headers.keys()):
        lk = key.lower()
        if lk in ("authorization", "api-key", "x-api-key"):
            safe_headers[key] = "<redacted>"
    return safe_headers


def _api_usage_file_for_url(url: str) -> Path | None:
    parsed = urllib.parse.urlparse(url or "")
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return None
    safe_host = re.sub(r"[^a-z0-9._-]+", "_", host)
    return API_USAGE_DIR / f"{safe_host}.ndjson"


def _update_api_usage_file(event: dict[str, Any]) -> None:
    if str(event.get("event", "")).strip().lower() != "request":
        return
    usage_path = _api_usage_file_for_url(str(event.get("url", "")))
    if usage_path is None:
        return
    API_USAGE_DIR.mkdir(parents=True, exist_ok=True)
    request_record = {
        "logged_utc": str(event.get("logged_utc") or now_iso()),
        "method": str(event.get("method", "") or ""),
        "url": str(event.get("url", "") or ""),
        "timeout_seconds": event.get("timeout_seconds"),
        "body_raw": str(event.get("body_raw", "") or ""),
    }
    existing_lines: list[str] = []
    if usage_path.exists():
        existing_lines = usage_path.read_text(encoding="utf-8", errors="replace").splitlines()
    request_lines = existing_lines[1:] if existing_lines else []
    request_lines.append(json.dumps(request_record, ensure_ascii=False))

    now_utc = dt.datetime.now(tz=UTC)
    windows = {
        "total_calls_ever": 0,
        "total_calls_last_7_days": 0,
        "total_calls_last_24_hours": 0,
        "total_calls_last_1_hour": 0,
        "total_calls_last_5_minutes": 0,
    }
    seven_days_ago = now_utc - dt.timedelta(days=7)
    day_ago = now_utc - dt.timedelta(hours=24)
    hour_ago = now_utc - dt.timedelta(hours=1)
    five_minutes_ago = now_utc - dt.timedelta(minutes=5)
    for line in request_lines:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        logged_text = str(obj.get("logged_utc", "") or "")
        try:
            logged_dt = dt.datetime.fromisoformat(logged_text.replace("Z", "+00:00"))
        except ValueError:
            continue
        if logged_dt.tzinfo is None:
            logged_dt = logged_dt.replace(tzinfo=UTC)
        logged_dt = logged_dt.astimezone(UTC)
        windows["total_calls_ever"] += 1
        if logged_dt >= seven_days_ago:
            windows["total_calls_last_7_days"] += 1
        if logged_dt >= day_ago:
            windows["total_calls_last_24_hours"] += 1
        if logged_dt >= hour_ago:
            windows["total_calls_last_1_hour"] += 1
        if logged_dt >= five_minutes_ago:
            windows["total_calls_last_5_minutes"] += 1
    summary = {
        "api": usage_path.stem,
        **windows,
        "updated_utc": now_iso(),
    }
    content = json.dumps(summary, ensure_ascii=False) + "\n"
    if request_lines:
        content += "\n".join(request_lines) + "\n"
    usage_path.write_text(content, encoding="utf-8")


def _append_network_log(event: dict[str, Any]) -> None:
    NETWORK_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    record = dict(event)
    record.setdefault("logged_utc", now_iso())
    with _NETWORK_LOG_LOCK:
        with NETWORK_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        _update_api_usage_file(record)
    active = _current_exec_log_context()
    if active:
        event_name = str(record.get("event") or "").strip().lower()
        body_lines = [
            f"Method: {record.get('method', '')}",
            f"URL: {record.get('url', '')}",
        ]
        if "status" in record:
            body_lines.append(f"Status: {record.get('status')}")
        if "duration_ms" in record:
            body_lines.append(f"Duration ms: {record.get('duration_ms')}")
        headers = record.get("headers")
        if headers:
            body_lines.append("Headers:")
            body_lines.append(json.dumps(headers, indent=2, ensure_ascii=False, default=str))
        body_raw = str(record.get("body_raw") or "")
        if body_raw:
            body_lines.append("Body:")
            body_lines.append(body_raw)
        title_map = {
            "request": "Workflow API request",
            "response": "Workflow API response",
            "error": "Workflow API error",
        }
        _append_active_exec_log(
            event_type=f"api_{event_name or 'event'}",
            title=title_map.get(event_name, "Workflow API event"),
            body="\n".join(body_lines),
        )


def _request_text(url: str, method: str = "GET", headers: dict[str, str] | None = None,
                  data: bytes | None = None, timeout: int = 60) -> tuple[str, int]:
    started = time.time()
    request_headers = headers or {}
    safe_headers = _redact_headers_for_log(request_headers)
    request_body_text = data.decode("utf-8", errors="replace") if data is not None else ""
    correlation_id = new_id()
    dev_proj = _engine_dev_api_active_project()
    url_path = urllib.parse.urlparse(url).path or ""
    human_base = f"{method} {url_path[:200] or url[:200]} id={correlation_id[:12]}"
    trace(2, f"[http] {method} {url}")
    trace(3, f"[http] timeout={timeout}s headers={json.dumps(safe_headers, ensure_ascii=False)}")
    if data is not None:
        trace(3, f"[http] request-body={trace_excerpt(_redact_network_text(request_body_text), 1200)}")
    _append_network_log({
        "event": "request",
        "method": method,
        "url": _redact_network_text(url),
        "timeout_seconds": timeout,
        "headers": safe_headers,
        "body_raw": _redact_network_text(request_body_text),
    })
    if dev_proj:
        _append_engine_log(
            dev_proj,
            {
                "event": "dev_api_request",
                "human_summary": f"dev_api_request | {human_base} | body_chars={len(request_body_text)}",
                "correlation_id": correlation_id,
                "method": method,
                "url": url,
                "timeout_seconds": timeout,
                "request_headers": _redact_headers_for_log(dict(request_headers)),
                "request_body_raw": request_body_text,
                "dev_workspace_api_log": True,
                **_engine_log_workflow_run_tags(),
            },
        )
    req = urllib.request.Request(url=url, method=method, headers=request_headers, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
            elapsed = time.time() - started
            status = getattr(resp, "status", 200)
            response_headers = dict(getattr(resp, "headers", {}).items()) if getattr(resp, "headers", None) else {}
            trace(2, f"[http] {method} {url} -> {status} in {elapsed:.2f}s")
            trace(3, f"[http] response-body={trace_excerpt(_redact_network_text(payload), 1200)}")
            _append_network_log({
                "event": "response",
                "method": method,
                "url": _redact_network_text(url),
                "status": status,
                "duration_ms": int(elapsed * 1000),
                "headers": _redact_headers_for_log(response_headers),
                "body_raw": _redact_network_text(payload),
            })
            if dev_proj:
                _append_engine_log(
                    dev_proj,
                    {
                        "event": "dev_api_response",
                        "human_summary": (
                            f"dev_api_response | {human_base} | HTTP {status} | "
                            f"{int(elapsed * 1000)}ms | resp_chars={len(payload)}"
                        ),
                        "correlation_id": correlation_id,
                        "method": method,
                        "url": url,
                        "status": status,
                        "duration_ms": int(elapsed * 1000),
                        "response_headers": _redact_headers_for_log(response_headers),
                        "response_body_raw": payload,
                        "dev_workspace_api_log": True,
                        **_engine_log_workflow_run_tags(),
                    },
                )
            return payload, status
    except urllib.error.HTTPError as ex:
        detail = ex.read().decode("utf-8", errors="replace")
        elapsed = time.time() - started
        trace(1, f"[http] {method} {url} -> HTTP {ex.code} in {elapsed:.2f}s")
        trace(3, f"[http] error-body={trace_excerpt(_redact_network_text(detail), 1200)}")
        _append_network_log({
            "event": "error",
            "method": method,
            "url": _redact_network_text(url),
            "status": ex.code,
            "duration_ms": int(elapsed * 1000),
            "headers": _redact_headers_for_log(dict(ex.headers.items()) if ex.headers else {}),
            "body_raw": _redact_network_text(detail),
        })
        if dev_proj:
            _append_engine_log(
                dev_proj,
                {
                    "event": "dev_api_http_error",
                    "human_summary": f"dev_api_http_error | {human_base} | HTTP {ex.code} | {int(elapsed * 1000)}ms",
                    "correlation_id": correlation_id,
                    "method": method,
                    "url": url,
                    "status": ex.code,
                    "duration_ms": int(elapsed * 1000),
                    "response_headers": _redact_headers_for_log(dict(ex.headers.items()) if ex.headers else {}),
                    "response_body_raw": detail,
                    "dev_workspace_api_log": True,
                    **_engine_log_workflow_run_tags(),
                },
            )
        raise RuntimeError(f"HTTP {ex.code} {url}\n{detail}") from ex
    except urllib.error.URLError as ex:
        elapsed = time.time() - started
        trace(1, f"[http] {method} {url} -> URLError in {elapsed:.2f}s :: {ex}")
        if dev_proj:
            reason = getattr(ex, "reason", None)
            _append_engine_log(
                dev_proj,
                {
                    "event": "dev_api_url_error",
                    "human_summary": f"dev_api_url_error | {human_base} | {type(ex).__name__}",
                    "correlation_id": correlation_id,
                    "method": method,
                    "url": url,
                    "duration_ms": int(elapsed * 1000),
                    "error_type": type(ex).__name__,
                    "error_message": str(ex),
                    "error_reason": repr(reason),
                    "dev_workspace_api_log": True,
                    **_engine_log_workflow_run_tags(),
                },
            )
        raise
    except Exception as ex:
        elapsed = time.time() - started
        if dev_proj:
            _append_engine_log(
                dev_proj,
                {
                    "event": "dev_api_transport_error",
                    "human_summary": f"dev_api_transport_error | {human_base} | {type(ex).__name__}",
                    "correlation_id": correlation_id,
                    "method": method,
                    "url": url,
                    "duration_ms": int(elapsed * 1000),
                    "error_type": type(ex).__name__,
                    "error_message": str(ex),
                    "dev_workspace_api_log": True,
                    **_engine_log_workflow_run_tags(),
                },
            )
        raise


def prefer_ipv4_resolution() -> None:
    global _PREFER_IPV4_ENABLED
    if _PREFER_IPV4_ENABLED:
        return

    def _ipv4_first_getaddrinfo(host: str, port: Any, family: int = 0, type: int = 0,
                                proto: int = 0, flags: int = 0) -> list[tuple[Any, ...]]:
        results = _ORIGINAL_GETADDRINFO(host, port, family, type, proto, flags)
        return sorted(results, key=lambda item: 0 if item[0] == socket.AF_INET else 1)

    socket.getaddrinfo = _ipv4_first_getaddrinfo
    _PREFER_IPV4_ENABLED = True


def ensure_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def _add_repeat_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--repeat",
        help='Create or update a recurring Windows scheduled task instead of running now. Examples: "5 minutes", "hourly", "daily".',
    )
    parser.add_argument(
        "--task-name",
        help="Optional Windows Task Scheduler name to create/update for --repeat.",
    )


def _add_trace_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--verbose", action="store_true", help="Output more detailed information about what the script is doing.")
    parser.add_argument("--diagnostic", action="store_true", help="Output exhaustive diagnostic tracing, including HTTP/file/workflow details.")


def _sanitize_task_name(name: str) -> str:
    cleaned = re.sub(r'[<>:\"/\\\\|?*]+', "-", (name or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    return cleaned or "TaiLOR Workflow"


def _parse_repeat_schedule(text: str) -> tuple[str, int, str]:
    value = (text or "").strip().lower()
    if not value:
        raise RuntimeError("Repeat frequency is required when using --repeat.")
    if value in {"hourly", "every hour", "each hour"}:
        return ("HOURLY", 1, "hourly")
    if value in {"daily", "every day", "each day"}:
        return ("DAILY", 1, "daily")

    match = re.fullmatch(r"(?:every\s+)?(\d+)\s*(minute|minutes|min|mins|hour|hours|day|days)", value)
    if not match:
        raise RuntimeError(
            f'Unsupported repeat frequency: "{text}". Supported examples: "5 minutes", "hourly", "daily".'
        )

    interval = int(match.group(1))
    unit = match.group(2)
    if unit.startswith("min"):
        if interval < 1 or interval > 1439:
            raise RuntimeError("Minute-based repeats must be between 1 and 1439 minutes.")
        return ("MINUTE", interval, f"every {interval} minute(s)")
    if unit.startswith("hour"):
        if interval < 1 or interval > 23:
            raise RuntimeError("Hourly repeats must be between 1 and 23 hours.")
        return ("HOURLY", interval, f"every {interval} hour(s)")
    if interval < 1 or interval > 365:
        raise RuntimeError("Daily repeats must be between 1 and 365 days.")
    return ("DAILY", interval, f"every {interval} day(s)")


def _append_cli_option(parts: list[str], flag: str, value: Any, *, path_base: Path | None = None) -> None:
    if value is None:
        return
    if isinstance(value, bool):
        if value:
            parts.append(flag)
        return
    text = str(value)
    if path_base is not None:
        text = str((path_base / text).resolve() if not Path(text).is_absolute() else Path(text).resolve())
    parts.extend([flag, text])


def _build_scheduled_workflow_invocation(args: argparse.Namespace, project: "TailorProject") -> list[str]:
    script_path = Path(__file__).resolve()
    parts = [
        sys.executable,
        str(script_path),
        args.cmd,
    ]

    if args.cmd == "workflow-exec":
        _append_cli_option(parts, "--workflow-file", args.workflow_file, path_base=project.root)
        _append_cli_option(parts, "--model", args.model)
        _append_cli_option(parts, "--api-key", args.api_key)
        _append_cli_option(parts, "--ai-endpoint", args.ai_endpoint)
        _append_cli_option(parts, "--temperature", args.temperature)
        _append_cli_option(parts, "--max-tokens", args.max_tokens)
        _append_cli_option(parts, "--ai-timeout-seconds", getattr(args, "ai_timeout_seconds", None))
        _append_cli_option(parts, "--dry-run", args.dry_run)
        _append_cli_option(parts, "--throttle-seconds", args.throttle_seconds)
        _append_cli_option(parts, "--verbosity", args.verbosity)
        _append_cli_option(parts, "--output-context-json", args.output_context_json, path_base=project.root)
        return parts

    if args.cmd == "run-workflow":
        _append_cli_option(parts, "--agent", args.agent)
        _append_cli_option(parts, "--workflow", args.workflow)
        _append_cli_option(parts, "--model", args.model)
        _append_cli_option(parts, "--api-key", args.api_key)
        _append_cli_option(parts, "--ai-endpoint", args.ai_endpoint)
        _append_cli_option(parts, "--temperature", args.temperature)
        _append_cli_option(parts, "--max-tokens", args.max_tokens)
        _append_cli_option(parts, "--ai-timeout-seconds", getattr(args, "ai_timeout_seconds", None))
        _append_cli_option(parts, "--dry-run", args.dry_run)
        _append_cli_option(parts, "--throttle-seconds", args.throttle_seconds)
        _append_cli_option(parts, "--verbosity", args.verbosity)
        _append_cli_option(parts, "--output-context-json", args.output_context_json, path_base=project.root)
        return parts

    if args.cmd == "workflow-run":
        _append_cli_option(parts, "--workflow", args.workflow)
        _append_cli_option(parts, "--channel-id", args.channel_id)
        _append_cli_option(parts, "--from-email", args.from_email)
        _append_cli_option(parts, "--reply-to", args.reply_to)
        _append_cli_option(parts, "--subject", args.subject)
        _append_cli_option(parts, "--body", args.body)
        _append_cli_option(parts, "--model", args.model)
        _append_cli_option(parts, "--api-key", args.api_key)
        _append_cli_option(parts, "--ai-endpoint", args.ai_endpoint)
        _append_cli_option(parts, "--temperature", args.temperature)
        _append_cli_option(parts, "--max-tokens", args.max_tokens)
        _append_cli_option(parts, "--dry-run", args.dry_run)
        return parts

    raise RuntimeError(f"Repeat scheduling is not supported for command: {args.cmd}")


def _write_scheduled_wrapper(task_name: str, invocation: list[str], project_root: Path) -> Path:
    wrapper_dir = project_root / ".tailor-schedules"
    wrapper_dir.mkdir(parents=True, exist_ok=True)
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", task_name).strip("-") or "tailor-workflow"
    wrapper_path = wrapper_dir / f"{slug}.cmd"
    body = [
        "@echo off",
        f'cd /d "{project_root.resolve()}"',
        subprocess.list2cmdline(invocation),
    ]
    wrapper_path.write_text("\r\n".join(body) + "\r\n", encoding="utf-8")
    return wrapper_path


def _schedule_workflow_task(args: argparse.Namespace, project: "TailorProject", workflow_label: str) -> int:
    schedule, modifier, schedule_label = _parse_repeat_schedule(args.repeat)
    default_name = f"TaiLOR {args.cmd} {workflow_label}"
    task_name = _sanitize_task_name(args.task_name or default_name)
    start_time = (dt.datetime.now() + dt.timedelta(minutes=1)).strftime("%H:%M")
    invocation = _build_scheduled_workflow_invocation(args, project)
    wrapper_path = _write_scheduled_wrapper(task_name, invocation, project.root)
    task_command = subprocess.list2cmdline([str(wrapper_path.resolve())])
    command = [
        "schtasks",
        "/Create",
        "/SC",
        schedule,
        "/MO",
        str(modifier),
        "/TN",
        task_name,
        "/TR",
        task_command,
        "/ST",
        start_time,
        "/F",
    ]

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except FileNotFoundError as ex:
        raise RuntimeError("Windows Task Scheduler (schtasks) is not available on this system.") from ex

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f'Failed to create scheduled task "{task_name}".\n{detail}')

    print(f'Created recurring task "{task_name}" ({schedule_label}).')
    print(f"Starts at {start_time}.")
    print(f"Wrapper: {wrapper_path.resolve()}")
    print(task_command)
    return 0


def _request_json(url: str, method: str = "GET", headers: dict[str, str] | None = None,
                  body: dict[str, Any] | None = None, timeout: int = 60) -> dict[str, Any]:
    data = None
    h = {"Accept": "application/json"}
    if headers:
        h.update(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        h["Content-Type"] = "application/json"
    payload, _ = _request_text(url, method=method, headers=h, data=data, timeout=timeout)
    return json.loads(payload) if payload.strip() else {}


GMAIL_SCOPE_ALIASES: dict[str, str] = {
    "send": "https://www.googleapis.com/auth/gmail.send",
    "readonly": "https://www.googleapis.com/auth/gmail.readonly",
    "modify": "https://www.googleapis.com/auth/gmail.modify",
    "compose": "https://www.googleapis.com/auth/gmail.compose",
    "full": "https://mail.google.com/",
}

DEFAULT_GMAIL_OAUTH_SCOPES: list[str] = [
    GMAIL_SCOPE_ALIASES["send"],
    GMAIL_SCOPE_ALIASES["readonly"],
    GMAIL_SCOPE_ALIASES["modify"],
    GMAIL_SCOPE_ALIASES["compose"],
]


def _expand_gmail_scopes(raw_scopes: str | None) -> list[str]:
    if not raw_scopes or not raw_scopes.strip():
        return list(DEFAULT_GMAIL_OAUTH_SCOPES)

    scopes: list[str] = []
    seen: set[str] = set()
    for token in re.split(r"[\s,]+", raw_scopes.strip()):
        if not token:
            continue
        scope = GMAIL_SCOPE_ALIASES.get(token.lower(), token)
        if scope not in seen:
            seen.add(scope)
            scopes.append(scope)
    if not scopes:
        raise RuntimeError("No Gmail scopes were provided.")
    return scopes


def _is_gmail_scope_error(message: str) -> bool:
    lowered = message.lower()
    return "access_token_scope_insufficient" in lowered or "insufficient permission" in lowered


def _gmail_oauth_command_hint(address: str) -> str:
    return (
        "Run this to refresh the Gmail token with the required scopes:\n"
        f"  python scripts/tailor_ops.py channel-gmail-oauth --address {address!s}"
    )


def _refresh_google_access_token(client_id: str, client_secret: str, refresh_token: str) -> dict[str, Any]:
    payload = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }).encode("utf-8")
    try:
        payload_text, _ = _request_text(
            GmailClient.TOKEN_URL,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=payload,
            timeout=60,
        )
        return json.loads(payload_text)
    except RuntimeError as ex:
        detail = str(ex).splitlines()[-1]
        raise RuntimeError(f"Failed to refresh Gmail token: {detail}") from ex


def _fetch_google_token_scopes(access_token: str) -> list[str]:
    info = _request_json(
        "https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=" + urllib.parse.quote(access_token, safe=""),
        timeout=60,
    )
    raw_scope = str(info.get("scope", "")).strip()
    if not raw_scope:
        return []
    return [scope for scope in raw_scope.split() if scope]


def _gmail_scopes_satisfied(granted_scopes: Iterable[str], required_scopes: list[str]) -> bool:
    granted = {scope.strip() for scope in granted_scopes if str(scope).strip()}
    if GMAIL_SCOPE_ALIASES["full"] in granted:
        return True
    return all(scope in granted for scope in required_scopes)


def _context_value_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, ensure_ascii=False)
    return str(value)


class _TemplateMissingValueError(RuntimeError):
    def __init__(self, name: str):
        super().__init__(name)
        self.name = name


TemplateFunction = Callable[..., Any]
# Register additional template functions here, for example ReadFromUrl(...)
_TEMPLATE_FUNCTIONS: dict[str, TemplateFunction] = {}


def _register_template_function(*names: str) -> Callable[[TemplateFunction], TemplateFunction]:
    def decorator(func: TemplateFunction) -> TemplateFunction:
        for name in names:
            _TEMPLATE_FUNCTIONS[name.lower()] = func
        return func
    return decorator


def _evaluate_template_node(node: ast.AST, context: dict[str, Any], *, base_dir: Path, label: str) -> Any:
    if isinstance(node, ast.Name):
        if node.id not in context:
            raise _TemplateMissingValueError(node.id)
        return context[node.id]
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise RuntimeError(f"Unsupported function expression in {label}.")
        handler = _TEMPLATE_FUNCTIONS.get(node.func.id.lower())
        if handler is None:
            raise RuntimeError(f"Unknown template function in {label}: {node.func.id}")
        args = [_evaluate_template_node(arg, context, base_dir=base_dir, label=label) for arg in node.args]
        kwargs = {
            kw.arg: _evaluate_template_node(kw.value, context, base_dir=base_dir, label=label)
            for kw in node.keywords
            if kw.arg
        }
        return handler(args, kwargs, context=context, base_dir=base_dir, label=label)
    raise RuntimeError(f"Unsupported template expression in {label}: {ast.dump(node)}")


def _evaluate_template_expression(expression: str, context: dict[str, Any], *, base_dir: Path, label: str) -> Any:
    try:
        tree = ast.parse(expression.strip(), mode="eval")
    except SyntaxError as ex:
        raise RuntimeError(f"Invalid template expression in {label}: {expression}") from ex
    return _evaluate_template_node(tree.body, context, base_dir=base_dir, label=label)


@_register_template_function("GetFileContents", "ReadFromFile", "ReadFileContents")
def _template_get_file_contents(args: list[Any], kwargs: dict[str, Any], *,
                                context: dict[str, Any], base_dir: Path, label: str) -> str:
    if len(args) > 1:
        raise RuntimeError(f"{label}: GetFileContents expects exactly one path argument.")
    raw_path = args[0] if args else kwargs.get("path")
    if raw_path is None:
        raise RuntimeError(f"{label}: GetFileContents requires a file path.")
    path = Path(_context_value_to_text(raw_path))
    if not path.is_absolute():
        path = base_dir / path
    ensure_file(path, f"{label} file")
    content = path.read_text(encoding="utf-8", errors="replace")
    return render_template(content, context, strict=True, label=f"{label} file contents", base_dir=base_dir)


@_register_template_function("SafeFileComponent", "ToSafeFileName")
def _template_safe_file_component(args: list[Any], kwargs: dict[str, Any], *,
                                  context: dict[str, Any], base_dir: Path, label: str) -> str:
    raw_value = args[0] if args else kwargs.get("value", kwargs.get("text", ""))
    text = _context_value_to_text(raw_value).strip()
    if not text:
        return ""
    text = re.sub(r'[<>:"/\\\\|?*]+', "-", text)
    text = re.sub(r"\s+", "_", text)
    return text.strip(" ._-")


def render_template(template: str, context: dict[str, Any], *,
                    strict: bool = False, label: str = "template", base_dir: Path | None = None) -> str:
    missing: list[str] = []
    base_dir = base_dir or Path.cwd()

    def repl(match: re.Match[str]) -> str:
        expression = match.group(1).strip()
        if expression in context:
            return _context_value_to_text(context[expression])
        try:
            return _context_value_to_text(
                _evaluate_template_expression(expression, context, base_dir=base_dir, label=label)
            )
        except _TemplateMissingValueError as ex:
            missing.append(ex.name)
            return match.group(0) if strict else ""

    result = re.sub(r"\{\{\s*([^}]+)\s*\}\}", repl, template)
    if strict and missing:
        missing_unique = ", ".join(sorted(set(missing)))
        raise RuntimeError(f"Unresolved variables in {label}: {missing_unique}")
    return result


def _resolve_dynamic_value(raw: Any, context: dict[str, Any], *, base_dir: Path, label: str) -> Any:
    if isinstance(raw, str):
        return render_template(raw, context, strict=True, label=label, base_dir=base_dir)
    if isinstance(raw, list):
        return [_resolve_dynamic_value(item, context, base_dir=base_dir, label=label) for item in raw]
    if isinstance(raw, dict):
        if "value_file" in raw or "file" in raw or "path" in raw:
            path_raw = raw.get("value_file", raw.get("file", raw.get("path")))
            path = _resolve_path(path_raw, context, base_dir=base_dir, required=True, label=f"{label}.file")
            return _load_file_text(path, context=context, label=label, base_dir=base_dir)
        return {
            str(key): _resolve_dynamic_value(value, context, base_dir=base_dir, label=f"{label}.{key}")
            for key, value in raw.items()
        }
    return raw


def _normalize_workflow_variables(raw: Any) -> list[tuple[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, dict):
        return [(str(key), value) for key, value in raw.items()]
    if not isinstance(raw, list):
        raise RuntimeError("Workflow 'variables' must be a list or object.")

    items: list[tuple[str, Any]] = []
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise RuntimeError(f"Workflow variable #{index} must be an object.")
        if "name" in item or "key" in item:
            name = item.get("name", item.get("key"))
            if not name:
                raise RuntimeError(f"Workflow variable #{index} is missing a name.")
            if "value" in item:
                value: Any = item["value"]
            elif "value_file" in item:
                value = {"value_file": item["value_file"]}
            elif "file" in item:
                value = {"file": item["file"]}
            elif "path" in item:
                value = {"path": item["path"]}
            else:
                raise RuntimeError(f"Workflow variable '{name}' is missing a value.")
            items.append((str(name), value))
            continue
        if len(item) != 1:
            raise RuntimeError(
                f"Workflow variable #{index} must either contain 'name'/'value' or a single key/value pair."
            )
        name, value = next(iter(item.items()))
        items.append((str(name), value))
    return items


def _coerce_bool(value: Any, *, label: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = _context_value_to_text(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    raise RuntimeError(f"Invalid boolean value for {label}: {value}")


def _resolve_number(raw: Any, context: dict[str, Any], *, base_dir: Path, label: str,
                    default: int | float, as_int: bool = False) -> int | float:
    if raw is None:
        return int(default) if as_int else float(default)
    value = _resolve_dynamic_value(raw, context, base_dir=base_dir, label=label)
    return int(value) if as_int else float(value)


def _step_verbosity(step: dict[str, Any], context: dict[str, Any], *, base_dir: Path, default: int) -> int:
    return int(_resolve_number(step.get("verbosity"), context, base_dir=base_dir, label="verbosity", default=default, as_int=True))


def _log(verbose: int, minimum: int, message: str) -> None:
    if verbose >= minimum:
        print(message, flush=True)


def _sleep_if_needed(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def _step_delay(step: dict[str, Any], field: str, context: dict[str, Any], *, base_dir: Path, default: float = 0.0) -> float:
    raw = step.get(field, default)
    return float(_resolve_number(raw, context, base_dir=base_dir, label=field, default=default))


def _resolve_string_value(raw: Any, context: dict[str, Any], *,
                          base_dir: Path, required: bool = False, label: str) -> str:
    if isinstance(raw, dict):
        raw = raw.get("value", raw.get("literal", raw.get("text", "")))
    if raw is None:
        if required:
            raise RuntimeError(f"Missing required value for {label}.")
        return ""
    text = render_template(str(raw), context, strict=True, label=label, base_dir=base_dir)
    if required and not text.strip():
        raise RuntimeError(f"Missing required value for {label}.")
    return text


def _resolve_path(raw: Any, context: dict[str, Any], *, base_dir: Path, required: bool = False, label: str) -> Path:
    path_text = _resolve_string_value(raw, context, base_dir=base_dir, required=required, label=label)
    path = Path(path_text)
    if not path.is_absolute():
        path = base_dir / path
    return path


def _resolve_text_source(step: dict[str, Any], field: str, context: dict[str, Any], *,
                         base_dir: Path, required: bool = False, default: str = "") -> str:
    raw = step.get(field, default)
    raw_file = step.get(f"{field}_file")
    if isinstance(raw, dict):
        raw_file = raw.get("file", raw.get("path", raw_file))
        raw = raw.get("text", raw.get("literal", raw.get("value", default)))

    if raw_file:
        path = _resolve_path(raw_file, context, base_dir=base_dir, required=True, label=f"{field}_file")
        ensure_file(path, f"{field}_file")
        trace(2, f"[file] reading {field}_file -> {path}")
        content = path.read_text(encoding="utf-8", errors="replace")
        trace(3, f"[file] read {len(content)} chars from {path}")
        return render_template(content, context, strict=True, label=f"{field}_file contents", base_dir=base_dir)

    if isinstance(raw, dict):
        resolved = _resolve_dynamic_value(raw, context, base_dir=base_dir, label=field)
        text = _context_value_to_text(resolved)
        if required and not text.strip():
            raise RuntimeError(f"Missing required value for {field}.")
        return text

    text = render_template(str(raw), context, strict=True, label=field, base_dir=base_dir)
    if required and not text.strip():
        raise RuntimeError(f"Missing required value for {field}.")
    return text


def _write_value(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    trace(2, f"[file] writing {path}")
    if isinstance(value, (dict, list)):
        path.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")
    else:
        path.write_text(_context_value_to_text(value), encoding="utf-8")
    trace(3, f"[file] wrote {path}")


def _resolve_new_output_path(path: Path) -> Path:
    if not path.exists():
        return path
    counter = 1
    while True:
        candidate = path.with_name(f"{path.stem}{counter}{path.suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _write_output_context_json(path_text: str, context: dict[str, Any]) -> Path:
    requested_path = Path(path_text)
    output_path = _resolve_new_output_path(requested_path)
    output_path.write_text(json.dumps(context, indent=2), encoding="utf-8")
    return output_path


def _store_output(context: dict[str, Any], step: dict[str, Any], value: Any, *,
                  default_key: str, base_dir: Path, verbose: int) -> str:
    out_key = str(step.get("output") or default_key)
    context[out_key] = value
    output_file = step.get("output_file")
    if output_file:
        requested_output_path = _resolve_path(output_file, context, base_dir=base_dir, required=True, label="output_file")
        if _coerce_bool(step.get("output_file_if_missing", False), label="output_file_if_missing"):
            if requested_output_path.exists():
                _log(verbose, 1, f"  -> kept existing {requested_output_path}")
            else:
                _write_value(requested_output_path, value)
                _log(verbose, 1, f"  -> wrote {requested_output_path}")
        else:
            output_path = _resolve_new_output_path(requested_output_path)
            _write_value(output_path, value)
            if output_path != requested_output_path:
                _log(verbose, 1, f"  -> wrote {output_path} (requested {requested_output_path} already existed)")
            else:
                _log(verbose, 1, f"  -> wrote {output_path}")
    return out_key


def _compile_regex(pattern: str | None, *, label: str) -> re.Pattern[str] | None:
    if not pattern:
        return None
    try:
        return re.compile(pattern)
    except re.error as ex:
        raise RuntimeError(f"Invalid regex for {label}: {pattern} ({ex})") from ex


def _iter_matching_text_files(folder: Path, *, recursive: bool,
                              include_regex: re.Pattern[str] | None,
                              exclude_regex: re.Pattern[str] | None) -> Iterable[Path]:
    ensure_file(folder, "Folder")
    iterator = folder.rglob("*") if recursive else folder.glob("*")
    for path in sorted(iterator):
        if not path.is_file():
            continue
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".db", ".exe", ".dll"}:
            continue
        candidate = str(path.relative_to(folder))
        if include_regex and not include_regex.search(candidate):
            continue
        if exclude_regex and exclude_regex.search(candidate):
            continue
        yield path


def _load_file_text(path: Path, *, context: dict[str, Any], label: str, base_dir: Path) -> str:
    ensure_file(path, label)
    trace(2, f"[file] loading {label}: {path}")
    content = path.read_text(encoding="utf-8", errors="replace")
    trace(3, f"[file] loaded {len(content)} chars from {path}")
    return render_template(content, context, strict=True, label=f"{label} contents", base_dir=base_dir)


def _compose_prompt_segments(segments: Iterable[str]) -> str:
    return "\n\n".join(segment.strip() for segment in segments if segment and segment.strip())


def _build_ai_user_text(step: dict[str, Any], context: dict[str, Any], *, base_dir: Path, input_field: str = "input") -> str:
    joiner = _resolve_string_value(step.get("join_with", "\n\n"), context, base_dir=base_dir, label="join_with")
    parts: list[str] = []

    direct_input = _resolve_text_source(step, input_field, context, base_dir=base_dir, required=False, default="")
    if direct_input.strip():
        parts.append(direct_input)

    for key in step.get("input_keys", []) or []:
        if key not in context:
            raise RuntimeError(f"Missing input key in context: {key}")
        parts.append(_context_value_to_text(context[key]))

    prefix = _resolve_text_source(step, "input_prefix", context, base_dir=base_dir, required=False, default="")
    suffix = _resolve_text_source(step, "input_suffix", context, base_dir=base_dir, required=False, default="")
    if prefix.strip():
        parts.insert(0, prefix)
    if suffix.strip():
        parts.append(suffix)

    combined = joiner.join(part for part in parts if part and part.strip())
    if not combined.strip():
        raise RuntimeError("Workflow step requires non-empty input after template expansion.")
    return combined


def _ensure_gmail_client(project: TailorProject, gmail_cache: dict[str, "GmailClient"], channel_id: str) -> "GmailClient":
    if channel_id not in gmail_cache:
        trace(2, f"[gmail] loading channel credentials for channel_id={channel_id}")
        with project.connect() as conn:
            _, creds = _load_channel_creds(conn, channel_id)
        gmail_cache[channel_id] = GmailClient(creds)
    return gmail_cache[channel_id]


def _ensure_gmail_client_by_address(project: TailorProject, gmail_cache: dict[str, "GmailClient"], address: str) -> "GmailClient":
    cache_key = f"address:{address.lower()}"
    if cache_key not in gmail_cache:
        trace(2, f"[gmail] loading channel credentials for address={address}")
        with project.connect() as conn:
            _, creds = _load_channel_creds_by_address(conn, address)
        gmail_cache[cache_key] = GmailClient(creds)
    return gmail_cache[cache_key]


def _read_list_items(path: Path) -> list[str]:
    ensure_file(path, "List file")
    trace(2, f"[file] reading list items from {path}")
    items = [line.strip() for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]
    trace(3, f"[file] loaded {len(items)} list item(s) from {path}")
    return items


def _read_gmail_inbox_items(project: TailorProject, gmail_cache: dict[str, "GmailClient"], *,
                            from_address: str, query: str, max_results: int,
                            process_order: str = "asc") -> list[dict[str, Any]]:
    trace(2, f"[gmail] loading inbox for {from_address} query={query!r} max_results={max_results} order={process_order}")
    gmail = _ensure_gmail_client_by_address(project, gmail_cache, from_address)
    items = gmail.list_inbox(max_results=max_results, query=query)
    trace(2, f"[gmail] inbox returned {len(items)} message stub(s)")
    if process_order.strip().lower() == "asc":
        items = list(reversed(items))

    results: list[dict[str, Any]] = []
    for item in items:
        message_id = item.get("id")
        if not message_id:
            continue
        trace(3, f"[gmail] fetching full message {message_id}")
        summary = gmail.get_message_summary(str(message_id))
        results.append(
            {
                "id": str(message_id),
                "from": summary.get("from", ""),
                "sender_email": _extract_sender_email(summary.get("from", "")),
                "subject": summary.get("subject", ""),
                "date": summary.get("date", ""),
                "thread_id": summary.get("thread_id", ""),
                "message_header_id": summary.get("message_header_id", ""),
                "in_reply_to": summary.get("in_reply_to", ""),
                "references": summary.get("references", ""),
                "body": summary.get("body", ""),
            }
        )
        trace(3, f"[gmail] loaded message {message_id} subject={summary.get('subject', '')!r}")
    return results


def _normalize_lookup_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _tokenize_lookup_text(value: str) -> list[str]:
    return [token for token in _normalize_lookup_text(value).split() if token]


def _load_text_if_exists(path: Path | None) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _gather_text_files(folder: Path, *, recursive: bool,
                       include_regex: re.Pattern[str] | None = None,
                       exclude_regex: re.Pattern[str] | None = None,
                       sort_by: str = "name",
                       descending: bool = False,
                       max_files: int = 0) -> list[Path]:
    if not folder.exists() or not folder.is_dir():
        return []
    files = list(_iter_matching_text_files(folder, recursive=recursive, include_regex=include_regex, exclude_regex=exclude_regex))
    entries = [
        {
            "path": str(file_path),
            "relative_path": str(file_path.relative_to(folder)),
            "name": file_path.name,
            "size": file_path.stat().st_size,
            "modified_utc": dt.datetime.fromtimestamp(file_path.stat().st_mtime, tz=UTC).isoformat(),
        }
        for file_path in files
    ]
    entries = _sort_file_entries(entries, sort_by=sort_by, descending=descending)
    ordered = [Path(item["path"]) for item in entries]
    if max_files > 0:
        ordered = ordered[:max_files]
    return ordered


def _combine_text_files(paths: list[Path], *, relative_to: Path | None = None,
                        context: dict[str, Any], base_dir: Path, header_paths: bool = True) -> str:
    chunks: list[str] = []
    for file_path in paths:
        ensure_file(file_path, f"Combined file {file_path}")
        trace(2, f"[file] combining raw text file {file_path}")
        text_value = file_path.read_text(encoding="utf-8", errors="replace")
        if header_paths:
            label = str(file_path.relative_to(relative_to)) if relative_to is not None else file_path.name
            chunks.append(f"===== {label} =====\n{text_value}")
        else:
            chunks.append(text_value)
    return "\n\n".join(chunk for chunk in chunks if chunk.strip())


def _build_agent_system_prompt_text(project: "TailorProject", step: dict[str, Any], context: dict[str, Any], *,
                                    base_dir: Path) -> tuple[str, str, str]:
    agent_root = _resolve_path(step.get("agent_root", context.get("agent_root")), context, base_dir=base_dir, required=True, label="agent_root")
    template_default = project.root / "prompts" / "system_prompt.txt"
    template_raw = step.get("template_file")
    template_path = _resolve_path(template_raw, context, base_dir=base_dir, required=True, label="template_file") if template_raw else template_default
    ensure_file(template_path, "system prompt template")

    persona_folder_raw = step.get("persona_folder")
    persona_folder = _resolve_path(persona_folder_raw, context, base_dir=base_dir, required=False, label="persona_folder") if persona_folder_raw else (agent_root / "persona")
    persona_recursive = _coerce_bool(step.get("persona_recursive", False), label="persona_recursive")
    persona_include_pattern = _resolve_string_value(step.get("persona_regex") or step.get("persona_include_regex") or r".+\.(txt|md|prompt)$", context, base_dir=base_dir, label="persona_regex")
    persona_exclude_pattern = _resolve_string_value(step.get("persona_exclude_regex"), context, base_dir=base_dir, label="persona_exclude_regex") if step.get("persona_exclude_regex") else None
    persona_include_regex = _compile_regex(persona_include_pattern, label="persona_regex")
    persona_exclude_regex = _compile_regex(persona_exclude_pattern, label="persona_exclude_regex")
    persona_sort_by = _resolve_string_value(step.get("persona_sort_by", "name"), context, base_dir=base_dir, label="persona_sort_by")
    persona_sort_direction = _resolve_string_value(step.get("persona_sort_direction", "asc"), context, base_dir=base_dir, label="persona_sort_direction").lower()
    persona_files = _gather_text_files(
        persona_folder,
        recursive=persona_recursive,
        include_regex=persona_include_regex,
        exclude_regex=persona_exclude_regex,
        sort_by=persona_sort_by,
        descending=persona_sort_direction == "desc",
        max_files=int(_resolve_number(step.get("persona_max_files"), context, base_dir=base_dir, label="persona_max_files", default=0, as_int=True) or 0),
    )
    agent_input_text = _combine_text_files(persona_files, relative_to=persona_folder, context=context, base_dir=base_dir, header_paths=True) if persona_files else ""

    client_history_folder_raw = step.get("client_history_folder", context.get("client_history_folder"))
    client_history_folder = _resolve_path(client_history_folder_raw, context, base_dir=base_dir, required=False, label="client_history_folder") if client_history_folder_raw else None
    history_recursive = _coerce_bool(step.get("history_recursive", True), label="history_recursive")
    history_include_pattern = _resolve_string_value(step.get("history_regex") or step.get("history_include_regex") or r".+\.(txt|md|prompt)$", context, base_dir=base_dir, label="history_regex")
    history_exclude_pattern = _resolve_string_value(step.get("history_exclude_regex"), context, base_dir=base_dir, label="history_exclude_regex") if step.get("history_exclude_regex") else None
    history_include_regex = _compile_regex(history_include_pattern, label="history_regex")
    history_exclude_regex = _compile_regex(history_exclude_pattern, label="history_exclude_regex")
    history_sort_by = _resolve_string_value(step.get("history_sort_by", "modified_utc"), context, base_dir=base_dir, label="history_sort_by")
    history_sort_direction = _resolve_string_value(step.get("history_sort_direction", "asc"), context, base_dir=base_dir, label="history_sort_direction").lower()
    history_files = _gather_text_files(
        client_history_folder,
        recursive=history_recursive,
        include_regex=history_include_regex,
        exclude_regex=history_exclude_regex,
        sort_by=history_sort_by,
        descending=history_sort_direction == "desc",
        max_files=int(_resolve_number(step.get("history_max_files"), context, base_dir=base_dir, label="history_max_files", default=0, as_int=True) or 0),
    ) if client_history_folder else []
    conversation_history_text = _combine_text_files(history_files, relative_to=client_history_folder, context=context, base_dir=base_dir, header_paths=True) if history_files and client_history_folder else ""

    trace(2, f"[file] loading raw system prompt template: {template_path}")
    template_text = template_path.read_text(encoding="utf-8", errors="replace")
    rendered = _replace_named_variables(
        template_text,
        {
            "agent_input": agent_input_text,
            "conversation_history": conversation_history_text,
            "conversation-history": conversation_history_text,
        },
    )
    rendered = render_template(rendered, context, strict=True, label="agent system prompt", base_dir=base_dir)
    return rendered, agent_input_text, conversation_history_text


def _discover_agent_prompt_file(agent_root: Path) -> Path | None:
    candidates: list[Path] = []
    search_roots = [agent_root / "prompts", agent_root]
    seen: set[Path] = set()
    for search_root in search_roots:
        if not search_root.exists() or not search_root.is_dir():
            continue
        for path in sorted(search_root.glob("*")):
            if not path.is_file() or path in seen:
                continue
            seen.add(path)
            name = path.name.lower()
            if path.suffix.lower() not in {".prompt", ".txt", ".md"}:
                continue
            if any(token in name for token in ("workflow", ".output.", "reply_email_rules", "emails.txt", "template")):
                continue
            if "prompt" in name:
                candidates.append(path)
    if not candidates:
        for search_root in search_roots:
            if not search_root.exists() or not search_root.is_dir():
                continue
            for path in sorted(search_root.glob("*")):
                if path.is_file() and path.suffix.lower() == ".prompt" and path not in seen:
                    candidates.append(path)
    if not candidates:
        return None
    candidates.sort(key=lambda item: (0 if "agent-prompt" in item.name.lower() else 1, -item.stat().st_size, item.name.lower()))
    return candidates[0]


def _build_agent_folder_context(project: "TailorProject", agent_root: Path) -> dict[str, Any]:
    folder_name = agent_root.name
    prompt_file = _discover_agent_prompt_file(agent_root)
    prompt_text = _load_text_if_exists(prompt_file)
    folder_tokens = set(_tokenize_lookup_text(folder_name))
    if prompt_file:
        folder_tokens.update(_tokenize_lookup_text(prompt_file.stem))

    best_row: sqlite3.Row | None = None
    best_score = -1
    with project.connect() as conn:
        rows = conn.execute(
            """
            SELECT
                a.Id AS AgentId,
                a.Name AS AgentName,
                a.GoalsAndMotivations,
                a.StrategiesAndTactics,
                p.Id AS PersonaId,
                p.Name AS PersonaName,
                p.Description AS PersonaDescription,
                p.ConfigJson AS PersonaConfigJson,
                d.Content AS PersonaDocumentContent
            FROM Agent a
            JOIN Persona p ON p.Id = a.PersonaId
            LEFT JOIN Document d ON d.Id = p.PersonaDescriptionDocumentId
            ORDER BY a.Name ASC
            """
        ).fetchall()

    for row in rows:
        score = 0
        haystacks = [
            _normalize_lookup_text(str(row["AgentName"] or "")),
            _normalize_lookup_text(str(row["PersonaName"] or "")),
        ]
        persona_config = {}
        try:
            persona_config = json.loads(row["PersonaConfigJson"] or "{}")
        except Exception:
            persona_config = {}
        source_file = _normalize_lookup_text(Path(str(persona_config.get("sourceFile", ""))).stem)
        if source_file:
            haystacks.append(source_file)
        combined = " ".join(part for part in haystacks if part)
        for token in folder_tokens:
            if token and token in combined:
                score += 3
        if folder_name.replace("-", " ") in combined:
            score += 4
        if prompt_file and _normalize_lookup_text(prompt_file.stem) in combined:
            score += 2
        if score > best_score:
            best_score = score
            best_row = row

    persona_source_file = ""
    persona_source_text = ""
    persona_document_text = ""
    agent_id = ""
    agent_name = folder_name
    persona_id = ""
    persona_name = folder_name
    agent_goals = ""
    agent_strategies = ""
    persona_description = ""

    if best_row is not None and best_score > 0:
        agent_id = str(best_row["AgentId"] or "")
        agent_name = str(best_row["AgentName"] or folder_name)
        persona_id = str(best_row["PersonaId"] or "")
        persona_name = str(best_row["PersonaName"] or folder_name)
        agent_goals = str(best_row["GoalsAndMotivations"] or "")
        agent_strategies = str(best_row["StrategiesAndTactics"] or "")
        persona_description = str(best_row["PersonaDescription"] or "")
        persona_document_text = str(best_row["PersonaDocumentContent"] or "")
        try:
            persona_config = json.loads(best_row["PersonaConfigJson"] or "{}")
        except Exception:
            persona_config = {}
        source_value = str(persona_config.get("sourceFile") or "")
        if source_value:
            persona_source_file = str(Path(source_value))
            persona_source_text = _load_text_if_exists(Path(source_value))

    context_parts = [
        f"Agent Folder: {folder_name}",
        f"Agent Name: {agent_name}",
        f"Persona Name: {persona_name}",
    ]
    if agent_goals.strip():
        context_parts.append(f"Goals and Motivations:\n{agent_goals}")
    if agent_strategies.strip():
        context_parts.append(f"Strategies and Tactics:\n{agent_strategies}")
    if persona_document_text.strip():
        context_parts.append(f"Persona Document:\n{persona_document_text}")
    elif persona_source_text.strip():
        context_parts.append(f"Persona Source File:\n{persona_source_text}")
    elif persona_description.strip():
        context_parts.append(f"Persona Description:\n{persona_description}")
    if prompt_text.strip():
        context_parts.append(f"Agent Prompt:\n{prompt_text}")

    return {
        "agent_root": str(agent_root),
        "agent_folder_name": folder_name,
        "agent_id": agent_id,
        "agent_name": agent_name,
        "agent_slug": folder_name,
        "persona_id": persona_id,
        "persona_name": persona_name,
        "persona_description": persona_description,
        "persona_document_text": persona_document_text,
        "persona_source_file": persona_source_file,
        "persona_source_text": persona_source_text,
        "agent_prompt_file": str(prompt_file) if prompt_file else "",
        "agent_prompt_text": prompt_text,
        "agent_goals": agent_goals,
        "agent_strategies": agent_strategies,
        "agent_context_text": "\n\n".join(part for part in context_parts if part.strip()),
    }


def _read_agent_history_items(project: "TailorProject", *, history_root_name: str = "history",
                              agent_folder_regex: str | None = None, client_folder_regex: str | None = None,
                              max_history_folders: int = 0) -> list[dict[str, Any]]:
    agents_root = project.root / "agents"
    if not agents_root.exists():
        raise FileNotFoundError(f"Agents folder not found: {agents_root}")

    agent_regex = _compile_regex(agent_folder_regex, label="agent_folder_regex")
    client_regex = _compile_regex(client_folder_regex, label="client_folder_regex")
    results: list[dict[str, Any]] = []

    for agent_root in sorted(path for path in agents_root.iterdir() if path.is_dir()):
        if agent_regex and not agent_regex.search(agent_root.name):
            continue
        history_root = agent_root / history_root_name
        if not history_root.exists() or not history_root.is_dir():
            continue
        for client_folder in sorted(path for path in history_root.iterdir() if path.is_dir()):
            if client_regex and not client_regex.search(client_folder.name):
                continue
            if not any(child.is_file() for child in client_folder.iterdir()):
                continue
            results.append(
                {
                    "agent_root": str(agent_root),
                    "agent_folder_name": agent_root.name,
                    "agent_history_root": str(history_root),
                    "client_history_folder": str(client_folder),
                    "client_identifier": client_folder.name,
                    "client_email": client_folder.name if "@" in client_folder.name else "",
                }
            )
            if max_history_folders > 0 and len(results) >= max_history_folders:
                return results
    return results


def _sort_file_entries(entries: list[dict[str, Any]], *, sort_by: str, descending: bool) -> list[dict[str, Any]]:
    mode = (sort_by or "name").strip().lower()

    def key(item: dict[str, Any]) -> Any:
        if mode in {"mtime", "modified", "modified_utc"}:
            return (item.get("modified_utc") or "", item.get("name") or "")
        if mode in {"path", "relative_path"}:
            return item.get("relative_path") or item.get("path") or ""
        if mode in {"size", "length"}:
            return int(item.get("size") or 0)
        return item.get("name") or item.get("relative_path") or item.get("path") or ""

    return sorted(entries, key=key, reverse=descending)


def _read_json_file_if_exists(path: Path) -> Any:
    if not path.exists() or not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8", errors="replace"))


def _normalize_text_key(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _normalize_industry_key(value: str) -> str:
    return _normalize_text_key(value)


def _normalize_pair_key(industry: str, city_and_state: str) -> str:
    return f"{_normalize_industry_key(industry)}|{_normalize_text_key(city_and_state)}"


def _parse_timestamp_value(value: str) -> dt.datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _select_business_research_target_from_spec(selection_spec: dict[str, Any], context: dict[str, Any], *,
                                               base_dir: Path) -> dict[str, Any]:
    history_path = _resolve_path(selection_spec.get("history_file"), context, base_dir=base_dir, required=True, label="history_file")
    industries = [
        _resolve_string_value(item, context, base_dir=base_dir, required=True, label="industries")
        for item in (selection_spec.get("industries") or [])
    ]
    locations = [
        _resolve_string_value(item, context, base_dir=base_dir, required=True, label="locations")
        for item in (selection_spec.get("locations") or [])
    ]
    if not industries:
        raise RuntimeError("select_business_research_target requires a non-empty industries list.")
    if not locations:
        raise RuntimeError("select_business_research_target requires a non-empty locations list.")

    lookback_days = int(_resolve_number(selection_spec.get("recent_days"), context, base_dir=base_dir, label="recent_days", default=7, as_int=True))
    history_raw = _read_json_file_if_exists(history_path)
    history_items = history_raw if isinstance(history_raw, list) else []
    now_utc = dt.datetime.now(tz=UTC)
    cutoff = now_utc - dt.timedelta(days=lookback_days)

    industry_last_used: dict[str, dt.datetime | None] = {industry: None for industry in industries}
    industry_recent: set[str] = set()
    pair_last_used: dict[str, dt.datetime | None] = {}

    for item in history_items:
        if not isinstance(item, dict):
            continue
        recorded_industry = str(item.get("industry") or "").strip()
        recorded_city = str(item.get("city_and_state") or "").strip()
        recorded_time = _parse_timestamp_value(str(item.get("selected_utc") or ""))
        if not recorded_industry or not recorded_city:
            continue
        industry_key = next((candidate for candidate in industries if _normalize_industry_key(candidate) == _normalize_industry_key(recorded_industry)), recorded_industry)
        previous_industry_time = industry_last_used.get(industry_key)
        if previous_industry_time is None or (recorded_time and recorded_time > previous_industry_time):
            industry_last_used[industry_key] = recorded_time
        if recorded_time and recorded_time >= cutoff:
            industry_recent.add(_normalize_industry_key(industry_key))
        pair_key = _normalize_pair_key(industry_key, recorded_city)
        previous_pair_time = pair_last_used.get(pair_key)
        if previous_pair_time is None or (recorded_time and recorded_time > previous_pair_time):
            pair_last_used[pair_key] = recorded_time

    eligible_industries = [industry for industry in industries if _normalize_industry_key(industry) not in industry_recent]
    candidate_industries = eligible_industries or industries
    candidate_industries = sorted(
        candidate_industries,
        key=lambda industry: industry_last_used.get(industry) or dt.datetime.min.replace(tzinfo=UTC)
    )

    unused_pairs: list[tuple[str, str]] = []
    for industry in candidate_industries:
        for location in locations:
            if _normalize_pair_key(industry, location) not in pair_last_used:
                unused_pairs.append((industry, location))

    if unused_pairs:
        def unused_pair_sort_key(item: tuple[str, str]) -> Any:
            industry, location = item
            return (
                industry_last_used.get(industry) or dt.datetime.min.replace(tzinfo=UTC),
                locations.index(location),
            )
        selected_industry, selected_city = sorted(unused_pairs, key=unused_pair_sort_key)[0]
    else:
        reusable_pairs = [(industry, location) for industry in candidate_industries for location in locations]
        selected_industry, selected_city = sorted(
            reusable_pairs,
            key=lambda item: pair_last_used.get(_normalize_pair_key(item[0], item[1])) or dt.datetime.min.replace(tzinfo=UTC)
        )[0]

    selection_record = {
        "industry": selected_industry,
        "city_and_state": selected_city,
        "selected_utc": now_utc.isoformat(),
    }
    history_items.append(selection_record)
    _write_value(history_path, history_items)

    industry_key_name = _resolve_string_value(selection_spec.get("industry_output", "industry"), context, base_dir=base_dir, label="industry_output")
    city_key_name = _resolve_string_value(selection_spec.get("city_output", "city_and_state"), context, base_dir=base_dir, label="city_output")
    context[industry_key_name] = selected_industry
    context[city_key_name] = selected_city
    if selection_spec.get("output"):
        output_key = _resolve_string_value(selection_spec.get("output"), context, base_dir=base_dir, required=True, label="output")
        context[output_key] = selection_record
    return selection_record


def _extract_emails_from_value(value: Any) -> list[str]:
    pattern = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
    found: list[str] = []
    seen: set[str] = set()

    def walk(item: Any) -> None:
        if item is None:
            return
        if isinstance(item, dict):
            for sub in item.values():
                walk(sub)
            return
        if isinstance(item, (list, tuple, set)):
            for sub in item:
                walk(sub)
            return
        for match in pattern.findall(_context_value_to_text(item)):
            lowered = match.lower()
            if lowered not in seen:
                seen.add(lowered)
                found.append(lowered)

    walk(value)
    return found


def _first_name_from_contact_value(value: Any) -> str:
    text = _context_value_to_text(value).strip()
    if not text:
        return ""
    cleaned = re.sub(r"^[^A-Za-z]+|[^A-Za-z' -]+$", "", text).strip()
    if not cleaned:
        return ""
    return cleaned.split()[0].strip()


def _extract_email_entries_from_value(value: Any, *, source_file: Path | None = None) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    email_pattern = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")

    def add_entry(email_value: str, payload: dict[str, Any] | None = None) -> None:
        email_text = _context_value_to_text(email_value).strip().lower()
        if not email_text or not email_pattern.fullmatch(email_text) or email_text in seen:
            return
        seen.add(email_text)
        payload = payload or {}
        contact_value = (
            payload.get("first_name")
            or payload.get("contact_name")
            or payload.get("name")
            or payload.get("contact")
            or ""
        )
        results.append(
            {
                "email": email_text,
                "first_name": _first_name_from_contact_value(contact_value),
                "contact_name": _context_value_to_text(contact_value).strip(),
                "business_name": _context_value_to_text(payload.get("business_name") or payload.get("company") or payload.get("business") or "").strip(),
                "website_url": _context_value_to_text(payload.get("website_url") or payload.get("website") or payload.get("url") or "").strip(),
                "source_file": str(source_file) if source_file else "",
                "payload": payload,
            }
        )

    def walk(item: Any) -> None:
        if item is None:
            return
        if isinstance(item, dict):
            candidate_email = item.get("email") or item.get("Email") or item.get("address") or item.get("Address")
            if candidate_email:
                add_entry(_context_value_to_text(candidate_email), item)
            for sub in item.values():
                walk(sub)
            return
        if isinstance(item, (list, tuple, set)):
            for sub in item:
                walk(sub)
            return
        for match in email_pattern.findall(_context_value_to_text(item)):
            add_entry(match, {})

    walk(value)
    return results


def _rename_remove_prefix(files: list[Path], prefix: str) -> list[Path]:
    if not prefix:
        return files
    renamed: list[Path] = []
    for path in files:
        name = path.name
        if not name.startswith(prefix):
            renamed.append(path)
            continue
        target_name = name[len(prefix):] or name
        target = path.with_name(target_name)
        final_target = target
        counter = 1
        while final_target.exists():
            final_target = target.with_name(f"{target.stem}_{counter}{target.suffix}")
            counter += 1
        path.rename(final_target)
        trace(2, f"[file] renamed {path} -> {final_target}")
        renamed.append(final_target)
    return renamed


def _replace_named_variables(text: str, replacements: dict[str, str]) -> str:
    result = text
    for key, value in replacements.items():
        pattern = re.compile(r"\{\{\s*" + re.escape(key) + r"\s*\}\}")
        # Callable repl: string repl treats \ as escapes (\m is invalid); persona/history
        # often contain Windows paths and other backslashes.
        result = pattern.sub(lambda _m, v=value: v, result)
    return result


def _extract_named_section(text: str, section_name: str) -> str:
    pattern = re.compile(
        rf"^\s*{re.escape(section_name)}\s*:\s*(.*?)(?=^\s*[A-Za-z][A-Za-z0-9 _-]*\s*:|\Z)",
        re.IGNORECASE | re.MULTILINE | re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        raise RuntimeError(f"Section not found in text: {section_name}")
    return match.group(1).strip()


def _format_message_rows(rows: list[dict[str, Any]]) -> str:
    blocks: list[str] = []
    for row in rows:
        blocks.append(
            "\n".join(
                [
                    f"MessageId: {row['Id']}",
                    f"ConversationId: {row['ConversationId']}",
                    f"ClientId: {row['ClientId']}",
                    f"ClientName: {row.get('ClientName') or '-'}",
                    f"AgentId: {row.get('AgentId') or '-'}",
                    f"Direction: {row['Direction']}",
                    f"ChannelType: {row['ChannelType']}",
                    f"SentUtc: {row['SentUtc']}",
                    f"ExternalMessageId: {row.get('ExternalMessageId') or '-'}",
                    "Content:",
                    row["Content"],
                ]
            )
        )
    return "\n\n".join(blocks)


def _query_messages(conn: sqlite3.Connection, *,
                    conversation_id: str | None = None,
                    client_id: str | None = None,
                    client_name: str | None = None,
                    agent_id: str | None = None,
                    direction: str | None = None,
                    channel_type: str | None = None,
                    limit: int | None = None,
                    order: str = "asc",
                    regex_filter: str | None = None) -> list[dict[str, Any]]:
    sql = """
        SELECT cm.Id, cm.ConversationId, cm.Direction, cm.ChannelType, cm.Content,
               cm.ExternalMessageId, cm.DeliveryStatus, cm.SentUtc, cm.MetadataJson,
               c.ClientId, c.AgentId, c.PersonaId, cl.Name AS ClientName
        FROM ConversationMessage cm
        JOIN Conversation c ON c.Id = cm.ConversationId
        LEFT JOIN Client cl ON cl.Id = c.ClientId
        WHERE 1 = 1
    """
    params: list[Any] = []
    if conversation_id:
        sql += " AND cm.ConversationId = ?"
        params.append(conversation_id)
    if client_id:
        sql += " AND c.ClientId = ?"
        params.append(client_id)
    if client_name:
        sql += " AND cl.Name = ?"
        params.append(client_name)
    if agent_id:
        sql += " AND c.AgentId = ?"
        params.append(agent_id)
    if direction:
        sql += " AND cm.Direction = ?"
        params.append(direction)
    if channel_type:
        sql += " AND cm.ChannelType = ?"
        params.append(channel_type)
    sql += f" ORDER BY cm.SentUtc {'DESC' if str(order).lower() == 'desc' else 'ASC'}"
    if limit and int(limit) > 0:
        sql += " LIMIT ?"
        params.append(int(limit))

    rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    regex = _compile_regex(regex_filter, label="regex_filter")
    if regex:
        rows = [
            row for row in rows
            if regex.search(row.get("Content") or "")
            or regex.search(row.get("ClientName") or "")
            or regex.search(row.get("Direction") or "")
            or regex.search(row.get("ChannelType") or "")
        ]
    return rows


# ----------------------------
# TaiLOR project access
# ----------------------------

def _is_tailor_project_root(path: Path) -> bool:
    """True if path looks like the TaiLOR repo root (has bundled CLI entry)."""
    return (path / "scripts" / "tailor_ops.py").is_file()


def discover_tailor_project_root(start: Path | None = None) -> Path:
    """
    Resolve the TaiLOR project root when the shell cwd is inside the repo (e.g. under agents/.../workflows).

    Order: TAILOR_PROJECT_ROOT env (if valid), then walk parents from start (default cwd) for scripts/tailor_ops.py.
    """
    start_path = (start or Path.cwd()).resolve()
    env_raw = (os.environ.get("TAILOR_PROJECT_ROOT") or "").strip()
    if env_raw:
        candidate = Path(env_raw).expanduser().resolve()
        if _is_tailor_project_root(candidate):
            return candidate
    for directory in (start_path, *start_path.parents):
        if _is_tailor_project_root(directory):
            return directory
    raise RuntimeError(
        "Could not find TaiLOR project root (no scripts/tailor_ops.py in this directory or any parent). "
        "Run from the repository root, or set environment variable TAILOR_PROJECT_ROOT to that root."
    )


@dataclass
class TailorProject:
    root: Path

    def __post_init__(self) -> None:
        # Always anchor config, SQLite, and paths to an absolute project root so CLI works from any cwd.
        object.__setattr__(self, "root", Path(self.root).expanduser().resolve())

    @property
    def db_path(self) -> Path:
        return self.root / "tailor-state.db"

    @property
    def appsettings_path(self) -> Path:
        preferred = self.root / APPSETTINGS_RELATIVE_PATH
        legacy = self.root / LEGACY_APPSETTINGS_RELATIVE_PATH
        if preferred.exists() or not legacy.exists():
            return preferred
        return legacy

    @property
    def appsettings_local_path(self) -> Path:
        preferred = self.root / APPSETTINGS_LOCAL_RELATIVE_PATH
        legacy = self.root / LEGACY_APPSETTINGS_LOCAL_RELATIVE_PATH
        if preferred.exists() or not legacy.exists():
            return preferred
        return legacy

    @property
    def workflows_path(self) -> Path:
        return self.root / "task-workflows.json"

    @property
    def prompts_dir(self) -> Path:
        return self.root / "prompts"

    @property
    def user_notifications_path(self) -> Path:
        return self.root / USER_NOTIFICATIONS_PATH

    @property
    def exec_logs_dir(self) -> Path:
        return self.root / EXEC_LOGS_DIR

    @property
    def engine_logs_dir(self) -> Path:
        """Central directory for cross-agent workflow/engine events (text + NDJSON)."""
        return self.root / ENGINE_LOG_DIR

    @property
    def fetlife_profile_snapshot_dir(self) -> Path:
        return self.root / "artifacts" / "fetlife-browser-profile"

    def connect(self) -> sqlite3.Connection:
        db_file = self.db_path
        ensure_file(db_file, "SQLite database")
        trace(3, f"[db] opening sqlite connection {db_file}")
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        return conn

    def load_workflows(self) -> list[dict[str, Any]]:
        ensure_file(self.workflows_path, "task-workflows.json")
        trace(2, f"[file] loading workflows from {self.workflows_path}")
        obj = json.loads(self.workflows_path.read_text(encoding="utf-8"))
        return obj.get("Tasks", [])

    def prompt_texts(self) -> dict[str, str]:
        if not self.prompts_dir.exists():
            return {}
        data: dict[str, str] = {}
        for p in sorted(self.prompts_dir.glob("*.txt")):
            trace(3, f"[file] loading prompt {p}")
            data[p.stem] = p.read_text(encoding="utf-8", errors="replace")
        return data


def _deep_merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_appsettings(project: TailorProject) -> dict[str, Any]:
    settings: dict[str, Any] = {}
    if project.appsettings_path.exists():
        settings = json.loads(project.appsettings_path.read_text(encoding="utf-8"))
    if project.appsettings_local_path.exists():
        local = json.loads(project.appsettings_local_path.read_text(encoding="utf-8"))
        settings = _deep_merge_dicts(settings, local)
    return settings


def _resolve_secret_from_settings(settings: dict[str, Any], secret_ref: str) -> str:
    if not secret_ref:
        return ""
    current: Any = settings.get("Secrets", {})
    for part in secret_ref.split(":"):
        if not isinstance(current, dict):
            return ""
        current = current.get(part)
    return str(current or "").strip() if current is not None else ""


def _slugify_for_filename(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9._-]+", "-", (value or "").strip())
    text = re.sub(r"-{2,}", "-", text).strip("-._")
    return text or "item"


def _redact_sensitive_value(value: Any, key_name: str = "") -> Any:
    key = (key_name or "").lower()
    if any(token in key for token in ("api_key", "token", "secret", "password", "authorization")):
        text = str(value or "")
        if not text:
            return ""
        if len(text) <= 8:
            return "<redacted>"
        return text[:4] + "*" * max(0, len(text) - 8) + text[-4:]
    if isinstance(value, dict):
        return {str(k): _redact_sensitive_value(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_sensitive_value(item, key_name) for item in value]
    return value


def _ensure_fetlife_dependencies() -> None:
    missing: list[str] = []
    if BeautifulSoup is None:
        missing.append("beautifulsoup4")
    if webdriver is None or ChromeOptions is None or ChromeService is None or By is None or EC is None or WebDriverWait is None:
        missing.append("selenium")
    if missing:
        joined = ", ".join(sorted(set(missing)))
        raise RuntimeError(
            f"FetLife automation requires Python packages that are not installed: {joined}. "
            f"Install them with: python -m pip install {joined}"
        )


def _current_exec_log_context() -> dict[str, Any] | None:
    value = getattr(_EXEC_LOG_CONTEXT, "value", None)
    return value if isinstance(value, dict) else None


def _set_exec_log_context(context: dict[str, Any] | None) -> None:
    if context is None:
        if hasattr(_EXEC_LOG_CONTEXT, "value"):
            delattr(_EXEC_LOG_CONTEXT, "value")
    else:
        _EXEC_LOG_CONTEXT.value = context


def _exec_general_log_path(project: TailorProject) -> Path:
    stamp = dt.datetime.now().astimezone().strftime("%Y%m%d")
    return project.exec_logs_dir / f"executive_summary_{stamp}.txt"


def _append_exec_log_file(path: Path, *, event_type: str, title: str, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    section = (
        f"[{timestamp}] [{event_type.upper()}] {title}\n"
        f"{body.rstrip()}\n"
        f"{'=' * 80}\n"
    )
    with _NETWORK_LOG_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(section)


def _append_active_exec_log(*, event_type: str, title: str, body: str) -> None:
    context = _current_exec_log_context()
    if not context:
        return
    path_text = str(context.get("log_path") or "").strip()
    if not path_text:
        return
    _append_exec_log_file(Path(path_text), event_type=event_type, title=title, body=body)


def _append_general_exec_log(project: TailorProject, *, event_type: str, title: str, body: str) -> None:
    _append_exec_log_file(_exec_general_log_path(project), event_type=event_type, title=title, body=body)


def _engine_log_paths(project: TailorProject) -> tuple[Path, Path]:
    """Returns (human-readable log, NDJSON structured log)."""
    root = project.engine_logs_dir
    day = dt.datetime.now().astimezone().strftime("%Y%m%d")
    return root / f"engine-{day}.log", root / f"engine-events-{day}.ndjson"


def _append_engine_log(project: TailorProject, record: dict[str, Any]) -> None:
    """Append one structured engine event (all agents / all workflows)."""
    record = dict(record)
    human_summary = record.pop("human_summary", None)
    record.setdefault("ts", dt.datetime.now().astimezone().isoformat())
    text_path, ndjson_path = _engine_log_paths(project)
    text_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False, default=str)
    if human_summary is not None:
        human = str(human_summary)[:1200]
    else:
        summary_bits = [
            str(record.get("event", "event")),
            str(record.get("agent", "") or "-"),
            str(record.get("workflow", "") or "-"),
            str(record.get("detail", "") or ""),
        ]
        human = " | ".join(b for b in summary_bits if b and b != "-")[:800]
    with _NETWORK_LOG_LOCK:
        with ndjson_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        with text_path.open("a", encoding="utf-8") as handle:
            handle.write(f"{record['ts']}  {human}\n")


def _engine_dev_api_logging_enabled(project: TailorProject) -> bool:
    """
    When True, workflow runs append full HTTP request/response bodies to engine NDJSON.

    Default: enabled if the project root looks like a git checkout (``.git`` present),
    or if ``TAILOR_DEV_MODE`` is truthy. Override with ``TAILOR_ENGINE_DEV_API_LOG``
    (1/true to force on, 0/false to force off).
    """
    flag = os.getenv("TAILOR_ENGINE_DEV_API_LOG", "").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return False
    if flag in ("1", "true", "yes", "on"):
        return True
    if os.getenv("TAILOR_DEV_MODE", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    try:
        return (project.root / ".git").exists()
    except OSError:
        return False


def _set_engine_workflow_dev_api_context(project: TailorProject | None) -> None:
    """Limit full API logging to the active workflow run (see ``_run_chained_workflow``)."""
    if project is None:
        if hasattr(_ENGINE_WORKFLOW_DEV_API, "value"):
            delattr(_ENGINE_WORKFLOW_DEV_API, "value")
        return
    if _engine_dev_api_logging_enabled(project):
        _ENGINE_WORKFLOW_DEV_API.value = {"project": project}
    elif hasattr(_ENGINE_WORKFLOW_DEV_API, "value"):
        delattr(_ENGINE_WORKFLOW_DEV_API, "value")


def _engine_dev_api_active_project() -> TailorProject | None:
    raw = getattr(_ENGINE_WORKFLOW_DEV_API, "value", None)
    if isinstance(raw, dict):
        p = raw.get("project")
        if isinstance(p, TailorProject):
            return p
    return None


def _engine_log_workflow_run_tags() -> dict[str, Any]:
    tags: dict[str, Any] = {}
    xc = _current_exec_log_context()
    if xc:
        tags["run_agent"] = xc.get("agent_name")
        tags["run_workflow"] = xc.get("workflow_name")
        tags["run_workflow_file"] = xc.get("workflow_file")
        tags["run_exec_log"] = xc.get("log_path")
    return tags


def _infer_agent_name_from_workflow_path(project: TailorProject, workflow_path: Path) -> str:
    try:
        relative = workflow_path.resolve().relative_to(project.root.resolve())
    except Exception:
        relative = workflow_path
    parts = list(relative.parts)
    if "agents" in parts:
        index = parts.index("agents")
        if index + 1 < len(parts):
            return parts[index + 1]
    return workflow_path.parent.name or "general"


def _begin_workflow_exec_logging(project: TailorProject, workflow_path: Path, args: argparse.Namespace,
                                 spec: dict[str, Any], base_context: dict[str, Any]) -> dict[str, Any]:
    agent_name = str(base_context.get("agent_key") or "").strip() or _infer_agent_name_from_workflow_path(project, workflow_path)
    workflow_name = workflow_path.stem
    timestamp = dt.datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    log_path = project.exec_logs_dir / f"{timestamp}_{_slugify_for_filename(agent_name)}_{_slugify_for_filename(workflow_name)}.txt"
    context = {
        "project_root": str(project.root),
        "agent_name": agent_name,
        "workflow_name": workflow_name,
        "workflow_file": str(workflow_path),
        "log_path": str(log_path),
    }
    _set_exec_log_context(context)
    workflow_settings = {key: value for key, value in spec.items() if key != "steps"}
    safe_args = _redact_sensitive_value(vars(args))
    body = (
        f"Agent: {agent_name}\n"
        f"Process ID: {os.getpid()}\n"
        f"Workflow file: {workflow_path}\n"
        f"Workflow name: {workflow_name}\n"
        f"CLI args:\n{json.dumps(safe_args, indent=2, ensure_ascii=False, default=str)}\n\n"
        f"Workflow settings:\n{json.dumps(workflow_settings, indent=2, ensure_ascii=False, default=str)}\n\n"
        f"Resolved base context:\n{json.dumps(base_context, indent=2, ensure_ascii=False, default=str)}"
    )
    _append_active_exec_log(event_type="workflow_start", title="Agent began running workflow", body=body)
    _append_engine_log(
        project,
        {
            "event": "workflow_start",
            "agent": agent_name,
            "workflow": workflow_name,
            "workflow_file": str(workflow_path),
            "pid": os.getpid(),
            "detail": f"{len(spec.get('steps', []) or [])} steps; type={spec.get('workflow-type', 'single_run')}",
        },
    )
    return context


def _load_user_notifications(project: TailorProject) -> list[dict[str, Any]]:
    path = project.user_notifications_path
    if not path.exists():
        return []
    notifications: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            item = {
                "id": new_id(),
                "created_utc": now_iso(),
                "level": "warning",
                "category": "legacy_notification",
                "title": "Legacy notification line",
                "message": line,
            }
        if isinstance(item, dict):
            notifications.append(item)
    notifications.sort(key=lambda item: str(item.get("created_utc") or ""), reverse=True)
    return notifications


def _append_user_notification(project: TailorProject, *, title: str, message: str,
                              level: str = "warning", category: str = "general",
                              details: dict[str, Any] | None = None) -> dict[str, Any]:
    path = project.user_notifications_path
    path.parent.mkdir(parents=True, exist_ok=True)
    notification = {
        "id": new_id(),
        "created_utc": now_iso(),
        "level": level,
        "category": category,
        "title": title,
        "message": message,
        "details": details or {},
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(notification, ensure_ascii=False) + "\n")
    detail_text = json.dumps(notification.get("details") or {}, indent=2, ensure_ascii=False, default=str)
    body = (
        f"Level: {level}\n"
        f"Category: {category}\n"
        f"Notification id: {notification['id']}\n"
        f"Message: {message}\n"
        f"Details:\n{detail_text}"
    )
    _append_general_exec_log(project, event_type="notification", title=title, body=body)
    _append_active_exec_log(event_type="notification", title=title, body=body)
    return notification


def _clear_user_notifications(project: TailorProject) -> int:
    path = project.user_notifications_path
    existing = _load_user_notifications(project)
    if path.exists():
        path.write_text("", encoding="utf-8")
    return len(existing)


def _format_notification_line(notification: dict[str, Any]) -> str:
    created = str(notification.get("created_utc") or "")
    level = str(notification.get("level") or "info").upper()
    title = str(notification.get("title") or "Notification")
    return f"{created} [{level}] {title}"


_LAST_CHANCE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("square_brackets", re.compile(r"\[[^\[\]\r\n]{1,200}\]")),
    ("curly_braces", re.compile(r"\{[^\{\}\r\n]{1,200}\}")),
    ("angle_brackets", re.compile(r"<[^<>\r\n]{1,200}>")),
]


def _review_outbound_email_text(subject: str, body: str) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for location, value in (("subject", subject or ""), ("body", body or "")):
        for kind, pattern in _LAST_CHANCE_PATTERNS:
            for match in pattern.finditer(value):
                snippet = match.group(0)
                dedupe_key = (location, kind, snippet)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                findings.append({
                    "location": location,
                    "kind": kind,
                    "text": snippet,
                })
    return {
        "blocked": bool(findings),
        "findings": findings,
    }


def _blocked_email_notification_message(*, to_email: str, subject: str, findings: list[dict[str, Any]],
                                        source_label: str, draft_id: str = "", recipient_count: int = 1) -> str:
    tokens = ", ".join(sorted({str(item.get("text") or "") for item in findings if item.get("text")}))
    recipient_text = f"{recipient_count} recipients" if recipient_count != 1 else to_email
    draft_text = f" Gmail draft id: {draft_id}." if draft_id else ""
    return (
        f"Blocked an outbound Gmail message from {source_label} to {recipient_text} because placeholder-like text was found. "
        f"Subject: {subject!r}. Suspicious text: {tokens}.{draft_text}"
    )


def _send_gmail_message_checked(project: TailorProject, gmail: "GmailClient", *, to_email: str, subject: str, body: str,
                                source_label: str, from_email: str | None = None, thread_id: str | None = None,
                                in_reply_to: str | None = None, references: str | None = None,
                                dry_run: bool = False, create_draft_when_blocked: bool = True,
                                recipient_count: int = 1) -> dict[str, Any]:
    review = _review_outbound_email_text(subject, body)
    result: dict[str, Any] = {
        "to": to_email,
        "from": from_email or "",
        "subject": subject,
        "body": body,
        "review": review,
        "blocked": False,
        "draft_created": False,
        "sent": False,
        "dry_run": dry_run,
    }
    if review["blocked"]:
        result["blocked"] = True
        draft_result: dict[str, Any] = {}
        if create_draft_when_blocked:
            draft_result = gmail.create_draft(
                to_email,
                subject,
                body,
                from_email=from_email,
                thread_id=thread_id,
                in_reply_to=in_reply_to,
                references=references,
            )
            result["draft_created"] = True
            result["draft_id"] = str(draft_result.get("id") or "")
        notification = _append_user_notification(
            project,
            title="Blocked outbound Gmail message saved as draft",
            message=_blocked_email_notification_message(
                to_email=to_email,
                subject=subject,
                findings=review["findings"],
                source_label=source_label,
                draft_id=str(result.get("draft_id") or ""),
                recipient_count=recipient_count,
            ),
            level="warning",
            category="outbound_email_review",
            details={
                "to": to_email,
                "from": from_email or "",
                "subject": subject,
                "source_label": source_label,
                "recipient_count": recipient_count,
                "findings": review["findings"],
                "draft_id": str(result.get("draft_id") or ""),
            },
        )
        result["notification_id"] = str(notification.get("id") or "")
        return result

    if dry_run:
        draft_result = gmail.create_draft(
            to_email,
            subject,
            body,
            from_email=from_email,
            thread_id=thread_id,
            in_reply_to=in_reply_to,
            references=references,
        )
        result["draft_created"] = True
        result["draft_id"] = str(draft_result.get("id") or "")
        return result

    send_result = gmail.send_message(
        to_email,
        subject,
        body,
        from_email=from_email,
        thread_id=thread_id,
        in_reply_to=in_reply_to,
        references=references,
    )
    result["sent"] = True
    result["message_id"] = str(send_result.get("id") or "")
    if thread_id:
        result["thread_id"] = thread_id
    if in_reply_to:
        result["in_reply_to"] = in_reply_to
    return result


def _active_ai_profile(project: TailorProject) -> dict[str, Any]:
    settings = _load_appsettings(project)
    runtime = settings.get("AiRuntime", {}) if isinstance(settings.get("AiRuntime"), dict) else {}
    profiles = runtime.get("Profiles", {}) if isinstance(runtime.get("Profiles"), dict) else {}
    active_name = str(runtime.get("ActiveProfile") or "").strip()
    profile = profiles.get(active_name, {}) if active_name else {}
    if not isinstance(profile, dict):
        profile = {}
    model = str(profile.get("model") or settings.get("OpenAi", {}).get("DefaultChatModel") or "gpt-4o-mini").strip()
    endpoint = str(profile.get("endpoint") or "https://api.openai.com/v1/chat/completions").strip()
    auth_mode = str(profile.get("auth_mode") or "").strip().lower()
    if not auth_mode:
        auth_mode = "api-key" if "openai.azure.com" in endpoint.lower() else "bearer"
    secret_ref = str(profile.get("api_key_ref") or "").strip()
    api_key = _resolve_secret_from_settings(settings, secret_ref)
    if not api_key:
        env_name = str(profile.get("api_key_env") or "").strip()
        if env_name:
            api_key = os.getenv(env_name, "")
        elif auth_mode == "bearer":
            api_key = os.getenv("OPENAI_API_KEY", "")
        else:
            api_key = os.getenv("AZURE_OPENAI_API_KEY", "")
    return {
        "profile_name": active_name or "default",
        "provider_name": str(profile.get("provider_name") or "OpenAI ChatGPT").strip(),
        "provider_type": str(profile.get("provider_type") or "OpenAI").strip(),
        "endpoint": str(profile.get("endpoint") or endpoint).strip(),
        "model": model,
        "auth_mode": auth_mode,
        "api_key_ref": secret_ref,
        "api_key_secondary_ref": str(profile.get("api_key_secondary_ref") or "").strip(),
        "api_key": str(api_key or "").strip(),
        "region": str(profile.get("region") or "").strip(),
        "settings": settings,
    }


def _argv_has_option(argv: list[str], option_name: str) -> bool:
    return any(arg == option_name or arg.startswith(option_name + "=") for arg in argv)


def _apply_active_ai_profile_to_args(args: argparse.Namespace, project: TailorProject, argv: list[str]) -> None:
    profile = _active_ai_profile(project)
    if hasattr(args, "model") and not _argv_has_option(argv, "--model") and profile.get("model"):
        args.model = str(profile["model"])
        setattr(args, "_model_from_profile", True)
    if hasattr(args, "ai_endpoint") and not _argv_has_option(argv, "--ai-endpoint") and profile.get("endpoint"):
        args.ai_endpoint = _normalize_ai_endpoint(str(profile["endpoint"]), str(profile.get("auth_mode") or ""))
        setattr(args, "_endpoint_from_profile", True)
    if hasattr(args, "api_key") and not _argv_has_option(argv, "--api-key") and profile.get("api_key"):
        args.api_key = str(profile["api_key"])
        setattr(args, "_api_key_from_profile", True)


def _normalize_ai_endpoint(endpoint: str, auth_mode: str = "") -> str:
    text = (endpoint or "").strip()
    if not text:
        return "https://api.openai.com/v1/chat/completions"
    parsed = urllib.parse.urlparse(text)
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    mode = (auth_mode or "").strip().lower()
    if not mode:
        mode = "api-key" if "openai.azure.com" in host else "bearer"
    if mode == "api-key" and "openai.azure.com" in host:
        if path.endswith("/openai/v1/chat/completions"):
            return text
        if path in {"", "/"}:
            return text.rstrip("/") + "/openai/v1/chat/completions"
    if mode == "bearer":
        if path.endswith("/v1/chat/completions"):
            return text
        if path in {"", "/"}:
            return text.rstrip("/") + "/v1/chat/completions"
    return text


def _write_appsettings_local(project: TailorProject, settings: dict[str, Any]) -> None:
    project.appsettings_local_path.parent.mkdir(parents=True, exist_ok=True)
    project.appsettings_local_path.write_text(json.dumps(settings, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _sync_ai_provider_from_profile(project: TailorProject, profile: dict[str, Any]) -> dict[str, Any]:
    created = False
    activated_provider_id = ""
    with project.connect() as conn:
        rows = conn.execute("SELECT * FROM AiProvider ORDER BY FallbackOrder ASC, CreatedUtc ASC").fetchall()
        selected = None
        for row in rows:
            config_json = row["ConfigJson"] if "ConfigJson" in row.keys() else None
            parsed = {}
            if config_json:
                try:
                    parsed = json.loads(config_json)
                except json.JSONDecodeError:
                    parsed = {}
            if parsed.get("profile_name") == profile["profile_name"]:
                selected = row
                break
        if selected is None:
            for row in rows:
                if str(row["Name"] or "").strip().lower() == profile["provider_name"].lower():
                    selected = row
                    break
        conn.execute("UPDATE AiProvider SET IsActive = 0 WHERE IsActive <> 0")
        config_json = json.dumps(
            {
                "profile_name": profile["profile_name"],
                "auth_mode": profile["auth_mode"],
                "region": profile["region"],
                "saved_utc": now_iso(),
            },
            ensure_ascii=False,
        )
        if selected is None:
            activated_provider_id = new_id()
            conn.execute(
                """
                INSERT INTO AiProvider (Id, Name, ProviderType, Endpoint, ModelName, ApiKeyRef,
                    FallbackOrder, Temperature, MaxTokens, IsActive, ConfigJson, CreatedUtc)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0.7, 4096, 1, ?, ?)
                """,
                (
                    activated_provider_id,
                    profile["provider_name"],
                    profile["provider_type"],
                    profile["endpoint"],
                    profile["model"],
                    profile["api_key_ref"] or None,
                    config_json,
                    now_iso(),
                ),
            )
            created = True
        else:
            activated_provider_id = str(selected["Id"])
            conn.execute(
                """
                UPDATE AiProvider
                SET Name = ?, ProviderType = ?, Endpoint = ?, ModelName = ?, ApiKeyRef = ?,
                    FallbackOrder = 0, IsActive = 1, ConfigJson = ?
                WHERE Id = ?
                """,
                (
                    profile["provider_name"],
                    profile["provider_type"],
                    profile["endpoint"],
                    profile["model"],
                    profile["api_key_ref"] or None,
                    config_json,
                    activated_provider_id,
                ),
            )
        conn.commit()
    return {"provider_id": activated_provider_id, "created": created}


# ----------------------------
# AI client (OpenAI-focused + generic HTTP option)
# ----------------------------

class AiClient:
    def __init__(self, model: str, api_key: str | None = None,
                 endpoint: str = "https://api.openai.com/v1/chat/completions",
                 extra_headers: dict[str, str] | None = None,
                 auth_mode: str | None = None,
                 request_timeout_seconds: int | None = None) -> None:
        self.model = model
        self.endpoint = _normalize_ai_endpoint(endpoint, auth_mode or "")
        inferred_auth_mode = (auth_mode or "").strip().lower()
        if not inferred_auth_mode:
            inferred_auth_mode = "api-key" if "openai.azure.com" in self.endpoint.lower() else "bearer"
        self.auth_mode = inferred_auth_mode
        default_env_key = os.getenv("AZURE_OPENAI_API_KEY") if self.auth_mode == "api-key" else os.getenv("OPENAI_API_KEY")
        self.api_key = api_key or default_env_key
        self.extra_headers = extra_headers or {}
        self.request_timeout_seconds = int(request_timeout_seconds) if request_timeout_seconds is not None else DEFAULT_AI_CHAT_TIMEOUT

    def chat(self, system: str, user: str, temperature: float = 0.2, max_tokens: int = 1200,
             timeout_seconds: int | None = None) -> str:
        if not self.api_key:
            raise RuntimeError("Missing API key. Set OPENAI_API_KEY or provide --api-key.")
        trace(2, f"[ai] sending chat request model={self.model} endpoint={self.endpoint}")
        trace(3, f"[ai] system={trace_excerpt(system, 1200)}")
        trace(3, f"[ai] user={trace_excerpt(user, 1200)}")

        headers = {
            **self.extra_headers,
        }
        if self.auth_mode == "api-key":
            headers["api-key"] = self.api_key
        else:
            headers["Authorization"] = f"Bearer {self.api_key}"
        body: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
        }
        body.update(_chat_api_token_limit_fields(self.model, max_tokens))
        timeout = int(timeout_seconds) if timeout_seconds is not None else self.request_timeout_seconds
        trace(2, f"[ai] HTTP timeout={timeout}s")
        obj = _request_json(self.endpoint, method="POST", headers=headers, body=body, timeout=timeout)
        choices = obj.get("choices") or []
        if not choices:
            raise RuntimeError(f"No choices in AI response: {obj}")
        choice0 = choices[0]
        message = choice0.get("message") or {}
        finish = choice0.get("finish_reason")
        if not isinstance(message, dict):
            raise RuntimeError(f"Unexpected AI message shape: {choice0!r}")
        if message.get("refusal"):
            raise RuntimeError(f"Model refused to respond: {message.get('refusal')!r}")
        content = _normalize_chat_completion_content(message.get("content"))
        if not content.strip():
            raise RuntimeError(
                "AI returned an empty assistant message. "
                f"finish_reason={finish!r}. "
                "Often caused by context/token limits, wrong max_tokens field for this model, or provider filters. "
                "Inspect dev_api_response in logs/engine for the full payload."
            )
        trace(2, f"[ai] received response ({len(content.strip())} chars)")
        trace(3, f"[ai] response={trace_excerpt(content.strip(), 1200)}")
        return content.strip()


# ----------------------------
# Gmail API support (refresh token flow)
# ----------------------------

@dataclass
class GmailCredentials:
    client_id: str
    client_secret: str
    refresh_token: str


@dataclass
class GmailOAuthTokenResult:
    refresh_token: str | None
    scope: str | None


class _OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        self.server.callback_params = {key: values[0] for key, values in params.items() if values}
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(
            b"<html><body><h2>TaiLOR received the Gmail OAuth callback.</h2>"
            b"<p>You can close this browser tab and return to the terminal.</p></body></html>"
        )
        self.server.callback_event.set()

    def log_message(self, format: str, *args: Any) -> None:
        return


class _OAuthCallbackServer(http.server.HTTPServer):
    def __init__(self, server_address: tuple[str, int]):
        super().__init__(server_address, _OAuthCallbackHandler)
        self.callback_params: dict[str, str] = {}
        self.callback_event = threading.Event()


def _exchange_google_auth_code(client_id: str, client_secret: str, code: str, redirect_uri: str) -> GmailOAuthTokenResult:
    payload = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }).encode("utf-8")
    try:
        payload_text, _ = _request_text(
            GmailClient.TOKEN_URL,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=payload,
            timeout=120,
        )
        obj = json.loads(payload_text)
    except RuntimeError as ex:
        detail = str(ex).splitlines()[-1]
        raise RuntimeError(f"Google OAuth token exchange failed: {detail}") from ex

    return GmailOAuthTokenResult(
        refresh_token=obj.get("refresh_token"),
        scope=obj.get("scope"),
    )


class GmailClient:
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"

    def __init__(self, creds: GmailCredentials):
        self.creds = creds
        self._cached_access_token: str | None = None
        self._cached_access_token_expires_utc: dt.datetime | None = None

    def _access_token(self) -> str:
        if (
            self._cached_access_token
            and self._cached_access_token_expires_utc
            and dt.datetime.now(tz=UTC) < (self._cached_access_token_expires_utc - dt.timedelta(seconds=60))
        ):
            trace(3, "[gmail] using cached Gmail access token")
            return self._cached_access_token

        trace(2, "[gmail] refreshing Gmail access token")
        started = time.time()
        obj = _refresh_google_access_token(self.creds.client_id, self.creds.client_secret, self.creds.refresh_token)

        token = obj.get("access_token")
        if not token:
            raise RuntimeError(f"No access_token in response: {obj}")
        expires_in_raw = obj.get("expires_in", 3600)
        try:
            expires_in = max(60, int(expires_in_raw))
        except (TypeError, ValueError):
            expires_in = 3600
        self._cached_access_token = str(token)
        self._cached_access_token_expires_utc = dt.datetime.now(tz=UTC) + dt.timedelta(seconds=expires_in)
        elapsed = time.time() - started
        trace(2, f"[gmail] access token acquired in {elapsed:.2f}s; expires in {expires_in} seconds")
        return self._cached_access_token

    def _api(self, path: str, method: str = "GET", body: dict[str, Any] | None = None) -> dict[str, Any]:
        token = self._access_token()
        url = f"{self.API_BASE}/{path.lstrip('/')}"
        headers = {"Authorization": f"Bearer {token}"}
        trace(2, f"[gmail] api {method} {path}")
        return _request_json(url, method=method, headers=headers, body=body, timeout=90)

    def list_inbox(self, max_results: int = 10, query: str | None = None) -> list[dict[str, Any]]:
        remaining = max_results if max_results and max_results > 0 else None
        page_token = ""
        items: list[dict[str, Any]] = []
        while True:
            page_size = min(remaining, 100) if remaining is not None else 100
            query_params = {
                "maxResults": str(max(page_size, 1)),
                "q": query or "in:inbox",
            }
            if page_token:
                query_params["pageToken"] = page_token
            q = urllib.parse.urlencode(query_params)
            trace(3, f"[gmail] list inbox querystring={q}")
            obj = self._api(f"messages?{q}")
            page_items = obj.get("messages", []) or []
            items.extend(page_items)
            if remaining is not None:
                remaining -= len(page_items)
                if remaining <= 0:
                    break
            page_token = str(obj.get("nextPageToken", "") or "")
            if not page_token or not page_items:
                break
        return items

    def get_message_raw(self, message_id: str) -> dict[str, Any]:
        trace(3, f"[gmail] get raw message {message_id}")
        return self._api(f"messages/{message_id}?format=raw")

    def get_message_summary(self, message_id: str) -> dict[str, str]:
        raw_obj = self.get_message_raw(message_id)
        raw_bytes = base64.urlsafe_b64decode(raw_obj.get("raw", "") + "===")
        msg = email.message_from_bytes(raw_bytes)
        subject = msg.get("Subject", "")
        from_h = msg.get("From", "")
        date_h = msg.get("Date", "")
        message_header_id = msg.get("Message-Id", "")
        in_reply_to = msg.get("In-Reply-To", "")
        references = " ".join(msg.get_all("References", []))
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                ctype = part.get_content_type()
                if ctype == "text/plain" and not part.get_filename():
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
                        break
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
        return {
            "subject": subject,
            "from": from_h,
            "date": date_h,
            "thread_id": str(raw_obj.get("threadId", "") or ""),
            "message_header_id": message_header_id,
            "in_reply_to": in_reply_to,
            "references": references.strip(),
            "body": body.strip(),
        }

    @staticmethod
    def _build_raw_message(to_email: str, subject: str, body: str, from_email: str | None = None,
                           in_reply_to: str | None = None, references: str | None = None) -> str:
        headers = []
        if from_email:
            headers.append(f"From: {from_email}")
        if in_reply_to:
            headers.append(f"In-Reply-To: {_normalize_message_id_header(in_reply_to)}")
        if references:
            headers.append(f"References: {references}")
        headers.extend(
            [
                f"To: {to_email}",
                f"Subject: {subject}",
                "Content-Type: text/plain; charset=UTF-8",
                "",
                body,
            ]
        )
        mime = "\r\n".join(headers)
        return base64.urlsafe_b64encode(mime.encode("utf-8")).decode("ascii").rstrip("=")

    def send_message(self, to_email: str, subject: str, body: str, *, from_email: str | None = None,
                     thread_id: str | None = None, in_reply_to: str | None = None,
                     references: str | None = None) -> dict[str, Any]:
        trace(2, f"[gmail] sending message to={to_email} subject={subject!r}")
        trace(3, f"[gmail] body={trace_excerpt(body, 1200)}")
        raw = self._build_raw_message(to_email, subject, body, from_email=from_email, in_reply_to=in_reply_to, references=references)
        payload: dict[str, Any] = {"raw": raw}
        if thread_id:
            payload["threadId"] = thread_id
        return self._api("messages/send", method="POST", body=payload)

    def create_draft(self, to_email: str, subject: str, body: str, from_email: str | None = None,
                     thread_id: str | None = None, in_reply_to: str | None = None,
                     references: str | None = None) -> dict[str, Any]:
        trace(2, f"[gmail] creating draft to={to_email} subject={subject!r}")
        trace(3, f"[gmail] draft body={trace_excerpt(body, 1200)}")
        raw = self._build_raw_message(to_email, subject, body, from_email=from_email, in_reply_to=in_reply_to, references=references)
        message_payload: dict[str, Any] = {"raw": raw}
        if thread_id:
            message_payload["threadId"] = thread_id
        return self._api("drafts", method="POST", body={"message": message_payload})

    def mark_read_and_archive(self, message_id: str) -> dict[str, Any]:
        return self._api(
            f"messages/{message_id}/modify",
            method="POST",
            body={"removeLabelIds": ["UNREAD", "INBOX"]},
        )


def _default_fetlife_user_data_dir(project: "TailorProject") -> Path:
    candidates = [
        project.fetlife_profile_snapshot_dir,
        FETLIFE_TEMP_PROFILE_DIR,
    ]
    for candidate in candidates:
        if (candidate / "Local State").exists():
            return candidate
    return project.fetlife_profile_snapshot_dir


def _copy_file_if_present(source: Path, destination: Path) -> bool:
    try:
        if not source.exists():
            return False
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return True
    except OSError:
        return False


def _prepare_fetlife_user_data_dir(project: "TailorProject", user_data_dir: Path, profile_directory: str) -> Path:
    target = Path(user_data_dir)
    if target == project.fetlife_profile_snapshot_dir:
        source_root = FETLIFE_TEMP_PROFILE_DIR
        if source_root.exists():
            _copy_file_if_present(source_root / "Local State", target / "Local State")
            _copy_file_if_present(source_root / profile_directory / "Network" / "Cookies", target / profile_directory / "Network" / "Cookies")
            _copy_file_if_present(source_root / profile_directory / "Preferences", target / profile_directory / "Preferences")
    return target


def _build_fetlife_runtime_profile(snapshot_root: Path, profile_directory: str) -> Path:
    runtime_root = Path(tempfile.mkdtemp(prefix="tailor-fetlife-runtime-"))
    _copy_file_if_present(snapshot_root / "Local State", runtime_root / "Local State")
    profile_source = snapshot_root / profile_directory
    if not profile_source.exists():
        raise FileNotFoundError(f"FetLife Chrome profile directory not found: {profile_source}")
    shutil.copytree(profile_source, runtime_root / profile_directory, dirs_exist_ok=True)
    for relative_path in (
        Path("DevToolsActivePort"),
        Path(profile_directory) / "SingletonCookie",
        Path(profile_directory) / "SingletonLock",
        Path(profile_directory) / "SingletonSocket",
    ):
        try:
            (runtime_root / relative_path).unlink(missing_ok=True)
        except OSError:
            pass
    return runtime_root


class FetlifeClient:
    BASE_URL = "https://fetlife.com"
    LOAD_MORE_PAGE_SIZE = 20

    def __init__(self, user_data_dir: Path, *, profile_directory: str = "Profile 9", page_load_timeout: int = 60,
                 headless: bool = False, show_window: bool = False, use_runtime_copy: bool = True):
        self.user_data_dir = Path(user_data_dir)
        self.profile_directory = profile_directory
        self.page_load_timeout = int(page_load_timeout)
        self.headless = bool(headless)
        self.show_window = bool(show_window)
        self.use_runtime_copy = bool(use_runtime_copy)
        self._driver: Any | None = None
        self._self_nickname = ""
        self._runtime_user_data_dir: Path | None = None

    def close(self) -> None:
        if self._driver is not None:
            try:
                self._driver.quit()
            except Exception:
                pass
            finally:
                self._driver = None
        if self._runtime_user_data_dir is not None:
            try:
                shutil.rmtree(self._runtime_user_data_dir, ignore_errors=True)
            except Exception:
                pass
            finally:
                self._runtime_user_data_dir = None

    def _ensure_driver(self) -> Any:
        _ensure_fetlife_dependencies()
        if self._driver is not None:
            return self._driver
        if not self.user_data_dir.exists():
            raise FileNotFoundError(f"FetLife browser profile not found: {self.user_data_dir}")
        effective_user_data_dir = self.user_data_dir
        if self.use_runtime_copy:
            self._runtime_user_data_dir = _build_fetlife_runtime_profile(self.user_data_dir, self.profile_directory)
            effective_user_data_dir = self._runtime_user_data_dir
        options = ChromeOptions()
        options.add_argument(f"--user-data-dir={effective_user_data_dir}")
        options.add_argument(f"--profile-directory={self.profile_directory}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-background-networking")
        if self.headless:
            options.add_argument("--headless=new")
        elif not self.show_window:
            # FetLife currently blocks true headless sessions via Cloudflare, so the reliable no-UI mode
            # is to run a normal browser off-screen from a disposable runtime profile.
            options.add_argument("--window-position=-2400,-2400")
            options.add_argument("--window-size=1280,900")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        driver = webdriver.Chrome(options=options, service=ChromeService())
        driver.set_page_load_timeout(self.page_load_timeout)
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
        )
        self._driver = driver
        atexit.register(self.close)
        return driver

    def _open(self, path_or_url: str, *, wait_selector: str | None = None, timeout_seconds: int = 30) -> Any:
        driver = self._ensure_driver()
        url = path_or_url if str(path_or_url).startswith("http") else f"{self.BASE_URL}{path_or_url}"
        driver.get(url)
        if wait_selector:
            WebDriverWait(driver, timeout_seconds).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
            )
        time.sleep(1.0)
        return driver

    def _wait_for_inbox_ready(self, driver: Any, timeout_seconds: int = 30) -> None:
        def _ready(current_driver: Any) -> bool:
            anchors = current_driver.find_elements(By.CSS_SELECTOR, "a[href^='/conversations/'][href$='#newest_message']")
            if anchors:
                return True
            page_text = str(current_driver.page_source or "")
            return (
                "No conversations in inbox" in page_text
                or "No conversations in all mail" in page_text
                or "Requests" in page_text
                or "Archived" in page_text
                or "All Mail" in page_text
            )

        WebDriverWait(driver, timeout_seconds).until(_ready)
        time.sleep(1.0)

    def ensure_authenticated(self) -> str:
        driver = self._open("/inbox")
        current_url = str(driver.current_url or "")
        title = str(driver.title or "")
        if "/login" in current_url or "login" in title.lower():
            raise RuntimeError(
                "FetLife is not logged in for the configured Chrome profile snapshot. "
                "Refresh the snapshot from a live logged-in FetLife browser session and try again."
            )
        if "Just a moment" in title:
            raise RuntimeError(
                "FetLife is still presenting a Cloudflare challenge to the automated browser session. "
                "Refresh the profile snapshot from a working Chrome session."
            )
        try:
            self._self_nickname = str(
                driver.execute_script("return (window.FL && FL.user && FL.user.nickname) || '';")
            ).strip()
        except Exception:
            self._self_nickname = self._self_nickname or ""
        return self._self_nickname

    @staticmethod
    def _find_inbox_row_node(anchor: Any) -> Any:
        node = anchor
        while node is not None:
            classes = node.get("class", []) if hasattr(node, "get") else []
            if getattr(node, "name", "") == "div" and "group" in classes and node.select_one("time[datetime]"):
                return node
            node = getattr(node, "parent", None)
        return anchor

    @staticmethod
    def _clean_text(value: str) -> str:
        text = (value or "").replace("\r", "")
        lines = [line.rstrip() for line in text.splitlines()]
        return "\n".join(lines).strip()

    @staticmethod
    def _extract_participant_from_row(row: Any) -> tuple[str, str]:
        for anchor in row.find_all("a", href=True):
            href = str(anchor.get("href") or "")
            if not href.startswith("/") or href.startswith("/conversations/") or href.startswith("/support"):
                continue
            nickname = " ".join(anchor.get_text(" ", strip=True).split()) or str(anchor.get("title") or "").strip()
            slug = href.strip("/").split("/", 1)[0]
            if nickname or slug:
                return nickname or slug, slug
        return "", ""

    @staticmethod
    def _mailbox_filter_from_path(mailbox_path: str) -> str | None:
        normalized = (mailbox_path or "").strip().lower()
        mapping = {
            "/inbox": "inbox",
            "/inbox/all": "all",
            "/inbox/archived": "archived",
            "/inbox/requests": "requests",
            "/inbox/l/unread": "unread",
            "/inbox/l/friends": "friends",
            "/inbox/l/followers": "followers",
        }
        return mapping.get(normalized)

    @staticmethod
    def _normalize_fetlife_html_text(value: str) -> str:
        text = re.sub(r"(?is)<br\s*/?>", "\n", value or "")
        text = re.sub(r"(?is)</p\s*>", "\n", text)
        text = re.sub(r"(?is)<[^>]+>", "", text)
        return html.unescape(text).strip()

    def _load_more_conversations(self, *, mailbox_path: str, max_results: int = 0, process_order: str = "desc") -> list[dict[str, Any]]:
        filter_name = self._mailbox_filter_from_path(mailbox_path)
        if not filter_name:
            return []
        driver = self._open(mailbox_path)
        self._wait_for_inbox_ready(driver)
        driver.set_script_timeout(max(60, self.page_load_timeout))
        items: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        offset = 0
        while True:
            result = driver.execute_async_script(
                """
const offset = arguments[0];
const filterName = arguments[1];
const done = arguments[arguments.length - 1];
let finished = false;
const finish = value => {
  if (!finished) {
    finished = true;
    done(value);
  }
};
const timer = window.setTimeout(() => finish({ timeout: true, offset }), 20000);
fetch(`/inbox/load_more?offset=${offset}&filter=${encodeURIComponent(filterName)}&order=newest`, {
  credentials: 'include',
  headers: { 'X-Requested-With': 'XMLHttpRequest' }
}).then(async response => {
  const text = await response.text();
  window.clearTimeout(timer);
  try {
    finish({ status: response.status, payload: JSON.parse(text) });
  } catch (error) {
    finish({ status: response.status, text, error: String(error) });
  }
}).catch(error => {
  window.clearTimeout(timer);
  finish({ error: String(error) });
});
                """,
                offset,
                filter_name,
            )
            if result.get("timeout"):
                trace(1, f"[fetlife] load_more timed out at offset={offset} filter={filter_name}; stopping pagination")
                break
            if result.get("error"):
                raise RuntimeError(f"FetLife load_more failed for offset {offset}: {result['error']}")
            status = int(result.get("status") or 0)
            if status != 200:
                raise RuntimeError(f"FetLife load_more returned HTTP {status} for offset {offset}")
            payload = result.get("payload")
            if not isinstance(payload, dict):
                raise RuntimeError(f"Unexpected FetLife load_more payload for offset {offset}")
            conversations = payload.get("conversations") or []
            if not isinstance(conversations, list):
                conversations = []
            for conversation in conversations:
                if not isinstance(conversation, dict):
                    continue
                conversation_id = str(conversation.get("conversation_id") or "")
                if not conversation_id or conversation_id in seen_ids:
                    continue
                seen_ids.add(conversation_id)
                last_message = conversation.get("last_message") or {}
                author = last_message.get("author") or {}
                participant_name = str(author.get("nickname") or "").strip()
                participant_slug = str(author.get("profile_url") or "").strip().strip("/").split("/", 1)[0]
                items.append(
                    {
                        "conversation_id": conversation_id,
                        "subject": str(conversation.get("display_subject") or "<no subject>"),
                        "snippet": self._normalize_fetlife_html_text(str(last_message.get("excerpt") or last_message.get("body") or "")),
                        "participant_name": participant_name,
                        "participant_slug": participant_slug,
                        "last_message_id": str(last_message.get("id") or ""),
                        "updated_at": str(conversation.get("updated_at") or ""),
                        "unread": bool(conversation.get("is_new") or conversation.get("new_messages_count")),
                        "mailbox_path": mailbox_path,
                    }
                )
                if max_results and max_results > 0 and len(items) >= max_results:
                    break
            if max_results and max_results > 0 and len(items) >= max_results:
                break
            if bool(payload.get("no_more")) or not conversations:
                break
            offset += self.LOAD_MORE_PAGE_SIZE
        items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=process_order.lower() != "asc")
        if max_results and max_results > 0:
            items = items[:max_results]
        return items

    def list_inbox_conversations(self, *, max_results: int = 0, process_order: str = "desc",
                                 mailbox_path: str = "/inbox/all") -> list[dict[str, Any]]:
        self.ensure_authenticated()
        items = self._load_more_conversations(mailbox_path=mailbox_path, max_results=max_results, process_order=process_order)
        if items:
            return items
        driver = self._open(mailbox_path)
        self._wait_for_inbox_ready(driver)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        fallback_items: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for anchor in soup.select("a[href^='/conversations/'][href$='#newest_message']"):
            row = self._find_inbox_row_node(anchor)
            href = str(anchor.get("href") or "")
            match = re.search(r"/conversations/(\d+)", href)
            if not match:
                continue
            conversation_id = match.group(1)
            if conversation_id in seen_ids:
                continue
            seen_ids.add(conversation_id)
            subject = " ".join(anchor.get_text(" ", strip=True).split()) or "<no subject>"
            snippet_anchor = None
            for candidate in row.select("p a[href^='/conversations/'][href$='#newest_message']"):
                if candidate is not anchor:
                    snippet_anchor = candidate
                    break
            snippet = " ".join((snippet_anchor or anchor).get_text(" ", strip=True).split())
            message_span = row.select_one("span[data-message-id]")
            updated_time = row.select_one("time[datetime]")
            participant_name, participant_slug = self._extract_participant_from_row(row)
            fallback_items.append(
                {
                    "conversation_id": conversation_id,
                    "subject": subject,
                    "snippet": snippet,
                    "participant_name": participant_name,
                    "participant_slug": participant_slug,
                    "last_message_id": str(message_span.get("data-message-id") or "") if message_span else "",
                    "updated_at": str(updated_time.get("datetime") or "") if updated_time else "",
                    "unread": bool(row.select_one("span.bg-red-600")),
                    "mailbox_path": mailbox_path,
                }
            )
        fallback_items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=process_order.lower() != "asc")
        if max_results and max_results > 0:
            fallback_items = fallback_items[:max_results]
        return fallback_items

    def get_conversation_details(self, conversation_id: str, *, inbox_item: dict[str, Any] | None = None) -> dict[str, Any]:
        self.ensure_authenticated()
        driver = self._open(f"/conversations/{conversation_id}", wait_selector="#messages")
        self_nickname = self._self_nickname or self.ensure_authenticated()
        soup = BeautifulSoup(driver.page_source, "html.parser")
        wrappers = soup.select("#messages > div[data-message-id]")
        messages: list[dict[str, Any]] = []
        for wrapper in wrappers:
            classes = set(wrapper.get("class", []))
            is_self = "justify-end" in classes
            author = ""
            author_slug = ""
            for anchor in wrapper.find_all("a", href=True):
                href = str(anchor.get("href") or "")
                if not href.startswith("/") or href.startswith("/support"):
                    continue
                text_value = " ".join(anchor.get_text(" ", strip=True).split())
                title_value = str(anchor.get("title") or "").strip()
                if text_value or title_value:
                    author = text_value or title_value
                    author_slug = href.strip("/").split("/", 1)[0]
                    break
            time_el = wrapper.select_one("time[datetime]")
            body_el = wrapper.select_one(".comment__copy")
            body = self._clean_text(body_el.get_text("\n", strip=True) if body_el else "")
            message_id = str(wrapper.get("data-message-id") or "")
            messages.append(
                {
                    "message_id": message_id,
                    "author": author or ("You" if is_self else ""),
                    "author_slug": author_slug,
                    "created_at": str(time_el.get("datetime") or "") if time_el else "",
                    "body": body,
                    "is_self": is_self or (author and author.lower() == self_nickname.lower()),
                }
            )
        title = str(driver.title or "").strip()
        subject = title
        suffix = " - Conversation | FetLife"
        if subject.endswith(suffix):
            subject = subject[: -len(suffix)].strip()
        subject = re.sub(r"^\(\d+\)\s*", "", subject).strip()
        subject = subject or str((inbox_item or {}).get("subject") or "<no subject>")
        participants = [
            name for name in [
                *(message.get("author") or "" for message in messages if not message.get("is_self")),
                str((inbox_item or {}).get("participant_name") or ""),
            ]
            if name
        ]
        deduped_participants: list[str] = []
        seen_lowered: set[str] = set()
        for name in participants:
            lowered = name.lower()
            if lowered in seen_lowered:
                continue
            seen_lowered.add(lowered)
            deduped_participants.append(name)
        participant_label = ", ".join(deduped_participants) or str((inbox_item or {}).get("participant_name") or "")
        last_message = messages[-1] if messages else {}
        thread_lines = [
            f"Conversation ID: {conversation_id}",
            f"Participants: {participant_label or '(unknown)'}",
            f"Subject: {subject}",
            f"Self Nickname: {self_nickname}",
            "",
        ]
        for message in messages:
            thread_lines.append(
                f"### {message.get('author') or 'Unknown'} | {message.get('created_at') or ''} | {message.get('message_id') or ''}"
            )
            thread_lines.append(str(message.get("body") or "").strip())
            thread_lines.append("")
        thread_text = "\n".join(thread_lines).strip()
        participant_slug = ""
        for message in messages:
            if not message.get("is_self") and str(message.get("author_slug") or "").strip():
                participant_slug = str(message.get("author_slug") or "").strip()
                break
        if not participant_slug:
            inbox_slug = str((inbox_item or {}).get("participant_slug") or "")
            if inbox_slug and inbox_slug.lower() != self_nickname.lower():
                participant_slug = inbox_slug
        if not participant_slug and deduped_participants:
            first_other = next((name for name in deduped_participants if name.lower() != self_nickname.lower()), deduped_participants[0])
            participant_slug = _slugify_for_filename(first_other.replace(" ", "-"))
        history_key = f"{participant_slug or 'conversation'}__{conversation_id}"
        return {
            "conversation_id": str(conversation_id),
            "subject": subject,
            "self_nickname": self_nickname,
            "participant_name": participant_label,
            "participant_slug": participant_slug,
            "history_key": history_key,
            "message_count": len(messages),
            "messages": messages,
            "thread_text": thread_text,
            "last_message_id": str(last_message.get("message_id") or ""),
            "last_message_author": str(last_message.get("author") or ""),
            "last_message_body": str(last_message.get("body") or ""),
            "last_message_created_at": str(last_message.get("created_at") or ""),
            "needs_reply": bool(messages and not bool(last_message.get("is_self"))),
        }

    def send_message(self, conversation_id: str, body: str, *, timeout_seconds: int = 45) -> dict[str, Any]:
        text = self._clean_text(body)
        if not text:
            raise RuntimeError("FetLife message body cannot be empty.")
        driver = self._open(f"/conversations/{conversation_id}", wait_selector="#new-message-textarea")
        existing_ids = {
            str(element.get_attribute("data-message-id") or "")
            for element in driver.find_elements(By.CSS_SELECTOR, "#messages > div[data-message-id]")
        }
        textarea = WebDriverWait(driver, timeout_seconds).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#new-message-textarea"))
        )
        driver.execute_script(
            """
const textarea = arguments[0];
const value = arguments[1];
textarea.focus();
textarea.value = value;
textarea.dispatchEvent(new Event('input', { bubbles: true }));
textarea.dispatchEvent(new Event('change', { bubbles: true }));
            """,
            textarea,
            text,
        )
        send_button = WebDriverWait(driver, timeout_seconds).until(
            lambda current_driver: next(
                (
                    button
                    for button in current_driver.find_elements(
                        By.XPATH,
                        "//button[@type='submit'][normalize-space()='Say It!' or .//span[normalize-space()='Say It!']]",
                    )
                    if button.is_enabled()
                ),
                None,
            )
        )
        send_button.click()

        def _message_sent(current_driver: Any) -> bool:
            wrappers = current_driver.find_elements(By.CSS_SELECTOR, "#messages > div[data-message-id]")
            wrapper_ids = {str(element.get_attribute("data-message-id") or "") for element in wrappers}
            return len(wrapper_ids - existing_ids) > 0

        WebDriverWait(driver, timeout_seconds).until(_message_sent)
        details = self.get_conversation_details(conversation_id)
        last_message = (details.get("messages") or [{}])[-1]
        return {
            "sent": bool(last_message and last_message.get("is_self")),
            "conversation_id": str(conversation_id),
            "message_id": str(last_message.get("message_id") or ""),
            "body": str(last_message.get("body") or text),
            "created_at": str(last_message.get("created_at") or ""),
        }


def _ensure_fetlife_client(project: "TailorProject", fetlife_cache: dict[str, FetlifeClient], *,
                           user_data_dir: Path | None = None, profile_directory: str = "Profile 9",
                           headless: bool = False, show_window: bool = False, use_runtime_copy: bool = True) -> FetlifeClient:
    resolved_dir = _prepare_fetlife_user_data_dir(
        project,
        user_data_dir or _default_fetlife_user_data_dir(project),
        profile_directory,
    )
    cache_key = (
        f"{resolved_dir}|{profile_directory}|headless={int(headless)}|show={int(show_window)}|runtime={int(use_runtime_copy)}"
    )
    if cache_key not in fetlife_cache:
        trace(
            2,
            f"[fetlife] loading browser profile user_data_dir={resolved_dir} profile={profile_directory} "
            f"headless={headless} show_window={show_window} runtime_copy={use_runtime_copy}",
        )
        fetlife_cache[cache_key] = FetlifeClient(
            resolved_dir,
            profile_directory=profile_directory,
            headless=headless,
            show_window=show_window,
            use_runtime_copy=use_runtime_copy,
        )
    return fetlife_cache[cache_key]


def _close_fetlife_clients(fetlife_cache: dict[str, FetlifeClient]) -> None:
    for client in fetlife_cache.values():
        client.close()


def _read_fetlife_inbox_items(client: FetlifeClient, *, max_results: int = 0, process_order: str = "desc",
                              mailbox_path: str = "/inbox/all") -> list[dict[str, Any]]:
    summaries = client.list_inbox_conversations(max_results=max_results, process_order=process_order, mailbox_path=mailbox_path)
    items: list[dict[str, Any]] = []
    for summary in summaries:
        details = client.get_conversation_details(str(summary.get("conversation_id") or ""), inbox_item=summary)
        merged = dict(summary)
        merged.update(details)
        items.append(merged)
    return items


# ----------------------------
# package-code (zip + JSONL text export)
# ----------------------------

_TAILOR_PACKAGE_JSONL_HEADER = "TAILOR_CODE_PACKAGE_JSONL_V1"
_TAILOR_REBUILD_PS1_START = "<<<TAILOR_REBUILD_FROM_PACKAGE_PS1>>>"
_TAILOR_REBUILD_PS1_END = "<<<TAILOR_REBUILD_FROM_PACKAGE_PS1_END>>>"

_TAILOR_PACKAGE_REBUILD_PS1 = r"""# TaiLOR: rebuild a directory tree from package.txt (JSON lines after TAILOR_CODE_PACKAGE_JSONL_V1).
# Usage (standalone script next to package.txt):
#   powershell -ExecutionPolicy Bypass -File .\rebuild-from-package.ps1
#   powershell -ExecutionPolicy Bypass -File .\rebuild-from-package.ps1 -PackageTxt D:\path\package.txt -Destination D:\out\restore
param(
    [Parameter(Mandatory = $false)]
    [string] $PackageTxt = "",
    [Parameter(Mandatory = $false)]
    [string] $Destination = ""
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($PackageTxt)) {
    $PackageTxt = Join-Path -Path $PSScriptRoot -ChildPath "package.txt"
}
if ([string]::IsNullOrWhiteSpace($Destination)) {
    $Destination = Join-Path -Path $PSScriptRoot -ChildPath "package-restore"
}
$utf8 = New-Object System.Text.UTF8Encoding $false
$lines = [System.IO.File]::ReadAllLines($PackageTxt, $utf8)
$start = -1
for ($i = 0; $i -lt $lines.Length; $i++) {
    if ($lines[$i] -eq "TAILOR_CODE_PACKAGE_JSONL_V1") { $start = $i + 1; break }
}
if ($start -lt 0) {
    throw "Header TAILOR_CODE_PACKAGE_JSONL_V1 not found in: $PackageTxt"
}
$destRoot = [System.IO.Path]::GetFullPath($Destination)
New-Item -ItemType Directory -Force -Path $destRoot | Out-Null
$count = 0
for ($i = $start; $i -lt $lines.Length; $i++) {
    $line = $lines[$i].TrimEnd()
    if ([string]::IsNullOrWhiteSpace($line)) { continue }
    $obj = $null
    try {
        $obj = $line | ConvertFrom-Json
    } catch {
        throw "Invalid JSON on line $($i + 1): $($_.Exception.Message)"
    }
    $rel = [string]$obj.path
    if ([string]::IsNullOrWhiteSpace($rel)) { continue }
    $relNorm = ($rel.Trim() -replace "\\", "/").TrimStart("/")
    if ($relNorm -match "(^|/)\.\.(/|$)") {
        throw "Unsafe path (..): $rel"
    }
    $accum = $destRoot
    foreach ($seg in $relNorm.Split("/")) {
        if (-not [string]::IsNullOrWhiteSpace($seg)) {
            $accum = Join-Path -Path $accum -ChildPath $seg
        }
    }
    $full = [System.IO.Path]::GetFullPath($accum)
    $sep = [char][System.IO.Path]::DirectorySeparatorChar
    $ok = ($full -eq $destRoot) -or ($full.StartsWith($destRoot + $sep, [System.StringComparison]::OrdinalIgnoreCase))
    if (-not $ok) {
        throw "Path escapes destination: $rel -> $full"
    }
    $parent = Split-Path -Parent $full
    if (-not [string]::IsNullOrWhiteSpace($parent) -and -not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    $b64 = [string]$obj.b64
    $bytes = if ([string]::IsNullOrEmpty($b64)) { New-Object byte[] 0 } else { [Convert]::FromBase64String($b64) }
    [System.IO.File]::WriteAllBytes($full, $bytes)
    $count++
}
Write-Host "Restored $count file(s) under $destRoot"
"""


def _package_code_extra_glob_match(rel_posix: str, pattern: str) -> bool:
    p = pattern.replace("\\", "/").strip()
    if not p:
        return False
    base = rel_posix.rsplit("/", 1)[-1]
    return fnmatch.fnmatch(rel_posix, p) or fnmatch.fnmatch(base, p)


def _package_code_dir_prune(
    dir_name: str,
    parent_rel: Path,
    *,
    root: Path,
    out_dir: Path,
    skip_heavy: bool,
) -> bool:
    """Return False if this child directory should not be descended into."""
    child_rel = parent_rel / dir_name
    parts = child_rel.parts
    if dir_name in {".git", "__pycache__", "node_modules", ".venv", "venv"}:
        return False
    if skip_heavy:
        if len(parts) == 1 and parts[0] in {
            "exec-logs",
            "task-history",
            "performance-metrics",
            "conversation-history",
            "artifacts",
            "logs",
        }:
            return False
        if len(parts) == 3 and parts[0] == "agents" and parts[2] == "history":
            return False
    child = root / child_rel
    try:
        c_res, o_res = child.resolve(), out_dir.resolve()
        if c_res == o_res or o_res in c_res.parents:
            return False
    except (OSError, ValueError):
        return False
    return True


def _package_code_should_skip(
    rel: Path,
    *,
    root: Path,
    out_dir: Path,
    skip_heavy: bool,
    extra_globs: list[str],
) -> str | None:
    rel_posix = rel.as_posix()
    try:
        full = (root / rel).resolve()
        out_resolved = out_dir.resolve()
        if full == out_resolved or out_resolved in full.parents:
            return "output directory"
    except (OSError, ValueError):
        return "unresolvable path"
    parts = rel_posix.split("/")
    if ".git" in parts:
        return ".git"
    if "__pycache__" in parts:
        return "__pycache__"
    if parts and parts[-1].endswith(".pyc"):
        return "*.pyc"
    top = parts[0] if parts else ""
    if top in {".venv", "venv", "node_modules"}:
        return top
    if skip_heavy:
        if top in {
            "exec-logs",
            "task-history",
            "performance-metrics",
            "conversation-history",
            "artifacts",
            "logs",
        }:
            return f"heavy:{top}"
        if len(parts) >= 3 and parts[0] == "agents" and parts[2] == "history":
            return "agents/.../history"
    for g in extra_globs:
        if _package_code_extra_glob_match(rel_posix, g):
            return f"exclude-glob:{g}"
    return None


def cmd_package_code(args: argparse.Namespace, project: TailorProject) -> int:
    root = project.root.resolve()
    out_rel = Path(str(getattr(args, "package_code_output_dir", "artifacts/package-code") or "artifacts/package-code").strip() or "artifacts/package-code")
    out_dir = (root / out_rel).resolve()
    skip_heavy = not bool(getattr(args, "package_code_include_heavy", False))
    raw_globs = getattr(args, "package_code_exclude_glob", None)
    extra_globs = list(raw_globs) if raw_globs else []
    max_mb = float(getattr(args, "package_code_max_file_mb", 64.0) or 64.0)
    max_bytes = max(1, int(max_mb * 1024 * 1024))
    dry = bool(getattr(args, "package_code_dry_run", False))

    entries: list[tuple[str, bytes]] = []
    skipped: list[tuple[str, str]] = []
    for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        current = Path(dirpath)
        try:
            parent_rel = current.relative_to(root)
        except ValueError:
            continue
        dirnames[:] = [
            d
            for d in sorted(dirnames)
            if _package_code_dir_prune(d, parent_rel, root=root, out_dir=out_dir, skip_heavy=skip_heavy)
        ]
        for name in sorted(filenames):
            path = current / name
            if not path.is_file():
                continue
            if path.is_symlink():
                skipped.append((path.relative_to(root).as_posix(), "symlink"))
                continue
            try:
                rel = path.relative_to(root)
            except ValueError:
                continue
            reason = _package_code_should_skip(rel, root=root, out_dir=out_dir, skip_heavy=skip_heavy, extra_globs=extra_globs)
            if reason:
                skipped.append((rel.as_posix(), reason))
                continue
            try:
                size = path.stat().st_size
            except OSError as ex:
                skipped.append((rel.as_posix(), f"stat:{ex}"))
                continue
            if size > max_bytes:
                skipped.append((rel.as_posix(), f"max_file_mb>{max_mb}"))
                continue
            try:
                data = path.read_bytes()
            except OSError as ex:
                skipped.append((rel.as_posix(), f"read:{ex}"))
                continue
            entries.append((rel.as_posix(), data))
    entries.sort(key=lambda t: t[0])

    trace(2, f"[package-code] include {len(entries)} file(s), skipped {len(skipped)}")
    if dry:
        print(f"package-code dry-run: would include {len(entries)} file(s) under {root}")
        for rel, _data in entries[:200]:
            print(f"  + {rel}")
        if len(entries) > 200:
            print(f"  ... and {len(entries) - 200} more")
        print(f"Skipped {len(skipped)} path(s) (showing up to 50):")
        for rel, why in skipped[:50]:
            print(f"  - {rel}  ({why})")
        if len(skipped) > 50:
            print(f"  ... and {len(skipped) - 50} more")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / "package.zip"
    txt_path = out_dir / "package.txt"
    ps1_path = out_dir / "rebuild-from-package.ps1"

    ps1_path.write_text(_TAILOR_PACKAGE_REBUILD_PS1, encoding="utf-8", newline="\r\n")

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel, data in entries:
            zf.writestr(rel.replace("\\", "/"), data)

    banner_lines = [
        "# TaiLOR package-code export",
        f"# Project root: {root}",
        f"# Generated (UTC): {dt.datetime.now(dt.timezone.utc).isoformat()}",
        f"# Files: {len(entries)}",
        "#",
        "# This file contains an embedded PowerShell script (between the markers below) and then JSONL records.",
        "# Each line after TAILOR_CODE_PACKAGE_JSONL_V1 is one JSON object:",
        '#   {"path":"relative/path","b64":"<standard base64>"}',
        "# Rebuild: run rebuild-from-package.ps1 from this folder, or extract the script between markers and run it with -PackageTxt.",
        "#",
    ]
    with txt_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(banner_lines) + "\n\n")
        f.write(_TAILOR_REBUILD_PS1_START + "\n")
        f.write(_TAILOR_PACKAGE_REBUILD_PS1)
        if not _TAILOR_PACKAGE_REBUILD_PS1.endswith("\n"):
            f.write("\n")
        f.write(_TAILOR_REBUILD_PS1_END + "\n\n")
        f.write(_TAILOR_PACKAGE_JSONL_HEADER + "\n")
        for rel, data in entries:
            line = json.dumps(
                {"path": rel, "b64": base64.standard_b64encode(data).decode("ascii")},
                ensure_ascii=False,
                separators=(",", ":"),
            )
            f.write(line + "\n")

    print(f"Wrote {zip_path} ({len(entries)} files)")
    print(f"Wrote {txt_path}")
    print(f"Wrote {ps1_path}")
    print("Tip: expand package.zip for a fast full restore; use rebuild-from-package.ps1 when you only have package.txt.")
    if skipped:
        print(f"Skipped {len(skipped)} path(s); run with --verbose for trace or use dry-run to list.")
    return 0


# ----------------------------
# DB-oriented operations
# ----------------------------

def cmd_project_summary(args: argparse.Namespace, project: TailorProject) -> int:
    prompts = project.prompt_texts()
    workflows = project.load_workflows()
    print(f"Project root: {project.root}")
    print(f"Database: {project.db_path} ({'exists' if project.db_path.exists() else 'missing'})")
    print(f"Prompts: {len(prompts)}")
    print(f"Workflows in task-workflows.json: {len(workflows)}")
    for wf in workflows:
        print(f"  - {wf.get('Name', '<unnamed>')} [{wf.get('Id', '?')[:8]}] actions={len(wf.get('Actions', []))}")
    return 0


def cmd_agent_create(args: argparse.Namespace, project: TailorProject) -> int:
    aid = new_id()
    created = now_iso()
    with project.connect() as conn:
        conn.execute(
            """
            INSERT INTO Agent
            (Id, Name, PersonaId, PromptDocumentId, IsActive, Notes, SpecialInstructions,
             PreviousAction, PreviousActionUtc, CurrentAction, CurrentActionUtc,
             NextAction, NextActionUtc, BehaviorsJson, GoalsAndMotivations, StrategiesAndTactics,
             CreatedUtc, UpdatedUtc)
            VALUES (?, ?, ?, ?, 1, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?)
            """,
            (
                aid,
                args.name,
                args.persona_id,
                args.prompt_document_id,
                args.notes,
                args.special_instructions,
                args.behaviors_json,
                args.goals,
                args.strategies,
                created,
                created,
            ),
        )
        conn.commit()
    print(f"Created agent: {aid} ({args.name})")
    return 0


def cmd_agent_list(args: argparse.Namespace, project: TailorProject) -> int:
    with project.connect() as conn:
        rows = conn.execute(
            "SELECT Id, Name, PersonaId, IsActive, CreatedUtc FROM Agent ORDER BY CreatedUtc DESC"
        ).fetchall()
    if not rows:
        print("No agents found.")
        return 0
    for r in rows:
        print(f"{r['Id'][:8]}  active={r['IsActive']}  persona={r['PersonaId'][:8]}  name={r['Name']}")
    return 0


def cmd_channel_gmail_add(args: argparse.Namespace, project: TailorProject) -> int:
    cid = new_id()
    with project.connect() as conn:
        conn.execute(
            """
            INSERT INTO CommunicationChannel
            (Id, PersonaId, ChannelType, Provider, Address, CredentialKeyRef,
             GmailOAuthClientId, GmailOAuthClientSecret, GmailOAuthRefreshToken,
             ConfigJson, IsActive, LastCheckedUtc, CreatedUtc)
            VALUES (?, ?, 'Email', 'Gmail', ?, ?, ?, ?, ?, ?, 1, NULL, ?)
            """,
            (
                cid,
                args.persona_id,
                args.address,
                args.credential_key_ref,
                args.gmail_client_id,
                args.gmail_client_secret,
                args.gmail_refresh_token,
                args.config_json,
                now_iso(),
            ),
        )
        conn.commit()
    print(f"Created Gmail channel: {cid} for {args.address}")
    return 0


def cmd_channel_gmail_oauth(args: argparse.Namespace, project: TailorProject) -> int:
    requested_scopes = _expand_gmail_scopes(args.scopes) if args.scopes and args.scopes.strip() else []
    oauth_scopes = requested_scopes or list(DEFAULT_GMAIL_OAUTH_SCOPES)
    with project.connect() as conn:
        row = _load_channel_oauth_row(conn, channel_id=args.channel_id, address=args.address)
        client_id = row["GmailOAuthClientId"]
        client_secret = row["GmailOAuthClientSecret"]
        existing_refresh_token = row["GmailOAuthRefreshToken"]
        if not client_id or not client_secret:
            raise RuntimeError(
                f"Channel {row['Address']} is missing Gmail client id/client secret. "
                "Populate those fields first, then rerun OAuth."
            )

        if existing_refresh_token and not getattr(args, "always_refresh", False):
            try:
                trace(2, f"[gmail-oauth] validating existing refresh token for {row['Address']}")
                token_payload = _refresh_google_access_token(client_id, client_secret, existing_refresh_token)
                access_token = token_payload.get("access_token")
                if not access_token:
                    raise RuntimeError(f"No access_token in response: {token_payload}")
                granted_scopes = _fetch_google_token_scopes(str(access_token))
                if not requested_scopes or _gmail_scopes_satisfied(granted_scopes, requested_scopes):
                    print(f"Existing Gmail refresh token is valid for channel {row['Id']} ({row['Address']}).")
                    if granted_scopes:
                        print("Granted scope:")
                        print(f"  {' '.join(granted_scopes)}")
                    return 0
                print("Existing Gmail refresh token is valid but missing one or more requested scopes.")
                print("Acquiring a new refresh token with the requested scopes.")
            except Exception as ex:
                print(f"Existing Gmail refresh token could not be reused: {ex}")
                print("Acquiring a new refresh token.")

        result = _acquire_gmail_refresh_token(
            client_id,
            client_secret,
            oauth_scopes,
            timeout_seconds=args.timeout_seconds,
            open_browser=not args.no_browser,
        )

        if not result.refresh_token:
            raise RuntimeError(
                "Google did not return a refresh token. Revoke TaiLOR access in your Google account and retry with consent."
            )

        _save_channel_refresh_token(conn, row["Id"], result.refresh_token)

    print(f"Saved new Gmail refresh token for channel {row['Id']} ({row['Address']}).")
    if result.scope:
        print("Granted scope:")
        print(f"  {result.scope}")
    return 0


def _load_channel_creds(conn: sqlite3.Connection, channel_id: str) -> tuple[str, GmailCredentials]:
    row = conn.execute(
        "SELECT Address, GmailOAuthClientId, GmailOAuthClientSecret, GmailOAuthRefreshToken FROM CommunicationChannel WHERE Id = ?",
        (channel_id,),
    ).fetchone()
    if not row:
        raise RuntimeError(f"Channel not found: {channel_id}")
    vals = [row["GmailOAuthClientId"], row["GmailOAuthClientSecret"], row["GmailOAuthRefreshToken"]]
    if not all(vals):
        raise RuntimeError("Channel missing Gmail OAuth fields. Set client id/secret/refresh token first.")
    return row["Address"], GmailCredentials(vals[0], vals[1], vals[2])


def _load_channel_creds_by_address(conn: sqlite3.Connection, address: str) -> tuple[str, GmailCredentials]:
    row = conn.execute(
        """
        SELECT Address, GmailOAuthClientId, GmailOAuthClientSecret, GmailOAuthRefreshToken
        FROM CommunicationChannel
        WHERE lower(Address) = lower(?)
        ORDER BY CreatedUtc ASC
        LIMIT 1
        """,
        (address,),
    ).fetchone()
    if not row:
        raise RuntimeError(f"CommunicationChannel not found for address: {address}")
    vals = [row["GmailOAuthClientId"], row["GmailOAuthClientSecret"], row["GmailOAuthRefreshToken"]]
    if not all(vals):
        raise RuntimeError(f"Channel for {address} is missing Gmail OAuth fields.")
    return row["Address"], GmailCredentials(vals[0], vals[1], vals[2])


def _load_channel_oauth_row(conn: sqlite3.Connection, *, channel_id: str | None = None, address: str | None = None) -> sqlite3.Row:
    if channel_id:
        row = conn.execute(
            """
            SELECT Id, Address, GmailOAuthClientId, GmailOAuthClientSecret, GmailOAuthRefreshToken
            FROM CommunicationChannel
            WHERE Id = ? OR Id LIKE ?
            ORDER BY CASE WHEN Id = ? THEN 0 ELSE 1 END, CreatedUtc ASC
            LIMIT 1
            """,
            (channel_id, f"{channel_id}%", channel_id),
        ).fetchone()
        if row:
            return row
        raise RuntimeError(f"Channel not found: {channel_id}")
    if address:
        row = conn.execute(
            """
            SELECT Id, Address, GmailOAuthClientId, GmailOAuthClientSecret, GmailOAuthRefreshToken
            FROM CommunicationChannel
            WHERE lower(Address) = lower(?)
            ORDER BY CreatedUtc ASC
            LIMIT 1
            """,
            (address,),
        ).fetchone()
        if row:
            return row
        raise RuntimeError(f"CommunicationChannel not found for address: {address}")
    raise RuntimeError("Either channel_id or address is required.")


def _save_channel_refresh_token(conn: sqlite3.Connection, channel_id: str, refresh_token: str) -> None:
    conn.execute(
        "UPDATE CommunicationChannel SET GmailOAuthRefreshToken = ? WHERE Id = ?",
        (refresh_token, channel_id),
    )
    conn.commit()


def _acquire_gmail_refresh_token(client_id: str, client_secret: str, scopes: list[str], *, timeout_seconds: int, open_browser: bool) -> GmailOAuthTokenResult:
    state = uuid.uuid4().hex
    server = _OAuthCallbackServer(("127.0.0.1", 0))
    redirect_uri = f"http://127.0.0.1:{server.server_port}/"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        scope_value = " ".join(scopes)
        authorize_url = (
            "https://accounts.google.com/o/oauth2/v2/auth"
            + "?client_id=" + urllib.parse.quote(client_id, safe="")
            + "&redirect_uri=" + urllib.parse.quote(redirect_uri, safe="")
            + "&response_type=code"
            + "&scope=" + urllib.parse.quote(scope_value, safe="")
            + "&access_type=offline"
            + "&prompt=consent"
            + "&include_granted_scopes=false"
            + "&state=" + urllib.parse.quote(state, safe="")
        )

        print("Google OAuth URL:")
        print(authorize_url)
        print("")
        print("Requested scopes:")
        for scope in scopes:
            print(f"  - {scope}")
        print("")
        print(f"Waiting for OAuth callback on {redirect_uri}")

        if open_browser:
            if not webbrowser.open(authorize_url):
                print("Browser did not open automatically. Open the URL above manually.")

        if not server.callback_event.wait(timeout_seconds):
            raise RuntimeError("Timed out waiting for the Gmail OAuth callback.")

        returned_state = server.callback_params.get("state")
        code = server.callback_params.get("code")
        error = server.callback_params.get("error")

        if error:
            raise RuntimeError(f"Google OAuth returned error: {error}")
        if returned_state != state:
            raise RuntimeError("Google OAuth state mismatch.")
        if not code:
            raise RuntimeError("Google OAuth did not return an authorization code.")

        return _exchange_google_auth_code(client_id, client_secret, code, redirect_uri)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _default_ai_model_from_db(conn: sqlite3.Connection, model_fallback: str) -> str:
    row = conn.execute(
        "SELECT ModelName FROM AiProvider WHERE IsActive = 1 ORDER BY FallbackOrder ASC, CreatedUtc ASC LIMIT 1"
    ).fetchone()
    return row["ModelName"] if row and row["ModelName"] else model_fallback


def _render_email_workflow_prompt(wf_name: str, msg: dict[str, str], prompts: dict[str, str]) -> str:
    analyst = prompts.get("email_analyst", "Analyze the inbound email and recommend reply strategy.")
    reply = prompts.get("reply_email_prompt", "Draft a concise, helpful email reply.")
    return (
        f"Workflow: {wf_name}\n"
        f"From: {msg.get('from', '')}\n"
        f"Date: {msg.get('date', '')}\n"
        f"Subject: {msg.get('subject', '')}\n\n"
        f"Body:\n{msg.get('body', '')}\n\n"
        "Instructions:\n"
        f"1) {analyst}\n"
        f"2) {reply}\n"
        "Return JSON with keys: analysis, sentiment, priority, reply_subject, reply_body."
    )


def cmd_gmail_monitor(args: argparse.Namespace, project: TailorProject) -> int:
    with project.connect() as conn:
        _, creds = _load_channel_creds(conn, args.channel_id)
        default_model = _default_ai_model_from_db(conn, args.model)
    gmail = GmailClient(creds)
    ai = AiClient(model=args.model or default_model, api_key=args.api_key, endpoint=args.ai_endpoint)
    prompts = project.prompt_texts()

    print("Starting Gmail monitor. Press Ctrl+C to stop.")
    seen: set[str] = set()
    try:
        while True:
            msgs = gmail.list_inbox(max_results=args.max_results, query=args.query)
            new_msgs = [m for m in msgs if m.get("id") and m["id"] not in seen]
            for m in reversed(new_msgs):
                mid = m["id"]
                summary = gmail.get_message_summary(mid)
                print(f"\n[{mid[:8]}] {summary['from']} :: {summary['subject']}")

                prompt = _render_email_workflow_prompt("gmail-monitor", summary, prompts)
                response = ai.chat(
                    system="You are TaiLOR's email automation analyst. Respond only in strict JSON.",
                    user=prompt,
                    temperature=args.temperature,
                    max_tokens=args.max_tokens,
                )
                try:
                    obj = json.loads(response)
                except json.JSONDecodeError:
                    obj = {"analysis": response, "reply_subject": f"Re: {summary['subject']}", "reply_body": response}

                if args.dry_run:
                    print("DRY RUN: proposed reply subject:", obj.get("reply_subject"))
                    print("DRY RUN: proposed reply body:\n", obj.get("reply_body"))
                else:
                    subject = obj.get("reply_subject") or f"Re: {summary['subject']}"
                    body = obj.get("reply_body") or obj.get("analysis") or "Thanks for your email."
                    send_result = _send_gmail_message_checked(
                        project,
                        gmail,
                        to_email=_extract_sender_email(summary["from"]),
                        subject=subject,
                        body=body,
                        source_label="gmail-monitor",
                    )
                    if send_result.get("blocked"):
                        print("Reply blocked by last-chance review. Draft saved and user notification added.")
                    else:
                        print("Reply sent.")
                    if args.archive_after_send and send_result.get("sent"):
                        gmail.mark_read_and_archive(mid)

                seen.add(mid)

            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print("Stopped.")
        return 0


def _fetlife_user_data_dir_from_args(args: argparse.Namespace, project: TailorProject) -> Path:
    raw_value = str(getattr(args, "fetlife_user_data_dir", "") or "").strip()
    return Path(raw_value) if raw_value else _default_fetlife_user_data_dir(project)


def _fetlife_headless_from_args(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "fetlife_headless", False))


def _fetlife_visible_from_args(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "fetlife_headed", False))


def _fetlife_mailbox_path_from_args(args: argparse.Namespace) -> str:
    value = str(getattr(args, "fetlife_mailbox_path", "") or "").strip()
    if not value:
        return "/inbox/all"
    if value.startswith("http://") or value.startswith("https://"):
        parsed = urllib.parse.urlparse(value)
        value = parsed.path or "/inbox/all"
    if not value.startswith("/"):
        value = "/" + value
    return value


def _fetlife_runtime_copy_from_args(args: argparse.Namespace) -> bool:
    return not bool(getattr(args, "fetlife_no_runtime_copy", False))


def _add_fetlife_browser_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--fetlife-user-data-dir", help="Saved Chrome user-data-dir snapshot to use for FetLife browsing")
    parser.add_argument("--fetlife-profile-directory", default="Profile 9")
    parser.add_argument("--fetlife-headless", action="store_true", help="Attempt true headless FetLife browser automation")
    parser.add_argument("--fetlife-headed", action="store_true", help="Run FetLife browser automation with a visible browser window")
    parser.add_argument("--fetlife-no-runtime-copy", action="store_true", help="Use the saved profile directory directly instead of a temporary runtime copy")
    parser.add_argument("--fetlife-mailbox-path", default="/inbox/all", help="FetLife mailbox path to read, e.g. /inbox/all or /inbox/archived")


def cmd_fetlife_list_inbox(args: argparse.Namespace, project: TailorProject) -> int:
    fetlife_cache: dict[str, FetlifeClient] = {}
    try:
        client = _ensure_fetlife_client(
            project,
            fetlife_cache,
            user_data_dir=_fetlife_user_data_dir_from_args(args, project),
            profile_directory=str(getattr(args, "fetlife_profile_directory", "Profile 9") or "Profile 9"),
            headless=_fetlife_headless_from_args(args),
            show_window=_fetlife_visible_from_args(args),
            use_runtime_copy=_fetlife_runtime_copy_from_args(args),
        )
        items = _read_fetlife_inbox_items(
            client,
            max_results=max(0, int(getattr(args, "max_results", 0) or 0)),
            process_order=str(getattr(args, "process_order", "desc") or "desc"),
            mailbox_path=_fetlife_mailbox_path_from_args(args),
        )
        if getattr(args, "output_json", None):
            Path(args.output_json).write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")
        if not items:
            print("No FetLife inbox conversations found.")
            return 0
        for index, item in enumerate(items, start=1):
            print(
                _console_safe_text(
                    f"{index}. {item.get('conversation_id')} | {item.get('participant_name') or '(unknown)'} | "
                    f"{item.get('subject') or '<no subject>'} | needs_reply={bool(item.get('needs_reply'))}"
                )
            )
            print(_console_safe_text(f"   Last message id: {item.get('last_message_id') or ''}"))
            print(_console_safe_text(f"   Last author: {item.get('last_message_author') or ''}"))
            print(_console_safe_text(f"   Last at: {item.get('last_message_created_at') or ''}"))
            print(_console_safe_text(f"   Preview: {trace_excerpt(item.get('last_message_body') or item.get('snippet') or '', 180)}"))
        return 0
    finally:
        _close_fetlife_clients(fetlife_cache)


def cmd_fetlife_fetch_thread(args: argparse.Namespace, project: TailorProject) -> int:
    fetlife_cache: dict[str, FetlifeClient] = {}
    try:
        client = _ensure_fetlife_client(
            project,
            fetlife_cache,
            user_data_dir=_fetlife_user_data_dir_from_args(args, project),
            profile_directory=str(getattr(args, "fetlife_profile_directory", "Profile 9") or "Profile 9"),
            headless=_fetlife_headless_from_args(args),
            show_window=_fetlife_visible_from_args(args),
            use_runtime_copy=_fetlife_runtime_copy_from_args(args),
        )
        details = client.get_conversation_details(str(args.conversation_id))
        text = str(details.get("thread_text") or "").strip()
        if getattr(args, "output_file", None):
            Path(args.output_file).write_text(text + ("\n" if text else ""), encoding="utf-8")
        print(text)
        return 0
    finally:
        _close_fetlife_clients(fetlife_cache)


def cmd_fetlife_send_message(args: argparse.Namespace, project: TailorProject) -> int:
    fetlife_cache: dict[str, FetlifeClient] = {}
    try:
        client = _ensure_fetlife_client(
            project,
            fetlife_cache,
            user_data_dir=_fetlife_user_data_dir_from_args(args, project),
            profile_directory=str(getattr(args, "fetlife_profile_directory", "Profile 9") or "Profile 9"),
            headless=_fetlife_headless_from_args(args),
            show_window=_fetlife_visible_from_args(args),
            use_runtime_copy=_fetlife_runtime_copy_from_args(args),
        )
        body = str(args.body or "").strip()
        review = _review_outbound_email_text("", body)
        if review["blocked"]:
            notification = _append_user_notification(
                project,
                title="Blocked outbound FetLife message",
                message=(
                    "A FetLife reply was blocked by the last-chance review because it still contains placeholder-like text."
                ),
                category="fetlife_send_blocked",
                details={
                    "conversation_id": str(args.conversation_id),
                    "review": review,
                },
            )
            print(f"Blocked by last-chance review. Notification saved: {notification['id']}")
            return 1
        if getattr(args, "dry_run", False):
            print(body)
            return 0
        result = client.send_message(str(args.conversation_id), body)
        if getattr(args, "output_file", None):
            Path(args.output_file).write_text(f"Body:\n{body}\n", encoding="utf-8")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("sent") else 1
    finally:
        _close_fetlife_clients(fetlife_cache)


def cmd_download_fetlife_message_history(args: argparse.Namespace, project: TailorProject) -> int:
    output_root = Path(
        str(getattr(args, "output_root", "") or (project.root / "artifacts" / "fetlife-message-history"))
    )
    fetlife_cache: dict[str, FetlifeClient] = {}
    try:
        client = _ensure_fetlife_client(
            project,
            fetlife_cache,
            user_data_dir=_fetlife_user_data_dir_from_args(args, project),
            profile_directory=str(getattr(args, "fetlife_profile_directory", "Profile 9") or "Profile 9"),
            headless=_fetlife_headless_from_args(args),
            show_window=_fetlife_visible_from_args(args),
            use_runtime_copy=_fetlife_runtime_copy_from_args(args),
        )
        items = _read_fetlife_inbox_items(
            client,
            max_results=max(0, int(getattr(args, "max_results", 0) or 0)),
            process_order=str(getattr(args, "process_order", "desc") or "desc"),
            mailbox_path=_fetlife_mailbox_path_from_args(args),
        )
        output_root.mkdir(parents=True, exist_ok=True)
        index_rows: list[dict[str, Any]] = []
        for item in items:
            participant_name = str(item.get("participant_name") or "").strip()
            participant_slug = str(item.get("participant_slug") or "").strip()
            folder_name = _slugify_for_filename(participant_slug or participant_name or "unknown-user")
            participant_folder = output_root / folder_name
            participant_folder.mkdir(parents=True, exist_ok=True)
            conversation_id = str(item.get("conversation_id") or "")
            thread_text = str(item.get("thread_text") or "").strip()
            thread_path = participant_folder / f"conversation_{conversation_id}.txt"
            latest_path = participant_folder / "latest.txt"
            metadata_path = participant_folder / f"conversation_{conversation_id}.json"
            thread_path.write_text(thread_text + ("\n" if thread_text else ""), encoding="utf-8")
            latest_path.write_text(thread_text + ("\n" if thread_text else ""), encoding="utf-8")
            metadata = {
                "conversation_id": conversation_id,
                "participant_name": participant_name,
                "participant_slug": participant_slug,
                "history_key": str(item.get("history_key") or ""),
                "subject": str(item.get("subject") or ""),
                "message_count": int(item.get("message_count", 0) or 0),
                "last_message_id": str(item.get("last_message_id") or ""),
                "last_message_author": str(item.get("last_message_author") or ""),
                "last_message_created_at": str(item.get("last_message_created_at") or ""),
                "downloaded_utc": now_iso(),
                "thread_file": str(thread_path),
            }
            metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
            index_rows.append({
                **metadata,
                "folder": str(participant_folder),
                "latest_file": str(latest_path),
                "metadata_file": str(metadata_path),
            })
        index_path = output_root / "index.json"
        index_path.write_text(json.dumps(index_rows, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Downloaded {len(index_rows)} FetLife conversation(s) to: {output_root}")
        print(f"Index: {index_path}")
        for row in index_rows[:20]:
            print(
                _console_safe_text(
                    f"- {row['participant_name'] or row['participant_slug'] or '(unknown)'} | "
                    f"{row['conversation_id']} | {row['subject']} -> {row['thread_file']}"
                )
            )
        if len(index_rows) > 20:
            print(f"... {len(index_rows) - 20} more conversation(s)")
        return 0
    finally:
        _close_fetlife_clients(fetlife_cache)


def _extract_sender_email(from_header: str) -> str:
    # supports 'Name <user@example.com>' and raw address
    m = re.search(r"<([^>]+)>", from_header)
    return (m.group(1) if m else from_header).strip()


def _normalize_message_id_header(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    return text if text.startswith("<") else f"<{text.strip('<>')}>"


def _format_reply_quote(from_header: str, date_header: str, body: str) -> str:
    lines = [f"> {line}" if line else ">" for line in (body or "").splitlines()]
    quoted_body = "\n".join(lines).rstrip()
    display_name, display_email = email.utils.parseaddr(from_header or "")
    sender_text = display_name or display_email or from_header or "the sender"
    intro = f"On {date_header or 'the previous message'}, {sender_text} wrote:"
    return f"{intro}\n{quoted_body}".rstrip()


def _build_reply_body(reply_body: str, from_header: str, date_header: str, original_body: str) -> str:
    cleaned_reply = (reply_body or "").rstrip()
    quote_block = _format_reply_quote(from_header, date_header, original_body)
    if not cleaned_reply:
        return quote_block
    return f"{cleaned_reply}\n\n{quote_block}"


def cmd_workflow_list(args: argparse.Namespace, project: TailorProject) -> int:
    tasks = project.load_workflows()
    for t in tasks:
        print(f"{t.get('Id', '')[:8]}  {t.get('Name', '<unnamed>')}  actions={len(t.get('Actions', []))}")
        for a in t.get("Actions", []):
            print(f"   - {a.get('$type', '<type>')} :: {a.get('Name', '<name>')}")
    return 0


def cmd_workflow_run(args: argparse.Namespace, project: TailorProject) -> int:
    tasks = project.load_workflows()
    task = next((t for t in tasks if t.get("Id", "").startswith(args.workflow) or t.get("Name") == args.workflow), None)
    if not task:
        raise RuntimeError(f"Workflow not found by id/name: {args.workflow}")
    if getattr(args, "repeat", None):
        workflow_label = str(task.get("Name") or args.workflow)
        return _schedule_workflow_task(args, project, workflow_label)

    with project.connect() as conn:
        _, creds = _load_channel_creds(conn, args.channel_id)
        model = args.model or _default_ai_model_from_db(conn, "gpt-4o-mini")
    gmail = GmailClient(creds)
    ai = AiClient(model=model, api_key=args.api_key, endpoint=args.ai_endpoint)

    summary = {
        "subject": args.subject,
        "from": args.from_email,
        "date": email.utils.format_datetime(dt.datetime.now(tz=UTC)),
        "body": args.body,
    }
    prompts = project.prompt_texts()
    prompt = _render_email_workflow_prompt(task.get("Name", "workflow"), summary, prompts)

    ai_text = ai.chat(
        system="You execute TaiLOR workflow actions. Return JSON with reply_subject and reply_body.",
        user=prompt,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
    )
    try:
        obj = json.loads(ai_text)
    except Exception:
        obj = {"reply_subject": f"Re: {args.subject}", "reply_body": ai_text}

    if args.dry_run:
        print(json.dumps(obj, indent=2))
    else:
        send_result = _send_gmail_message_checked(
            project,
            gmail,
            to_email=args.reply_to,
            subject=obj.get("reply_subject", f"Re: {args.subject}"),
            body=obj.get("reply_body", ai_text),
            source_label=f"workflow-run:{task.get('Name', 'workflow')}",
        )
        if send_result.get("blocked"):
            print("Workflow reply blocked by last-chance review. Draft saved and user notification added.")
        else:
            print("Workflow reply sent.")
    return 0


def _read_recipients(path: Path) -> list[str]:
    ensure_file(path, "Recipients file")
    if path.suffix.lower() == ".csv":
        rows: list[str] = []
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for key in ("email", "Email", "address", "Address"):
                    val = (row.get(key) or "").strip()
                    if val:
                        rows.append(val)
                        break
        return rows
    lines = [ln.strip() for ln in path.read_text(encoding="utf-8", errors="replace").splitlines()]
    return [ln for ln in lines if ln and "@" in ln]


def _bulk_send(
    project: TailorProject,
    gmail: GmailClient,
    recipients: list[str],
    subject: str,
    body: str,
    *,
    dry_run: bool,
    throttle_seconds: float,
) -> tuple[int, int]:
    sent = 0
    failed = 0
    review = _review_outbound_email_text(subject, body)
    if review["blocked"]:
        first_recipient = recipients[0] if recipients else ""
        send_result = _send_gmail_message_checked(
            project,
            gmail,
            to_email=first_recipient,
            subject=subject,
            body=body,
            source_label="send_email_list",
            dry_run=dry_run,
            recipient_count=len(recipients),
        )
        if send_result.get("blocked"):
            print("BLOCKED [0/{0}] -> placeholder-like text detected; draft saved and user notification added.".format(len(recipients)))
        return 0, len(recipients)
    for i, recipient in enumerate(recipients, start=1):
        try:
            if dry_run:
                print(f"DRY RUN [{i}/{len(recipients)}] -> {recipient} :: {subject}")
            else:
                send_result = _send_gmail_message_checked(
                    project,
                    gmail,
                    to_email=recipient,
                    subject=subject,
                    body=body,
                    source_label="send_email_list",
                )
                if send_result.get("blocked"):
                    print(f"BLOCKED [{i}/{len(recipients)}] -> {recipient} :: draft saved and user notification added.")
                    failed += 1
                    break
                print(f"SENT [{i}/{len(recipients)}] -> {recipient}")
            sent += 1
            if throttle_seconds > 0:
                time.sleep(throttle_seconds)
        except Exception as ex:
            failed += 1
            print(f"FAILED [{i}/{len(recipients)}] -> {recipient} :: {ex}", file=sys.stderr)
    return sent, failed


def _resolve_workflow_path(project: TailorProject, agent_key: str, workflow_ref: str) -> Path:
    raw = (workflow_ref or "").strip()
    if not raw:
        raise RuntimeError("Workflow name or path is empty.")
    tried: list[str] = []
    candidates: list[Path] = []
    p = Path(raw)
    if p.is_absolute():
        candidates.append(p)
    else:
        candidates.append(Path.cwd() / p)
        candidates.append(project.root / p)
    fname = raw if raw.lower().endswith(".json") else f"{raw}.json"
    agents_root = project.root / "agents"
    candidates.extend(
        [
            agents_root / agent_key / "workflows" / fname,
            agents_root / agent_key / fname,
            agents_root / "general" / "workflows" / fname,
            project.root / "workflows" / fname,
        ]
    )
    seen: set[str] = set()
    for c in candidates:
        try:
            resolved = c.resolve()
        except Exception:
            continue
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        tried.append(key)
        if resolved.is_file():
            return resolved
    detail = "\n  - ".join(tried) if tried else "(no paths)"
    raise FileNotFoundError(f"Workflow not found for agent {agent_key!r} ({workflow_ref!r}). Checked:\n  - {detail}")


def _inject_agent_workflow_variables(spec: dict[str, Any], project: TailorProject, agent_key: str) -> None:
    """Prepend agent_root, agent_key, and per-agent output_root (run-workflow only)."""
    agent_root = str((project.root / "agents" / agent_key).resolve())
    agent_results_root = str((project.root / "agents" / agent_key / "workflow-results").resolve())
    rows_in = _normalize_workflow_variables(spec.get("variables"))
    skip = {"agent_root", "agent_key", "output_root"}
    filtered = [(n, v) for n, v in rows_in if n not in skip]
    injected: list[tuple[str, Any]] = [
        ("agent_root", agent_root),
        ("agent_key", agent_key),
        ("output_root", agent_results_root),
    ]
    spec["variables"] = [{"name": n, "value": v} for n, v in injected + filtered]


def _workflow_step_http_timeout_seconds(
    step: dict[str, Any], context: dict[str, Any], *, base_dir: Path, args: argparse.Namespace
) -> int:
    """HTTP read timeout for AI steps (urllib); separate from model max_tokens generation time."""
    default_ts = int(getattr(args, "ai_timeout_seconds", DEFAULT_AI_CHAT_TIMEOUT))
    raw = _resolve_number(
        step.get("timeout_seconds"),
        context,
        base_dir=base_dir,
        label="timeout_seconds",
        default=default_ts,
        as_int=True,
    )
    ts = int(raw) if raw is not None else default_ts
    return max(30, ts)


def _plugin_manifest_paths(project: "TailorProject") -> list[Path]:
    root = project.root / WORKFLOW_PLUGINS_DIR
    if not root.exists():
        return []
    return sorted(path for path in root.glob("*.json") if path.is_file())


def _load_plugin_manifest(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"Plugin manifest must be an object: {path}")
    name = str(data.get("name") or "").strip()
    handler = str(data.get("handler") or "").strip()
    if not name:
        raise RuntimeError(f"Plugin manifest missing 'name': {path}")
    if not handler:
        raise RuntimeError(f"Plugin manifest missing 'handler': {path}")
    data["_manifest_path"] = str(path)
    return data


def _workflow_plugin_registry(project: "TailorProject") -> dict[str, dict[str, Any]]:
    adapter = WorkflowPluginRegistry(project.root / WORKFLOW_PLUGINS_DIR)
    registry: dict[str, dict[str, Any]] = {}
    for plugin in adapter.list():
        registry[plugin.name] = {
            "name": plugin.name,
            "display_name": plugin.display_name,
            "description": plugin.description,
            "category": plugin.category,
            "contract_version": plugin.contract_version,
            "handler": plugin.handler,
            "config_schema": plugin.config_schema,
            "_manifest_path": str(plugin.manifest_path),
        }
    return registry


def _required_workflow_plugin(step_type: str, registry: dict[str, dict[str, Any]], *, step_index: int) -> dict[str, Any]:
    plugin = registry.get(step_type)
    if plugin is None:
        raise RuntimeError(
            f"Step {step_index} references plugin '{step_type}', but no plugin manifest exists in {WORKFLOW_PLUGINS_DIR}."
        )
    return plugin


def cmd_plugin_list(args: argparse.Namespace, project: "TailorProject") -> int:
    registry = _workflow_plugin_registry(project)
    if not registry:
        print(f"No workflow plugins found under {(project.root / WORKFLOW_PLUGINS_DIR).resolve()}")
        return 0
    for name in sorted(registry):
        plugin = registry[name]
        category = str(plugin.get("category") or "general")
        display = str(plugin.get("display_name") or name)
        summary = str(plugin.get("description") or "").strip()
        print(f"{name} [{category}] - {display}")
        if summary:
            print(f"  {summary}")
    return 0


def cmd_plugin_create(args: argparse.Namespace, project: "TailorProject") -> int:
    name = _slugify_name(str(args.name))
    category = str(args.category or "custom").strip() or "custom"
    adapter = WorkflowPluginRegistry(project.root / WORKFLOW_PLUGINS_DIR)
    manifest_path = adapter.plugins_dir / f"{name}.json"
    if manifest_path.exists():
        raise RuntimeError(f"Plugin already exists: {manifest_path}")
    manifest_path = adapter.create(
        name=name,
        display_name=str(args.display_name or args.name),
        description=str(args.description or "Custom workflow plugin"),
        category=category,
        handler=str(args.handler or "legacy_step_adapter"),
    )
    print(f"Created plugin manifest: {manifest_path}")
    return 0


def cmd_workflow_compose(args: argparse.Namespace, project: "TailorProject") -> int:
    registry = _workflow_plugin_registry(project)
    requested = [item.strip() for item in str(args.plugins or "").split(",") if item.strip()]
    if not requested:
        raise RuntimeError("Provide at least one plugin via --plugins step_a,step_b")
    for plugin_name in requested:
        if plugin_name not in registry:
            raise RuntimeError(f"Unknown plugin '{plugin_name}'. Run plugin-list to view available plugins.")
    payload = WorkflowComposer.compose(workflow_type=str(args.workflow_type or "single_run"), plugin_names=requested)
    output_path = _resolve_new_output_path((project.root / str(args.output_file)).resolve())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote workflow template: {output_path}")
    return 0


def cmd_workflow_exec(args: argparse.Namespace, project: TailorProject) -> int:
    """
    Execute a JSON-defined workflow with step chaining.

    Supported top-level properties:
    - variables:
      [
        {"name":"prompt_path","value":"{{project_root}}/prompts/example.txt"},
        {"name":"prompt_text","value":"{{GetFileContents(prompt_path)}}"}
      ]

    Supported step types:
    - combine_files
    - combine_all_files_in_folder
    - load_agent_context
    - list_files_in_folder
    - count_items
    - compare_number
    - slice_items
    - combine_listed_files
    - select_business_research_target
    - extract_emails_from_value
    - append_unique_lines
    - append_to_file
    - sync_history_emails_to_file
    - file_contains_line
    - output
    - file_to_context
    - save_variable_value
    - unified_text_diff
    - build_agent_system_prompt
    - path_exists
    - regex_match
    - any_true
    - skip_remaining_steps_if
    - skip_remaining_steps_unless
    - replace_variables_in_file
    - get_next_item_in_list
    - ai_chat
    - send_input_to_API
    - send_file_to_AI
    - send_all_files_in_folder_to_AI
    - generate_list_from_API
    - extract_section_from_file
    - get_all_messages
    - get_all_messages_from_client
    - send_email
    - send_email_list
    - send_fetlife_message

    Supported workflow types:
    - single_run
    - iteration_over_list_file
    - iteration_over_gmail_inbox
    - iteration_over_fetlife_inbox
    - iteration_over_agent_history_folders
    - iteration_over_extracted_emails_from_files

    Cost estimate:
    - By default (config WorkflowCostEstimate.enabled true), workflow-exec and run-workflow print a rough OpenAI-style
      token/cost estimate and ask for confirmation before any iterations run.
    - Disable globally via appsettings WorkflowCostEstimate.enabled false, per workflow JSON with
      "workflow-cost-estimate": {"enabled": false}, env TAILOR_SKIP_WORKFLOW_COST_APPROVAL=1, or CLI --skip-cost-approval.
    """
    wf_path = Path(args.workflow_file).resolve()
    ensure_file(wf_path, "Workflow file")
    if getattr(args, "repeat", None):
        return _schedule_workflow_task(args, project, wf_path.stem)
    trace(2, f"[workflow] loading workflow file {wf_path}")
    spec = json.loads(wf_path.read_text(encoding="utf-8"))
    return _run_chained_workflow(project, wf_path, spec, args, base_dir=wf_path.parent)


def cmd_run_workflow(args: argparse.Namespace, project: TailorProject) -> int:
    """Run a workflow JSON by agent key and workflow name or path (sets agent_root, agent_key, output_root under that agent)."""
    agent_key = args.agent.strip()
    if not agent_key:
        raise RuntimeError("--agent is required.")
    wf_path = _resolve_workflow_path(project, agent_key, args.workflow)
    args.workflow_file = str(wf_path)
    if getattr(args, "repeat", None):
        return _schedule_workflow_task(args, project, wf_path.stem)
    trace(2, f"[run-workflow] agent={agent_key!r} workflow={wf_path}")
    spec = json.loads(wf_path.read_text(encoding="utf-8"))
    _inject_agent_workflow_variables(spec, project, agent_key)
    return _run_chained_workflow(project, wf_path, spec, args, base_dir=wf_path.parent)


def cmd_engine_log(args: argparse.Namespace, project: TailorProject) -> int:
    """Show recent central engine log output (all workflows / agents)."""
    root = project.engine_logs_dir.resolve()
    text_path, nd_path = _engine_log_paths(project)
    tail_n = max(0, int(getattr(args, "tail", 80)))
    print(f"Central engine log directory:\n  {root}\n")
    print("Daily files:")
    print(f"  {text_path.name}  - human-readable tail-friendly lines")
    print(f"  {nd_path.name}  - structured JSON lines")
    print(
        "  In a git checkout (or TAILOR_DEV_MODE / TAILOR_ENGINE_DEV_API_LOG), workflow runs also "
        "append dev_api_request / dev_api_response events with full bodies (see NDJSON).\n"
    )
    if not root.exists():
        print("No log directory yet (nothing recorded). Run a workflow to create it.")
        return 0
    for label, path in (("Readable log", text_path), ("NDJSON events", nd_path)):
        print(f"--- {label}: {path} ---")
        if not path.exists():
            print("(empty — no events today)\n")
            continue
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        if tail_n and len(lines) > tail_n:
            lines = lines[-tail_n:]
            print(f"(last {tail_n} lines)\n")
        for line in lines:
            print(line)
        print()
    return 0


# ----------------------------
# Workflow API cost estimate (OpenAI-style; interactive approval)
# ----------------------------

_WORKFLOW_AI_STEP_TYPES = frozenset({
    "ai_chat",
    "send_input_to_API",
    "send_file_to_AI",
    "send_all_files_in_folder_to_AI",
    "generate_list_from_API",
})


def _workflow_cost_estimate_globally_enabled(project: TailorProject) -> bool:
    settings = _load_appsettings(project)
    block = settings.get("WorkflowCostEstimate") or settings.get("workflowCostEstimate")
    if isinstance(block, dict) and "enabled" in block:
        return bool(block.get("enabled"))
    return True


def _workflow_cost_estimate_enabled_in_spec(spec: dict[str, Any]) -> bool:
    block = spec.get("workflow-cost-estimate") or spec.get("workflow_cost_estimate")
    if isinstance(block, dict) and block.get("enabled") is False:
        return False
    return True


def _workflow_skip_cost_approval_interaction(args: argparse.Namespace, project: TailorProject, spec: dict[str, Any]) -> bool:
    if getattr(args, "repeat", None):
        return True
    if bool(getattr(args, "skip_cost_approval", False)):
        return True
    if (os.environ.get("TAILOR_SKIP_WORKFLOW_COST_APPROVAL") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return True
    if not _workflow_cost_estimate_globally_enabled(project):
        return True
    if not _workflow_cost_estimate_enabled_in_spec(spec):
        return True
    return False


def _approx_llm_tokens_from_text(text: str) -> int:
    if not text:
        return 0
    return max(1, len(text) // 4)


def _openai_public_price_per_million(model_name: str) -> tuple[float, float, str]:
    """Returns (input_usd_per_1m, output_usd_per_1m, pricing_note). Approximate public list prices."""
    m = (model_name or "").strip().lower()
    if "gpt-4o-mini" in m or "4o-mini" in m:
        return (0.15, 0.60, "gpt-4o-mini-class public list")
    if "gpt-4o" in m and "mini" not in m:
        return (2.50, 10.00, "gpt-4o-class public list")
    if "gpt-4-turbo" in m or "gpt-4-turbo-preview" in m:
        return (10.00, 30.00, "gpt-4-turbo-class public list")
    if "gpt-3.5-turbo" in m or "gpt-35-turbo" in m:
        return (0.50, 1.50, "gpt-3.5-turbo-class public list")
    if "gpt-5" in m:
        return (2.50, 10.00, "unknown gpt-5 variant — using gpt-4o-tier guess")
    if "o1-mini" in m:
        return (3.00, 12.00, "o1-mini rough guess")
    if "o1" in m:
        return (15.00, 60.00, "o1 rough guess")
    return (0.15, 0.60, "unknown model — using mini-tier guess")


def _workflow_estimated_openai_usd(model_name: str, input_tokens: int, output_tokens: int) -> tuple[float, str]:
    inp_m, out_m, note = _openai_public_price_per_million(model_name)
    usd = (input_tokens / 1_000_000.0) * inp_m + (output_tokens / 1_000_000.0) * out_m
    return usd, note


def _workflow_build_sample_context_for_cost(
    workflow_type: str,
    base_context: dict[str, Any],
    iteration_items: list[Any],
    list_file_path: Path | None,
) -> dict[str, Any]:
    context = dict(base_context)
    iteration_index = 1
    current_item = iteration_items[0] if iteration_items else None
    context["iteration_index"] = iteration_index
    context["iteration_count"] = len(iteration_items)
    if current_item is not None:
        context["current_list_item"] = current_item
        context["current_item"] = current_item
        if workflow_type == "iteration_over_gmail_inbox" and isinstance(current_item, dict):
            context["current_message"] = current_item
            context["current_message_id"] = str(current_item.get("id", ""))
            context["current_message_from"] = str(current_item.get("from", ""))
            context["current_message_sender_email"] = str(current_item.get("sender_email", ""))
            context["current_message_subject"] = str(current_item.get("subject", ""))
            context["current_message_body"] = str(current_item.get("body", ""))
            context["current_message_date"] = str(current_item.get("date", ""))
            context["current_message_thread_id"] = str(current_item.get("thread_id", ""))
            context["current_message_header_id"] = str(current_item.get("message_header_id", ""))
            context["current_message_references"] = str(current_item.get("references", ""))
            context["current_message_in_reply_to"] = str(current_item.get("in_reply_to", ""))
        elif workflow_type == "iteration_over_fetlife_inbox" and isinstance(current_item, dict):
            context["current_fetlife_message"] = current_item
            context["current_fetlife_conversation_id"] = str(current_item.get("conversation_id", ""))
            context["current_fetlife_subject"] = str(current_item.get("subject", ""))
            context["current_fetlife_participant_name"] = str(current_item.get("participant_name", ""))
            context["current_fetlife_participant_slug"] = str(current_item.get("participant_slug", ""))
            context["current_fetlife_history_key"] = str(current_item.get("history_key", ""))
            context["current_fetlife_thread_text"] = str(current_item.get("thread_text", ""))
            context["current_fetlife_message_count"] = int(current_item.get("message_count", 0) or 0)
            context["current_fetlife_last_message_id"] = str(current_item.get("last_message_id", ""))
            context["current_fetlife_last_message_author"] = str(current_item.get("last_message_author", ""))
            context["current_fetlife_last_message_body"] = str(current_item.get("last_message_body", ""))
            context["current_fetlife_last_message_created_at"] = str(current_item.get("last_message_created_at", ""))
            context["current_fetlife_needs_reply"] = bool(current_item.get("needs_reply"))
            context["current_fetlife_self_nickname"] = str(current_item.get("self_nickname", ""))
        elif workflow_type == "iteration_over_agent_history_folders" and isinstance(current_item, dict):
            context["current_history_item"] = current_item
            for key, value in current_item.items():
                context[str(key)] = value
        elif workflow_type == "iteration_over_extracted_emails_from_files" and isinstance(current_item, dict):
            email_value = str(current_item.get("email") or "").strip().lower()
            first_name = str(current_item.get("first_name") or "").strip()
            context["current_email_entry"] = current_item
            context["current_item"] = email_value
            context["current_list_item"] = email_value
            context["current_email"] = email_value
            context["current_email_first_name"] = first_name
            context["current_email_contact_name"] = str(current_item.get("contact_name") or "").strip()
            context["current_email_business_name"] = str(current_item.get("business_name") or "").strip()
            context["current_email_website_url"] = str(current_item.get("website_url") or "").strip()
            context["current_email_source_file"] = str(current_item.get("source_file") or "").strip()
            context["first_name"] = first_name or "there"
    if list_file_path is not None:
        context["list_file"] = str(list_file_path)
    return context


def _workflow_iter_send_all_folder_files(step: dict[str, Any], context: dict[str, Any], base_dir: Path) -> list[Path]:
    folder = _resolve_path(step.get("folder"), context, base_dir=base_dir, required=True, label="folder")
    include_pattern = (
        _resolve_string_value(step.get("regex_filter") or step.get("include_regex"), context, base_dir=base_dir, label="include_regex")
        if (step.get("regex_filter") or step.get("include_regex"))
        else None
    )
    exclude_pattern = (
        _resolve_string_value(step.get("exclude_regex"), context, base_dir=base_dir, label="exclude_regex") if step.get("exclude_regex") else None
    )
    include_regex = _compile_regex(include_pattern, label="include_regex")
    exclude_regex = _compile_regex(exclude_pattern, label="exclude_regex")
    recursive = _coerce_bool(step.get("recursive", True), label="recursive")
    max_files = int(_resolve_number(step.get("max_files"), context, base_dir=base_dir, label="max_files", default=0, as_int=True) or 0)
    files = list(_iter_matching_text_files(folder, recursive=recursive, include_regex=include_regex, exclude_regex=exclude_regex))
    if max_files > 0:
        files = files[:max_files]
    return files


def _workflow_estimate_step_api_calls(
    project: TailorProject,
    step: dict[str, Any],
    step_idx: int,
    context: dict[str, Any],
    base_dir: Path,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    stype = str(step.get("type") or "")
    if stype not in _WORKFLOW_AI_STEP_TYPES:
        return []
    out: list[dict[str, Any]] = []
    try:
        if stype == "ai_chat":
            system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You are a helpful assistant.")
            user = _resolve_text_source(step, "user", context, base_dir=base_dir, required=False, default="")
            max_tokens = int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True))
            in_t = _approx_llm_tokens_from_text(system) + _approx_llm_tokens_from_text(user)
            out.append({
                "step": step_idx,
                "type": stype,
                "calls": 1,
                "input_tokens_est": in_t,
                "output_tokens_cap": max(1, max_tokens),
                "note": "",
            })
        elif stype == "send_input_to_API":
            system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You are a helpful assistant.")
            user = _build_ai_user_text(step, context, base_dir=base_dir, input_field="input")
            max_tokens = int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True))
            minimum_extracted_emails = int(_resolve_number(step.get("minimum_extracted_emails"), context, base_dir=base_dir, label="minimum_extracted_emails", default=0, as_int=True) or 0)
            retry_attempts_before_relaxing = int(_resolve_number(step.get("retry_attempts_before_relaxing"), context, base_dir=base_dir, label="retry_attempts_before_relaxing", default=0, as_int=True) or 0)
            default_max_attempts = retry_attempts_before_relaxing + 1 if minimum_extracted_emails > 0 and retry_attempts_before_relaxing > 0 else 1
            max_attempts = int(_resolve_number(step.get("max_attempts"), context, base_dir=base_dir, label="max_attempts", default=default_max_attempts, as_int=True) or 1)
            in_t = _approx_llm_tokens_from_text(system) + _approx_llm_tokens_from_text(user)
            ma = max(1, max_attempts)
            note = f"up to {ma} attempt(s) if retries engage"
            out.append({
                "step": step_idx,
                "type": stype,
                "calls": ma,
                "input_tokens_est": in_t * ma,
                "output_tokens_cap": max(1, max_tokens) * ma,
                "note": note,
            })
        elif stype == "send_file_to_AI":
            file_path = _resolve_path(step.get("path"), context, base_dir=base_dir, required=True, label="path")
            file_text = _load_file_text(file_path, context=context, label="cost estimate file", base_dir=base_dir)
            system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You analyze provided file content.")
            prompt = _resolve_text_source(step, "prompt", context, base_dir=base_dir, required=False, default="")
            include_path = _coerce_bool(step.get("include_file_path", True), label="include_file_path")
            payload_parts = [prompt] if prompt.strip() else []
            if include_path:
                payload_parts.append(f"File path: {file_path}")
            payload_parts.append(file_text)
            user = _compose_prompt_segments(payload_parts)
            max_tokens = int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True))
            in_t = _approx_llm_tokens_from_text(system) + _approx_llm_tokens_from_text(user)
            out.append({
                "step": step_idx,
                "type": stype,
                "calls": 1,
                "input_tokens_est": in_t,
                "output_tokens_cap": max(1, max_tokens),
                "note": str(file_path.name),
            })
        elif stype == "send_all_files_in_folder_to_AI":
            files = _workflow_iter_send_all_folder_files(step, context, base_dir)
            if not files:
                out.append({"step": step_idx, "type": stype, "calls": 0, "input_tokens_est": 0, "output_tokens_cap": 0, "note": "no files matched"})
                return out
            system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You analyze provided file content.")
            prompt = _resolve_text_source(step, "prompt", context, base_dir=base_dir, required=False, default="")
            max_tokens = int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True))
            folder = _resolve_path(step.get("folder"), context, base_dir=base_dir, required=True, label="folder")
            total_in = 0
            for file_path in files:
                file_text = _load_file_text(file_path, context=context, label="cost estimate folder file", base_dir=base_dir)
                file_context = {**context, "file_name": file_path.name, "file_path": str(file_path), "relative_path": str(file_path.relative_to(folder))}
                prompt_for_file = render_template(prompt, file_context, strict=True, label="prompt", base_dir=base_dir)
                user = _compose_prompt_segments([prompt_for_file, f"File path: {file_path.relative_to(folder)}", file_text])
                total_in += _approx_llm_tokens_from_text(system) + _approx_llm_tokens_from_text(user)
            out.append({
                "step": step_idx,
                "type": stype,
                "calls": len(files),
                "input_tokens_est": total_in,
                "output_tokens_cap": max(1, max_tokens) * len(files),
                "note": f"{len(files)} file(s)",
            })
        elif stype == "generate_list_from_API":
            system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You generate structured lists. Return only valid JSON.")
            user = _build_ai_user_text(step, context, base_dir=base_dir, input_field="input")
            max_tokens = int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True))
            in_t = _approx_llm_tokens_from_text(system) + _approx_llm_tokens_from_text(user)
            out.append({
                "step": step_idx,
                "type": stype,
                "calls": 1,
                "input_tokens_est": in_t,
                "output_tokens_cap": max(1, max_tokens),
                "note": "",
            })
    except Exception as ex:
        out.append({
            "step": step_idx,
            "type": stype,
            "calls": 1,
            "input_tokens_est": max(500, _approx_llm_tokens_from_text(json.dumps(step, default=str)[:8000])),
            "output_tokens_cap": max(1, int(getattr(args, "max_tokens", 1200) or 1200)),
            "note": f"estimate fell back (template/context not fully resolved: {ex})",
        })
    return out


def _workflow_compute_cost_estimate(
    project: TailorProject,
    wf_path: Path,
    spec: dict[str, Any],
    steps: list[Any],
    sample_context: dict[str, Any],
    base_dir: Path,
    args: argparse.Namespace,
    *,
    iteration_count: int,
    default_model: str,
) -> dict[str, Any]:
    line_items: list[dict[str, Any]] = []
    for idx, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            continue
        line_items.extend(_workflow_estimate_step_api_calls(project, step, idx, sample_context, base_dir, args))

    total_calls = 0
    total_in = 0
    total_out_cap = 0
    for row in line_items:
        total_calls += int(row.get("calls") or 0)
        total_in += int(row.get("input_tokens_est") or 0)
        total_out_cap += int(row.get("output_tokens_cap") or 0)
    mult = max(1, int(iteration_count))
    grand_in = total_in * mult
    grand_out = total_out_cap * mult
    grand_calls = total_calls * mult
    usd, price_note = _workflow_estimated_openai_usd(default_model, grand_in, grand_out)
    return {
        "workflow_file": str(wf_path),
        "model": default_model,
        "iteration_count": mult,
        "line_items": line_items,
        "per_iteration": {"api_calls": total_calls, "input_tokens_est": total_in, "output_tokens_cap_est": total_out_cap},
        "total": {
            "api_calls": grand_calls,
            "input_tokens_est": grand_in,
            "output_tokens_cap_est": grand_out,
            "estimated_usd_openai_public_list": round(usd, 4),
        },
        "pricing_note": price_note,
        "disclaimer": (
            "Rough estimate only: ~4 chars/token heuristic, assumes output fills max_tokens caps (actual usage is usually lower), "
            "and uses approximate public OpenAI list prices (Azure/resellers differ)."
        ),
    }


def _workflow_print_and_prompt_cost_approval(project: TailorProject, est: dict[str, Any]) -> bool:
    print("\n--- TaiLOR workflow API cost estimate (non-free chat completions) ---")
    print(f"Workflow: {est['workflow_file']}")
    print(f"Model: {est['model']}  ({est['pricing_note']})")
    print(f"Iterations: {est['iteration_count']}")
    total = est["total"]
    print(
        f"Total API calls (approx): {total['api_calls']}  |  "
        f"Input tokens (est): {total['input_tokens_est']:,}  |  "
        f"Output cap tokens (est): {total['output_tokens_cap_est']:,}"
    )
    print(f"Estimated cost (USD, public list prices): ~${total['estimated_usd_openai_public_list']}")
    print(est["disclaimer"])
    if est["line_items"]:
        print("Breakdown (per iteration, before multiplying by iteration count):")
        for row in est["line_items"]:
            if int(row.get("calls") or 0) <= 0:
                continue
            n = row.get("note") or ""
            print(
                f"  step {row['step']:>2} {row['type']:<32} calls={row['calls']}  "
                f"in≈{row['input_tokens_est']:,}tok  out_cap≈{row['output_tokens_cap']:,}  {n}".rstrip()
            )
    else:
        print("No billable AI steps detected in this workflow.")
    print("---")
    if not sys.stdin.isatty():
        print("(stdin is not interactive — continuing without confirmation.)\n")
        return True
    try:
        ans = input("Proceed with this workflow run? [y/N]: ").strip().lower()
    except EOFError:
        ans = ""
    if ans in {"y", "yes"}:
        return True
    return False


def _run_chained_workflow(
    project: TailorProject,
    wf_path: Path,
    spec: dict[str, Any],
    args: argparse.Namespace,
    *,
    base_dir: Path,
) -> int:
    steps = spec.get("steps", [])
    if not isinstance(steps, list) or not steps:
        raise RuntimeError("Workflow file must contain a non-empty 'steps' array.")

    _set_engine_workflow_dev_api_context(project)

    base_dir = base_dir.resolve()
    context: dict[str, Any] = {
        "project_root": str(project.root),
        "utc_now": now_iso(),
    }
    workflow_variables = _normalize_workflow_variables(spec.get("variables"))
    for name, raw_value in workflow_variables:
        context[name] = _resolve_dynamic_value(raw_value, context, base_dir=base_dir, label=f"variables.{name}")
        trace(3, f"[workflow] variable {name}={trace_excerpt(context[name], 300)}")

    default_model = args.model
    with project.connect() as conn:
        default_model = _default_ai_model_from_db(conn, args.model)
    ai = AiClient(
        model=default_model,
        api_key=args.api_key,
        endpoint=args.ai_endpoint,
        request_timeout_seconds=int(getattr(args, "ai_timeout_seconds", DEFAULT_AI_CHAT_TIMEOUT)),
    )
    gmail_cache: dict[str, GmailClient] = {}
    fetlife_cache: dict[str, FetlifeClient] = {}
    default_verbose = int(getattr(args, "verbosity", 1))

    workflow_type = str(spec.get("workflow-type", "single_run")).strip().lower()
    trace(2, f"[workflow] type={workflow_type or 'single_run'} steps={len(steps)}")
    base_context = dict(context)
    _begin_workflow_exec_logging(project, wf_path, args, spec, base_context)
    list_file_path: Path | None = None
    iteration_items: list[Any] = [None]
    if workflow_type == "iteration_over_list_file":
        list_file_path = _resolve_path(spec.get("file"), base_context, base_dir=base_dir, required=True, label="file")
        iteration_items = _read_list_items(list_file_path)
        if not iteration_items:
            raise RuntimeError(f"Workflow list file is empty: {list_file_path}")
    elif workflow_type == "iteration_over_gmail_inbox":
        from_address = _resolve_string_value(
            spec.get("from") or spec.get("from_email"),
            base_context,
            base_dir=base_dir,
            required=True,
            label="from_email",
        )
        query = _resolve_string_value(spec.get("query", "in:inbox"), base_context, base_dir=base_dir, label="query")
        max_results = int(_resolve_number(spec.get("max_results"), base_context, base_dir=base_dir, label="max_results", default=10, as_int=True))
        process_order = _resolve_string_value(spec.get("process_order", "asc"), base_context, base_dir=base_dir, label="process_order")
        archive_after_iteration = _coerce_bool(spec.get("archive_after_iteration", False), label="archive_after_iteration")
        iteration_items = _read_gmail_inbox_items(
            project,
            gmail_cache,
            from_address=from_address,
            query=query,
            max_results=max_results,
            process_order=process_order,
        )
        if not iteration_items:
            print("No Gmail inbox messages matched the workflow query.")
            _append_active_exec_log(
                event_type="workflow_complete",
                title="Workflow completed with no matching inbox messages",
                body=f"Workflow file: {wf_path}\nQuery: {query}\nFrom: {from_address}",
            )
            if args.output_context_json:
                _write_output_context_json(args.output_context_json, base_context)
            return 0
    elif workflow_type == "iteration_over_fetlife_inbox":
        fetlife_profile_directory = _resolve_string_value(
            spec.get("fetlife_profile_directory", "Profile 9"),
            base_context,
            base_dir=base_dir,
            label="fetlife_profile_directory",
        )
        fetlife_mailbox_path = _resolve_string_value(
            spec.get("fetlife_mailbox_path", "/inbox/all"),
            base_context,
            base_dir=base_dir,
            label="fetlife_mailbox_path",
        )
        fetlife_user_data_dir = (
            _resolve_path(spec.get("fetlife_user_data_dir"), base_context, base_dir=base_dir, required=True, label="fetlife_user_data_dir")
            if spec.get("fetlife_user_data_dir")
            else _default_fetlife_user_data_dir(project)
        )
        max_results = int(
            _resolve_number(spec.get("max_results"), base_context, base_dir=base_dir, label="max_results", default=0, as_int=True) or 0
        )
        process_order = _resolve_string_value(spec.get("process_order", "desc"), base_context, base_dir=base_dir, label="process_order")
        fetlife = _ensure_fetlife_client(
            project,
            fetlife_cache,
            user_data_dir=fetlife_user_data_dir,
            profile_directory=fetlife_profile_directory,
        )
        iteration_items = _read_fetlife_inbox_items(
            fetlife,
            max_results=max_results,
            process_order=process_order,
            mailbox_path=fetlife_mailbox_path,
        )
        base_context["fetlife_user_data_dir"] = str(fetlife_user_data_dir)
        base_context["fetlife_profile_directory"] = fetlife_profile_directory
        base_context["fetlife_mailbox_path"] = fetlife_mailbox_path
        if not iteration_items:
            print("No FetLife inbox conversations matched the workflow filters.")
            _append_active_exec_log(
                event_type="workflow_complete",
                title="Workflow completed with no matching FetLife inbox conversations",
                body=(
                    f"Workflow file: {wf_path}\nProfile: {fetlife_profile_directory}\n"
                    f"Mailbox path: {fetlife_mailbox_path}\nUser data dir: {fetlife_user_data_dir}"
                ),
            )
            if args.output_context_json:
                _write_output_context_json(args.output_context_json, base_context)
            _close_fetlife_clients(fetlife_cache)
            return 0
    elif workflow_type == "iteration_over_agent_history_folders":
        history_root_name = _resolve_string_value(spec.get("history_root_name", "history"), base_context, base_dir=base_dir, label="history_root_name")
        agent_folder_regex = _resolve_string_value(spec.get("agent_folder_regex"), base_context, base_dir=base_dir, label="agent_folder_regex") if spec.get("agent_folder_regex") else None
        client_folder_regex = _resolve_string_value(spec.get("client_folder_regex"), base_context, base_dir=base_dir, label="client_folder_regex") if spec.get("client_folder_regex") else None
        max_history_folders = int(_resolve_number(spec.get("max_history_folders"), base_context, base_dir=base_dir, label="max_history_folders", default=0, as_int=True) or 0)
        iteration_items = _read_agent_history_items(
            project,
            history_root_name=history_root_name,
            agent_folder_regex=agent_folder_regex,
            client_folder_regex=client_folder_regex,
            max_history_folders=max_history_folders,
        )
        if not iteration_items:
            print("No agent history folders matched the workflow filters.")
            _append_active_exec_log(
                event_type="workflow_complete",
                title="Workflow completed with no matching history folders",
                body=f"Workflow file: {wf_path}\nHistory root name: {history_root_name}",
            )
            if args.output_context_json:
                _write_output_context_json(args.output_context_json, base_context)
            return 0
    elif workflow_type == "iteration_over_extracted_emails_from_files":
        folder = _resolve_path(spec.get("folder"), base_context, base_dir=base_dir, required=True, label="folder")
        include_pattern = _resolve_string_value(spec.get("regex_filter") or spec.get("include_regex"), base_context, base_dir=base_dir, label="include_regex") if (spec.get("regex_filter") or spec.get("include_regex")) else None
        exclude_pattern = _resolve_string_value(spec.get("exclude_regex"), base_context, base_dir=base_dir, label="exclude_regex") if spec.get("exclude_regex") else None
        include_regex = _compile_regex(include_pattern, label="include_regex")
        exclude_regex = _compile_regex(exclude_pattern, label="exclude_regex")
        recursive = _coerce_bool(spec.get("recursive", False), label="recursive")
        max_files = int(_resolve_number(spec.get("max_files"), base_context, base_dir=base_dir, label="max_files", default=0, as_int=True) or 0)
        allow_missing = _coerce_bool(spec.get("allow_missing", False), label="allow_missing")
        allow_empty = _coerce_bool(spec.get("allow_empty", True), label="allow_empty")
        sort_by = _resolve_string_value(spec.get("sort_by", "name"), base_context, base_dir=base_dir, label="sort_by")
        sort_direction = _resolve_string_value(spec.get("sort_direction", "asc"), base_context, base_dir=base_dir, label="sort_direction").lower()
        rename_remove_prefix = _resolve_string_value(spec.get("rename_remove_prefix", ""), base_context, base_dir=base_dir, label="rename_remove_prefix") if spec.get("rename_remove_prefix") else ""
        if not folder.exists():
            if allow_missing:
                files = []
            else:
                raise FileNotFoundError(f"Folder not found: {folder}")
        else:
            files = list(_iter_matching_text_files(folder, recursive=recursive, include_regex=include_regex, exclude_regex=exclude_regex))
        file_entries = [
            {
                "path": str(file_path),
                "relative_path": str(file_path.relative_to(folder)),
                "name": file_path.name,
                "size": file_path.stat().st_size,
                "modified_utc": dt.datetime.fromtimestamp(file_path.stat().st_mtime, tz=UTC).isoformat(),
            }
            for file_path in files
        ]
        file_entries = _sort_file_entries(file_entries, sort_by=sort_by, descending=sort_direction == "desc")
        files = [Path(item["path"]) for item in file_entries]
        if max_files > 0:
            files = files[:max_files]
        if rename_remove_prefix and files:
            files = _rename_remove_prefix(files, rename_remove_prefix)
        extracted_items: list[dict[str, Any]] = []
        seen_emails: set[str] = set()
        for file_path in files:
            text_value = file_path.read_text(encoding="utf-8", errors="replace")
            for entry in _extract_email_entries_from_value(text_value, source_file=file_path):
                lowered = str(entry.get("email") or "").lower()
                if lowered in seen_emails:
                    continue
                seen_emails.add(lowered)
                extracted_items.append(entry)
        base_context["source_folder"] = str(folder)
        base_context["source_files_processed"] = [str(path) for path in files]
        base_context["source_file_count"] = len(files)
        base_context["extracted_recipient_count"] = len(extracted_items)
        iteration_items = extracted_items
        if not iteration_items:
            if not allow_empty:
                raise RuntimeError(f"No email addresses were extracted from matching files in folder: {folder}")
            print("No extracted email recipients matched the workflow folder filters.")
            _append_active_exec_log(
                event_type="workflow_complete",
                title="Workflow completed with no extracted recipients",
                body=f"Workflow file: {wf_path}\nSource folder: {folder}",
            )
            if args.output_context_json:
                _write_output_context_json(args.output_context_json, base_context)
            return 0
    elif workflow_type not in {"", "single_run"}:
        raise RuntimeError(f"Unsupported workflow-type: {workflow_type}")

    if not _workflow_skip_cost_approval_interaction(args, project, spec):
        sample_ctx = _workflow_build_sample_context_for_cost(workflow_type, base_context, iteration_items, list_file_path)
        est = _workflow_compute_cost_estimate(
            project,
            wf_path,
            spec,
            steps,
            sample_ctx,
            base_dir,
            args,
            iteration_count=len(iteration_items),
            default_model=default_model,
        )
        if not _workflow_print_and_prompt_cost_approval(project, est):
            print("Workflow cancelled (cost estimate not approved).", flush=True)
            _append_engine_log(
                project,
                {
                    "event": "workflow_cancelled_cost",
                    "workflow": wf_path.stem,
                    "workflow_file": str(wf_path),
                    "detail": "user declined cost estimate",
                },
            )
            if getattr(args, "output_context_json", None):
                _write_output_context_json(args.output_context_json, base_context)
            return 2

    plugin_registry = _workflow_plugin_registry(project)
    for iteration_index, current_item in enumerate(iteration_items, start=1):
        context = dict(base_context)
        context["iteration_index"] = iteration_index
        context["iteration_count"] = len(iteration_items)
        trace(2, f"[workflow] starting iteration {iteration_index}/{len(iteration_items)}")
        if current_item is not None:
            context["current_list_item"] = current_item
            context["current_item"] = current_item
            if workflow_type == "iteration_over_gmail_inbox" and isinstance(current_item, dict):
                context["current_message"] = current_item
                context["current_message_id"] = str(current_item.get("id", ""))
                context["current_message_from"] = str(current_item.get("from", ""))
                context["current_message_sender_email"] = str(current_item.get("sender_email", ""))
                context["current_message_subject"] = str(current_item.get("subject", ""))
                context["current_message_body"] = str(current_item.get("body", ""))
                context["current_message_date"] = str(current_item.get("date", ""))
                context["current_message_thread_id"] = str(current_item.get("thread_id", ""))
                context["current_message_header_id"] = str(current_item.get("message_header_id", ""))
                context["current_message_references"] = str(current_item.get("references", ""))
                context["current_message_in_reply_to"] = str(current_item.get("in_reply_to", ""))
                trace(2, f"[workflow] current message id={context['current_message_id']} from={context['current_message_from']!r} subject={context['current_message_subject']!r}")
            elif workflow_type == "iteration_over_fetlife_inbox" and isinstance(current_item, dict):
                context["current_fetlife_message"] = current_item
                context["current_fetlife_conversation_id"] = str(current_item.get("conversation_id", ""))
                context["current_fetlife_subject"] = str(current_item.get("subject", ""))
                context["current_fetlife_participant_name"] = str(current_item.get("participant_name", ""))
                context["current_fetlife_participant_slug"] = str(current_item.get("participant_slug", ""))
                context["current_fetlife_history_key"] = str(current_item.get("history_key", ""))
                context["current_fetlife_thread_text"] = str(current_item.get("thread_text", ""))
                context["current_fetlife_message_count"] = int(current_item.get("message_count", 0) or 0)
                context["current_fetlife_last_message_id"] = str(current_item.get("last_message_id", ""))
                context["current_fetlife_last_message_author"] = str(current_item.get("last_message_author", ""))
                context["current_fetlife_last_message_body"] = str(current_item.get("last_message_body", ""))
                context["current_fetlife_last_message_created_at"] = str(current_item.get("last_message_created_at", ""))
                context["current_fetlife_needs_reply"] = bool(current_item.get("needs_reply"))
                context["current_fetlife_self_nickname"] = str(current_item.get("self_nickname", ""))
                trace(
                    2,
                    f"[workflow] current FetLife conversation id={context['current_fetlife_conversation_id']} "
                    f"participant={context['current_fetlife_participant_name']!r} needs_reply={context['current_fetlife_needs_reply']!r}"
                )
            elif workflow_type == "iteration_over_agent_history_folders" and isinstance(current_item, dict):
                context["current_history_item"] = current_item
                for key, value in current_item.items():
                    context[str(key)] = value
                trace(2, f"[workflow] current history folder={context.get('client_history_folder')} agent={context.get('agent_folder_name')}")
            elif workflow_type == "iteration_over_extracted_emails_from_files" and isinstance(current_item, dict):
                email_value = str(current_item.get("email") or "").strip().lower()
                first_name = str(current_item.get("first_name") or "").strip()
                context["current_email_entry"] = current_item
                context["current_item"] = email_value
                context["current_list_item"] = email_value
                context["current_email"] = email_value
                context["current_email_first_name"] = first_name
                context["current_email_contact_name"] = str(current_item.get("contact_name") or "").strip()
                context["current_email_business_name"] = str(current_item.get("business_name") or "").strip()
                context["current_email_website_url"] = str(current_item.get("website_url") or "").strip()
                context["current_email_source_file"] = str(current_item.get("source_file") or "").strip()
                context["first_name"] = first_name or "there"
                trace(2, f"[workflow] current email={email_value!r} first_name={context['first_name']!r} source={context.get('current_email_source_file', '')!r}")
        if list_file_path is not None:
            context["list_file"] = str(list_file_path)

        for idx, step in enumerate(steps, start=1):
            stype = step.get("type")
            if not stype:
                raise RuntimeError(f"Step {idx} missing 'type'.")
            plugin_manifest = _required_workflow_plugin(str(stype), plugin_registry, step_index=idx)
            if str(plugin_manifest.get("handler") or "").strip() != "legacy_step_adapter":
                raise RuntimeError(
                    f"Plugin '{stype}' uses unsupported handler '{plugin_manifest.get('handler')}'. "
                    "Current engine supports handler=legacy_step_adapter."
                )

            verbose = _step_verbosity(step, context, base_dir=base_dir, default=default_verbose)
            _sleep_if_needed(_step_delay(step, "delay_before_seconds", context, base_dir=base_dir))
            step_prefix = f"[iteration {iteration_index}/{len(iteration_items)}] " if len(iteration_items) > 1 else ""
            print(f"{step_prefix}[step {idx}] {stype}", flush=True)
            trace(3, f"[workflow] step-config={json.dumps(step, ensure_ascii=False, default=str)}")
            _append_engine_log(
                project,
                {
                    "event": "workflow_step",
                    "agent": str(
                        context.get("agent_key")
                        or base_context.get("agent_key")
                        or _infer_agent_name_from_workflow_path(project, wf_path)
                    ),
                    "workflow": wf_path.stem,
                    "workflow_file": str(wf_path),
                    "iteration_index": iteration_index,
                    "iteration_total": len(iteration_items),
                    "step_index": idx,
                    "step_type": stype,
                    "detail": str(step.get("comment") or "")[:500],
                },
            )

            if stype == "get_next_item_in_list":
                if current_item is None and "current_list_item" not in context:
                    raise RuntimeError("get_next_item_in_list is only valid for iteration_over_list_file workflows.")
                save_to = _resolve_string_value(step.get("save_to") or step.get("output"), context, base_dir=base_dir, required=True, label="save_to")
                value_to_store = context.get("current_list_item", current_item)
                context[save_to] = value_to_store
                _log(verbose, 1, f"  -> stored {save_to} = {value_to_store}")

            elif stype == "combine_files":
                files = step.get("files") or []
                if not files:
                    raise RuntimeError(f"Step {idx} combine_files requires 'files'.")
                chunks: list[str] = []
                for fp in files:
                    p = _resolve_path(fp, context, base_dir=base_dir, required=True, label=f"files[{fp}]")
                    ensure_file(p, f"Input file in step {idx}")
                    text_value = _load_file_text(p, context=context, label=f"Input file in step {idx}", base_dir=base_dir)
                    chunks.append(f"\n\n===== {p} =====\n{text_value}")
                combined_text = "".join(chunks)
                out_key = _store_output(context, step, combined_text, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(combined_text)} chars)")

            elif stype == "file_to_context":
                p = _resolve_path(step.get("path", ""), context, base_dir=base_dir, required=True, label="path")
                allow_missing = _coerce_bool(step.get("allow_missing", False), label="allow_missing")
                default_value = _resolve_text_source(step, "default_value", context, base_dir=base_dir, required=False, default=str(step.get("default", ""))) if ("default_value" in step or "default" in step) else ""
                parse_json = _coerce_bool(step.get("parse_json", False), label="parse_json")
                if not p.exists():
                    if not allow_missing:
                        ensure_file(p, f"Input file in step {idx}")
                    content: Any = json.loads(default_value) if parse_json and default_value.strip() else ({} if parse_json else default_value)
                else:
                    content_text = _load_file_text(p, context=context, label=f"Input file in step {idx}", base_dir=base_dir)
                    content = json.loads(content_text) if parse_json and content_text.strip() else content_text
                out_key = _store_output(context, step, content, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                if isinstance(content, str):
                    _log(verbose, 1, f"  -> stored {out_key} ({len(content)} chars)")
                else:
                    _log(verbose, 1, f"  -> stored {out_key}")

            elif stype == "unified_text_diff":
                after_key = _resolve_string_value(step.get("after_key") or step.get("after"), context, base_dir=base_dir, required=True, label="after_key")
                before_key = _resolve_string_value(step.get("before_key") or step.get("before"), context, base_dir=base_dir, required=False, label="before_key").strip()
                before_text = ""
                if before_key:
                    if before_key not in context:
                        raise RuntimeError(f"Step {idx} unified_text_diff before_key not found in context: {before_key}")
                    before_raw = context[before_key]
                    before_text = before_raw if isinstance(before_raw, str) else json.dumps(before_raw, ensure_ascii=False, indent=2)
                if after_key not in context:
                    raise RuntimeError(f"Step {idx} unified_text_diff after_key not found in context: {after_key}")
                after_raw = context[after_key]
                after_text = after_raw if isinstance(after_raw, str) else json.dumps(after_raw, ensure_ascii=False, indent=2)
                from_label = _resolve_string_value(step.get("from_label", "previous"), context, base_dir=base_dir, label="from_label")
                to_label = _resolve_string_value(step.get("to_label", "current"), context, base_dir=base_dir, label="to_label")
                n_ctx = int(_resolve_number(step.get("context_lines"), context, base_dir=base_dir, default=3, label="context_lines", as_int=True) or 3)
                n_ctx = max(0, n_ctx)
                if not before_text.strip():
                    line_count = after_text.count("\n") + (1 if after_text else 0)
                    result = (
                        "No previous API request core on disk to compare against "
                        "(first dry run, or latest_bulk_test_history_dry_run_api_core.txt was missing).\n\n"
                        f"This run's API core: {len(after_text)} characters, ~{line_count} lines.\n"
                        "The core file is updated after this run so the next dry run can show a unified diff here.\n"
                    )
                elif before_text == after_text:
                    result = "No changes: the API system+user core matches the previous dry run exactly.\n"
                else:
                    diff_iter = difflib.unified_diff(
                        before_text.splitlines(keepends=True),
                        after_text.splitlines(keepends=True),
                        fromfile=from_label,
                        tofile=to_label,
                        n=n_ctx,
                    )
                    diff_lines = list(diff_iter)
                    result = "".join(diff_lines) if diff_lines else "(unified diff produced no output; treating as identical)\n"
                out_key = _store_output(context, step, result, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(result)} chars)")

            elif stype == "save_variable_value":
                name = _resolve_string_value(step.get("name") or step.get("output"), context, base_dir=base_dir, required=True, label="name")
                if step.get("source_key"):
                    source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                    if source_key not in context:
                        raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                    value: Any = context[source_key]
                else:
                    value = _resolve_text_source(step, "value", context, base_dir=base_dir, required=True, default="")
                    if _coerce_bool(step.get("parse_json", False), label="parse_json"):
                        value = json.loads(value)
                context[name] = value
                if step.get("output_file"):
                    _store_output(
                        context,
                        {
                            "output": name,
                            "output_file": step.get("output_file"),
                            "output_file_if_missing": step.get("output_file_if_missing", False),
                        },
                        value,
                                  default_key=name, base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> saved variable {name}")

            elif stype == "build_agent_system_prompt":
                expose_fields = _coerce_bool(step.get("expose_fields", True), label="expose_fields")
                system_text, agent_input_text, conversation_history_text = _build_agent_system_prompt_text(project, step, context, base_dir=base_dir)
                if expose_fields:
                    context["agent_input"] = agent_input_text
                    context["conversation_history"] = conversation_history_text
                    context["conversation-history"] = conversation_history_text
                out_key = _store_output(context, step, system_text, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(
                    verbose,
                    1,
                    f"  -> stored {out_key} (persona {len(agent_input_text)} chars, history {len(conversation_history_text)} chars)",
                )

            elif stype == "path_exists":
                path = _resolve_path(step.get("path"), context, base_dir=base_dir, required=True, label="path")
                exists = path.exists()
                out_key = _store_output(context, step, exists, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} = {exists}")

            elif stype == "regex_match":
                if step.get("source_key"):
                    source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                    if source_key not in context:
                        raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                    source_text = _context_value_to_text(context[source_key])
                else:
                    # Blank text should behave like "no match", not abort the workflow.
                    source_text = _resolve_text_source(step, "text", context, base_dir=base_dir, required=False, default="")
                pattern_text = _resolve_string_value(step.get("pattern"), context, base_dir=base_dir, required=True, label="pattern")
                regex = _compile_regex(pattern_text, label="pattern")
                matched = bool(regex.search(source_text)) if regex else False
                out_key = _store_output(context, step, matched, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} = {matched}")

            elif stype == "any_true":
                source_keys = step.get("source_keys") or []
                if not isinstance(source_keys, list) or not source_keys:
                    raise RuntimeError(f"Step {idx} any_true requires a non-empty source_keys list.")
                values: list[bool] = []
                for raw_key in source_keys:
                    source_key = _resolve_string_value(raw_key, context, base_dir=base_dir, required=True, label="source_keys")
                    if source_key not in context:
                        raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                    values.append(_coerce_bool(context[source_key], label=f"any_true[{source_key}]"))
                result = any(values)
                out_key = _store_output(context, step, result, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} = {result}")

            elif stype == "skip_remaining_steps_if":
                if step.get("source_key"):
                    source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                    if source_key not in context:
                        raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                    raw_value = context[source_key]
                else:
                    raw_value = _resolve_dynamic_value(step.get("value"), context, base_dir=base_dir, label="value")
                should_skip = _coerce_bool(raw_value, label="skip_remaining_steps_if")
                _log(verbose, 1, f"  -> condition evaluated to {should_skip}")
                if should_skip:
                    _log(verbose, 1, "  -> skipping remaining steps for this iteration")
                    break

            elif stype == "skip_remaining_steps_unless":
                if step.get("source_key"):
                    source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                    if source_key not in context:
                        raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                    raw_value = context[source_key]
                else:
                    raw_value = _resolve_dynamic_value(step.get("value"), context, base_dir=base_dir, label="value")
                should_continue = _coerce_bool(raw_value, label="skip_remaining_steps_unless")
                _log(verbose, 1, f"  -> condition evaluated to {should_continue}")
                if not should_continue:
                    _log(verbose, 1, "  -> skipping remaining steps for this iteration")
                    break

            elif stype == "replace_variables_in_file":
                file_path = _resolve_path(step.get("file"), context, base_dir=base_dir, required=True, label="file")
                ensure_file(file_path, f"Replacement source file {file_path}")
                file_text = file_path.read_text(encoding="utf-8", errors="replace")
                replacements_list = step.get("replacements") or []
                if not isinstance(replacements_list, list) or not replacements_list:
                    raise RuntimeError(f"Step {idx} replace_variables_in_file requires 'replacements'.")
                replacements: dict[str, str] = {}
                for replacement in replacements_list:
                    if not isinstance(replacement, dict):
                        raise RuntimeError(f"Step {idx} replacement entries must be objects.")
                    key = _resolve_string_value(replacement.get("key"), context, base_dir=base_dir, required=True, label="replacement.key")
                    value = _resolve_text_source(replacement, "value", context, base_dir=base_dir, required=False, default="")
                    replacements[key] = value
                replaced = _replace_named_variables(file_text, replacements)
                replaced = render_template(replaced, context, strict=True, label=f"replace_variables_in_file[{idx}]", base_dir=base_dir)
                out_key = _store_output(context, step, replaced, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(replaced)} chars)")

            elif stype == "ai_chat":
                system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You are a helpful assistant.")
                user = _resolve_text_source(step, "user", context, base_dir=base_dir, required=True, default="")
                if not user.strip():
                    raise RuntimeError(f"Step {idx} ai_chat requires non-empty 'user'.")
                temp = float(_resolve_number(step.get("temperature"), context, base_dir=base_dir, label="temperature", default=args.temperature))
                max_tokens = int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True))
                chat_timeout = _workflow_step_http_timeout_seconds(step, context, base_dir=base_dir, args=args)
                response = ai.chat(system=system, user=user, temperature=temp, max_tokens=max_tokens, timeout_seconds=chat_timeout)
                out_key = _store_output(context, step, response, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(response)} chars)")

            elif stype == "extract_section_from_file":
                source_variable = _resolve_string_value(step.get("source_variable"), context, base_dir=base_dir, required=True, label="source_variable")
                if source_variable not in context:
                    raise RuntimeError(f"Step {idx} source_variable not found in context: {source_variable}")
                section_name = _resolve_string_value(step.get("section_name"), context, base_dir=base_dir, required=True, label="section_name")
                extracted = _extract_named_section(_context_value_to_text(context[source_variable]), section_name)
                if not extracted.strip() and ("default_value" in step or "default" in step):
                    extracted = _resolve_text_source(
                        step,
                        "default_value",
                        context,
                        base_dir=base_dir,
                        required=False,
                        default=str(step.get("default", "")),
                    )
                out_key = _resolve_string_value(step.get("save_to_variable") or step.get("output"), context, base_dir=base_dir, required=True, label="save_to_variable")
                context[out_key] = extracted
                _log(verbose, 1, f"  -> stored {out_key} ({len(extracted)} chars)")

            elif stype == "send_input_to_API":
                system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You are a helpful assistant.")
                temp = float(_resolve_number(step.get("temperature"), context, base_dir=base_dir, label="temperature", default=args.temperature))
                max_tokens = int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True))
                chat_timeout = _workflow_step_http_timeout_seconds(step, context, base_dir=base_dir, args=args)
                parse_json = _coerce_bool(step.get("parse_json", False), label="parse_json")
                minimum_extracted_emails = int(_resolve_number(step.get("minimum_extracted_emails"), context, base_dir=base_dir, label="minimum_extracted_emails", default=0, as_int=True) or 0)
                retry_attempts_before_relaxing = int(_resolve_number(step.get("retry_attempts_before_relaxing"), context, base_dir=base_dir, label="retry_attempts_before_relaxing", default=0, as_int=True) or 0)
                relaxed_minimum_extracted_emails = int(_resolve_number(step.get("relaxed_minimum_extracted_emails"), context, base_dir=base_dir, label="relaxed_minimum_extracted_emails", default=1 if minimum_extracted_emails > 0 else 0, as_int=True) or 0)
                default_max_attempts = retry_attempts_before_relaxing + 1 if minimum_extracted_emails > 0 and retry_attempts_before_relaxing > 0 else 1
                max_attempts = int(_resolve_number(step.get("max_attempts"), context, base_dir=base_dir, label="max_attempts", default=default_max_attempts, as_int=True) or 1)
                retry_target_selection = step.get("retry_target_selection") if isinstance(step.get("retry_target_selection"), dict) else None
                attempts_output = _resolve_string_value(step.get("attempts_output"), context, base_dir=base_dir, label="attempts_output") if step.get("attempts_output") else None
                attempts: list[dict[str, Any]] = []
                value: Any = None
                accepted = False

                for attempt_number in range(1, max_attempts + 1):
                    user = _build_ai_user_text(step, context, base_dir=base_dir, input_field="input")
                    response = ai.chat(
                        system=system,
                        user=user,
                        temperature=temp,
                        max_tokens=max_tokens,
                        timeout_seconds=chat_timeout,
                    )
                    value = _parse_json_from_model_text(response, label=f"send_input_to_API step {idx}") if parse_json else response
                    extracted_emails = _extract_emails_from_value(value)
                    required_minimum = 0
                    if minimum_extracted_emails > 0:
                        if attempt_number <= retry_attempts_before_relaxing:
                            required_minimum = minimum_extracted_emails
                        else:
                            required_minimum = relaxed_minimum_extracted_emails
                    accepted = len(extracted_emails) >= required_minimum if required_minimum > 0 else True
                    attempts.append({
                        "attempt": attempt_number,
                        "industry": str(context.get("industry", "")),
                        "city_and_state": str(context.get("city_and_state", "")),
                        "required_minimum_emails": required_minimum,
                        "returned_email_count": len(extracted_emails),
                        "accepted": accepted,
                    })
                    if accepted:
                        break
                    if attempt_number < max_attempts and retry_target_selection:
                        _select_business_research_target_from_spec(retry_target_selection, context, base_dir=base_dir)

                if attempts_output:
                    context[attempts_output] = attempts
                if not accepted:
                    last_attempt = attempts[-1] if attempts else {}
                    raise RuntimeError(
                        "send_input_to_API did not return enough emails after retries. "
                        f"Last attempt returned {last_attempt.get('returned_email_count', 0)} email(s); "
                        f"required {last_attempt.get('required_minimum_emails', 0)}."
                    )
                out_key = _store_output(context, step, value, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key}")

            elif stype == "send_file_to_AI":
                file_path = _resolve_path(step.get("path"), context, base_dir=base_dir, required=True, label="path")
                file_text = _load_file_text(file_path, context=context, label=f"Input file in step {idx}", base_dir=base_dir)
                system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You analyze provided file content.")
                prompt = _resolve_text_source(step, "prompt", context, base_dir=base_dir, required=False, default="")
                include_path = _coerce_bool(step.get("include_file_path", True), label="include_file_path")
                chat_timeout = _workflow_step_http_timeout_seconds(step, context, base_dir=base_dir, args=args)
                payload_parts = [prompt] if prompt.strip() else []
                if include_path:
                    payload_parts.append(f"File path: {file_path}")
                payload_parts.append(file_text)
                user = _compose_prompt_segments(payload_parts)
                response = ai.chat(
                    system=system,
                    user=user,
                    temperature=float(_resolve_number(step.get("temperature"), context, base_dir=base_dir, label="temperature", default=args.temperature)),
                    max_tokens=int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True)),
                    timeout_seconds=chat_timeout,
                )
                out_key = _store_output(context, step, response, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(response)} chars)")

            elif stype == "combine_all_files_in_folder":
                folder = _resolve_path(step.get("folder"), context, base_dir=base_dir, required=True, label="folder")
                include_pattern = _resolve_string_value(step.get("regex_filter") or step.get("include_regex"), context, base_dir=base_dir, label="include_regex") if (step.get("regex_filter") or step.get("include_regex")) else None
                exclude_pattern = _resolve_string_value(step.get("exclude_regex"), context, base_dir=base_dir, label="exclude_regex") if step.get("exclude_regex") else None
                include_regex = _compile_regex(include_pattern, label="include_regex")
                exclude_regex = _compile_regex(exclude_pattern, label="exclude_regex")
                recursive = _coerce_bool(step.get("recursive", True), label="recursive")
                max_files = int(_resolve_number(step.get("max_files"), context, base_dir=base_dir, label="max_files", default=0, as_int=True) or 0)
                skip_first = int(_resolve_number(step.get("skip_first"), context, base_dir=base_dir, label="skip_first", default=0, as_int=True) or 0)
                skip_last = int(_resolve_number(step.get("skip_last"), context, base_dir=base_dir, label="skip_last", default=0, as_int=True) or 0)
                sort_by = _resolve_string_value(step.get("sort_by", "name"), context, base_dir=base_dir, label="sort_by")
                sort_direction = _resolve_string_value(step.get("sort_direction", "asc"), context, base_dir=base_dir, label="sort_direction").lower()
                allow_missing = _coerce_bool(step.get("allow_missing", False), label="allow_missing")
                allow_empty = _coerce_bool(step.get("allow_empty", False), label="allow_empty")
                if not folder.exists():
                    if allow_missing:
                        files: list[Path] = []
                    else:
                        raise FileNotFoundError(f"Folder not found: {folder}")
                else:
                    files = list(_iter_matching_text_files(folder, recursive=recursive, include_regex=include_regex, exclude_regex=exclude_regex))
                file_entries = [
                    {
                        "path": str(file_path),
                        "relative_path": str(file_path.relative_to(folder)),
                        "name": file_path.name,
                        "size": file_path.stat().st_size,
                        "modified_utc": dt.datetime.fromtimestamp(file_path.stat().st_mtime, tz=UTC).isoformat(),
                    }
                    for file_path in files
                ]
                file_entries = _sort_file_entries(file_entries, sort_by=sort_by, descending=sort_direction == "desc")
                if skip_first > 0:
                    file_entries = file_entries[skip_first:]
                if skip_last > 0:
                    file_entries = file_entries[:-skip_last] if skip_last < len(file_entries) else []
                files = [Path(item["path"]) for item in file_entries]
                if max_files > 0:
                    files = files[:max_files]
                if not files and not (allow_missing or allow_empty):
                    raise RuntimeError(f"Step {idx} found no matching files in folder: {folder}")
                chunks: list[str] = []
                for file_path in files:
                    text_value = _load_file_text(file_path, context=context, label=f"Folder file {file_path}", base_dir=base_dir)
                    chunks.append(f"===== {file_path.relative_to(folder)} =====\n{text_value}")
                combined = "\n\n".join(chunks)
                out_key = _store_output(context, step, combined, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} from {len(files)} file(s)")

            elif stype == "load_agent_context":
                agent_root = _resolve_path(step.get("agent_root", context.get("agent_root")), context, base_dir=base_dir, required=True, label="agent_root")
                agent_context = _build_agent_folder_context(project, agent_root)
                expose_fields = _coerce_bool(step.get("expose_fields", True), label="expose_fields")
                if expose_fields:
                    context.update(agent_context)
                out_key = _store_output(context, step, agent_context, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> loaded agent context for {agent_context.get('agent_name') or agent_root.name}")

            elif stype == "list_files_in_folder":
                folder = _resolve_path(step.get("folder"), context, base_dir=base_dir, required=True, label="folder")
                include_pattern = _resolve_string_value(step.get("regex_filter") or step.get("include_regex"), context, base_dir=base_dir, label="include_regex") if (step.get("regex_filter") or step.get("include_regex")) else None
                exclude_pattern = _resolve_string_value(step.get("exclude_regex"), context, base_dir=base_dir, label="exclude_regex") if step.get("exclude_regex") else None
                include_regex = _compile_regex(include_pattern, label="include_regex")
                exclude_regex = _compile_regex(exclude_pattern, label="exclude_regex")
                recursive = _coerce_bool(step.get("recursive", True), label="recursive")
                allow_missing = _coerce_bool(step.get("allow_missing", False), label="allow_missing")
                allow_empty = _coerce_bool(step.get("allow_empty", False), label="allow_empty")
                sort_by = _resolve_string_value(step.get("sort_by", "name"), context, base_dir=base_dir, label="sort_by")
                sort_direction = _resolve_string_value(step.get("sort_direction", "asc"), context, base_dir=base_dir, label="sort_direction").lower()
                max_files = int(_resolve_number(step.get("max_files"), context, base_dir=base_dir, label="max_files", default=0, as_int=True) or 0)
                if not folder.exists():
                    if allow_missing:
                        files = []
                    else:
                        raise FileNotFoundError(f"Folder not found: {folder}")
                else:
                    files = list(_iter_matching_text_files(folder, recursive=recursive, include_regex=include_regex, exclude_regex=exclude_regex))
                entries = [
                    {
                        "path": str(file_path),
                        "relative_path": str(file_path.relative_to(folder)),
                        "name": file_path.name,
                        "size": file_path.stat().st_size,
                        "modified_utc": dt.datetime.fromtimestamp(file_path.stat().st_mtime, tz=UTC).isoformat(),
                    }
                    for file_path in files
                ]
                entries = _sort_file_entries(entries, sort_by=sort_by, descending=sort_direction == "desc")
                if max_files > 0:
                    entries = entries[:max_files]
                if not entries and not (allow_missing or allow_empty):
                    raise RuntimeError(f"Step {idx} found no matching files in folder: {folder}")
                out_key = _store_output(context, step, entries, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(entries)} file(s))")

            elif stype == "count_items":
                source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                if source_key not in context:
                    raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                value = context[source_key]
                if isinstance(value, dict):
                    count = len(value.keys())
                elif isinstance(value, (list, tuple, set, str)):
                    count = len(value)
                else:
                    raise RuntimeError(f"Step {idx} count_items requires list, tuple, set, dict, or string input.")
                out_key = _store_output(context, step, count, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} = {count}")

            elif stype == "compare_number":
                if step.get("source_key"):
                    source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                    if source_key not in context:
                        raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                    left_value = context[source_key]
                else:
                    left_value = _resolve_dynamic_value(step.get("value"), context, base_dir=base_dir, label="value")
                left_number = float(left_value)
                right_number = float(_resolve_number(step.get("compare_to"), context, base_dir=base_dir, label="compare_to", default=0))
                operator = _resolve_string_value(step.get("operator", "gte"), context, base_dir=base_dir, label="operator").strip().lower()
                if operator in {"eq", "=="}:
                    result = left_number == right_number
                elif operator in {"ne", "!="}:
                    result = left_number != right_number
                elif operator in {"gt", ">"}:
                    result = left_number > right_number
                elif operator in {"gte", ">="}:
                    result = left_number >= right_number
                elif operator in {"lt", "<"}:
                    result = left_number < right_number
                elif operator in {"lte", "<="}:
                    result = left_number <= right_number
                else:
                    raise RuntimeError(f"Unsupported compare_number operator: {operator}")
                out_key = _store_output(context, step, result, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} = {result}")

            elif stype == "slice_items":
                source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                if source_key not in context:
                    raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                items = context[source_key]
                if not isinstance(items, list):
                    raise RuntimeError(f"Step {idx} slice_items requires a list source.")
                start = None if step.get("start") is None else int(_resolve_number(step.get("start"), context, base_dir=base_dir, label="start", default=0, as_int=True))
                end = None if step.get("end") is None else int(_resolve_number(step.get("end"), context, base_dir=base_dir, label="end", default=0, as_int=True))
                step_value = None if step.get("step") is None else int(_resolve_number(step.get("step"), context, base_dir=base_dir, label="step", default=1, as_int=True))
                sliced = items[slice(start, end, step_value)]
                out_key = _store_output(context, step, sliced, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(sliced)} item(s))")

            elif stype == "combine_listed_files":
                source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                if source_key not in context:
                    raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                items = context[source_key]
                if not isinstance(items, list):
                    raise RuntimeError(f"Step {idx} combine_listed_files requires a list source.")
                allow_empty = _coerce_bool(step.get("allow_empty", False), label="allow_empty")
                include_header = _coerce_bool(step.get("include_header", True), label="include_header")
                path_field = _resolve_string_value(step.get("path_field", "path"), context, base_dir=base_dir, label="path_field")
                header_field = _resolve_string_value(step.get("header_field", "relative_path"), context, base_dir=base_dir, label="header_field")
                chunks: list[str] = []
                for item in items:
                    if isinstance(item, dict):
                        raw_path = item.get(path_field)
                        header_value = item.get(header_field) or item.get("relative_path") or item.get("name") or raw_path
                    else:
                        raw_path = item
                        header_value = item
                    if not raw_path:
                        raise RuntimeError(f"Step {idx} combine_listed_files encountered an item without a path.")
                    file_path = Path(str(raw_path))
                    ensure_file(file_path, f"Listed file in step {idx}")
                    text_value = _load_file_text(file_path, context=context, label=f"Listed file in step {idx}", base_dir=base_dir)
                    chunks.append(f"===== {header_value} =====\n{text_value}" if include_header else text_value)
                if not chunks and not allow_empty:
                    raise RuntimeError(f"Step {idx} combine_listed_files received no files to combine.")
                combined = "\n\n".join(chunks)
                out_key = _store_output(context, step, combined, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} from {len(chunks)} file(s)")

            elif stype == "select_business_research_target":
                selection_record = _select_business_research_target_from_spec(step, context, base_dir=base_dir)
                out_key = _store_output(context, step, selection_record, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> selected industry={selection_record['industry']} city_and_state={selection_record['city_and_state']}")

            elif stype == "extract_emails_from_value":
                source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                if source_key not in context:
                    raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                emails = _extract_emails_from_value(context[source_key])
                out_key = _store_output(context, step, emails, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(emails)} email(s))")

            elif stype == "append_unique_lines":
                path = _resolve_path(step.get("path"), context, base_dir=base_dir, required=True, label="path")
                source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                if source_key not in context:
                    raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                normalize_mode = _resolve_string_value(step.get("normalize", "none"), context, base_dir=base_dir, label="normalize").lower()
                sort_lines = _coerce_bool(step.get("sort", False), label="sort")
                value = context[source_key]
                if isinstance(value, list):
                    incoming_lines = [_context_value_to_text(item).strip() for item in value if _context_value_to_text(item).strip()]
                else:
                    incoming_lines = [line.strip() for line in _context_value_to_text(value).splitlines() if line.strip()]

                existing_lines: list[str] = []
                if path.exists():
                    existing_lines = [line.strip() for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]

                def canonical(line: str) -> str:
                    if normalize_mode == "lower":
                        return line.strip().lower()
                    return line.strip()

                existing_keys = {canonical(line) for line in existing_lines if canonical(line)}
                merged: list[str] = []
                seen: set[str] = set()
                added_lines: list[str] = []
                for line in existing_lines + incoming_lines:
                    key = canonical(line)
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    normalized_line = line.strip().lower() if normalize_mode == "lower" else line.strip()
                    if key not in existing_keys and canonical(line) in {canonical(item) for item in incoming_lines if canonical(item)}:
                        added_lines.append(normalized_line)
                    merged.append(normalized_line)
                if sort_lines:
                    merged = sorted(merged)
                    added_lines = sorted(set(added_lines))
                _write_value(path, "\n".join(merged) + ("\n" if merged else ""))
                unique_incoming_keys = {canonical(line) for line in incoming_lines if canonical(line)}
                duplicate_count = len(unique_incoming_keys & existing_keys)
                result = {
                    "path": str(path),
                    "existing_count_before": len(existing_lines),
                    "incoming_count": len(incoming_lines),
                    "incoming_unique_count": len(unique_incoming_keys),
                    "added_lines": added_lines,
                    "line_count": len(merged),
                    "added_count": len(added_lines),
                    "duplicate_count": duplicate_count,
                }
                out_key = _store_output(context, step, result, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> updated {path} ({len(merged)} unique line(s))")

            elif stype == "append_to_file":
                path = _resolve_path(step.get("path"), context, base_dir=base_dir, required=True, label="path")
                normalize_mode = _resolve_string_value(step.get("normalize", "none"), context, base_dir=base_dir, label="normalize").lower()
                unique_only = _coerce_bool(step.get("unique", False), label="unique")
                ensure_trailing_newline = _coerce_bool(step.get("newline", True), label="newline")
                if step.get("source_key"):
                    source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                    if source_key not in context:
                        raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                    value = context[source_key]
                elif step.get("source_path"):
                    source_path = _resolve_path(step.get("source_path"), context, base_dir=base_dir, required=True, label="source_path")
                    value = _load_file_text(source_path, context=context, label=f"Source file in step {idx}", base_dir=base_dir)
                else:
                    value = _resolve_text_source(step, "text", context, base_dir=base_dir, required=False, default="")

                if isinstance(value, list):
                    raw_lines = [_context_value_to_text(item).strip() for item in value if _context_value_to_text(item).strip()]
                else:
                    raw_text = _context_value_to_text(value)
                    raw_lines = [line.strip() for line in raw_text.splitlines() if line.strip()] if "\n" in raw_text else ([raw_text.strip()] if raw_text.strip() else [])

                def canonical(line: str) -> str:
                    if normalize_mode == "lower":
                        return line.strip().lower()
                    return line.strip()

                existing_lines: list[str] = []
                if path.exists():
                    existing_lines = [line.rstrip("\r\n") for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]
                existing_keys = {canonical(line) for line in existing_lines if canonical(line)}

                appended_lines: list[str] = []
                for line in raw_lines:
                    normalized_line = canonical(line) if normalize_mode == "lower" else line.strip()
                    key = canonical(normalized_line)
                    if not key:
                        continue
                    if unique_only and key in existing_keys:
                        continue
                    appended_lines.append(normalized_line)
                    if unique_only:
                        existing_keys.add(key)

                path.parent.mkdir(parents=True, exist_ok=True)
                existing_text = path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
                append_text = "\n".join(appended_lines)
                if append_text:
                    if existing_text and not existing_text.endswith(("\n", "\r")):
                        existing_text += "\n"
                    existing_text += append_text
                    if ensure_trailing_newline:
                        existing_text += "\n"
                    path.write_text(existing_text, encoding="utf-8")

                result = {
                    "path": str(path),
                    "incoming_count": len(raw_lines),
                    "appended_count": len(appended_lines),
                    "appended_lines": appended_lines,
                    "unique": unique_only,
                }
                out_key = _store_output(context, step, result, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> appended {len(appended_lines)} line(s) to {path}")

            elif stype == "sync_history_emails_to_file":
                history_root = _resolve_path(step.get("history_root"), context, base_dir=base_dir, required=True, label="history_root")
                target_path = _resolve_path(step.get("path"), context, base_dir=base_dir, required=True, label="path")
                normalize_mode = _resolve_string_value(step.get("normalize", "lower"), context, base_dir=base_dir, label="normalize").lower()
                sort_lines = _coerce_bool(step.get("sort", True), label="sort")
                sent_prefix = _resolve_string_value(step.get("sent_prefix", "sent_"), context, base_dir=base_dir, label="sent_prefix")
                discovered: list[str] = []
                if history_root.exists():
                    for folder in sorted(path for path in history_root.iterdir() if path.is_dir()):
                        folder_name = folder.name.strip()
                        if "@" not in folder_name:
                            continue
                        if any(child.is_file() and child.name.startswith(sent_prefix) for child in folder.iterdir()):
                            discovered.append(folder_name)

                existing_lines: list[str] = []
                if target_path.exists():
                    existing_lines = [line.strip() for line in target_path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]

                def canonical(line: str) -> str:
                    if normalize_mode == "lower":
                        return line.strip().lower()
                    return line.strip()

                existing_keys = {canonical(line) for line in existing_lines if canonical(line)}
                merged: list[str] = []
                seen: set[str] = set()
                added_lines: list[str] = []
                for line in existing_lines + discovered:
                    key = canonical(line)
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    normalized_line = key if normalize_mode == "lower" else line.strip()
                    if key not in existing_keys:
                        added_lines.append(normalized_line)
                    merged.append(normalized_line)
                if sort_lines:
                    merged = sorted(merged)
                    added_lines = sorted(added_lines)
                _write_value(target_path, "\n".join(merged) + ("\n" if merged else ""))
                result = {
                    "path": str(target_path),
                    "discovered_count": len(discovered),
                    "existing_count_before": len(existing_lines),
                    "line_count": len(merged),
                    "added_count": len(added_lines),
                    "added_lines": added_lines,
                }
                out_key = _store_output(context, step, result, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> synced {len(added_lines)} history email(s) into {target_path}")

            elif stype == "file_contains_line":
                path = _resolve_path(step.get("path"), context, base_dir=base_dir, required=True, label="path")
                line_value = _resolve_text_source(step, "text", context, base_dir=base_dir, required=True, default="")
                normalize_mode = _resolve_string_value(step.get("normalize", "lower"), context, base_dir=base_dir, label="normalize").lower()
                if normalize_mode == "lower":
                    needle = line_value.strip().lower()
                else:
                    needle = line_value.strip()
                matched = False
                if path.exists() and needle:
                    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                        candidate = raw_line.strip().lower() if normalize_mode == "lower" else raw_line.strip()
                        if candidate == needle:
                            matched = True
                            break
                out_key = _store_output(context, step, matched, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} = {matched}")

            elif stype == "output":
                if step.get("source_key"):
                    source_key = _resolve_string_value(step.get("source_key"), context, base_dir=base_dir, required=True, label="source_key")
                    if source_key not in context:
                        raise RuntimeError(f"Step {idx} source_key not found in context: {source_key}")
                    value: Any = context[source_key]
                elif step.get("path"):
                    file_path = _resolve_path(step.get("path"), context, base_dir=base_dir, required=True, label="path")
                    value = _load_file_text(file_path, context=context, label=f"Output file in step {idx}", base_dir=base_dir)
                else:
                    value = _resolve_text_source(step, "text", context, base_dir=base_dir, required=False, default="")
                label_text = _resolve_text_source(step, "label", context, base_dir=base_dir, required=False, default="").strip()
                rendered = _context_value_to_text(value)
                if label_text:
                    print(f"{label_text}\n{rendered}", flush=True)
                else:
                    print(rendered, flush=True)
                if step.get("output") or step.get("output_file"):
                    out_key = _store_output(context, step, value, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                    _log(verbose, 1, f"  -> stored {out_key}")

            elif stype == "send_all_files_in_folder_to_AI":
                folder = _resolve_path(step.get("folder"), context, base_dir=base_dir, required=True, label="folder")
                include_pattern = _resolve_string_value(step.get("regex_filter") or step.get("include_regex"), context, base_dir=base_dir, label="include_regex") if (step.get("regex_filter") or step.get("include_regex")) else None
                exclude_pattern = _resolve_string_value(step.get("exclude_regex"), context, base_dir=base_dir, label="exclude_regex") if step.get("exclude_regex") else None
                include_regex = _compile_regex(include_pattern, label="include_regex")
                exclude_regex = _compile_regex(exclude_pattern, label="exclude_regex")
                recursive = _coerce_bool(step.get("recursive", True), label="recursive")
                max_files = int(_resolve_number(step.get("max_files"), context, base_dir=base_dir, label="max_files", default=0, as_int=True) or 0)
                throttle = float(_resolve_number(step.get("throttle_seconds"), context, base_dir=base_dir, label="throttle_seconds", default=args.throttle_seconds))
                files = list(_iter_matching_text_files(folder, recursive=recursive, include_regex=include_regex, exclude_regex=exclude_regex))
                if max_files > 0:
                    files = files[:max_files]
                if not files:
                    raise RuntimeError(f"Step {idx} found no matching files in folder: {folder}")
                system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You analyze provided file content.")
                prompt = _resolve_text_source(step, "prompt", context, base_dir=base_dir, required=False, default="")
                chat_timeout = _workflow_step_http_timeout_seconds(step, context, base_dir=base_dir, args=args)
                results: list[dict[str, Any]] = []
                for file_index, file_path in enumerate(files, start=1):
                    file_text = _load_file_text(file_path, context=context, label=f"Folder file {file_path}", base_dir=base_dir)
                    file_context = {**context, "file_name": file_path.name, "file_path": str(file_path), "relative_path": str(file_path.relative_to(folder))}
                    prompt_for_file = render_template(prompt, file_context, strict=True, label="prompt", base_dir=base_dir)
                    user = _compose_prompt_segments([prompt_for_file, f"File path: {file_path.relative_to(folder)}", file_text])
                    response = ai.chat(
                        system=system,
                        user=user,
                        temperature=float(_resolve_number(step.get("temperature"), context, base_dir=base_dir, label="temperature", default=args.temperature)),
                        max_tokens=int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True)),
                        timeout_seconds=chat_timeout,
                    )
                    results.append({"file": str(file_path), "relative_path": str(file_path.relative_to(folder)), "response": response})
                    _log(verbose, 2, f"    processed [{file_index}/{len(files)}] {file_path.relative_to(folder)}")
                    _sleep_if_needed(throttle)
                output_mode = _resolve_string_value(step.get("output_mode", "json"), context, base_dir=base_dir, label="output_mode").lower()
                value = results if output_mode == "json" else "\n\n".join(
                    f"===== {item['relative_path']} =====\n{item['response']}" for item in results
                )
                out_key = _store_output(context, step, value, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} from {len(results)} AI response(s)")

            elif stype == "generate_list_from_API":
                system = _resolve_text_source(step, "system", context, base_dir=base_dir, required=False, default="You generate structured lists. Return only valid JSON.")
                user = _build_ai_user_text(step, context, base_dir=base_dir, input_field="input")
                chat_timeout = _workflow_step_http_timeout_seconds(step, context, base_dir=base_dir, args=args)
                response = ai.chat(
                    system=system,
                    user=user,
                    temperature=float(_resolve_number(step.get("temperature"), context, base_dir=base_dir, label="temperature", default=args.temperature)),
                    max_tokens=int(_resolve_number(step.get("max_tokens"), context, base_dir=base_dir, label="max_tokens", default=args.max_tokens, as_int=True)),
                    timeout_seconds=chat_timeout,
                )
                try:
                    parsed = _parse_json_from_model_text(response, label=f"generate_list_from_API step {idx}")
                except RuntimeError as ex:
                    raise RuntimeError(f"Step {idx} generate_list_from_API expected JSON list output. {ex}") from ex
                if not isinstance(parsed, list):
                    raise RuntimeError(f"Step {idx} generate_list_from_API expected a JSON list.")
                out_key = _store_output(context, step, parsed, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(parsed)} item(s))")

            elif stype == "get_all_messages":
                with project.connect() as conn:
                    rows = _query_messages(
                        conn,
                        conversation_id=_resolve_string_value(step.get("conversation_id"), context, base_dir=base_dir, label="conversation_id") if step.get("conversation_id") else None,
                        client_id=_resolve_string_value(step.get("client_id"), context, base_dir=base_dir, label="client_id") if step.get("client_id") else None,
                        client_name=_resolve_string_value(step.get("client_name"), context, base_dir=base_dir, label="client_name") if step.get("client_name") else None,
                        agent_id=_resolve_string_value(step.get("agent_id"), context, base_dir=base_dir, label="agent_id") if step.get("agent_id") else None,
                        direction=_resolve_string_value(step.get("direction"), context, base_dir=base_dir, label="direction") if step.get("direction") else None,
                        channel_type=_resolve_string_value(step.get("channel_type"), context, base_dir=base_dir, label="channel_type") if step.get("channel_type") else None,
                        limit=int(_resolve_number(step.get("limit"), context, base_dir=base_dir, label="limit", default=0, as_int=True) or 0) or None,
                        order=_resolve_string_value(step.get("order", "asc"), context, base_dir=base_dir, label="order"),
                        regex_filter=_resolve_string_value(step.get("regex_filter"), context, base_dir=base_dir, label="regex_filter") if step.get("regex_filter") else None,
                    )
                output_mode = _resolve_string_value(step.get("output_mode", "json"), context, base_dir=base_dir, label="output_mode").lower()
                value = rows if output_mode == "json" else _format_message_rows(rows)
                out_key = _store_output(context, step, value, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(rows)} message(s))")

            elif stype == "get_all_messages_from_client":
                client_id = step.get("client_id")
                client_name = step.get("client_name")
                if not client_id and not client_name:
                    raise RuntimeError(f"Step {idx} get_all_messages_from_client requires 'client_id' or 'client_name'.")
                with project.connect() as conn:
                    rows = _query_messages(
                        conn,
                        client_id=_resolve_string_value(client_id, context, base_dir=base_dir, label="client_id") if client_id else None,
                        client_name=_resolve_string_value(client_name, context, base_dir=base_dir, label="client_name") if client_name else None,
                        agent_id=_resolve_string_value(step.get("agent_id"), context, base_dir=base_dir, label="agent_id") if step.get("agent_id") else None,
                        direction=_resolve_string_value(step.get("direction"), context, base_dir=base_dir, label="direction") if step.get("direction") else None,
                        channel_type=_resolve_string_value(step.get("channel_type"), context, base_dir=base_dir, label="channel_type") if step.get("channel_type") else None,
                        limit=int(_resolve_number(step.get("limit"), context, base_dir=base_dir, label="limit", default=0, as_int=True) or 0) or None,
                        order=_resolve_string_value(step.get("order", "asc"), context, base_dir=base_dir, label="order"),
                        regex_filter=_resolve_string_value(step.get("regex_filter"), context, base_dir=base_dir, label="regex_filter") if step.get("regex_filter") else None,
                    )
                output_mode = _resolve_string_value(step.get("output_mode", "json"), context, base_dir=base_dir, label="output_mode").lower()
                value = rows if output_mode == "json" else _format_message_rows(rows)
                out_key = _store_output(context, step, value, default_key=f"step_{idx}", base_dir=base_dir, verbose=verbose)
                _log(verbose, 1, f"  -> stored {out_key} ({len(rows)} client message(s))")

            elif stype == "send_email":
                to_address = _resolve_text_source(step, "to", context, base_dir=base_dir, required=True, default="")
                from_address = _resolve_text_source(step, "from", context, base_dir=base_dir, required=True, default="")
                subject = _resolve_text_source(step, "subject", context, base_dir=base_dir, required=True, default="")
                body = _resolve_text_source(step, "body", context, base_dir=base_dir, required=True, default="")
                reply_to_message_id = _resolve_text_source(step, "reply_to_message_id", context, base_dir=base_dir, required=False, default="") if step.get("reply_to_message_id") else ""
                reply_to_thread_id = _resolve_text_source(step, "reply_to_thread_id", context, base_dir=base_dir, required=False, default="") if step.get("reply_to_thread_id") else ""
                reply_references = _resolve_text_source(step, "reply_references", context, base_dir=base_dir, required=False, default="") if step.get("reply_references") else ""
                quote_original = _coerce_bool(step.get("quote_original_message", False), label="quote_original_message")
                if quote_original:
                    quoted_from = _resolve_text_source(step, "quoted_from", context, base_dir=base_dir, required=False, default="") if step.get("quoted_from") else ""
                    quoted_date = _resolve_text_source(step, "quoted_date", context, base_dir=base_dir, required=False, default="") if step.get("quoted_date") else ""
                    quoted_body = _resolve_text_source(step, "quoted_body", context, base_dir=base_dir, required=False, default="") if step.get("quoted_body") else ""
                    body = _build_reply_body(body, quoted_from, quoted_date, quoted_body)
                normalized_in_reply_to = _normalize_message_id_header(reply_to_message_id) if reply_to_message_id else ""
                normalized_references = " ".join(
                    part for part in [
                        reply_references.strip(),
                        normalized_in_reply_to,
                    ]
                    if part
                ).strip()
                dry_run = _coerce_bool(step.get("dry_run", args.dry_run), label="dry_run")
                throttle = float(_resolve_number(step.get("throttle_seconds") or step.get("throttle"), context, base_dir=base_dir, label="throttle_seconds", default=args.throttle_seconds))
                gmail = _ensure_gmail_client_by_address(project, gmail_cache, from_address)
                message_record: dict[str, Any] = {
                    "to": to_address,
                    "from": from_address,
                    "subject": subject,
                    "body": body,
                    "dry_run": dry_run,
                }
                if dry_run:
                    try:
                        draft_result = _send_gmail_message_checked(
                            project,
                            gmail,
                            to_email=to_address,
                            subject=subject,
                            body=body,
                            source_label=f"workflow:{wf_path.name}",
                            from_email=from_address,
                            thread_id=reply_to_thread_id or None,
                            in_reply_to=normalized_in_reply_to or None,
                            references=normalized_references or None,
                            dry_run=True,
                        )
                        message_record.update(draft_result)
                        if draft_result.get("blocked"):
                            context["skip_archive_current_message"] = True
                            _log(verbose, 1, "  -> outbound email blocked by last-chance review; draft saved and user notification added")
                        else:
                            _log(verbose, 1, f"  -> draft created for {to_address}")
                    except Exception as ex:
                        error_text = str(ex)
                        if _is_gmail_scope_error(error_text):
                            error_text = f"{error_text}\n\n{_gmail_oauth_command_hint(from_address)}"
                            message_record["scope_help"] = _gmail_oauth_command_hint(from_address)
                        message_record["draft_error"] = error_text
                        message_record["draft_created"] = False
                        _log(verbose, 1, f"  -> draft creation skipped for {to_address}: {error_text}")
                else:
                    try:
                        send_result = _send_gmail_message_checked(
                            project,
                            gmail,
                            to_email=to_address,
                            subject=subject,
                            body=body,
                            source_label=f"workflow:{wf_path.name}",
                            from_email=from_address,
                            thread_id=reply_to_thread_id or None,
                            in_reply_to=normalized_in_reply_to or None,
                            references=normalized_references or None,
                        )
                        message_record.update(send_result)
                        if send_result.get("blocked"):
                            context["skip_archive_current_message"] = True
                            _log(verbose, 1, "  -> outbound email blocked by last-chance review; draft saved and user notification added")
                        else:
                            if workflow_type == "iteration_over_gmail_inbox" and message_record.get("sent"):
                                context["skip_archive_current_message"] = True
                            _log(verbose, 1, f"  -> email sent to {to_address}")
                    except Exception as ex:
                        error_text = str(ex)
                        if _is_gmail_scope_error(error_text):
                            error_text = f"{error_text}\n\n{_gmail_oauth_command_hint(from_address)}"
                        raise RuntimeError(error_text) from ex
                if step.get("output_file"):
                    requested_output_path = _resolve_path(step.get("output_file"), context, base_dir=base_dir, required=True, label="output_file")
                    output_path = _resolve_new_output_path(requested_output_path)
                    _write_value(output_path, f"Subject: {subject}\n\nBody:\n{body}\n")
                    message_record["output_file"] = str(output_path)
                    if output_path != requested_output_path:
                        _log(verbose, 1, f"  -> wrote {output_path} (requested {requested_output_path} already existed)")
                    else:
                        _log(verbose, 1, f"  -> wrote {output_path}")
                context[str(step.get("output", f"send_email_result_{idx}"))] = message_record
                if message_record.get("sent"):
                    inbound_link = str(context.get("received_message_path") or context.get("current_email_source_file") or "")
                    outbound_link = str(message_record.get("output_file") or "")
                    _append_active_exec_log(
                        event_type="message_response",
                        title="Agent responded to a message",
                        body=(
                            f"To: {to_address}\n"
                            f"From: {from_address}\n"
                            f"Subject: {subject}\n"
                            f"History message link: {inbound_link}\n"
                            f"Sent message link: {outbound_link}"
                        ),
                    )
                elif message_record.get("blocked"):
                    _append_active_exec_log(
                        event_type="warning",
                        title="Outbound message blocked by last-chance review",
                        body=(
                            f"To: {to_address}\n"
                            f"From: {from_address}\n"
                            f"Subject: {subject}\n"
                            f"Draft id: {str(message_record.get('draft_id') or '')}\n"
                            f"Notification id: {str(message_record.get('notification_id') or '')}\n"
                            f"Review findings:\n{json.dumps(message_record.get('review') or {}, indent=2, ensure_ascii=False, default=str)}"
                        ),
                    )
                if message_record.get("blocked"):
                    break
                _sleep_if_needed(throttle)

            elif stype == "send_fetlife_message":
                conversation_id = _resolve_text_source(step, "conversation_id", context, base_dir=base_dir, required=True, default="")
                body = _resolve_text_source(step, "body", context, base_dir=base_dir, required=True, default="")
                dry_run = _coerce_bool(step.get("dry_run", args.dry_run), label="dry_run")
                throttle = float(_resolve_number(step.get("throttle_seconds") or step.get("throttle"), context, base_dir=base_dir, label="throttle_seconds", default=args.throttle_seconds))
                profile_directory = _resolve_string_value(
                    step.get("fetlife_profile_directory", context.get("fetlife_profile_directory", "Profile 9")),
                    context,
                    base_dir=base_dir,
                    label="fetlife_profile_directory",
                )
                user_data_dir = (
                    _resolve_path(step.get("fetlife_user_data_dir"), context, base_dir=base_dir, required=True, label="fetlife_user_data_dir")
                    if step.get("fetlife_user_data_dir")
                    else Path(str(context.get("fetlife_user_data_dir") or _default_fetlife_user_data_dir(project)))
                )
                review = _review_outbound_email_text("", body)
                message_record = {
                    "conversation_id": conversation_id,
                    "body": body,
                    "dry_run": dry_run,
                    "review": review,
                }
                if review["blocked"]:
                    notification = _append_user_notification(
                        project,
                        title="Blocked outbound FetLife message",
                        message=(
                            "A FetLife reply was blocked by the last-chance review because it still contains placeholder-like text."
                        ),
                        category="fetlife_send_blocked",
                        details={
                            "conversation_id": str(conversation_id),
                            "review": review,
                        },
                    )
                    message_record["blocked"] = True
                    message_record["notification_id"] = str(notification.get("id") or "")
                    _log(verbose, 1, "  -> outbound FetLife message blocked by last-chance review; notification added")
                elif dry_run:
                    message_record["sent"] = False
                    message_record["dry_run_only"] = True
                    _log(verbose, 1, f"  -> FetLife dry run ready for conversation {conversation_id}")
                else:
                    fetlife = _ensure_fetlife_client(
                        project,
                        fetlife_cache,
                        user_data_dir=user_data_dir,
                        profile_directory=profile_directory,
                    )
                    send_result = fetlife.send_message(conversation_id, body)
                    message_record.update(send_result)
                    _log(verbose, 1, f"  -> FetLife message sent to conversation {conversation_id}")
                if step.get("output_file"):
                    requested_output_path = _resolve_path(step.get("output_file"), context, base_dir=base_dir, required=True, label="output_file")
                    output_path = _resolve_new_output_path(requested_output_path)
                    _write_value(output_path, f"Body:\n{body}\n")
                    message_record["output_file"] = str(output_path)
                    if output_path != requested_output_path:
                        _log(verbose, 1, f"  -> wrote {output_path} (requested {requested_output_path} already existed)")
                    else:
                        _log(verbose, 1, f"  -> wrote {output_path}")
                context[str(step.get("output", f"send_fetlife_result_{idx}"))] = message_record
                if message_record.get("sent"):
                    inbound_link = str(context.get("received_message_path") or "")
                    outbound_link = str(message_record.get("output_file") or "")
                    _append_active_exec_log(
                        event_type="message_response",
                        title="Agent responded to a FetLife message",
                        body=(
                            f"Conversation ID: {conversation_id}\n"
                            f"Participant: {str(context.get('current_fetlife_participant_name') or '')}\n"
                            f"History message link: {inbound_link}\n"
                            f"Sent message link: {outbound_link}"
                        ),
                    )
                elif message_record.get("blocked"):
                    _append_active_exec_log(
                        event_type="warning",
                        title="Outbound FetLife message blocked by last-chance review",
                        body=(
                            f"Conversation ID: {conversation_id}\n"
                            f"Notification id: {str(message_record.get('notification_id') or '')}\n"
                            f"Review findings:\n{json.dumps(review, indent=2, ensure_ascii=False, default=str)}"
                        ),
                    )
                if message_record.get("blocked"):
                    break
                _sleep_if_needed(throttle)

            elif stype == "send_email_list":
                channel_id = _resolve_string_value(step.get("channel_id") or "", context, base_dir=base_dir, required=True, label="channel_id")
                recipients_file = _resolve_path(step.get("recipients_file", ""), context, base_dir=base_dir, required=True, label="recipients_file")
                recipients = _read_recipients(recipients_file)
                recipients_pattern = _resolve_string_value(step.get("regex_filter"), context, base_dir=base_dir, label="regex_filter") if step.get("regex_filter") else None
                recipients_regex = _compile_regex(recipients_pattern, label="regex_filter")
                if recipients_regex:
                    recipients = [recipient for recipient in recipients if recipients_regex.search(recipient)]
                if not recipients:
                    raise RuntimeError(f"Step {idx} has no recipients in {recipients_file}.")
                gmail = _ensure_gmail_client(project, gmail_cache, channel_id)
                subject = _resolve_text_source(step, "subject", context, base_dir=base_dir, required=False, default="TaiLOR update")
                body = _resolve_text_source(step, "body", context, base_dir=base_dir, required=True, default="")
                dry_run = _coerce_bool(step.get("dry_run", args.dry_run), label="dry_run")
                throttle = float(_resolve_number(step.get("throttle_seconds"), context, base_dir=base_dir, label="throttle_seconds", default=args.throttle_seconds))
                sent, failed = _bulk_send(project, gmail, recipients, subject, body, dry_run=dry_run, throttle_seconds=throttle)
                out_key = _store_output(
                    context,
                    step,
                    {"sent": sent, "failed": failed, "recipients": recipients},
                    default_key=f"email_result_{idx}",
                    base_dir=base_dir,
                    verbose=verbose,
                )
                _log(verbose, 1, f"  -> email batch done. stored {out_key}. sent={sent} failed={failed}")

            else:
                raise RuntimeError(f"Unknown step type at step {idx}: {stype}")

            _sleep_if_needed(_step_delay(step, "delay_after_seconds", context, base_dir=base_dir))

        if workflow_type == "iteration_over_gmail_inbox" and archive_after_iteration and not _coerce_bool(context.get("skip_archive_current_message", False), label="skip_archive_current_message"):
            message_id = str(context.get("current_message_id", "") or "")
            if message_id:
                gmail = _ensure_gmail_client_by_address(project, gmail_cache, from_address)
                archive_result = gmail.mark_read_and_archive(message_id)
                context["archive_result"] = archive_result
                _log(1, 1, f"  -> archived Gmail message {message_id}")

    if args.output_context_json:
        requested_context_path = Path(args.output_context_json)
        output_context_path = _write_output_context_json(args.output_context_json, context)
        if output_context_path != requested_context_path:
            print(f"Wrote context: {output_context_path} (requested {requested_context_path} already existed)")
        else:
            print(f"Wrote context: {output_context_path}")
    _append_active_exec_log(
        event_type="workflow_complete",
        title="Workflow finished",
        body=(
            f"Workflow file: {wf_path}\n"
            f"Workflow type: {workflow_type or 'single_run'}\n"
            f"Iterations processed: {len(iteration_items)}\n"
            f"Final context output: {str(args.output_context_json or '')}"
        ),
    )
    _append_engine_log(
        project,
        {
            "event": "workflow_complete",
            "agent": str(base_context.get("agent_key") or _infer_agent_name_from_workflow_path(project, wf_path)),
            "workflow": wf_path.stem,
            "workflow_file": str(wf_path),
            "detail": f"type={workflow_type or 'single_run'}; iterations={len(iteration_items)}; steps={len(steps)}",
        },
    )
    _close_fetlife_clients(fetlife_cache)
    return 0


# ----------------------------
# Dev utilities
# ----------------------------

def _iter_text_files(folder: Path) -> Iterable[Path]:
    for p in sorted(folder.rglob("*")):
        if p.is_file() and p.suffix.lower() not in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".db"}:
            yield p


def cmd_dev_folder_each(args: argparse.Namespace, project: TailorProject) -> int:
    ai = AiClient(model=args.model, api_key=args.api_key, endpoint=args.ai_endpoint)
    folder = Path(args.folder)
    ensure_file(folder, "Folder")

    out = []
    for f in _iter_text_files(folder):
        text = f.read_text(encoding="utf-8", errors="replace")
        user = f"File path: {f}\n\n{text[:args.max_chars_per_file]}"
        resp = ai.chat(system=args.system, user=user, temperature=args.temperature, max_tokens=args.max_tokens)
        out.append({"file": str(f), "response": resp})
        print(f"Processed: {f}")

    if args.output_json:
        Path(args.output_json).write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(f"Wrote {args.output_json}")
    return 0


def cmd_dev_folder_combined(args: argparse.Namespace, project: TailorProject) -> int:
    ai = AiClient(model=args.model, api_key=args.api_key, endpoint=args.ai_endpoint)
    folder = Path(args.folder)
    ensure_file(folder, "Folder")

    chunks = []
    for f in _iter_text_files(folder):
        text = f.read_text(encoding="utf-8", errors="replace")
        chunks.append(f"\n\n===== {f} =====\n{text[:args.max_chars_per_file]}")
    combined = "".join(chunks)[: args.max_chars_total]

    resp = ai.chat(system=args.system, user=combined, temperature=args.temperature, max_tokens=args.max_tokens)
    print(resp)
    if args.output_file:
        Path(args.output_file).write_text(resp, encoding="utf-8")
    return 0


def cmd_dev_ai_bounce(args: argparse.Namespace, project: TailorProject) -> int:
    ai = AiClient(model=args.model, api_key=args.api_key, endpoint=args.ai_endpoint)
    text = args.input_text
    for i in range(args.rounds):
        text = ai.chat(system=args.system, user=text, temperature=args.temperature, max_tokens=args.max_tokens)
        print(f"--- Round {i + 1} ---")
        print(text)
    if args.output_file:
        Path(args.output_file).write_text(text, encoding="utf-8")
    return 0


_DIAGNOSTIC_LOG_HINTS = (
    ".log",
    ".err",
    ".out",
    ".trace",
    ".output",
    ".ndjson",
)

_CHECK_LOG_HINTS = (
    ".log",
    ".err",
    ".out",
    ".trace",
    ".ndjson",
)

_DIAGNOSTIC_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("exception", re.compile(r"\b(exception|traceback|stack trace)\b", re.IGNORECASE)),
    ("error", re.compile(r"\b(error|failed|failure|fatal)\b", re.IGNORECASE)),
    ("http", re.compile(r"\bhttp\s+(4\d\d|5\d\d)\b|\bstatus[=: ]+(4\d\d|5\d\d)\b", re.IGNORECASE)),
    ("workflow", re.compile(r"\b(unresolved variables?|missing required value|invalid template expression)\b", re.IGNORECASE)),
    ("leads", re.compile(r"\b(did not return enough emails after retries|no extracted email recipients matched|0 emails?)\b", re.IGNORECASE)),
]


def _iter_recent_diagnostic_files(root: Path, since_utc: dt.datetime, max_files: int) -> list[Path]:
    candidates: list[tuple[float, Path]] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        lowered = path.name.lower()
        if not any(hint in lowered for hint in _DIAGNOSTIC_LOG_HINTS):
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        modified = dt.datetime.fromtimestamp(mtime, tz=UTC)
        if modified < since_utc:
            continue
        candidates.append((mtime, path))
    candidates.sort(key=lambda item: item[0], reverse=True)
    return [path for _, path in candidates[: max(1, max_files)]]


def _summarize_log_text(text: str, max_lines: int = 12) -> tuple[list[dict[str, str]], dict[str, int]]:
    findings: list[dict[str, str]] = []
    counts: dict[str, int] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        matched_tags: list[str] = []
        for tag, pattern in _DIAGNOSTIC_PATTERNS:
            if pattern.search(line):
                counts[tag] = counts.get(tag, 0) + 1
                matched_tags.append(tag)
        if matched_tags and len(findings) < max_lines:
            findings.append({"tags": ", ".join(matched_tags), "line": line[:1200]})
    return findings, counts


def _tail_excerpt_lines(text: str, max_lines: int = 8) -> list[str]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return [line[:1200] for line in lines[-max_lines:]]


def _diagnostics_report_text(report: dict[str, Any]) -> str:
    lines = [
        f"Diagnostics window: last {report['hours']} hour(s)",
        f"Scanned files: {report['scanned_file_count']}",
        f"Files with suspicious lines: {report['problem_file_count']}",
    ]
    if report.get("pattern_totals"):
        totals = ", ".join(f"{key}={value}" for key, value in sorted(report["pattern_totals"].items()))
        lines.append(f"Pattern totals: {totals}")
    for entry in report.get("files", []):
        lines.append("")
        lines.append(f"- {entry['path']}")
        lines.append(f"  modified_utc={entry['modified_utc']} suspicious_lines={entry['suspicious_line_count']}")
        for finding in entry.get("findings", []):
            lines.append(f"  [{finding['tags']}] {finding['line']}")
    if report.get("ai_summary"):
        lines.append("")
        lines.append("AI summary:")
        lines.append(report["ai_summary"])
    return "\n".join(lines)


def _ask_ai_for_diagnostics(args: argparse.Namespace, report: dict[str, Any]) -> str | None:
    api_key = getattr(args, "api_key", None) or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    ai = AiClient(
        model=getattr(args, "model", "gpt-4o-mini"),
        api_key=api_key,
        endpoint=getattr(args, "ai_endpoint", "https://api.openai.com/v1/chat/completions"),
    )
    evidence_lines: list[str] = []
    evidence_entries = report.get("files") or report.get("scanned_samples") or []
    for entry in evidence_entries:
        evidence_lines.append(f"FILE: {entry['path']}")
        evidence_lines.append(f"MODIFIED_UTC: {entry.get('modified_utc')}")
        for finding in entry.get("findings", []):
            evidence_lines.append(f"[{finding['tags']}] {finding['line']}")
        for line in entry.get("tail_excerpt", []):
            evidence_lines.append(f"[tail] {line}")
        evidence_lines.append("")
    user = (
        "Review these recent TaiLOR logs and output excerpts.\n"
        "Explain what the logs indicate, identify likely root causes, and list the next checks or fixes.\n"
        "Be concise but specific.\n\n"
        + "\n".join(evidence_lines[:4000])
    )
    return ai.chat(
        system="You diagnose automation-system logs. Focus on concrete failures, likely causes, and the next validation steps.",
        user=user,
        temperature=0.1,
        max_tokens=getattr(args, "max_tokens", 1400),
    ).strip()


def cmd_diagnostics(args: argparse.Namespace, project: TailorProject) -> int:
    if getattr(args, "cmd", None) == "diagnostics":
        hours = max(1, int(getattr(args, "hours", 24)))
        max_files = max(1, int(getattr(args, "max_files", 20)))
        max_chars = max(500, int(getattr(args, "max_chars_per_file", 6000)))
    else:
        hours = max(1, int(getattr(args, "diagnostics_hours", 24)))
        max_files = max(1, int(getattr(args, "diagnostics_max_files", 20)))
        max_chars = max(500, int(getattr(args, "diagnostics_max_chars_per_file", 6000)))
    since_utc = dt.datetime.now(tz=UTC) - dt.timedelta(hours=hours)
    files = _iter_recent_diagnostic_files(project.root, since_utc, max_files=max_files)

    report: dict[str, Any] = {
        "generated_utc": now_iso(),
        "hours": hours,
        "scanned_file_count": len(files),
        "problem_file_count": 0,
        "pattern_totals": {},
        "files": [],
        "scanned_samples": [],
        "ai_summary": None,
    }
    pattern_totals: dict[str, int] = {}
    for path in files:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as ex:
            report["files"].append({
                "path": str(path),
                "modified_utc": None,
                "suspicious_line_count": 1,
                "findings": [{"tags": "read-error", "line": str(ex)}],
            })
            report["problem_file_count"] += 1
            pattern_totals["read-error"] = pattern_totals.get("read-error", 0) + 1
            continue
        findings, counts = _summarize_log_text(text[-max_chars:], max_lines=12)
        if not findings:
            if len(report["scanned_samples"]) < 5:
                report["scanned_samples"].append({
                    "path": str(path.relative_to(project.root)) if path.is_relative_to(project.root) else str(path),
                    "modified_utc": dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat(),
                    "tail_excerpt": _tail_excerpt_lines(text[-max_chars:], max_lines=8),
                })
            continue
        modified_utc = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()
        report["files"].append({
            "path": str(path.relative_to(project.root)) if path.is_relative_to(project.root) else str(path),
            "modified_utc": modified_utc,
            "suspicious_line_count": sum(counts.values()),
            "findings": findings,
            "tail_excerpt": _tail_excerpt_lines(text[-max_chars:], max_lines=5),
        })
        report["problem_file_count"] += 1
        for key, value in counts.items():
            pattern_totals[key] = pattern_totals.get(key, 0) + value
    report["pattern_totals"] = pattern_totals

    if report["files"] or report["scanned_samples"]:
        try:
            report["ai_summary"] = _ask_ai_for_diagnostics(args, report)
        except Exception as ex:
            report["ai_summary"] = f"AI diagnostics request failed: {ex}"

    text_report = _diagnostics_report_text(report)
    print(text_report)

    output_file = getattr(args, "output_file", None)
    if output_file:
        Path(output_file).write_text(text_report, encoding="utf-8")
    output_json = getattr(args, "output_json", None)
    if output_json:
        Path(output_json).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return 0 if report["problem_file_count"] == 0 else 2


def _load_prompt_file(project: "TailorProject", name: str, default_text: str = "") -> str:
    path = project.root / "prompts" / name
    if not path.exists():
        return default_text
    return path.read_text(encoding="utf-8", errors="replace")


def _extract_json_block(text: str) -> Any:
    payload = (text or "").strip()
    if not payload:
        raise RuntimeError("AI returned an empty response when JSON was required.")
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(\{.*\}|\[.*\])\s*```", payload, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        return json.loads(fenced.group(1))
    decoder = json.JSONDecoder()
    for index, char in enumerate(payload):
        if char not in "{[":
            continue
        try:
            obj, end = decoder.raw_decode(payload[index:])
            if payload[index + end :].strip():
                continue
            return obj
        except json.JSONDecodeError:
            continue
    raise RuntimeError(f"AI response did not contain valid JSON. Response excerpt: {trace_excerpt(payload, 500)}")


def _current_ai_client(args: argparse.Namespace, project: "TailorProject") -> AiClient:
    active_profile = _active_ai_profile(project)
    auth_mode = str(active_profile.get("auth_mode") or "")
    api_key = (
        getattr(args, "api_key", None)
        or str(active_profile.get("api_key") or "")
        or (os.getenv("AZURE_OPENAI_API_KEY") if auth_mode == "api-key" else os.getenv("OPENAI_API_KEY"))
    )
    return AiClient(
        model=getattr(args, "model", None) or str(active_profile.get("model") or "gpt-4o-mini"),
        api_key=api_key,
        endpoint=getattr(args, "ai_endpoint", None) or str(active_profile.get("endpoint") or "https://api.openai.com/v1/chat/completions"),
        auth_mode=auth_mode or None,
    )


def _read_text_file_excerpt(path: Path, *, max_chars: int) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 64] + "\n\n[TRUNCATED FOR CONTEXT SIZE]\n"


def _build_agent_and_workflow_generation_documents(project: "TailorProject", *,
                                                   attach_files: Iterable[str] | None = None,
                                                   max_chars_per_file: int = 24000) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_file(path: Path, *, label: str = "") -> None:
        resolved = path if path.is_absolute() else (project.root / path)
        resolved = resolved.resolve()
        key = str(resolved).lower()
        if key in seen or not resolved.exists() or not resolved.is_file():
            return
        seen.add(key)
        relative = str(resolved.relative_to(project.root)) if resolved.is_relative_to(project.root) else str(resolved)
        documents.append(
            {
                "path": relative,
                "label": label or relative,
                "file-contents": _read_text_file_excerpt(resolved, max_chars=max_chars_per_file),
            }
        )

    add_file(project.root / "docs" / "PYTHON_AUTOMATION_CLI.md", label="CLI and workflow documentation")
    add_file(project.root / "Codex_lessons_learned.txt", label="Repository lessons learned")
    add_file(project.root / "agents" / "general" / "workflows" / "fetlife_reply_workflow.json", label="Example FetLife reply workflow")
    add_file(project.root / "agents" / "felicity-single-mom" / "workflows" / "felicity_reply_gmail_workflow.json", label="Example Gmail reply workflow")
    add_file(project.root / "agents" / "derek-tech" / "workflows" / "gather_leads_for_industry_workflow.json", label="Example lead gathering workflow")
    add_file(project.root / "prompts" / "d-and-f_reply_prompt.txt", label="Example agent prompt")

    workflow_support_summary = (
        "Workflow executor reference from scripts/tailor_ops.py\n\n"
        "Supported step types:\n"
        "- combine_files\n- combine_all_files_in_folder\n- load_agent_context\n- list_files_in_folder\n"
        "- count_items\n- compare_number\n- slice_items\n- combine_listed_files\n- select_business_research_target\n"
        "- extract_emails_from_value\n- append_unique_lines\n- append_to_file\n- sync_history_emails_to_file\n"
        "- file_contains_line\n- output\n- file_to_context\n- save_variable_value\n- unified_text_diff\n- path_exists\n- regex_match\n"
        "- any_true\n- skip_remaining_steps_if\n- skip_remaining_steps_unless\n- replace_variables_in_file\n"
        "- get_next_item_in_list\n- ai_chat\n- send_input_to_API\n- send_file_to_AI\n- send_all_files_in_folder_to_AI\n"
        "- generate_list_from_API\n- extract_section_from_file\n- get_all_messages\n- get_all_messages_from_client\n"
        "- send_email\n- send_email_list\n- send_fetlife_message\n\n"
        "Supported workflow types:\n"
        "- single_run\n- iteration_over_list_file\n- iteration_over_gmail_inbox\n- iteration_over_fetlife_inbox\n"
        "- iteration_over_agent_history_folders\n- iteration_over_extracted_emails_from_files\n"
    )
    documents.append(
        {
            "path": "scripts/tailor_ops.py::workflow-exec-summary",
            "label": "Workflow executor supported types summary",
            "file-contents": workflow_support_summary,
        }
    )

    if attach_files:
        for item in attach_files:
            add_file(Path(item), label="User attached file")

    return documents


def _prompt_for_answers(questions: list[str]) -> list[dict[str, str]]:
    answers: list[dict[str, str]] = []
    for index, question in enumerate(questions, start=1):
        print(f"Question {index}: {question}")
        answer = input("Answer: ").strip()
        answers.append({"question": question, "answer": answer})
    return answers


def _write_generated_file_bundle(project: "TailorProject", payload: dict[str, Any]) -> list[str]:
    written_paths: list[str] = []
    files = payload.get("files") or []
    if not isinstance(files, list):
        raise RuntimeError("Generated payload 'files' must be a list.")
    for index, item in enumerate(files, start=1):
        if not isinstance(item, dict):
            raise RuntimeError(f"Generated file entry {index} must be an object.")
        path_text = str(item.get("file-path") or "").strip()
        contents = item.get("file-contents")
        if not path_text:
            raise RuntimeError(f"Generated file entry {index} is missing 'file-path'.")
        if contents is None:
            raise RuntimeError(f"Generated file entry {index} is missing 'file-contents'.")
        target = (project.root / path_text).resolve()
        if not str(target).lower().startswith(str(project.root.resolve()).lower()):
            raise RuntimeError(f"Refusing to write outside project root: {target}")
        output_path = _resolve_new_output_path(target)
        _write_value(output_path, contents)
        written_paths.append(str(output_path))
    return written_paths


def cmd_generate_agent_and_workflows_with_ai(args: argparse.Namespace, project: TailorProject) -> int:
    description = " ".join(getattr(args, "description", []) or []).strip()
    if not description:
        raise RuntimeError("Provide a non-empty description of what should be created.")

    documents = _build_agent_and_workflow_generation_documents(
        project,
        attach_files=getattr(args, "attach_file", None),
        max_chars_per_file=max(2000, int(getattr(args, "max_chars_per_file", 24000) or 24000)),
    )
    ai = _current_ai_client(args, project)

    question_system = _load_prompt_file(
        project,
        "generate_agent_and_workflows_questions_prompt.txt",
        default_text=(
            "You analyze requests to create TaiLOR agents, workflows, and support files.\n"
            "Use the provided repository documents as the source of truth.\n"
            "If clarification is needed, return JSON only in this format:\n"
            "{\"clarifying-questions\":[\"question 1\",\"question 2\"]}\n"
            "Return all clarifying questions in a single list.\n"
            "If no clarification is needed, return:\n"
            "{\"clarifying-questions\":[]}\n"
            "Do not produce any other text."
        ),
    )
    question_user = json.dumps(
        {
            "user-request": description,
            "documents": documents,
        },
        indent=2,
        ensure_ascii=False,
    )
    questions_response = ai.chat(
        system=question_system,
        user=question_user,
        temperature=0.1,
        max_tokens=1200,
    )
    question_payload = _extract_json_block(questions_response)
    questions = question_payload.get("clarifying-questions") or []
    if not isinstance(questions, list) or any(not isinstance(item, str) for item in questions):
        raise RuntimeError("AI clarifying-questions response was not a JSON array of strings.")

    answers: list[dict[str, str]] = []
    if questions:
        print("The AI needs clarification before generating files.")
        answers = _prompt_for_answers([item.strip() for item in questions if item.strip()])

    generation_system = _load_prompt_file(
        project,
        "generate_agent_and_workflows_output_prompt.txt",
        default_text=(
            "You generate TaiLOR agent folders, workflow files, and supporting text files.\n"
            "Use the provided repository documents and answers as the only source of truth.\n"
            "Return JSON only in this exact structure:\n"
            "{\n"
            "  \"files\": [\n"
            "    {\n"
            "      \"file-path\": \"relative/path/from/project/root.ext\",\n"
            "      \"file-contents\": \"full file contents here\"\n"
            "    }\n"
            "  ]\n"
            "}\n"
            "Rules:\n"
            "- Include every file needed for the requested agent/workflow implementation.\n"
            "- Do not include commentary, markdown fences, notes, or explanations.\n"
            "- File contents must be complete and directly writable.\n"
            "- Paths must be relative to the TaiLOR project root.\n"
            "- Prefer JSON workflow files and prompt text files that match the current system patterns.\n"
            "- Reuse existing workflow step types and conventions shown in the provided documents."
        ),
    )
    generation_user = json.dumps(
        {
            "user-request": description,
            "clarifying-questions": questions,
            "clarifying-answers": answers,
            "documents": documents,
        },
        indent=2,
        ensure_ascii=False,
    )
    generated_response = ai.chat(
        system=generation_system,
        user=generation_user,
        temperature=0.2,
        max_tokens=max(1800, int(getattr(args, "max_tokens", 4000) or 4000)),
    )
    generated_payload = _extract_json_block(generated_response)
    if not isinstance(generated_payload, dict):
        raise RuntimeError("Generated payload must be a JSON object.")
    files_value = generated_payload.get("files")
    if not isinstance(files_value, list):
        raise RuntimeError("Generated payload must contain a 'files' array.")

    artifacts_root = project.root / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    generated_at = dt.datetime.now().astimezone()
    output_json = Path(getattr(args, "output_json", "") or "")
    if not output_json:
        output_json = artifacts_root / f"generated_agent_and_workflows_{generated_at.strftime('%Y%m%d_%H%M%S')}.json"
    output_json = _resolve_new_output_path(output_json if output_json.is_absolute() else (project.root / output_json))
    output_json.write_text(json.dumps(generated_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Generated JSON bundle written to: {output_json}")

    if _coerce_bool(getattr(args, "write_files", False), label="write_files"):
        written_paths = _write_generated_file_bundle(project, generated_payload)
        print("Wrote generated files:")
        for path in written_paths:
            print(f"  - {path}")

    return 0


def _mask_secret(value: str, *, prefix: int = 6, suffix: int = 4) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if len(text) <= prefix + suffix:
        return "*" * len(text)
    return f"{text[:prefix]}{'*' * max(4, len(text) - prefix - suffix)}{text[-suffix:]}"


def _current_ai_settings(args: argparse.Namespace, project: TailorProject | None = None) -> dict[str, Any]:
    profile = _active_ai_profile(project) if project is not None else {}
    explicit_key = getattr(args, "api_key", None)
    config_key = str(profile.get("api_key") or "")
    fallback_env = os.getenv("AZURE_OPENAI_API_KEY", "") if str(profile.get("auth_mode") or "") == "api-key" else os.getenv("OPENAI_API_KEY", "")
    resolved_key = explicit_key or config_key or fallback_env
    source = "argument"
    if getattr(args, "_api_key_from_profile", False):
        source = "profile"
    elif not explicit_key:
        source = "profile" if config_key else ("environment" if fallback_env else "missing")
    return {
        "profile_name": str(profile.get("profile_name") or ""),
        "provider_name": str(profile.get("provider_name") or ""),
        "auth_mode": str(profile.get("auth_mode") or ""),
        "model": getattr(args, "model", None) or str(profile.get("model") or "gpt-4o-mini"),
        "endpoint": _normalize_ai_endpoint(getattr(args, "ai_endpoint", None) or str(profile.get("endpoint") or "https://api.openai.com/v1/chat/completions"), str(profile.get("auth_mode") or "")),
        "api_key_configured": bool(resolved_key),
        "api_key_source": source,
        "api_key_masked": _mask_secret(resolved_key),
    }


def _classify_ai_status_error(error_text: str) -> str:
    text = (error_text or "").lower()
    if "missing api key" in text:
        return "missing_api_key"
    if "http 401" in text or "incorrect api key" in text or "invalid api key" in text:
        return "authentication_error"
    if "http 403" in text:
        return "forbidden"
    if "deploymentnotfound" in text or ("http 404" in text and "deployment" in text):
        return "deployment_not_found"
    if "http 429" in text or "rate limit" in text or "quota" in text:
        return "rate_limit_or_quota"
    if any(code in text for code in ["http 500", "http 502", "http 503", "http 504"]):
        return "server_error"
    if any(token in text for token in ["timed out", "timeout", "temporary failure", "name or service not known", "nodename nor servname", "connection refused", "network is unreachable", "urlopen error"]):
        return "network_error"
    return "unknown_error"


def _api_status_text(report: dict[str, Any]) -> str:
    settings = report["settings"]
    lines = [
        f"Checked UTC: {report['checked_utc']}",
        f"Status: {report['status']}",
        f"Category: {report['category']}",
        "AI settings:",
        f"  profile={settings.get('profile_name') or '-'}",
        f"  provider={settings.get('provider_name') or '-'}",
        f"  auth_mode={settings.get('auth_mode') or '-'}",
        f"  model={settings['model']}",
        f"  endpoint={settings['endpoint']}",
        f"  api_key_source={settings['api_key_source']}",
        f"  api_key_configured={settings['api_key_configured']}",
        f"  api_key_masked={settings['api_key_masked'] or '(missing)'}",
    ]
    if report.get("latency_ms") is not None:
        lines.append(f"Latency ms: {report['latency_ms']}")
    if report.get("response_excerpt"):
        lines.append(f"Response excerpt: {report['response_excerpt']}")
    if report.get("error"):
        lines.append(f"Error: {report['error']}")
    return "\n".join(lines)


def cmd_check_api_status(args: argparse.Namespace, project: TailorProject) -> int:
    settings = _current_ai_settings(args, project)
    report: dict[str, Any] = {
        "checked_utc": now_iso(),
        "status": "ok",
        "category": "healthy",
        "settings": settings,
        "latency_ms": None,
        "response_excerpt": "",
        "error": "",
    }
    if not settings["api_key_configured"]:
        report["status"] = "error"
        report["category"] = "missing_api_key"
        report["error"] = "No API key is configured. Set OPENAI_API_KEY or pass --api-key."
    else:
        ai = AiClient(
            model=settings["model"],
            api_key=getattr(args, "api_key", None) or (str(_active_ai_profile(project).get("api_key") or "") or os.getenv("OPENAI_API_KEY") or os.getenv("AZURE_OPENAI_API_KEY")),
            endpoint=settings["endpoint"],
            auth_mode=settings.get("auth_mode"),
        )
        started = time.time()
        try:
            reply = ai.chat(
                system="You are a health check endpoint. Reply with only OK.",
                user="Status check. Reply only with OK.",
                temperature=0.0,
                max_tokens=16,
            )
            report["latency_ms"] = int((time.time() - started) * 1000)
            report["response_excerpt"] = trace_excerpt(reply, 120)
        except Exception as ex:
            report["latency_ms"] = int((time.time() - started) * 1000)
            report["status"] = "error"
            report["error"] = str(ex)
            report["category"] = _classify_ai_status_error(str(ex))
    print(_api_status_text(report))
    output_json = getattr(args, "output_json", None)
    if output_json:
        Path(output_json).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return 0 if report["status"] == "ok" else 2


def cmd_switch_ai_profile(args: argparse.Namespace, project: TailorProject) -> int:
    settings = _load_appsettings(project)
    runtime = settings.get("AiRuntime", {}) if isinstance(settings.get("AiRuntime"), dict) else {}
    profiles = runtime.get("Profiles", {}) if isinstance(runtime.get("Profiles"), dict) else {}
    profile_name = str(args.profile).strip()
    if profile_name not in profiles:
        available = ", ".join(sorted(str(name) for name in profiles.keys()))
        raise RuntimeError(f"Unknown AI profile '{profile_name}'. Available profiles: {available}")

    local_settings: dict[str, Any] = {}
    if project.appsettings_local_path.exists():
        local_settings = json.loads(project.appsettings_local_path.read_text(encoding="utf-8"))
        backup_path = project.appsettings_local_path.with_name("appsettings.local.backup.json")
        backup_path.write_text(json.dumps(local_settings, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    local_settings.setdefault("AiRuntime", {})
    if not isinstance(local_settings["AiRuntime"], dict):
        local_settings["AiRuntime"] = {}
    local_settings["AiRuntime"]["ActiveProfile"] = profile_name
    local_settings.setdefault("OpenAi", {})
    if not isinstance(local_settings["OpenAi"], dict):
        local_settings["OpenAi"] = {}
    local_settings["OpenAi"]["DefaultChatModel"] = str(profiles[profile_name].get("model") or "gpt-4o-mini")
    _write_appsettings_local(project, local_settings)

    active_profile = _active_ai_profile(project)
    provider_result = _sync_ai_provider_from_profile(project, active_profile)
    print(f"Switched AI profile to: {active_profile['profile_name']}")
    print(f"Provider: {active_profile['provider_name']}")
    print(f"Model: {active_profile['model']}")
    print(f"Endpoint: {active_profile['endpoint']}")
    print(f"DB provider id: {provider_result['provider_id']}")
    return 0


def _message_tags_for_log_text(text: str) -> list[str]:
    tags: list[str] = []
    for tag, pattern in _DIAGNOSTIC_PATTERNS:
        if pattern.search(text):
            tags.append(tag)
    return tags


def _log_message_from_network_event(obj: dict[str, Any]) -> str:
    event = str(obj.get("event", "")).strip() or "event"
    method = str(obj.get("method", "")).strip()
    url = str(obj.get("url", "")).strip()
    status = str(obj.get("status", "")).strip()
    duration_ms = str(obj.get("duration_ms", "")).strip()
    body_excerpt = trace_excerpt(obj.get("body_raw", ""), 240) if obj.get("body_raw") else ""
    parts = [f"[network:{event}]"]
    if method:
        parts.append(method)
    if url:
        parts.append(url)
    if status:
        parts.append(f"status={status}")
    if duration_ms:
        parts.append(f"duration_ms={duration_ms}")
    text = " ".join(parts)
    if body_excerpt:
        text = f"{text} body={body_excerpt}"
    return text.strip()


def _aggregate_log_message(aggregate: dict[str, dict[str, Any]], *, message: str, source_path: Path,
                           observed_utc: str | None, kind: str) -> None:
    normalized = (message or "").strip()
    if not normalized:
        return
    item = aggregate.get(normalized)
    if item is None:
        item = {
            "message": normalized,
            "count": 0,
            "kind": kind,
            "tags": _message_tags_for_log_text(normalized),
            "first_seen_utc": observed_utc or "",
            "last_seen_utc": observed_utc or "",
            "source_files": [],
        }
        aggregate[normalized] = item
    item["count"] += 1
    if observed_utc and (not item["first_seen_utc"] or observed_utc < item["first_seen_utc"]):
        item["first_seen_utc"] = observed_utc
    if observed_utc and (not item["last_seen_utc"] or observed_utc > item["last_seen_utc"]):
        item["last_seen_utc"] = observed_utc
    rel = str(source_path)
    if rel not in item["source_files"] and len(item["source_files"]) < 5:
        item["source_files"].append(rel)


def _collect_recent_log_summary(project: "TailorProject", *, hours: int, max_files: int = 200) -> dict[str, Any]:
    since_utc = dt.datetime.now(tz=UTC) - dt.timedelta(hours=hours)
    files = [
        path
        for path in _iter_recent_diagnostic_files(project.root, since_utc, max_files=max_files * 3)
        if any(hint in path.name.lower() for hint in _CHECK_LOG_HINTS) and ".output" not in path.name.lower()
    ][:max_files]
    aggregate: dict[str, dict[str, Any]] = {}
    scanned_files: list[str] = []
    total_events = 0
    for path in files:
        scanned_files.append(str(path.relative_to(project.root)) if path.is_relative_to(project.root) else str(path))
        if path.name.lower().endswith(".ndjson"):
            for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    _aggregate_log_message(
                        aggregate,
                        message=f"[raw-ndjson] {line[:1200]}",
                        source_path=path,
                        observed_utc="",
                        kind="raw-line",
                    )
                    total_events += 1
                    continue
                logged_utc = str(obj.get("logged_utc", "") or "")
                if logged_utc:
                    try:
                        observed = dt.datetime.fromisoformat(logged_utc.replace("Z", "+00:00"))
                    except ValueError:
                        observed = None
                    if observed and observed.tzinfo is None:
                        observed = observed.replace(tzinfo=UTC)
                    if observed and observed.astimezone(UTC) < since_utc:
                        continue
                message = _log_message_from_network_event(obj)
                _aggregate_log_message(
                    aggregate,
                    message=message,
                    source_path=path,
                    observed_utc=logged_utc,
                    kind="network",
                )
                total_events += 1
            continue
        modified = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        if modified < since_utc:
            continue
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            _aggregate_log_message(
                aggregate,
                message=line[:1200],
                source_path=path,
                observed_utc=modified.isoformat(),
                kind="line",
            )
            total_events += 1
    messages = sorted(
        aggregate.values(),
        key=lambda item: (-int(item["count"]), item["last_seen_utc"], item["message"]),
    )
    tag_totals: dict[str, int] = {}
    for item in messages:
        for tag in item.get("tags", []):
            tag_totals[tag] = tag_totals.get(tag, 0) + int(item["count"])
    return {
        "generated_utc": now_iso(),
        "hours": hours,
        "source_file_count": len(scanned_files),
        "total_log_events": total_events,
        "unique_log_messages": len(messages),
        "source_files": scanned_files,
        "tag_totals": tag_totals,
        "messages": messages,
    }


def _check_logs_fallback_html(report: dict[str, Any]) -> str:
    table_id = "check-logs-table"
    rows = []
    for item in report.get("messages", [])[:100]:
        rows.append(
            "<tr>"
            f"<td data-sort-value=\"{int(item['count'])}\">{int(item['count'])}</td>"
            f"<td>{_html_cell(', '.join(item.get('tags', [])) or 'info')}</td>"
            f"<td>{_html_cell(item.get('kind', ''))}</td>"
            f"<td data-sort-value=\"{_html_cell(item.get('last_seen_utc', ''))}\">{_html_cell(item.get('last_seen_utc', ''))}</td>"
            f"<td>{_html_cell(item.get('message', ''))}</td>"
            "</tr>"
        )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Check Logs Report</title>
  <style>
    body {{ font-family: Georgia, serif; margin: 0; padding: 24px; background: #f6f1e8; color: #1f2933; }}
    .hero {{ background: #fffaf1; border: 1px solid #d7c8b5; border-radius: 16px; padding: 18px 20px; }}
    .stats {{ display: flex; gap: 16px; flex-wrap: wrap; margin-top: 14px; }}
    .stat {{ background: #f0e3d3; border-radius: 12px; padding: 12px 14px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 20px; background: #fffaf1; }}
    th, td {{ border: 1px solid #d7c8b5; padding: 10px; text-align: left; vertical-align: top; }}
    th {{ background: #f0e3d3; }}
    {_html_table_interaction_assets()}
  </style>
</head>
<body>
  <div class="hero">
    <h1>Check Logs Report</h1>
    <p>AI analysis was unavailable, so this fallback report shows the deduplicated log summary collected in the last {report['hours']} hour(s).</p>
    <div class="stats">
      <div class="stat">Files scanned: {report['source_file_count']}</div>
      <div class="stat">Total events: {report['total_log_events']}</div>
      <div class="stat">Unique messages: {report['unique_log_messages']}</div>
    </div>
  </div>
  {_html_table_controls(table_id, "Search repeated log messages...")}
  <table id="{table_id}" data-enhanced-table="true">
    <thead>
      <tr><th>Count</th><th>Tags</th><th>Kind</th><th>Last Seen UTC</th><th>Message</th></tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
  {_html_table_interaction_script()}
</body>
</html>
"""


def _ask_ai_for_check_logs(args: argparse.Namespace, project: "TailorProject", report: dict[str, Any]) -> str | None:
    api_key = getattr(args, "api_key", None) or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    ai = AiClient(
        model=getattr(args, "model", "gpt-4o-mini"),
        api_key=api_key,
        endpoint=getattr(args, "ai_endpoint", "https://api.openai.com/v1/chat/completions"),
    )
    system = _load_prompt_file(
        project,
        "analyze_logs_prompt.txt",
        default_text="Return a complete standalone HTML log analysis report.",
    )
    top_messages = report.get("messages", [])[:200]
    evidence = {
        "hours": report["hours"],
        "source_file_count": report["source_file_count"],
        "total_log_events": report["total_log_events"],
        "unique_log_messages": report["unique_log_messages"],
        "tag_totals": report.get("tag_totals", {}),
        "top_messages": top_messages,
        "source_files": report.get("source_files", [])[:50],
    }
    user = (
        "Analyze this deduplicated TaiLOR log digest and generate the HTML report.\n\n"
        + json.dumps(evidence, indent=2, ensure_ascii=False)
    )
    return ai.chat(
        system=system,
        user=user,
        temperature=0.1,
        max_tokens=2200,
    ).strip()


def cmd_check_logs(args: argparse.Namespace, project: TailorProject) -> int:
    hours = max(1, int(getattr(args, "hours", 24) or 24))
    report = _collect_recent_log_summary(project, hours=hours)
    generated = dt.datetime.now().astimezone()
    html_text: str
    try:
        html_text = _ask_ai_for_check_logs(args, project, report) or _check_logs_fallback_html(report)
    except Exception as ex:
        report["ai_error"] = str(ex)
        html_text = _check_logs_fallback_html(report)
    reports_root = project.root / "agents" / "general" / "reports"
    reports_root.mkdir(parents=True, exist_ok=True)
    output_html = reports_root / f"check_logs_report_{generated.strftime('%Y%m%d_%H%M%S')}.html"
    latest_html = reports_root / "latest_check_logs_report.html"
    output_json = reports_root / f"check_logs_report_{generated.strftime('%Y%m%d_%H%M%S')}.json"
    output_html.write_text(html_text, encoding="utf-8")
    latest_html.write_text(html_text, encoding="utf-8")
    output_json.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Check logs report written to: {output_html}")
    print(f"Latest check logs report: {latest_html}")
    print(f"Check logs summary JSON: {output_json}")
    return 0


def _extract_history_message_fields(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    lines = text.splitlines()
    body_start = None
    for index, raw_line in enumerate(lines):
        line = raw_line.rstrip()
        if not line.strip():
            body_start = index + 1
            break
        if ":" in line:
            key, value = line.split(":", 1)
            fields[key.strip().lower()] = value.strip()
    if body_start is None:
        body_start = len(lines)
    if "body" in fields:
        fields["body_text"] = "\n".join(lines[body_start:]).strip()
    else:
        body_lines = lines[body_start:]
        fields["body_text"] = "\n".join(body_lines).strip()
    return fields


def _history_message_direction(path: Path) -> str:
    lowered = path.name.lower()
    if lowered.startswith("sent_"):
        return "sent"
    if lowered.startswith("received_"):
        return "received"
    if lowered.startswith("draft_"):
        return "draft"
    if lowered.startswith("replied_"):
        return "reply-marker"
    return "other"


def _preview_text(text: str, limit: int = 220) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."


def _parse_history_message(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8", errors="replace")
    fields = _extract_history_message_fields(text)
    modified = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    direction = _history_message_direction(path)
    return {
        "path": path,
        "direction": direction,
        "modified_dt": modified,
        "modified_utc": modified.isoformat(),
        "subject": fields.get("subject", ""),
        "from": fields.get("from", ""),
        "sender_email": fields.get("sender email", ""),
        "message_id": fields.get("message id", ""),
        "body": fields.get("body_text", ""),
        "body_preview": _preview_text(fields.get("body_text", "")),
    }


def _format_report_dt(value: dt.datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _html_cell(text: str) -> str:
    return html.escape(text or "")


def _html_message_block(item: dict[str, Any] | None) -> str:
    if not item:
        return '<div class="empty">None</div>'
    subject = _html_cell(item.get("subject") or "(no subject)")
    when = _html_cell(_format_report_dt(item.get("modified_dt")))
    preview = _html_cell(item.get("body_preview") or "")
    path_text = _html_cell(str(item.get("path", "")))
    return (
        f'<div class="msg"><div class="subject">{subject}</div>'
        f'<div class="meta">{when}</div>'
        f'<div class="preview">{preview}</div>'
        f'<div class="path">{path_text}</div></div>'
    )


def _html_table_interaction_assets() -> str:
    return """
    .table-tools {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }
    .table-search {
      width: min(380px, 100%);
      padding: 10px 12px;
      border: 1px solid var(--line, #d7c8b5);
      border-radius: 10px;
      background: #fffdf8;
      color: inherit;
      font: inherit;
    }
    .table-meta {
      color: var(--muted, #586574);
      font-size: 12px;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }
    th.sortable {
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
      position: sticky;
    }
    th.sortable::after {
      content: " ↕";
      color: var(--muted, #586574);
      opacity: 0.75;
      font-size: 11px;
    }
    th.sortable[data-sort-dir="asc"]::after {
      content: " ↑";
      opacity: 1;
    }
    th.sortable[data-sort-dir="desc"]::after {
      content: " ↓";
      opacity: 1;
    }
    th.resizable {
      overflow: visible;
    }
    .col-resize-handle {
      position: absolute;
      top: 0;
      right: -3px;
      width: 8px;
      height: 100%;
      cursor: col-resize;
      z-index: 4;
      touch-action: none;
    }
    .col-resize-handle::before {
      content: "";
      position: absolute;
      top: 20%;
      bottom: 20%;
      left: 50%;
      width: 1px;
      transform: translateX(-50%);
      background: rgba(90, 103, 118, 0.28);
    }
    th.resizing,
    body.col-resizing {
      cursor: col-resize !important;
      user-select: none !important;
    }
    .dashboard-controls {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px;
      margin: 0 0 14px;
      align-items: end;
    }
    .control-group {
      display: grid;
      gap: 4px;
      min-width: 0;
    }
    .control-group label {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted, #586574);
    }
    .control-group select,
    .control-group button {
      width: 100%;
      min-width: 0;
      padding: 8px 10px;
      border: 1px solid var(--line, #d7c8b5);
      border-radius: 9px;
      background: #fffdf8;
      color: inherit;
      font: inherit;
    }
    .columns-panel {
      margin: 0 0 14px;
      padding: 10px;
      border: 1px solid var(--line, #d7c8b5);
      border-radius: 10px;
      background: #fffdf9;
    }
    .column-list {
      display: grid;
      gap: 8px;
    }
    .column-item {
      display: grid;
      grid-template-columns: minmax(140px, 1.2fr) auto auto minmax(120px, 1fr);
      gap: 8px;
      align-items: center;
    }
    .column-label {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
      font-size: 12px;
    }
    .column-actions {
      display: inline-flex;
      gap: 6px;
      align-items: center;
    }
    .mini-btn {
      border: 1px solid var(--line, #d7c8b5);
      background: #fffdf8;
      color: inherit;
      border-radius: 8px;
      padding: 5px 8px;
      font-size: 11px;
      line-height: 1;
      cursor: pointer;
    }
    .mini-btn:hover {
      background: #f3eadc;
    }
    .format-select {
      width: 100%;
      min-width: 0;
      padding: 6px 8px;
      border: 1px solid var(--line, #d7c8b5);
      border-radius: 8px;
      background: #fffdf8;
      color: inherit;
      font: inherit;
      font-size: 12px;
    }
    .group-row td {
      background: rgba(29, 39, 50, 0.08);
      color: var(--ink);
      font-weight: 600;
      letter-spacing: 0.02em;
    }
    .group-count {
      color: var(--muted, #586574);
      font-weight: 500;
      margin-left: 8px;
    }
    @media (max-width: 900px) {
      .column-item {
        grid-template-columns: 1fr;
      }
    }
    """.strip()


def _html_table_interaction_script() -> str:
    return """
<script>
(() => {
  function storageKey(tableId, suffix) {
    return `tailor-table:${window.location.pathname}:${tableId}:${suffix}`;
  }

  function saveJson(key, value) {
    try {
      window.localStorage.setItem(key, JSON.stringify(value));
    } catch (_error) {
      // ignore storage failures
    }
  }

  function loadJson(key, fallback) {
    try {
      const raw = window.localStorage.getItem(key);
      return raw ? JSON.parse(raw) : fallback;
    } catch (_error) {
      return fallback;
    }
  }

  function parseSortValue(value) {
    const text = String(value ?? "").trim();
    const compact = text.replace(/,/g, "");
    if (/^-?\\d+(?:\\.\\d+)?$/.test(compact)) {
      return { type: "number", value: Number(compact) };
    }
    const date = Date.parse(text);
    if (!Number.isNaN(date) && /[-/:]|\\b(?:UTC|EST|EDT|PST|CST|GMT)\\b/i.test(text)) {
      return { type: "date", value: date };
    }
    return { type: "text", value: text.toLocaleLowerCase() };
  }

  function compareValues(a, b) {
    const av = parseSortValue(a);
    const bv = parseSortValue(b);
    if (av.type === bv.type) {
      if (av.value < bv.value) return -1;
      if (av.value > bv.value) return 1;
      return 0;
    }
    const as = String(a ?? "").toLocaleLowerCase();
    const bs = String(b ?? "").toLocaleLowerCase();
    return as.localeCompare(bs);
  }

  function getColumnMaxWidthPx() {
    return Math.max(88, Math.floor(window.innerWidth * 0.15));
  }

  function getColumnMinWidthPx(headerText) {
    const compact = String(headerText ?? "").trim();
    if (!compact) return 56;
    return Math.max(56, Math.min(120, compact.length * 9 + 28));
  }

  function ensureColGroup(table, columnCount) {
    let colgroup = table.querySelector(":scope > colgroup");
    if (!colgroup) {
      colgroup = document.createElement("colgroup");
      table.insertBefore(colgroup, table.firstChild);
    }
    while (colgroup.children.length < columnCount) {
      colgroup.appendChild(document.createElement("col"));
    }
    while (colgroup.children.length > columnCount) {
      colgroup.removeChild(colgroup.lastElementChild);
    }
    return Array.from(colgroup.children);
  }

  function measureColumnWidths(table, headers) {
    const canvas = document.createElement("canvas");
    const context = canvas.getContext("2d");
    if (!context) {
      return headers.map(() => getColumnMaxWidthPx());
    }
    const sampleHeader = headers[0] || table;
    context.font = window.getComputedStyle(sampleHeader).font || window.getComputedStyle(table).font || "11px Segoe UI";
    const maxWidth = getColumnMaxWidthPx();
    return headers.map((header, index) => {
      let widest = context.measureText((header.textContent || "").trim()).width + 28;
      table.querySelectorAll("tbody tr").forEach(row => {
        const cell = row.children[index];
        if (!cell) return;
        const cellText = (cell.getAttribute("data-sort-value") || cell.textContent || "").trim();
        widest = Math.max(widest, context.measureText(cellText).width + 24);
      });
      return Math.max(getColumnMinWidthPx(header.textContent || ""), Math.min(maxWidth, Math.ceil(widest)));
    });
  }

  function applyColumnWidths(table, headers, widths) {
    const cols = ensureColGroup(table, headers.length);
    headers.forEach((header, index) => {
      const requested = Number(widths[index] || 0);
      const maxWidth = getColumnMaxWidthPx();
      const minWidth = getColumnMinWidthPx(header.textContent || "");
      const finalWidth = Math.max(minWidth, Math.min(maxWidth, Number.isFinite(requested) ? requested : minWidth));
      cols[index].style.width = `${finalWidth}px`;
      cols[index].style.minWidth = `${finalWidth}px`;
      cols[index].style.maxWidth = `${finalWidth}px`;
    });
  }

  function attachColumnResizers(table, headers, persistWidths) {
    let active = null;
    headers.forEach((header, index) => {
      header.classList.add("resizable");
      let handle = header.querySelector(".col-resize-handle");
      if (!handle) {
        handle = document.createElement("span");
        handle.className = "col-resize-handle";
        handle.setAttribute("aria-hidden", "true");
        header.appendChild(handle);
      }
      handle.addEventListener("pointerdown", event => {
        event.preventDefault();
        const liveHeaders = Array.from(table.querySelectorAll("thead th"));
        const liveIndex = liveHeaders.indexOf(header);
        const cols = ensureColGroup(table, liveHeaders.length);
        const currentWidth = cols[liveIndex >= 0 ? liveIndex : index].getBoundingClientRect().width || header.getBoundingClientRect().width;
        active = {
          header,
          startX: event.clientX,
          startWidth: currentWidth,
        };
        header.classList.add("resizing");
        document.body.classList.add("col-resizing");
        if (handle.setPointerCapture) {
          handle.setPointerCapture(event.pointerId);
        }
      });
    });

    function finishResize() {
      if (!active) return;
      active.header?.classList.remove("resizing");
      document.body.classList.remove("col-resizing");
      persistWidths();
      active = null;
    }

    window.addEventListener("pointermove", event => {
      if (!active) return;
      const liveHeaders = Array.from(table.querySelectorAll("thead th"));
      const liveIndex = liveHeaders.indexOf(active.header);
      const cols = ensureColGroup(table, liveHeaders.length);
      const header = liveIndex >= 0 ? liveHeaders[liveIndex] : active.header;
      if (!header || liveIndex < 0 || !cols[liveIndex]) return;
      const maxWidth = getColumnMaxWidthPx();
      const minWidth = getColumnMinWidthPx(header.textContent || "");
      const next = Math.max(minWidth, Math.min(maxWidth, active.startWidth + (event.clientX - active.startX)));
      const rounded = Math.round(next);
      cols[liveIndex].style.width = `${rounded}px`;
      cols[liveIndex].style.minWidth = `${rounded}px`;
      cols[liveIndex].style.maxWidth = `${rounded}px`;
    });

    window.addEventListener("pointerup", finishResize);
    window.addEventListener("pointercancel", finishResize);
  }

  function formatDurationShort(seconds) {
    if (!Number.isFinite(seconds) || seconds <= 0) return "0.0s";
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    if (seconds < 3600) return `${(seconds / 60).toFixed(1)}m`;
    if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
    return `${(seconds / 86400).toFixed(1)}d`;
  }

  function formatDurationHms(seconds) {
    if (!Number.isFinite(seconds) || seconds < 0) return "";
    const total = Math.round(seconds);
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const secs = total % 60;
    return [hours, minutes, secs].map((value, index) => index === 0 ? String(value) : String(value).padStart(2, "0")).join(":");
  }

  function formatUtcText(isoValue) {
    const date = new Date(isoValue);
    if (Number.isNaN(date.getTime())) return isoValue || "";
    return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-${String(date.getUTCDate()).padStart(2, "0")} ${String(date.getUTCHours()).padStart(2, "0")}:${String(date.getUTCMinutes()).padStart(2, "0")}:${String(date.getUTCSeconds()).padStart(2, "0")} UTC`;
  }

  function formatRelativeTime(isoValue) {
    const date = new Date(isoValue);
    if (Number.isNaN(date.getTime())) return isoValue || "";
    const diffSeconds = Math.round((date.getTime() - Date.now()) / 1000);
    const absSeconds = Math.abs(diffSeconds);
    const units = [
      ["day", 86400],
      ["hour", 3600],
      ["minute", 60],
      ["second", 1],
    ];
    const unit = units.find(([, size]) => absSeconds >= size) || units[units.length - 1];
    const value = Math.round(diffSeconds / unit[1]);
    return new Intl.RelativeTimeFormat(undefined, { numeric: "auto" }).format(value, unit[0]);
  }

  function setupTable(table) {
    const tableId = table.getAttribute("id");
    if (!tableId) return;
    const searchStateKey = storageKey(tableId, "search");
    const sortStateKey = storageKey(tableId, "sort");
    const widthStateKey = storageKey(tableId, "widths");
    const orderStateKey = storageKey(tableId, "column-order");
    const hiddenStateKey = storageKey(tableId, "hidden-columns");
    const formatStateKey = storageKey(tableId, "column-formats");
    const filterStateKey = storageKey(tableId, "filters");
    const groupStateKey = storageKey(tableId, "group-by");
    const search = document.querySelector(`[data-table-search="${tableId}"]`);
    const meta = document.querySelector(`[data-table-meta="${tableId}"]`);
    const filterControls = Array.from(document.querySelectorAll(`[data-table-filter="${tableId}"]`));
    const groupControl = document.querySelector(`[data-table-group="${tableId}"]`);
    const columnsToggle = document.querySelector(`[data-columns-toggle="${tableId}"]`);
    const columnsPanel = document.querySelector(`[data-columns-panel="${tableId}"]`);
    const columnsList = document.querySelector(`[data-columns-list="${tableId}"]`);
    const columnsReset = document.querySelector(`[data-columns-reset="${tableId}"]`);
    const tbody = table.tBodies[0];
    if (!tbody) return;
    const headerRow = table.tHead?.rows?.[0];
    if (!headerRow) return;
    const headers = Array.from(headerRow.children);
    const savedSearch = loadJson(searchStateKey, "");
    const savedWidths = loadJson(widthStateKey, {});
    const savedOrder = loadJson(orderStateKey, []);
    const savedHidden = loadJson(hiddenStateKey, []);
    const savedFormats = loadJson(formatStateKey, {});
    const savedFilters = loadJson(filterStateKey, {});
    const savedGroup = loadJson(groupStateKey, "");
    const baseRows = Array.from(tbody.querySelectorAll("tr")).filter(row => !row.classList.contains("group-row"));

    function getHeaders() {
      return Array.from(headerRow.children);
    }

    function getColumnKey(header, index) {
      return header?.getAttribute("data-col-key") || `col_${index}`;
    }

    function getCurrentColumnKeys() {
      return getHeaders().map((header, index) => getColumnKey(header, index));
    }

    function getHeaderByKey(key) {
      return getHeaders().find((header, index) => getColumnKey(header, index) === key) || null;
    }

    function getColumnIndexByKey(key) {
      return getCurrentColumnKeys().indexOf(key);
    }

    function getCellByKey(row, key) {
      const index = getColumnIndexByKey(key);
      return index >= 0 ? row.children[index] || null : null;
    }

    function getCellText(row, key) {
      const cell = getCellByKey(row, key);
      if (!cell) return "";
      return (cell.getAttribute("data-filter-value") || cell.textContent || "").trim();
    }

    function normalizeWidthState(value) {
      if (value && typeof value === "object" && !Array.isArray(value)) {
        return value;
      }
      if (Array.isArray(value) && value.length === headers.length) {
        const mapped = {};
        headers.forEach((header, index) => {
          mapped[getColumnKey(header, index)] = Number(value[index] || 0);
        });
        return mapped;
      }
      return {};
    }

    const state = {
      search: typeof savedSearch === "string" ? savedSearch : "",
      sort: loadJson(sortStateKey, null),
      widths: normalizeWidthState(savedWidths),
      hidden: new Set(Array.isArray(savedHidden) ? savedHidden.map(String) : []),
      formats: savedFormats && typeof savedFormats === "object" ? savedFormats : {},
      filters: savedFilters && typeof savedFilters === "object" ? savedFilters : {},
      groupBy: typeof savedGroup === "string" ? savedGroup : "",
    };

    function persistWidths() {
      const cols = ensureColGroup(table, getHeaders().length);
      const next = {};
      getHeaders().forEach((header, index) => {
        next[getColumnKey(header, index)] = Math.round(cols[index].getBoundingClientRect().width || parseFloat(cols[index].style.width || "0") || 0);
      });
      state.widths = next;
      saveJson(widthStateKey, next);
    }

    function applyCurrentWidths() {
      const liveHeaders = getHeaders();
      const requestedWidths = liveHeaders.map((header, index) => Number(state.widths[getColumnKey(header, index)] || 0));
      const hasSaved = requestedWidths.length === liveHeaders.length && requestedWidths.some(value => value > 0);
      applyColumnWidths(table, liveHeaders, hasSaved ? requestedWidths : measureColumnWidths(table, liveHeaders));
    }

    function reorderColumns(targetOrder, persist = true) {
      const currentHeaders = getHeaders();
      const currentKeys = currentHeaders.map((header, index) => getColumnKey(header, index));
      const finalKeys = [
        ...targetOrder.filter(key => currentKeys.includes(key)),
        ...currentKeys.filter(key => !targetOrder.includes(key)),
      ];
      const cols = ensureColGroup(table, currentHeaders.length);
      const headerMap = new Map(currentHeaders.map((header, index) => [currentKeys[index], header]));
      const colMap = new Map(cols.map((col, index) => [currentKeys[index], col]));
      const rowMaps = baseRows.map(row => {
        const cells = Array.from(row.children);
        return { row, cellMap: new Map(currentKeys.map((key, index) => [key, cells[index]])) };
      });
      finalKeys.forEach(key => {
        const header = headerMap.get(key);
        const col = colMap.get(key);
        if (header) headerRow.appendChild(header);
        if (col) table.querySelector(":scope > colgroup")?.appendChild(col);
      });
      rowMaps.forEach(({ row, cellMap }) => {
        finalKeys.forEach(key => {
          const cell = cellMap.get(key);
          if (cell) row.appendChild(cell);
        });
      });
      if (persist) {
        saveJson(orderStateKey, finalKeys);
      }
      state.sort = state.sort && finalKeys.includes(String(state.sort.key || "")) ? state.sort : state.sort;
      applyCurrentWidths();
    }

    function applyHiddenColumns() {
      const liveHeaders = getHeaders();
      const cols = ensureColGroup(table, liveHeaders.length);
      liveHeaders.forEach((header, index) => {
        const key = getColumnKey(header, index);
        const hidden = state.hidden.has(key);
        header.style.display = hidden ? "none" : "";
        if (cols[index]) cols[index].style.display = hidden ? "none" : "";
        baseRows.forEach(row => {
          const cell = row.children[index];
          if (cell) {
            cell.style.display = hidden ? "none" : "";
          }
        });
      });
      Array.from(tbody.querySelectorAll("tr.group-row td")).forEach(cell => {
        cell.colSpan = Math.max(1, liveHeaders.filter((header, index) => !state.hidden.has(getColumnKey(header, index))).length);
      });
    }

    function applyColumnFormats() {
      getHeaders().forEach((header, index) => {
        const key = getColumnKey(header, index);
        const format = String(state.formats[key] || "default");
        baseRows.forEach(row => {
          const cell = row.children[index];
          if (!cell) return;
          if (key === "started" || key === "ended") {
            const isoValue = cell.getAttribute("data-sort-value") || "";
            const localValue = cell.getAttribute("data-format-local") || "";
            let nextText = localValue;
            if (format === "iso") nextText = isoValue;
            else if (format === "utc") nextText = formatUtcText(isoValue);
            else if (format === "relative") nextText = formatRelativeTime(isoValue);
            cell.textContent = nextText;
            cell.title = nextText;
          } else if (key === "duration") {
            const seconds = Number(cell.getAttribute("data-sort-value") || 0);
            let nextText = cell.getAttribute("data-format-short") || formatDurationShort(seconds);
            if (format === "seconds") nextText = `${seconds.toFixed(1)}s`;
            else if (format === "hms") nextText = formatDurationHms(seconds);
            cell.textContent = nextText;
            cell.title = nextText;
          } else if (["workflow_file", "run_log", "context", "inbound", "outbound"].includes(key)) {
            const anchor = cell.querySelector("a");
            if (!anchor) return;
            const labelValue = cell.getAttribute("data-label-value") || anchor.textContent || "";
            const pathValue = cell.getAttribute("title") || labelValue;
            anchor.textContent = format === "path" ? pathValue : labelValue;
          }
        });
      });
    }

    function getSortValue(row, key) {
      const cell = getCellByKey(row, key);
      return cell?.getAttribute("data-sort-value") ?? cell?.textContent ?? "";
    }

    function getGroupValue(row, key) {
      const cell = getCellByKey(row, key);
      return (cell?.getAttribute("data-filter-value") || cell?.textContent || "").trim() || "(empty)";
    }

    function rowMatches(row) {
      const query = String(state.search || "").toLocaleLowerCase().trim();
      if (query && !row.textContent.toLocaleLowerCase().includes(query)) {
        return false;
      }
      return filterControls.every(control => {
        const key = String(control.getAttribute("data-filter-key") || "").trim();
        const wanted = String(state.filters[key] || "").trim();
        if (!key || !wanted) return true;
        return getCellText(row, key) === wanted;
      });
    }

    function updateMeta(visibleCount, groupCount) {
      if (!meta) return;
      const parts = [`${visibleCount} row(s)`];
      if (String(state.search || "").trim()) {
        parts.push("filtered");
      }
      if (groupCount > 0) {
        parts.push(`${groupCount} group(s)`);
      }
      meta.textContent = parts.join(" | ");
    }

    function renderRows() {
      const filtered = baseRows.filter(rowMatches);
      if (state.sort && state.sort.key && (state.sort.direction === "asc" || state.sort.direction === "desc")) {
        filtered.sort((rowA, rowB) => {
          const result = compareValues(getSortValue(rowA, state.sort.key), getSortValue(rowB, state.sort.key));
          return state.sort.direction === "asc" ? result : -result;
        });
      }
      tbody.innerHTML = "";
      let groupCount = 0;
      const visibleColumnCount = Math.max(1, getHeaders().filter((header, index) => !state.hidden.has(getColumnKey(header, index))).length);
      if (state.groupBy) {
        let currentGroup = null;
        let currentRows = [];
        const flush = () => {
          if (currentGroup === null) return;
          const groupRow = document.createElement("tr");
          groupRow.className = "group-row";
          const cell = document.createElement("td");
          cell.colSpan = visibleColumnCount;
          cell.appendChild(document.createTextNode(currentGroup));
          const count = document.createElement("span");
          count.className = "group-count";
          count.textContent = `${currentRows.length} row(s)`;
          cell.appendChild(count);
          groupRow.appendChild(cell);
          tbody.appendChild(groupRow);
          currentRows.forEach(row => tbody.appendChild(row));
          groupCount += 1;
        };
        filtered.forEach(row => {
          const groupValue = getGroupValue(row, state.groupBy);
          if (currentGroup === null) {
            currentGroup = groupValue;
            currentRows = [row];
          } else if (groupValue === currentGroup) {
            currentRows.push(row);
          } else {
            flush();
            currentGroup = groupValue;
            currentRows = [row];
          }
        });
        flush();
      } else {
        filtered.forEach(row => tbody.appendChild(row));
      }
      applyColumnFormats();
      applyHiddenColumns();
      updateMeta(filtered.length, groupCount);
    }

    function applySort(key, direction, persist = true) {
      const header = getHeaderByKey(key);
      if (!header) return;
      getHeaders().forEach(other => other.removeAttribute("data-sort-dir"));
      header.setAttribute("data-sort-dir", direction);
      state.sort = { key, direction };
      if (persist) {
        saveJson(sortStateKey, state.sort);
      }
      renderRows();
    }

    function rebuildFilterOptions() {
      filterControls.forEach(control => {
        const key = String(control.getAttribute("data-filter-key") || "").trim();
        if (!key) return;
        const existing = String(state.filters[key] || "");
        const values = Array.from(new Set(baseRows.map(row => getCellText(row, key)).filter(Boolean))).sort((a, b) => a.localeCompare(b));
        control.innerHTML = "";
        const blankOption = document.createElement("option");
        blankOption.value = "";
        blankOption.textContent = "All";
        control.appendChild(blankOption);
        values.forEach(value => {
          const option = document.createElement("option");
          option.value = value;
          option.textContent = value;
          control.appendChild(option);
        });
        if (values.includes(existing)) {
          control.value = existing;
        } else {
          control.value = "";
          delete state.filters[key];
        }
      });
      saveJson(filterStateKey, state.filters);
    }

    function rebuildGroupOptions() {
      if (!groupControl) return;
      const existing = String(state.groupBy || "");
      const options = getHeaders()
        .map((header, index) => ({ key: getColumnKey(header, index), label: (header.textContent || "").trim(), groupable: header.getAttribute("data-groupable") !== "false" }))
        .filter(item => item.groupable && item.key !== "kill");
      groupControl.innerHTML = `<option value="">None</option>${options.map(item => `<option value="${item.key}">${item.label}</option>`).join("")}`;
      if (options.some(item => item.key === existing)) {
        groupControl.value = existing;
      } else {
        groupControl.value = "";
        state.groupBy = "";
        saveJson(groupStateKey, state.groupBy);
      }
    }

    function moveColumn(key, offset) {
      const currentKeys = getCurrentColumnKeys();
      const index = currentKeys.indexOf(key);
      if (index < 0) return;
      const targetIndex = index + offset;
      if (targetIndex < 0 || targetIndex >= currentKeys.length) return;
      const next = currentKeys.slice();
      const [item] = next.splice(index, 1);
      next.splice(targetIndex, 0, item);
      reorderColumns(next, true);
      rebuildGroupOptions();
      rebuildColumnManager();
      renderRows();
    }

    function setColumnHidden(key, hidden) {
      if (hidden && getCurrentColumnKeys().filter(currentKey => !state.hidden.has(currentKey) || currentKey === key).length <= 1) {
        return;
      }
      if (hidden) state.hidden.add(key);
      else state.hidden.delete(key);
      saveJson(hiddenStateKey, Array.from(state.hidden));
      rebuildColumnManager();
      renderRows();
    }

    function setColumnFormat(key, format) {
      if (!format || format === "default") {
        delete state.formats[key];
      } else {
        state.formats[key] = format;
      }
      saveJson(formatStateKey, state.formats);
      renderRows();
      rebuildColumnManager();
    }

    function rebuildColumnManager() {
      if (!columnsList) return;
      const headersNow = getHeaders();
      const currentKeys = getCurrentColumnKeys();
      const hiddenKeys = state.hidden;
      columnsList.innerHTML = "";
      headersNow.forEach((header, index) => {
        const key = currentKeys[index];
        const label = (header.textContent || "").trim();
        const formatOptions = String(header.getAttribute("data-format-options") || "").split(",").map(value => value.trim()).filter(Boolean);
        const item = document.createElement("div");
        item.className = "column-item";
        const formatSelect = formatOptions.length
          ? `<select class="format-select" data-format-key="${key}"><option value="default">Default</option>${formatOptions.map(option => `<option value="${option}">${option}</option>`).join("")}</select>`
          : `<select class="format-select" disabled><option>Fixed</option></select>`;
        item.innerHTML = `
          <label class="column-label">
            <input type="checkbox" data-visibility-key="${key}" ${hiddenKeys.has(key) ? "" : "checked"}>
            <span>${label}</span>
          </label>
          <div class="column-actions">
            <button type="button" class="mini-btn" data-move-key="${key}" data-move-offset="-1">Up</button>
            <button type="button" class="mini-btn" data-move-key="${key}" data-move-offset="1">Down</button>
          </div>
          <div class="column-actions"><span class="table-meta">${index + 1}</span></div>
          ${formatSelect}
        `;
        columnsList.appendChild(item);
      });
      columnsList.querySelectorAll("[data-visibility-key]").forEach(input => {
        input.addEventListener("change", event => {
          const key = event.currentTarget.getAttribute("data-visibility-key") || "";
          setColumnHidden(key, !event.currentTarget.checked);
        });
      });
      columnsList.querySelectorAll("[data-move-key]").forEach(button => {
        button.addEventListener("click", event => {
          const key = event.currentTarget.getAttribute("data-move-key") || "";
          const offset = Number(event.currentTarget.getAttribute("data-move-offset") || 0);
          moveColumn(key, offset);
        });
      });
      columnsList.querySelectorAll("[data-format-key]").forEach(select => {
        const key = select.getAttribute("data-format-key") || "";
        select.value = String(state.formats[key] || "default");
        select.addEventListener("change", event => {
          setColumnFormat(key, event.currentTarget.value || "default");
        });
      });
    }

    attachColumnResizers(table, headers, persistWidths);
    reorderColumns(Array.isArray(savedOrder) ? savedOrder.map(String) : [], false);
    applyCurrentWidths();
    rebuildFilterOptions();
    rebuildGroupOptions();
    rebuildColumnManager();

    function applySearch() {
      state.search = search?.value ?? "";
      saveJson(searchStateKey, state.search);
      renderRows();
    }

    getHeaders().forEach((th, index) => {
      th.classList.add("sortable");
      th.tabIndex = 0;
      const sort = () => {
        const key = getColumnKey(th, index);
        const current = th.getAttribute("data-sort-dir") === "asc" ? "asc" : (th.getAttribute("data-sort-dir") === "desc" ? "desc" : "");
        applySort(key, current === "asc" ? "desc" : "asc", true);
      };
      th.addEventListener("click", event => {
        if (event.target.closest(".col-resize-handle")) return;
        sort();
      });
      th.addEventListener("keydown", event => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          sort();
        }
      });
    });

    if (search) {
      search.value = state.search;
      search.addEventListener("input", applySearch);
    }
    filterControls.forEach(control => {
      const key = String(control.getAttribute("data-filter-key") || "").trim();
      if (key) {
        control.value = String(state.filters[key] || "");
      }
      control.addEventListener("change", event => {
        const filterKey = String(event.currentTarget.getAttribute("data-filter-key") || "").trim();
        if (!filterKey) return;
        const value = String(event.currentTarget.value || "");
        if (value) state.filters[filterKey] = value;
        else delete state.filters[filterKey];
        saveJson(filterStateKey, state.filters);
        renderRows();
      });
    });
    if (groupControl) {
      groupControl.value = state.groupBy;
      groupControl.addEventListener("change", event => {
        state.groupBy = String(event.currentTarget.value || "");
        saveJson(groupStateKey, state.groupBy);
        renderRows();
      });
    }
    if (columnsToggle && columnsPanel) {
      columnsToggle.addEventListener("click", () => {
        columnsPanel.hidden = !columnsPanel.hidden;
      });
    }
    if (columnsReset) {
      columnsReset.addEventListener("click", () => {
        state.hidden = new Set();
        state.formats = {};
        state.filters = {};
        state.groupBy = "";
        state.widths = {};
        saveJson(hiddenStateKey, []);
        saveJson(formatStateKey, {});
        saveJson(filterStateKey, {});
        saveJson(groupStateKey, "");
        saveJson(widthStateKey, {});
        reorderColumns(headers.map((header, index) => getColumnKey(header, index)), true);
        rebuildFilterOptions();
        rebuildGroupOptions();
        rebuildColumnManager();
        applyCurrentWidths();
        renderRows();
      });
    }
    if (state.sort && state.sort.key && (state.sort.direction === "asc" || state.sort.direction === "desc") && getHeaderByKey(String(state.sort.key))) {
      applySort(String(state.sort.key), state.sort.direction, false);
    } else {
      state.sort = null;
      saveJson(sortStateKey, null);
      renderRows();
    }
  }

  function setupAutoRefresh() {
    const rawSeconds = document.body?.getAttribute("data-auto-refresh-seconds") || "";
    const seconds = Number(rawSeconds);
    if (!Number.isFinite(seconds) || seconds <= 0) return;
    const scrollKey = `tailor-page-scroll:${window.location.pathname}`;
    try {
      const saved = window.sessionStorage.getItem(scrollKey);
      if (saved) {
        const state = JSON.parse(saved);
        if (Number.isFinite(state?.x) || Number.isFinite(state?.y)) {
          window.scrollTo(Number(state?.x || 0), Number(state?.y || 0));
        }
      }
    } catch (_error) {
      // ignore
    }
    window.addEventListener("beforeunload", () => {
      try {
        window.sessionStorage.setItem(scrollKey, JSON.stringify({ x: window.scrollX, y: window.scrollY }));
      } catch (_error) {
        // ignore
      }
    });
    window.setTimeout(() => {
      try {
        window.sessionStorage.setItem(scrollKey, JSON.stringify({ x: window.scrollX, y: window.scrollY }));
      } catch (_error) {
        // ignore
      }
      window.location.reload();
    }, Math.round(seconds * 1000));
  }

  document.querySelectorAll("table[data-enhanced-table='true']").forEach(setupTable);
  setupAutoRefresh();
})();
</script>
    """.strip()


def _html_table_controls(table_id: str, placeholder: str = "Search all fields...") -> str:
    return (
        f'<div class="table-tools">'
        f'<input class="table-search" type="search" data-table-search="{_html_cell(table_id)}" '
        f'placeholder="{_html_cell(placeholder)}" aria-label="{_html_cell(placeholder)}">'
        f'<div class="table-meta" data-table-meta="{_html_cell(table_id)}">0 row(s)</div>'
        f'</div>'
    )


def _parse_exec_log_header(line: str) -> dict[str, str] | None:
    match = re.match(r"^\[(?P<timestamp>[^\]]+)\]\s+\[(?P<event>[^\]]+)\]\s+(?P<title>.+)$", (line or "").strip())
    if not match:
        return None
    return {
        "timestamp": str(match.group("timestamp") or "").strip(),
        "event": str(match.group("event") or "").strip(),
        "title": str(match.group("title") or "").strip(),
    }


def _parse_exec_log_timestamp(text: str) -> dt.datetime | None:
    raw = (text or "").strip()
    if not raw:
        return None
    simple = re.match(r"^(?P<date>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", raw)
    if not simple:
        return None
    try:
        parsed = dt.datetime.strptime(str(simple.group("date")), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    local_tz = dt.datetime.now().astimezone().tzinfo or UTC
    return parsed.replace(tzinfo=local_tz).astimezone(UTC)


def _parse_exec_log_sections(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8", errors="replace")
    raw_sections = [item.strip() for item in text.split("=" * 80) if item.strip()]
    sections: list[dict[str, Any]] = []
    for raw in raw_sections:
        lines = raw.splitlines()
        if not lines:
            continue
        header = _parse_exec_log_header(lines[0])
        if not header:
            continue
        body = "\n".join(lines[1:]).strip()
        parsed_dt = _parse_exec_log_timestamp(header["timestamp"])
        sections.append(
            {
                "event": header["event"].lower(),
                "title": header["title"],
                "timestamp_text": header["timestamp"],
                "timestamp_dt": parsed_dt,
                "body": body,
            }
        )
    return sections


def _extract_body_field(body: str, label: str) -> str:
    pattern = re.compile(rf"^{re.escape(label)}:\s*(.*)$", flags=re.MULTILINE)
    match = pattern.search(body or "")
    return str(match.group(1) or "").strip() if match else ""


def _extract_body_json(body: str, start_label: str, end_label: str) -> dict[str, Any]:
    pattern = re.compile(
        rf"{re.escape(start_label)}\n(?P<json>\{{.*?\}})\n\n{re.escape(end_label)}",
        flags=re.DOTALL,
    )
    match = pattern.search(body or "")
    if not match:
        return {}
    try:
        return json.loads(str(match.group("json") or "{}"))
    except json.JSONDecodeError:
        return {}


def _resolve_report_path(project: TailorProject, path_text: str) -> Path | None:
    text = (path_text or "").strip()
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = project.root / path
    if not path.exists():
        try:
            relative = path.relative_to(project.root)
        except Exception:
            relative = None
        if relative is not None and len(relative.parts) == 3 and relative.parts[0].lower() == "agents":
            agent_root = project.root / relative.parts[0] / relative.parts[1]
            file_name = relative.parts[2]
            for folder_name in ("workflows", "prompts", "templates", "data"):
                relocated_candidate = agent_root / folder_name / file_name
                if relocated_candidate.exists():
                    path = relocated_candidate
                    break
        if not path.exists():
            try:
                relative = path.relative_to(project.root)
            except Exception:
                relative = None
            if relative is not None:
                clutter_candidate = project.root / "clutter" / relative
                if clutter_candidate.exists():
                    path = clutter_candidate
    try:
        return path.resolve()
    except Exception:
        return path


def _best_matching_output_context_path(project: TailorProject, requested_path_text: str, started_dt: dt.datetime | None) -> Path | None:
    requested = _resolve_report_path(project, requested_path_text)
    if requested is None:
        return None
    parent = requested.parent
    if not parent.exists():
        return requested if requested.exists() else None
    pattern = f"{requested.stem}*{requested.suffix}"
    candidates = [path for path in parent.glob(pattern) if path.is_file()]
    if requested.exists() and requested not in candidates:
        candidates.append(requested)
    if not candidates:
        return requested if requested.exists() else None
    if started_dt is not None:
        cutoff = started_dt - dt.timedelta(minutes=2)
        time_candidates = [
            path for path in candidates
            if dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC) >= cutoff
        ]
        if time_candidates:
            candidates = time_candidates
    return max(candidates, key=lambda item: item.stat().st_mtime)


def _snippet_for_path(path: Path | None, *, limit: int = 500) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _html_file_link(project: TailorProject, path_text: str, *, label: str = "") -> str:
    resolved = _resolve_report_path(project, path_text)
    if resolved is None:
        return '<span class="empty">None</span>'
    display = label or (str(resolved.relative_to(project.root)) if resolved.is_relative_to(project.root) else str(resolved))
    href = resolved.as_uri() if resolved.is_absolute() else ""
    return f'<a href="{_html_cell(href)}">{_html_cell(display)}</a>'


def _html_path_and_snippet(project: TailorProject, path_text: str, *, snippet: str = "", empty_label: str = "None") -> str:
    resolved = _resolve_report_path(project, path_text)
    if resolved is None:
        return f'<div class="empty">{_html_cell(empty_label)}</div>'
    rel = str(resolved.relative_to(project.root)) if resolved.is_relative_to(project.root) else str(resolved)
    href = resolved.as_uri() if resolved.is_absolute() else ""
    snippet_text = snippet or _snippet_for_path(resolved, limit=420)
    return (
        '<div class="msg">'
        f'<div class="subject"><a href="{_html_cell(href)}">{_html_cell(rel)}</a></div>'
        f'<div class="preview">{_html_cell(snippet_text)}</div>'
        '</div>'
    )


def _parse_workflow_file_from_command_line(command_line: str) -> str:
    text = str(command_line or "").strip()
    if not text:
        return ""
    match = re.search(r'--workflow-file\s+("([^"]+)"|(\S+))', text, flags=re.IGNORECASE)
    if not match:
        return ""
    return (match.group(2) or match.group(3) or "").strip()


def _parse_windows_cim_datetime(value: str) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.match(r"^(\d{14})\.(\d{6})([+-]\d{3})$", text)
    if not match:
        return None
    base_text, micros_text, offset_minutes_text = match.groups()
    try:
        naive = dt.datetime.strptime(base_text, "%Y%m%d%H%M%S").replace(microsecond=int(micros_text))
        offset = dt.timedelta(minutes=int(offset_minutes_text))
    except ValueError:
        return None
    tzinfo = dt.timezone(offset)
    return naive.replace(tzinfo=tzinfo).astimezone(UTC)


def _list_live_workflow_processes(project: TailorProject) -> list[dict[str, Any]]:
    processes: list[dict[str, Any]] = []
    if os.name == "nt":
        ps_command = (
            "Get-CimInstance Win32_Process | "
            "Where-Object { $_.Name -match 'python' -and $_.CommandLine -match 'tailor_ops.py' -and $_.CommandLine -match 'workflow-exec' } | "
            "Select-Object ProcessId, Name, CommandLine, CreationDate | ConvertTo-Json -Depth 3 -Compress"
        )
        try:
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps_command],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
        except Exception:
            return []
        if result.returncode != 0:
            return []
        raw = (result.stdout or "").strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        items = parsed if isinstance(parsed, list) else [parsed]
        for item in items:
            if not isinstance(item, dict):
                continue
            pid_text = str(item.get("ProcessId") or "").strip()
            if not pid_text.isdigit():
                continue
            workflow_file_text = _parse_workflow_file_from_command_line(str(item.get("CommandLine") or ""))
            workflow_path = _resolve_report_path(project, workflow_file_text) if workflow_file_text else None
            processes.append(
                {
                    "pid": int(pid_text),
                    "command_line": str(item.get("CommandLine") or ""),
                    "workflow_file": workflow_file_text,
                    "workflow_path": str(workflow_path) if workflow_path else "",
                    "created_dt": _parse_windows_cim_datetime(str(item.get("CreationDate") or "")),
                }
            )
        return processes

    try:
        result = subprocess.run(
            ["ps", "-ax", "-o", "pid=,command="],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except Exception:
        return []
    if result.returncode != 0:
        return []
    for line in (result.stdout or "").splitlines():
        stripped = line.strip()
        if not stripped or "tailor_ops.py" not in stripped or "workflow-exec" not in stripped:
            continue
        parts = stripped.split(maxsplit=1)
        if not parts or not parts[0].isdigit():
            continue
        workflow_file_text = _parse_workflow_file_from_command_line(parts[1] if len(parts) > 1 else "")
        workflow_path = _resolve_report_path(project, workflow_file_text) if workflow_file_text else None
        processes.append(
            {
                "pid": int(parts[0]),
                "command_line": parts[1] if len(parts) > 1 else "",
                "workflow_file": workflow_file_text,
                "workflow_path": str(workflow_path) if workflow_path else "",
                "created_dt": None,
            }
        )
    return processes


def _match_live_workflow_process(project: TailorProject, row: dict[str, Any], processes: list[dict[str, Any]]) -> dict[str, Any] | None:
    row_workflow_path = _resolve_report_path(project, str(row.get("workflow_file") or ""))
    if row_workflow_path is None:
        return None
    matching = [item for item in processes if str(item.get("workflow_path") or "") == str(row_workflow_path)]
    if not matching:
        return None
    started_dt = row.get("started_dt")
    if isinstance(started_dt, dt.datetime):
        def sort_key(item: dict[str, Any]) -> Any:
            created_dt = item.get("created_dt")
            delta = abs((created_dt - started_dt).total_seconds()) if isinstance(created_dt, dt.datetime) else float("inf")
            return (delta, -int(item.get("pid") or 0))
        matching.sort(key=sort_key)
        return matching[0]
    matching.sort(key=lambda item: -int(item.get("pid") or 0))
    return matching[0]


def _workflow_kill_command(project: TailorProject, pid: int) -> str:
    python_exe = Path(sys.executable).resolve()
    script_path = (project.root / "scripts" / "tailor_ops.py").resolve()
    return f'"{python_exe}" "{script_path}" kill-workflow-run --pid {int(pid)}'


def _write_workflow_kill_script(project: TailorProject, pid: int) -> Path:
    actions_dir = project.root / "artifacts" / "dashboard-actions"
    actions_dir.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        script_path = actions_dir / f"kill_workflow_{int(pid)}.cmd"
        command = _workflow_kill_command(project, pid)
        script_text = (
            "@echo off\r\n"
            f'cd /d "{project.root.resolve()}"\r\n'
            f"{command}\r\n"
            "pause\r\n"
        )
    else:
        script_path = actions_dir / f"kill_workflow_{int(pid)}.sh"
        command = _workflow_kill_command(project, pid)
        script_text = (
            "#!/usr/bin/env bash\n"
            f'cd "{project.root.resolve()}"\n'
            f"{command}\n"
        )
    script_path.write_text(script_text, encoding="utf-8")
    return script_path


def _build_workflow_run_row_from_exec_log(project: TailorProject, path: Path) -> dict[str, Any] | None:
    sections = _parse_exec_log_sections(path)
    if not sections:
        return None
    start = next((section for section in sections if section["event"] == "workflow_start"), None)
    if start is None:
        return None
    completed = next((section for section in reversed(sections) if section["event"] == "workflow_complete"), None)
    errored = next((section for section in reversed(sections) if section["event"] == "error"), None)
    status = "running"
    end_section = None
    if completed is not None:
        status = "completed"
        end_section = completed
    elif errored is not None:
        status = "error"
        end_section = errored

    start_body = start["body"]
    cli_args = _extract_body_json(start_body, "CLI args:", "Workflow settings:")
    workflow_settings = _extract_body_json(start_body, "Workflow settings:", "Resolved base context:")
    agent = _extract_body_field(start_body, "Agent") or _infer_agent_name_from_workflow_path(project, path)
    process_id_text = _extract_body_field(start_body, "Process ID")
    process_id = int(process_id_text) if process_id_text.isdigit() else 0
    workflow_file = _extract_body_field(start_body, "Workflow file")
    workflow_name = _extract_body_field(start_body, "Workflow name") or Path(workflow_file or path.name).stem
    workflow_type = _extract_body_field(completed["body"], "Workflow type") if completed is not None else ""
    if not workflow_type:
        workflow_type = str(workflow_settings.get("workflow-type") or "single_run")
    iterations_text = _extract_body_field(completed["body"], "Iterations processed") if completed is not None else ""
    try:
        iterations_processed = int(iterations_text or 0)
    except ValueError:
        iterations_processed = 0
    requested_output_context = str(cli_args.get("output_context_json") or _extract_body_field(completed["body"] if completed else "", "Final context output") or "")
    output_context_path = _best_matching_output_context_path(project, requested_output_context, start["timestamp_dt"])
    latest_message_response = next((section for section in reversed(sections) if section["event"] == "message_response"), None)
    latest_warning = next((section for section in reversed(sections) if section["event"] in {"warning", "notification"}), None)
    latest_error = errored["body"] if errored is not None else ""
    last_section_dt = next((section["timestamp_dt"] for section in reversed(sections) if isinstance(section.get("timestamp_dt"), dt.datetime)), None)
    if last_section_dt is None:
        try:
            last_section_dt = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        except OSError:
            last_section_dt = None
    if end_section is not None and start["timestamp_dt"] is not None and end_section["timestamp_dt"] is not None:
        duration_seconds = max(0.0, (end_section["timestamp_dt"] - start["timestamp_dt"]).total_seconds())
    elif start["timestamp_dt"] is not None:
        duration_seconds = max(0.0, (dt.datetime.now(tz=UTC) - start["timestamp_dt"]).total_seconds())
    else:
        duration_seconds = 0.0

    history_link = _extract_body_field(latest_message_response["body"], "History message link") if latest_message_response else ""
    sent_link = _extract_body_field(latest_message_response["body"], "Sent message link") if latest_message_response else ""
    history_snippet = _snippet_for_path(_resolve_report_path(project, history_link), limit=320)
    sent_snippet = _snippet_for_path(_resolve_report_path(project, sent_link), limit=320)
    error_excerpt = _preview_text(latest_error, limit=260)
    warning_excerpt = _preview_text(latest_warning["body"], limit=260) if latest_warning else ""

    return {
        "agent": agent,
        "process_id": process_id,
        "workflow_name": workflow_name,
        "workflow_file": workflow_file,
        "workflow_type": workflow_type or "single_run",
        "status": status,
        "started_dt": start["timestamp_dt"],
        "started_at": _format_report_dt(start["timestamp_dt"]),
        "ended_dt": end_section["timestamp_dt"] if end_section is not None else None,
        "ended_at": _format_report_dt(end_section["timestamp_dt"]) if end_section is not None else "",
        "last_activity_dt": last_section_dt,
        "last_activity_at": _format_report_dt(last_section_dt),
        "duration_seconds": duration_seconds,
        "iterations_processed": iterations_processed,
        "api_request_count": sum(1 for section in sections if section["event"] == "api_request"),
        "api_response_count": sum(1 for section in sections if section["event"] == "api_response"),
        "warning_count": sum(1 for section in sections if section["event"] == "warning"),
        "notification_count": sum(1 for section in sections if section["event"] == "notification"),
        "message_response_count": sum(1 for section in sections if section["event"] == "message_response"),
        "log_file": str(path),
        "log_file_relative": str(path.relative_to(project.root)) if path.is_relative_to(project.root) else str(path),
        "output_context_path": str(output_context_path) if output_context_path else "",
        "output_context_relative": (
            str(output_context_path.relative_to(project.root)) if output_context_path and output_context_path.is_relative_to(project.root) else str(output_context_path or "")
        ),
        "output_context_snippet": _snippet_for_path(output_context_path, limit=360) if output_context_path else "",
        "error_excerpt": error_excerpt,
        "warning_excerpt": warning_excerpt,
        "history_link": history_link,
        "history_snippet": history_snippet,
        "sent_link": sent_link,
        "sent_snippet": sent_snippet,
        "latest_message_response_title": latest_message_response["title"] if latest_message_response else "",
    }


def _workflow_dashboard_html(project: TailorProject, report: dict[str, Any]) -> str:
    table_id = "workflow-dashboard-table"
    summary = report["summary"]
    stopped_count = max(0, int(summary.get("run_count", 0)) - int(summary.get("running_count", 0)))

    notification_items = []
    for item in report.get("notifications", [])[:12]:
        detail_text = _preview_text(str(item.get("message") or ""), limit=180)
        notification_items.append(
            f'<li><strong>{_html_cell(str(item.get("level") or "").upper())}</strong> '
            f'{_html_cell(str(item.get("title") or ""))} '
            f'<span class="note-time">{_html_cell(str(item.get("created_utc") or ""))}</span>'
            f'<div class="note-body">{_html_cell(detail_text)}</div></li>'
        )

    rows_html: list[str] = []
    for row in report.get("rows", []):
        state = "running" if row["status"] == "running" else "stopped"
        result = "running" if row["status"] == "running" else ("ok" if row["status"] == "completed" else "error")
        pid_value = int(row.get("live_process_id") or row.get("process_id") or 0)
        kill_command = str(row.get("kill_command") or "")
        kill_script_path = str(row.get("kill_script_path") or "")
        workflow_file_label = Path(str(row.get("workflow_file") or "")).name or "-"
        log_file_label = str(row.get("log_file_relative") or Path(str(row.get("log_file") or "")).name or "-")
        output_context_label = str(row.get("output_context_relative") or Path(str(row.get("output_context_path") or "")).name or "-")
        history_label = Path(str(row.get("history_link") or "")).name or "-"
        sent_label = Path(str(row.get("sent_link") or "")).name or "-"
        issue_text = row["warning_excerpt"] or row["error_excerpt"] or ""
        api_summary = f"{row['api_request_count']}/{row['api_response_count']}"
        started_iso = row["started_dt"].isoformat() if row["started_dt"] else ""
        ended_iso = row["ended_dt"].isoformat() if row["ended_dt"] else ""
        row_title = (
            f"Agent: {row['agent']}\n"
            f"Workflow: {row['workflow_name']}\n"
            f"Status: {row['status']}\n"
            f"PID: {pid_value if pid_value > 0 else 'n/a'}\n"
            f"Started: {row['started_at']}\n"
            f"Ended: {row['ended_at']}\n"
            f"Issue: {issue_text}"
        )
        kill_cell = '<span class="empty">-</span>'
        if state == "running" and pid_value > 0 and kill_command:
            parts = [
                f'<button type="button" class="action-btn" data-copy-text="{html.escape(kill_command, quote=True)}">copy</button>'
            ]
            if kill_script_path:
                parts.append(_html_file_link(project, kill_script_path, label="script"))
            kill_cell = '<div class="action-group">' + " ".join(parts) + "</div>"
        rows_html.append(
            f"<tr class=\"data-row row-{state}\" data-row=\"data\" title=\"{html.escape(row_title, quote=True)}\">"
            f"<td class=\"col-agent\" data-filter-value=\"{html.escape(str(row['agent']), quote=True)}\" title=\"{html.escape(str(row['agent']), quote=True)}\">{_html_cell(row['agent'])}</td>"
            f"<td class=\"col-workflow\" data-filter-value=\"{html.escape(str(row['workflow_name']), quote=True)}\" title=\"{html.escape(str(row['workflow_name']), quote=True)}\">{_html_cell(row['workflow_name'])}</td>"
            f"<td class=\"col-type\" data-filter-value=\"{html.escape(str(row['workflow_type']), quote=True)}\" title=\"{html.escape(str(row['workflow_type']), quote=True)}\">{_html_cell(row['workflow_type'])}</td>"
            f"<td class=\"col-state\" data-sort-value=\"{state}\" data-filter-value=\"{state}\"><span class=\"status-pill status-{state}\">{_html_cell(state)}</span></td>"
            f"<td class=\"col-result\" data-sort-value=\"{row['status']}\" data-filter-value=\"{result}\"><span class=\"result-pill result-{result}\">{_html_cell(result)}</span></td>"
            f"<td class=\"col-pid\" data-sort-value=\"{pid_value}\">{pid_value if pid_value > 0 else ''}</td>"
            f"<td class=\"col-time\" data-sort-value=\"{started_iso}\" data-format-local=\"{html.escape(str(row['started_at']), quote=True)}\" title=\"{html.escape(str(row['started_at']), quote=True)}\">{_html_cell(row['started_at'])}</td>"
            f"<td class=\"col-time\" data-sort-value=\"{ended_iso}\" data-format-local=\"{html.escape(str(row['ended_at']), quote=True)}\" title=\"{html.escape(str(row['ended_at']), quote=True)}\">{_html_cell(row['ended_at'])}</td>"
            f"<td class=\"col-num\" data-sort-value=\"{row['duration_seconds']:.3f}\" data-format-short=\"{row['duration_seconds']:.1f}s\">{row['duration_seconds']:.1f}s</td>"
            f"<td class=\"col-num\" data-sort-value=\"{row['iterations_processed']}\">{row['iterations_processed']}</td>"
            f"<td class=\"col-num\" data-sort-value=\"{row['api_request_count'] * 1000000 + row['api_response_count']}\">{_html_cell(api_summary)}</td>"
            f"<td class=\"col-num\" data-sort-value=\"{row['message_response_count']}\">{row['message_response_count']}</td>"
            f"<td class=\"col-num\" data-sort-value=\"{row['warning_count']}\">{row['warning_count']}</td>"
            f"<td class=\"col-num\" data-sort-value=\"{row['notification_count']}\">{row['notification_count']}</td>"
            f"<td class=\"col-link\" data-label-value=\"{html.escape(workflow_file_label, quote=True)}\" title=\"{html.escape(str(row.get('workflow_file') or ''), quote=True)}\">{_html_file_link(project, row['workflow_file'], label=workflow_file_label)}</td>"
            f"<td class=\"col-link\" data-label-value=\"{html.escape(log_file_label, quote=True)}\" title=\"{html.escape(str(row.get('log_file_relative') or ''), quote=True)}\">{_html_file_link(project, row['log_file'], label=log_file_label)}</td>"
            f"<td class=\"col-link\" data-label-value=\"{html.escape(output_context_label, quote=True)}\" title=\"{html.escape(str(row.get('output_context_relative') or ''), quote=True)}\">{_html_file_link(project, row['output_context_path'], label=output_context_label) if row.get('output_context_path') else '<span class=\"empty\">-</span>'}</td>"
            f"<td class=\"col-link\" data-label-value=\"{html.escape(history_label, quote=True)}\" title=\"{html.escape(str(row.get('history_link') or ''), quote=True)}\">{_html_file_link(project, row['history_link'], label=history_label) if row.get('history_link') else '<span class=\"empty\">-</span>'}</td>"
            f"<td class=\"col-link\" data-label-value=\"{html.escape(sent_label, quote=True)}\" title=\"{html.escape(str(row.get('sent_link') or ''), quote=True)}\">{_html_file_link(project, row['sent_link'], label=sent_label) if row.get('sent_link') else '<span class=\"empty\">-</span>'}</td>"
            f"<td class=\"col-kill\" title=\"{html.escape(kill_command, quote=True)}\">{kill_cell}</td>"
            f"<td class=\"col-issue\" title=\"{html.escape(issue_text, quote=True)}\">{_html_cell(issue_text)}</td>"
            "</tr>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Workflow Dashboard</title>
  <style>
    :root {{
      --bg: #f4efe7;
      --panel: #fffaf2;
      --ink: #1d2732;
      --muted: #5a6776;
      --accent: #8b3d23;
      --line: #d8cab9;
      --stripe: #f8f1e7;
      --running: #1f7a43;
      --stopped: #a32f2f;
      --ok: #2f6f44;
      --err: #9a2b2b;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(139,61,35,0.10), transparent 22%),
        linear-gradient(180deg, #fbf6ef 0%, var(--bg) 100%);
    }}
    .wrap {{ padding: 10px; }}
    .hero {{
      background: linear-gradient(140deg, rgba(29,39,50,0.96), rgba(139,61,35,0.92));
      color: #fff9f1;
      border-radius: 12px;
      padding: 12px 14px;
      box-shadow: 0 10px 24px rgba(22,32,43,0.14);
    }}
    h1 {{ margin: 0 0 4px; font-size: 20px; line-height: 1.1; }}
    .sub {{ color: rgba(255,249,241,0.88); font-size: 12px; line-height: 1.25; }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
      gap: 8px;
      margin-top: 10px;
    }}
    .stat {{
      background: rgba(255,255,255,0.10);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 10px;
      padding: 8px;
    }}
    .stat .label {{ font-size: 10px; text-transform: uppercase; letter-spacing: 0.06em; opacity: 0.82; }}
    .stat .value {{ font-size: 18px; margin-top: 2px; line-height: 1.1; }}
    .stack {{ display: grid; gap: 10px; margin-top: 10px; }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px;
      box-shadow: 0 8px 20px rgba(31,41,51,0.06);
      overflow: hidden;
    }}
    .panel h2 {{
      margin: 0 0 8px;
      font-size: 14px;
      line-height: 1.2;
    }}
    .meta {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 8px;
      font-size: 11px;
      color: var(--muted);
    }}
    .table-wrap {{
      overflow: auto;
      max-height: calc(100vh - 220px);
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fffdf9;
    }}
    table {{
      width: max-content;
      min-width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 3px 6px;
      vertical-align: middle;
      text-align: left;
      font-size: 11px;
      line-height: 1.15;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      height: 22px;
      max-height: 22px;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #f0e3d4;
      z-index: 1;
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted);
    }}
    tbody tr:nth-child(even) {{ background: var(--stripe); }}
    .row-running {{ background: rgba(31,122,67,0.08); }}
    .row-stopped {{ background: rgba(163,47,47,0.06); }}
    .row-stopped:nth-child(even) {{ background: rgba(163,47,47,0.09); }}
    .row-running:nth-child(even) {{ background: rgba(31,122,67,0.11); }}
    .empty {{ color: var(--muted); font-style: italic; }}
    .notes {{
      margin: 0;
      padding-left: 18px;
      font-size: 11px;
      max-height: 160px;
      overflow: auto;
    }}
    .notes li {{ margin-bottom: 6px; }}
    .note-time {{ color: var(--muted); font-size: 10px; margin-left: 6px; }}
    .note-body {{ margin-top: 2px; color: var(--ink); font-size: 11px; line-height: 1.25; }}
    .status-pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 58px;
      padding: 2px 6px;
      border-radius: 999px;
      border: 1px solid transparent;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      font-size: 10px;
    }}
    .status-running {{
      background: rgba(31,122,67,0.14);
      color: var(--running);
      border-color: rgba(31,122,67,0.25);
    }}
    .status-stopped {{
      background: rgba(163,47,47,0.12);
      color: var(--stopped);
      border-color: rgba(163,47,47,0.22);
    }}
    .result-pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 52px;
      padding: 2px 6px;
      border-radius: 999px;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }}
    .result-ok {{
      background: rgba(47,111,68,0.12);
      color: var(--ok);
    }}
    .result-error {{
      background: rgba(154,43,43,0.12);
      color: var(--err);
    }}
    .result-running {{
      background: rgba(31,122,67,0.14);
      color: var(--running);
    }}
    a {{
      color: #214f8d;
      text-decoration: none;
    }}
    a:hover {{ text-decoration: underline; }}
    .col-agent {{ width: 110px; max-width: 110px; }}
    .col-workflow {{ width: 210px; max-width: 210px; }}
    .col-type {{ width: 88px; max-width: 88px; }}
    .col-state {{ width: 72px; max-width: 72px; }}
    .col-result {{ width: 72px; max-width: 72px; }}
    .col-pid {{ width: 68px; max-width: 68px; text-align: right; font-variant-numeric: tabular-nums; }}
    .col-time {{ width: 146px; max-width: 146px; }}
    .col-num {{ width: 58px; max-width: 58px; text-align: right; font-variant-numeric: tabular-nums; }}
    .col-link {{ width: 160px; max-width: 160px; }}
    .col-kill {{ width: 120px; max-width: 120px; }}
    .col-issue {{ width: 260px; max-width: 260px; }}
    .action-group {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      max-width: 100%;
    }}
    .action-btn {{
      border: 1px solid var(--line);
      background: #fffdf8;
      color: var(--ink);
      border-radius: 999px;
      padding: 1px 7px;
      font-size: 10px;
      line-height: 1.2;
      cursor: pointer;
    }}
    .action-btn:hover {{
      background: #f3eadc;
    }}
    {_html_table_interaction_assets()}
    @media (max-width: 1100px) {{
      .table-wrap {{ max-height: calc(100vh - 250px); }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Workflow Dashboard</h1>
      <div class="sub">Workflow runs from the last {_html_cell(str(report.get('hours') or 24))} hours. Running workflows are green. Stopped workflows are red. The table supports search, filters, grouping, sortable and resizable columns, column reordering and hiding, and per-column display formats. Auto-refresh runs every 5 seconds while preserving table state.</div>
      <div class="stats">
        <div class="stat"><div class="label">Generated</div><div class="value">{_html_cell(report['generated_local'])}</div></div>
        <div class="stat"><div class="label">Shown</div><div class="value">{summary['run_count']}</div></div>
        <div class="stat"><div class="label">Running</div><div class="value">{summary['running_count']}</div></div>
        <div class="stat"><div class="label">Stopped</div><div class="value">{stopped_count}</div></div>
        <div class="stat"><div class="label">Errors</div><div class="value">{summary['error_count']}</div></div>
        <div class="stat"><div class="label">Completed</div><div class="value">{summary['completed_count']}</div></div>
        <div class="stat"><div class="label">Last 24h</div><div class="value">{summary['runs_last_24h']}</div></div>
        <div class="stat"><div class="label">Agents</div><div class="value">{summary['agent_count']}</div></div>
        <div class="stat"><div class="label">Avg Duration</div><div class="value">{summary['avg_duration_seconds']:.1f}s</div></div>
      </div>
    </section>
    <section class="stack">
      <section class="panel">
        <div class="meta">
          <div>One row per workflow run. Hover truncated cells for the full value. Kill actions are only shown for live running processes.</div>
          <div>{summary['run_count']} rows visible</div>
        </div>
        {_html_table_controls(table_id, "Search workflow runs, links, warnings, and statuses...")}
        <div class="dashboard-controls">
          <div class="control-group">
            <label for="{table_id}-agent-filter">Agent</label>
            <select id="{table_id}-agent-filter" data-table-filter="{table_id}" data-filter-key="agent"><option value="">All</option></select>
          </div>
          <div class="control-group">
            <label for="{table_id}-workflow-filter">Workflow</label>
            <select id="{table_id}-workflow-filter" data-table-filter="{table_id}" data-filter-key="workflow"><option value="">All</option></select>
          </div>
          <div class="control-group">
            <label for="{table_id}-type-filter">Type</label>
            <select id="{table_id}-type-filter" data-table-filter="{table_id}" data-filter-key="type"><option value="">All</option></select>
          </div>
          <div class="control-group">
            <label for="{table_id}-state-filter">State</label>
            <select id="{table_id}-state-filter" data-table-filter="{table_id}" data-filter-key="state"><option value="">All</option></select>
          </div>
          <div class="control-group">
            <label for="{table_id}-result-filter">Result</label>
            <select id="{table_id}-result-filter" data-table-filter="{table_id}" data-filter-key="result"><option value="">All</option></select>
          </div>
          <div class="control-group">
            <label for="{table_id}-group-by">Group By</label>
            <select id="{table_id}-group-by" data-table-group="{table_id}"><option value="">None</option></select>
          </div>
          <div class="control-group">
            <label>Columns</label>
            <button type="button" data-columns-toggle="{table_id}">Manage Columns</button>
          </div>
          <div class="control-group">
            <label>Reset</label>
            <button type="button" data-columns-reset="{table_id}">Reset Layout</button>
          </div>
        </div>
        <div class="columns-panel" data-columns-panel="{table_id}" hidden>
          <div class="column-list" data-columns-list="{table_id}"></div>
        </div>
        <div class="table-wrap">
          <table id="{table_id}" data-enhanced-table="true">
            <thead>
              <tr>
                <th data-col-key="agent">Agent</th>
                <th data-col-key="workflow">Workflow</th>
                <th data-col-key="type">Type</th>
                <th data-col-key="state">State</th>
                <th data-col-key="result">Result</th>
                <th data-col-key="pid">PID</th>
                <th data-col-key="started" data-format-options="local,relative,utc,iso">Started</th>
                <th data-col-key="ended" data-format-options="local,relative,utc,iso">Ended</th>
                <th data-col-key="duration" data-format-options="short,seconds,hms">Duration</th>
                <th data-col-key="iterations">Iter</th>
                <th data-col-key="api">API</th>
                <th data-col-key="replies">Replies</th>
                <th data-col-key="warnings">Warn</th>
                <th data-col-key="notifications">Note</th>
                <th data-col-key="workflow_file" data-format-options="label,path">Workflow File</th>
                <th data-col-key="run_log" data-format-options="label,path">Run Log</th>
                <th data-col-key="context" data-format-options="label,path">Context</th>
                <th data-col-key="inbound" data-format-options="label,path">Inbound</th>
                <th data-col-key="outbound" data-format-options="label,path">Outbound</th>
                <th data-col-key="kill" data-groupable="false">Kill</th>
                <th data-col-key="issue">Latest Issue</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows_html)}
            </tbody>
          </table>
        </div>
      </section>
      <aside class="panel">
        <h2>Notifications</h2>
        <ul class="notes">
          {''.join(notification_items) or '<li>No active notifications.</li>'}
        </ul>
      </aside>
    </section>
  </div>
  <script>
    document.addEventListener("DOMContentLoaded", () => {{
      document.querySelectorAll(".action-btn[data-copy-text]").forEach(button => {{
        button.addEventListener("click", async () => {{
          const text = button.getAttribute("data-copy-text") || "";
          if (!text) return;
          try {{
            await navigator.clipboard.writeText(text);
            const original = button.textContent;
            button.textContent = "copied";
            window.setTimeout(() => {{ button.textContent = original; }}, 1200);
          }} catch (_error) {{
            window.prompt("Copy kill command:", text);
          }}
        }});
      }});
    }});
  </script>
  {_html_table_interaction_script()}
</body>
</html>
"""


def cmd_generate_workflow_dashboard(args: argparse.Namespace, project: TailorProject) -> int:
    hours = max(1, int(getattr(args, "hours", 24) or 24))
    since_utc = dt.datetime.now(tz=UTC) - dt.timedelta(hours=hours)
    rows: list[dict[str, Any]] = []
    for path in sorted(project.exec_logs_dir.glob("*.txt"), key=lambda item: item.stat().st_mtime, reverse=True):
        if path.name.startswith("executive_summary_"):
            continue
        row = _build_workflow_run_row_from_exec_log(project, path)
        if row is None:
            continue
        if not getattr(args, "include_all", False):
            started_dt = row.get("started_dt")
            if isinstance(started_dt, dt.datetime) and started_dt < since_utc:
                continue
        rows.append(row)
        if getattr(args, "limit", 0):
            if len(rows) >= int(getattr(args, "limit", 0)):
                break
    rows.sort(key=lambda item: item.get("started_dt") or dt.datetime.min.replace(tzinfo=UTC), reverse=True)
    live_processes = _list_live_workflow_processes(project)
    for row in rows:
        live_match = _match_live_workflow_process(project, row, live_processes) if row.get("status") == "running" else None
        row["live_process_id"] = int(live_match.get("pid") or 0) if live_match else 0
        row["live_process_started_at"] = _format_report_dt(live_match.get("created_dt")) if live_match and isinstance(live_match.get("created_dt"), dt.datetime) else ""
        if live_match and int(live_match.get("pid") or 0) > 0:
            pid_value = int(live_match["pid"])
            row["kill_command"] = _workflow_kill_command(project, pid_value)
            row["kill_script_path"] = str(_write_workflow_kill_script(project, pid_value))
        else:
            row["kill_command"] = ""
            row["kill_script_path"] = ""
            if row.get("status") == "running":
                row["status"] = "stopped"
                fallback_end_dt = row.get("last_activity_dt") if isinstance(row.get("last_activity_dt"), dt.datetime) else None
                row["ended_dt"] = fallback_end_dt
                row["ended_at"] = _format_report_dt(fallback_end_dt) if fallback_end_dt else ""
                start_dt = row.get("started_dt")
                if isinstance(start_dt, dt.datetime) and isinstance(fallback_end_dt, dt.datetime):
                    row["duration_seconds"] = max(0.0, (fallback_end_dt - start_dt).total_seconds())
                if not str(row.get("warning_excerpt") or "").strip() and not str(row.get("error_excerpt") or "").strip():
                    row["error_excerpt"] = "Workflow is not live now; the log ended without a completion marker."
    completed_durations = [float(item["duration_seconds"]) for item in rows if item.get("status") == "completed"]
    summary = {
        "run_count": len(rows),
        "completed_count": sum(1 for item in rows if item.get("status") == "completed"),
        "error_count": sum(1 for item in rows if item.get("status") == "error"),
        "running_count": sum(1 for item in rows if item.get("status") == "running"),
        "runs_last_24h": sum(1 for item in rows if isinstance(item.get("started_dt"), dt.datetime) and item["started_dt"] >= dt.datetime.now(tz=UTC) - dt.timedelta(hours=24)),
        "agent_count": len({str(item.get("agent") or "") for item in rows if str(item.get("agent") or "").strip()}),
        "avg_duration_seconds": (sum(completed_durations) / len(completed_durations)) if completed_durations else 0.0,
    }
    generated = dt.datetime.now().astimezone()
    report = {
        "generated_utc": now_iso(),
        "generated_local": generated.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "hours": hours,
        "summary": summary,
        "rows": rows,
        "notifications": _load_user_notifications(project),
    }
    html_text = _workflow_dashboard_html(project, report)
    output_file = Path(getattr(args, "output_file", "") or (project.root / "agents" / "general" / "reports" / f"workflow_dashboard_{generated.strftime('%Y%m%d_%H%M%S')}.html"))
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(html_text, encoding="utf-8")
    latest_file = Path(getattr(args, "latest_file", "") or (project.root / "agents" / "general" / "reports" / "latest_workflow_dashboard.html"))
    latest_file.parent.mkdir(parents=True, exist_ok=True)
    latest_file.write_text(html_text, encoding="utf-8")
    output_json = getattr(args, "output_json", None)
    if output_json:
        safe_report = dict(report)
        safe_rows = []
        for row in rows:
            safe = dict(row)
            for key in ("started_dt", "ended_dt", "last_activity_dt"):
                value = safe.get(key)
                safe[key] = value.isoformat() if isinstance(value, dt.datetime) else ""
            safe_rows.append(safe)
        safe_report["rows"] = safe_rows
        Path(output_json).write_text(json.dumps(safe_report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Workflow dashboard written to: {output_file}")
    print(f"Latest workflow dashboard: {latest_file}")
    return 0


def _refresh_latest_workflow_dashboard(project: TailorProject) -> None:
    latest_path = project.root / "agents" / "general" / "reports" / "latest_workflow_dashboard.html"
    args = argparse.Namespace(
        hours=24,
        include_all=False,
        limit=0,
        output_file=str(latest_path),
        latest_file=str(latest_path),
        output_json=None,
    )
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
            cmd_generate_workflow_dashboard(args, project)
    except Exception as ex:
        body = (
            f"Latest workflow dashboard refresh failed.\n"
            f"Latest path: {latest_path}\n"
            f"Error: {ex}\n"
            f"Captured stdout:\n{stdout_buffer.getvalue()}\n"
            f"Captured stderr:\n{stderr_buffer.getvalue()}"
        )
        _append_general_exec_log(project, event_type="warning", title="Workflow dashboard refresh failed", body=body)


def _agent_report_html(report: dict[str, Any]) -> str:
    table_id = "agent-report-table"
    rows_html: list[str] = []
    for row in report.get("rows", []):
        rows_html.append(
            "<tr>"
            f"<td>{_html_cell(row['agent'])}</td>"
            f"<td>{_html_cell(row['client_email'])}</td>"
            f"<td>{_html_cell(row.get('client_folder', ''))}</td>"
            f"<td data-sort-value=\"{row['message_count']}\">{row['message_count']}</td>"
            f"<td data-sort-value=\"{row['message_count_last_24h']}\">{row['message_count_last_24h']}</td>"
            f"<td data-sort-value=\"{row['sent_count']}\">{row['sent_count']}</td>"
            f"<td data-sort-value=\"{row['received_count']}\">{row['received_count']}</td>"
            f"<td data-sort-value=\"{row['sent_count_last_24h']}\">{row['sent_count_last_24h']}</td>"
            f"<td data-sort-value=\"{row['received_count_last_24h']}\">{row['received_count_last_24h']}</td>"
            f"<td data-sort-value=\"{row['draft_count']}\">{row['draft_count']}</td>"
            f"<td data-sort-value=\"{row['reply_marker_count']}\">{row['reply_marker_count']}</td>"
            f"<td>{_html_cell(row['last_message_direction'])}</td>"
            f"<td data-sort-value=\"{_html_cell(row['last_message_at'])}\">{_html_cell(row['last_message_at'])}</td>"
            f"<td data-sort-value=\"{_html_cell(row['last_sent_at'])}\">{_html_cell(row['last_sent_at'])}</td>"
            f"<td data-sort-value=\"{_html_cell(row['last_received_at'])}\">{_html_cell(row['last_received_at'])}</td>"
            f"<td>{_html_message_block(row.get('latest_sent'))}</td>"
            f"<td>{_html_message_block(row.get('latest_received'))}</td>"
            "</tr>"
        )
    summary = report["summary"]
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Agent Report</title>
  <style>
    :root {{
      --bg: #f5efe4;
      --panel: #fffaf1;
      --ink: #1f2933;
      --muted: #586574;
      --accent: #a34b2a;
      --line: #d7c8b5;
      --stripe: #f8f0e4;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(163,75,42,0.12), transparent 28%),
        linear-gradient(180deg, #fbf6ee 0%, var(--bg) 100%);
    }}
    .wrap {{ padding: 24px; }}
    .hero {{
      background: linear-gradient(135deg, rgba(163,75,42,0.95), rgba(72,92,115,0.92));
      color: #fffaf3;
      border-radius: 20px;
      padding: 24px 28px;
      box-shadow: 0 18px 45px rgba(31,41,51,0.18);
    }}
    h1 {{ margin: 0 0 8px; font-size: 34px; }}
    .sub {{ color: rgba(255,250,243,0.9); max-width: 900px; }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-top: 18px;
    }}
    .stat {{
      background: rgba(255,255,255,0.12);
      border: 1px solid rgba(255,255,255,0.15);
      border-radius: 14px;
      padding: 14px;
    }}
    .stat .label {{ font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; opacity: 0.8; }}
    .stat .value {{ font-size: 28px; margin-top: 6px; }}
    .panel {{
      margin-top: 22px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 16px 35px rgba(31,41,51,0.08);
      overflow: hidden;
    }}
    .table-wrap {{ overflow: auto; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1800px; }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 12px 10px;
      vertical-align: top;
      text-align: left;
      font-size: 14px;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #f3e6d6;
      z-index: 1;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--muted);
    }}
    tbody tr:nth-child(even) {{ background: var(--stripe); }}
    .msg .subject {{ font-weight: 700; margin-bottom: 4px; }}
    .msg .meta, .msg .path {{ color: var(--muted); font-size: 12px; }}
    .msg .preview {{ margin-top: 6px; line-height: 1.4; white-space: pre-wrap; }}
    .empty {{ color: var(--muted); font-style: italic; }}
    {_html_table_interaction_assets()}
  </style>
</head>
<body data-auto-refresh-seconds="5">
  <div class="wrap">
    <section class="hero">
      <h1>Agent / Client Activity Report</h1>
      <div class="sub">This report rolls up message history from each agent's local history folders so you can quickly see volume, recency, and the latest sent and received content for every client conversation.</div>
      <div class="stats">
        <div class="stat"><div class="label">Generated</div><div class="value">{_html_cell(report['generated_local'])}</div></div>
        <div class="stat"><div class="label">Agent / Client Rows</div><div class="value">{summary['row_count']}</div></div>
        <div class="stat"><div class="label">Total Messages</div><div class="value">{summary['message_count']}</div></div>
        <div class="stat"><div class="label">Last 24 Hours</div><div class="value">{summary['message_count_last_24h']}</div></div>
        <div class="stat"><div class="label">Sent</div><div class="value">{summary['sent_count']}</div></div>
        <div class="stat"><div class="label">Received</div><div class="value">{summary['received_count']}</div></div>
      </div>
    </section>
    <section class="panel">
      {_html_table_controls(table_id, "Search all agent/client activity fields...")}
      <div class="table-wrap">
        <table id="{table_id}" data-enhanced-table="true">
          <thead>
            <tr>
              <th>Agent</th>
              <th>Client Email</th>
              <th>History Folder</th>
              <th>Total Msgs</th>
              <th>Msgs 24h</th>
              <th>Sent</th>
              <th>Received</th>
              <th>Sent 24h</th>
              <th>Received 24h</th>
              <th>Drafts</th>
              <th>Reply Markers</th>
              <th>Last Direction</th>
              <th>Last Message</th>
              <th>Last Sent</th>
              <th>Last Received</th>
              <th>Latest Sent</th>
              <th>Latest Received</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>
      </div>
    </section>
  </div>
  {_html_table_interaction_script()}
</body>
</html>
"""


def _json_safe_agent_row(row: dict[str, Any]) -> dict[str, Any]:
    safe = dict(row)
    for key in ("latest_sent", "latest_received"):
        item = safe.get(key)
        if isinstance(item, dict):
            safe[key] = {
                "path": str(item.get("path", "")),
                "direction": item.get("direction", ""),
                "modified_utc": item.get("modified_utc", ""),
                "subject": item.get("subject", ""),
                "from": item.get("from", ""),
                "sender_email": item.get("sender_email", ""),
                "message_id": item.get("message_id", ""),
                "body_preview": item.get("body_preview", ""),
                "body": item.get("body", ""),
            }
    return safe


def cmd_generate_agent_report(args: argparse.Namespace, project: TailorProject) -> int:
    hours = max(1, int(getattr(args, "hours", 24)))
    since_utc = dt.datetime.now(tz=UTC) - dt.timedelta(hours=hours)
    items = _read_agent_history_items(
        project,
        history_root_name=getattr(args, "history_root_name", "history"),
        agent_folder_regex=getattr(args, "agent_folder_regex", None),
        client_folder_regex=getattr(args, "client_folder_regex", None),
        max_history_folders=0,
    )
    rows: list[dict[str, Any]] = []
    totals = {
        "row_count": 0,
        "message_count": 0,
        "message_count_last_24h": 0,
        "sent_count": 0,
        "received_count": 0,
    }
    for item in items:
        folder = Path(item["client_history_folder"])
        files = [path for path in folder.iterdir() if path.is_file()]
        parsed = [_parse_history_message(path) for path in files]
        messages = [entry for entry in parsed if entry["direction"] in {"sent", "received"}]
        if not messages and not getattr(args, "include_empty", False):
            continue
        sent_messages = [entry for entry in messages if entry["direction"] == "sent"]
        received_messages = [entry for entry in messages if entry["direction"] == "received"]
        sent_recent = [entry for entry in sent_messages if entry["modified_dt"] >= since_utc]
        received_recent = [entry for entry in received_messages if entry["modified_dt"] >= since_utc]
        latest_message = max(messages, key=lambda entry: entry["modified_dt"], default=None)
        latest_sent = max(sent_messages, key=lambda entry: entry["modified_dt"], default=None)
        latest_received = max(received_messages, key=lambda entry: entry["modified_dt"], default=None)
        row = {
            "agent": item["agent_folder_name"],
            "client_email": item["client_email"] or item["client_identifier"],
            "client_folder": str(folder.relative_to(project.root)) if folder.is_relative_to(project.root) else str(folder),
            "message_count": len(messages),
            "message_count_last_24h": len(sent_recent) + len(received_recent),
            "sent_count": len(sent_messages),
            "received_count": len(received_messages),
            "sent_count_last_24h": len(sent_recent),
            "received_count_last_24h": len(received_recent),
            "draft_count": sum(1 for entry in parsed if entry["direction"] == "draft"),
            "reply_marker_count": sum(1 for entry in parsed if entry["direction"] == "reply-marker"),
            "last_message_direction": latest_message["direction"] if latest_message else "",
            "last_message_at": _format_report_dt(latest_message["modified_dt"]) if latest_message else "",
            "last_sent_at": _format_report_dt(latest_sent["modified_dt"]) if latest_sent else "",
            "last_received_at": _format_report_dt(latest_received["modified_dt"]) if latest_received else "",
            "latest_sent": latest_sent,
            "latest_received": latest_received,
        }
        rows.append(row)
        totals["row_count"] += 1
        totals["message_count"] += row["message_count"]
        totals["message_count_last_24h"] += row["message_count_last_24h"]
        totals["sent_count"] += row["sent_count"]
        totals["received_count"] += row["received_count"]
    rows.sort(key=lambda entry: entry["last_message_at"], reverse=True)
    generated = dt.datetime.now().astimezone()
    report = {
        "generated_utc": now_iso(),
        "generated_local": generated.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "summary": totals,
        "rows": rows,
    }
    html_text = _agent_report_html(report)
    output_file = Path(getattr(args, "output_file", "") or (project.root / "agents" / "general" / "reports" / f"agent_report_{generated.strftime('%Y%m%d_%H%M%S')}.html"))
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(html_text, encoding="utf-8")
    latest_file = Path(getattr(args, "latest_file", "") or (project.root / "agents" / "general" / "reports" / "latest_agent_report.html"))
    latest_file.parent.mkdir(parents=True, exist_ok=True)
    latest_file.write_text(html_text, encoding="utf-8")
    output_json = getattr(args, "output_json", None)
    if output_json:
        safe_report = dict(report)
        safe_report["rows"] = [_json_safe_agent_row(row) for row in rows]
        Path(output_json).write_text(json.dumps(safe_report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Agent report written to: {output_file}")
    print(f"Latest agent report: {latest_file}")
    return 0


def cmd_show_notifications(args: argparse.Namespace, project: TailorProject) -> int:
    notifications = _load_user_notifications(project)
    path = project.user_notifications_path
    if not notifications:
        print(f"No user notifications in {path}")
        return 0

    print(f"User notifications: {path}")
    print(f"Count: {len(notifications)}")
    for index, notification in enumerate(notifications, start=1):
        print("")
        print(f"{index}. {_format_notification_line(notification)}")
        print(str(notification.get("message") or "").strip())
        details = notification.get("details")
        if isinstance(details, dict) and details:
            for key in sorted(details.keys()):
                value = details.get(key)
                if isinstance(value, (dict, list)):
                    value_text = json.dumps(value, ensure_ascii=False)
                else:
                    value_text = str(value)
                print(f"   {key}: {value_text}")

    if sys.stdin.isatty():
        try:
            answer = input("\nClear these notifications now? [y/N]: ").strip().lower()
        except EOFError:
            answer = ""
        if answer in {"y", "yes"}:
            cleared = _clear_user_notifications(project)
            print(f"Cleared {cleared} notification(s).")
            return 0
    print("Run `python scripts/tailor_ops.py clear-notifications` (from the repo root) to clear them.")
    return 0


def cmd_clear_notifications(args: argparse.Namespace, project: TailorProject) -> int:
    cleared = _clear_user_notifications(project)
    print(f"Cleared {cleared} notification(s) from {project.user_notifications_path}")
    return 0


def _kill_workflow_process_ids(pids: list[int]) -> tuple[list[int], list[str]]:
    killed: list[int] = []
    errors: list[str] = []
    for pid in pids:
        try:
            if os.name == "nt":
                result = subprocess.run(
                    ["taskkill", "/PID", str(int(pid)), "/T", "/F"],
                    capture_output=True,
                    text=True,
                    timeout=20,
                    check=False,
                )
                if result.returncode == 0:
                    killed.append(int(pid))
                else:
                    message = (result.stderr or result.stdout or "").strip() or f"taskkill failed for PID {pid}"
                    errors.append(f"PID {pid}: {message}")
            else:
                os.kill(int(pid), 15)
                killed.append(int(pid))
        except Exception as ex:
            errors.append(f"PID {pid}: {ex}")
    return killed, errors


def cmd_kill_workflow_run(args: argparse.Namespace, project: TailorProject) -> int:
    requested_pid = int(getattr(args, "pid", 0) or 0)
    live_processes = _list_live_workflow_processes(project)
    target_processes: list[dict[str, Any]] = []

    if requested_pid > 0:
        target_processes = [item for item in live_processes if int(item.get("pid") or 0) == requested_pid]
        if not target_processes:
            raise RuntimeError(f"No live workflow process found for PID {requested_pid}.")
    elif getattr(args, "log_file", ""):
        log_path = _resolve_report_path(project, str(getattr(args, "log_file", "") or ""))
        if log_path is None or not log_path.exists():
            raise RuntimeError("log_file was not found.")
        row = _build_workflow_run_row_from_exec_log(project, log_path)
        if row is None:
            raise RuntimeError("Could not resolve a workflow run from the specified log file.")
        live_match = _match_live_workflow_process(project, row, live_processes)
        if live_match is None:
            raise RuntimeError("The workflow from that log file does not appear to be running now.")
        target_processes = [live_match]
    elif getattr(args, "workflow_file", ""):
        workflow_path = _resolve_report_path(project, str(getattr(args, "workflow_file", "") or ""))
        if workflow_path is None:
            raise RuntimeError("workflow_file was not found.")
        target_processes = [item for item in live_processes if str(item.get("workflow_path") or "") == str(workflow_path)]
        if not target_processes:
            raise RuntimeError("No live workflow process matched the specified workflow file.")
        if len(target_processes) > 1 and not _coerce_bool(getattr(args, "all", False), label="all"):
            pids_text = ", ".join(str(int(item.get("pid") or 0)) for item in target_processes)
            raise RuntimeError(f"Multiple live processes match that workflow file. Re-run with --all or choose --pid. Matching PIDs: {pids_text}")
    else:
        raise RuntimeError("kill-workflow-run requires --pid, --log-file, or --workflow-file.")

    pids = sorted({int(item.get("pid") or 0) for item in target_processes if int(item.get("pid") or 0) > 0})
    if not pids:
        raise RuntimeError("No valid workflow PIDs were found to kill.")

    if _coerce_bool(getattr(args, "dry_run", False), label="dry_run"):
        for pid in pids:
            print(f"Would kill workflow PID {pid}")
        return 0

    killed, errors = _kill_workflow_process_ids(pids)
    if killed:
        body = "\n".join([f"Killed workflow PID {pid}" for pid in killed])
        _append_general_exec_log(project, event_type="warning", title="Workflow process terminated", body=body)
        for pid in killed:
            print(f"Killed workflow PID {pid}")
    if errors:
        raise RuntimeError("; ".join(errors))
    return 0


# ----------------------------
# Agent instruction auto-training loop
# ----------------------------


def _instruction_training_defaults(agent_key: str) -> dict[str, Any]:
    return {
        "agent_key": agent_key,
        "test_data_folder": "agents/agent-test-data",
        "test_message_limit": 50,
        "trainable_persona_files": ["special_instructions.txt"],
        "max_iterations": 3,
        "stop_if_composite_gte": None,
        "prompt_self_improve_enabled": True,
        "trainable_prompt_files": [
            "prompts/agent_instruction_training_batch_user.txt",
            "prompts/agent_instruction_training_scorer_system.txt",
            "prompts/agent_instruction_training_rewriter_system.txt",
            "prompts/agent_instruction_training_meta_improver_system.txt",
        ],
        "meta_improver_system_prompt_file": "prompts/agent_instruction_training_meta_improver_system.txt",
        "meta_history_run_count": 5,
        "meta_history_max_chars_per_artifact": 10000,
        "metrics": [
            {
                "id": "alignment",
                "description": "Replies match persona and goals; not generic or off-brand.",
                "weight": 1.0,
            },
            {
                "id": "clarity",
                "description": "Readable, appropriate length, well-structured plain text.",
                "weight": 1.0,
            },
            {
                "id": "usefulness",
                "description": "Concrete, helpful substance and sensible next steps when relevant.",
                "weight": 1.0,
            },
        ],
        "models": {"batch": None, "scorer": None, "rewriter": None, "meta_improver": None},
        "max_tokens": {"batch": 16000, "scorer": 4096, "rewriter": 8192, "meta_improver": 12000},
        "temperature": {"batch": 0.2, "scorer": 0.1, "rewriter": 0.35, "meta_improver": 0.25},
        "scorer_system_prompt_file": "prompts/agent_instruction_training_scorer_system.txt",
        "rewriter_system_prompt_file": "prompts/agent_instruction_training_rewriter_system.txt",
        "batch_user_prompt_file": "prompts/agent_instruction_training_batch_user.txt",
        "batch_timeout_seconds": 1200,
    }


def _instruction_training_load_config(project: TailorProject, agent_key: str, config_path: Path | None) -> dict[str, Any]:
    merged = _instruction_training_defaults(agent_key)
    if config_path and config_path.is_file():
        raw = json.loads(config_path.read_text(encoding="utf-8", errors="replace"))
        if not isinstance(raw, dict):
            raise RuntimeError("Instruction-training config must be a JSON object.")
        merged = _deep_merge_dicts(merged, raw)
    merged["agent_key"] = agent_key
    return merged


def _instruction_training_collect_fixtures(test_root: Path, limit: int, seed: int | None) -> list[Path]:
    if not test_root.is_dir():
        raise RuntimeError(f"Test data folder not found: {test_root}")
    files = sorted(p for p in test_root.rglob("*.json") if p.is_file())
    if not files:
        raise RuntimeError(f"No .json fixtures under {test_root}")
    take = list(files)
    if seed is not None:
        rnd = random.Random(int(seed))
        rnd.shuffle(take)
    return take[: int(limit)]


def _instruction_training_persona_checksum(persona_dir: Path, basenames: list[str]) -> str:
    h = hashlib.sha256()
    for name in sorted(basenames):
        path = persona_dir / name
        h.update(name.encode("utf-8"))
        h.update(b"\0")
        if path.is_file():
            h.update(path.read_bytes())
        h.update(b"\0")
    return h.hexdigest()


@contextlib.contextmanager
def _instruction_training_mirror_fixtures(test_root: Path, selected: list[Path]) -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="tailor-instruction-train-") as tmp:
        base = Path(tmp) / "fixtures"
        base.mkdir(parents=True)
        for src in selected:
            rel = src.relative_to(test_root)
            dst = base / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        yield base


def _instruction_training_build_system(
    project: TailorProject, agent_root: Path, history_mirror: Path, ctx: dict[str, Any]
) -> str:
    step: dict[str, Any] = {
        "agent_root": str(agent_root),
        "client_history_folder": str(history_mirror),
        "history_recursive": True,
        "history_regex": r".+\.json$",
        "history_sort_by": "relative_path",
        "history_sort_direction": "asc",
    }
    text, _a, _c = _build_agent_system_prompt_text(project, step, ctx, base_dir=project.root)
    return text


def _instruction_training_ai_client_for_model(project: TailorProject, args: argparse.Namespace, model: str) -> AiClient:
    a = copy.copy(args)
    a.model = model
    return _current_ai_client(a, project)


def _instruction_training_load_leaderboard(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"entries": [], "best_composite": None, "best_run": None}
    try:
        obj = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        return obj if isinstance(obj, dict) else {"entries": [], "best_composite": None, "best_run": None}
    except Exception:
        return {"entries": [], "best_composite": None, "best_run": None}


def _instruction_training_save_leaderboard(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _instruction_training_apply_persona_files(
    persona_dir: Path,
    instruction_files: list[dict[str, Any]],
    allowed_basenames: set[str],
) -> None:
    persona_res = persona_dir.resolve()
    seen: set[str] = set()
    for index, item in enumerate(instruction_files, start=1):
        if not isinstance(item, dict):
            raise RuntimeError(f"instruction_files[{index}] must be an object.")
        rel = str(item.get("relative_path") or "").strip().replace("\\", "/")
        if not rel or "/" in rel:
            raise RuntimeError(f"relative_path must be a single filename under persona: {rel!r}")
        if rel not in allowed_basenames:
            raise RuntimeError(f"Refusing to write disallowed persona file: {rel!r}")
        if rel in seen:
            raise RuntimeError(f"Duplicate relative_path in model output: {rel!r}")
        seen.add(rel)
        content = item.get("content")
        if content is None:
            raise RuntimeError(f"Missing content for {rel!r}")
        text = content if isinstance(content, str) else str(content)
        target = (persona_dir / rel).resolve()
        if persona_res not in target.parents and target != persona_res:
            raise RuntimeError(f"Path escapes persona folder: {target}")
        target.write_text(text, encoding="utf-8", newline="\n")
    missing = allowed_basenames - seen
    if missing:
        raise RuntimeError(f"Model did not return instruction_files for: {sorted(missing)}")


def _instruction_training_allowed_repo_paths(project: TailorProject, rel_entries: list[str]) -> set[str]:
    root = project.root.resolve()
    out: set[str] = set()
    for raw in rel_entries:
        s = str(raw).strip().replace("\\", "/")
        if not s or ".." in Path(s).parts:
            raise RuntimeError(f"Invalid repo-relative path: {raw!r}")
        tgt = (root / s).resolve()
        try:
            rel = tgt.relative_to(root).as_posix()
        except ValueError as ex:
            raise RuntimeError(f"trainable path escapes project root: {raw!r}") from ex
        out.add(rel)
    return out


def _instruction_training_backup_repo_paths(project: TailorProject, rels: list[str], dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    root = project.root.resolve()
    for rel in rels:
        src = (root / rel).resolve()
        if not src.is_file():
            continue
        dst = dest_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def _instruction_training_apply_prompt_files(
    project: TailorProject,
    prompt_files: list[dict[str, Any]],
    allowed_rels: set[str],
) -> None:
    root = project.root.resolve()
    seen: set[str] = set()
    for index, item in enumerate(prompt_files, start=1):
        if not isinstance(item, dict):
            raise RuntimeError(f"prompt_files[{index}] must be an object.")
        rel = str(item.get("relative_path") or "").strip().replace("\\", "/")
        if not rel:
            raise RuntimeError(f"prompt_files[{index}] missing relative_path")
        if ".." in Path(rel).parts:
            raise RuntimeError(f"Invalid relative_path in prompt_files[{index}]: {rel!r}")
        target = (root / rel).resolve()
        try:
            key = target.relative_to(root).as_posix()
        except ValueError as ex:
            raise RuntimeError(f"Path escapes project root: {rel!r}") from ex
        if key not in allowed_rels:
            raise RuntimeError(f"Refusing to write disallowed prompt file: {key!r}")
        if key in seen:
            raise RuntimeError(f"Duplicate prompt relative_path: {key!r}")
        seen.add(key)
        content = item.get("content")
        if content is None:
            raise RuntimeError(f"Missing content for {key!r}")
        text = content if isinstance(content, str) else str(content)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8", newline="\n")
    missing = allowed_rels - seen
    if missing:
        raise RuntimeError(f"Model did not return prompt_files for: {sorted(missing)}")


def _instruction_training_prior_runs_payload(
    runs_dir: Path,
    *,
    exclude_run_id: str,
    max_runs: int,
    max_chars: int,
) -> list[dict[str, Any]]:
    if not runs_dir.is_dir() or max_runs <= 0:
        return []
    candidates = sorted(
        [p for p in runs_dir.iterdir() if p.is_dir() and p.name != exclude_run_id],
        key=lambda p: p.name,
        reverse=True,
    )[:max_runs]

    def clip_file(path: Path) -> str:
        if not path.is_file():
            return ""
        text = path.read_text(encoding="utf-8", errors="replace")
        if len(text) <= max_chars:
            return text
        return text[: max_chars - 80] + "\n\n[TRUNCATED]\n"

    def load_json(path: Path) -> Any:
        if not path.is_file():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            return None

    out: list[dict[str, Any]] = []
    for run_path in candidates:
        it_dirs = sorted(run_path.glob("iter-*"))
        if not it_dirs:
            continue
        last = it_dirs[-1]
        out.append(
            {
                "run_id": run_path.name,
                "last_iter": last.name,
                "score_report": load_json(last / "score_report.json"),
                "rewrite_proposal": load_json(last / "rewrite_proposal.json"),
                "meta_prompt_proposal": load_json(last / "meta_prompt_proposal.json"),
                "batch_replies_excerpt": clip_file(last / "batch_replies.json"),
            }
        )
    return out


def cmd_agent_instruction_refine(args: argparse.Namespace, project: TailorProject) -> int:
    agent_key = str(getattr(args, "instruction_agent", "") or "").strip()
    if not agent_key:
        raise RuntimeError("--agent is required.")
    agent_root = (project.root / "agents" / agent_key).resolve()
    if not agent_root.is_dir():
        raise RuntimeError(f"Agent folder not found: {agent_root}")

    cfg_path_raw = getattr(args, "instruction_config", None) or ""
    cfg_path = Path(cfg_path_raw).resolve() if str(cfg_path_raw).strip() else None
    if cfg_path and not cfg_path.is_file():
        raise RuntimeError(f"--config file not found: {cfg_path}")

    cfg = _instruction_training_load_config(project, agent_key, cfg_path)
    test_rel = str(cfg.get("test_data_folder") or "agents/agent-test-data")
    test_root = (project.root / test_rel).resolve()
    limit = int(getattr(args, "instruction_max_messages", None) or cfg.get("test_message_limit") or 50)
    iterations = int(getattr(args, "instruction_iterations", None) or cfg.get("max_iterations") or 3)
    if iterations < 1:
        raise RuntimeError("iterations / max_iterations must be at least 1.")
    seed = getattr(args, "instruction_seed", None)
    seed_i = int(seed) if seed is not None else None
    dry_run = bool(getattr(args, "instruction_dry_run", False))
    stop_at = cfg.get("stop_if_composite_gte")
    stop_f: float | None = float(stop_at) if stop_at is not None and str(stop_at).strip() != "" else None

    trainable = cfg.get("trainable_persona_files") or []
    if not isinstance(trainable, list) or not all(isinstance(x, str) and x.strip() for x in trainable):
        raise RuntimeError("trainable_persona_files must be a non-empty list of filenames.")
    allowed = {str(x).strip().replace("\\", "/").split("/")[-1] for x in trainable}
    persona_dir = (agent_root / "persona").resolve()
    if not persona_dir.is_dir():
        raise RuntimeError(f"Persona folder not found: {persona_dir}")

    metrics = cfg.get("metrics") or []
    if not isinstance(metrics, list) or not metrics:
        raise RuntimeError("config.metrics must be a non-empty list.")

    models = cfg.get("models") or {}
    def_m = str(getattr(args, "model", None) or _active_ai_profile(project).get("model") or "gpt-4o-mini")
    m_batch = str(models.get("batch") or def_m)
    m_scorer = str(models.get("scorer") or def_m)
    m_rewriter = str(models.get("rewriter") or def_m)
    m_meta = str(models.get("meta_improver") or models.get("rewriter") or def_m)

    mtoks = cfg.get("max_tokens") or {}
    tt = cfg.get("temperature") or {}
    mt_batch = int(mtoks.get("batch") or 16000)
    mt_score = int(mtoks.get("scorer") or 4096)
    mt_rewrite = int(mtoks.get("rewriter") or 8192)
    mt_meta = int(mtoks.get("meta_improver") or 12000)
    t_batch = float(tt.get("batch", 0.2))
    t_score = float(tt.get("scorer", 0.1))
    t_rewrite = float(tt.get("rewriter", 0.35))
    t_meta = float(tt.get("meta_improver", 0.25))
    batch_timeout = int(cfg.get("batch_timeout_seconds") or 1200)

    meta_enabled = _coerce_bool(cfg.get("prompt_self_improve_enabled", True), label="prompt_self_improve_enabled")
    trainable_prompt_raw = cfg.get("trainable_prompt_files")
    allowed_prompt_rels: set[str] = set()
    if meta_enabled and isinstance(trainable_prompt_raw, list) and trainable_prompt_raw:
        allowed_prompt_rels = _instruction_training_allowed_repo_paths(project, [str(x) for x in trainable_prompt_raw])
    elif meta_enabled and not (isinstance(trainable_prompt_raw, list) and trainable_prompt_raw):
        meta_enabled = False

    fixtures = _instruction_training_collect_fixtures(test_root, limit, seed_i)
    print(f"[instruction-training] agent={agent_key} fixtures={len(fixtures)} iterations={iterations} dry_run={dry_run}")

    train_root = agent_root / "instruction-training"
    runs_dir = train_root / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    board_path = train_root / "leaderboard.json"
    leaderboard = _instruction_training_load_leaderboard(board_path)

    scorer_prompt_path = project.root / str(cfg.get("scorer_system_prompt_file") or "")
    rewriter_prompt_path = project.root / str(cfg.get("rewriter_system_prompt_file") or "")
    batch_user_path = project.root / str(cfg.get("batch_user_prompt_file") or "")
    meta_system_path = project.root / str(cfg.get("meta_improver_system_prompt_file") or "")
    for label, p in [("scorer_system", scorer_prompt_path), ("rewriter_system", rewriter_prompt_path), ("batch_user", batch_user_path)]:
        if not p.is_file():
            raise RuntimeError(f"Missing prompt file ({label}): {p}")
    if meta_enabled:
        if not meta_system_path.is_file():
            raise RuntimeError(f"Meta-improver enabled but system prompt missing: {meta_system_path}")

    meta_history_n = int(cfg.get("meta_history_run_count") or 5)
    meta_history_chars = int(cfg.get("meta_history_max_chars_per_artifact") or 10000)

    meta_fixtures = [{"path": f.relative_to(test_root).as_posix(), "id": str(f)} for f in fixtures]
    (run_dir / "fixtures.json").write_text(json.dumps(meta_fixtures, indent=2, ensure_ascii=False), encoding="utf-8")
    (run_dir / "config.resolved.json").write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")

    ctx_base: dict[str, Any] = {"agent_root": str(agent_root), "agent_key": agent_key}

    for it in range(1, iterations + 1):
        it_dir = run_dir / f"iter-{it:02d}"
        it_dir.mkdir()
        print(f"[instruction-training] --- iteration {it}/{iterations} ---")

        scorer_system = scorer_prompt_path.read_text(encoding="utf-8", errors="replace")
        rewriter_system = rewriter_prompt_path.read_text(encoding="utf-8", errors="replace")
        batch_user_tmpl = batch_user_path.read_text(encoding="utf-8", errors="replace")
        batch_user_text = batch_user_tmpl.replace("{{fixture_count}}", str(len(fixtures)))
        meta_system = meta_system_path.read_text(encoding="utf-8", errors="replace") if meta_enabled else ""

        checksum_before = _instruction_training_persona_checksum(persona_dir, sorted(allowed))

        with _instruction_training_mirror_fixtures(test_root, fixtures) as mirror:
            system_prompt = _instruction_training_build_system(project, agent_root, mirror, ctx_base)

        (it_dir / "system_prompt_snapshot.txt").write_text(system_prompt, encoding="utf-8", errors="replace")

        client_b = _instruction_training_ai_client_for_model(project, args, m_batch)
        trace(1, f"[instruction-training] batch model={m_batch}")
        batch_raw = client_b.chat(
            system=system_prompt,
            user=batch_user_text,
            temperature=t_batch,
            max_tokens=mt_batch,
            timeout_seconds=batch_timeout,
        )
        batch_obj = _parse_json_from_model_text(batch_raw, label="batch replies")
        (it_dir / "batch_replies.json").write_text(json.dumps(batch_obj, indent=2, ensure_ascii=False), encoding="utf-8")

        responses = batch_obj.get("responses") if isinstance(batch_obj, dict) else None
        if not isinstance(responses, list) or len(responses) != len(fixtures):
            raise RuntimeError(
                f"Batch replies: expected {len(fixtures)} responses, got {len(responses) if isinstance(responses, list) else '?'}"
            )

        scorer_user = json.dumps(
            {
                "metrics": metrics,
                "batch_results": batch_obj,
                "fixture_paths": [f.relative_to(test_root).as_posix() for f in fixtures],
            },
            ensure_ascii=False,
            indent=2,
        )
        client_s = _instruction_training_ai_client_for_model(project, args, m_scorer)
        trace(1, f"[instruction-training] scorer model={m_scorer}")
        score_raw = client_s.chat(
            system=scorer_system,
            user=scorer_user,
            temperature=t_score,
            max_tokens=mt_score,
            timeout_seconds=batch_timeout,
        )
        score_obj = _parse_json_from_model_text(score_raw, label="scorer report")
        (it_dir / "score_report.json").write_text(json.dumps(score_obj, indent=2, ensure_ascii=False), encoding="utf-8")

        composite = score_obj.get("composite") if isinstance(score_obj, dict) else None
        try:
            composite_f = float(composite) if composite is not None else 0.0
        except (TypeError, ValueError):
            composite_f = 0.0
        print(f"[instruction-training] composite score: {composite_f}")

        current_map: dict[str, str] = {}
        for name in sorted(allowed):
            p = persona_dir / name
            current_map[name] = p.read_text(encoding="utf-8", errors="replace") if p.is_file() else ""

        rewriter_user = json.dumps(
            {
                "metrics": metrics,
                "score_report": score_obj,
                "trainable_files": sorted(allowed),
                "current_instructions": current_map,
            },
            ensure_ascii=False,
            indent=2,
        )
        client_r = _instruction_training_ai_client_for_model(project, args, m_rewriter)
        trace(1, f"[instruction-training] rewriter model={m_rewriter}")
        rewrite_raw = client_r.chat(
            system=rewriter_system,
            user=rewriter_user,
            temperature=t_rewrite,
            max_tokens=mt_rewrite,
            timeout_seconds=batch_timeout,
        )
        rewrite_obj = _parse_json_from_model_text(rewrite_raw, label="rewriter proposal")
        (it_dir / "rewrite_proposal.json").write_text(json.dumps(rewrite_obj, indent=2, ensure_ascii=False), encoding="utf-8")

        instruction_files = rewrite_obj.get("instruction_files") if isinstance(rewrite_obj, dict) else None
        if not isinstance(instruction_files, list):
            raise RuntimeError("rewriter JSON missing instruction_files array")

        backup_dir = it_dir / "persona-backup"
        if not dry_run:
            shutil.copytree(persona_dir, backup_dir, dirs_exist_ok=False)
            _instruction_training_apply_persona_files(persona_dir, instruction_files, allowed)
        else:
            (it_dir / "persona-would-apply.json").write_text(
                json.dumps(instruction_files, indent=2, ensure_ascii=False), encoding="utf-8"
            )

        checksum_after = _instruction_training_persona_checksum(persona_dir, sorted(allowed)) if not dry_run else checksum_before

        persona_snapshot: dict[str, str] = {}
        for name in sorted(allowed):
            p = persona_dir / name
            persona_snapshot[name] = p.read_text(encoding="utf-8", errors="replace") if p.is_file() else ""

        meta_obj: dict[str, Any] | None = None
        if meta_enabled and allowed_prompt_rels:
            prior = _instruction_training_prior_runs_payload(
                runs_dir,
                exclude_run_id=run_id,
                max_runs=meta_history_n,
                max_chars=meta_history_chars,
            )
            trainable_prompt_list = sorted(allowed_prompt_rels)
            current_prompts_body = {
                "batch_user_template": batch_user_tmpl,
                "scorer_system": scorer_system,
                "rewriter_system": rewriter_system,
                "meta_improver_system": meta_system,
            }
            meta_user = json.dumps(
                {
                    "agent_key": agent_key,
                    "iteration_in_run": it,
                    "composite_this_iteration": composite_f,
                    "metrics": metrics,
                    "trainable_prompt_relative_paths": trainable_prompt_list,
                    "latest_score_report": score_obj,
                    "latest_rewrite_proposal": rewrite_obj,
                    "persona_after_iteration": persona_snapshot,
                    "current_training_prompts": current_prompts_body,
                    "prior_runs": prior,
                },
                ensure_ascii=False,
                indent=2,
            )
            client_meta = _instruction_training_ai_client_for_model(project, args, m_meta)
            trace(1, f"[instruction-training] meta-improver model={m_meta}")
            meta_raw = client_meta.chat(
                system=meta_system,
                user=meta_user,
                temperature=t_meta,
                max_tokens=mt_meta,
                timeout_seconds=batch_timeout,
            )
            meta_obj = _parse_json_from_model_text(meta_raw, label="meta prompt improver")
            if not isinstance(meta_obj, dict):
                raise RuntimeError("meta improver must return a JSON object.")
            (it_dir / "meta_prompt_proposal.json").write_text(json.dumps(meta_obj, indent=2, ensure_ascii=False), encoding="utf-8")
            prompt_files = meta_obj.get("prompt_files")
            if not isinstance(prompt_files, list):
                raise RuntimeError("meta improver JSON missing prompt_files array")
            backup_prompts = it_dir / "trainable-prompts-backup"
            if not dry_run:
                _instruction_training_backup_repo_paths(project, trainable_prompt_list, backup_prompts)
                _instruction_training_apply_prompt_files(project, prompt_files, allowed_prompt_rels)
            else:
                (it_dir / "prompts-would-apply.json").write_text(
                    json.dumps(prompt_files, indent=2, ensure_ascii=False), encoding="utf-8"
                )

        entry = {
            "run_id": run_id,
            "iteration": it,
            "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "composite": composite_f,
            "checksum_persona_before": checksum_before,
            "checksum_persona_after": checksum_after,
            "dry_run": dry_run,
            "models": {
                "batch": m_batch,
                "scorer": m_scorer,
                "rewriter": m_rewriter,
                **({"meta_improver": m_meta} if meta_enabled and allowed_prompt_rels else {}),
            },
            "summary": (rewrite_obj.get("summary_of_changes") if isinstance(rewrite_obj, dict) else "") or "",
            "meta_prompt_summary": (meta_obj.get("change_summary") if isinstance(meta_obj, dict) else "") or "",
        }
        leaderboard.setdefault("entries", []).append(entry)
        best = leaderboard.get("best_composite")
        if best is None or composite_f > float(best):
            leaderboard["best_composite"] = composite_f
            leaderboard["best_run"] = run_id
            leaderboard["best_iteration"] = it
        _instruction_training_save_leaderboard(board_path, leaderboard)

        if stop_f is not None and composite_f >= stop_f:
            print(f"[instruction-training] stopping: composite {composite_f} >= configured threshold {stop_f}")
            break

    print(f"[instruction-training] done. Artifacts under {run_dir}")
    print(f"[instruction-training] leaderboard: {board_path}")
    return 0


# ----------------------------
# CLI wiring
# ----------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TaiLOR Python automation CLI (project = current working directory)")
    p.add_argument("--diagnostics", action="store_true", help="Run recent-log diagnostics and exit.")
    p.add_argument("--diagnostics-hours", type=int, default=24, help="How far back to scan for diagnostics.")
    p.add_argument("--diagnostics-max-files", type=int, default=20, help="Maximum number of recent log/output files to inspect.")
    p.add_argument("--diagnostics-max-chars-per-file", type=int, default=6000, help="Maximum tail characters read from each diagnostic file.")
    _add_trace_args(p)

    sub = p.add_subparsers(dest="cmd", required=False)

    sub.add_parser("project-summary", help="Show prompts/workflows/database overview")

    agent = sub.add_parser("agent-create", help="Create an Agent row in tailor-state.db")
    agent.add_argument("--name", required=True)
    agent.add_argument("--persona-id", required=True)
    agent.add_argument("--prompt-document-id")
    agent.add_argument("--notes")
    agent.add_argument("--special-instructions")
    agent.add_argument("--behaviors-json")
    agent.add_argument("--goals")
    agent.add_argument("--strategies")

    sub.add_parser("agent-list", help="List agents from tailor-state.db")

    ch = sub.add_parser("channel-gmail-add", help="Insert Gmail communication channel")
    ch.add_argument("--address", required=True)
    ch.add_argument("--persona-id")
    ch.add_argument("--credential-key-ref")
    ch.add_argument("--gmail-client-id", required=True)
    ch.add_argument("--gmail-client-secret", required=True)
    ch.add_argument("--gmail-refresh-token", required=True)
    ch.add_argument("--config-json")

    cho = sub.add_parser("channel-gmail-oauth", help="Open Gmail OAuth consent flow and save a refreshed token on a channel")
    target = cho.add_mutually_exclusive_group(required=True)
    target.add_argument("--channel-id")
    target.add_argument("--address")
    cho.add_argument("--scopes", help="Comma or space separated Gmail scopes or aliases: send, readonly, modify, compose, full")
    cho.add_argument("--timeout-seconds", type=int, default=300)
    cho.add_argument("--no-browser", action="store_true", help="Print the OAuth URL without trying to open a browser")
    cho.add_argument("--always-refresh", "--alwaysRefresh", dest="always_refresh", action="store_true",
                     help="Force a new Gmail OAuth consent flow even if a usable refresh token already exists.")

    wfl = sub.add_parser("workflow-list", help="List task-workflows.json tasks")
    wfl.set_defaults(handler=cmd_workflow_list)

    wfr = sub.add_parser("workflow-run", help="Run a workflow-like AI+Gmail email response")
    wfr.add_argument("--workflow", required=True, help="Workflow ID prefix or exact name")
    wfr.add_argument("--channel-id", required=True)
    wfr.add_argument("--from-email", required=True)
    wfr.add_argument("--reply-to", required=True)
    wfr.add_argument("--subject", required=True)
    wfr.add_argument("--body", required=True)
    wfr.add_argument("--model", default="gpt-4o-mini")
    wfr.add_argument("--api-key")
    wfr.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    wfr.add_argument("--temperature", type=float, default=0.2)
    wfr.add_argument("--max-tokens", type=int, default=1200)
    wfr.add_argument("--dry-run", action="store_true")
    _add_repeat_args(wfr)
    _add_trace_args(wfr)

    wfx = sub.add_parser("workflow-exec", help="Execute chained JSON workflow steps (AI + files + bulk email)")
    wfx.add_argument("--workflow-file", required=True, help="Path to workflow JSON definition")
    wfx.add_argument("--model", default="gpt-4o-mini")
    wfx.add_argument("--api-key")
    wfx.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    wfx.add_argument("--temperature", type=float, default=0.2)
    wfx.add_argument("--max-tokens", type=int, default=1200)
    wfx.add_argument(
        "--ai-timeout-seconds",
        type=int,
        default=DEFAULT_AI_CHAT_TIMEOUT,
        dest="ai_timeout_seconds",
        help=f"HTTP read timeout per AI request (default {DEFAULT_AI_CHAT_TIMEOUT}s; override with env TAILOR_AI_CHAT_TIMEOUT)",
    )
    wfx.add_argument("--dry-run", action="store_true", help="Default for send_email_list steps")
    wfx.add_argument("--throttle-seconds", type=float, default=0.3, help="Default delay between outbound emails")
    wfx.add_argument("--verbosity", type=int, default=1, help="Default workflow step verbosity (0-quiet, 1-normal, 2-verbose)")
    wfx.add_argument("--output-context-json")
    wfx.add_argument(
        "--skip-cost-approval",
        action="store_true",
        dest="skip_cost_approval",
        help="Do not show API cost estimate or prompt for confirmation (see appsettings WorkflowCostEstimate)",
    )
    _add_repeat_args(wfx)
    _add_trace_args(wfx)

    pl = sub.add_parser("plugin-list", help="List all available workflow step plugins")
    _add_trace_args(pl)

    pc = sub.add_parser("plugin-create", help="Create a new workflow plugin manifest")
    pc.add_argument("--name", required=True, help="Plugin step type key")
    pc.add_argument("--display-name")
    pc.add_argument("--description")
    pc.add_argument("--category", default="custom")
    pc.add_argument("--handler", default="legacy_step_adapter")
    _add_trace_args(pc)

    wc = sub.add_parser("workflow-compose", help="Compose a new workflow JSON from existing plugins")
    wc.add_argument("--plugins", required=True, help="Comma-separated plugin names in execution order")
    wc.add_argument("--workflow-type", default="single_run")
    wc.add_argument("--output-file", required=True)
    _add_trace_args(wc)

    rwf = sub.add_parser(
        "run-workflow",
        help="Run a JSON workflow for an agent (sets agent_root, agent_key, output_root=agents/<agent>/workflow-results; resolves workflow path by name)",
    )
    rwf.add_argument("--agent", required=True, help="Agent folder name under agents/, e.g. derek-net")
    rwf.add_argument(
        "--workflow",
        required=True,
        help="Workflow .json name or path (searches agents/<agent>/workflows/, agents/general/workflows/, workflows/)",
    )
    rwf.add_argument("--model", default="gpt-4o-mini")
    rwf.add_argument("--api-key")
    rwf.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    rwf.add_argument("--temperature", type=float, default=0.2)
    rwf.add_argument("--max-tokens", type=int, default=1200)
    rwf.add_argument(
        "--ai-timeout-seconds",
        type=int,
        default=DEFAULT_AI_CHAT_TIMEOUT,
        dest="ai_timeout_seconds",
        help=f"HTTP read timeout per AI request (default {DEFAULT_AI_CHAT_TIMEOUT}s; override with env TAILOR_AI_CHAT_TIMEOUT)",
    )
    rwf.add_argument("--dry-run", action="store_true", help="Default for send_email_list steps")
    rwf.add_argument("--throttle-seconds", type=float, default=0.3, help="Default delay between outbound emails")
    rwf.add_argument("--verbosity", type=int, default=1, help="Default workflow step verbosity (0-quiet, 1-normal, 2-verbose)")
    rwf.add_argument("--output-context-json")
    rwf.add_argument(
        "--workflow-file",
        default="",
        help=argparse.SUPPRESS,
    )
    rwf.add_argument(
        "--skip-cost-approval",
        action="store_true",
        dest="skip_cost_approval",
        help="Do not show API cost estimate or prompt for confirmation (see appsettings WorkflowCostEstimate)",
    )
    _add_repeat_args(rwf)
    _add_trace_args(rwf)

    eng = sub.add_parser(
        "engine-log",
        help="Print the central engine log (all agents/workflows) written under logs/engine/",
    )
    eng.add_argument("--tail", type=int, default=80, help="Last N lines per daily log file (0 = entire file)")

    mon = sub.add_parser("gmail-monitor", help="Poll Gmail inbox and auto-reply via AI")
    mon.add_argument("--channel-id", required=True)
    mon.add_argument("--query", default="in:inbox is:unread")
    mon.add_argument("--max-results", type=int, default=5)
    mon.add_argument("--poll-seconds", type=int, default=30)
    mon.add_argument("--model", default="gpt-4o-mini")
    mon.add_argument("--api-key")
    mon.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    mon.add_argument("--temperature", type=float, default=0.2)
    mon.add_argument("--max-tokens", type=int, default=1200)
    mon.add_argument("--dry-run", action="store_true")
    mon.add_argument("--archive-after-send", action="store_true")

    fli = sub.add_parser("fetlife-list-inbox", help="List FetLife inbox conversations using a browser-backed session")
    _add_fetlife_browser_args(fli)
    fli.add_argument("--max-results", type=int, default=0)
    fli.add_argument("--process-order", default="desc", choices=["asc", "desc"])
    fli.add_argument("--output-json")

    flt = sub.add_parser("fetlife-fetch-thread", help="Fetch a FetLife conversation thread")
    flt.add_argument("--conversation-id", required=True)
    _add_fetlife_browser_args(flt)
    flt.add_argument("--output-file")

    fls = sub.add_parser("fetlife-send-message", help="Send a FetLife direct-message reply in an existing conversation")
    fls.add_argument("--conversation-id", required=True)
    fls.add_argument("--body", required=True)
    _add_fetlife_browser_args(fls)
    fls.add_argument("--dry-run", action="store_true")
    fls.add_argument("--output-file")

    flh = sub.add_parser("download-fetlife-message-history", help="Download the current FetLife inbox conversation history into local folders by username")
    _add_fetlife_browser_args(flh)
    flh.add_argument("--output-root", help="Root folder where FetLife history should be saved")
    flh.add_argument("--max-results", type=int, default=0)
    flh.add_argument("--process-order", default="desc", choices=["asc", "desc"])

    dfe = sub.add_parser("dev-folder-each", help="Send each file in folder to AI one by one")
    dfe.add_argument("--folder", required=True)
    dfe.add_argument("--system", default="Analyze this file and return concise insights.")
    dfe.add_argument("--model", default="gpt-4o-mini")
    dfe.add_argument("--api-key")
    dfe.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    dfe.add_argument("--temperature", type=float, default=0.1)
    dfe.add_argument("--max-tokens", type=int, default=900)
    dfe.add_argument("--max-chars-per-file", type=int, default=12000)
    dfe.add_argument("--output-json")

    dfc = sub.add_parser("dev-folder-combined", help="Combine files in folder and send as one request")
    dfc.add_argument("--folder", required=True)
    dfc.add_argument("--system", default="Analyze this codebase snapshot and propose improvements.")
    dfc.add_argument("--model", default="gpt-4o-mini")
    dfc.add_argument("--api-key")
    dfc.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    dfc.add_argument("--temperature", type=float, default=0.2)
    dfc.add_argument("--max-tokens", type=int, default=1400)
    dfc.add_argument("--max-chars-per-file", type=int, default=8000)
    dfc.add_argument("--max-chars-total", type=int, default=120000)
    dfc.add_argument("--output-file")

    dab = sub.add_parser("dev-ai-bounce", help="Send response back into same AI API for N rounds")
    dab.add_argument("--input-text", required=True)
    dab.add_argument("--rounds", type=int, default=3)
    dab.add_argument("--system", default="Refine and improve the content while preserving intent.")
    dab.add_argument("--model", default="gpt-4o-mini")
    dab.add_argument("--api-key")
    dab.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    dab.add_argument("--temperature", type=float, default=0.2)
    dab.add_argument("--max-tokens", type=int, default=1200)
    dab.add_argument("--output-file")

    diag = sub.add_parser("diagnostics", help="Scan recent logs/output files and summarize likely problems")
    diag.add_argument("--hours", type=int, default=24)
    diag.add_argument("--max-files", type=int, default=20)
    diag.add_argument("--max-chars-per-file", type=int, default=6000)
    diag.add_argument("--model", default="gpt-4o-mini")
    diag.add_argument("--api-key")
    diag.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    diag.add_argument("--max-tokens", type=int, default=1400)
    diag.add_argument("--output-file")
    diag.add_argument("--output-json")
    _add_trace_args(diag)

    cl = sub.add_parser("check-logs", help="Deduplicate recent logs, analyze them with AI, and generate an HTML report")
    cl.add_argument("hours", nargs="?", type=int, default=24, help="How many past hours of logs to analyze. Defaults to 24.")
    cl.add_argument("--model", default="gpt-4o-mini")
    cl.add_argument("--api-key")
    cl.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    _add_trace_args(cl)

    cas = sub.add_parser("check-api-status", help="Probe the AI API and report live status plus current AI settings")
    cas.add_argument("--model", default="gpt-4o-mini")
    cas.add_argument("--api-key")
    cas.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    cas.add_argument("--output-json")
    _add_trace_args(cas)

    sap = sub.add_parser("switch-ai-profile", help="Switch the active AI profile and sync the DB provider row")
    sap.add_argument("profile", help="Profile name from AiRuntime.Profiles, such as azure_foundry or legacy_openai")
    _add_trace_args(sap)

    gar = sub.add_parser("generate-agent-report", help="Generate an HTML report of agent/client message activity")
    gar.add_argument("--hours", type=int, default=24, help="Window used for the recent-message counts")
    gar.add_argument("--history-root-name", default="history")
    gar.add_argument("--agent-folder-regex")
    gar.add_argument("--client-folder-regex")
    gar.add_argument("--include-empty", action="store_true", help="Include agent/client folders even if they have no sent or received messages")
    gar.add_argument("--output-file")
    gar.add_argument("--latest-file")
    gar.add_argument("--output-json")
    _add_trace_args(gar)

    gwd = sub.add_parser("generate-workflow-dashboard", help="Generate an HTML dashboard of workflow execution across all agents")
    gwd.add_argument("--hours", type=int, default=24, help="Default lookback window for included runs unless --include-all is used")
    gwd.add_argument("--include-all", action="store_true", help="Include all discovered workflow runs, not just the recent window")
    gwd.add_argument("--limit", type=int, default=0, help="Maximum number of workflow runs to include, newest first")
    gwd.add_argument("--output-file")
    gwd.add_argument("--latest-file")
    gwd.add_argument("--output-json")
    _add_trace_args(gwd)

    kwr = sub.add_parser("kill-workflow-run", help="Terminate a live workflow-exec process by PID, log file, or workflow file")
    kwr_target = kwr.add_mutually_exclusive_group(required=True)
    kwr_target.add_argument("--pid", type=int)
    kwr_target.add_argument("--log-file")
    kwr_target.add_argument("--workflow-file")
    kwr.add_argument("--all", action="store_true", help="Kill all matching live workflow processes when using --workflow-file")
    kwr.add_argument("--dry-run", action="store_true")
    _add_trace_args(kwr)

    sn = sub.add_parser("show-notifications", help="Show stored user notifications and optionally clear them")
    _add_trace_args(sn)

    cn = sub.add_parser("clear-notifications", help="Clear stored user notifications")
    _add_trace_args(cn)

    pcode = sub.add_parser(
        "package-code",
        help="Zip the project tree and emit package.txt (JSONL+base64) plus rebuild-from-package.ps1 to recreate files",
    )
    pcode.add_argument(
        "--output-dir",
        default="artifacts/package-code",
        dest="package_code_output_dir",
        help="Directory under project root for package.zip, package.txt, rebuild-from-package.ps1 (default: artifacts/package-code)",
    )
    pcode.add_argument(
        "--include-heavy",
        action="store_true",
        dest="package_code_include_heavy",
        help="Include exec-logs, task-history, artifacts, logs, conversation-history, and agents/*/history (large; skipped by default)",
    )
    pcode.add_argument(
        "--exclude-glob",
        action="append",
        default=None,
        dest="package_code_exclude_glob",
        metavar="PATTERN",
        help="Extra fnmatch pattern vs relative path or basename (repeatable), e.g. '*.sqlite' or 'clutter/*'",
    )
    pcode.add_argument(
        "--max-file-mb",
        type=float,
        default=64.0,
        dest="package_code_max_file_mb",
        help="Skip files larger than this for both zip and package.txt (default: 64)",
    )
    pcode.add_argument(
        "--dry-run",
        action="store_true",
        dest="package_code_dry_run",
        help="List included and skipped paths only; do not write outputs",
    )
    _add_trace_args(pcode)

    air = sub.add_parser(
        "agent-instruction-refine",
        help="Training loop: N test replies → AI scores (configurable metrics) → AI rewrites persona files → apply; leaderboard under agents/<agent>/instruction-training/",
    )
    air.add_argument("--agent", required=True, metavar="AGENT", dest="instruction_agent", help="Agent folder name under agents/")
    air.add_argument(
        "--config",
        dest="instruction_config",
        default="",
        help="Optional JSON config file path (see agents/general/instruction-training/example_config.json)",
    )
    air.add_argument("--iterations", type=int, dest="instruction_iterations", default=None, help="Override max_iterations from config")
    air.add_argument(
        "--max-messages",
        type=int,
        dest="instruction_max_messages",
        default=None,
        help="Max test fixtures to use (default: config test_message_limit)",
    )
    air.add_argument(
        "--seed",
        type=int,
        dest="instruction_seed",
        default=None,
        help="Random seed to shuffle fixtures before taking the first max-messages (default: sorted path order)",
    )
    air.add_argument(
        "--dry-run",
        action="store_true",
        dest="instruction_dry_run",
        help="Run scoring and rewriting but do not write persona files (still writes run artifacts)",
    )
    air.add_argument("--model", default="gpt-4o-mini")
    air.add_argument("--api-key")
    air.add_argument("--ai-endpoint", default="https://api.openai.com/v1/chat/completions")
    air.add_argument("--temperature", type=float, default=0.2)
    air.add_argument("--max-tokens", type=int, default=1200)
    air.add_argument(
        "--ai-timeout-seconds",
        type=int,
        default=DEFAULT_AI_CHAT_TIMEOUT,
        dest="ai_timeout_seconds",
        help=f"HTTP read timeout per AI request (default {DEFAULT_AI_CHAT_TIMEOUT}s)",
    )
    _add_trace_args(air)

    return p


def main(argv: list[str] | None = None) -> int:
    argv_effective = list(argv) if argv is not None else sys.argv[1:]
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True, write_through=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True, write_through=True)
    parser = build_parser()
    args = parser.parse_args(argv_effective)
    if getattr(args, "diagnostic", False):
        set_trace_level(3)
    elif getattr(args, "verbose", False):
        set_trace_level(2)
    else:
        set_trace_level(1)

    prefer_ipv4_resolution()
    trace(2, "[network] IPv4-first DNS resolution enabled")

    if hasattr(args, "verbosity"):
        if getattr(args, "diagnostic", False):
            args.verbosity = max(int(getattr(args, "verbosity", 1)), 3)
        elif getattr(args, "verbose", False):
            args.verbosity = max(int(getattr(args, "verbosity", 1)), 2)

    project = TailorProject(discover_tailor_project_root())
    _apply_active_ai_profile_to_args(args, project, argv_effective)

    if getattr(args, "diagnostics", False) and not getattr(args, "cmd", None):
        return cmd_diagnostics(args, project)
    if not getattr(args, "cmd", None):
        parser.error("a command is required unless --diagnostics is used")

    handlers = {
        "project-summary": cmd_project_summary,
        "agent-create": cmd_agent_create,
        "agent-list": cmd_agent_list,
        "channel-gmail-add": cmd_channel_gmail_add,
        "channel-gmail-oauth": cmd_channel_gmail_oauth,
        "workflow-list": cmd_workflow_list,
        "workflow-run": cmd_workflow_run,
        "workflow-exec": cmd_workflow_exec,
        "plugin-list": cmd_plugin_list,
        "plugin-create": cmd_plugin_create,
        "workflow-compose": cmd_workflow_compose,
        "run-workflow": cmd_run_workflow,
        "engine-log": cmd_engine_log,
        "gmail-monitor": cmd_gmail_monitor,
        "fetlife-list-inbox": cmd_fetlife_list_inbox,
        "fetlife-fetch-thread": cmd_fetlife_fetch_thread,
        "fetlife-send-message": cmd_fetlife_send_message,
        "download-fetlife-message-history": cmd_download_fetlife_message_history,
        "dev-folder-each": cmd_dev_folder_each,
        "dev-folder-combined": cmd_dev_folder_combined,
        "dev-ai-bounce": cmd_dev_ai_bounce,
        "diagnostics": cmd_diagnostics,
        "check-logs": cmd_check_logs,
        "check-api-status": cmd_check_api_status,
        "switch-ai-profile": cmd_switch_ai_profile,
        "generate-agent-report": cmd_generate_agent_report,
        "generate-workflow-dashboard": cmd_generate_workflow_dashboard,
        "kill-workflow-run": cmd_kill_workflow_run,
        "show-notifications": cmd_show_notifications,
        "clear-notifications": cmd_clear_notifications,
        "package-code": cmd_package_code,
        "agent-instruction-refine": cmd_agent_instruction_refine,
    }

    try:
        result = handlers[args.cmd](args, project)
        if getattr(args, "cmd", "") in ("workflow-exec", "run-workflow"):
            _refresh_latest_workflow_dashboard(project)
        return result
    except Exception as ex:
        safe_args = _redact_sensitive_value(vars(args))
        body = (
            f"Command: {getattr(args, 'cmd', '')}\n"
            f"Project root: {project.root}\n"
            f"Args:\n{json.dumps(safe_args, indent=2, ensure_ascii=False, default=str)}\n\n"
            f"Error:\n{str(ex)}"
        )
        _append_general_exec_log(project, event_type="error", title="Unhandled command error", body=body)
        _append_active_exec_log(event_type="error", title="Unhandled workflow error", body=body)
        _append_engine_log(
            project,
            {
                "event": "command_error",
                "command": str(getattr(args, "cmd", "") or ""),
                "detail": str(ex)[:1200],
            },
        )
        if getattr(args, "cmd", "") in ("workflow-exec", "run-workflow"):
            _refresh_latest_workflow_dashboard(project)
        print(f"ERROR: {ex}", file=sys.stderr)
        return 1
    finally:
        _set_engine_workflow_dev_api_context(None)


if __name__ == "__main__":
    raise SystemExit(main())
