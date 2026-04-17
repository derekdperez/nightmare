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
import json
import mimetypes
import os
import re
import ssl
import subprocess
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

from output_cleanup import clear_output_root_children
from reporting.server_pages import render_dashboard_html, render_database_html, render_workers_html
from server_app.store import CoordinatorStore

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = BASE_DIR / "output"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 443
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "server.json"
MAX_LOG_TAIL_BYTES = 64 * 1024
DEFAULT_COORDINATOR_LEASE_SECONDS = 120
RESET_COORDINATOR_CONFIRM = "RESET_COORDINATOR_DATA"


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


def collect_dashboard_data(output_root: Path) -> dict[str, Any]:
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
        "batch_counts": batch_counts,
    }

    return {
        "generated_at_utc": _iso_now(),
        "output_root": str(output_root),
        "totals": totals,
        "master_reports": _collect_master_reports(output_root),
        "domains": domains,
    }


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
    def coordinator_token(self) -> str:
        return str(getattr(self.server, "coordinator_token", "") or "")  # type: ignore[attr-defined]

    @property
    def master_report_regen_token(self) -> str:
        return str(getattr(self.server, "master_report_regen_token", "") or "").strip()  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        # Keep server output concise.
        sys.stdout.write("[server] " + format % args + "\n")

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
        if path == "/api/summary":
            payload = collect_dashboard_data(self.output_root)
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
            self._write_json(self.coordinator_store.database_status())
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
            self._write_json(self.coordinator_store.worker_statuses(stale_after_seconds=stale_after_seconds))
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
            snapshot = self.coordinator_store.worker_control_snapshot(stale_after_seconds=stale_after_seconds)
            for worker in snapshot.get("workers", []):
                worker_id = str(worker.get("worker_id", "") or "").strip()
                worker["logs"] = self._discover_worker_log_links(worker_id)
                worker["config"] = self._resolve_worker_config_rel(worker_id)
            self._write_json(snapshot)
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

    coordinator_store: CoordinatorStore | None = None
    if database_url:
        coordinator_store = CoordinatorStore(database_url)

    def _prepare_server(port_value: int) -> ThreadingHTTPServer:
        srv = ThreadingHTTPServer((host, port_value), DashboardHandler)
        srv.app_root = BASE_DIR  # type: ignore[attr-defined]
        srv.output_root = output_root  # type: ignore[attr-defined]
        srv.coordinator_store = coordinator_store  # type: ignore[attr-defined]
        srv.coordinator_token = coordinator_token  # type: ignore[attr-defined]
        srv.master_report_regen_token = master_report_regen_token  # type: ignore[attr-defined]
        srv._master_regen_lock = threading.Lock()  # type: ignore[attr-defined]
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
    else:
        print("[server] coordinator mode disabled (database_url not set)", flush=True)
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



