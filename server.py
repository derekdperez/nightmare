#!/usr/bin/env python3
"""Small web server for Nightmare/Fozzy reporting and live run overview.

Usage:
    python server.py
    python server.py --host 127.0.0.1 --port 8080 --output-root output
    python server.py --config server.json

The dashboard provides:
- aggregate counts (domains discovered, completed/running/pending/failed),
- per-domain status rows with links to generated HTML reports and JSON artifacts,
- master-report links when available,
- recent log tail snippets.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import mimetypes
import os
import re
import ssl
import sys
import time
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg = None

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = BASE_DIR / "output"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 443
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "server.json"
MAX_LOG_TAIL_BYTES = 64 * 1024
DEFAULT_COORDINATOR_LEASE_SECONDS = 120


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
    }


def _merged_value(cli_value: Any, cfg: dict[str, Any], key: str, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if key in cfg:
        return cfg[key]
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


def _get_root_domain(hostname: str) -> str:
    hostname = (hostname or "").lower().strip(".")
    if not hostname:
        return ""
    parts = hostname.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return hostname


def _normalize_target_url(raw: str) -> tuple[str, str]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("empty target")
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        text = f"https://{text.lstrip('/')}"
        parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("target must be a valid http/https URL or hostname")
    root_domain = _get_root_domain(parsed.hostname)
    if not root_domain:
        raise ValueError("could not derive root domain")
    normalized_path = parsed.path or "/"
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        path=normalized_path,
        fragment="",
    )
    return normalized.geturl(), root_domain


def _make_target_entry_id(line_no: int, raw: str) -> str:
    return hashlib.sha1(f"{line_no}:{raw}".encode("utf-8")).hexdigest()[:16]


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


class CoordinatorStore:
    def __init__(self, database_url: str):
        if psycopg is None:
            raise RuntimeError("psycopg is required for postgres coordinator mode")
        self.database_url = str(database_url or "").strip()
        if not self.database_url:
            raise ValueError("database_url is required for coordinator mode")
        self._ensure_schema()

    def _connect(self):
        return psycopg.connect(self.database_url, autocommit=False)

    def _ensure_schema(self) -> None:
        ddl = """
CREATE TABLE IF NOT EXISTS coordinator_targets (
  entry_id TEXT PRIMARY KEY,
  line_number INTEGER NOT NULL,
  raw TEXT NOT NULL,
  start_url TEXT NOT NULL,
  root_domain TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  error TEXT,
  exit_code INTEGER,
  worker_id TEXT,
  lease_expires_at TIMESTAMPTZ,
  started_at_utc TIMESTAMPTZ,
  completed_at_utc TIMESTAMPTZ,
  heartbeat_at_utc TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_coordinator_targets_status ON coordinator_targets(status);
CREATE INDEX IF NOT EXISTS idx_coordinator_targets_lease ON coordinator_targets(lease_expires_at);

CREATE TABLE IF NOT EXISTS coordinator_sessions (
  root_domain TEXT PRIMARY KEY,
  start_url TEXT NOT NULL,
  max_pages INTEGER,
  saved_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  payload JSONB NOT NULL
);
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def register_targets(self, targets: list[str]) -> dict[str, Any]:
        inserted = 0
        skipped = 0
        rows: list[tuple[str, int, str, str, str]] = []
        for line_no, raw in enumerate(targets, start=1):
            text = str(raw or "").strip()
            if not text or text.startswith("#"):
                continue
            try:
                start_url, root_domain = _normalize_target_url(text)
            except Exception:
                skipped += 1
                continue
            rows.append((_make_target_entry_id(line_no, text), line_no, text, start_url, root_domain))
        if not rows:
            return {"inserted": 0, "skipped": skipped}
        upsert_sql = """
INSERT INTO coordinator_targets(entry_id, line_number, raw, start_url, root_domain, status, updated_at_utc)
VALUES (%s, %s, %s, %s, %s, 'pending', NOW())
ON CONFLICT (entry_id) DO UPDATE
SET line_number = EXCLUDED.line_number,
    raw = EXCLUDED.raw,
    start_url = EXCLUDED.start_url,
    root_domain = EXCLUDED.root_domain,
    updated_at_utc = NOW();
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(upsert_sql, rows)
            conn.commit()
        inserted = len(rows)
        return {"inserted": inserted, "skipped": skipped}

    def claim_target(self, worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
        worker = str(worker_id or "").strip()
        if not worker:
            raise ValueError("worker_id is required")
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        sql = """
WITH candidate AS (
    SELECT entry_id
    FROM coordinator_targets
    WHERE status = 'pending'
       OR (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < NOW())
    ORDER BY line_number ASC, created_at_utc ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE coordinator_targets t
SET status = 'running',
    worker_id = %s,
    lease_expires_at = NOW() + ((%s)::text || ' seconds')::interval,
    started_at_utc = COALESCE(t.started_at_utc, NOW()),
    heartbeat_at_utc = NOW(),
    completed_at_utc = NULL,
    updated_at_utc = NOW(),
    attempt_count = t.attempt_count + 1,
    error = NULL
FROM candidate
WHERE t.entry_id = candidate.entry_id
RETURNING t.entry_id, t.line_number, t.raw, t.start_url, t.root_domain, t.attempt_count, t.status, t.worker_id, t.lease_expires_at;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (worker, lease))
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        return {
            "entry_id": row[0],
            "line": int(row[1]),
            "raw": row[2],
            "start_url": row[3],
            "root_domain": row[4],
            "attempt_count": int(row[5] or 0),
            "status": row[6],
            "worker_id": row[7],
            "lease_expires_at": row[8].isoformat() if row[8] else None,
        }

    def heartbeat(self, entry_id: str, worker_id: str, lease_seconds: int) -> bool:
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        sql = """
UPDATE coordinator_targets
SET heartbeat_at_utc = NOW(),
    lease_expires_at = NOW() + ((%s)::text || ' seconds')::interval,
    updated_at_utc = NOW()
WHERE entry_id = %s
  AND worker_id = %s
  AND status = 'running';
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (lease, str(entry_id), str(worker_id)))
                updated = int(cur.rowcount or 0)
            conn.commit()
        return updated > 0

    def finish(self, entry_id: str, worker_id: str, *, exit_code: int, error: str = "") -> bool:
        ok = int(exit_code) == 0
        status = "completed" if ok else "failed"
        sql = """
UPDATE coordinator_targets
SET status = %s,
    exit_code = %s,
    error = %s,
    completed_at_utc = NOW(),
    lease_expires_at = NULL,
    heartbeat_at_utc = NOW(),
    updated_at_utc = NOW()
WHERE entry_id = %s
  AND worker_id = %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (status, int(exit_code), str(error or "")[:2000], str(entry_id), str(worker_id)))
                updated = int(cur.rowcount or 0)
            conn.commit()
        return updated > 0

    def load_session(self, root_domain: str) -> dict[str, Any] | None:
        sql = """
SELECT root_domain, start_url, max_pages, saved_at_utc, payload
FROM coordinator_sessions
WHERE root_domain = %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (str(root_domain).strip().lower(),))
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        payload = row[4] if isinstance(row[4], dict) else {}
        return {
            "root_domain": row[0],
            "start_url": row[1],
            "max_pages": row[2],
            "saved_at_utc": row[3].isoformat() if row[3] else None,
            "state": payload.get("state", {}),
            "frontier": payload.get("frontier", []),
            "payload": payload,
        }

    def save_session(
        self,
        *,
        root_domain: str,
        start_url: str,
        max_pages: int,
        payload: dict[str, Any],
        saved_at_utc: str | None = None,
    ) -> bool:
        rd = str(root_domain or "").strip().lower()
        if not rd:
            return False
        su = str(start_url or "").strip()
        if not su:
            return False
        saved_dt = None
        if saved_at_utc:
            try:
                saved_dt = datetime.fromisoformat(str(saved_at_utc).replace("Z", "+00:00"))
            except Exception:
                saved_dt = None
        sql = """
INSERT INTO coordinator_sessions(root_domain, start_url, max_pages, saved_at_utc, payload)
VALUES (%s, %s, %s, COALESCE(%s, NOW()), %s::jsonb)
ON CONFLICT (root_domain) DO UPDATE
SET start_url = EXCLUDED.start_url,
    max_pages = EXCLUDED.max_pages,
    saved_at_utc = EXCLUDED.saved_at_utc,
    payload = EXCLUDED.payload;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        rd,
                        su,
                        int(max_pages) if max_pages is not None else None,
                        saved_dt,
                        json.dumps(payload, ensure_ascii=False),
                    ),
                )
            conn.commit()
        return True

    def status_summary(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status, COUNT(*) FROM coordinator_targets GROUP BY status;"
                )
                rows = cur.fetchall()
            conn.commit()
        for status, count in rows:
            counts[str(status)] = int(count or 0)
        return {
            "counts": counts,
            "generated_at_utc": _iso_now(),
        }


def _normalize_and_validate_relative_path(root: Path, raw_relative: str) -> Path | None:
    cleaned = str(raw_relative or "").strip().replace("\\", "/")
    cleaned = cleaned.lstrip("/")
    candidate = (root / cleaned).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        return None
    return candidate


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

    def log_message(self, format: str, *args: Any) -> None:
        # Keep server output concise.
        sys.stdout.write("[server] " + format % args + "\n")

    def _write_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
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
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
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
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
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
        return x_token == token

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
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
                self._write_text(self._render_dashboard_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/dashboard":
            self._write_text(self._render_dashboard_html(), content_type="text/html; charset=utf-8")
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
        if path == "/api/coord/state":
            if self.coordinator_store is None:
                self._write_json({"error": "coordinator is not configured (database_url missing)"}, status=503)
                return
            if not self._is_coordinator_authorized():
                self._write_json({"error": "unauthorized"}, status=401)
                return
            self._write_json(self.coordinator_store.status_summary())
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

        self._write_json({"error": "not found"}, status=404)

    def _render_dashboard_html(self) -> str:
        return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Nightmare Dashboard</title>
  <style>
    body { font-family: Segoe UI, Arial, sans-serif; margin: 16px; background: #f7fafc; color: #0f172a; }
    h1 { margin: 0 0 10px; }
    .meta { color: #475569; margin-bottom: 14px; font-size: 13px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 16px; }
    .card { background: #fff; border: 1px solid #cbd5e1; border-radius: 8px; padding: 10px; }
    .k { font-size: 12px; color: #475569; }
    .v { font-size: 22px; font-weight: 700; margin-top: 4px; }
    table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #cbd5e1; }
    th, td { border: 1px solid #cbd5e1; padding: 6px 8px; font-size: 12px; vertical-align: top; text-align: left; }
    th { background: #e2e8f0; }
    .log { white-space: pre-wrap; word-break: break-word; max-height: 220px; overflow: auto; margin: 0; background: #0b1220; color: #c8d3f5; padding: 8px; border-radius: 6px; }
    .rowlinks a { margin-right: 8px; }
    .muted { color: #64748b; }
    code { background: #e2e8f0; padding: 1px 4px; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>Nightmare Live Dashboard</h1>
  <div class="meta">Auto-refresh every 5s. Reports are served from this app via <code>/files/*</code>.</div>
  <div id="cards" class="cards"></div>
  <h2>Master Reports</h2>
  <div id="masters" class="muted">Loading…</div>
  <h2>Domains</h2>
  <table>
    <thead>
      <tr>
        <th>Domain</th>
        <th>Status</th>
        <th>URLs</th>
        <th>Findings</th>
        <th>Reports</th>
        <th>Latest Log Tail</th>
      </tr>
    </thead>
    <tbody id="rows"><tr><td colspan="6">Loading…</td></tr></tbody>
  </table>
  <script>
    function esc(v) {
      return String(v || "").replace(/[&<>\"']/g, (ch) => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[ch]));
    }
    function fileLink(relPath, label) {
      if (!relPath) return "";
      const encoded = encodeURIComponent(relPath).replaceAll("%2F", "/");
      return `<a href="/files/${encoded}" target="_blank" rel="noopener noreferrer">${esc(label)}</a>`;
    }
    function buildCards(t) {
      const cards = [
        ["Domains", t.domains],
        ["Fozzy Complete", t.fozzy_complete],
        ["Fozzy Interrupted", t.fozzy_interrupted],
        ["Parameterized", t.parameterized],
        ["Discovered Only", t.discovered],
      ];
      return cards.map(([k, v]) => `<div class="card"><div class="k">${esc(k)}</div><div class="v">${esc(v)}</div></div>`).join("");
    }
    function render(data) {
      document.getElementById("cards").innerHTML = buildCards(data.totals || {});
      const masters = (data.master_reports || []).map((m) => fileLink(m.relative, m.name)).join(" | ");
      document.getElementById("masters").innerHTML = masters || "<span class='muted'>No master reports found.</span>";

      const rows = data.domains || [];
      if (!rows.length) {
        document.getElementById("rows").innerHTML = "<tr><td colspan='6'>No domain output folders found.</td></tr>";
        return;
      }
      document.getElementById("rows").innerHTML = rows.map((d) => {
        const p = d.paths_rel || {};
        const links = [
          fileLink(p.nightmare_report_html, "Nightmare HTML"),
          fileLink(p.fozzy_results_html, "Fozzy HTML"),
          fileLink(p.url_inventory, "URL Inventory"),
          fileLink(p.requests, "Requests JSON"),
          fileLink(p.fozzy_summary_json, "Fozzy Summary")
        ].filter(Boolean).join(" ");
        return `<tr>
          <td>${esc(d.domain)}</td>
          <td>${esc(d.status)}</td>
          <td>unique=${esc(d.unique_urls)}<br>requested=${esc(d.requested_urls)}</td>
          <td>anomalies=${esc(d.anomalies)}<br>reflections=${esc(d.reflections)}</td>
          <td class="rowlinks">${links || "<span class='muted'>none</span>"}</td>
          <td><pre class="log">${esc((d.latest_log_tail || "").slice(-4000))}</pre></td>
        </tr>`;
      }).join("");
    }
    async function refresh() {
      try {
        const rsp = await fetch("/api/summary", { cache: "no-store" });
        if (!rsp.ok) return;
        const data = await rsp.json();
        render(data);
      } catch (_) {}
    }
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""


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

    if args.http_port is None and args.https_port is None and args.port is not None:
        http_port = legacy_port
        https_port = 0
    if http_port and (http_port < 1 or http_port > 65535):
        raise ValueError("http_port must be in range 1..65535")
    if https_port and (https_port < 1 or https_port > 65535):
        raise ValueError("https_port must be in range 1..65535")

    coordinator_store: CoordinatorStore | None = None
    if database_url:
        coordinator_store = CoordinatorStore(database_url)

    def _prepare_server(port_value: int) -> ThreadingHTTPServer:
        srv = ThreadingHTTPServer((host, port_value), DashboardHandler)
        srv.app_root = BASE_DIR  # type: ignore[attr-defined]
        srv.output_root = output_root  # type: ignore[attr-defined]
        srv.coordinator_store = coordinator_store  # type: ignore[attr-defined]
        srv.coordinator_token = coordinator_token  # type: ignore[attr-defined]
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
