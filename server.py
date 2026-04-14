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
import hashlib
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

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg = None

from output_cleanup import clear_output_root_children

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

CREATE TABLE IF NOT EXISTS coordinator_stage_tasks (
  root_domain TEXT NOT NULL,
  stage TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  worker_id TEXT,
  lease_expires_at TIMESTAMPTZ,
  started_at_utc TIMESTAMPTZ,
  completed_at_utc TIMESTAMPTZ,
  heartbeat_at_utc TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  exit_code INTEGER,
  error TEXT,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(root_domain, stage)
);
CREATE INDEX IF NOT EXISTS idx_stage_tasks_status_stage ON coordinator_stage_tasks(stage, status);
CREATE INDEX IF NOT EXISTS idx_stage_tasks_lease ON coordinator_stage_tasks(lease_expires_at);

CREATE TABLE IF NOT EXISTS coordinator_artifacts (
  root_domain TEXT NOT NULL,
  artifact_type TEXT NOT NULL,
  source_worker TEXT,
  content BYTEA NOT NULL,
  content_encoding TEXT NOT NULL DEFAULT 'identity',
  content_sha256 TEXT NOT NULL,
  content_size_bytes BIGINT NOT NULL,
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(root_domain, artifact_type)
);
CREATE INDEX IF NOT EXISTS idx_artifacts_domain ON coordinator_artifacts(root_domain);

CREATE TABLE IF NOT EXISTS coordinator_fleet_settings (
  singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
  output_clear_generation BIGINT NOT NULL DEFAULT 0,
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO coordinator_fleet_settings(singleton) VALUES (1)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS coordinator_worker_commands (
  id BIGSERIAL PRIMARY KEY,
  worker_id TEXT NOT NULL,
  command TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'queued',
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_worker_commands_worker_created
  ON coordinator_worker_commands(worker_id, created_at_utc DESC);
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

    def reset_coordinator_tables(self) -> dict[str, Any]:
        """Truncate all coordinator tables (targets, sessions, stage tasks, artifacts)."""
        truncate_sql = """
TRUNCATE TABLE coordinator_targets, coordinator_sessions, coordinator_stage_tasks, coordinator_artifacts;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(truncate_sql)
            conn.commit()
        return {
            "truncated_tables": [
                "coordinator_targets",
                "coordinator_sessions",
                "coordinator_stage_tasks",
                "coordinator_artifacts",
            ],
            "reset_at_utc": _iso_now(),
        }

    def get_fleet_settings(self) -> dict[str, Any]:
        sql = """
SELECT output_clear_generation, updated_at_utc
FROM coordinator_fleet_settings
WHERE singleton = 1;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return {"output_clear_generation": 0, "updated_at_utc": None}
        gen, updated = row[0], row[1]
        return {
            "output_clear_generation": int(gen or 0),
            "updated_at_utc": updated.isoformat() if updated else None,
        }

    def bump_output_clear_generation(self) -> dict[str, Any]:
        sql = """
UPDATE coordinator_fleet_settings
SET output_clear_generation = output_clear_generation + 1,
    updated_at_utc = NOW()
WHERE singleton = 1
RETURNING output_clear_generation, updated_at_utc;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("coordinator_fleet_settings row missing (schema not initialized?)")
            conn.commit()
        gen, updated = row[0], row[1]
        return {
            "output_clear_generation": int(gen or 0),
            "updated_at_utc": updated.isoformat() if updated else None,
        }

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

    def worker_statuses(self, *, stale_after_seconds: int = DEFAULT_COORDINATOR_LEASE_SECONDS) -> dict[str, Any]:
        stale_after = max(15, int(stale_after_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        sql = """
WITH target_agg AS (
    SELECT
      worker_id,
      MAX(heartbeat_at_utc) AS last_target_heartbeat,
      COUNT(*) FILTER (WHERE status = 'running') AS running_targets,
      COUNT(*) FILTER (
        WHERE status = 'running'
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at > NOW()
      ) AS active_target_leases
    FROM coordinator_targets
    WHERE worker_id IS NOT NULL AND worker_id <> ''
    GROUP BY worker_id
),
stage_agg AS (
    SELECT
      worker_id,
      MAX(heartbeat_at_utc) AS last_stage_heartbeat,
      COUNT(*) FILTER (WHERE status = 'running') AS running_stage_tasks,
      COUNT(*) FILTER (
        WHERE status = 'running'
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at > NOW()
      ) AS active_stage_leases,
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT CASE WHEN status = 'running' THEN stage ELSE NULL END), NULL) AS active_stages
    FROM coordinator_stage_tasks
    WHERE worker_id IS NOT NULL AND worker_id <> ''
    GROUP BY worker_id
)
SELECT
  COALESCE(t.worker_id, s.worker_id) AS worker_id,
  CASE
    WHEN t.last_target_heartbeat IS NULL THEN s.last_stage_heartbeat
    WHEN s.last_stage_heartbeat IS NULL THEN t.last_target_heartbeat
    ELSE GREATEST(t.last_target_heartbeat, s.last_stage_heartbeat)
  END AS last_heartbeat_at_utc,
  COALESCE(t.running_targets, 0) AS running_targets,
  COALESCE(s.running_stage_tasks, 0) AS running_stage_tasks,
  COALESCE(t.active_target_leases, 0) AS active_target_leases,
  COALESCE(s.active_stage_leases, 0) AS active_stage_leases,
  COALESCE(s.active_stages, ARRAY[]::text[]) AS active_stages
FROM target_agg t
FULL OUTER JOIN stage_agg s
  ON s.worker_id = t.worker_id
ORDER BY worker_id ASC;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            conn.commit()

        now_utc = datetime.now(timezone.utc)
        workers: list[dict[str, Any]] = []
        online_count = 0
        for row in rows:
            worker_id = str(row[0] or "").strip()
            last_heartbeat = row[1]
            running_targets = int(row[2] or 0)
            running_stage_tasks = int(row[3] or 0)
            active_target_leases = int(row[4] or 0)
            active_stage_leases = int(row[5] or 0)
            active_stages_raw = row[6] if isinstance(row[6], list) else []
            active_stages = [str(item) for item in active_stages_raw if str(item or "").strip()]

            seconds_since: int | None = None
            last_heartbeat_iso: str | None = None
            if last_heartbeat is not None:
                last_heartbeat_iso = last_heartbeat.isoformat()
                delta_seconds = (now_utc - last_heartbeat).total_seconds()
                seconds_since = max(0, int(delta_seconds))
            is_online = seconds_since is not None and seconds_since <= stale_after
            if is_online:
                online_count += 1
            workers.append(
                {
                    "worker_id": worker_id,
                    "status": "online" if is_online else "stale",
                    "last_heartbeat_at_utc": last_heartbeat_iso,
                    "seconds_since_heartbeat": seconds_since,
                    "running_targets": running_targets,
                    "running_stage_tasks": running_stage_tasks,
                    "active_target_leases": active_target_leases,
                    "active_stage_leases": active_stage_leases,
                    "active_stages": active_stages,
                }
            )

        total = len(workers)
        return {
            "generated_at_utc": now_utc.isoformat(),
            "stale_after_seconds": stale_after,
            "counts": {
                "total_workers": total,
                "online_workers": online_count,
                "stale_workers": max(0, total - online_count),
            },
            "workers": workers,
        }

    def worker_control_snapshot(self, *, stale_after_seconds: int = DEFAULT_COORDINATOR_LEASE_SECONDS) -> dict[str, Any]:
        stale_after = max(15, int(stale_after_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        sql = """
WITH target_agg AS (
    SELECT
      worker_id,
      MAX(heartbeat_at_utc) AS last_target_heartbeat,
      COUNT(*) FILTER (WHERE status = 'running') AS running_targets,
      COUNT(*) FILTER (WHERE status IN ('running', 'completed')) AS urls_scanned_session,
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT CASE WHEN status = 'running' THEN COALESCE(start_url, root_domain) ELSE NULL END), NULL) AS current_targets
    FROM coordinator_targets
    WHERE worker_id IS NOT NULL AND worker_id <> ''
    GROUP BY worker_id
),
stage_agg AS (
    SELECT
      worker_id,
      MAX(heartbeat_at_utc) AS last_stage_heartbeat,
      COUNT(*) FILTER (WHERE status = 'running') AS running_stage_tasks
    FROM coordinator_stage_tasks
    WHERE worker_id IS NOT NULL AND worker_id <> ''
    GROUP BY worker_id
),
commands AS (
    SELECT worker_id, COUNT(*) FILTER (WHERE status = 'queued') AS queued_commands
    FROM coordinator_worker_commands
    GROUP BY worker_id
)
SELECT
  COALESCE(t.worker_id, s.worker_id, c.worker_id) AS worker_id,
  CASE
    WHEN t.last_target_heartbeat IS NULL THEN s.last_stage_heartbeat
    WHEN s.last_stage_heartbeat IS NULL THEN t.last_target_heartbeat
    ELSE GREATEST(t.last_target_heartbeat, s.last_stage_heartbeat)
  END AS last_heartbeat_at_utc,
  COALESCE(t.running_targets, 0) AS running_targets,
  COALESCE(s.running_stage_tasks, 0) AS running_stage_tasks,
  COALESCE(t.urls_scanned_session, 0) AS urls_scanned_session,
  COALESCE(t.current_targets, ARRAY[]::text[]) AS current_targets,
  COALESCE(c.queued_commands, 0) AS queued_commands
FROM target_agg t
FULL OUTER JOIN stage_agg s ON s.worker_id = t.worker_id
FULL OUTER JOIN commands c ON c.worker_id = COALESCE(t.worker_id, s.worker_id)
ORDER BY worker_id ASC;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            conn.commit()
        now_utc = datetime.now(timezone.utc)
        workers: list[dict[str, Any]] = []
        online_count = 0
        for row in rows:
            worker_id = str(row[0] or "").strip()
            if not worker_id:
                continue
            last_heartbeat = row[1]
            seconds_since: int | None = None
            last_heartbeat_iso: str | None = None
            if last_heartbeat is not None:
                last_heartbeat_iso = last_heartbeat.isoformat()
                seconds_since = max(0, int((now_utc - last_heartbeat).total_seconds()))
            is_online = seconds_since is not None and seconds_since <= stale_after
            if is_online:
                online_count += 1
            current_targets_raw = row[5] if isinstance(row[5], list) else []
            workers.append(
                {
                    "worker_id": worker_id,
                    "status": "online" if is_online else "stale",
                    "last_heartbeat_at_utc": last_heartbeat_iso,
                    "seconds_since_heartbeat": seconds_since,
                    "running_targets": int(row[2] or 0),
                    "running_stage_tasks": int(row[3] or 0),
                    "urls_scanned_session": int(row[4] or 0),
                    "current_targets": [str(item) for item in current_targets_raw if str(item or "").strip()],
                    "queued_commands": int(row[6] or 0),
                }
            )
        total = len(workers)
        return {
            "generated_at_utc": now_utc.isoformat(),
            "stale_after_seconds": stale_after,
            "counts": {
                "total_workers": total,
                "online_workers": online_count,
                "stale_workers": max(0, total - online_count),
            },
            "workers": workers,
        }

    def queue_worker_command(self, worker_id: str, command: str, payload: dict[str, Any] | None = None) -> bool:
        wid = str(worker_id or "").strip()
        cmd = str(command or "").strip().lower()
        if not wid or not cmd:
            return False
        safe_payload = payload if isinstance(payload, dict) else {}
        sql = """
INSERT INTO coordinator_worker_commands(worker_id, command, payload, status, updated_at_utc)
VALUES (%s, %s, %s::jsonb, 'queued', NOW());
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (wid, cmd, json.dumps(safe_payload)))
            conn.commit()
        return True

    def enqueue_stage(self, root_domain: str, stage: str) -> bool:
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        if not rd or not stg:
            return False
        sql = """
INSERT INTO coordinator_stage_tasks(root_domain, stage, status, updated_at_utc)
VALUES (%s, %s, 'pending', NOW())
ON CONFLICT (root_domain, stage) DO UPDATE
SET status = CASE
      WHEN coordinator_stage_tasks.status = 'completed' THEN coordinator_stage_tasks.status
      ELSE 'pending'
    END,
    worker_id = NULL,
    lease_expires_at = NULL,
    heartbeat_at_utc = NULL,
    completed_at_utc = CASE
      WHEN coordinator_stage_tasks.status = 'completed' THEN coordinator_stage_tasks.completed_at_utc
      ELSE NULL
    END,
    updated_at_utc = NOW();
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (rd, stg))
            conn.commit()
        return True

    def claim_stage(self, stage: str, worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        if not stg or not wid:
            raise ValueError("stage and worker_id are required")
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        sql = """
WITH candidate AS (
    SELECT root_domain
    FROM coordinator_stage_tasks
    WHERE stage = %s
      AND (
        status = 'pending'
        OR (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < NOW())
      )
    ORDER BY created_at_utc ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE coordinator_stage_tasks t
SET status = 'running',
    worker_id = %s,
    lease_expires_at = NOW() + ((%s)::text || ' seconds')::interval,
    started_at_utc = COALESCE(t.started_at_utc, NOW()),
    completed_at_utc = NULL,
    heartbeat_at_utc = NOW(),
    attempt_count = t.attempt_count + 1,
    updated_at_utc = NOW(),
    error = NULL
FROM candidate
WHERE t.root_domain = candidate.root_domain
  AND t.stage = %s
RETURNING t.root_domain, t.stage, t.status, t.worker_id, t.attempt_count, t.lease_expires_at;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (stg, wid, lease, stg))
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        return {
            "root_domain": row[0],
            "stage": row[1],
            "status": row[2],
            "worker_id": row[3],
            "attempt_count": int(row[4] or 0),
            "lease_expires_at": row[5].isoformat() if row[5] else None,
        }

    def heartbeat_stage(self, root_domain: str, stage: str, worker_id: str, lease_seconds: int) -> bool:
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        if not rd or not stg or not wid:
            return False
        sql = """
UPDATE coordinator_stage_tasks
SET heartbeat_at_utc = NOW(),
    lease_expires_at = NOW() + ((%s)::text || ' seconds')::interval,
    updated_at_utc = NOW()
WHERE root_domain = %s
  AND stage = %s
  AND worker_id = %s
  AND status = 'running';
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (lease, rd, stg, wid))
                updated = int(cur.rowcount or 0)
            conn.commit()
        return updated > 0

    def complete_stage(
        self,
        root_domain: str,
        stage: str,
        worker_id: str,
        *,
        exit_code: int,
        error: str = "",
    ) -> bool:
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        if not rd or not stg or not wid:
            return False
        ok = int(exit_code) == 0
        next_status = "completed" if ok else "failed"
        sql = """
UPDATE coordinator_stage_tasks
SET status = %s,
    exit_code = %s,
    error = %s,
    completed_at_utc = NOW(),
    heartbeat_at_utc = NOW(),
    lease_expires_at = NULL,
    updated_at_utc = NOW()
WHERE root_domain = %s
  AND stage = %s
  AND worker_id = %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (next_status, int(exit_code), str(error or "")[:2000], rd, stg, wid))
                updated = int(cur.rowcount or 0)
            conn.commit()
        return updated > 0

    def upload_artifact(
        self,
        *,
        root_domain: str,
        artifact_type: str,
        content: bytes,
        source_worker: str = "",
        content_encoding: str = "identity",
    ) -> bool:
        rd = str(root_domain or "").strip().lower()
        at = str(artifact_type or "").strip().lower()
        if not rd or not at:
            return False
        data = bytes(content or b"")
        digest = hashlib.sha256(data).hexdigest()
        sql = """
INSERT INTO coordinator_artifacts(
    root_domain, artifact_type, source_worker, content, content_encoding, content_sha256, content_size_bytes, updated_at_utc
)
VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (root_domain, artifact_type) DO UPDATE
SET source_worker = EXCLUDED.source_worker,
    content = EXCLUDED.content,
    content_encoding = EXCLUDED.content_encoding,
    content_sha256 = EXCLUDED.content_sha256,
    content_size_bytes = EXCLUDED.content_size_bytes,
    updated_at_utc = NOW();
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        rd,
                        at,
                        str(source_worker or "")[:200],
                        data,
                        str(content_encoding or "identity")[:120],
                        digest,
                        len(data),
                    ),
                )
            conn.commit()
        return True

    def get_artifact(self, root_domain: str, artifact_type: str) -> dict[str, Any] | None:
        rd = str(root_domain or "").strip().lower()
        at = str(artifact_type or "").strip().lower()
        if not rd or not at:
            return None
        sql = """
SELECT root_domain, artifact_type, source_worker, content, content_encoding, content_sha256, content_size_bytes, updated_at_utc
FROM coordinator_artifacts
WHERE root_domain = %s AND artifact_type = %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (rd, at))
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        return {
            "root_domain": row[0],
            "artifact_type": row[1],
            "source_worker": row[2],
            "content": bytes(row[3] or b""),
            "content_encoding": row[4],
            "content_sha256": row[5],
            "content_size_bytes": int(row[6] or 0),
            "updated_at_utc": row[7].isoformat() if row[7] else None,
        }

    def list_artifacts(self, root_domain: str) -> list[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        if not rd:
            return []
        sql = """
SELECT artifact_type, source_worker, content_encoding, content_sha256, content_size_bytes, updated_at_utc
FROM coordinator_artifacts
WHERE root_domain = %s
ORDER BY artifact_type ASC;
"""
        out: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (rd,))
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            out.append(
                {
                    "artifact_type": row[0],
                    "source_worker": row[1],
                    "content_encoding": row[2],
                    "content_sha256": row[3],
                    "content_size_bytes": int(row[4] or 0),
                    "updated_at_utc": row[5].isoformat() if row[5] else None,
                }
            )
        return out


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

    @property
    def master_report_regen_token(self) -> str:
        return str(getattr(self.server, "master_report_regen_token", "") or "").strip()  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        # Keep server output concise.
        sys.stdout.write("[server] " + format % args + "\n")

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
        return x_token == token

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
                self._write_text(self._render_dashboard_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/dashboard":
            self._write_text(self._render_dashboard_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/workers":
            self._write_text(self._render_workers_html(), content_type="text/html; charset=utf-8")
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
  <div class="meta">Auto-refresh every 5s. Reports are served from this app via <code>/files/*</code>. <a href="/workers">Open Worker Control</a></div>
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

    def _render_workers_html(self) -> str:
        return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Worker Control</title>
  <style>
    body { font-family: Segoe UI, Arial, sans-serif; margin: 16px; background: #f8fafc; color: #0f172a; }
    h1 { margin-bottom: 8px; }
    .meta { margin-bottom: 12px; color: #475569; font-size: 13px; }
    .cards { display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); margin-bottom: 14px; }
    .card { background: #fff; border: 1px solid #cbd5e1; border-radius: 8px; padding: 10px; }
    .k { font-size: 12px; color: #64748b; }
    .v { font-size: 20px; font-weight: 700; margin-top: 4px; }
    .toolbar { display: flex; gap: 8px; align-items: center; margin: 10px 0; flex-wrap: wrap; }
    button { border: 1px solid #94a3b8; background: #fff; border-radius: 6px; padding: 6px 10px; cursor: pointer; }
    button.primary { background: #0ea5e9; border-color: #0284c7; color: white; }
    table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #cbd5e1; }
    th, td { border: 1px solid #cbd5e1; padding: 6px 8px; font-size: 12px; vertical-align: top; text-align: left; }
    th { background: #e2e8f0; }
    .online { color: #15803d; font-weight: 600; }
    .stale { color: #b45309; font-weight: 600; }
    .muted { color: #64748b; }
    #configModal { position: fixed; inset: 0; display: none; background: rgba(15, 23, 42, .35); }
    #configModal .panel { margin: 5vh auto; width: min(940px, 95vw); background: #fff; padding: 12px; border-radius: 10px; border: 1px solid #cbd5e1; }
    textarea { width: 100%; min-height: 340px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
  </style>
</head>
<body>
  <h1>Worker Control Center</h1>
  <div class="meta">Asynchronous auto-refresh every 3s. Use row selection to run bulk start/stop/pause commands. <a href="/dashboard">Back to Dashboard</a></div>
  <div id="cards" class="cards"></div>
  <div class="toolbar">
    <input id="token" type="password" placeholder="Coordinator token">
    <button class="primary" onclick="bulkCommand('start')">Start selected</button>
    <button onclick="bulkCommand('pause')">Pause selected</button>
    <button onclick="bulkCommand('stop')">Stop selected</button>
    <span id="msg" class="muted"></span>
  </div>
  <table>
    <thead>
      <tr>
        <th><input type="checkbox" id="selectAll"></th>
        <th>Worker</th>
        <th>Status</th>
        <th>Current Targets</th>
        <th>URLs Scanned (session)</th>
        <th>Actions</th>
        <th>Logs</th>
        <th>Config</th>
      </tr>
    </thead>
    <tbody id="rows"><tr><td colspan="8">Loading…</td></tr></tbody>
  </table>

  <div id="configModal">
    <div class="panel">
      <h3 id="configTitle">Edit worker config</h3>
      <textarea id="configText"></textarea>
      <div class="toolbar">
        <button class="primary" onclick="saveConfig()">Save config</button>
        <button onclick="closeConfig()">Close</button>
        <span id="configMsg" class="muted"></span>
      </div>
    </div>
  </div>

  <script>
    let selectedConfigWorker = "";
    function esc(v){ return String(v || "").replace(/[&<>\"']/g, (ch) => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[ch])); }
    function escJs(v){ return String(v || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/'/g, "\\'").replace(/\n/g, "\\n").replace(/\r/g, "\\r"); }
    function authHeaders() {
      const token = document.getElementById("token").value.trim();
      return token ? { "Authorization": `Bearer ${token}` } : {};
    }
    async function apiGet(path) {
      const rsp = await fetch(path, { cache: "no-store", headers: authHeaders() });
      if (!rsp.ok) throw new Error(await rsp.text());
      return rsp.json();
    }
    async function apiPost(path, body) {
      const rsp = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify(body || {}),
      });
      if (!rsp.ok) throw new Error(await rsp.text());
      return rsp.json();
    }
    function selectedWorkers() { return [...document.querySelectorAll("input.workerSel:checked")].map((el) => el.value); }
    async function bulkCommand(command) {
      const workers = selectedWorkers();
      if (!workers.length) { document.getElementById("msg").textContent = "Select at least one worker."; return; }
      try {
        const data = await apiPost("/api/coord/workers/command", { command, worker_ids: workers });
        document.getElementById("msg").textContent = `Queued ${data.queued} ${command} command(s).`;
        refresh();
      } catch (e) {
        document.getElementById("msg").textContent = `Command failed: ${e.message}`;
      }
    }
    async function bulkCommandForOne(workerId, command) {
      document.querySelectorAll("input.workerSel").forEach((el) => { el.checked = (el.value === workerId); });
      await bulkCommand(command);
    }
    async function openConfig(workerId) {
      selectedConfigWorker = workerId;
      document.getElementById("configTitle").textContent = `Edit worker config: ${workerId}`;
      document.getElementById("configMsg").textContent = "";
      document.getElementById("configModal").style.display = "block";
      try {
        const data = await apiGet(`/api/coord/worker-config?worker_id=${encodeURIComponent(workerId)}`);
        document.getElementById("configText").value = data.content || "";
      } catch (e) {
        document.getElementById("configText").value = "";
        document.getElementById("configMsg").textContent = `Load failed: ${e.message}`;
      }
    }
    function closeConfig() { document.getElementById("configModal").style.display = "none"; selectedConfigWorker = ""; }
    async function saveConfig() {
      if (!selectedConfigWorker) return;
      try {
        await apiPost("/api/coord/worker-config", { worker_id: selectedConfigWorker, content: document.getElementById("configText").value });
        document.getElementById("configMsg").textContent = "Saved.";
      } catch (e) {
        document.getElementById("configMsg").textContent = `Save failed: ${e.message}`;
      }
    }
    function render(data) {
      const c = data.counts || {};
      const cards = [["Workers", c.total_workers || 0],["Online", c.online_workers || 0],["Stale", c.stale_workers || 0]];
      document.getElementById("cards").innerHTML = cards.map(([k,v]) => `<div class="card"><div class="k">${esc(k)}</div><div class="v">${esc(v)}</div></div>`).join("");
      const rows = data.workers || [];
      if (!rows.length) { document.getElementById("rows").innerHTML = "<tr><td colspan='8'>No workers discovered yet.</td></tr>"; return; }
      document.getElementById("rows").innerHTML = rows.map((w) => {
        const logs = (w.logs || []).map((l) => `<a href="/files/${encodeURIComponent(l.relative).replaceAll("%2F","/")}" target="_blank" rel="noopener noreferrer">${esc(l.label)}</a>`).join("<br>") || "<span class='muted'>none</span>";
        const statusClass = w.status === "online" ? "online" : "stale";
        const targets = (w.current_targets || []).map(esc).join("<br>") || "<span class='muted'>none</span>";
        return `<tr>
          <td><input class="workerSel" type="checkbox" value="${esc(w.worker_id)}"></td>
          <td>${esc(w.worker_id)}</td>
          <td><span class="${statusClass}">${esc(w.status)}</span><br><span class="muted">${esc(w.seconds_since_heartbeat)}s since heartbeat</span></td>
          <td>${targets}</td>
          <td>${esc(w.urls_scanned_session)}</td>
          <td><button onclick="bulkCommandForOne('${escJs(w.worker_id)}','start')">Start</button><button onclick="bulkCommandForOne('${escJs(w.worker_id)}','pause')">Pause</button><button onclick="bulkCommandForOne('${escJs(w.worker_id)}','stop')">Stop</button></td>
          <td>${logs}</td>
          <td><button onclick="openConfig('${escJs(w.worker_id)}')">Edit</button><br><span class="muted">${esc(w.config || "")}</span></td>
        </tr>`;
      }).join("");
    }
    async function refresh() {
      try { render(await apiGet("/api/coord/worker-control")); }
      catch (e) { document.getElementById("rows").innerHTML = `<tr><td colspan='8'>Failed to load worker state: ${esc(e.message)}</td></tr>`; }
    }
    document.getElementById("selectAll").addEventListener("change", (e) => { document.querySelectorAll("input.workerSel").forEach((el) => { el.checked = e.target.checked; }); });
    refresh();
    setInterval(refresh, 3000);
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
