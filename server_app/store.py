#!/usr/bin/env python3
"""Coordinator database access layer for the server."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg = None

DEFAULT_COORDINATOR_LEASE_SECONDS = 120


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_root_domain(hostname: str) -> str:
    host = str(hostname or "").strip().lower().strip(".")
    if not host:
        return ""
    parts = [p for p in host.split(".") if p]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


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
            except Exception as exc:
                print(f"[register_targets] skip line {line_no}: {text!r} -> {exc!r}")
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

    def claim_target(self, worker_id: str, lease_seconds: int) -> Optional[dict[str, Any]]:
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

    def load_session(self, root_domain: str) -> Optional[dict[str, Any]]:
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
        saved_at_utc: Optional[str] = None,
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
                cur.execute("SELECT status, COUNT(*) FROM coordinator_targets GROUP BY status;")
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

            seconds_since: Optional[int] = None
            last_heartbeat_iso: Optional[str] = None
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
            seconds_since: Optional[int] = None
            last_heartbeat_iso: Optional[str] = None
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

    def queue_worker_command(self, worker_id: str, command: str, payload: Optional[dict[str, Any]] = None) -> bool:
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

    def claim_stage(self, stage: str, worker_id: str, lease_seconds: int) -> Optional[dict[str, Any]]:
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

    def get_artifact(self, root_domain: str, artifact_type: str) -> Optional[dict[str, Any]]:
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