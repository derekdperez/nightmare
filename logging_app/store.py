#!/usr/bin/env python3
"""Optional structured log event storage (separate Postgres backend)."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency in some test envs
    psycopg = None  # type: ignore[assignment]


class LogStore:
    def __init__(self, database_url: str):
        self.database_url = str(database_url or "").strip()
        if not self.database_url:
            raise ValueError("database_url is required")
        if psycopg is None:
            raise RuntimeError("psycopg is required for LogStore")
        self.connect_timeout_seconds = self._resolve_connect_timeout_seconds()
        self._ensure_schema()

    @staticmethod
    def _resolve_connect_timeout_seconds() -> int:
        raw = str(
            os.getenv(
                "LOG_DB_CONNECT_TIMEOUT_SECONDS",
                os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "8"),
            )
            or "8"
        ).strip()
        try:
            value = int(raw)
        except Exception:
            value = 8
        return max(1, min(60, value))

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(
            self.database_url,
            autocommit=False,
            connect_timeout=self.connect_timeout_seconds,
        )

    def _ensure_schema(self) -> None:
        ddl = """
CREATE TABLE IF NOT EXISTS application_logs (
  log_id BIGSERIAL PRIMARY KEY,
  event_time_utc TIMESTAMPTZ NOT NULL,
  event_time_est TEXT NOT NULL,
  severity TEXT NOT NULL,
  description TEXT NOT NULL,
  machine TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_type TEXT NOT NULL,
  program_name TEXT NOT NULL DEFAULT '',
  component_name TEXT NOT NULL DEFAULT '',
  class_name TEXT NOT NULL DEFAULT '',
  function_name TEXT NOT NULL DEFAULT '',
  exception_type TEXT NOT NULL DEFAULT '',
  stacktrace TEXT NOT NULL DEFAULT '',
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_line TEXT,
  entry_hash TEXT NOT NULL UNIQUE,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_application_logs_event_time ON application_logs(event_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_application_logs_severity ON application_logs(severity);
CREATE INDEX IF NOT EXISTS idx_application_logs_machine ON application_logs(machine);
CREATE INDEX IF NOT EXISTS idx_application_logs_source ON application_logs(source_id);
ALTER TABLE application_logs ADD COLUMN IF NOT EXISTS program_name TEXT NOT NULL DEFAULT '';
ALTER TABLE application_logs ADD COLUMN IF NOT EXISTS component_name TEXT NOT NULL DEFAULT '';
ALTER TABLE application_logs ADD COLUMN IF NOT EXISTS class_name TEXT NOT NULL DEFAULT '';
ALTER TABLE application_logs ADD COLUMN IF NOT EXISTS function_name TEXT NOT NULL DEFAULT '';
ALTER TABLE application_logs ADD COLUMN IF NOT EXISTS exception_type TEXT NOT NULL DEFAULT '';
ALTER TABLE application_logs ADD COLUMN IF NOT EXISTS stacktrace TEXT NOT NULL DEFAULT '';
ALTER TABLE application_logs ADD COLUMN IF NOT EXISTS metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb;
"""

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    @staticmethod
    def _entry_hash(entry: dict[str, Any]) -> str:
        material = "|".join(
            [
                str(entry.get("event_time_est", "") or ""),
                str(entry.get("severity", "") or ""),
                str(entry.get("description", "") or ""),
                str(entry.get("machine", "") or ""),
                str(entry.get("source_id", "") or ""),
                str(entry.get("source_type", "") or ""),
                str(entry.get("program_name", "") or ""),
                str(entry.get("component_name", "") or ""),
                str(entry.get("class_name", "") or ""),
                str(entry.get("function_name", "") or ""),
                str(entry.get("exception_type", "") or ""),
                str(entry.get("stacktrace", "") or ""),
                json.dumps(entry.get("metadata_json", {}) or {}, sort_keys=True, default=str),
                str(entry.get("raw_line", "") or ""),
            ]
        )
        return hashlib.sha256(material.encode("utf-8", errors="replace")).hexdigest()

    def insert_events(self, events: list[dict[str, Any]]) -> int:
        rows = []
        for item in events:
            if not isinstance(item, dict):
                continue
            event_time_utc = item.get("event_time_utc")
            if isinstance(event_time_utc, str):
                try:
                    event_time_utc = datetime.fromisoformat(event_time_utc.replace("Z", "+00:00"))
                except Exception:
                    event_time_utc = None
            if not isinstance(event_time_utc, datetime):
                event_time_utc = datetime.now(timezone.utc)
            rows.append(
                (
                    event_time_utc,
                    str(item.get("event_time_est", "") or ""),
                    str(item.get("severity", "info") or "info"),
                    str(item.get("description", "") or ""),
                    str(item.get("machine", "") or ""),
                    str(item.get("source_id", "") or ""),
                    str(item.get("source_type", "") or ""),
                    str(item.get("program_name", "") or ""),
                    str(item.get("component_name", "") or ""),
                    str(item.get("class_name", "") or ""),
                    str(item.get("function_name", "") or ""),
                    str(item.get("exception_type", "") or ""),
                    str(item.get("stacktrace", "") or ""),
                    json.dumps(item.get("metadata_json", {}) or {}, default=str),
                    str(item.get("raw_line", "") or ""),
                    self._entry_hash(item),
                )
            )
        if not rows:
            return 0
        inserted = 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        """
INSERT INTO application_logs (
  event_time_utc, event_time_est, severity, description, machine, source_id, source_type,
  program_name, component_name, class_name, function_name, exception_type, stacktrace, metadata_json,
  raw_line, entry_hash
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
ON CONFLICT (entry_hash) DO NOTHING;
""",
                        row,
                    )
                    inserted += int(cur.rowcount or 0)
            conn.commit()
        return inserted

    def query_events(
        self,
        *,
        source_id: str = "",
        search: str = "",
        severity: str = "",
        machine: str = "",
        program_name: str = "",
        component_name: str = "",
        exception_type: str = "",
        limit: int = 500,
        offset: int = 0,
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        clauses: list[str] = ["1=1"]
        params: list[Any] = []
        if source_id:
            clauses.append("source_id = %s")
            params.append(source_id)
        if severity:
            clauses.append("LOWER(severity) = %s")
            params.append(severity.lower())
        if machine:
            clauses.append("LOWER(machine) LIKE %s")
            params.append(f"%{machine.lower()}%")
        if program_name:
            clauses.append("LOWER(program_name) LIKE %s")
            params.append(f"%{program_name.lower()}%")
        if component_name:
            clauses.append("LOWER(component_name) LIKE %s")
            params.append(f"%{component_name.lower()}%")
        if exception_type:
            clauses.append("LOWER(exception_type) LIKE %s")
            params.append(f"%{exception_type.lower()}%")
        if search:
            clauses.append("(LOWER(description) LIKE %s OR LOWER(raw_line) LIKE %s OR LOWER(stacktrace) LIKE %s)")
            needle = f"%{search.lower()}%"
            params.extend([needle, needle, needle])
        where_sql = " AND ".join(clauses)
        order_sql = "DESC" if str(sort_dir or "").lower() != "asc" else "ASC"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM application_logs WHERE {where_sql};", tuple(params))
                total = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    f"""
SELECT event_time_utc, event_time_est, severity, description, machine, source_id, source_type,
       program_name, component_name, class_name, function_name, exception_type, stacktrace, metadata_json, raw_line
FROM application_logs
WHERE {where_sql}
ORDER BY event_time_utc {order_sql}
OFFSET %s LIMIT %s;
""",
                    tuple([*params, int(offset), int(limit)]),
                )
                rows = cur.fetchall()
        out = []
        for row in rows:
            out.append(
                {
                    "event_time_utc": row[0].isoformat() if isinstance(row[0], datetime) else str(row[0] or ""),
                    "event_time_est": str(row[1] or ""),
                    "severity": str(row[2] or "info"),
                    "description": str(row[3] or ""),
                    "machine": str(row[4] or ""),
                    "source_id": str(row[5] or ""),
                    "source_type": str(row[6] or ""),
                    "raw_line": str(row[7] or ""),
                }
            )
        return {"total": total, "offset": int(offset), "limit": int(limit), "events": out}



    def latest_events_by_source_ids(self, source_ids: list[str], *, limit_per_source: int = 1) -> dict[str, dict[str, Any]]:
        normalized = [str(item or "").strip() for item in list(source_ids or []) if str(item or "").strip()]
        if not normalized:
            return {}
        out: dict[str, dict[str, Any]] = {}
        sql = """
WITH ranked AS (
    SELECT
      event_time_utc,
      event_time_est,
      severity,
      description,
      machine,
      source_id,
      source_type,
      program_name,
      component_name,
      class_name,
      function_name,
      exception_type,
      stacktrace,
      metadata_json,
      raw_line,
      ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY event_time_utc DESC, log_id DESC) AS rn
    FROM application_logs
    WHERE source_id = ANY(%s)
)
SELECT
  event_time_utc,
  event_time_est,
  severity,
  description,
  machine,
  source_id,
  source_type,
  program_name,
  component_name,
  class_name,
  function_name,
  exception_type,
  stacktrace,
  metadata_json,
  raw_line
FROM ranked
WHERE rn <= %s
ORDER BY event_time_utc DESC;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (normalized, max(1, int(limit_per_source or 1))))
                rows = cur.fetchall()
        for row in rows:
            source_id = str(row[5] or "").strip()
            if not source_id or source_id in out:
                continue
            out[source_id] = {
                "event_time_utc": row[0].isoformat() if isinstance(row[0], datetime) else str(row[0] or ""),
                "event_time_est": str(row[1] or ""),
                "severity": str(row[2] or "info"),
                "description": str(row[3] or ""),
                "machine": str(row[4] or ""),
                "source_id": source_id,
                "source_type": str(row[6] or ""),
                "program_name": str(row[7] or ""),
                "component_name": str(row[8] or ""),
                "class_name": str(row[9] or ""),
                "function_name": str(row[10] or ""),
                "exception_type": str(row[11] or ""),
                "stacktrace": str(row[12] or ""),
                "metadata_json": row[13] if isinstance(row[13], dict) else {},
                "raw_line": str(row[14] or ""),
            }
        return out



    def latest_events_by_worker_ids(self, worker_ids: list[str], *, limit_per_worker: int = 1) -> dict[str, dict[str, Any]]:
        normalized = [str(item or "").strip() for item in list(worker_ids or []) if str(item or "").strip()]
        if not normalized:
            return {}
        alias_map: dict[str, set[str]] = {}
        all_aliases: set[str] = set()
        for worker_id in normalized:
            canonical = worker_id.strip().lower()
            aliases = {worker_id, canonical}
            if canonical.startswith("worker-"):
                aliases.add(canonical.replace("worker-", ""))
            alias_map[canonical] = {alias for alias in aliases if str(alias or "").strip()}
            all_aliases.update(alias_map[canonical])
        out: dict[str, dict[str, Any]] = {}
        sql = """
WITH ranked AS (
    SELECT
      event_time_utc,
      event_time_est,
      severity,
      description,
      machine,
      source_id,
      source_type,
      program_name,
      component_name,
      class_name,
      function_name,
      exception_type,
      stacktrace,
      metadata_json,
      raw_line,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(NULLIF(LOWER(metadata_json->>'worker_id'), ''), NULLIF(LOWER(source_id), ''), NULLIF(LOWER(machine), ''))
        ORDER BY event_time_utc DESC, log_id DESC
      ) AS rn
    FROM application_logs
    WHERE LOWER(source_id) = ANY(%s)
       OR LOWER(machine) = ANY(%s)
       OR LOWER(COALESCE(metadata_json->>'worker_id', '')) = ANY(%s)
       OR LOWER(COALESCE(metadata_json->>'source_id', '')) = ANY(%s)
)
SELECT
  event_time_utc,
  event_time_est,
  severity,
  description,
  machine,
  source_id,
  source_type,
  program_name,
  component_name,
  class_name,
  function_name,
  exception_type,
  stacktrace,
  metadata_json,
  raw_line
FROM ranked
WHERE rn <= %s
ORDER BY event_time_utc DESC;
"""
        alias_list = sorted({str(item).strip().lower() for item in all_aliases if str(item).strip()})
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (alias_list, alias_list, alias_list, alias_list, max(1, int(limit_per_worker or 1))))
                rows = cur.fetchall()
        for row in rows:
            metadata_json = row[13] if isinstance(row[13], dict) else {}
            worker_candidates = [
                str(metadata_json.get("worker_id") or "").strip(),
                str(row[5] or "").strip(),
                str(row[4] or "").strip(),
                str(metadata_json.get("source_id") or "").strip(),
            ]
            matched_worker_id = ""
            matched_sort_key = None
            for candidate in worker_candidates:
                if not candidate:
                    continue
                candidate_lower = candidate.lower()
                for worker_id in normalized:
                    worker_key = worker_id.strip().lower()
                    if candidate_lower in alias_map.get(worker_key, set()):
                        sort_key = str(row[0].isoformat() if isinstance(row[0], datetime) else str(row[0] or ""))
                        if matched_sort_key is None or sort_key > matched_sort_key:
                            matched_sort_key = sort_key
                            matched_worker_id = worker_id
            if not matched_worker_id:
                continue
            existing = out.get(matched_worker_id)
            current_time = row[0].isoformat() if isinstance(row[0], datetime) else str(row[0] or "")
            if existing is not None and str(existing.get("event_time_utc") or "") >= current_time:
                continue
            out[matched_worker_id] = {
                "event_time_utc": current_time,
                "event_time_est": str(row[1] or ""),
                "severity": str(row[2] or "info"),
                "description": str(row[3] or ""),
                "machine": str(row[4] or ""),
                "source_id": str(row[5] or ""),
                "source_type": str(row[6] or ""),
                "program_name": str(row[7] or ""),
                "component_name": str(row[8] or ""),
                "class_name": str(row[9] or ""),
                "function_name": str(row[10] or ""),
                "exception_type": str(row[11] or ""),
                "stacktrace": str(row[12] or ""),
                "metadata_json": metadata_json,
                "raw_line": str(row[14] or ""),
            }
        return out
    def query_error_events(
        self,
        *,
        search: str = "",
        machine: str = "",
        program_name: str = "",
        component_name: str = "",
        exception_type: str = "",
        limit: int = 500,
        offset: int = 0,
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        clauses = ["LOWER(severity) IN ('error', 'critical', 'fatal')"]
        params: list[Any] = []
        if machine:
            clauses.append("LOWER(machine) LIKE %s")
            params.append(f"%{machine.lower()}%")
        if program_name:
            clauses.append("LOWER(program_name) LIKE %s")
            params.append(f"%{program_name.lower()}%")
        if component_name:
            clauses.append("LOWER(component_name) LIKE %s")
            params.append(f"%{component_name.lower()}%")
        if exception_type:
            clauses.append("LOWER(exception_type) LIKE %s")
            params.append(f"%{exception_type.lower()}%")
        if search:
            needle = f"%{search.lower()}%"
            clauses.append("(LOWER(description) LIKE %s OR LOWER(raw_line) LIKE %s OR LOWER(stacktrace) LIKE %s)")
            params.extend([needle, needle, needle])
        where_sql = " AND ".join(clauses)
        order_sql = "DESC" if str(sort_dir or "").lower() != "asc" else "ASC"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM application_logs WHERE {where_sql};", tuple(params))
                total = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    f"""
SELECT event_time_utc, event_time_est, severity, description, machine, source_id, source_type,
       program_name, component_name, class_name, function_name, exception_type, stacktrace, metadata_json, raw_line
FROM application_logs
WHERE {where_sql}
ORDER BY event_time_utc {order_sql}
OFFSET %s LIMIT %s;
""",
                    tuple([*params, int(offset), int(limit)]),
                )
                rows = cur.fetchall()
        events = []
        for row in rows:
            events.append(
                {
                    "event_time_utc": row[0].isoformat() if isinstance(row[0], datetime) else str(row[0] or ""),
                    "event_time_est": row[1],
                    "severity": row[2],
                    "description": row[3],
                    "machine": row[4],
                    "source_id": row[5],
                    "source_type": row[6],
                    "program_name": row[7],
                    "component_name": row[8],
                    "class_name": row[9],
                    "function_name": row[10],
                    "exception_type": row[11],
                    "stacktrace": row[12],
                    "metadata_json": row[13] if isinstance(row[13], dict) else {},
                    "raw_line": row[14],
                }
            )
        return {"total": total, "events": events}
