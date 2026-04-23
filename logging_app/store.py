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
    from psycopg_pool import ConnectionPool
except Exception:  # pragma: no cover - optional dependency in some test envs
    psycopg = None  # type: ignore[assignment]
    ConnectionPool = None  # type: ignore[assignment]


class LogStore:
    def __init__(self, database_url: str):
        self.database_url = str(database_url or "").strip()
        if not self.database_url:
            raise ValueError("database_url is required")
        if psycopg is None or ConnectionPool is None:
            raise RuntimeError("psycopg and psycopg_pool are required for LogStore")
        self.connect_timeout_seconds = self._resolve_connect_timeout_seconds()
        self._pool = self._build_pool()
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

    def _build_pool(self):
        min_size = max(1, int(os.getenv("LOG_DB_POOL_MIN_SIZE", "1") or "1"))
        max_size = max(min_size, int(os.getenv("LOG_DB_POOL_MAX_SIZE", "8") or "8"))
        return ConnectionPool(
            conninfo=self.database_url,
            min_size=min_size,
            max_size=max_size,
            open=True,
            kwargs={
                "autocommit": False,
                "connect_timeout": self.connect_timeout_seconds,
                "prepare_threshold": None,
            },
        )

    def _connect(self):
        return self._pool.connection()

    def close(self) -> None:
        try:
            self._pool.close()
        except Exception:
            pass

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
                str(entry.get("raw_line", "") or ""),
            ]
        )
        return hashlib.sha256(material.encode("utf-8", errors="ignore")).hexdigest()

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
        sql = """
INSERT INTO application_logs (
  event_time_utc, event_time_est, severity, description, machine, source_id, source_type,
  program_name, component_name, class_name, function_name, exception_type, stacktrace, metadata_json,
  raw_line, entry_hash
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
ON CONFLICT (entry_hash) DO NOTHING;
"""
        inserted = 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
                status = str(cur.statusmessage or "").split()
                inserted = int(status[-1]) if status and status[-1].isdigit() else 0
            conn.commit()
        return inserted

    def query_events(
        self,
        *,
        source_id: str = "",
        search: str = "",
        severity: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if source_id:
            filters.append("source_id = %s")
            params.append(str(source_id))
        if severity:
            filters.append("severity = %s")
            params.append(str(severity))
        if search:
            filters.append(
                "(description ILIKE %s OR raw_line ILIKE %s OR metadata_json::text ILIKE %s OR stacktrace ILIKE %s)"
            )
            needle = f"%{str(search)}%"
            params.extend([needle, needle, needle, needle])
        where = "WHERE " + " AND ".join(filters) if filters else ""
        params.extend([max(1, min(5000, int(limit))), max(0, int(offset))])
        sql = f"""
SELECT log_id, event_time_utc, event_time_est, severity, description, machine, source_id, source_type,
       program_name, component_name, class_name, function_name, exception_type, stacktrace, metadata_json,
       raw_line, entry_hash, created_at_utc
FROM application_logs
{where}
ORDER BY event_time_utc DESC, log_id DESC
LIMIT %s OFFSET %s;
"""
        out: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            out.append(
                {
                    "log_id": int(row[0]),
                    "event_time_utc": row[1].isoformat() if row[1] else None,
                    "event_time_est": row[2],
                    "severity": row[3],
                    "description": row[4],
                    "machine": row[5],
                    "source_id": row[6],
                    "source_type": row[7],
                    "program_name": row[8],
                    "component_name": row[9],
                    "class_name": row[10],
                    "function_name": row[11],
                    "exception_type": row[12],
                    "stacktrace": row[13],
                    "metadata_json": row[14] if isinstance(row[14], dict) else {},
                    "raw_line": row[15],
                    "entry_hash": row[16],
                    "created_at_utc": row[17].isoformat() if row[17] else None,
                }
            )
        return out

    def count_events(self, *, source_id: str = "", search: str = "", severity: str = "") -> int:
        filters = []
        params: list[Any] = []
        if source_id:
            filters.append("source_id = %s")
            params.append(str(source_id))
        if severity:
            filters.append("severity = %s")
            params.append(str(severity))
        if search:
            filters.append(
                "(description ILIKE %s OR raw_line ILIKE %s OR metadata_json::text ILIKE %s OR stacktrace ILIKE %s)"
            )
            needle = f"%{str(search)}%"
            params.extend([needle, needle, needle, needle])
        where = "WHERE " + " AND ".join(filters) if filters else ""
        sql = f"SELECT COUNT(*) FROM application_logs {where};"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
            conn.commit()
        return int(row[0] or 0) if row else 0
