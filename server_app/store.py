#!/usr/bin/env python3
"""Coordinator database access layer for the server."""

from __future__ import annotations

import hashlib
import json
import base64
import io
import os
import zipfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg = None

from nightmare_app.artifacts import FileSystemArtifactStore
from shared.events import EventStream, build_projection
from shared.models import EventRecord, RiskScorecard
from shared.versioning import registry

from nightmare_shared.page_classification import (
    PAGE_CLASS_API_ERROR,
    PAGE_CLASS_BLOCKED,
    PAGE_CLASS_EXISTS,
    PAGE_CLASS_LIKELY_SOFT_404,
    PAGE_CLASS_REDIRECT_PLACEHOLDER,
    PAGE_CLASS_UNKNOWN,
    PageFingerprint,
    classify_page as classify_page_fingerprint,
)

DEFAULT_COORDINATOR_LEASE_SECONDS = 120
DEFAULT_WORKER_RETENTION_SECONDS = 3600
MAX_AUDIT_TEXT_LEN = 4000

SUPPORTED_PAGE_CLASSIFICATIONS = {
    PAGE_CLASS_EXISTS,
    PAGE_CLASS_LIKELY_SOFT_404,
    PAGE_CLASS_UNKNOWN,
    PAGE_CLASS_REDIRECT_PLACEHOLDER,
    PAGE_CLASS_API_ERROR,
    PAGE_CLASS_BLOCKED,
}

SUPPORTED_SUPPRESSION_SCOPE_TYPES = {
    "single_finding",
    "exact_url",
    "url_pattern",
    "page_fingerprint",
    "rule_only",
    "rule_and_fingerprint",
    "host_prefix",
}

SUPPORTED_SUPPRESSION_REASON_TYPES = {
    "soft_404",
    "catch_all_router",
    "generic_login_redirect",
    "bad_regex",
    "overbroad_regex",
    "duplicate_finding",
    "irrelevant_path_pattern",
    "environment_specific_noise",
    "parser_error",
    "manual_other",
}

SUPPORTED_PRUNE_MODES = {"archive_then_delete", "soft_delete", "hard_delete"}
SUPPORTED_REPROCESS_MODES = {
    "future_only",
    "apply_to_existing_findings",
    "apply_to_existing_pages_and_findings",
    "apply_and_prune_existing_data",
}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe_db_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return {
            "_type": "bytes",
            "size": len(raw),
            "base64": base64.b64encode(raw).decode("ascii"),
        }
    if isinstance(value, dict):
        return {str(k): _json_safe_db_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_db_value(v) for v in value]
    iso = getattr(value, "isoformat", None)
    if callable(iso):
        try:
            return iso()
        except Exception:
            pass
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _build_database_preview_expr(column_name: str, data_type: str, text_limit: int) -> str:
    ident = _quote_ident(column_name)
    dtype = str(data_type or "").strip().lower()
    if dtype == "bytea":
        # Never inline raw binary payload in status API responses.
        return (
            f"CASE WHEN {ident} IS NULL THEN NULL "
            f"ELSE '<bytea ' || octet_length({ident})::text || ' bytes>' END AS {ident}"
        )
    if dtype in {"text", "character varying", "character", "json", "jsonb"}:
        safe_limit = max(64, int(text_limit or 4096))
        return (
            f"CASE WHEN {ident} IS NULL THEN NULL "
            f"WHEN length({ident}::text) <= {safe_limit} THEN {ident}::text "
            f"ELSE left({ident}::text, {safe_limit}) || ' ...[truncated]' END AS {ident}"
        )
    return ident


def _pick_recent_order_column(columns: list[tuple[str, str, str]]) -> Optional[str]:
    if not columns:
        return None
    names = [str(row[0] or "") for row in columns]
    preferred = [
        "updated_at_utc",
        "completed_at_utc",
        "heartbeat_at_utc",
        "started_at_utc",
        "created_at_utc",
        "updated_at",
        "created_at",
        "id",
        "line_number",
    ]
    lowered = {name.lower(): name for name in names}
    for candidate in preferred:
        if candidate in lowered:
            return lowered[candidate]
    return None


def _parse_summary_match_count(content: bytes, content_encoding: str) -> Optional[int]:
    data = bytes(content or b"")
    if not data:
        return None
    encoding = str(content_encoding or "identity").strip().lower()
    try:
        if encoding == "zip":
            with zipfile.ZipFile(io.BytesIO(data), mode="r") as zf:
                names = [name for name in zf.namelist() if name.lower().endswith(".json")]
                if not names:
                    return None
                payload = zf.read(names[0])
        else:
            payload = data
        parsed = json.loads(payload.decode("utf-8", errors="replace"))
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    if "match_count" in parsed:
        try:
            return max(0, int(parsed.get("match_count", 0) or 0))
        except Exception:
            return None
    rows = parsed.get("rows")
    if isinstance(rows, list):
        return len(rows)
    return None


def _parse_fozzy_summary_totals(content: bytes, content_encoding: str) -> dict[str, int]:
    data = bytes(content or b"")
    if not data:
        return {
            "groups": 0,
            "baseline_requests": 0,
            "fuzz_requests": 0,
            "anomalies": 0,
            "reflections": 0,
        }
    encoding = str(content_encoding or "identity").strip().lower()
    parsed: dict[str, Any] = {}
    try:
        payload = data
        if encoding == "zip":
            with zipfile.ZipFile(io.BytesIO(data), mode="r") as zf:
                names = [name for name in zf.namelist() if name.lower().endswith(".json")]
                if not names:
                    return {
                        "groups": 0,
                        "baseline_requests": 0,
                        "fuzz_requests": 0,
                        "anomalies": 0,
                        "reflections": 0,
                    }
                payload = zf.read(names[0])
        decoded = json.loads(payload.decode("utf-8", errors="replace"))
        if isinstance(decoded, dict):
            parsed = decoded
    except Exception:
        parsed = {}
    totals = parsed.get("totals") if isinstance(parsed.get("totals"), dict) else {}
    groups = parsed.get("groups") if isinstance(parsed.get("groups"), list) else []
    out = {
        "groups": max(0, int(totals.get("groups", len(groups)) or len(groups))),
        "baseline_requests": max(0, int(totals.get("baseline_requests", 0) or 0)),
        "fuzz_requests": max(0, int(totals.get("fuzz_requests", 0) or 0)),
        "anomalies": max(0, int(totals.get("anomalies", 0) or 0)),
        "reflections": max(0, int(totals.get("reflections", 0) or 0)),
    }
    return out


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
    def __init__(self, database_url: str, *, artifact_store_root: str | None = None):
        if psycopg is None:
            raise RuntimeError("psycopg is required for postgres coordinator mode")
        self.database_url = str(database_url or "").strip()
        if not self.database_url:
            raise ValueError("database_url is required for coordinator mode")
        self._connect_timeout_seconds = self._resolve_connect_timeout_seconds()
        artifact_root = str(
            artifact_store_root
            or os.getenv("NIGHTMARE_ARTIFACT_STORE_ROOT", "").strip()
            or (Path.cwd() / ".artifact_store")
        )
        self._artifact_store = FileSystemArtifactStore(
            artifact_root,
            compression_threshold_bytes=int(os.getenv("NIGHTMARE_ARTIFACT_COMPRESSION_THRESHOLD_BYTES", "1000000") or "1000000"),
            enable_compression=str(os.getenv("NIGHTMARE_ARTIFACT_COMPRESSION_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"},
        )
        self._db_inline_artifact_max_bytes = max(0, int(os.getenv("NIGHTMARE_DB_INLINE_ARTIFACT_MAX_BYTES", "262144") or "262144"))
        self._event_stream = EventStream(Path(artifact_root) / "events.ndjson")
        self._ensure_schema()

    @staticmethod
    def _resolve_connect_timeout_seconds() -> int:
        raw = str(
            os.getenv(
                "COORDINATOR_DB_CONNECT_TIMEOUT_SECONDS",
                os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "8"),
            )
            or "8"
        ).strip()
        try:
            value = int(raw)
        except Exception:
            value = 8
        return max(1, min(60, value))

    def _connect(self):
        return psycopg.connect(
            self.database_url,
            autocommit=False,
            connect_timeout=self._connect_timeout_seconds,
        )

    def _emit_event(self, event_type: str, aggregate_key: str, payload: dict[str, Any]) -> None:
        try:
            event = EventRecord(
                event_type=str(event_type or ""),
                aggregate_key=str(aggregate_key or ""),
                schema_version=registry.current_version("event_record"),
                payload=dict(payload or {}),
            )
            self._event_stream.append(event)
        except Exception:
            return

    def projection_snapshot(self) -> dict[str, dict[str, Any]]:
        return build_projection(self._event_stream.read())

    @staticmethod
    def _safe_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in dict(payload or {}).items():
            try:
                json.dumps(value)
                out[str(key)] = value
            except Exception:
                out[str(key)] = str(value)
        return out

    @staticmethod
    def _build_event_message(event_type: str, payload: dict[str, Any]) -> str:
        source = str(payload.get("source") or "").strip()
        worker_id = str(payload.get("worker_id") or "").strip()
        root_domain = str(payload.get("root_domain") or "").strip()
        stage = str(payload.get("stage") or "").strip()
        status = str(payload.get("status") or "").strip()
        command = str(payload.get("command") or "").strip()
        table = str(payload.get("table") or "").strip()
        artifact_type = str(payload.get("artifact_type") or "").strip()
        parts = [event_type]
        for item in (worker_id, root_domain, stage, command, artifact_type, table, status, source):
            if item:
                parts.append(item)
        return " · ".join(parts[:8])

    def record_system_event(
        self,
        event_type: str,
        aggregate_key: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        safe_payload = self._safe_event_payload(payload or {})
        safe_payload.setdefault("source", "coordinator_store")
        safe_payload.setdefault("message", self._build_event_message(event_type, safe_payload))
        self._emit_event(event_type, aggregate_key, safe_payload)

    def list_events(
        self,
        *,
        limit: int = 250,
        offset: int = 0,
        search: str = "",
        event_type: str = "",
        aggregate_key: str = "",
        source: str = "",
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        requested = max(1, min(5000, int(limit or 250)))
        skip = max(0, int(offset or 0))
        read_limit = min(20000, max(requested + skip, requested * 4, 1000))
        reverse = str(sort_dir or "desc").strip().lower() != "asc"
        rows = self._event_stream.read(limit=read_limit, reverse=reverse)
        needle = str(search or "").strip().lower()
        event_type_q = str(event_type or "").strip().lower()
        aggregate_q = str(aggregate_key or "").strip().lower()
        source_q = str(source or "").strip().lower()
        filtered: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            payload = item.get("payload")
            payload_dict = payload if isinstance(payload, dict) else {}
            item_source = str(payload_dict.get("source") or "").strip()
            item_message = str(payload_dict.get("message") or "").strip()
            normalized = {
                "event_id": str(item.get("event_id") or ""),
                "created_at": str(item.get("created_at") or ""),
                "event_type": str(item.get("event_type") or ""),
                "aggregate_key": str(item.get("aggregate_key") or ""),
                "schema_version": int(item.get("schema_version") or 1),
                "source": item_source,
                "message": item_message,
                "payload": payload_dict,
            }
            if event_type_q and event_type_q not in normalized["event_type"].lower():
                continue
            if aggregate_q and aggregate_q not in normalized["aggregate_key"].lower():
                continue
            if source_q and source_q not in normalized["source"].lower():
                continue
            if needle:
                haystack = " ".join(
                    [
                        normalized["created_at"],
                        normalized["event_type"],
                        normalized["aggregate_key"],
                        normalized["source"],
                        normalized["message"],
                        json.dumps(payload_dict, sort_keys=True, default=str),
                    ]
                ).lower()
                if needle not in haystack:
                    continue
            filtered.append(normalized)
        total = len(filtered)
        page = filtered[skip : skip + requested]
        return {
            "generated_at_utc": _iso_now(),
            "total": total,
            "offset": skip,
            "limit": requested,
            "events": page,
        }

    @staticmethod
    def _risk_score_for_artifact(artifact_type: str, size_bytes: int) -> float:
        lowered = str(artifact_type or "").lower()
        score = 0.0
        if any(token in lowered for token in ("critical", "high_risk", "secret", "credential")):
            score += 80.0
        elif any(token in lowered for token in ("finding", "match", "anomaly")):
            score += 35.0
        score += min(20.0, max(0.0, float(size_bytes or 0) / 500_000.0))
        return round(score, 2)

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
  workflow_id TEXT NOT NULL DEFAULT 'default',
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
  checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  progress_artifact_type TEXT NOT NULL DEFAULT '',
  resume_mode TEXT NOT NULL DEFAULT 'exact',
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(workflow_id, root_domain, stage)
);
CREATE INDEX IF NOT EXISTS idx_stage_tasks_status_stage ON coordinator_stage_tasks(stage, status);
CREATE INDEX IF NOT EXISTS idx_stage_tasks_lease ON coordinator_stage_tasks(lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_stage_tasks_domain_status ON coordinator_stage_tasks(root_domain, status, lease_expires_at);

CREATE TABLE IF NOT EXISTS coordinator_artifacts (
  root_domain TEXT NOT NULL,
  artifact_type TEXT NOT NULL,
  source_worker TEXT,
  content BYTEA NOT NULL DEFAULT '',
  content_encoding TEXT NOT NULL DEFAULT 'identity',
  content_sha256 TEXT NOT NULL,
  content_size_bytes BIGINT NOT NULL,
  storage_backend TEXT NOT NULL DEFAULT 'database_inline',
  storage_uri TEXT NOT NULL DEFAULT '',
  media_type TEXT NOT NULL DEFAULT 'application/octet-stream',
  compression TEXT NOT NULL DEFAULT 'identity',
  schema_version INTEGER NOT NULL DEFAULT 1,
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

CREATE TABLE IF NOT EXISTS coordinator_ui_preferences (
  page TEXT NOT NULL,
  pref_key TEXT NOT NULL,
  pref_value JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(page, pref_key)
);
CREATE INDEX IF NOT EXISTS idx_coordinator_ui_preferences_page
  ON coordinator_ui_preferences(page);

CREATE TABLE IF NOT EXISTS coordinator_worker_commands (
  id BIGSERIAL PRIMARY KEY,
  worker_id TEXT NOT NULL,
  command TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'queued',
  result_error TEXT,
  completed_at_utc TIMESTAMPTZ,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_worker_commands_worker_created
  ON coordinator_worker_commands(worker_id, created_at_utc DESC);
ALTER TABLE coordinator_worker_commands ADD COLUMN IF NOT EXISTS result_error TEXT;
ALTER TABLE coordinator_worker_commands ADD COLUMN IF NOT EXISTS completed_at_utc TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS coordinator_worker_presence (
  worker_id TEXT PRIMARY KEY,
  last_seen_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_activity TEXT NOT NULL DEFAULT 'unknown',
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_worker_presence_last_seen
  ON coordinator_worker_presence(last_seen_at_utc DESC);
"""
        migration_statements = [
            "ALTER TABLE coordinator_artifacts ALTER COLUMN content DROP NOT NULL",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS storage_backend TEXT NOT NULL DEFAULT 'database_inline'",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS storage_uri TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS media_type TEXT NOT NULL DEFAULT 'application/octet-stream'",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS compression TEXT NOT NULL DEFAULT 'identity'",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS workflow_id TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS progress_json JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS progress_artifact_type TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS resume_mode TEXT NOT NULL DEFAULT 'exact'",
            """
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'coordinator_stage_tasks_pkey'
  ) THEN
    ALTER TABLE coordinator_stage_tasks DROP CONSTRAINT coordinator_stage_tasks_pkey;
  END IF;
EXCEPTION WHEN undefined_table THEN
  NULL;
END $$;
""",
            "ALTER TABLE coordinator_stage_tasks ADD CONSTRAINT coordinator_stage_tasks_pkey PRIMARY KEY(workflow_id, root_domain, stage)",
            "DROP INDEX IF EXISTS idx_stage_tasks_status_stage",
            "CREATE INDEX IF NOT EXISTS idx_stage_tasks_status_stage ON coordinator_stage_tasks(workflow_id, stage, status)",
            "CREATE INDEX IF NOT EXISTS idx_stage_tasks_domain_status ON coordinator_stage_tasks(root_domain, status, lease_expires_at)",
            "CREATE TABLE IF NOT EXISTS coordinator_summary_latest (root_domain TEXT NOT NULL, stage_name TEXT NOT NULL, summary_json JSONB NOT NULL DEFAULT '{}'::jsonb, updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(root_domain, stage_name))",
            "CREATE TABLE IF NOT EXISTS coordinator_projection_state (aggregate_key TEXT PRIMARY KEY, projection_json JSONB NOT NULL DEFAULT '{}'::jsonb, updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW())",
        ]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
            for sql in migration_statements:
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                    conn.commit()
                except Exception:
                    conn.rollback()

    @staticmethod
    def _touch_worker_presence(cur: Any, worker_id: str, activity: str) -> None:
        wid = str(worker_id or "").strip()
        if not wid:
            return
        act = str(activity or "").strip().lower() or "unknown"
        sql = """
INSERT INTO coordinator_worker_presence(worker_id, last_seen_at_utc, last_activity, updated_at_utc)
VALUES (%s, NOW(), %s, NOW())
ON CONFLICT (worker_id) DO UPDATE
SET last_seen_at_utc = NOW(),
    last_activity = EXCLUDED.last_activity,
    updated_at_utc = NOW();
"""
        cur.execute(sql, (wid, act[:64]))

    def register_targets(self, targets: list[str], *, replace_existing: bool = False) -> dict[str, Any]:
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
        if not rows and not bool(replace_existing):
            return {"inserted": 0, "skipped": skipped, "replaced_existing": False}
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
                if bool(replace_existing):
                    cur.execute("TRUNCATE TABLE coordinator_targets;")
                if rows:
                    cur.executemany(upsert_sql, rows)
            conn.commit()
        inserted = len(rows)
        self.record_system_event(
            "target.batch_registered",
            "targets",
            {
                "source": "coordinator_store.register_targets",
                "inserted": inserted,
                "skipped": skipped,
                "replace_existing": bool(replace_existing),
                "target_count": len(rows),
                "sample_targets": [row[4] for row in rows[:10]],
                "table": "coordinator_targets",
            },
        )
        return {"inserted": inserted, "skipped": skipped, "replaced_existing": bool(replace_existing)}

    def claim_target(self, worker_id: str, lease_seconds: int) -> Optional[dict[str, Any]]:
        worker = str(worker_id or "").strip()
        if not worker:
            raise ValueError("worker_id is required")
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        sql = """
WITH candidate AS (
    SELECT entry_id
    FROM coordinator_targets ct
    WHERE (
        ct.status = 'pending'
        OR (ct.status = 'running' AND ct.lease_expires_at IS NOT NULL AND ct.lease_expires_at < NOW())
    )
      AND NOT EXISTS (
          SELECT 1
          FROM coordinator_stage_tasks s
          WHERE s.root_domain = ct.root_domain
            AND s.status = 'running'
            AND s.lease_expires_at IS NOT NULL
            AND s.lease_expires_at >= NOW()
      )
    ORDER BY ct.line_number ASC, ct.created_at_utc ASC
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
                self._touch_worker_presence(cur, worker, "claim_target")
                cur.execute(sql, (worker, lease))
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        self.record_system_event(
            "target.claimed",
            f"target:{row[4]}",
            {
                "source": "coordinator_store.claim_target",
                "entry_id": row[0],
                "line_number": int(row[1] or 0),
                "raw": row[2],
                "start_url": row[3],
                "root_domain": row[4],
                "attempt_count": int(row[5] or 0),
                "status": row[6],
                "worker_id": row[7],
                "lease_expires_at": row[8].isoformat() if row[8] else None,
            },
        )
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
                self._touch_worker_presence(cur, str(worker_id), "heartbeat_target")
                cur.execute(sql, (lease, str(entry_id), str(worker_id)))
                updated = int(cur.rowcount or 0)
            conn.commit()
        if updated > 0:
            self.record_system_event(
                "target.heartbeat",
                f"target:{str(entry_id)}",
                {
                    "source": "coordinator_store.heartbeat",
                    "entry_id": str(entry_id),
                    "worker_id": str(worker_id),
                    "lease_seconds": lease,
                    "status": "running",
                },
            )
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
                self._touch_worker_presence(cur, str(worker_id), "complete_target")
                cur.execute(sql, (status, int(exit_code), str(error or "")[:2000], str(entry_id), str(worker_id)))
                updated = int(cur.rowcount or 0)
            conn.commit()
        if updated > 0:
            self.record_system_event(
                f"target.{status}",
                f"target:{str(entry_id)}",
                {
                    "source": "coordinator_store.finish",
                    "entry_id": str(entry_id),
                    "worker_id": str(worker_id),
                    "status": status,
                    "exit_code": int(exit_code),
                    "error": str(error or "")[:2000],
                },
            )
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
        self.record_system_event(
            "session.saved",
            f"session:{rd}",
            {
                "source": "coordinator_store.save_session",
                "root_domain": rd,
                "start_url": su,
                "max_pages": int(max_pages) if max_pages is not None else None,
                "saved_at_utc": saved_dt.isoformat() if saved_dt else None,
                "table": "coordinator_sessions",
            },
        )
        return True

    def reset_coordinator_tables(self) -> dict[str, Any]:
        truncate_sql = """
TRUNCATE TABLE coordinator_targets, coordinator_sessions, coordinator_stage_tasks, coordinator_artifacts;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(truncate_sql)
            conn.commit()
        self.record_system_event(
            "coordinator.tables_reset",
            "coordinator",
            {
                "source": "coordinator_store.reset_coordinator_tables",
                "status": "completed",
                "tables": [
                    "coordinator_targets",
                    "coordinator_sessions",
                    "coordinator_stage_tasks",
                    "coordinator_artifacts",
                ],
            },
        )
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

    def get_ui_preference(self, *, page: str, pref_key: str) -> dict[str, Any]:
        page_text = str(page or "").strip().lower()
        key_text = str(pref_key or "").strip().lower()
        if not page_text:
            raise ValueError("page is required")
        if not key_text:
            raise ValueError("pref_key is required")
        sql = """
SELECT pref_value, updated_at_utc
FROM coordinator_ui_preferences
WHERE page = %s AND pref_key = %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (page_text, key_text))
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return {
                "page": page_text,
                "pref_key": key_text,
                "found": False,
                "pref_value": {},
                "updated_at_utc": None,
            }
        value = row[0] if isinstance(row[0], dict) else {}
        updated = row[1]
        return {
            "page": page_text,
            "pref_key": key_text,
            "found": True,
            "pref_value": value,
            "updated_at_utc": updated.isoformat() if updated else None,
        }

    def set_ui_preference(self, *, page: str, pref_key: str, pref_value: dict[str, Any]) -> dict[str, Any]:
        page_text = str(page or "").strip().lower()
        key_text = str(pref_key or "").strip().lower()
        if not page_text:
            raise ValueError("page is required")
        if not key_text:
            raise ValueError("pref_key is required")
        if not isinstance(pref_value, dict):
            raise ValueError("pref_value must be an object")
        sql = """
INSERT INTO coordinator_ui_preferences(page, pref_key, pref_value, updated_at_utc)
VALUES (%s, %s, %s::jsonb, NOW())
ON CONFLICT (page, pref_key) DO UPDATE
SET pref_value = EXCLUDED.pref_value,
    updated_at_utc = NOW()
RETURNING updated_at_utc;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (page_text, key_text, json.dumps(pref_value, ensure_ascii=False)))
                row = cur.fetchone()
            conn.commit()
        updated = row[0] if row else None
        return {
            "page": page_text,
            "pref_key": key_text,
            "pref_value": pref_value,
            "updated_at_utc": updated.isoformat() if updated else None,
        }

    def database_status(self) -> dict[str, Any]:
        max_rows_per_table = 20
        max_text_preview_chars = 4096
        tables_sql = """
SELECT
  n.nspname AS table_schema,
  c.relname AS table_name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname;
"""
        columns_sql = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
ORDER BY ordinal_position;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user, version(), NOW();")
                db_row = cur.fetchone()
                cur.execute(tables_sql)
                table_rows = cur.fetchall()
                tables: list[dict[str, Any]] = []
                for schema_name, table_name in table_rows:
                    safe_ident = f'{_quote_ident(schema_name)}.{_quote_ident(table_name)}'
                    try:
                        cur.execute(columns_sql, (schema_name, table_name))
                        column_rows = cur.fetchall()
                        columns = [
                            {
                                "name": col_name,
                                "data_type": data_type,
                                "nullable": str(is_nullable).upper() == "YES",
                            }
                            for col_name, data_type, is_nullable in column_rows
                        ]
                        select_exprs = [
                            _build_database_preview_expr(str(col_name), str(data_type), max_text_preview_chars)
                            for col_name, data_type, _is_nullable in column_rows
                        ]
                        select_list = ", ".join(select_exprs) if select_exprs else "NULL"
                        order_col = _pick_recent_order_column(column_rows)
                        order_clause = f" ORDER BY {_quote_ident(order_col)} DESC" if order_col else ""
                        cur.execute(f"SELECT COUNT(*) FROM {safe_ident};")
                        row_count_value = int((cur.fetchone() or [0])[0] or 0)
                        cur.execute(
                            f"SELECT {select_list} FROM {safe_ident}{order_clause} LIMIT %s;",
                            (max_rows_per_table,),
                        )
                        rows = cur.fetchall()
                        colnames = [desc[0] for desc in cur.description]
                        serialized_rows: list[dict[str, Any]] = []
                        for row in rows:
                            serialized_rows.append({
                                colnames[idx]: _json_safe_db_value(value)
                                for idx, value in enumerate(row)
                            })
                        tables.append(
                            {
                                "schema": schema_name,
                                "name": table_name,
                                "row_count": row_count_value,
                                "row_count_is_estimate": False,
                                "rows_returned": len(serialized_rows),
                                "rows_limited": row_count_value > len(serialized_rows),
                                "columns": columns,
                                "rows": serialized_rows,
                            }
                        )
                    except Exception as exc:
                        tables.append(
                            {
                                "schema": schema_name,
                                "name": table_name,
                                "row_count": 0,
                                "row_count_is_estimate": False,
                                "rows_returned": 0,
                                "rows_limited": False,
                                "columns": [],
                                "rows": [],
                                "table_error": str(exc),
                            }
                        )
            conn.commit()
        return {
            "database": {
                "name": db_row[0],
                "current_user": db_row[1],
                "version": db_row[2],
                "server_time_utc": _json_safe_db_value(db_row[3]),
            },
            "table_count": len(tables),
            "max_rows_per_table": max_rows_per_table,
            "max_text_preview_chars": max_text_preview_chars,
            "tables": tables,
            "generated_at_utc": _iso_now(),
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

    def workflow_scheduler_snapshot(self, *, limit: int = 2000) -> dict[str, Any]:
        safe_limit = max(1, min(5000, int(limit or 2000)))
        domains_sql = """
SELECT root_domain
FROM (
    SELECT DISTINCT root_domain FROM coordinator_targets
    UNION
    SELECT DISTINCT root_domain FROM coordinator_stage_tasks
    UNION
    SELECT DISTINCT root_domain FROM coordinator_artifacts
) d
WHERE root_domain IS NOT NULL AND root_domain <> ''
ORDER BY root_domain ASC
LIMIT %s;
"""
        domains: list[str] = []
        stage_rows: list[Any] = []
        artifact_rows: list[Any] = []
        target_rows: list[Any] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(domains_sql, (safe_limit,))
                domains = [str(row[0] or "").strip().lower() for row in cur.fetchall() if str(row[0] or "").strip()]
                if domains:
                    cur.execute(
                        """
SELECT workflow_id, root_domain, stage, status, attempt_count, exit_code, error, updated_at_utc, completed_at_utc,
       checkpoint_json, progress_json, progress_artifact_type, resume_mode, worker_id
FROM coordinator_stage_tasks
WHERE root_domain = ANY(%s)
ORDER BY workflow_id ASC, root_domain ASC, stage ASC;
""",
                        (domains,),
                    )
                    stage_rows = cur.fetchall()
                    cur.execute(
                        """
SELECT root_domain, artifact_type, updated_at_utc
FROM coordinator_artifacts
WHERE root_domain = ANY(%s)
ORDER BY root_domain ASC, artifact_type ASC;
""",
                        (domains,),
                    )
                    artifact_rows = cur.fetchall()
                    cur.execute(
                        """
SELECT
  root_domain,
  COUNT(*) FILTER (WHERE status = 'pending') AS pending_targets,
  COUNT(*) FILTER (WHERE status = 'running') AS running_targets,
  COUNT(*) FILTER (WHERE status = 'completed') AS completed_targets,
  COUNT(*) FILTER (WHERE status = 'failed') AS failed_targets
FROM coordinator_targets
WHERE root_domain = ANY(%s)
GROUP BY root_domain
ORDER BY root_domain ASC;
""",
                        (domains,),
                    )
                    target_rows = cur.fetchall()
            conn.commit()

        stage_map: dict[str, dict[str, dict[str, Any]]] = {}
        legacy_stage_map: dict[str, dict[str, Any]] = {}
        for row in stage_rows:
            workflow_id = str(row[0] or "default").strip().lower() or "default"
            rd = str(row[1] or "").strip().lower()
            stg = str(row[2] or "").strip().lower()
            if not rd or not stg:
                continue
            row_payload = {
                "workflow_id": workflow_id,
                "stage": stg,
                "plugin_name": stg,
                "status": str(row[3] or "").strip().lower(),
                "attempt_count": int(row[4] or 0),
                "exit_code": (int(row[5]) if row[5] is not None else None),
                "error": str(row[6] or ""),
                "updated_at_utc": row[7].isoformat() if row[7] else None,
                "completed_at_utc": row[8].isoformat() if row[8] else None,
                "checkpoint": row[9] if isinstance(row[9], dict) else {},
                "progress": row[10] if isinstance(row[10], dict) else {},
                "progress_artifact_type": str(row[11] or ""),
                "resume_mode": str(row[12] or "exact"),
                "worker_id": str(row[13] or ""),
            }
            stage_map.setdefault(rd, {}).setdefault(workflow_id, {})[stg] = row_payload
            if workflow_id == "default":
                legacy_stage_map.setdefault(rd, {})[stg] = row_payload

        artifact_map: dict[str, list[dict[str, Any]]] = {}
        for row in artifact_rows:
            rd = str(row[0] or "").strip().lower()
            artifact_type = str(row[1] or "").strip().lower()
            if not rd or not artifact_type:
                continue
            artifact_map.setdefault(rd, []).append(
                {
                    "artifact_type": artifact_type,
                    "updated_at_utc": row[2].isoformat() if row[2] else None,
                }
            )

        target_map: dict[str, dict[str, int]] = {}
        for row in target_rows:
            rd = str(row[0] or "").strip().lower()
            if not rd:
                continue
            target_map[rd] = {
                "pending": int(row[1] or 0),
                "running": int(row[2] or 0),
                "completed": int(row[3] or 0),
                "failed": int(row[4] or 0),
            }

        domain_rows: list[dict[str, Any]] = []
        for rd in domains:
            artifacts = artifact_map.get(rd, [])
            domain_rows.append(
                {
                    "root_domain": rd,
                    "targets": target_map.get(rd, {"pending": 0, "running": 0, "completed": 0, "failed": 0}),
                    "plugin_tasks": stage_map.get(rd, {}),
                    "stage_tasks": legacy_stage_map.get(rd, {}),
                    "artifact_types": [item["artifact_type"] for item in artifacts],
                    "artifacts": artifacts,
                }
            )

        return {
            "generated_at_utc": _iso_now(),
            "limit": safe_limit,
            "domain_count": len(domain_rows),
            "domains": domain_rows,
        }

    def crawl_progress_snapshot(self, *, limit: int = 2000) -> dict[str, Any]:
        safe_limit = max(1, min(2000, int(limit or 2000)))
        sql = """
WITH domain_set AS (
    SELECT DISTINCT root_domain FROM coordinator_targets
    UNION
    SELECT DISTINCT root_domain FROM coordinator_stage_tasks
    UNION
    SELECT DISTINCT root_domain FROM coordinator_sessions
),
target_agg AS (
    SELECT
      root_domain,
      COUNT(*) FILTER (WHERE status = 'pending') AS pending_targets,
      COUNT(*) FILTER (WHERE status = 'running') AS running_targets,
      COUNT(*) FILTER (WHERE status = 'completed') AS completed_targets,
      COUNT(*) FILTER (WHERE status = 'failed') AS failed_targets,
      MAX(heartbeat_at_utc) AS last_target_heartbeat,
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT CASE WHEN status = 'running' THEN worker_id ELSE NULL END), NULL) AS target_workers,
      MIN(start_url) FILTER (WHERE start_url IS NOT NULL AND start_url <> '') AS sample_start_url
    FROM coordinator_targets
    GROUP BY root_domain
),
stage_agg AS (
    SELECT
      root_domain,
      COUNT(*) FILTER (WHERE status = 'pending') AS pending_stage_tasks,
      COUNT(*) FILTER (WHERE status = 'running') AS running_stage_tasks,
      COUNT(*) FILTER (WHERE status = 'completed') AS completed_stage_tasks,
      COUNT(*) FILTER (WHERE status = 'failed') AS failed_stage_tasks,
      MAX(heartbeat_at_utc) AS last_stage_heartbeat,
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT CASE WHEN status = 'running' THEN stage ELSE NULL END), NULL) AS active_stages,
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT CASE WHEN status = 'running' THEN worker_id ELSE NULL END), NULL) AS stage_workers
    FROM coordinator_stage_tasks
    GROUP BY root_domain
)
SELECT
  d.root_domain,
  COALESCE(t.sample_start_url, sess.start_url, '') AS start_url,
  COALESCE(
    CASE
      WHEN jsonb_typeof(sess.payload #> '{state,discovered_urls}') = 'array'
      THEN jsonb_array_length(sess.payload #> '{state,discovered_urls}')
      ELSE 0
    END,
    0
  ) AS discovered_urls_count,
  COALESCE(
    CASE
      WHEN jsonb_typeof(sess.payload #> '{state,visited_urls}') = 'array'
      THEN jsonb_array_length(sess.payload #> '{state,visited_urls}')
      ELSE 0
    END,
    0
  ) AS visited_urls_count,
  COALESCE(
    CASE
      WHEN jsonb_typeof(sess.payload #> '{frontier}') = 'array'
      THEN jsonb_array_length(sess.payload #> '{frontier}')
      ELSE 0
    END,
    0
  ) AS frontier_count,
  sess.saved_at_utc,
  COALESCE(
    GREATEST(t.last_target_heartbeat, st.last_stage_heartbeat, sess.saved_at_utc),
    GREATEST(t.last_target_heartbeat, st.last_stage_heartbeat),
    GREATEST(t.last_target_heartbeat, sess.saved_at_utc),
    GREATEST(st.last_stage_heartbeat, sess.saved_at_utc),
    t.last_target_heartbeat,
    st.last_stage_heartbeat,
    sess.saved_at_utc
  ) AS last_activity_at_utc,
  COALESCE(t.pending_targets, 0) AS pending_targets,
  COALESCE(t.running_targets, 0) AS running_targets,
  COALESCE(t.completed_targets, 0) AS completed_targets,
  COALESCE(t.failed_targets, 0) AS failed_targets,
  COALESCE(st.pending_stage_tasks, 0) AS pending_stage_tasks,
  COALESCE(st.running_stage_tasks, 0) AS running_stage_tasks,
  COALESCE(st.completed_stage_tasks, 0) AS completed_stage_tasks,
  COALESCE(st.failed_stage_tasks, 0) AS failed_stage_tasks,
  COALESCE(st.active_stages, ARRAY[]::text[]) AS active_stages,
  COALESCE(t.target_workers, ARRAY[]::text[]) AS target_workers,
  COALESCE(st.stage_workers, ARRAY[]::text[]) AS stage_workers
FROM domain_set d
LEFT JOIN target_agg t ON t.root_domain = d.root_domain
LEFT JOIN stage_agg st ON st.root_domain = d.root_domain
LEFT JOIN coordinator_sessions sess ON sess.root_domain = d.root_domain
ORDER BY last_activity_at_utc DESC NULLS LAST, d.root_domain ASC
LIMIT %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (safe_limit,))
                rows = cur.fetchall()
            conn.commit()

        now_utc = datetime.now(timezone.utc)
        domains: list[dict[str, Any]] = []
        running_domains = 0
        queued_domains = 0
        failed_domains = 0
        completed_domains = 0

        for row in rows:
            root_domain = str(row[0] or "").strip().lower()
            if not root_domain:
                continue
            active_stages_raw = row[15] if isinstance(row[15], list) else []
            active_stages = [str(item) for item in active_stages_raw if str(item or "").strip()]
            target_workers_raw = row[16] if isinstance(row[16], list) else []
            stage_workers_raw = row[17] if isinstance(row[17], list) else []
            active_workers = sorted(
                {
                    str(item).strip()
                    for item in [*target_workers_raw, *stage_workers_raw]
                    if str(item or "").strip()
                }
            )
            last_activity = row[6]
            last_activity_iso: Optional[str] = None
            seconds_since_activity: Optional[int] = None
            if last_activity is not None:
                last_activity_iso = last_activity.isoformat()
                seconds_since_activity = max(0, int((now_utc - last_activity).total_seconds()))

            pending_targets = int(row[7] or 0)
            running_targets = int(row[8] or 0)
            completed_targets = int(row[9] or 0)
            failed_targets = int(row[10] or 0)
            pending_stage_tasks = int(row[11] or 0)
            running_stage_tasks = int(row[12] or 0)
            completed_stage_tasks = int(row[13] or 0)
            failed_stage_tasks = int(row[14] or 0)

            phase = "idle"
            if running_targets > 0:
                phase = "nightmare_running"
            elif running_stage_tasks > 0:
                if any(str(item).startswith("nightmare_") for item in active_stages):
                    phase = "nightmare_plugin_running"
                elif "fozzy" in active_stages:
                    phase = "fozzy_running"
                elif "extractor" in active_stages:
                    phase = "extractor_running"
                else:
                    phase = "stage_running"
            elif pending_targets > 0:
                phase = "nightmare_pending"
            elif pending_stage_tasks > 0:
                if any(str(item).startswith("nightmare_") for item in active_stages):
                    phase = "nightmare_plugin_pending"
                else:
                    phase = "stage_pending"
            elif failed_targets > 0 or failed_stage_tasks > 0:
                phase = "failed"
            elif completed_targets > 0 or completed_stage_tasks > 0:
                phase = "completed"

            if phase.endswith("_running"):
                running_domains += 1
            elif phase.endswith("_pending"):
                queued_domains += 1
            elif phase == "failed":
                failed_domains += 1
            elif phase == "completed":
                completed_domains += 1

            domains.append(
                {
                    "root_domain": root_domain,
                    "start_url": str(row[1] or ""),
                    "phase": phase,
                    "discovered_urls_count": int(row[2] or 0),
                    "visited_urls_count": int(row[3] or 0),
                    "frontier_count": int(row[4] or 0),
                    "session_saved_at_utc": row[5].isoformat() if row[5] else None,
                    "last_activity_at_utc": last_activity_iso,
                    "seconds_since_activity": seconds_since_activity,
                    "pending_targets": pending_targets,
                    "running_targets": running_targets,
                    "completed_targets": completed_targets,
                    "failed_targets": failed_targets,
                    "pending_stage_tasks": pending_stage_tasks,
                    "running_stage_tasks": running_stage_tasks,
                    "completed_stage_tasks": completed_stage_tasks,
                    "failed_stage_tasks": failed_stage_tasks,
                    "active_stages": active_stages,
                    "active_workers": active_workers,
                }
            )

        return {
            "generated_at_utc": now_utc.isoformat(),
            "limit": safe_limit,
            "counts": {
                "total_domains": len(domains),
                "running_domains": running_domains,
                "queued_domains": queued_domains,
                "failed_domains": failed_domains,
                "completed_domains": completed_domains,
            },
            "domains": domains,
        }

    def auth0r_overview(self, *, completed_only: bool = False, limit: int = 5000) -> dict[str, Any]:
        safe_limit = max(1, min(5000, int(limit or 5000)))
        order_by_clause = (
            "root_domain ASC"
            if bool(completed_only)
            else "COALESCE(saved_at_utc, NOW()) DESC, root_domain ASC"
        )
        sql = """
WITH target_agg AS (
    SELECT
      root_domain,
      MIN(start_url) FILTER (WHERE start_url IS NOT NULL AND start_url <> '') AS start_url,
      COUNT(*) FILTER (WHERE status = 'pending') AS pending_targets,
      COUNT(*) FILTER (WHERE status = 'running') AS running_targets,
      COUNT(*) FILTER (WHERE status = 'completed') AS completed_targets,
      COUNT(*) FILTER (WHERE status = 'failed') AS failed_targets
    FROM coordinator_targets
    GROUP BY root_domain
),
domain_rows AS (
    SELECT
      sess.root_domain,
      COALESCE(t.start_url, sess.start_url, '') AS start_url,
      COALESCE(
        CASE
          WHEN jsonb_typeof(sess.payload #> '{state,discovered_urls}') = 'array'
          THEN jsonb_array_length(sess.payload #> '{state,discovered_urls}')
          ELSE 0
        END,
        0
      ) AS discovered_urls_count,
      COALESCE(t.pending_targets, 0) AS pending_targets,
      COALESCE(t.running_targets, 0) AS running_targets,
      COALESCE(t.completed_targets, 0) AS completed_targets,
      COALESCE(t.failed_targets, 0) AS failed_targets,
      sess.saved_at_utc
    FROM coordinator_sessions sess
    LEFT JOIN target_agg t ON t.root_domain = sess.root_domain
)
SELECT
  root_domain,
  start_url,
  discovered_urls_count,
  CASE
    WHEN running_targets > 0 THEN 'running'
    WHEN pending_targets > 0 THEN 'pending'
    WHEN failed_targets > 0 AND completed_targets = 0 THEN 'failed'
    WHEN failed_targets > 0 THEN 'completed_with_failures'
    WHEN completed_targets > 0 THEN 'completed'
    ELSE 'unknown'
  END AS status,
  saved_at_utc
FROM domain_rows
WHERE discovered_urls_count > 0
  AND (%s = FALSE OR (
    running_targets = 0
    AND pending_targets = 0
    AND failed_targets = 0
    AND completed_targets > 0
  ))
ORDER BY {order_by_clause}
LIMIT %s;
""".replace("{order_by_clause}", order_by_clause)
        domains: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (bool(completed_only), safe_limit))
                rows = cur.fetchall()
        for row in rows:
            domains.append(
                {
                    "root_domain": str(row[0] or "").strip().lower(),
                    "start_url": str(row[1] or "").strip(),
                    "discovered_urls_count": int(row[2] or 0),
                    "status": str(row[3] or "unknown"),
                    "saved_at_utc": row[4].isoformat() if row[4] else None,
                }
            )
        return {
            "generated_at_utc": _iso_now(),
            "completed_only": bool(completed_only),
            "total_domains": len(domains),
            "domains": domains,
        }



    def list_discovered_target_domains(self, *, limit: int = 5000, q: str = "") -> list[dict[str, Any]]:
        safe_limit = max(1, min(20000, int(limit or 5000)))
        needle = str(q or "").strip().lower()
        sql = """
WITH target_status AS (
    SELECT
      root_domain,
      COUNT(*) FILTER (WHERE status = 'pending') AS pending_targets,
      COUNT(*) FILTER (WHERE status = 'running') AS running_targets,
      COUNT(*) FILTER (WHERE status = 'completed') AS completed_targets,
      COUNT(*) FILTER (WHERE status = 'failed') AS failed_targets
    FROM coordinator_targets
    GROUP BY root_domain
),
domain_rows AS (
    SELECT
      sess.root_domain,
      COALESCE(ts.pending_targets, 0) AS pending_targets,
      COALESCE(ts.running_targets, 0) AS running_targets,
      COALESCE(ts.completed_targets, 0) AS completed_targets,
      COALESCE(ts.failed_targets, 0) AS failed_targets,
      sess.saved_at_utc,
      sess.start_url,
      sess.payload
    FROM coordinator_sessions sess
    LEFT JOIN target_status ts ON ts.root_domain = sess.root_domain
)
SELECT root_domain, start_url, saved_at_utc, payload,
       pending_targets, running_targets, completed_targets, failed_targets
FROM domain_rows
ORDER BY COALESCE(saved_at_utc, NOW()) DESC, root_domain ASC
LIMIT %s;
"""
        rows_out: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (safe_limit,))
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            root_domain = str(row[0] or "").strip().lower()
            if not root_domain:
                continue
            if needle and needle not in root_domain:
                continue
            payload = row[3] if isinstance(row[3], dict) else {}
            state = payload.get("state") if isinstance(payload.get("state"), dict) else {}
            discovered_urls = state.get("discovered_urls")
            discovered_count = len(discovered_urls) if isinstance(discovered_urls, list) else 0
            if discovered_count <= 0:
                continue
            url_inventory = state.get("url_inventory") if isinstance(state.get("url_inventory"), dict) else {}
            method_counts: dict[str, int] = {}
            for _url, record in url_inventory.items():
                if not isinstance(record, dict):
                    continue
                discovered_via = record.get("discovered_via")
                if isinstance(discovered_via, list):
                    for method in discovered_via:
                        key = str(method or "").strip()
                        if key:
                            method_counts[key] = int(method_counts.get(key, 0) or 0) + 1
            if int(row[5] or 0) > 0:
                status = "running"
            elif int(row[4] or 0) > 0:
                status = "pending"
            elif int(row[7] or 0) > 0 and int(row[6] or 0) == 0:
                status = "failed"
            elif int(row[7] or 0) > 0:
                status = "completed_with_failures"
            elif int(row[6] or 0) > 0:
                status = "completed"
            else:
                status = "unknown"
            rows_out.append({
                "root_domain": root_domain,
                "start_url": str(row[1] or "").strip(),
                "saved_at_utc": row[2].isoformat() if row[2] else None,
                "discovered_urls_count": discovered_count,
                "method_counts": method_counts,
                "status": status,
            })
        return rows_out

    def get_discovered_target_sitemap(self, root_domain: str) -> dict[str, Any]:
        rd = str(root_domain or "").strip().lower()
        if not rd:
            return {"root_domain": "", "start_url": "", "page_count": 0, "pages": []}
        session = self.load_session(rd) or {}
        start_url = str(session.get("start_url", "") or "").strip()
        state = session.get("state") if isinstance(session.get("state"), dict) else {}
        link_graph = state.get("link_graph") if isinstance(state.get("link_graph"), dict) else {}
        inventory = state.get("url_inventory") if isinstance(state.get("url_inventory"), dict) else {}
        discovered_urls = state.get("discovered_urls") if isinstance(state.get("discovered_urls"), list) else []
        pages: list[dict[str, Any]] = []
        discovered_set = {str(u or "").strip() for u in discovered_urls if str(u or "").strip()}
        for url in sorted(discovered_set):
            if rd not in str(urlparse(url).hostname or "").lower():
                continue
            record = inventory.get(url) if isinstance(inventory.get(url), dict) else {}
            discovered_via = [str(v or "").strip() for v in (record.get("discovered_via") if isinstance(record.get("discovered_via"), list) else []) if str(v or "").strip()]
            parents: list[str] = []
            for src, targets in link_graph.items():
                if isinstance(targets, list) and url in {str(t or "").strip() for t in targets}:
                    src_text = str(src or "").strip()
                    if src_text:
                        parents.append(src_text)
            outbound_targets = link_graph.get(url) if isinstance(link_graph.get(url), list) else []
            outbound_clean = sorted({str(t or "").strip() for t in outbound_targets if str(t or "").strip()})
            discovered_from = sorted(set(parents))[:50]
            crawl_status_code = record.get("status_code")
            existence_status_code = record.get("existence_status_code")
            page = {
                "url": url,
                "inbound_count": len(discovered_from),
                "outbound_count": len(outbound_clean),
                "discovered_via": discovered_via,
                "discovered_from": discovered_from,
                "was_crawled": bool(record.get("was_crawled")),
                "crawl_requested": bool(record.get("crawl_requested")),
                "exists_confirmed": bool(record.get("exists_confirmed")),
                "crawl_status_code": crawl_status_code,
                "existence_status_code": existence_status_code,
                # Backward-compatible aliases used by older API consumers.
                "parent_count": len(discovered_from),
                "parents": discovered_from,
                "status_code": crawl_status_code,
                "content_type": str(record.get("content_type", "") or ""),
            }
            pages.append(page)
        return {
            "root_domain": rd,
            "start_url": start_url,
            "page_count": len(pages),
            "pages": pages,
        }

    def list_discovered_files(self, *, limit: int = 5000) -> list[dict[str, Any]]:
        safe_limit = max(1, min(20000, int(limit or 5000)))
        sql = """
SELECT a.root_domain, a.artifact_type, a.content_size_bytes, a.updated_at_utc,
       COALESCE(s.start_url, '') AS start_url
FROM coordinator_artifacts a
LEFT JOIN coordinator_sessions s ON s.root_domain = a.root_domain
ORDER BY a.updated_at_utc DESC NULLS LAST, a.root_domain ASC, a.artifact_type ASC
LIMIT %s
"""
        out: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (safe_limit,))
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            updated_at = row[3].isoformat() if row[3] else None
            content_size = int(row[2] or 0)
            out.append({
                "updated_at_utc": updated_at,
                "discovered_at_utc": updated_at,
                "root_domain": str(row[0] or "").strip().lower(),
                "artifact_type": str(row[1] or "").strip(),
                "content_size_bytes": content_size,
                "file_size": content_size,
                "source_url": str(row[4] or "").strip(),
            })
        return out

    def list_high_value_files(self, *, limit: int = 5000) -> list[dict[str, Any]]:
        safe_limit = max(1, min(20000, int(limit or 5000)))
        sql = """
SELECT root_domain, content, content_encoding, updated_at_utc
FROM coordinator_artifacts
WHERE artifact_type = 'nightmare_high_value_zip'
ORDER BY updated_at_utc DESC NULLS LAST, root_domain ASC
LIMIT %s
"""
        out: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (safe_limit,))
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            root_domain = str(row[0] or "").strip().lower()
            data = bytes(row[1] or b"")
            encoding = str(row[2] or "identity")
            updated_at = row[3].isoformat() if row[3] else None
            if encoding != "zip" or not data:
                continue
            try:
                with zipfile.ZipFile(io.BytesIO(data), mode="r") as zf:
                    for name in zf.namelist():
                        if name.endswith("/") or name.lower().endswith(".json"):
                            continue
                        info = zf.getinfo(name)
                        content_size = int(info.file_size or 0)
                        out.append({
                            "captured_at_utc": updated_at,
                            "updated_at_utc": updated_at,
                            "discovered_at_utc": updated_at,
                            "root_domain": root_domain,
                            "saved_relative": name,
                            "content_size_bytes": content_size,
                            "file_size": content_size,
                            "source_url": "",
                        })
            except Exception:
                continue
        return out


    @staticmethod
    def _normalize_worker_state(state: str) -> str:
        text = str(state or "").strip().lower()
        return text if text in {"running", "paused", "stopped", "errored", "idle"} else "idle"

    @staticmethod
    def _derive_worker_status(
        *,
        running_targets: int,
        running_stage_tasks: int,
        last_activity: str,
        is_online: bool,
    ) -> str:
        if int(running_targets or 0) > 0 or int(running_stage_tasks or 0) > 0:
            return "running"
        activity = str(last_activity or "").strip().lower()
        if activity.startswith("state_"):
            state = CoordinatorStore._normalize_worker_state(activity[6:])
            if state != "idle":
                return state
            return "idle" if is_online else "stale"
        if activity.startswith("command_") and "error" in activity:
            return "errored"
        if is_online:
            return "idle"
        return "stale"

    def worker_statuses(
        self,
        *,
        stale_after_seconds: int = DEFAULT_COORDINATOR_LEASE_SECONDS,
        retention_seconds: int = DEFAULT_WORKER_RETENTION_SECONDS,
    ) -> dict[str, Any]:
        stale_after = max(15, int(stale_after_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        retention = max(int(retention_seconds or DEFAULT_WORKER_RETENTION_SECONDS), stale_after * 3, 300)
        sql = """
WITH limits AS (
    SELECT NOW() - ((%s)::text || ' seconds')::interval AS recent_cutoff
),
target_agg AS (
    SELECT
      t.worker_id,
      MAX(t.heartbeat_at_utc) AS last_target_heartbeat,
      COUNT(*) FILTER (WHERE t.status = 'running') AS running_targets,
      COUNT(*) FILTER (
        WHERE t.status = 'running'
          AND t.lease_expires_at IS NOT NULL
          AND t.lease_expires_at > NOW()
      ) AS active_target_leases
    FROM coordinator_targets t
    CROSS JOIN limits l
    WHERE t.worker_id IS NOT NULL
      AND t.worker_id <> ''
      AND (
        t.status = 'running'
        OR (t.heartbeat_at_utc IS NOT NULL AND t.heartbeat_at_utc >= l.recent_cutoff)
        OR (t.completed_at_utc IS NOT NULL AND t.completed_at_utc >= l.recent_cutoff)
      )
    GROUP BY t.worker_id
),
stage_agg AS (
    SELECT
      s.worker_id,
      MAX(s.heartbeat_at_utc) AS last_stage_heartbeat,
      COUNT(*) FILTER (WHERE s.status = 'running') AS running_stage_tasks,
      COUNT(*) FILTER (
        WHERE s.status = 'running'
          AND s.lease_expires_at IS NOT NULL
          AND s.lease_expires_at > NOW()
      ) AS active_stage_leases,
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT CASE WHEN s.status = 'running' THEN s.stage ELSE NULL END), NULL) AS active_stages
    FROM coordinator_stage_tasks s
    CROSS JOIN limits l
    WHERE s.worker_id IS NOT NULL
      AND s.worker_id <> ''
      AND (
        s.status = 'running'
        OR (s.heartbeat_at_utc IS NOT NULL AND s.heartbeat_at_utc >= l.recent_cutoff)
        OR (s.completed_at_utc IS NOT NULL AND s.completed_at_utc >= l.recent_cutoff)
      )
    GROUP BY s.worker_id
),
presence_recent AS (
    SELECT
      p.worker_id,
      p.last_seen_at_utc AS last_presence_heartbeat,
      p.last_activity AS last_activity
    FROM coordinator_worker_presence p
    CROSS JOIN limits l
    WHERE p.worker_id IS NOT NULL
      AND p.worker_id <> ''
      AND p.last_seen_at_utc >= l.recent_cutoff
),
worker_ids AS (
    SELECT worker_id FROM target_agg
    UNION
    SELECT worker_id FROM stage_agg
    UNION
    SELECT worker_id FROM presence_recent
)
SELECT
  w.worker_id AS worker_id,
  COALESCE(
    GREATEST(t.last_target_heartbeat, s.last_stage_heartbeat, p.last_presence_heartbeat),
    GREATEST(t.last_target_heartbeat, s.last_stage_heartbeat),
    GREATEST(t.last_target_heartbeat, p.last_presence_heartbeat),
    GREATEST(s.last_stage_heartbeat, p.last_presence_heartbeat),
    t.last_target_heartbeat,
    s.last_stage_heartbeat,
    p.last_presence_heartbeat
  ) AS last_heartbeat_at_utc,
  COALESCE(t.running_targets, 0) AS running_targets,
  COALESCE(s.running_stage_tasks, 0) AS running_stage_tasks,
  COALESCE(t.active_target_leases, 0) AS active_target_leases,
  COALESCE(s.active_stage_leases, 0) AS active_stage_leases,
  COALESCE(s.active_stages, ARRAY[]::text[]) AS active_stages,
  COALESCE(p.last_activity, 'unknown') AS last_activity
FROM worker_ids w
LEFT JOIN target_agg t ON t.worker_id = w.worker_id
LEFT JOIN stage_agg s ON s.worker_id = w.worker_id
LEFT JOIN presence_recent p ON p.worker_id = w.worker_id
ORDER BY w.worker_id ASC;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (retention,))
                rows = cur.fetchall()
            conn.commit()

        now_utc = datetime.now(timezone.utc)
        workers: list[dict[str, Any]] = []
        online_count = 0
        status_counts = {"running": 0, "paused": 0, "stopped": 0, "errored": 0, "idle": 0}
        for row in rows:
            worker_id = str(row[0] or "").strip()
            if not worker_id:
                continue
            last_heartbeat = row[1]
            running_targets = int(row[2] or 0)
            running_stage_tasks = int(row[3] or 0)
            active_target_leases = int(row[4] or 0)
            active_stage_leases = int(row[5] or 0)
            active_stages_raw = row[6] if isinstance(row[6], list) else []
            active_stages = [str(item) for item in active_stages_raw if str(item or "").strip()]
            last_activity = str(row[7] or "unknown")

            seconds_since: Optional[int] = None
            last_heartbeat_iso: Optional[str] = None
            if last_heartbeat is not None:
                last_heartbeat_iso = last_heartbeat.isoformat()
                delta_seconds = (now_utc - last_heartbeat).total_seconds()
                seconds_since = max(0, int(delta_seconds))
            is_online = seconds_since is not None and seconds_since <= stale_after
            if is_online:
                online_count += 1
            status = self._derive_worker_status(
                running_targets=running_targets,
                running_stage_tasks=running_stage_tasks,
                last_activity=last_activity,
                is_online=is_online,
            )
            if status == "stale":
                continue
            if status in status_counts:
                status_counts[status] += 1
            workers.append(
                {
                    "worker_id": worker_id,
                    "status": status,
                    "last_activity": last_activity,
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
            "retention_seconds": retention,
            "counts": {
                "total_workers": total,
                "online_workers": online_count,
                "stale_workers": 0,
                "running_workers": status_counts["running"],
                "paused_workers": status_counts["paused"],
                "stopped_workers": status_counts["stopped"],
                "errored_workers": status_counts["errored"],
                "idle_workers": status_counts["idle"],
            },
            "workers": workers,
        }


    @staticmethod
    def _worker_id_aliases(value: Any) -> set[str]:
        text = str(value or "").strip()
        if not text:
            return set()
        aliases = {text, text.lower()}
        lowered = text.lower()
        if lowered.startswith("worker-"):
            aliases.add(lowered.replace("worker-", "", 1))
        return {item for item in aliases if str(item or "").strip()}

    @classmethod
    def _worker_id_from_event_row(cls, event: dict[str, Any]) -> str:
        if not isinstance(event, dict):
            return ""
        payload = event.get("payload")
        payload_dict = payload if isinstance(payload, dict) else {}
        candidates = [
            payload_dict.get("worker_id"),
            payload_dict.get("source_worker"),
            payload_dict.get("actor_worker_id"),
            payload_dict.get("claimed_by_worker_id"),
            payload_dict.get("machine"),
            payload_dict.get("worker"),
        ]
        for candidate in candidates:
            worker_id = str(candidate or "").strip()
            if worker_id:
                return worker_id
        aggregate_key = str(event.get("aggregate_key") or "").strip()
        if aggregate_key.startswith("worker:"):
            return aggregate_key.split(":", 1)[1].strip()
        return ""

    @staticmethod
    def _normalize_last_action_label(value: str) -> str:
        text = str(value or "").strip().strip("_")
        if not text:
            return "unknown"
        text = text.replace("_", " ")
        return " ".join(part for part in text.split() if part)

    def _latest_worker_event_map(self, worker_ids: list[str], *, scan_limit: int = 5000) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        normalized_ids = [str(item or "").strip() for item in list(worker_ids or []) if str(item or "").strip()]
        if not normalized_ids:
            return out
        alias_to_worker: dict[str, str] = {}
        for worker_id in normalized_ids:
            for alias in self._worker_id_aliases(worker_id):
                alias_to_worker.setdefault(alias.lower(), worker_id)
        stream = getattr(self, "_event_stream", None)
        if stream is None:
            return out
        try:
            rows = stream.read(limit=max(1000, int(scan_limit or 5000)), reverse=True)
        except Exception:
            return out
        for item in rows:
            candidate = self._worker_id_from_event_row(item)
            if not candidate:
                continue
            worker_id = alias_to_worker.get(str(candidate).strip().lower(), "")
            if not worker_id:
                continue
            payload = item.get("payload")
            payload_dict = payload if isinstance(payload, dict) else {}
            message = str(payload_dict.get("message") or "").strip() or str(item.get("event_type") or "").strip()
            current_time = str(item.get("created_at") or "").strip()
            existing = out.get(worker_id)
            if existing is not None and str(existing.get("last_event_emitted_at_utc") or "") >= current_time:
                continue
            out[worker_id] = {
                "last_event_emitted": message,
                "last_event_type": str(item.get("event_type") or "").strip(),
                "last_event_emitted_at_utc": current_time,
            }
            if len(out) >= len(normalized_ids):
                break
        return out

    def worker_control_snapshot(
        self,
        *,
        stale_after_seconds: int = DEFAULT_COORDINATOR_LEASE_SECONDS,
        retention_seconds: int = DEFAULT_WORKER_RETENTION_SECONDS,
    ) -> dict[str, Any]:
        stale_after = max(15, int(stale_after_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        retention = max(int(retention_seconds or DEFAULT_WORKER_RETENTION_SECONDS), stale_after * 3, 300)
        sql = """
WITH limits AS (
    SELECT NOW() - ((%s)::text || ' seconds')::interval AS recent_cutoff
),
target_agg AS (
    SELECT
      t.worker_id,
      MAX(t.heartbeat_at_utc) AS last_target_heartbeat,
      COUNT(*) FILTER (WHERE t.status = 'running') AS running_targets,
      COUNT(*) FILTER (
        WHERE t.status IN ('running', 'completed')
          AND (
            t.heartbeat_at_utc IS NULL
            OR t.heartbeat_at_utc >= (SELECT recent_cutoff FROM limits)
          )
      ) AS urls_scanned_session,
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT CASE WHEN t.status = 'running' THEN COALESCE(t.start_url, t.root_domain) ELSE NULL END), NULL) AS current_targets
    FROM coordinator_targets t
    CROSS JOIN limits l
    WHERE t.worker_id IS NOT NULL
      AND t.worker_id <> ''
      AND (
        t.status = 'running'
        OR (t.heartbeat_at_utc IS NOT NULL AND t.heartbeat_at_utc >= l.recent_cutoff)
        OR (t.completed_at_utc IS NOT NULL AND t.completed_at_utc >= l.recent_cutoff)
      )
    GROUP BY t.worker_id
),
stage_agg AS (
    SELECT
      s.worker_id,
      MAX(s.heartbeat_at_utc) AS last_stage_heartbeat,
      COUNT(*) FILTER (WHERE s.status = 'running') AS running_stage_tasks
    FROM coordinator_stage_tasks s
    CROSS JOIN limits l
    WHERE s.worker_id IS NOT NULL
      AND s.worker_id <> ''
      AND (
        s.status = 'running'
        OR (s.heartbeat_at_utc IS NOT NULL AND s.heartbeat_at_utc >= l.recent_cutoff)
        OR (s.completed_at_utc IS NOT NULL AND s.completed_at_utc >= l.recent_cutoff)
      )
    GROUP BY s.worker_id
),
commands AS (
    SELECT
      c.worker_id,
      COUNT(*) FILTER (WHERE c.status IN ('queued', 'in_progress')) AS queued_commands
    FROM coordinator_worker_commands c
    CROSS JOIN limits l
    WHERE c.worker_id IS NOT NULL
      AND c.worker_id <> ''
      AND (
        c.status IN ('queued', 'in_progress')
        OR c.updated_at_utc >= l.recent_cutoff
      )
    GROUP BY c.worker_id
),
presence_recent AS (
    SELECT
      p.worker_id,
      p.last_seen_at_utc AS last_presence_heartbeat,
      p.last_activity AS last_activity
    FROM coordinator_worker_presence p
    CROSS JOIN limits l
    WHERE p.worker_id IS NOT NULL
      AND p.worker_id <> ''
      AND p.last_seen_at_utc >= l.recent_cutoff
),
worker_ids AS (
    SELECT worker_id FROM target_agg
    UNION
    SELECT worker_id FROM stage_agg
    UNION
    SELECT worker_id FROM commands
    UNION
    SELECT worker_id FROM presence_recent
)
SELECT
  w.worker_id AS worker_id,
  COALESCE(
    GREATEST(t.last_target_heartbeat, s.last_stage_heartbeat, p.last_presence_heartbeat),
    GREATEST(t.last_target_heartbeat, s.last_stage_heartbeat),
    GREATEST(t.last_target_heartbeat, p.last_presence_heartbeat),
    GREATEST(s.last_stage_heartbeat, p.last_presence_heartbeat),
    t.last_target_heartbeat,
    s.last_stage_heartbeat,
    p.last_presence_heartbeat
  ) AS last_heartbeat_at_utc,
  COALESCE(t.running_targets, 0) AS running_targets,
  COALESCE(s.running_stage_tasks, 0) AS running_stage_tasks,
  COALESCE(t.urls_scanned_session, 0) AS urls_scanned_session,
  COALESCE(t.current_targets, ARRAY[]::text[]) AS current_targets,
  COALESCE(c.queued_commands, 0) AS queued_commands,
  COALESCE(p.last_activity, 'unknown') AS last_activity
FROM worker_ids w
LEFT JOIN target_agg t ON t.worker_id = w.worker_id
LEFT JOIN stage_agg s ON s.worker_id = w.worker_id
LEFT JOIN commands c ON c.worker_id = w.worker_id
LEFT JOIN presence_recent p ON p.worker_id = w.worker_id
ORDER BY w.worker_id ASC;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (retention,))
                rows = cur.fetchall()
            conn.commit()
        now_utc = datetime.now(timezone.utc)
        workers: list[dict[str, Any]] = []
        online_count = 0
        status_counts = {"running": 0, "paused": 0, "stopped": 0, "errored": 0, "idle": 0}
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
            last_activity = str(row[7] or "unknown")
            status = self._derive_worker_status(
                running_targets=int(row[2] or 0),
                running_stage_tasks=int(row[3] or 0),
                last_activity=last_activity,
                is_online=is_online,
            )
            if status == "stale":
                continue
            if status in status_counts:
                status_counts[status] += 1
            workers.append(
                {
                    "worker_id": worker_id,
                    "status": status,
                    "last_activity": last_activity,
                    "last_action_performed": self._normalize_last_action_label(last_activity),
                    "last_heartbeat_at_utc": last_heartbeat_iso,
                    "seconds_since_heartbeat": seconds_since,
                    "running_targets": int(row[2] or 0),
                    "running_stage_tasks": int(row[3] or 0),
                    "urls_scanned_session": int(row[4] or 0),
                    "current_targets": [str(item) for item in current_targets_raw if str(item or "").strip()],
                    "queued_commands": int(row[6] or 0),
                    "last_event_emitted": "",
                    "last_event_type": "",
                    "last_event_emitted_at_utc": "",
                    "last_log_message": "",
                    "last_log_message_at_utc": "",
                    "last_run_time_at_utc": last_heartbeat_iso or "",
                }
            )
        event_map = self._latest_worker_event_map([str(worker.get("worker_id") or "") for worker in workers])
        for worker in workers:
            event_info = event_map.get(str(worker.get("worker_id") or "").strip(), {})
            if event_info:
                worker["last_event_emitted"] = str(event_info.get("last_event_emitted") or "")
                worker["last_event_type"] = str(event_info.get("last_event_type") or "")
                worker["last_event_emitted_at_utc"] = str(event_info.get("last_event_emitted_at_utc") or "")
                if not str(worker.get("last_action_performed") or "").strip() or str(worker.get("last_action_performed") or "").strip().lower() == "unknown":
                    worker["last_action_performed"] = str(event_info.get("last_event_emitted") or "")
                if str(event_info.get("last_event_emitted_at_utc") or "").strip():
                    worker["last_run_time_at_utc"] = str(event_info.get("last_event_emitted_at_utc") or "")
                continue
            fallback_activity = self._normalize_last_action_label(str(worker.get("last_activity") or "").strip())
            fallback_time = str(worker.get("last_heartbeat_at_utc") or "").strip()
            if fallback_activity and fallback_activity.lower() != "unknown":
                worker["last_event_emitted"] = fallback_activity
                worker["last_event_type"] = "worker.presence"
                worker["last_event_emitted_at_utc"] = fallback_time
        total = len(workers)
        return {
            "generated_at_utc": now_utc.isoformat(),
            "stale_after_seconds": stale_after,
            "retention_seconds": retention,
            "counts": {
                "total_workers": total,
                "online_workers": online_count,
                "stale_workers": 0,
                "running_workers": status_counts["running"],
                "paused_workers": status_counts["paused"],
                "stopped_workers": status_counts["stopped"],
                "errored_workers": status_counts["errored"],
                "idle_workers": status_counts["idle"],
            },
            "workers": workers,
        }

    def queue_worker_command(self, worker_id: str, command: str, payload: Optional[dict[str, Any]] = None) -> bool:
        wid = str(worker_id or "").strip()
        cmd = str(command or "").strip().lower()
        if not wid or cmd not in {"start", "pause", "stop", "reload"}:
            return False
        safe_payload = payload if isinstance(payload, dict) else {}
        cancel_sql = """
UPDATE coordinator_worker_commands
SET status = 'cancelled',
    result_error = 'superseded by newer command',
    completed_at_utc = NOW(),
    updated_at_utc = NOW()
WHERE worker_id = %s
  AND status = 'queued';
"""
        insert_sql = """
INSERT INTO coordinator_worker_commands(worker_id, command, payload, status, updated_at_utc)
VALUES (%s, %s, %s::jsonb, 'queued', NOW());
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(cancel_sql, (wid,))
                cur.execute(insert_sql, (wid, cmd, json.dumps(safe_payload)))
                self._touch_worker_presence(cur, wid, f"command_queued_{cmd}")
            conn.commit()
        self.record_system_event(
            "worker.command_queued",
            f"worker:{wid}",
            {
                "source": "coordinator_store.queue_worker_command",
                "worker_id": wid,
                "command": cmd,
                "payload": safe_payload,
                "table": "coordinator_worker_commands",
            },
        )
        return True

    def claim_worker_command(self, worker_id: str, *, worker_state: str = "idle") -> Optional[dict[str, Any]]:
        wid = str(worker_id or "").strip()
        if not wid:
            return None
        state = self._normalize_worker_state(worker_state)
        sql = """
WITH candidate AS (
    SELECT id
    FROM coordinator_worker_commands
    WHERE worker_id = %s
      AND status = 'queued'
    ORDER BY created_at_utc ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE coordinator_worker_commands c
SET status = 'in_progress',
    updated_at_utc = NOW()
FROM candidate
WHERE c.id = candidate.id
RETURNING c.id, c.command, c.payload;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._touch_worker_presence(cur, wid, f"state_{state}")
                cur.execute(sql, (wid,))
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        payload = row[2] if isinstance(row[2], dict) else {}
        self.record_system_event(
            "worker.command_claimed",
            f"worker:{wid}",
            {
                "source": "coordinator_store.claim_worker_command",
                "worker_id": wid,
                "command_id": int(row[0] or 0),
                "command": str(row[1] or "").strip().lower(),
                "payload": payload,
                "worker_state": state,
            },
        )
        return {
            "id": int(row[0]),
            "command": str(row[1] or "").strip().lower(),
            "payload": payload,
        }

    def complete_worker_command(
        self,
        worker_id: str,
        command_id: int,
        *,
        success: bool,
        error: str = "",
    ) -> bool:
        wid = str(worker_id or "").strip()
        if not wid:
            return False
        cid = int(command_id or 0)
        if cid <= 0:
            return False
        next_status = "completed" if bool(success) else "failed"
        sql = """
UPDATE coordinator_worker_commands
SET status = %s,
    result_error = %s,
    completed_at_utc = NOW(),
    updated_at_utc = NOW()
WHERE id = %s
  AND worker_id = %s
  AND status IN ('queued', 'in_progress')
RETURNING command;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (next_status, str(error or "")[:2000], cid, wid))
                row = cur.fetchone()
                if row is not None:
                    command = str(row[0] or "").strip().lower()
                    if next_status == "completed":
                        mapped_state = {
                            "start": "running",
                            "pause": "paused",
                            "stop": "stopped",
                        }.get(command, "idle")
                        self._touch_worker_presence(cur, wid, f"state_{mapped_state}")
                    else:
                        self._touch_worker_presence(cur, wid, f"command_{command}_error")
                updated = int(cur.rowcount or 0)
            conn.commit()
        if updated > 0:
            self.record_system_event(
                f"worker.command_{next_status}",
                f"worker:{wid}",
                {
                    "source": "coordinator_store.complete_worker_command",
                    "worker_id": wid,
                    "command_id": cid,
                    "command": command if row is not None else "",
                    "status": next_status,
                    "error": str(error or "")[:2000],
                },
            )
            if next_status == "completed" and row is not None:
                self.record_system_event(
                    "worker.state_changed",
                    f"worker:{wid}",
                    {
                        "source": "coordinator_store.complete_worker_command",
                        "worker_id": wid,
                        "command": command,
                        "status": mapped_state,
                    },
                )
        return updated > 0

    def schedule_stage(
        self,
        root_domain: str,
        stage: str,
        *,
        workflow_id: str = "",
        worker_id: str = "",
        reason: str = "",
        allow_retry_failed: bool = False,
        max_attempts: int = 0,
        checkpoint: Optional[dict[str, Any]] = None,
        progress: Optional[dict[str, Any]] = None,
        progress_artifact_type: str = "",
        resume_mode: str = "exact",
    ) -> dict[str, Any]:
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        widf = str(workflow_id or "").strip().lower() or "default"
        wid = str(worker_id or "").strip()
        source_reason = str(reason or "").strip()
        max_attempts_int = max(0, int(max_attempts or 0))
        if not rd or not stg:
            return {
                "ok": False,
                "scheduled": False,
                "workflow_id": widf,
                "root_domain": rd,
                "stage": stg,
                "plugin_name": stg,
                "status": "",
                "reason": "invalid_input",
                "attempt_count": 0,
            }
        checkpoint_obj = dict(checkpoint or {}) if isinstance(checkpoint, dict) else {}
        progress_obj = dict(progress or {}) if isinstance(progress, dict) else {}
        progress_artifact_type_text = str(progress_artifact_type or "").strip().lower()
        resume_mode_text = str(resume_mode or "exact").strip().lower() or "exact"

        scheduled = False
        status = ""
        decision_reason = ""
        attempt_count = 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                if wid:
                    self._touch_worker_presence(cur, wid, f"schedule_stage_{stg}")
                cur.execute(
                    """
SELECT status, attempt_count
FROM coordinator_stage_tasks
WHERE workflow_id = %s AND root_domain = %s AND stage = %s
FOR UPDATE;
""",
                    (widf, rd, stg),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        """
INSERT INTO coordinator_stage_tasks(
    workflow_id, root_domain, stage, status, checkpoint_json, progress_json, progress_artifact_type, resume_mode, updated_at_utc
)
VALUES (%s, %s, %s, 'pending', %s::jsonb, %s::jsonb, %s, %s, NOW());
""",
                        (
                            widf,
                            rd,
                            stg,
                            json.dumps(checkpoint_obj, ensure_ascii=False),
                            json.dumps(progress_obj, ensure_ascii=False),
                            progress_artifact_type_text,
                            resume_mode_text,
                        ),
                    )
                    scheduled = True
                    status = "pending"
                    decision_reason = "inserted"
                    attempt_count = 0
                else:
                    current_status = str(row[0] or "").strip().lower()
                    attempt_count = int(row[1] or 0)
                    status = current_status
                    if current_status == "completed":
                        decision_reason = "already_completed"
                    elif current_status in {"pending", "running"}:
                        decision_reason = f"already_{current_status}"
                    elif current_status == "failed":
                        can_retry = bool(allow_retry_failed)
                        if max_attempts_int > 0 and attempt_count >= max_attempts_int:
                            can_retry = False
                            decision_reason = "max_attempts_reached"
                        if can_retry:
                            cur.execute(
                                """
UPDATE coordinator_stage_tasks
SET status = 'pending',
    worker_id = NULL,
    lease_expires_at = NULL,
    heartbeat_at_utc = NULL,
    completed_at_utc = NULL,
    updated_at_utc = NOW(),
    error = NULL,
    checkpoint_json = %s::jsonb,
    progress_json = %s::jsonb,
    progress_artifact_type = %s,
    resume_mode = %s
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s;
""",
                                (
                                    json.dumps(checkpoint_obj, ensure_ascii=False),
                                    json.dumps(progress_obj, ensure_ascii=False),
                                    progress_artifact_type_text,
                                    resume_mode_text,
                                    widf,
                                    rd,
                                    stg,
                                ),
                            )
                            scheduled = True
                            status = "pending"
                            decision_reason = "retry_enqueued"
                        elif not decision_reason:
                            decision_reason = "retry_not_allowed"
                    else:
                        decision_reason = f"unsupported_status_{current_status or 'unknown'}"
            conn.commit()

        if scheduled:
            self.record_system_event(
                "workflow.task.enqueued",
                f"workflow_task:{widf}:{rd}:{stg}",
                {
                    "source": "coordinator_store.schedule_stage",
                    "workflow_id": widf,
                    "root_domain": rd,
                    "stage": stg,
                    "plugin_name": stg,
                    "status": "pending",
                    "worker_id": wid,
                    "reason": source_reason or decision_reason,
                    "allow_retry_failed": bool(allow_retry_failed),
                    "max_attempts": max_attempts_int,
                    "resume_mode": resume_mode_text,
                    "progress_artifact_type": progress_artifact_type_text,
                    "table": "coordinator_stage_tasks",
                },
            )

        return {
            "ok": True,
            "scheduled": scheduled,
            "workflow_id": widf,
            "root_domain": rd,
            "stage": stg,
            "plugin_name": stg,
            "status": status,
            "reason": decision_reason,
            "attempt_count": attempt_count,
        }

    def enqueue_stage(
        self,
        root_domain: str,
        stage: str,
        *,
        workflow_id: str = "",
        worker_id: str = "",
        reason: str = "",
        allow_retry_failed: bool = False,
        max_attempts: int = 0,
        checkpoint: Optional[dict[str, Any]] = None,
        progress: Optional[dict[str, Any]] = None,
        progress_artifact_type: str = "",
        resume_mode: str = "exact",
    ) -> bool:
        result = self.schedule_stage(
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
        return bool(result.get("scheduled"))

    def claim_next_stage(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        workflow_id: str = "",
        plugin_allowlist: Optional[list[str]] = None,
    ) -> Optional[dict[str, Any]]:
        wid = str(worker_id or "").strip()
        widf = str(workflow_id or "").strip().lower()
        if not wid:
            raise ValueError("worker_id is required")
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        allowlist = [
            str(item or "").strip().lower()
            for item in (plugin_allowlist or [])
            if str(item or "").strip()
        ]
        allowlist_param: Optional[list[str]] = allowlist if allowlist else None
        sql = """
WITH candidate AS (
    SELECT workflow_id, root_domain, stage
    FROM coordinator_stage_tasks t
    WHERE (
        status = 'pending'
        OR (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < NOW())
    )
      AND (%s::text[] IS NULL OR stage = ANY(%s))
      AND NOT EXISTS (
          SELECT 1
          FROM coordinator_stage_tasks r
          WHERE r.root_domain = t.root_domain
            AND r.status = 'running'
            AND r.lease_expires_at IS NOT NULL
            AND r.lease_expires_at >= NOW()
            AND NOT (r.workflow_id = t.workflow_id AND r.stage = t.stage)
      )
      AND NOT EXISTS (
          SELECT 1
          FROM coordinator_targets q
          WHERE q.root_domain = t.root_domain
            AND q.status = 'running'
            AND q.lease_expires_at IS NOT NULL
            AND q.lease_expires_at >= NOW()
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
WHERE t.workflow_id = candidate.workflow_id
  AND t.root_domain = candidate.root_domain
  AND t.stage = candidate.stage
RETURNING
    t.workflow_id,
    t.root_domain,
    t.stage,
    t.status,
    t.worker_id,
    t.attempt_count,
    t.lease_expires_at,
    t.checkpoint_json,
    t.progress_json,
    t.progress_artifact_type,
    t.resume_mode;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._touch_worker_presence(cur, wid, "claim_stage")
                cur.execute(sql, (allowlist_param, allowlist_param, wid, lease))
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        self.record_system_event(
            "workflow.task.claimed",
            f"workflow_task:{row[0]}:{row[1]}:{row[2]}",
            {
                "source": "coordinator_store.claim_next_stage",
                "workflow_id": row[0],
                "root_domain": row[1],
                "stage": row[2],
                "plugin_name": row[2],
                "status": row[3],
                "worker_id": row[4],
                "attempt_count": int(row[5] or 0),
                "lease_expires_at": row[6].isoformat() if row[6] else None,
                "resume_mode": str(row[10] or "exact"),
            },
        )
        return {
            "workflow_id": str(row[0] or "default"),
            "root_domain": str(row[1] or "").strip().lower(),
            "stage": str(row[2] or "").strip().lower(),
            "plugin_name": str(row[2] or "").strip().lower(),
            "status": str(row[3] or "").strip().lower(),
            "worker_id": str(row[4] or ""),
            "attempt_count": int(row[5] or 0),
            "lease_expires_at": row[6].isoformat() if row[6] else None,
            "checkpoint": row[7] if isinstance(row[7], dict) else {},
            "progress": row[8] if isinstance(row[8], dict) else {},
            "progress_artifact_type": str(row[9] or ""),
            "resume_mode": str(row[10] or "exact"),
        }

    def claim_stage(
        self,
        stage: str,
        worker_id: str,
        lease_seconds: int,
        *,
        workflow_id: str = "",
    ) -> Optional[dict[str, Any]]:
        stg = str(stage or "").strip().lower()
        if not stg:
            raise ValueError("stage is required")
        return self.claim_next_stage(
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            workflow_id=workflow_id,
            plugin_allowlist=[stg],
        )

    def heartbeat_stage(self, root_domain: str, stage: str, worker_id: str, lease_seconds: int) -> bool:
        return self.heartbeat_stage_with_workflow(
            root_domain=root_domain,
            stage=stage,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            workflow_id="default",
        )

    def heartbeat_stage_with_workflow(
        self,
        *,
        root_domain: str,
        stage: str,
        worker_id: str,
        lease_seconds: int,
        workflow_id: str = "default",
        checkpoint: Optional[dict[str, Any]] = None,
        progress: Optional[dict[str, Any]] = None,
        progress_artifact_type: str = "",
    ) -> bool:
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        widf = str(workflow_id or "").strip().lower() or "default"
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        if not rd or not stg or not wid:
            return False
        checkpoint_json = (
            json.dumps(dict(checkpoint), ensure_ascii=False)
            if isinstance(checkpoint, dict)
            else None
        )
        progress_json = (
            json.dumps(dict(progress), ensure_ascii=False)
            if isinstance(progress, dict)
            else None
        )
        artifact_type_text = str(progress_artifact_type or "").strip().lower()
        sql = """
UPDATE coordinator_stage_tasks
SET heartbeat_at_utc = NOW(),
    lease_expires_at = NOW() + ((%s)::text || ' seconds')::interval,
    checkpoint_json = COALESCE(%s::jsonb, checkpoint_json),
    progress_json = COALESCE(%s::jsonb, progress_json),
    progress_artifact_type = CASE
      WHEN %s <> '' THEN %s
      ELSE progress_artifact_type
    END,
    updated_at_utc = NOW()
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s
  AND worker_id = %s
  AND status = 'running';
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._touch_worker_presence(cur, wid, f"heartbeat_stage_{stg}")
                cur.execute(
                    sql,
                    (
                        lease,
                        checkpoint_json,
                        progress_json,
                        artifact_type_text,
                        artifact_type_text,
                        widf,
                        rd,
                        stg,
                        wid,
                    ),
                )
                updated = int(cur.rowcount or 0)
            conn.commit()
        if updated > 0:
            self.record_system_event(
                "workflow.task.heartbeat",
                f"workflow_task:{widf}:{rd}:{stg}",
                {
                    "source": "coordinator_store.heartbeat_stage_with_workflow",
                    "workflow_id": widf,
                    "root_domain": rd,
                    "stage": stg,
                    "plugin_name": stg,
                    "worker_id": wid,
                    "lease_seconds": lease,
                    "status": "running",
                },
            )
        return updated > 0

    def update_stage_progress(
        self,
        *,
        root_domain: str,
        stage: str,
        worker_id: str,
        workflow_id: str = "default",
        checkpoint: Optional[dict[str, Any]] = None,
        progress: Optional[dict[str, Any]] = None,
        progress_artifact_type: str = "",
    ) -> bool:
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        widf = str(workflow_id or "").strip().lower() or "default"
        if not rd or not stg or not wid:
            return False
        checkpoint_json = (
            json.dumps(dict(checkpoint), ensure_ascii=False)
            if isinstance(checkpoint, dict)
            else None
        )
        progress_json = (
            json.dumps(dict(progress), ensure_ascii=False)
            if isinstance(progress, dict)
            else None
        )
        artifact_type_text = str(progress_artifact_type or "").strip().lower()
        sql = """
UPDATE coordinator_stage_tasks
SET checkpoint_json = COALESCE(%s::jsonb, checkpoint_json),
    progress_json = COALESCE(%s::jsonb, progress_json),
    progress_artifact_type = CASE
      WHEN %s <> '' THEN %s
      ELSE progress_artifact_type
    END,
    heartbeat_at_utc = NOW(),
    updated_at_utc = NOW()
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s
  AND worker_id = %s
  AND status = 'running';
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._touch_worker_presence(cur, wid, f"progress_stage_{stg}")
                cur.execute(
                    sql,
                    (
                        checkpoint_json,
                        progress_json,
                        artifact_type_text,
                        artifact_type_text,
                        widf,
                        rd,
                        stg,
                        wid,
                    ),
                )
                updated = int(cur.rowcount or 0)
            conn.commit()
        if updated > 0:
            checkpoint_payload = dict(checkpoint or {}) if isinstance(checkpoint, dict) else {}
            progress_payload = dict(progress or {}) if isinstance(progress, dict) else {}
            self.record_system_event(
                "workflow.task.progress",
                f"workflow_task:{widf}:{rd}:{stg}",
                {
                    "source": "coordinator_store.update_stage_progress",
                    "workflow_id": widf,
                    "root_domain": rd,
                    "stage": stg,
                    "plugin_name": stg,
                    "worker_id": wid,
                    "checkpoint": checkpoint_payload,
                    "progress": progress_payload,
                    "progress_artifact_type": artifact_type_text,
                    "status": "running",
                },
            )
        return updated > 0

    def complete_stage(
        self,
        root_domain: str,
        stage: str,
        worker_id: str,
        *,
        workflow_id: str = "",
        exit_code: int,
        error: str = "",
        checkpoint: Optional[dict[str, Any]] = None,
        progress: Optional[dict[str, Any]] = None,
        progress_artifact_type: str = "",
        resume_mode: str = "",
    ) -> bool:
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        widf = str(workflow_id or "").strip().lower() or "default"
        if not rd or not stg or not wid:
            return False
        ok = int(exit_code) == 0
        next_status = "completed" if ok else "failed"
        checkpoint_json = (
            json.dumps(dict(checkpoint), ensure_ascii=False)
            if isinstance(checkpoint, dict)
            else None
        )
        progress_json = (
            json.dumps(dict(progress), ensure_ascii=False)
            if isinstance(progress, dict)
            else None
        )
        progress_artifact_type_text = str(progress_artifact_type or "").strip().lower()
        resume_mode_text = str(resume_mode or "").strip().lower()
        sql = """
UPDATE coordinator_stage_tasks
SET status = %s,
    exit_code = %s,
    error = %s,
    checkpoint_json = COALESCE(%s::jsonb, checkpoint_json),
    progress_json = COALESCE(%s::jsonb, progress_json),
    progress_artifact_type = CASE
      WHEN %s <> '' THEN %s
      ELSE progress_artifact_type
    END,
    resume_mode = CASE
      WHEN %s <> '' THEN %s
      ELSE resume_mode
    END,
    completed_at_utc = NOW(),
    heartbeat_at_utc = NOW(),
    lease_expires_at = NULL,
    updated_at_utc = NOW()
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s
  AND worker_id = %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._touch_worker_presence(cur, wid, f"complete_stage_{stg}")
                cur.execute(
                    sql,
                    (
                        next_status,
                        int(exit_code),
                        str(error or "")[:2000],
                        checkpoint_json,
                        progress_json,
                        progress_artifact_type_text,
                        progress_artifact_type_text,
                        resume_mode_text,
                        resume_mode_text,
                        widf,
                        rd,
                        stg,
                        wid,
                    ),
                )
                updated = int(cur.rowcount or 0)
            conn.commit()
        if updated > 0:
            self.record_system_event(
                f"workflow.task.{next_status}",
                f"workflow_task:{widf}:{rd}:{stg}",
                {
                    "source": "coordinator_store.complete_stage",
                    "workflow_id": widf,
                    "root_domain": rd,
                    "stage": stg,
                    "plugin_name": stg,
                    "worker_id": wid,
                    "status": next_status,
                    "exit_code": int(exit_code),
                    "error": str(error or "")[:2000],
                    "progress_artifact_type": progress_artifact_type_text,
                },
            )
        return updated > 0

    def reset_stage_tasks(
        self,
        *,
        workflow_id: str = "",
        root_domains: Optional[list[str]] = None,
        plugins: Optional[list[str]] = None,
        hard_delete: bool = False,
    ) -> dict[str, Any]:
        widf = str(workflow_id or "").strip().lower()
        domains = [
            str(item or "").strip().lower()
            for item in (root_domains or [])
            if str(item or "").strip()
        ]
        stages = [
            str(item or "").strip().lower()
            for item in (plugins or [])
            if str(item or "").strip()
        ]
        where_sql = ["1=1"]
        params: list[Any] = []
        if widf:
            where_sql.append("workflow_id = %s")
            params.append(widf)
        if domains:
            where_sql.append("root_domain = ANY(%s)")
            params.append(domains)
        if stages:
            where_sql.append("stage = ANY(%s)")
            params.append(stages)
        where_clause = " AND ".join(where_sql)
        if hard_delete:
            sql = f"""
DELETE FROM coordinator_stage_tasks
WHERE {where_clause};
"""
        else:
            sql = f"""
UPDATE coordinator_stage_tasks
SET status = 'pending',
    worker_id = NULL,
    lease_expires_at = NULL,
    started_at_utc = NULL,
    completed_at_utc = NULL,
    heartbeat_at_utc = NULL,
    attempt_count = 0,
    exit_code = NULL,
    error = NULL,
    checkpoint_json = '{{}}'::jsonb,
    progress_json = '{{}}'::jsonb,
    progress_artifact_type = '',
    resume_mode = 'exact',
    updated_at_utc = NOW()
WHERE {where_clause};
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                affected = int(cur.rowcount or 0)
            conn.commit()
        self.record_system_event(
            "workflow.task.reset",
            "workflow_tasks",
            {
                "source": "coordinator_store.reset_stage_tasks",
                "workflow_id": widf or "",
                "root_domains": domains,
                "plugins": stages,
                "hard_delete": bool(hard_delete),
                "affected_rows": affected,
            },
        )
        return {
            "ok": True,
            "workflow_id": widf or "",
            "root_domains": domains,
            "plugins": stages,
            "hard_delete": bool(hard_delete),
            "affected_rows": affected,
            "reset_at_utc": _iso_now(),
        }

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
        metadata = self._artifact_store.put_bytes(
            artifact_type=at,
            root_domain=rd,
            payload=data,
            encoding=content_encoding or "binary",
            worker_id=source_worker or "",
        )
        inline_content = data if len(data) <= self._db_inline_artifact_max_bytes else b""
        sql = """
INSERT INTO coordinator_artifacts(
    root_domain, artifact_type, source_worker, content, content_encoding, content_sha256, content_size_bytes,
    storage_backend, storage_uri, media_type, compression, schema_version, updated_at_utc
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (root_domain, artifact_type) DO UPDATE
SET source_worker = EXCLUDED.source_worker,
    content = EXCLUDED.content,
    content_encoding = EXCLUDED.content_encoding,
    content_sha256 = EXCLUDED.content_sha256,
    content_size_bytes = EXCLUDED.content_size_bytes,
    storage_backend = EXCLUDED.storage_backend,
    storage_uri = EXCLUDED.storage_uri,
    media_type = EXCLUDED.media_type,
    compression = EXCLUDED.compression,
    schema_version = EXCLUDED.schema_version,
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
                        inline_content,
                        str(content_encoding or "identity")[:120],
                        metadata.sha256,
                        len(data),
                        metadata.storage_backend,
                        metadata.storage_uri,
                        metadata.media_type,
                        metadata.compression,
                        metadata.schema_version,
                    ),
                )
                summary_payload = json.dumps(
                    {
                        "schema_version": registry.current_version("summary_envelope"),
                        "stage": "artifact_upload",
                        "status": "completed",
                        "root_domain": rd,
                        "counts": {"artifacts": 1},
                        "metrics": {"artifact_bytes": float(len(data)), "risk_score": self._risk_score_for_artifact(at, len(data))},
                        "output_artifacts": {at: metadata.sha256},
                    }
                )
                cur.execute(
                    """
                    INSERT INTO coordinator_summary_latest(root_domain, stage_name, summary_json, updated_at_utc)
                    VALUES (%s, %s, %s::jsonb, NOW())
                    ON CONFLICT (root_domain, stage_name) DO UPDATE
                    SET summary_json = EXCLUDED.summary_json,
                        updated_at_utc = NOW()
                    """,
                    (rd, f"artifact:{at}", summary_payload),
                )
            conn.commit()
        self.record_system_event(
            "artifact.uploaded",
            f"artifact:{rd}:{at}",
            {
                "source": "coordinator_store.upload_artifact",
                "root_domain": rd,
                "artifact_type": at,
                "worker_id": str(source_worker or "")[:200],
                "sha256": metadata.sha256,
                "size_bytes": len(data),
                "storage_backend": metadata.storage_backend,
                "storage_uri": metadata.storage_uri,
                "compression": metadata.compression,
                "risk_score": self._risk_score_for_artifact(at, len(data)),
                "table": "coordinator_artifacts",
            },
        )
        return True


    def get_artifact(self, root_domain: str, artifact_type: str) -> Optional[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        at = str(artifact_type or "").strip().lower()
        if not rd or not at:
            return None
        sql = """
SELECT root_domain, artifact_type, source_worker, content, content_encoding, content_sha256, content_size_bytes,
       storage_backend, storage_uri, media_type, compression, schema_version, updated_at_utc
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
        content = bytes(row[3] or b"")
        storage_backend = row[7] or "database_inline"
        storage_uri = row[8] or ""
        compression = row[10] or "identity"
        if storage_uri and (not content):
            try:
                content = self._artifact_store.get_bytes(storage_uri, compression=compression)
            except Exception:
                content = b""
        return {
            "root_domain": row[0],
            "artifact_type": row[1],
            "source_worker": row[2],
            "content": content,
            "content_encoding": row[4],
            "content_sha256": row[5],
            "content_size_bytes": int(row[6] or 0),
            "storage_backend": storage_backend,
            "storage_uri": storage_uri,
            "media_type": row[9] or "application/octet-stream",
            "compression": compression,
            "schema_version": int(row[11] or 1),
            "updated_at_utc": row[12].isoformat() if row[12] else None,
        }

    def list_artifacts(self, root_domain: str) -> list[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        if not rd:
            return []
        sql = """
SELECT artifact_type, source_worker, content_encoding, content_sha256, content_size_bytes,
       storage_backend, storage_uri, media_type, compression, schema_version, updated_at_utc
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
                    "storage_backend": row[5] or "database_inline",
                    "storage_uri": row[6] or "",
                    "media_type": row[7] or "application/octet-stream",
                    "compression": row[8] or "identity",
                    "schema_version": int(row[9] or 1),
                    "updated_at_utc": row[10].isoformat() if row[10] else None,
                }
            )
        return out

    def list_extractor_match_domains(self, *, limit: int = 5000) -> list[dict[str, Any]]:
        safe_limit = max(1, min(20000, int(limit or 5000)))
        sql = """
SELECT
  m.root_domain,
  m.source_worker,
  m.content_sha256,
  m.content_size_bytes,
  m.updated_at_utc,
  s.content,
  s.content_encoding,
  s.updated_at_utc
FROM coordinator_artifacts m
LEFT JOIN coordinator_artifacts s
  ON s.root_domain = m.root_domain
 AND s.artifact_type = 'extractor_summary_json'
WHERE m.artifact_type = 'extractor_matches_zip'
ORDER BY m.updated_at_utc DESC NULLS LAST, m.root_domain ASC
LIMIT %s;
"""
        out: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (safe_limit,))
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            summary_bytes = bytes(row[5] or b"")
            summary_encoding = str(row[6] or "identity")
            out.append(
                {
                    "root_domain": str(row[0] or "").strip().lower(),
                    "source_worker": str(row[1] or ""),
                    "content_sha256": str(row[2] or ""),
                    "content_size_bytes": int(row[3] or 0),
                    "updated_at_utc": row[4].isoformat() if row[4] else None,
                    "summary_match_count": _parse_summary_match_count(summary_bytes, summary_encoding),
                    "summary_updated_at_utc": row[7].isoformat() if row[7] else None,
                }
            )
        return out

    def list_fozzy_summary_domains(self, *, limit: int = 5000) -> list[dict[str, Any]]:
        safe_limit = max(1, min(20000, int(limit or 5000)))
        sql = """
SELECT
  s.root_domain,
  s.source_worker,
  s.content_sha256,
  s.content_size_bytes,
  s.updated_at_utc,
  s.content,
  s.content_encoding,
  z.content_sha256,
  z.content_size_bytes,
  z.updated_at_utc
FROM coordinator_artifacts s
LEFT JOIN coordinator_artifacts z
  ON z.root_domain = s.root_domain
 AND z.artifact_type = 'fozzy_results_zip'
WHERE s.artifact_type = 'fozzy_summary_json'
ORDER BY s.updated_at_utc DESC NULLS LAST, s.root_domain ASC
LIMIT %s;
"""
        out: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (safe_limit,))
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            totals = _parse_fozzy_summary_totals(bytes(row[5] or b""), str(row[6] or "identity"))
            out.append(
                {
                    "root_domain": str(row[0] or "").strip().lower(),
                    "source_worker": str(row[1] or ""),
                    "summary_content_sha256": str(row[2] or ""),
                    "summary_content_size_bytes": int(row[3] or 0),
                    "summary_updated_at_utc": row[4].isoformat() if row[4] else None,
                    "totals": totals,
                    "results_zip_content_sha256": str(row[7] or ""),
                    "results_zip_content_size_bytes": int(row[8] or 0),
                    "results_zip_updated_at_utc": row[9].isoformat() if row[9] else None,
                    "has_results_zip": bool(row[7]),
                }
            )
        return out
