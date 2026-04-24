#!/usr/bin/env python3
"""Coordinator database access layer for the server."""

from __future__ import annotations

import hashlib
import json
import base64
import io
import gzip
import os
import zipfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urljoin, urlparse

try:
    import psycopg
    from psycopg_pool import ConnectionPool
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg = None
    ConnectionPool = None  # type: ignore[assignment]

from nightmare_app.artifacts import FileSystemArtifactStore
from shared.events import build_projection
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


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _max_iso_datetime(*values: Any) -> str:
    latest: datetime | None = None
    for item in values:
        parsed = _parse_iso_datetime(item)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest.isoformat() if latest is not None else ""


def _stream_file_chunks(path: str | Path, *, chunk_size: int = 1024 * 1024):
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            yield chunk


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
        if psycopg is None or ConnectionPool is None:
            raise RuntimeError("psycopg and psycopg_pool are required for postgres coordinator mode")
        self.database_url = str(database_url or "").strip()
        if not self.database_url:
            raise ValueError("database_url is required for coordinator mode")
        self._connect_timeout_seconds = self._resolve_connect_timeout_seconds()
        self._pool = self._build_pool()
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
        self._db_inline_artifact_max_bytes = max(0, int(os.getenv("NIGHTMARE_DB_INLINE_ARTIFACT_MAX_BYTES", "65536") or "65536"))
        # Production event queries are DB-backed through coordinator_recent_events.
        # JSONL event streams were removed from the hot/query path; keep the flag only
        # as a disabled compatibility marker for older configuration.
        self._event_stream_enabled = False
        self._worker_presence_min_interval_seconds = max(5, int(os.getenv("NIGHTMARE_WORKER_PRESENCE_INTERVAL_SECONDS", "30") or "30"))
        self._durable_progress_min_interval_seconds = max(15, int(os.getenv("NIGHTMARE_DURABLE_PROGRESS_INTERVAL_SECONDS", "60") or "60"))
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

    def _build_pool(self):
        min_size = max(1, int(os.getenv("COORDINATOR_DB_POOL_MIN_SIZE", "1") or "1"))
        max_size = max(min_size, int(os.getenv("COORDINATOR_DB_POOL_MAX_SIZE", "12") or "12"))
        return ConnectionPool(
            conninfo=self.database_url,
            min_size=min_size,
            max_size=max_size,
            open=True,
            kwargs={
                "autocommit": False,
                "connect_timeout": self._connect_timeout_seconds,
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


    def _emit_event(self, event_type: str, aggregate_key: str, payload: dict[str, Any]) -> None:
        safe_payload = self._safe_event_payload(payload or {})
        event = EventRecord(
            event_type=str(event_type or ""),
            aggregate_key=str(aggregate_key or ""),
            schema_version=registry.current_version("event_record"),
            payload=safe_payload,
        )
        sql = """
INSERT INTO coordinator_recent_events(
    event_id, created_at_utc, event_type, aggregate_key, schema_version, source, message, payload_json
)
VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (event_id) DO NOTHING;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        str(event.event_id),
                        str(event.event_type),
                        str(event.aggregate_key),
                        int(event.schema_version or 1),
                        str(safe_payload.get("source") or "")[:120],
                        str(safe_payload.get("message") or "")[:1000],
                        json.dumps(safe_payload, ensure_ascii=False, default=str),
                    ),
                )
                cur.execute(
                    """
INSERT INTO coordinator_projection_state(aggregate_key, projection_json, updated_at_utc)
VALUES (%s, %s::jsonb, NOW())
ON CONFLICT (aggregate_key) DO UPDATE
SET projection_json = EXCLUDED.projection_json,
    updated_at_utc = NOW();
""",
                    (
                        str(event.aggregate_key),
                        json.dumps(
                            {
                                "event_type": str(event.event_type),
                                "aggregate_key": str(event.aggregate_key),
                                "source": str(safe_payload.get("source") or ""),
                                "message": str(safe_payload.get("message") or ""),
                                "payload": safe_payload,
                                "updated_at_utc": _iso_now(),
                            },
                            ensure_ascii=False,
                            default=str,
                        ),
                    ),
                )
            conn.commit()

    def projection_snapshot(self) -> dict[str, dict[str, Any]]:
        sql = """
SELECT aggregate_key, projection_json
FROM coordinator_projection_state
ORDER BY updated_at_utc DESC;
"""
        out: dict[str, dict[str, Any]] = {}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            conn.commit()
        for aggregate_key, projection_json in rows:
            out[str(aggregate_key)] = projection_json if isinstance(projection_json, dict) else {}
        return out

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
        filters: list[str] = []
        params: list[Any] = []
        if event_type:
            filters.append("LOWER(event_type) LIKE %s")
            params.append(f"%{str(event_type).strip().lower()}%")
        if aggregate_key:
            filters.append("LOWER(aggregate_key) LIKE %s")
            params.append(f"%{str(aggregate_key).strip().lower()}%")
        if source:
            filters.append("LOWER(source) LIKE %s")
            params.append(f"%{str(source).strip().lower()}%")
        if search:
            needle = f"%{str(search).strip().lower()}%"
            filters.append("(LOWER(event_type) LIKE %s OR LOWER(aggregate_key) LIKE %s OR LOWER(source) LIKE %s OR LOWER(message) LIKE %s OR LOWER(payload_json::text) LIKE %s)")
            params.extend([needle, needle, needle, needle, needle])
        where = "WHERE " + " AND ".join(filters) if filters else ""
        order = "ASC" if str(sort_dir or "desc").strip().lower() == "asc" else "DESC"
        count_sql = f"SELECT COUNT(*) FROM coordinator_recent_events {where};"
        list_sql = f"""
SELECT event_id, created_at_utc, event_type, aggregate_key, schema_version, source, message, payload_json
FROM coordinator_recent_events
{where}
ORDER BY created_at_utc {order}, event_id {order}
LIMIT %s OFFSET %s;
"""
        page: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total_row = cur.fetchone()
                cur.execute(list_sql, [*params, requested, skip])
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            page.append(
                {
                    "event_id": str(row[0] or ""),
                    "created_at": row[1].isoformat() if row[1] else None,
                    "event_type": str(row[2] or ""),
                    "aggregate_key": str(row[3] or ""),
                    "schema_version": int(row[4] or 1),
                    "source": str(row[5] or ""),
                    "message": str(row[6] or ""),
                    "payload": row[7] if isinstance(row[7], dict) else {},
                }
            )
        return {
            "generated_at_utc": _iso_now(),
            "total": int(total_row[0] or 0) if total_row else 0,
            "offset": skip,
            "limit": requested,
            "events": page,
            "source": "coordinator_recent_events",
        }

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
CREATE INDEX IF NOT EXISTS idx_targets_claim_partial ON coordinator_targets(line_number, created_at_utc) WHERE status IN ('pending','running');
CREATE INDEX IF NOT EXISTS idx_targets_running_domain_lease ON coordinator_targets(root_domain, lease_expires_at) WHERE status='running';

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
  max_attempts INTEGER NOT NULL DEFAULT 1,
  exit_code INTEGER,
  error TEXT,
  checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  progress_artifact_type TEXT NOT NULL DEFAULT '',
  resume_mode TEXT NOT NULL DEFAULT 'exact',
  resource_class TEXT NOT NULL DEFAULT 'default',
  access_mode TEXT NOT NULL DEFAULT 'write',
  concurrency_group TEXT NOT NULL DEFAULT '',
  max_parallelism INTEGER NOT NULL DEFAULT 1,
  progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
  current_unit TEXT NOT NULL DEFAULT '',
  last_milestone TEXT NOT NULL DEFAULT '',
  durable_checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  durable_progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  durable_progress_updated_at_utc TIMESTAMPTZ,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(workflow_id, root_domain, stage)
);
CREATE INDEX IF NOT EXISTS idx_stage_tasks_status_stage ON coordinator_stage_tasks(stage, status);
CREATE INDEX IF NOT EXISTS idx_stage_tasks_lease ON coordinator_stage_tasks(lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_stage_tasks_domain_status ON coordinator_stage_tasks(root_domain, status, lease_expires_at);

CREATE TABLE IF NOT EXISTS coordinator_resource_leases (
  lease_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  root_domain TEXT NOT NULL,
  stage TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  resource_class TEXT NOT NULL DEFAULT 'default',
  access_mode TEXT NOT NULL DEFAULT 'write',
  concurrency_group TEXT NOT NULL DEFAULT '',
  lease_key TEXT NOT NULL,
  max_parallelism INTEGER NOT NULL DEFAULT 1,
  leased_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  lease_expires_at TIMESTAMPTZ NOT NULL,
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_resource_leases_key
  ON coordinator_resource_leases(root_domain, lease_key, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_resource_leases_stage
  ON coordinator_resource_leases(workflow_id, root_domain, stage);
CREATE INDEX IF NOT EXISTS idx_resource_leases_expiry
  ON coordinator_resource_leases(lease_expires_at);

CREATE TABLE IF NOT EXISTS coordinator_artifacts (
  root_domain TEXT NOT NULL,
  artifact_type TEXT NOT NULL,
  source_worker TEXT,
  content BYTEA,
  content_encoding TEXT NOT NULL DEFAULT 'identity',
  content_sha256 TEXT NOT NULL,
  content_size_bytes BIGINT NOT NULL,
  storage_backend TEXT NOT NULL DEFAULT 'filesystem',
  storage_uri TEXT NOT NULL DEFAULT '',
  media_type TEXT NOT NULL DEFAULT 'application/octet-stream',
  compression TEXT NOT NULL DEFAULT 'identity',
  schema_version INTEGER NOT NULL DEFAULT 1,
  manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  retention_class TEXT NOT NULL DEFAULT 'derived_rebuildable',
  summary_match_count INTEGER NOT NULL DEFAULT 0,
  summary_anomaly_count INTEGER NOT NULL DEFAULT 0,
  summary_request_count INTEGER NOT NULL DEFAULT 0,
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(root_domain, artifact_type)
);
CREATE INDEX IF NOT EXISTS idx_artifacts_domain ON coordinator_artifacts(root_domain);

CREATE TABLE IF NOT EXISTS coordinator_artifact_objects (
  content_sha256 TEXT PRIMARY KEY,
  storage_backend TEXT NOT NULL DEFAULT 'filesystem',
  storage_uri TEXT NOT NULL,
  content_size_bytes BIGINT NOT NULL DEFAULT 0,
  media_type TEXT NOT NULL DEFAULT 'application/octet-stream',
  compression TEXT NOT NULL DEFAULT 'identity',
  ref_count BIGINT NOT NULL DEFAULT 1,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_artifact_objects_storage
  ON coordinator_artifact_objects(storage_backend, storage_uri);

CREATE TABLE IF NOT EXISTS coordinator_artifact_manifest_entries (
  root_domain TEXT NOT NULL,
  artifact_type TEXT NOT NULL,
  entry_path TEXT NOT NULL,
  content_sha256 TEXT NOT NULL,
  content_size_bytes BIGINT NOT NULL DEFAULT 0,
  media_type TEXT NOT NULL DEFAULT 'application/octet-stream',
  storage_uri TEXT NOT NULL DEFAULT '',
  shard_key TEXT NOT NULL DEFAULT '',
  logical_role TEXT NOT NULL DEFAULT '',
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(root_domain, artifact_type, entry_path)
);
CREATE INDEX IF NOT EXISTS idx_manifest_entries_hash
  ON coordinator_artifact_manifest_entries(content_sha256);
CREATE INDEX IF NOT EXISTS idx_manifest_entries_shard
  ON coordinator_artifact_manifest_entries(root_domain, artifact_type, shard_key);

CREATE TABLE IF NOT EXISTS coordinator_recent_events (
  event_id TEXT PRIMARY KEY,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  event_type TEXT NOT NULL,
  aggregate_key TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  source TEXT NOT NULL DEFAULT '',
  message TEXT NOT NULL DEFAULT '',
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_recent_events_created ON coordinator_recent_events(created_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_recent_events_lookup ON coordinator_recent_events(event_type, aggregate_key, source);

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

CREATE TABLE IF NOT EXISTS coordinator_worker_presence (
  worker_id TEXT PRIMARY KEY,
  last_seen_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_activity TEXT NOT NULL DEFAULT 'unknown',
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_worker_presence_last_seen
  ON coordinator_worker_presence(last_seen_at_utc DESC);

CREATE TABLE IF NOT EXISTS coordinator_summary_latest (
  root_domain TEXT NOT NULL,
  stage_name TEXT NOT NULL,
  summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(root_domain, stage_name)
);

CREATE TABLE IF NOT EXISTS coordinator_projection_state (
  aggregate_key TEXT PRIMARY KEY,
  projection_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""
        migration_statements = [
            "ALTER TABLE coordinator_artifacts ALTER COLUMN content DROP NOT NULL",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS retention_class TEXT NOT NULL DEFAULT 'derived_rebuildable'",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS summary_match_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS summary_anomaly_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE coordinator_artifacts ADD COLUMN IF NOT EXISTS summary_request_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS workflow_id TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS progress_json JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS progress_artifact_type TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS resume_mode TEXT NOT NULL DEFAULT 'exact'",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS resource_class TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS access_mode TEXT NOT NULL DEFAULT 'write'",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS concurrency_group TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS max_parallelism INTEGER NOT NULL DEFAULT 1",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS current_unit TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS last_milestone TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS durable_checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS durable_progress_json JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE coordinator_stage_tasks ADD COLUMN IF NOT EXISTS durable_progress_updated_at_utc TIMESTAMPTZ",
            "CREATE TABLE IF NOT EXISTS coordinator_recent_events (event_id TEXT PRIMARY KEY, created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(), event_type TEXT NOT NULL, aggregate_key TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1, source TEXT NOT NULL DEFAULT '', message TEXT NOT NULL DEFAULT '', payload_json JSONB NOT NULL DEFAULT '{}'::jsonb)",
            "CREATE INDEX IF NOT EXISTS idx_recent_events_created ON coordinator_recent_events(created_at_utc DESC)",
            "CREATE INDEX IF NOT EXISTS idx_recent_events_lookup ON coordinator_recent_events(event_type, aggregate_key, source)",
            "CREATE INDEX IF NOT EXISTS idx_targets_claim_partial ON coordinator_targets(line_number, created_at_utc) WHERE status IN ('pending','running')",
            "CREATE INDEX IF NOT EXISTS idx_targets_running_domain_lease ON coordinator_targets(root_domain, lease_expires_at) WHERE status='running'",
            "CREATE INDEX IF NOT EXISTS idx_stage_tasks_claim_partial ON coordinator_stage_tasks(workflow_id, created_at_utc) WHERE status IN ('ready','running')",
            "CREATE INDEX IF NOT EXISTS idx_stage_tasks_running_domain_lease ON coordinator_stage_tasks(root_domain, lease_expires_at) WHERE status='running'",
            "CREATE INDEX IF NOT EXISTS idx_stage_tasks_concurrency ON coordinator_stage_tasks(root_domain, concurrency_group, status, lease_expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_stage_tasks_workflow_stage_status ON coordinator_stage_tasks(workflow_id, stage, status)",
            "CREATE INDEX IF NOT EXISTS idx_artifacts_retention ON coordinator_artifacts(retention_class)",
            "CREATE INDEX IF NOT EXISTS idx_artifacts_hot_fields ON coordinator_artifacts(root_domain, artifact_type, summary_match_count, summary_anomaly_count)",
        ]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                cur.execute("""
ALTER TABLE coordinator_stage_tasks
  ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 1;
UPDATE coordinator_stage_tasks SET max_attempts = 1 WHERE COALESCE(max_attempts, 0) < 1;
""")
            conn.commit()
            for sql in migration_statements:
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                    conn.commit()
                except Exception:
                    conn.rollback()

    def _touch_worker_presence(self, cur: Any, worker_id: str, activity: str) -> None:
        wid = str(worker_id or "").strip()
        if not wid:
            return
        act = str(activity or "").strip().lower() or "unknown"
        sql = """
INSERT INTO coordinator_worker_presence(worker_id, last_seen_at_utc, last_activity, updated_at_utc)
VALUES (%s, NOW(), %s, NOW())
ON CONFLICT (worker_id) DO UPDATE
SET last_seen_at_utc = CASE
      WHEN coordinator_worker_presence.last_seen_at_utc < NOW() - ((%s)::text || ' seconds')::interval THEN NOW()
      ELSE coordinator_worker_presence.last_seen_at_utc
    END,
    last_activity = EXCLUDED.last_activity,
    updated_at_utc = CASE
      WHEN coordinator_worker_presence.last_seen_at_utc < NOW() - ((%s)::text || ' seconds')::interval THEN NOW()
      ELSE coordinator_worker_presence.updated_at_utc
    END;
"""
        cur.execute(sql, (wid, act[:64], self._worker_presence_min_interval_seconds, self._worker_presence_min_interval_seconds))

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
            AND s.resource_class IN ('crawl', 'network_scan')
            AND s.access_mode <> 'read'
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

    def _decode_json_bytes(self, raw: bytes) -> dict[str, Any]:
        if not raw:
            return {}
        for encoding in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                parsed = json.loads(raw.decode(encoding))
            except Exception:
                continue
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def _load_session_artifact_payload(self, root_domain: str) -> dict[str, Any]:
        artifact = self.get_artifact(str(root_domain or "").strip().lower(), "nightmare_session_json")
        if not isinstance(artifact, dict):
            return {}
        return self._decode_json_bytes(bytes(artifact.get("content") or b""))


    def _bulk_load_sessions(
        self,
        root_domains: list[str],
        *,
        include_artifact_fallback: bool = False,
        artifact_fallback_limit: Optional[int] = None,
    ) -> dict[str, dict[str, Any]]:
        domains = [str(item or "").strip().lower() for item in root_domains if str(item or "").strip()]
        if not domains:
            return {}

        ordered_domains: list[str] = []
        seen: set[str] = set()
        for domain in domains:
            if domain in seen:
                continue
            seen.add(domain)
            ordered_domains.append(domain)

        sessions_by_domain: dict[str, dict[str, Any]] = {}
        sql = """
SELECT root_domain, start_url, max_pages, saved_at_utc, payload
FROM coordinator_sessions
WHERE root_domain = ANY(%s);
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (ordered_domains,))
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            rd = str(row[0] or "").strip().lower()
            if not rd:
                continue
            payload = row[4] if isinstance(row[4], dict) else {}
            normalized = self._normalize_session_payload(
                rd,
                payload,
                fallback_start_url=str(row[1] or "").strip(),
                fallback_saved_at_utc=row[3].isoformat() if row[3] else None,
                fallback_max_pages=row[2],
            )
            if normalized is not None:
                sessions_by_domain[rd] = normalized

        if not include_artifact_fallback:
            return sessions_by_domain

        missing_domains: list[str] = []
        for rd in ordered_domains:
            metrics = self._session_url_metrics(sessions_by_domain.get(rd))
            if not bool(metrics.get("has_session_data")):
                missing_domains.append(rd)

        if artifact_fallback_limit is not None:
            fallback_cap = max(0, int(artifact_fallback_limit or 0))
            missing_domains = missing_domains[:fallback_cap]
        if not missing_domains:
            return sessions_by_domain

        artifact_sql = """
SELECT root_domain, content, content_encoding, storage_uri, compression, updated_at_utc
FROM coordinator_artifacts
WHERE root_domain = ANY(%s) AND artifact_type = 'nightmare_session_json';
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(artifact_sql, (missing_domains,))
                artifact_rows = cur.fetchall()
            conn.commit()

        for row in artifact_rows:
            rd = str(row[0] or "").strip().lower()
            if not rd:
                continue
            content = bytes(row[1] or b"")
            encoding = str(row[2] or "identity")
            storage_uri = str(row[3] or "").strip()
            compression = str(row[4] or "identity")
            updated_at = row[5].isoformat() if row[5] else None
            if storage_uri and (not content):
                try:
                    content = self._artifact_store.get_bytes(storage_uri, compression=compression)
                except Exception:
                    content = b""
            if encoding == "gzip":
                try:
                    content = gzip.decompress(content)
                except Exception:
                    content = b""
            artifact_payload = self._decode_json_bytes(content)
            normalized = self._normalize_session_payload(rd, artifact_payload, fallback_saved_at_utc=updated_at)
            if normalized is None:
                continue
            current = sessions_by_domain.get(rd)
            if current is None:
                sessions_by_domain[rd] = normalized
                continue

            current_metrics = self._session_url_metrics(current)
            if not bool(current_metrics.get("has_session_data")):
                sessions_by_domain[rd] = normalized
                continue

            def _parse_ts(value: Any) -> Optional[datetime]:
                raw = str(value or "").strip()
                if not raw:
                    return None
                try:
                    return datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except Exception:
                    return None

            current_saved = _parse_ts(current.get("saved_at_utc"))
            artifact_saved = _parse_ts(normalized.get("saved_at_utc"))
            if artifact_saved and (current_saved is None or artifact_saved >= current_saved):
                sessions_by_domain[rd] = normalized
                continue

            current_metrics = self._session_url_metrics(current)
            artifact_metrics = self._session_url_metrics(normalized)
            if int(artifact_metrics.get("discovered_urls_count") or 0) > int(current_metrics.get("discovered_urls_count") or 0):
                sessions_by_domain[rd] = normalized

        return sessions_by_domain

    def _normalize_session_payload(
        self,
        root_domain: str,
        payload: Optional[dict[str, Any]],
        *,
        fallback_start_url: str = "",
        fallback_saved_at_utc: Optional[str] = None,
        fallback_max_pages: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        body = payload if isinstance(payload, dict) else {}
        if not body:
            return None
        state = body.get("state") if isinstance(body.get("state"), dict) else {}
        frontier = body.get("frontier") if isinstance(body.get("frontier"), list) else []
        start_url = str(body.get("start_url") or fallback_start_url or "").strip()
        saved_at_utc = body.get("saved_at_utc") or fallback_saved_at_utc
        max_pages = body.get("max_pages")
        if max_pages is None:
            max_pages = fallback_max_pages
        return {
            "root_domain": str(body.get("root_domain") or root_domain or "").strip().lower(),
            "start_url": start_url,
            "max_pages": max_pages,
            "saved_at_utc": saved_at_utc,
            "state": state,
            "frontier": frontier,
            "payload": body,
        }


    def _session_url_metrics(self, session: Optional[dict[str, Any]]) -> dict[str, Any]:
        body = session if isinstance(session, dict) else {}
        state = body.get("state") if isinstance(body.get("state"), dict) else {}
        frontier = body.get("frontier") if isinstance(body.get("frontier"), list) else []
        discovered_urls = state.get("discovered_urls") if isinstance(state.get("discovered_urls"), list) else []
        visited_urls = state.get("visited_urls") if isinstance(state.get("visited_urls"), list) else []
        url_inventory = state.get("url_inventory") if isinstance(state.get("url_inventory"), dict) else {}

        discovered_urls_count = len(discovered_urls)
        if discovered_urls_count <= 0 and url_inventory:
            discovered_urls_count = len([u for u in url_inventory.keys() if str(u or "").strip()])

        method_counts: dict[str, int] = {}
        spider_stats: dict[str, int] = {}
        for _url, record in url_inventory.items():
            if not isinstance(record, dict):
                continue
            discovered_via = record.get("discovered_via")
            methods = discovered_via if isinstance(discovered_via, list) else [discovered_via]
            for method in methods:
                key = str(method or "").strip()
                if not key:
                    continue
                method_counts[key] = int(method_counts.get(key, 0) or 0) + 1
                spider_key = key
                if spider_key.startswith("spider:"):
                    spider_key = spider_key.split(":", 1)[1].strip()
                if spider_key.startswith("crawler:"):
                    spider_key = spider_key.split(":", 1)[1].strip()
                if spider_key.startswith("nightmare_spider_"):
                    spider_key = spider_key.removeprefix("nightmare_spider_")
                if spider_key:
                    spider_stats[spider_key] = int(spider_stats.get(spider_key, 0) or 0) + 1

        return {
            "discovered_urls_count": discovered_urls_count,
            "visited_urls_count": len(visited_urls),
            "frontier_count": len(frontier),
            "method_counts": method_counts,
            "spider_stats": spider_stats,
            "has_session_data": bool(body) and (
                discovered_urls_count > 0
                or len(visited_urls) > 0
                or len(frontier) > 0
                or bool(url_inventory)
            ),
        }

    def load_session(self, root_domain: str, *, include_artifact_fallback: bool = True) -> Optional[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        if not rd:
            return None
        sql = """
SELECT root_domain, start_url, max_pages, saved_at_utc, payload
FROM coordinator_sessions
WHERE root_domain = %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (rd,))
                row = cur.fetchone()
            conn.commit()
        db_session: Optional[dict[str, Any]] = None
        if row is not None:
            payload = row[4] if isinstance(row[4], dict) else {}
            db_session = self._normalize_session_payload(
                rd,
                payload,
                fallback_start_url=str(row[1] or "").strip(),
                fallback_saved_at_utc=row[3].isoformat() if row[3] else None,
                fallback_max_pages=row[2],
            )
        if db_session is not None:
            db_metrics = self._session_url_metrics(db_session)
            if bool(db_metrics.get("has_session_data")):
                return db_session
        if not include_artifact_fallback:
            return db_session
        artifact_session = self._normalize_session_payload(rd, self._load_session_artifact_payload(rd))
        if artifact_session is None:
            return db_session
        if db_session is None:
            return artifact_session
        def _parse_ts(value: Any) -> Optional[datetime]:
            raw = str(value or "").strip()
            if not raw:
                return None
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except Exception:
                return None
        db_saved = _parse_ts(db_session.get("saved_at_utc"))
        artifact_saved = _parse_ts(artifact_session.get("saved_at_utc"))
        if artifact_saved and (db_saved is None or artifact_saved >= db_saved):
            return artifact_session
        db_state = db_session.get("state") if isinstance(db_session.get("state"), dict) else {}
        artifact_state = artifact_session.get("state") if isinstance(artifact_session.get("state"), dict) else {}
        db_urls = db_state.get("discovered_urls") if isinstance(db_state.get("discovered_urls"), list) else []
        artifact_urls = artifact_state.get("discovered_urls") if isinstance(artifact_state.get("discovered_urls"), list) else []
        if len(artifact_urls) > len(db_urls):
            return artifact_session
        return db_session

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
        safe_limit = max(1, min(20000, int(limit or 2000)))
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
ORDER BY
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM coordinator_stage_tasks s
      WHERE s.root_domain = d.root_domain
    ) THEN 0
    ELSE 1
  END ASC,
  root_domain ASC
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
            "mode": "admin_snapshot",
        }

    def workflow_domain_scheduler_state(self, root_domain: str) -> dict[str, Any]:
        rd = str(root_domain or "").strip().lower()
        if not rd:
            return {"generated_at_utc": _iso_now(), "found": False, "root_domain": rd}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
SELECT workflow_id, root_domain, stage, status, attempt_count, exit_code, error, updated_at_utc, completed_at_utc,
       checkpoint_json, progress_json, progress_artifact_type, resume_mode, worker_id,
       resource_class, access_mode, concurrency_group, max_parallelism,
       progress_percent, current_unit, last_milestone
FROM coordinator_stage_tasks
WHERE root_domain = %s
ORDER BY workflow_id ASC, stage ASC;
""",
                    (rd,),
                )
                stage_rows = cur.fetchall()
                cur.execute(
                    """
SELECT artifact_type, updated_at_utc, storage_backend, storage_uri, content_sha256, content_size_bytes,
       manifest_json, retention_class, summary_match_count, summary_anomaly_count, summary_request_count
FROM coordinator_artifacts
WHERE root_domain = %s
ORDER BY artifact_type ASC;
""",
                    (rd,),
                )
                artifact_rows = cur.fetchall()
                cur.execute(
                    """
SELECT
  COUNT(*) FILTER (WHERE status = 'pending') AS pending_targets,
  COUNT(*) FILTER (WHERE status = 'running') AS running_targets,
  COUNT(*) FILTER (WHERE status = 'completed') AS completed_targets,
  COUNT(*) FILTER (WHERE status = 'failed') AS failed_targets
FROM coordinator_targets
WHERE root_domain = %s;
""",
                    (rd,),
                )
                target_row = cur.fetchone()
            conn.commit()
        stage_map: dict[str, dict[str, Any]] = {}
        workflows: dict[str, dict[str, Any]] = {}
        for row in stage_rows:
            wid = str(row[0] or "default").strip().lower() or "default"
            stg = str(row[2] or "").strip().lower()
            payload = {
                "workflow_id": wid,
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
                "resource_class": str(row[14] or "default"),
                "access_mode": str(row[15] or "write"),
                "concurrency_group": str(row[16] or ""),
                "max_parallelism": int(row[17] or 1),
                "progress_percent": float(row[18] or 0.0),
                "current_unit": str(row[19] or ""),
                "last_milestone": str(row[20] or ""),
            }
            workflows.setdefault(wid, {})[stg] = payload
            if wid == "default":
                stage_map[stg] = payload
        artifacts = []
        for row in artifact_rows:
            artifacts.append(
                {
                    "artifact_type": str(row[0] or "").strip().lower(),
                    "updated_at_utc": row[1].isoformat() if row[1] else None,
                    "storage_backend": str(row[2] or ""),
                    "storage_uri": str(row[3] or ""),
                    "content_sha256": str(row[4] or ""),
                    "content_size_bytes": int(row[5] or 0),
                    "manifest": row[6] if isinstance(row[6], dict) else {},
                    "retention_class": str(row[7] or "derived_rebuildable"),
                    "summary_match_count": int(row[8] or 0),
                    "summary_anomaly_count": int(row[9] or 0),
                    "summary_request_count": int(row[10] or 0),
                }
            )
        targets = {
            "pending": int(target_row[0] or 0) if target_row else 0,
            "running": int(target_row[1] or 0) if target_row else 0,
            "completed": int(target_row[2] or 0) if target_row else 0,
            "failed": int(target_row[3] or 0) if target_row else 0,
        }
        return {
            "generated_at_utc": _iso_now(),
            "found": bool(stage_rows or artifact_rows or sum(targets.values()) > 0),
            "root_domain": rd,
            "targets": targets,
            "plugin_tasks": workflows,
            "stage_tasks": stage_map,
            "artifact_types": [item["artifact_type"] for item in artifacts],
            "artifacts": artifacts,
        }

    def count_stage_tasks(
        self,
        *,
        workflow_id: str = "",
        root_domains: Optional[list[str]] = None,
        plugins: Optional[list[str]] = None,
    ) -> int:
        widf = str(workflow_id or "").strip().lower()
        domains = sorted(
            {
                str(item or "").strip().lower()
                for item in (root_domains or [])
                if str(item or "").strip()
            }
        )
        stages = sorted(
            {
                str(item or "").strip().lower()
                for item in (plugins or [])
                if str(item or "").strip()
            }
        )
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
        query = f"SELECT COUNT(*) FROM coordinator_stage_tasks WHERE {' AND '.join(where_sql)};"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                row = cur.fetchone()
            conn.commit()
        return int((row[0] if row else 0) or 0)

    def crawl_progress_snapshot(self, *, limit: int = 2000) -> dict[str, Any]:
        safe_limit = max(1, min(2000, int(limit or 2000)))
        sql = """
WITH domain_set AS (
    SELECT DISTINCT root_domain FROM coordinator_targets
    UNION
    SELECT DISTINCT root_domain FROM coordinator_stage_tasks
    UNION
    SELECT DISTINCT root_domain FROM coordinator_sessions
    UNION
    SELECT DISTINCT root_domain FROM coordinator_artifacts WHERE artifact_type = 'nightmare_session_json'
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
),
artifact_agg AS (
    SELECT
      root_domain,
      MAX(updated_at_utc) FILTER (WHERE artifact_type = 'nightmare_session_json') AS nightmare_session_updated_at_utc
    FROM coordinator_artifacts
    GROUP BY root_domain
)
SELECT
  d.root_domain,
  COALESCE(t.sample_start_url, sess.start_url, '') AS start_url,
  sess.saved_at_utc,
  COALESCE(
    GREATEST(t.last_target_heartbeat, st.last_stage_heartbeat, sess.saved_at_utc, art.nightmare_session_updated_at_utc),
    GREATEST(t.last_target_heartbeat, st.last_stage_heartbeat, sess.saved_at_utc),
    GREATEST(t.last_target_heartbeat, st.last_stage_heartbeat, art.nightmare_session_updated_at_utc),
    GREATEST(t.last_target_heartbeat, sess.saved_at_utc, art.nightmare_session_updated_at_utc),
    GREATEST(st.last_stage_heartbeat, sess.saved_at_utc, art.nightmare_session_updated_at_utc),
    t.last_target_heartbeat,
    st.last_stage_heartbeat,
    sess.saved_at_utc,
    art.nightmare_session_updated_at_utc
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
  COALESCE(st.stage_workers, ARRAY[]::text[]) AS stage_workers,
  art.nightmare_session_updated_at_utc
FROM domain_set d
LEFT JOIN target_agg t ON t.root_domain = d.root_domain
LEFT JOIN stage_agg st ON st.root_domain = d.root_domain
LEFT JOIN coordinator_sessions sess ON sess.root_domain = d.root_domain
LEFT JOIN artifact_agg art ON art.root_domain = d.root_domain
ORDER BY last_activity_at_utc DESC NULLS LAST, d.root_domain ASC
LIMIT %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (safe_limit,))
                rows = cur.fetchall()
            conn.commit()

        row_map: dict[str, tuple[Any, ...]] = {}
        ordered_domains: list[str] = []
        for row in rows:
            root_domain = str(row[0] or "").strip().lower()
            if not root_domain:
                continue
            ordered_domains.append(root_domain)
            row_map[root_domain] = row

        sessions_by_domain = self._bulk_load_sessions(
            ordered_domains,
            include_artifact_fallback=True,
            artifact_fallback_limit=max(25, min(250, len(ordered_domains))),
        )

        now_utc = datetime.now(timezone.utc)
        domains: list[dict[str, Any]] = []
        running_domains = 0
        queued_domains = 0
        failed_domains = 0
        completed_domains = 0

        for root_domain in ordered_domains:
            row = row_map[root_domain]
            session = sessions_by_domain.get(root_domain) or {}
            metrics = self._session_url_metrics(session)
            discovered_urls_count = int(metrics.get("discovered_urls_count") or 0)
            visited_urls_count = int(metrics.get("visited_urls_count") or 0)
            frontier_count = int(metrics.get("frontier_count") or 0)
            spider_stats = metrics.get("spider_stats") if isinstance(metrics.get("spider_stats"), dict) else {}

            active_stages_raw = row[12] if isinstance(row[12], list) else []
            active_stages = [str(item).strip() for item in active_stages_raw if str(item or "").strip()]
            target_workers_raw = row[13] if isinstance(row[13], list) else []
            stage_workers_raw = row[14] if isinstance(row[14], list) else []
            active_workers = sorted({
                str(item).strip()
                for item in [*target_workers_raw, *stage_workers_raw]
                if str(item or "").strip()
            })

            last_activity = row[3]
            for raw_ts in (
                session.get("saved_at_utc"),
                row[15].isoformat() if row[15] else None,
                row[2].isoformat() if row[2] else None,
            ):
                if not raw_ts:
                    continue
                try:
                    dt = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
                except Exception:
                    continue
                if last_activity is None or dt > last_activity:
                    last_activity = dt

            last_activity_iso: Optional[str] = None
            seconds_since_activity: Optional[int] = None
            if last_activity is not None:
                last_activity_iso = last_activity.isoformat()
                seconds_since_activity = max(0, int((now_utc - last_activity).total_seconds()))

            pending_targets = int(row[4] or 0)
            running_targets = int(row[5] or 0)
            completed_targets = int(row[6] or 0)
            failed_targets = int(row[7] or 0)
            pending_stage_tasks = int(row[8] or 0)
            running_stage_tasks = int(row[9] or 0)
            completed_stage_tasks = int(row[10] or 0)
            failed_stage_tasks = int(row[11] or 0)

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
                phase = "stage_pending"
            elif failed_targets > 0 or failed_stage_tasks > 0:
                phase = "failed"
            elif completed_targets > 0 or completed_stage_tasks > 0 or discovered_urls_count > 0:
                phase = "completed"

            if phase.endswith("_running"):
                running_domains += 1
            elif phase.endswith("_pending"):
                queued_domains += 1
            elif phase == "failed":
                failed_domains += 1
            elif phase == "completed":
                completed_domains += 1

            domains.append({
                "root_domain": root_domain,
                "start_url": str(session.get("start_url") or row[1] or ""),
                "phase": phase,
                "discovered_urls_count": discovered_urls_count,
                "visited_urls_count": visited_urls_count,
                "frontier_count": frontier_count,
                "spider_stats": spider_stats,
                "session_saved_at_utc": session.get("saved_at_utc") or (row[2].isoformat() if row[2] else None),
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
            })

        return {
            "generated_at_utc": _iso_now(),
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
stage_status AS (
    SELECT
      root_domain,
      COUNT(*) FILTER (WHERE status = 'pending') AS pending_stage_tasks,
      COUNT(*) FILTER (WHERE status = 'running') AS running_stage_tasks,
      COUNT(*) FILTER (WHERE status = 'completed') AS completed_stage_tasks,
      COUNT(*) FILTER (WHERE status = 'failed') AS failed_stage_tasks
    FROM coordinator_stage_tasks
    GROUP BY root_domain
),
artifact_status AS (
    SELECT
      root_domain,
      MAX(updated_at_utc) FILTER (WHERE artifact_type = 'nightmare_session_json') AS nightmare_session_updated_at_utc
    FROM coordinator_artifacts
    GROUP BY root_domain
),
domain_rows AS (
    SELECT DISTINCT root_domain FROM coordinator_sessions
    UNION
    SELECT DISTINCT root_domain FROM coordinator_targets
    UNION
    SELECT DISTINCT root_domain FROM coordinator_stage_tasks
    UNION
    SELECT DISTINCT root_domain FROM coordinator_artifacts WHERE artifact_type = 'nightmare_session_json'
)
SELECT
  d.root_domain,
  COALESCE(sess.start_url, '') AS start_url,
  sess.saved_at_utc,
  COALESCE(ts.pending_targets, 0) AS pending_targets,
  COALESCE(ts.running_targets, 0) AS running_targets,
  COALESCE(ts.completed_targets, 0) AS completed_targets,
  COALESCE(ts.failed_targets, 0) AS failed_targets,
  COALESCE(ss.pending_stage_tasks, 0) AS pending_stage_tasks,
  COALESCE(ss.running_stage_tasks, 0) AS running_stage_tasks,
  COALESCE(ss.completed_stage_tasks, 0) AS completed_stage_tasks,
  COALESCE(ss.failed_stage_tasks, 0) AS failed_stage_tasks,
  art.nightmare_session_updated_at_utc
FROM domain_rows d
LEFT JOIN coordinator_sessions sess ON sess.root_domain = d.root_domain
LEFT JOIN target_status ts ON ts.root_domain = d.root_domain
LEFT JOIN stage_status ss ON ss.root_domain = d.root_domain
LEFT JOIN artifact_status art ON art.root_domain = d.root_domain
ORDER BY COALESCE(art.nightmare_session_updated_at_utc, sess.saved_at_utc, NOW()) DESC, d.root_domain ASC
LIMIT %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (safe_limit,))
                rows = cur.fetchall()
            conn.commit()

        row_map: dict[str, tuple[Any, ...]] = {}
        ordered_domains: list[str] = []
        for row in rows:
            root_domain = str(row[0] or "").strip().lower()
            if not root_domain:
                continue
            if needle and needle not in root_domain:
                continue
            ordered_domains.append(root_domain)
            row_map[root_domain] = row

        sessions_by_domain = self._bulk_load_sessions(
            ordered_domains,
            include_artifact_fallback=True,
            artifact_fallback_limit=max(25, min(300, len(ordered_domains))),
        )

        rows_out: list[dict[str, Any]] = []
        for root_domain in ordered_domains:
            row = row_map[root_domain]
            session = sessions_by_domain.get(root_domain) or {}
            metrics = self._session_url_metrics(session)
            discovered_count = int(metrics.get("discovered_urls_count") or 0)
            method_counts = metrics.get("method_counts") if isinstance(metrics.get("method_counts"), dict) else {}

            pending_targets = int(row[3] or 0)
            running_targets = int(row[4] or 0)
            completed_targets = int(row[5] or 0)
            failed_targets = int(row[6] or 0)
            pending_stage_tasks = int(row[7] or 0)
            running_stage_tasks = int(row[8] or 0)
            completed_stage_tasks = int(row[9] or 0)
            failed_stage_tasks = int(row[10] or 0)

            has_any_work = any((
                pending_targets, running_targets, completed_targets, failed_targets,
                pending_stage_tasks, running_stage_tasks, completed_stage_tasks, failed_stage_tasks,
            ))
            has_any_session = bool(metrics.get("has_session_data"))
            if not has_any_work and not has_any_session:
                continue

            if running_targets > 0 or running_stage_tasks > 0:
                status = "running"
            elif pending_targets > 0 or pending_stage_tasks > 0:
                status = "pending"
            elif failed_targets > 0 or failed_stage_tasks > 0:
                status = "failed" if (completed_targets + completed_stage_tasks) == 0 else "completed_with_failures"
            elif completed_targets > 0 or completed_stage_tasks > 0:
                status = "completed"
            elif discovered_count > 0:
                status = "discovered"
            else:
                status = "unknown"

            saved_at = session.get("saved_at_utc") or (row[11].isoformat() if row[11] else None) or (row[2].isoformat() if row[2] else None)
            rows_out.append({
                "root_domain": root_domain,
                "start_url": str(session.get("start_url") or row[1] or "").strip(),
                "saved_at_utc": saved_at,
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
        def _normalize_url(raw_url: Any) -> str:
            text = str(raw_url or "").strip()
            if not text:
                return ""
            parsed = urlparse(text)
            if parsed.scheme in {"http", "https"} and parsed.hostname:
                return text
            if start_url:
                try:
                    joined = urljoin(start_url, text)
                    joined_parsed = urlparse(joined)
                    if joined_parsed.scheme in {"http", "https"} and joined_parsed.hostname:
                        return joined
                except Exception:
                    return text
            return text

        discovered_set = {
            _normalize_url(u)
            for u in [*discovered_urls, *list(inventory.keys())]
            if str(u or "").strip()
        }
        discovered_set.discard("")
        normalized_link_graph: dict[str, set[str]] = {}
        discovered_from_map: dict[str, set[str]] = {}
        for src, targets in link_graph.items():
            src_text = _normalize_url(src)
            if not src_text:
                continue
            target_set: set[str] = set()
            for target in targets if isinstance(targets, list) else []:
                target_text = _normalize_url(target)
                if not target_text:
                    continue
                target_set.add(target_text)
                discovered_from_map.setdefault(target_text, set()).add(src_text)
            if target_set:
                normalized_link_graph.setdefault(src_text, set()).update(target_set)
        for url in sorted(discovered_set):
            host = str(urlparse(url).hostname or "").strip().lower()
            if not host:
                continue
            if host != rd and not host.endswith(f".{rd}"):
                continue
            record = self._lookup_inventory_record(inventory, url)
            discovered_via_raw = record.get("discovered_via")
            discovered_via = (
                [str(v or "").strip() for v in discovered_via_raw if str(v or "").strip()]
                if isinstance(discovered_via_raw, list)
                else ([str(discovered_via_raw or "").strip()] if str(discovered_via_raw or "").strip() else [])
            )
            discovered_from = sorted(discovered_from_map.get(url, set()))[:50]
            outbound_clean = sorted(normalized_link_graph.get(url, set()))
            crawl_status_code = record.get("crawl_status_code", record.get("status_code"))
            existence_status_code = record.get("existence_status_code")
            subdomain = ""
            if host:
                if host == rd:
                    subdomain = "@root"
                elif host.endswith(f".{rd}"):
                    subdomain = host[: -(len(rd) + 1)] or "@root"
                else:
                    subdomain = host
            parsed_url = urlparse(url)
            path_text = str(parsed_url.path or "/")
            query_text = str(parsed_url.query or "")
            evidence_files = (
                [str(item or "").strip() for item in record.get("discovery_evidence_files", [])]
                if isinstance(record.get("discovery_evidence_files"), list)
                else []
            )
            exists_confirmed = bool(record.get("exists_confirmed"))
            soft_404 = bool(record.get("soft_404_detected"))
            exists_status = "unknown"
            if soft_404:
                exists_status = "likely_soft_404"
            elif exists_confirmed:
                exists_status = "exists"
            elif isinstance(existence_status_code, int):
                if existence_status_code in {404, 410}:
                    exists_status = "missing"
                elif 200 <= existence_status_code < 400:
                    exists_status = "exists"
                elif existence_status_code >= 400:
                    exists_status = "error"
            elif isinstance(crawl_status_code, int):
                if 200 <= crawl_status_code < 400:
                    exists_status = "exists"
                elif crawl_status_code in {404, 410}:
                    exists_status = "missing"
                elif crawl_status_code >= 400:
                    exists_status = "error"
            page = {
                "url": url,
                "host": host,
                "subdomain": subdomain,
                "path": path_text,
                "query": query_text,
                "inbound_count": len(discovered_from),
                "outbound_count": len(outbound_clean),
                "discovered_via": discovered_via,
                "discovered_from": discovered_from,
                "was_crawled": bool(record.get("was_crawled")),
                "crawl_requested": bool(record.get("crawl_requested")),
                "exists_confirmed": exists_confirmed,
                "exists_status": exists_status,
                "exists_reason": str(
                    record.get("existence_check_note")
                    or record.get("crawl_note")
                    or record.get("soft_404_reason")
                    or ""
                ).strip(),
                "crawl_status_code": crawl_status_code,
                "existence_status_code": existence_status_code,
                "response_available": bool(evidence_files),
                "evidence_file_count": len([item for item in evidence_files if item]),
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

    def _read_discovery_evidence_payload(self, evidence_path: Path) -> dict[str, Any]:
        if not evidence_path.is_file():
            return {}
        try:
            if evidence_path.suffix.lower() == ".gz":
                with gzip.open(evidence_path, "rt", encoding="utf-8") as handle:
                    parsed = json.load(handle)
            else:
                parsed = json.loads(evidence_path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        try:
            if value is None or value == "":
                return None
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _resolve_header_value(headers: dict[str, Any], *names: str) -> str:
        if not isinstance(headers, dict):
            return ""
        lowered = {str(k or "").strip().lower(): str(v or "") for k, v in headers.items()}
        for name in names:
            value = lowered.get(str(name or "").strip().lower(), "")
            if value:
                return value
        return ""

    @staticmethod
    def _normalize_headers(value: Any) -> dict[str, str]:
        if not isinstance(value, dict):
            return {}
        out: dict[str, str] = {}
        for key, item in value.items():
            name = str(key or "").strip()
            if not name:
                continue
            out[name] = str(item or "")
        return out

    @staticmethod
    def _decode_body_base64_bytes(payload: Any) -> bytes:
        if not isinstance(payload, dict):
            return b""
        body_b64 = str(payload.get("body_base64") or "")
        if not body_b64:
            return b""
        try:
            return base64.b64decode(body_b64)
        except Exception:
            return b""

    @staticmethod
    def _decode_bytes_to_text(value: bytes) -> str:
        raw = bytes(value or b"")
        if not raw:
            return ""
        try:
            return raw.decode("utf-8")
        except Exception:
            return raw.decode("utf-8", errors="replace")

    @staticmethod
    def _to_json_text(value: Any) -> str:
        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
            except Exception:
                return str(value)
        if value is None:
            return ""
        return str(value)

    @staticmethod
    def _exists_status_from_row(
        *,
        exists_confirmed: bool,
        soft_404_detected: bool,
        existence_status_code: Optional[int],
        crawl_status_code: Optional[int],
    ) -> str:
        if soft_404_detected:
            return "likely_soft_404"
        if exists_confirmed:
            return "exists"
        if isinstance(existence_status_code, int):
            if existence_status_code in {404, 410}:
                return "missing"
            if 200 <= existence_status_code < 400:
                return "exists"
            if existence_status_code >= 400:
                return "error"
        if isinstance(crawl_status_code, int):
            if crawl_status_code in {404, 410}:
                return "missing"
            if 200 <= crawl_status_code < 400:
                return "exists"
            if crawl_status_code >= 400:
                return "error"
        return "unknown"

    def _pick_best_discovery_evidence(self, evidence_files: list[str]) -> tuple[dict[str, Any], str]:
        best_payload: dict[str, Any] = {}
        best_path = ""
        best_score = -1
        for raw_path in reversed(list(evidence_files or [])):
            text = str(raw_path or "").strip()
            if not text:
                continue
            candidate = Path(text).expanduser()
            if not candidate.is_absolute():
                candidate = (Path.cwd() / candidate).resolve()
            payload = self._read_discovery_evidence_payload(candidate)
            if not payload:
                continue
            source_type = str(payload.get("source_type") or "").strip().lower()
            score = 0
            if source_type == "existence_probe":
                score += 3
            elif source_type == "crawl_response":
                score += 2
            if isinstance(payload.get("response"), dict):
                score += 2
            if isinstance(payload.get("request"), dict):
                score += 1
            if score > best_score:
                best_score = score
                best_payload = payload
                best_path = str(candidate)
            if score >= 6:
                break
        return best_payload, best_path

    @staticmethod
    def _lookup_inventory_record(inventory: dict[str, Any], raw_url: str) -> dict[str, Any]:
        text = str(raw_url or "").strip()
        if not text or not isinstance(inventory, dict):
            return {}
        if isinstance(inventory.get(text), dict):
            return inventory.get(text)  # type: ignore[return-value]
        normalized = text.split("#", 1)[0].strip()
        if isinstance(inventory.get(normalized), dict):
            return inventory.get(normalized)  # type: ignore[return-value]
        if normalized.endswith("/") and isinstance(inventory.get(normalized[:-1]), dict):
            return inventory.get(normalized[:-1])  # type: ignore[return-value]
        if (normalized + "/") in inventory and isinstance(inventory.get(normalized + "/"), dict):
            return inventory.get(normalized + "/")  # type: ignore[return-value]
        return {}

    def _build_discovered_target_row_payload(
        self,
        *,
        root_domain: str,
        url: str,
        record: dict[str, Any],
        include_body: bool,
    ) -> dict[str, Any]:
        evidence_files = (
            [str(item or "").strip() for item in record.get("discovery_evidence_files", [])]
            if isinstance(record.get("discovery_evidence_files"), list)
            else []
        )
        evidence_payload, evidence_path = self._pick_best_discovery_evidence(evidence_files)
        request_payload = evidence_payload.get("request") if isinstance(evidence_payload.get("request"), dict) else {}
        response_payload = evidence_payload.get("response") if isinstance(evidence_payload.get("response"), dict) else {}
        response_headers = response_payload.get("headers") if isinstance(response_payload.get("headers"), dict) else {}
        request_headers = request_payload.get("headers") if isinstance(request_payload.get("headers"), dict) else {}
        response_status = self._coerce_int(response_payload.get("status"))
        response_elapsed_ms = self._coerce_int(response_payload.get("elapsed_ms"))
        response_size = self._coerce_int(response_payload.get("body_size_bytes"))
        if response_size is None:
            response_size = self._coerce_int(response_payload.get("body_size"))
        if response_size is None:
            body_b64 = str(response_payload.get("body_base64") or "")
            if body_b64:
                try:
                    response_size = len(base64.b64decode(body_b64))
                except Exception:
                    response_size = None

        crawl_status_code = self._coerce_int(record.get("crawl_status_code", record.get("status_code")))
        existence_status_code = self._coerce_int(record.get("existence_status_code"))
        exists_confirmed = bool(record.get("exists_confirmed"))
        soft_404_detected = bool(record.get("soft_404_detected"))
        exists_status = self._exists_status_from_row(
            exists_confirmed=exists_confirmed,
            soft_404_detected=soft_404_detected,
            existence_status_code=existence_status_code,
            crawl_status_code=crawl_status_code,
        )
        exists_reason = str(
            record.get("existence_check_note")
            or record.get("crawl_note")
            or record.get("soft_404_reason")
            or ""
        ).strip()
        request_method = str(request_payload.get("method") or record.get("existence_check_method") or "").strip().upper()
        view_path = (
            f"/discovered-target-response?root_domain={quote(root_domain, safe='')}&url={quote(url, safe='')}"
        )
        download_base = (
            f"/api/coord/discovered-target-download?root_domain={quote(root_domain, safe='')}&url={quote(url, safe='')}"
        )
        summary = {
            "exists_status": exists_status,
            "exists_reason": exists_reason,
            "probe_note": str(record.get("existence_check_note") or "").strip(),
            "request_method": request_method,
            "response_status_code": response_status if response_status is not None else crawl_status_code,
            "response_url": str(response_payload.get("url") or ""),
            "response_elapsed_ms": response_elapsed_ms,
            "response_size_bytes": response_size,
            "response_content_type": self._resolve_header_value(
                response_headers,
                "content-type",
            )
            or str(record.get("content_type") or ""),
            "body_truncated": bool(response_payload.get("body_truncated")),
            "soft_404_detected": soft_404_detected,
            "soft_404_reason": str(record.get("soft_404_reason") or "").strip(),
            "captured_at_utc": str(evidence_payload.get("captured_at_utc") or ""),
            "evidence_source_type": str(evidence_payload.get("source_type") or ""),
            "evidence_path": evidence_path,
        }
        request_out = dict(request_payload)
        response_out = dict(response_payload)
        if not include_body:
            request_out.pop("body_base64", None)
            response_out.pop("body_base64", None)
        payload = {
            "found": True,
            "root_domain": root_domain,
            "url": url,
            "summary": summary,
            "request": request_out,
            "response": response_out,
            "captured_at_utc": summary["captured_at_utc"],
            "discovered_via": record.get("discovered_via") if isinstance(record.get("discovered_via"), list) else [],
            "discovered_from": record.get("discovered_from") if isinstance(record.get("discovered_from"), list) else [],
            "response_available": bool(response_out),
            "view_response_path": view_path,
            "download_request_path": f"{download_base}&part=request_json",
            "download_response_path": f"{download_base}&part=response_json",
            "download_response_body_path": f"{download_base}&part=response_body",
            "download_request_body_path": f"{download_base}&part=request_body",
            "download_evidence_path": f"{download_base}&part=evidence_json",
            "evidence_path": evidence_path,
            "evidence_file_count": len([item for item in evidence_files if item]),
        }
        return payload

    def get_discovered_target_response(self, root_domain: str, url: str) -> dict[str, Any]:
        rd = str(root_domain or "").strip().lower()
        target_url = str(url or "").strip()
        if not rd or not target_url:
            return {"found": False, "root_domain": rd, "url": target_url, "error": "root_domain and url are required"}
        session = self.load_session(rd) or {}
        state = session.get("state") if isinstance(session.get("state"), dict) else {}
        inventory = state.get("url_inventory") if isinstance(state.get("url_inventory"), dict) else {}
        record = self._lookup_inventory_record(inventory, target_url)
        if not record:
            return {"found": False, "root_domain": rd, "url": target_url, "error": "url not found in discovered inventory"}
        payload = self._build_discovered_target_row_payload(
            root_domain=rd,
            url=target_url,
            record=record,
            include_body=True,
        )
        payload["start_url"] = str(session.get("start_url") or "").strip()
        payload["saved_at_utc"] = str(session.get("saved_at_utc") or "").strip()
        return payload

    def enrich_discovered_target_sitemap_rows(
        self,
        *,
        root_domain: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        if not rd or not rows:
            return []
        session = self.load_session(rd) or {}
        state = session.get("state") if isinstance(session.get("state"), dict) else {}
        inventory = state.get("url_inventory") if isinstance(state.get("url_inventory"), dict) else {}
        enriched: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_url = str(row.get("url") or "").strip()
            if not row_url:
                enriched.append(dict(row))
                continue
            record = self._lookup_inventory_record(inventory, row_url)
            if not record:
                enriched.append(dict(row))
                continue
            details = self._build_discovered_target_row_payload(
                root_domain=rd,
                url=row_url,
                record=record,
                include_body=False,
            )
            summary = details.get("summary") if isinstance(details.get("summary"), dict) else {}
            merged = dict(row)
            merged.update(
                {
                    "exists_status": str(summary.get("exists_status") or merged.get("exists_status") or "unknown"),
                    "exists_reason": str(summary.get("exists_reason") or merged.get("exists_reason") or ""),
                    "response_status_code": summary.get("response_status_code"),
                    "response_elapsed_ms": summary.get("response_elapsed_ms"),
                    "response_size_bytes": summary.get("response_size_bytes"),
                    "response_content_type": str(summary.get("response_content_type") or merged.get("content_type") or ""),
                    "captured_at_utc": str(summary.get("captured_at_utc") or ""),
                    "request_method": str(summary.get("request_method") or ""),
                    "response_available": bool(details.get("response_available")),
                    "view_response_path": str(details.get("view_response_path") or ""),
                    "download_request_path": str(details.get("download_request_path") or ""),
                    "download_response_path": str(details.get("download_response_path") or ""),
                    "download_response_body_path": str(details.get("download_response_body_path") or ""),
                    "download_request_body_path": str(details.get("download_request_body_path") or ""),
                    "download_evidence_path": str(details.get("download_evidence_path") or ""),
                    "evidence_path": str(details.get("evidence_path") or ""),
                    "evidence_file_count": int(details.get("evidence_file_count") or merged.get("evidence_file_count") or 0),
                }
            )
            enriched.append(merged)
        return enriched

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

    def list_http_requests(
        self,
        *,
        limit: int = 5000,
        q: str = "",
        root_domain: str = "",
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(50000, int(limit or 5000)))
        domain_filter = str(root_domain or "").strip().lower()
        needle = str(q or "").strip().lower()
        domain_scan_limit = 50000

        domains: list[str] = []
        if domain_filter:
            domains = [domain_filter]
        else:
            sql = """
SELECT root_domain
FROM (
    SELECT DISTINCT root_domain
    FROM coordinator_artifacts
    WHERE artifact_type IN ('nightmare_requests_json', 'nightmare_session_json')
    UNION
    SELECT DISTINCT root_domain FROM coordinator_sessions
    UNION
    SELECT DISTINCT root_domain FROM coordinator_stage_tasks
) d
WHERE root_domain IS NOT NULL AND root_domain <> ''
ORDER BY root_domain ASC
LIMIT %s;
"""
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (domain_scan_limit,))
                    rows = cur.fetchall()
                conn.commit()
            domains = [str(row[0] or "").strip().lower() for row in rows if str(row[0] or "").strip()]

        stage_rows_by_domain: dict[str, list[dict[str, Any]]] = {}
        if domains:
            stage_sql = """
SELECT root_domain, workflow_id, stage, worker_id, progress_json, updated_at_utc
FROM coordinator_stage_tasks
WHERE root_domain = ANY(%s)
ORDER BY updated_at_utc DESC NULLS LAST;
"""
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(stage_sql, (domains,))
                    rows = cur.fetchall()
                conn.commit()
            for row in rows:
                rd = str(row[0] or "").strip().lower()
                if not rd:
                    continue
                stage_rows_by_domain.setdefault(rd, []).append(
                    {
                        "workflow_id": str(row[1] or "").strip().lower() or "default",
                        "stage": str(row[2] or "").strip().lower(),
                        "worker_id": str(row[3] or "").strip(),
                        "progress_json": row[4] if isinstance(row[4], dict) else {},
                        "updated_at_utc": row[5].isoformat() if row[5] else "",
                    }
                )

        out: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        def _files_link(rd: str, relative_path: str) -> str:
            rel = str(relative_path or "").strip().replace("\\", "/").lstrip("/")
            if not rel:
                return ""
            return f"/files/{quote(rd, safe='')}/{quote(rel, safe='/._-')}"

        def _blob_link(rd: str, blob: Any) -> str:
            if not isinstance(blob, dict):
                return ""
            rel = str(blob.get("relative_path") or "").strip().replace("\\", "/").lstrip("/")
            if not rel:
                return ""
            return f"/files/{quote(rd, safe='')}/collected_data/{quote(rel, safe='/._-')}"

        def _append_row(row: dict[str, Any]) -> None:
            row_id = str(row.get("id") or "").strip()
            if not row_id or row_id in seen_ids:
                return
            if needle:
                haystack = " ".join(
                    [
                        str(row.get("root_domain") or ""),
                        str(row.get("source") or ""),
                        str(row.get("request_url") or ""),
                        str(row.get("request_method") or ""),
                        self._to_json_text(row.get("request_headers") or {}),
                        str(row.get("request_body") or ""),
                        str(row.get("response_code") or ""),
                        self._to_json_text(row.get("response_headers") or {}),
                        str(row.get("response_body") or ""),
                    ]
                ).lower()
                if needle not in haystack:
                    return
            seen_ids.add(row_id)
            out.append(row)

        for rd in domains:
            artifact = self.get_artifact(rd, "nightmare_requests_json")
            if isinstance(artifact, dict):
                payload = self._decode_json_bytes(bytes(artifact.get("content") or b""))
                events = payload.get("request_events") if isinstance(payload.get("request_events"), list) else []
                updated_at = str(artifact.get("updated_at_utc") or "")
                for idx, event in enumerate(events):
                    if not isinstance(event, dict):
                        continue
                    request_payload = event.get("request") if isinstance(event.get("request"), dict) else {}
                    response_payload = event.get("response") if isinstance(event.get("response"), dict) else {}
                    request_headers = self._normalize_headers(request_payload.get("headers"))
                    response_headers = self._normalize_headers(response_payload.get("headers"))
                    request_body = self._decode_body_base64_bytes(request_payload)
                    response_body = self._decode_body_base64_bytes(response_payload)
                    request_blob = request_payload.get("body_blob")
                    response_blob = response_payload.get("body_blob")
                    captured_at = str(event.get("captured_at_utc") or "").strip() or updated_at
                    request_url = str(
                        request_payload.get("url")
                        or response_payload.get("url")
                        or ""
                    ).strip()
                    response_code = self._coerce_int(response_payload.get("status"))
                    response_elapsed_ms = self._coerce_int(response_payload.get("elapsed_ms"))
                    request_size_bytes = self._coerce_int(request_payload.get("body_size_bytes"))
                    response_size_bytes = self._coerce_int(response_payload.get("body_size_bytes"))
                    if request_size_bytes is None:
                        request_size_bytes = len(request_body)
                    if response_size_bytes is None:
                        response_size_bytes = len(response_body)
                    event_id = str(event.get("event_id") or "").strip() or f"request_event_{idx}"
                    _append_row(
                        {
                            "id": f"{rd}:artifact:{event_id}",
                            "source": "nightmare_requests_json",
                            "root_domain": rd,
                            "worker_id": "",
                            "captured_at_utc": captured_at,
                            "request_url": request_url,
                            "request_method": str(request_payload.get("method") or "").strip().upper(),
                            "request_headers": request_headers,
                            "request_body": self._decode_bytes_to_text(request_body),
                            "request_size_bytes": int(request_size_bytes or 0),
                            "response_code": int(response_code or 0),
                            "response_size_bytes": int(response_size_bytes or 0),
                            "response_time_ms": int(response_elapsed_ms or 0),
                            "response_headers": response_headers,
                            "response_body": self._decode_bytes_to_text(response_body),
                            "request_link": _files_link(rd, str(event.get("raw_request_link") or "")),
                            "response_link": _files_link(rd, str(event.get("raw_response_link") or "")),
                            "request_body_link": _blob_link(rd, request_blob),
                            "response_body_link": _blob_link(rd, response_blob),
                        }
                    )

            session = self.load_session(rd, include_artifact_fallback=False) or {}
            state = session.get("state") if isinstance(session.get("state"), dict) else {}
            inventory = state.get("url_inventory") if isinstance(state.get("url_inventory"), dict) else {}
            for inventory_url, record in inventory.items():
                if not isinstance(record, dict):
                    continue
                evidence_files = record.get("discovery_evidence_files")
                if not isinstance(evidence_files, list):
                    continue
                for raw_path in evidence_files:
                    text_path = str(raw_path or "").strip()
                    if not text_path:
                        continue
                    evidence_path = Path(text_path).expanduser()
                    if not evidence_path.is_absolute():
                        evidence_path = (Path.cwd() / evidence_path).resolve()
                    payload = self._read_discovery_evidence_payload(evidence_path)
                    request_payload = payload.get("request") if isinstance(payload.get("request"), dict) else {}
                    response_payload = payload.get("response") if isinstance(payload.get("response"), dict) else {}
                    if not request_payload:
                        continue
                    request_headers = self._normalize_headers(request_payload.get("headers"))
                    response_headers = self._normalize_headers(response_payload.get("headers"))
                    request_body = self._decode_body_base64_bytes(request_payload)
                    response_body = self._decode_body_base64_bytes(response_payload)
                    request_url = str(
                        request_payload.get("url")
                        or inventory_url
                        or response_payload.get("url")
                        or ""
                    ).strip()
                    if not request_url:
                        continue
                    response_code = self._coerce_int(response_payload.get("status"))
                    response_elapsed_ms = self._coerce_int(response_payload.get("elapsed_ms"))
                    request_size_bytes = self._coerce_int(request_payload.get("body_size_bytes"))
                    response_size_bytes = self._coerce_int(response_payload.get("body_size_bytes"))
                    if request_size_bytes is None:
                        request_size_bytes = len(request_body)
                    if response_size_bytes is None:
                        response_size_bytes = len(response_body)
                    download_base = (
                        f"/api/coord/discovered-target-download?root_domain={quote(rd, safe='')}&url={quote(request_url, safe='')}"
                    )
                    _append_row(
                        {
                            "id": f"{rd}:evidence:{str(evidence_path)}",
                            "source": str(payload.get("source_type") or "discovery_evidence"),
                            "root_domain": rd,
                            "worker_id": "",
                            "captured_at_utc": str(payload.get("captured_at_utc") or ""),
                            "request_url": request_url,
                            "request_method": str(request_payload.get("method") or "").strip().upper(),
                            "request_headers": request_headers,
                            "request_body": self._decode_bytes_to_text(request_body),
                            "request_size_bytes": int(request_size_bytes or 0),
                            "response_code": int(response_code or 0),
                            "response_size_bytes": int(response_size_bytes or 0),
                            "response_time_ms": int(response_elapsed_ms or 0),
                            "response_headers": response_headers,
                            "response_body": self._decode_bytes_to_text(response_body),
                            "request_link": f"{download_base}&part=request_json",
                            "response_link": f"{download_base}&part=response_json",
                            "request_body_link": f"{download_base}&part=request_body",
                            "response_body_link": f"{download_base}&part=response_body",
                        }
                    )

            progress_rows = stage_rows_by_domain.get(rd, [])
            for stage_row in progress_rows:
                progress_payload = stage_row.get("progress_json") if isinstance(stage_row.get("progress_json"), dict) else {}
                requests_made = progress_payload.get("requests_made")
                if not isinstance(requests_made, list):
                    continue
                for idx, item in enumerate(requests_made):
                    if not isinstance(item, dict):
                        continue
                    request_url = str(
                        item.get("url")
                        or item.get("endpoint")
                        or item.get("resolved_url")
                        or item.get("start_url")
                        or ""
                    ).strip()
                    request_method = str(item.get("method") or item.get("request_method") or "").strip().upper()
                    if not request_method and str(item.get("type") or "").strip().lower() == "probe":
                        request_method = "GET"
                    request_headers = self._normalize_headers(item.get("headers"))
                    request_body = ""
                    command_value = item.get("command")
                    if isinstance(command_value, list):
                        request_body = " ".join(str(part or "") for part in command_value)
                    elif isinstance(command_value, str):
                        request_body = command_value
                    elif isinstance(item.get("body"), (str, dict, list)):
                        request_body = self._to_json_text(item.get("body"))
                    response_code = self._coerce_int(item.get("status_code"))
                    response_elapsed_ms = self._coerce_int(item.get("elapsed_ms"))
                    response_headers = self._normalize_headers(item.get("response_headers"))
                    response_body = self._to_json_text(item.get("response_body"))
                    captured_at = str(item.get("started_at_utc") or item.get("captured_at_utc") or stage_row.get("updated_at_utc") or "")
                    request_size_bytes = self._coerce_int(item.get("request_size_bytes"))
                    response_size_bytes = self._coerce_int(item.get("response_size_bytes"))
                    if request_size_bytes is None:
                        request_size_bytes = len(request_body.encode("utf-8", errors="replace"))
                    if response_size_bytes is None:
                        response_size_bytes = len(response_body.encode("utf-8", errors="replace")) if response_body else 0
                    _append_row(
                        {
                            "id": f"{rd}:progress:{stage_row.get('workflow_id')}:{stage_row.get('stage')}:{idx}:{captured_at}:{request_url}",
                            "source": f"progress:{stage_row.get('stage')}",
                            "root_domain": rd,
                            "worker_id": str(stage_row.get("worker_id") or ""),
                            "captured_at_utc": captured_at,
                            "request_url": request_url,
                            "request_method": request_method,
                            "request_headers": request_headers,
                            "request_body": request_body,
                            "request_size_bytes": int(request_size_bytes or 0),
                            "response_code": int(response_code or 0),
                            "response_size_bytes": int(response_size_bytes or 0),
                            "response_time_ms": int(response_elapsed_ms or 0),
                            "response_headers": response_headers,
                            "response_body": response_body,
                            "request_link": "",
                            "response_link": "",
                            "request_body_link": "",
                            "response_body_link": "",
                        }
                    )

        out.sort(
            key=lambda row: (
                str(row.get("captured_at_utc") or ""),
                str(row.get("root_domain") or ""),
                str(row.get("request_url") or ""),
            ),
            reverse=True,
        )
        return out[:safe_limit]


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
            if state in {"paused", "stopped", "errored"}:
                return state
            # "running" historically meant "the worker loop is alive".  A worker
            # with no leased target/stage task is available, not actively running.
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
        sql = """
SELECT created_at_utc, event_type, aggregate_key, message, payload_json
FROM coordinator_recent_events
ORDER BY created_at_utc DESC, event_id DESC
LIMIT %s;
"""
        rows: list[Any] = []
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (max(1000, int(scan_limit or 5000)),))
                    rows = cur.fetchall()
                conn.commit()
        except Exception:
            rows = []

        def _ingest_item(item: dict[str, Any]) -> None:
            candidate = self._worker_id_from_event_row(item)
            if not candidate:
                return
            worker_id = alias_to_worker.get(str(candidate).strip().lower(), "")
            if not worker_id:
                return
            payload = item.get("payload")
            payload_dict = payload if isinstance(payload, dict) else {}
            message = (
                str(payload_dict.get("message") or "").strip()
                or str(item.get("message") or "").strip()
                or str(item.get("event_type") or "").strip()
            )
            current_time = str(item.get("created_at") or "").strip()
            existing = out.get(worker_id)
            if existing is not None and str(existing.get("last_event_emitted_at_utc") or "") >= current_time:
                return
            out[worker_id] = {
                "last_event_emitted": message,
                "last_event_type": str(item.get("event_type") or "").strip(),
                "last_event_emitted_at_utc": current_time,
            }

        for row in rows:
            item = {
                "created_at": row[0].isoformat() if row[0] else "",
                "event_type": str(row[1] or "").strip(),
                "aggregate_key": str(row[2] or "").strip(),
                "message": str(row[3] or "").strip(),
                "payload": row[4] if isinstance(row[4], dict) else {},
            }
            _ingest_item(item)
            if len(out) >= len(normalized_ids):
                return out

        # Backward-compatible fallback for tests and legacy in-memory event streams.
        if len(out) < len(normalized_ids):
            event_stream = getattr(self, "_event_stream", None)
            if event_stream is not None and hasattr(event_stream, "read"):
                try:
                    legacy_rows = event_stream.read(limit=max(1000, int(scan_limit or 5000)), reverse=True)
                except TypeError:
                    legacy_rows = event_stream.read(limit=max(1000, int(scan_limit or 5000)))
                except Exception:
                    legacy_rows = []
                for raw in legacy_rows if isinstance(legacy_rows, list) else []:
                    if not isinstance(raw, dict):
                        continue
                    item = {
                        "created_at": str(raw.get("created_at") or raw.get("created_at_utc") or "").strip(),
                        "event_type": str(raw.get("event_type") or "").strip(),
                        "aggregate_key": str(raw.get("aggregate_key") or "").strip(),
                        "message": str(raw.get("message") or "").strip(),
                        "payload": raw.get("payload") if isinstance(raw.get("payload"), dict) else {},
                    }
                    _ingest_item(item)
                    if len(out) >= len(normalized_ids):
                        return out
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
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT CASE WHEN t.status = 'running' THEN COALESCE(t.root_domain, t.start_url) ELSE NULL END), NULL) AS current_targets
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
stage_current AS (
    SELECT DISTINCT ON (s.worker_id)
      s.worker_id,
      s.workflow_id,
      s.stage,
      s.root_domain,
      s.status,
      COALESCE(s.heartbeat_at_utc, s.updated_at_utc, s.started_at_utc, s.completed_at_utc) AS current_stage_activity_at_utc
    FROM coordinator_stage_tasks s
    CROSS JOIN limits l
    WHERE s.worker_id IS NOT NULL
      AND s.worker_id <> ''
      AND (
        s.status = 'running'
        OR (s.heartbeat_at_utc IS NOT NULL AND s.heartbeat_at_utc >= l.recent_cutoff)
        OR (s.updated_at_utc IS NOT NULL AND s.updated_at_utc >= l.recent_cutoff)
        OR (s.completed_at_utc IS NOT NULL AND s.completed_at_utc >= l.recent_cutoff)
      )
    ORDER BY
      s.worker_id ASC,
      CASE WHEN s.status = 'running' THEN 0 ELSE 1 END ASC,
      COALESCE(s.heartbeat_at_utc, s.updated_at_utc, s.started_at_utc, s.completed_at_utc) DESC NULLS LAST
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
    SELECT worker_id FROM stage_current
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
  COALESCE(p.last_activity, 'unknown') AS last_activity,
  COALESCE(sc.workflow_id, '') AS current_workflow_id,
  COALESCE(sc.stage, '') AS current_stage,
  COALESCE(sc.root_domain, '') AS current_stage_root_domain,
  COALESCE(sc.status, '') AS current_stage_status,
  sc.current_stage_activity_at_utc AS current_stage_activity_at_utc
FROM worker_ids w
LEFT JOIN target_agg t ON t.worker_id = w.worker_id
LEFT JOIN stage_agg s ON s.worker_id = w.worker_id
LEFT JOIN stage_current sc ON sc.worker_id = w.worker_id
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
            row_values = row if isinstance(row, (list, tuple)) else ()

            def _row_value(index: int, default: Any = None) -> Any:
                if index < 0 or index >= len(row_values):
                    return default
                return row_values[index]

            worker_id = str(_row_value(0, "") or "").strip()
            if not worker_id:
                continue
            last_heartbeat = _row_value(1)
            seconds_since: Optional[int] = None
            last_heartbeat_iso: Optional[str] = None
            if last_heartbeat is not None:
                last_heartbeat_iso = last_heartbeat.isoformat()
                seconds_since = max(0, int((now_utc - last_heartbeat).total_seconds()))
            is_online = seconds_since is not None and seconds_since <= stale_after
            if is_online:
                online_count += 1
            current_targets_value = _row_value(5, [])
            current_targets_raw = current_targets_value if isinstance(current_targets_value, list) else []
            last_activity = str(_row_value(7, "unknown") or "unknown")
            current_workflow_id = str(_row_value(8, "") or "").strip().lower()
            current_stage = str(_row_value(9, "") or "").strip().lower()
            current_stage_root_domain = str(_row_value(10, "") or "").strip().lower()
            current_stage_status = str(_row_value(11, "") or "").strip().lower()
            stage_activity_value = _row_value(12)
            current_stage_activity_at_utc = stage_activity_value.isoformat() if stage_activity_value else ""
            status = self._derive_worker_status(
                running_targets=int(_row_value(2, 0) or 0),
                running_stage_tasks=int(_row_value(3, 0) or 0),
                last_activity=last_activity,
                is_online=is_online,
            )
            if status == "stale":
                continue
            if status in status_counts:
                status_counts[status] += 1
            current_targets = [str(item) for item in current_targets_raw if str(item or "").strip()]
            is_actively_running = status == "running"
            if is_actively_running and current_stage_root_domain and current_stage_root_domain not in current_targets:
                current_targets.insert(0, current_stage_root_domain)
            if not is_actively_running:
                current_targets = []
                current_workflow_id = ""
                current_stage = ""
                current_stage_status = ""
                current_stage_activity_at_utc = ""
            current_action = ""
            if is_actively_running and (current_workflow_id or current_stage):
                current_action = f"Current Workflow: {current_workflow_id}\nCurrent Plugin: {current_stage}"
            workers.append(
                {
                    "worker_id": worker_id,
                    "status": status,
                    "last_activity": last_activity,
                    "last_action_performed": self._normalize_last_action_label(last_activity),
                    "last_heartbeat_at_utc": last_heartbeat_iso,
                    "seconds_since_heartbeat": seconds_since,
                    "running_targets": int(_row_value(2, 0) or 0),
                    "running_stage_tasks": int(_row_value(3, 0) or 0),
                    "urls_scanned_session": int(_row_value(4, 0) or 0),
                    "current_targets": current_targets,
                    "queued_commands": int(_row_value(6, 0) or 0),
                    "current_workflow_id": current_workflow_id,
                    "current_plugin_name": current_stage,
                    "current_stage_status": current_stage_status,
                    "current_stage_activity_at_utc": current_stage_activity_at_utc,
                    "current_action": current_action,
                    "last_event_emitted": "",
                    "last_event_type": "",
                    "last_event_emitted_at_utc": "",
                    "last_log_message": "",
                    "last_log_message_at_utc": "",
                    "last_seen_time_at_utc": last_heartbeat_iso or "",
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
                worker["last_seen_time_at_utc"] = _max_iso_datetime(
                    worker.get("last_seen_time_at_utc"),
                    worker.get("last_heartbeat_at_utc"),
                    worker.get("current_stage_activity_at_utc"),
                    worker.get("last_event_emitted_at_utc"),
                )
                worker["last_run_time_at_utc"] = str(worker.get("last_seen_time_at_utc") or "")
                continue
            fallback_activity = self._normalize_last_action_label(str(worker.get("last_activity") or "").strip())
            fallback_time = str(worker.get("last_heartbeat_at_utc") or "").strip()
            if fallback_activity and fallback_activity.lower() != "unknown":
                worker["last_event_emitted"] = fallback_activity
                worker["last_event_type"] = "worker.presence"
                worker["last_event_emitted_at_utc"] = fallback_time
            worker["last_seen_time_at_utc"] = _max_iso_datetime(
                worker.get("last_seen_time_at_utc"),
                worker.get("last_heartbeat_at_utc"),
                worker.get("current_stage_activity_at_utc"),
                worker.get("last_event_emitted_at_utc"),
            )
            worker["last_run_time_at_utc"] = str(worker.get("last_seen_time_at_utc") or "")
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
                            "start": "idle",
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

    
    @staticmethod
    def _stage_execution_profile(stage: str) -> dict[str, Any]:
        stg = str(stage or "").strip().lower()
        if stg.startswith("recon_spider") or stg.startswith("recon_subdomain"):
            return {"resource_class": "network_scan", "access_mode": "read", "concurrency_group": "recon_network", "max_parallelism": 4}
        if stg.startswith("nightmare"):
            return {"resource_class": "crawl", "access_mode": "write", "concurrency_group": "crawl", "max_parallelism": 1}
        if stg == "fozzy":
            return {"resource_class": "read_artifacts", "access_mode": "read", "concurrency_group": "fozzy", "max_parallelism": 2}
        if stg == "extractor":
            return {"resource_class": "read_artifacts", "access_mode": "read", "concurrency_group": "extractor", "max_parallelism": 4}
        if stg == "auth0r":
            return {"resource_class": "write_artifacts", "access_mode": "write", "concurrency_group": "auth0r", "max_parallelism": 1}
        return {"resource_class": "default", "access_mode": "write", "concurrency_group": stg, "max_parallelism": 1}


    def _workflow_catalog_dir(self) -> Path:
        return Path(__file__).resolve().parent.parent / "workflows"

    def _normalize_workflow_token(self, value: Any, *, default: str = "") -> str:
        raw = str(value or "").strip().lower()
        token = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in raw).strip("-")
        if token:
            return token
        fallback = str(default or "").strip().lower()
        return "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in fallback).strip("-")

    def _load_workflow_stage_preconditions(self, workflow_id: str, stage: str) -> dict[str, Any]:
        """Return prerequisite/precondition rules for a coordinator stage task.

        The coordinator queue intentionally stores workflow_id as task metadata
        only, but the stage's prerequisites still come from the workflow
        definition.  Missing workflow files or stages are treated as no
        prerequisites so ad-hoc/manual tasks remain runnable.
        """
        wid = self._normalize_workflow_token(workflow_id, default="default")
        stg = str(stage or "").strip().lower()
        if not wid or not stg:
            return {}
        # Prefer DB-backed workflow-builder definitions. Users can edit these in
        # the builder, so relying only on shipped JSON files can leave workers
        # evaluating stale preconditions and skipping the wrong task.
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
SELECT s.preconditions_json
FROM workflow_definitions w
JOIN workflow_steps s ON s.workflow_definition_id = w.id
WHERE w.workflow_key = %s AND s.plugin_key = %s
ORDER BY w.updated_at_utc DESC, s.ordinal ASC
LIMIT 1;
""",
                        (wid, stg),
                    )
                    row = cur.fetchone()
                conn.commit()
            if row and isinstance(row[0], dict):
                return dict(row[0] or {})
        except Exception:
            pass

        candidates: list[Path] = []
        workflow_dir = self._workflow_catalog_dir()
        direct = workflow_dir / f"{wid}.workflow.json"
        if direct.is_file():
            candidates.append(direct)
        if workflow_dir.is_dir():
            candidates.extend(path for path in sorted(workflow_dir.glob("*.workflow.json")) if path not in candidates)
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8-sig"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            payload_id = self._normalize_workflow_token(payload.get("workflow_id"), default=path.name.replace(".workflow.json", ""))
            if payload_id != wid:
                continue
            plugins = payload.get("plugins")
            if not isinstance(plugins, list):
                continue
            for entry in plugins:
                if not isinstance(entry, dict):
                    continue
                entry_stage = str(
                    entry.get("plugin_name")
                    or entry.get("stage")
                    or entry.get("name")
                    or entry.get("type")
                    or ""
                ).strip().lower()
                if entry_stage != stg:
                    continue
                prereq = entry.get("preconditions", entry.get("prerequisites", {}))
                prereq = dict(prereq or {}) if isinstance(prereq, dict) else {}
                inputs = entry.get("inputs") if isinstance(entry.get("inputs"), dict) else {}
                for key in ("artifacts_all", "artifacts_any"):
                    values = inputs.get(key)
                    if isinstance(values, list):
                        merged = list(prereq.get(key) or [])
                        for item in values:
                            if item not in merged:
                                merged.append(item)
                        prereq[key] = merged
                return prereq
        return {}

    def _stage_prerequisites_satisfied(
        self,
        cur: Any,
        *,
        workflow_id: str,
        root_domain: str,
        stage: str,
    ) -> tuple[bool, str]:
        """Evaluate whether a stage task may move from waiting to ready.

        Preconditions are evaluated against the current database state at claim
        time and whenever artifacts/tasks complete, preventing workers from
        leasing tasks whose required artifacts or predecessor plugins are not
        available yet.
        """
        rd = str(root_domain or "").strip().lower()
        wid = str(workflow_id or "").strip().lower() or "default"
        stg = str(stage or "").strip().lower()
        prereq = self._load_workflow_stage_preconditions(wid, stg)
        if not prereq:
            try:
                cur.execute(
                    """
SELECT checkpoint_json
FROM coordinator_stage_tasks
WHERE workflow_id = %s AND root_domain = %s AND stage = %s;
""",
                    (wid, rd, stg),
                )
                row = cur.fetchone()
                checkpoint = row[0] if row and isinstance(row[0], dict) else {}
                checkpoint_prereq = checkpoint.get("preconditions_json", checkpoint.get("preconditions", {})) if isinstance(checkpoint, dict) else {}
                prereq = dict(checkpoint_prereq or {}) if isinstance(checkpoint_prereq, dict) else {}
            except Exception:
                prereq = {}
        if not prereq:
            return True, ""

        # Compatibility with workflow_app.conditions-style checks.
        explicit_checks = prereq.get("all") if isinstance(prereq.get("all"), list) else []
        for check in explicit_checks:
            if not isinstance(check, dict):
                continue
            check_type = str(check.get("type") or "").strip().lower()
            if check_type == "file_exists":
                file_path = Path(str(check.get("path") or "").strip()).expanduser()
                if not str(file_path):
                    continue
                if not file_path.exists():
                    return False, f"waiting for file {file_path}"
            elif check_type in {"artifact_exists", "artifact"}:
                artifact_name = str(check.get("artifact_type") or check.get("name") or "").strip().lower()
                if artifact_name:
                    cur.execute(
                        "SELECT 1 FROM coordinator_artifacts WHERE root_domain = %s AND artifact_type = %s LIMIT 1;",
                        (rd, artifact_name),
                    )
                    if cur.fetchone() is None:
                        return False, f"waiting for artifact: {artifact_name}"

        def _names(value: Any) -> set[str]:
            if not isinstance(value, list):
                return set()
            return {str(item or "").strip().lower() for item in value if str(item or "").strip()}

        required_all = _names(prereq.get("artifacts_all"))
        required_any = _names(prereq.get("artifacts_any"))
        required_plugins_all = _names(prereq.get("requires_plugins_all") or prereq.get("plugins_all"))
        required_plugins_any = _names(prereq.get("requires_plugins_any") or prereq.get("plugins_any"))
        required_target_statuses = _names(prereq.get("target_statuses"))
        require_target_completed = bool(prereq.get("require_target_completed", False))
        # Recon bootstrap should run even when legacy target rows are already
        # failed so unprocessed domains can still enter recon workflow.
        if stg == "recon_subdomain_enumeration" and required_target_statuses:
            required_target_statuses.add("failed")

        cur.execute(
            "SELECT artifact_type FROM coordinator_artifacts WHERE root_domain = %s;",
            (rd,),
        )
        artifacts = {str(row[0] or "").strip().lower() for row in cur.fetchall() if str(row[0] or "").strip()}
        if required_all and not required_all.issubset(artifacts):
            missing = ", ".join(sorted(required_all - artifacts))
            return False, f"waiting for artifacts: {missing}"
        if required_any and artifacts.isdisjoint(required_any):
            return False, "waiting for one of artifacts: " + ", ".join(sorted(required_any))

        cur.execute(
            """
SELECT stage, status
FROM coordinator_stage_tasks
WHERE workflow_id = %s AND root_domain = %s;
""",
            (wid, rd),
        )
        stage_status = {str(row[0] or "").strip().lower(): str(row[1] or "").strip().lower() for row in cur.fetchall()}
        if required_plugins_all:
            missing_plugins = sorted(name for name in required_plugins_all if stage_status.get(name) != "completed")
            if missing_plugins:
                return False, "waiting for completed plugins: " + ", ".join(missing_plugins)
        if required_plugins_any and not any(stage_status.get(name) == "completed" for name in required_plugins_any):
            return False, "waiting for one of completed plugins: " + ", ".join(sorted(required_plugins_any))

        if required_target_statuses or require_target_completed:
            cur.execute(
                """
SELECT
  COUNT(*) FILTER (WHERE status = 'pending') AS pending,
  COUNT(*) FILTER (WHERE status = 'running') AS running,
  COUNT(*) FILTER (WHERE status = 'completed') AS completed,
  COUNT(*) FILTER (WHERE status = 'failed') AS failed
FROM coordinator_targets
WHERE root_domain = %s;
""",
                (rd,),
            )
            row = cur.fetchone() or (0, 0, 0, 0)
            pending_targets = int(row[0] or 0)
            running_targets = int(row[1] or 0)
            completed_targets = int(row[2] or 0)
            failed_targets = int(row[3] or 0)
            total_targets = pending_targets + running_targets + completed_targets + failed_targets
            if running_targets > 0:
                current_target_status = "running"
            elif pending_targets > 0:
                current_target_status = "pending"
            elif failed_targets > 0 and completed_targets <= 0:
                current_target_status = "failed"
            elif completed_targets > 0:
                current_target_status = "completed"
            elif total_targets <= 0:
                # Domain-level workflow runs may not have explicit target rows yet.
                # Treat this as pending so first-stage plugins are not blocked forever.
                current_target_status = "pending"
            else:
                current_target_status = "unknown"
            if required_target_statuses and current_target_status not in required_target_statuses:
                return False, "waiting for target status: " + ", ".join(sorted(required_target_statuses))
            if require_target_completed and completed_targets <= 0:
                return False, "waiting for completed target"

        return True, ""

    def _sync_workflow_step_runs_for_stage_cur(
        self,
        cur: Any,
        *,
        workflow_id: str,
        root_domain: str,
        stage: str,
        status: str,
        worker_id: str = "",
        error: str = "",
    ) -> None:
        """Mirror coordinator-stage state into workflow_step_runs when linked."""
        wid = str(workflow_id or "").strip().lower() or "default"
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        mapped_status = str(status or "").strip().lower()
        if mapped_status == "pending":
            blocked_reason = "Waiting for Prerequisites..."
        else:
            blocked_reason = ""
        try:
            cur.execute(
                """
UPDATE workflow_step_runs wr
SET status = %s,
    blocked_reason = %s,
    worker_id = CASE WHEN %s <> '' THEN %s ELSE worker_id END,
    error = CASE WHEN %s <> '' THEN %s ELSE error END,
    started_at_utc = CASE WHEN %s = 'running' THEN COALESCE(started_at_utc, NOW()) ELSE started_at_utc END,
    completed_at_utc = CASE WHEN %s IN ('completed','failed') THEN NOW() ELSE completed_at_utc END,
    updated_at_utc = NOW()
FROM coordinator_stage_tasks ct
WHERE ct.workflow_id = %s
  AND ct.root_domain = %s
  AND ct.stage = %s
  AND ct.checkpoint_json ? 'workflow_run_id'
  AND ct.checkpoint_json ? 'step_key'
  AND wr.workflow_run_id::text = ct.checkpoint_json->>'workflow_run_id'
  AND wr.step_key = ct.checkpoint_json->>'step_key';
""",
                (
                    mapped_status,
                    blocked_reason,
                    str(worker_id or ""),
                    str(worker_id or ""),
                    str(error or ""),
                    str(error or "")[:2000],
                    mapped_status,
                    mapped_status,
                    wid,
                    rd,
                    stg,
                ),
            )
        except Exception:
            # Older deployments may not have workflow_app tables. The coordinator
            # queue must still function in those environments.
            return

    def refresh_stage_task_readiness(
        self,
        *,
        root_domain: str = "",
        workflow_id: str = "",
        limit: int = 500,
    ) -> int:
        """Re-evaluate waiting/ready coordinator stage tasks.

        Returns the number of rows whose claimability changed.  This method is
        intentionally safe to call often: it only examines non-terminal,
        non-running tasks and updates rows whose prerequisite result changed.
        """
        rd_filter = str(root_domain or "").strip().lower()
        wid_filter = str(workflow_id or "").strip().lower()
        max_rows = max(1, min(5000, int(limit or 500)))
        changed = 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                where = ["status IN ('pending','ready')"]
                params: list[Any] = []
                if rd_filter:
                    where.append("root_domain = %s")
                    params.append(rd_filter)
                if wid_filter:
                    where.append("workflow_id = %s")
                    params.append(wid_filter)
                params.append(max_rows)
                cur.execute(
                    f"""
SELECT workflow_id, root_domain, stage, status
FROM coordinator_stage_tasks
WHERE {' AND '.join(where)}
ORDER BY created_at_utc ASC
LIMIT %s
FOR UPDATE SKIP LOCKED;
""",
                    params,
                )
                rows = cur.fetchall()
                for row in rows:
                    wid = str(row[0] or "default").strip().lower() or "default"
                    rd = str(row[1] or "").strip().lower()
                    stg = str(row[2] or "").strip().lower()
                    current = str(row[3] or "").strip().lower()
                    ready, reason = self._stage_prerequisites_satisfied(cur, workflow_id=wid, root_domain=rd, stage=stg)
                    desired = "ready" if ready else "pending"
                    if desired == current:
                        if desired == "pending":
                            self._sync_workflow_step_runs_for_stage_cur(cur, workflow_id=wid, root_domain=rd, stage=stg, status="pending")
                        continue
                    cur.execute(
                        """
UPDATE coordinator_stage_tasks
SET status = %s,
    worker_id = NULL,
    lease_expires_at = NULL,
    heartbeat_at_utc = NULL,
    error = %s,
    updated_at_utc = NOW()
WHERE workflow_id = %s AND root_domain = %s AND stage = %s;
""",
                        (desired, reason[:2000], wid, rd, stg),
                    )
                    self._sync_workflow_step_runs_for_stage_cur(cur, workflow_id=wid, root_domain=rd, stage=stg, status=desired, error=reason if desired == "pending" else "")
                    changed += int(cur.rowcount or 0)
            conn.commit()
        return changed

    def schedule_stage(
        self,
        root_domain: str,
        stage: str,
        *,
        workflow_id: str = "default",
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
        max_attempts_int = max(1, int(max_attempts or 1))
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
        profile = self._stage_execution_profile(stg)

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
                prereq_ready, prereq_reason = self._stage_prerequisites_satisfied(
                    cur,
                    workflow_id=widf,
                    root_domain=rd,
                    stage=stg,
                )
                initial_status = "ready" if prereq_ready else "pending"
                if row is None:
                    cur.execute(
                        """
INSERT INTO coordinator_stage_tasks(
    workflow_id, root_domain, stage, status, checkpoint_json, progress_json, progress_artifact_type, resume_mode,
    resource_class, access_mode, concurrency_group, max_parallelism, max_attempts, updated_at_utc
)
VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, NOW());
""",
                        (
                            widf,
                            rd,
                            stg,
                            initial_status,
                            json.dumps(checkpoint_obj, ensure_ascii=False),
                            json.dumps(progress_obj, ensure_ascii=False),
                            progress_artifact_type_text,
                            resume_mode_text,
                            profile["resource_class"],
                            profile["access_mode"],
                            profile["concurrency_group"],
                            int(profile["max_parallelism"]),
                            max_attempts_int,
                        ),
                    )
                    scheduled = True
                    status = initial_status
                    decision_reason = "inserted_ready" if initial_status == "ready" else "waiting_for_prerequisites"
                    if initial_status == "pending":
                        cur.execute(
                            "UPDATE coordinator_stage_tasks SET error = %s WHERE workflow_id = %s AND root_domain = %s AND stage = %s;",
                            (prereq_reason[:2000], widf, rd, stg),
                        )
                    self._sync_workflow_step_runs_for_stage_cur(
                        cur,
                        workflow_id=widf,
                        root_domain=rd,
                        stage=stg,
                        status=initial_status,
                        error=prereq_reason if initial_status == "pending" else "",
                    )
                    attempt_count = 0
                else:
                    current_status = str(row[0] or "").strip().lower()
                    attempt_count = int(row[1] or 0)
                    status = current_status
                    if current_status == "completed":
                        decision_reason = "already_completed"
                    elif current_status in {"pending", "ready", "running"}:
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
SET status = %s,
    worker_id = NULL,
    lease_expires_at = NULL,
    heartbeat_at_utc = NULL,
    completed_at_utc = NULL,
    updated_at_utc = NOW(),
    error = NULL,
    checkpoint_json = %s::jsonb,
    progress_json = %s::jsonb,
    progress_artifact_type = %s,
    resume_mode = %s,
    resource_class = %s,
    access_mode = %s,
    concurrency_group = %s,
    max_parallelism = %s,
    max_attempts = %s
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s;
""",
                                (
                                    initial_status,
                                    json.dumps(checkpoint_obj, ensure_ascii=False),
                                    json.dumps(progress_obj, ensure_ascii=False),
                                    progress_artifact_type_text,
                                    resume_mode_text,
                                    profile["resource_class"],
                                    profile["access_mode"],
                                    profile["concurrency_group"],
                                    int(profile["max_parallelism"]),
                                    max_attempts_int,
                                    widf,
                                    rd,
                                    stg,
                                ),
                            )
                            scheduled = True
                            status = initial_status
                            decision_reason = "retry_ready" if initial_status == "ready" else "waiting_for_prerequisites"
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
                    "status": status,
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
        workflow_id: str = "default",
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


    def _stage_lease_key(self, row: dict[str, Any]) -> str:
        group = str(row.get("concurrency_group") or "").strip().lower()
        resource = str(row.get("resource_class") or "default").strip().lower() or "default"
        stage = str(row.get("stage") or "").strip().lower()
        return group or resource or stage or "default"

    def _resource_conflict_exists(
        self,
        cur: Any,
        *,
        root_domain: str,
        lease_key: str,
        access_mode: str,
        max_parallelism: int,
    ) -> bool:
        cur.execute(
            """
SELECT access_mode, COUNT(*)
FROM coordinator_resource_leases
WHERE root_domain = %s
  AND lease_key = %s
  AND lease_expires_at >= NOW()
GROUP BY access_mode;
""",
            (root_domain, lease_key),
        )
        rows = cur.fetchall()
        total = sum(int(row[1] or 0) for row in rows)
        modes = {str(row[0] or "write").lower() for row in rows}
        mode = str(access_mode or "write").strip().lower() or "write"
        if mode == "write":
            return total > 0
        if "write" in modes:
            return True
        return total >= max(1, int(max_parallelism or 1))

    def release_stage_resources(
        self,
        *,
        workflow_id: str,
        root_domain: str,
        stage: str,
        worker_id: str = "",
    ) -> int:
        widf = str(workflow_id or "").strip().lower() or "default"
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        if not rd or not stg:
            return 0
        where = "workflow_id = %s AND root_domain = %s AND stage = %s"
        params: list[Any] = [widf, rd, stg]
        if wid:
            where += " AND worker_id = %s"
            params.append(wid)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM coordinator_resource_leases WHERE {where};", params)
                deleted = int(cur.rowcount or 0)
            conn.commit()
        return deleted

    def refresh_stage_resource_leases(
        self,
        *,
        workflow_id: str,
        root_domain: str,
        stage: str,
        worker_id: str,
        lease_seconds: int,
    ) -> bool:
        widf = str(workflow_id or "").strip().lower() or "default"
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        if not rd or not stg or not wid:
            return False
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
UPDATE coordinator_resource_leases
SET lease_expires_at = NOW() + ((%s)::text || ' seconds')::interval,
    updated_at_utc = NOW()
WHERE workflow_id = %s AND root_domain = %s AND stage = %s AND worker_id = %s;
""",
                    (lease, widf, rd, stg, wid),
                )
                updated = int(cur.rowcount or 0)
            conn.commit()
        return updated > 0

    def try_claim_stage_with_resources(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        workflow_id: str = "",
        plugin_allowlist: Optional[list[str]] = None,
    ) -> Optional[dict[str, Any]]:
        wid = str(worker_id or "").strip()
        # Workers are global consumers.  workflow_id is retained on each task for
        # reporting/progress, but it must never scope task acquisition.
        widf = ""
        if not wid:
            raise ValueError("worker_id is required")
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        allowlist = [
            str(item or "").strip().lower()
            for item in (plugin_allowlist or [])
            if str(item or "").strip()
        ]
        # Convert newly-unblocked tasks to ready before looking for work.
        # This is intentionally global: workflow_id is task metadata, not a
        # worker lane selector.
        self.refresh_stage_task_readiness(limit=1000)
        candidates_sql = """
SELECT workflow_id, root_domain, stage, status, worker_id, attempt_count, lease_expires_at,
       checkpoint_json, progress_json, progress_artifact_type, resume_mode,
       COALESCE(resource_class, 'default'), COALESCE(access_mode, 'write'),
       COALESCE(concurrency_group, ''), GREATEST(COALESCE(max_parallelism, 1), 1),
       GREATEST(COALESCE(max_attempts, 1), 1)
FROM coordinator_stage_tasks
WHERE (
    status = 'ready'
    OR (
      status = 'running'
      AND lease_expires_at IS NOT NULL
      AND lease_expires_at < NOW()
      AND attempt_count < GREATEST(COALESCE(max_attempts, 1), 1)
    )
)
  AND (%s::text[] IS NULL OR stage = ANY(%s))
  AND (
      stage = 'recon_subdomain_enumeration'
      OR NOT EXISTS (
          SELECT 1
          FROM coordinator_targets q
          WHERE q.root_domain = coordinator_stage_tasks.root_domain
            AND q.status = 'running'
            AND q.lease_expires_at IS NOT NULL
            AND q.lease_expires_at >= NOW()
      )
  )
ORDER BY created_at_utc ASC
FOR UPDATE SKIP LOCKED
LIMIT 50;
"""
        update_sql = """
UPDATE coordinator_stage_tasks
SET status = 'running',
    worker_id = %s,
    lease_expires_at = NOW() + ((%s)::text || ' seconds')::interval,
    started_at_utc = COALESCE(started_at_utc, NOW()),
    completed_at_utc = NULL,
    heartbeat_at_utc = NOW(),
    attempt_count = attempt_count + 1,
    checkpoint_json = CASE
        WHEN COALESCE(checkpoint_json, '{}'::jsonb) ? 'force_run_override'
            THEN (COALESCE(checkpoint_json, '{}'::jsonb) - 'force_run_override' - 'force_run_requested_at_utc')
        ELSE checkpoint_json
    END,
    updated_at_utc = NOW(),
    error = NULL
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s
RETURNING workflow_id, root_domain, stage, status, worker_id, attempt_count, lease_expires_at,
          checkpoint_json, progress_json, progress_artifact_type, resume_mode,
          resource_class, access_mode, concurrency_group, max_parallelism, max_attempts;
"""
        claimed = None
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._touch_worker_presence(cur, wid, "claim_stage")
                cur.execute("DELETE FROM coordinator_resource_leases WHERE lease_expires_at < NOW();")
                cur.execute(candidates_sql, (allowlist or None, allowlist or None))
                rows = cur.fetchall()
                for row in rows:
                    checkpoint_json = row[7] if isinstance(row[7], dict) else {}
                    row_map = {
                        "workflow_id": str(row[0] or "default"),
                        "root_domain": str(row[1] or "").strip().lower(),
                        "stage": str(row[2] or "").strip().lower(),
                        "checkpoint_json": checkpoint_json,
                        "force_run_override": bool(checkpoint_json.get("force_run_override")) if isinstance(checkpoint_json, dict) else False,
                        "resource_class": str(row[11] or "default").strip().lower() or "default",
                        "access_mode": str(row[12] or "write").strip().lower() or "write",
                        "concurrency_group": str(row[13] or "").strip().lower(),
                        "max_parallelism": int(row[14] or 1),
                        "max_attempts": int(row[15] or 1),
                    }
                    if bool(row_map.get("force_run_override")):
                        still_ready, blocked_reason = True, ""
                    else:
                        still_ready, blocked_reason = self._stage_prerequisites_satisfied(
                            cur,
                            workflow_id=row_map["workflow_id"],
                            root_domain=row_map["root_domain"],
                            stage=row_map["stage"],
                        )
                    if not still_ready:
                        cur.execute(
                            """
UPDATE coordinator_stage_tasks
SET status = 'pending',
    worker_id = NULL,
    lease_expires_at = NULL,
    heartbeat_at_utc = NULL,
    error = %s,
    updated_at_utc = NOW()
WHERE workflow_id = %s AND root_domain = %s AND stage = %s;
""",
                            (blocked_reason[:2000], row_map["workflow_id"], row_map["root_domain"], row_map["stage"]),
                        )
                        self._sync_workflow_step_runs_for_stage_cur(
                            cur,
                            workflow_id=row_map["workflow_id"],
                            root_domain=row_map["root_domain"],
                            stage=row_map["stage"],
                            status="pending",
                            error=blocked_reason,
                        )
                        continue
                    lease_key = self._stage_lease_key(row_map)
                    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (f"{row_map['root_domain']}:{lease_key}",))
                    if self._resource_conflict_exists(
                        cur,
                        root_domain=row_map["root_domain"],
                        lease_key=lease_key,
                        access_mode=row_map["access_mode"],
                        max_parallelism=row_map["max_parallelism"],
                    ):
                        continue
                    cur.execute(update_sql, (wid, lease, row_map["workflow_id"], row_map["root_domain"], row_map["stage"]))
                    updated = cur.fetchone()
                    if updated is None:
                        continue
                    self._sync_workflow_step_runs_for_stage_cur(
                        cur,
                        workflow_id=row_map["workflow_id"],
                        root_domain=row_map["root_domain"],
                        stage=row_map["stage"],
                        status="running",
                        worker_id=wid,
                    )
                    cur.execute(
                        """
INSERT INTO coordinator_resource_leases(
    lease_id, workflow_id, root_domain, stage, worker_id, resource_class,
    access_mode, concurrency_group, lease_key, max_parallelism, lease_expires_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() + ((%s)::text || ' seconds')::interval)
ON CONFLICT (lease_id) DO UPDATE
SET lease_expires_at = EXCLUDED.lease_expires_at,
    updated_at_utc = NOW();
""",
                        (
                            f"{updated[0]}:{updated[1]}:{updated[2]}",
                            updated[0],
                            updated[1],
                            updated[2],
                            wid,
                            row_map["resource_class"],
                            row_map["access_mode"],
                            row_map["concurrency_group"],
                            lease_key,
                            row_map["max_parallelism"],
                            lease,
                        ),
                    )
                    claimed = updated
                    break
            conn.commit()
        if claimed is None:
            return None
        row = claimed
        self.record_system_event(
            "workflow.task.claimed",
            f"workflow_task:{row[0]}:{row[1]}:{row[2]}",
            {
                "source": "coordinator_store.try_claim_stage_with_resources",
                "workflow_id": row[0],
                "root_domain": row[1],
                "stage": row[2],
                "plugin_name": row[2],
                "status": row[3],
                "worker_id": row[4],
                "attempt_count": int(row[5] or 0),
                "lease_expires_at": row[6].isoformat() if row[6] else None,
                "resume_mode": str(row[10] or "exact"),
                "resource_class": str(row[11] or "default"),
                "access_mode": str(row[12] or "write"),
                "concurrency_group": str(row[13] or ""),
                "max_parallelism": int(row[14] or 1),
                "max_attempts": int(row[15] or 1),
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
            "resource_class": str(row[11] or "default"),
            "access_mode": str(row[12] or "write"),
            "concurrency_group": str(row[13] or ""),
            "max_parallelism": int(row[14] or 1),
            "max_attempts": int(row[15] or 1),
        }

    def claim_next_stage(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        workflow_id: str = "",
        plugin_allowlist: Optional[list[str]] = None,
    ) -> Optional[dict[str, Any]]:
        # Intentionally ignore workflow_id: workers pick the next ready task from
        # the global ready queue.  Workflow remains task metadata only.
        return self.try_claim_stage_with_resources(
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            workflow_id="",
            plugin_allowlist=plugin_allowlist,
        )


    def claim_stage(
        self,
        stage: str,
        worker_id: str,
        lease_seconds: int,
        *,
        workflow_id: str = "default",
    ) -> Optional[dict[str, Any]]:
        stg = str(stage or "").strip().lower()
        if not stg:
            raise ValueError("stage is required")
        return self.claim_next_stage(
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            workflow_id="",
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
        """Refresh only volatile liveness/lease state.

        Durable checkpoint/progress writes are intentionally handled by
        update_stage_progress() or complete_stage(); callers may still pass the
        legacy checkpoint/progress arguments, but heartbeat does not persist
        those JSON blobs on every tick.
        """
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        widf = str(workflow_id or "").strip().lower() or "default"
        lease = max(15, int(lease_seconds or DEFAULT_COORDINATOR_LEASE_SECONDS))
        if not rd or not stg or not wid:
            return False
        sql = """
UPDATE coordinator_stage_tasks
SET heartbeat_at_utc = NOW(),
    lease_expires_at = NOW() + ((%s)::text || ' seconds')::interval,
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
                cur.execute(sql, (lease, widf, rd, stg, wid))
                updated = int(cur.rowcount or 0)
                if updated > 0:
                    cur.execute(
                        """
UPDATE coordinator_resource_leases
SET lease_expires_at = NOW() + ((%s)::text || ' seconds')::interval,
    updated_at_utc = NOW()
WHERE workflow_id = %s AND root_domain = %s AND stage = %s AND worker_id = %s;
""",
                        (lease, widf, rd, stg, wid),
                    )
            conn.commit()
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
        checkpoint_dict = dict(checkpoint or {}) if isinstance(checkpoint, dict) else {}
        progress_dict = dict(progress or {}) if isinstance(progress, dict) else {}
        checkpoint_json = json.dumps(checkpoint_dict, ensure_ascii=False) if checkpoint_dict else None
        progress_json = json.dumps(progress_dict, ensure_ascii=False) if progress_dict else None
        artifact_type_text = str(progress_artifact_type or "").strip().lower()
        progress_percent = float(progress_dict.get("percent", progress_dict.get("progress_percent", 0.0)) or 0.0)
        current_unit = str(progress_dict.get("current_unit", progress_dict.get("unit", "")) or "")
        last_milestone = str(progress_dict.get("last_milestone", progress_dict.get("milestone", "")) or "")
        sql = """
UPDATE coordinator_stage_tasks
SET checkpoint_json = COALESCE(%s::jsonb, checkpoint_json),
    progress_json = COALESCE(%s::jsonb, progress_json),
    progress_percent = CASE WHEN %s >= 0 THEN %s ELSE progress_percent END,
    current_unit = CASE WHEN %s <> '' THEN %s ELSE current_unit END,
    last_milestone = CASE WHEN %s <> '' THEN %s ELSE last_milestone END,
    progress_artifact_type = CASE
      WHEN %s <> '' THEN %s
      ELSE progress_artifact_type
    END,
    heartbeat_at_utc = NOW(),
    updated_at_utc = NOW(),
    durable_checkpoint_json = CASE WHEN %s <> '' THEN COALESCE(%s::jsonb, durable_checkpoint_json) ELSE durable_checkpoint_json END,
    durable_progress_json = CASE WHEN %s <> '' THEN COALESCE(%s::jsonb, durable_progress_json) ELSE durable_progress_json END,
    durable_progress_updated_at_utc = CASE WHEN %s <> '' THEN NOW() ELSE durable_progress_updated_at_utc END
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
                        progress_percent,
                        progress_percent,
                        current_unit,
                        current_unit,
                        last_milestone,
                        last_milestone,
                        artifact_type_text,
                        artifact_type_text,
                        last_milestone,
                        checkpoint_json,
                        last_milestone,
                        progress_json,
                        last_milestone,
                        widf,
                        rd,
                        stg,
                        wid,
                    ),
                )
                updated = int(cur.rowcount or 0)
            conn.commit()
        if updated > 0 and last_milestone:
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
                    "progress_percent": progress_percent,
                    "current_unit": current_unit,
                    "last_milestone": last_milestone,
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
        workflow_id: str = "default",
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
        checkpoint_dict = dict(checkpoint or {}) if isinstance(checkpoint, dict) else {}
        progress_dict = dict(progress or {}) if isinstance(progress, dict) else {}
        checkpoint_json = json.dumps(checkpoint_dict, ensure_ascii=False) if checkpoint_dict else None
        progress_json = json.dumps(progress_dict, ensure_ascii=False) if progress_dict else None
        progress_artifact_type_text = str(progress_artifact_type or "").strip().lower()
        resume_mode_text = str(resume_mode or "").strip().lower()
        progress_percent = float(progress_dict.get("percent", progress_dict.get("progress_percent", 100.0 if ok else 0.0)) or 0.0)
        current_unit = str(progress_dict.get("current_unit", progress_dict.get("unit", "")) or "")
        last_milestone = str(progress_dict.get("last_milestone", progress_dict.get("milestone", next_status)) or next_status)
        sql = """
UPDATE coordinator_stage_tasks
SET status = %s,
    exit_code = %s,
    error = %s,
    checkpoint_json = COALESCE(%s::jsonb, checkpoint_json),
    progress_json = COALESCE(%s::jsonb, progress_json),
    durable_checkpoint_json = COALESCE(%s::jsonb, durable_checkpoint_json),
    durable_progress_json = COALESCE(%s::jsonb, durable_progress_json),
    durable_progress_updated_at_utc = NOW(),
    progress_percent = %s,
    current_unit = CASE WHEN %s <> '' THEN %s ELSE current_unit END,
    last_milestone = CASE WHEN %s <> '' THEN %s ELSE last_milestone END,
    progress_artifact_type = CASE WHEN %s <> '' THEN %s ELSE progress_artifact_type END,
    resume_mode = CASE WHEN %s <> '' THEN %s ELSE resume_mode END,
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
                        checkpoint_json,
                        progress_json,
                        progress_percent,
                        current_unit,
                        current_unit,
                        last_milestone,
                        last_milestone,
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
                if updated > 0:
                    cur.execute(
                        "DELETE FROM coordinator_resource_leases WHERE workflow_id = %s AND root_domain = %s AND stage = %s AND worker_id = %s;",
                        (widf, rd, stg, wid),
                    )
                    self._sync_workflow_step_runs_for_stage_cur(
                        cur,
                        workflow_id=widf,
                        root_domain=rd,
                        stage=stg,
                        status=next_status,
                        worker_id=wid,
                        error=str(error or "")[:2000],
                    )
            conn.commit()
        if updated > 0:
            # A task completing can satisfy plugin prerequisites for other
            # waiting tasks in the same domain/workflow.
            try:
                self.refresh_stage_task_readiness(root_domain=rd, workflow_id=widf, limit=1000)
            except Exception:
                pass
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
                    "progress_percent": progress_percent,
                    "current_unit": current_unit,
                    "last_milestone": last_milestone,
                },
            )
        return updated > 0

    def control_stage_task(
        self,
        *,
        workflow_id: str,
        root_domain: str,
        stage: str,
        action: str,
    ) -> dict[str, Any]:
        widf = str(workflow_id or "").strip().lower() or "default"
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        act = str(action or "").strip().lower()
        if not rd or not stg:
            return {"ok": False, "error": "root_domain and stage are required"}
        if act not in {"delete", "pause", "run"}:
            return {"ok": False, "error": "unsupported action"}

        previous_status = ""
        affected = 0
        next_status = ""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
SELECT status
FROM coordinator_stage_tasks
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s
FOR UPDATE;
""",
                    (widf, rd, stg),
                )
                row = cur.fetchone()
                if row is None:
                    conn.commit()
                    return {
                        "ok": False,
                        "error": "task not found",
                        "workflow_id": widf,
                        "root_domain": rd,
                        "stage": stg,
                        "action": act,
                    }
                previous_status = str(row[0] or "").strip().lower()

                if act == "delete":
                    cur.execute(
                        """
DELETE FROM coordinator_stage_tasks
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s;
""",
                        (widf, rd, stg),
                    )
                    stage_affected = int(cur.rowcount or 0)
                    cur.execute(
                        """
DELETE FROM coordinator_resource_leases
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s;
""",
                        (widf, rd, stg),
                    )
                    affected = stage_affected
                    next_status = "deleted"
                elif act == "pause":
                    cur.execute(
                        """
UPDATE coordinator_stage_tasks
SET status = 'paused',
    worker_id = NULL,
    lease_expires_at = NULL,
    heartbeat_at_utc = NULL,
    error = 'Paused by operator',
    updated_at_utc = NOW()
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s;
""",
                        (widf, rd, stg),
                    )
                    stage_affected = int(cur.rowcount or 0)
                    cur.execute(
                        """
DELETE FROM coordinator_resource_leases
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s;
""",
                        (widf, rd, stg),
                    )
                    affected = stage_affected
                    next_status = "paused"
                    self._sync_workflow_step_runs_for_stage_cur(
                        cur,
                        workflow_id=widf,
                        root_domain=rd,
                        stage=stg,
                        status=next_status,
                    )
                else:
                    force_payload = json.dumps(
                        {
                            "force_run_override": True,
                            "force_run_requested_at_utc": _iso_now(),
                        },
                        ensure_ascii=False,
                    )
                    cur.execute(
                        """
UPDATE coordinator_stage_tasks
SET status = 'ready',
    worker_id = NULL,
    lease_expires_at = NULL,
    heartbeat_at_utc = NULL,
    checkpoint_json = COALESCE(checkpoint_json, '{}'::jsonb) || %s::jsonb,
    error = '',
    updated_at_utc = NOW()
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s;
""",
                        (force_payload, widf, rd, stg),
                    )
                    stage_affected = int(cur.rowcount or 0)
                    cur.execute(
                        """
DELETE FROM coordinator_resource_leases
WHERE workflow_id = %s
  AND root_domain = %s
  AND stage = %s;
""",
                        (widf, rd, stg),
                    )
                    affected = stage_affected
                    next_status = "ready"
                    self._sync_workflow_step_runs_for_stage_cur(
                        cur,
                        workflow_id=widf,
                        root_domain=rd,
                        stage=stg,
                        status=next_status,
                    )
            conn.commit()

        self.record_system_event(
            "workflow.task.control",
            f"workflow_task:{widf}:{rd}:{stg}",
            {
                "source": "coordinator_store.control_stage_task",
                "workflow_id": widf,
                "root_domain": rd,
                "stage": stg,
                "plugin_name": stg,
                "action": act,
                "previous_status": previous_status,
                "status": next_status,
                "affected_rows": affected,
            },
        )

        return {
            "ok": bool(affected > 0),
            "workflow_id": widf,
            "root_domain": rd,
            "stage": stg,
            "action": act,
            "previous_status": previous_status,
            "status": next_status,
            "affected_rows": affected,
            "updated_at_utc": _iso_now(),
        }

    def reset_stage_tasks(
        self,
        *,
        workflow_id: str = "",
        root_domains: Optional[list[str]] = None,
        plugins: Optional[list[str]] = None,
        statuses: Optional[list[str]] = None,
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
        normalized_statuses: list[str] = []
        for item in (statuses or []):
            text = str(item or "").strip().lower()
            if not text:
                continue
            if text in {"errored", "error"}:
                text = "failed"
            if text not in {"pending", "ready", "running", "completed", "failed", "paused"}:
                continue
            if text not in normalized_statuses:
                normalized_statuses.append(text)
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
        if normalized_statuses:
            where_sql.append("status = ANY(%s)")
            params.append(normalized_statuses)
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
                "statuses": normalized_statuses,
                "hard_delete": bool(hard_delete),
                "affected_rows": affected,
            },
        )
        return {
            "ok": True,
            "workflow_id": widf or "",
            "root_domains": domains,
            "plugins": stages,
            "statuses": normalized_statuses,
            "hard_delete": bool(hard_delete),
            "affected_rows": affected,
            "reset_at_utc": _iso_now(),
        }

    def reset_targets(
        self,
        *,
        root_domains: Optional[list[str]] = None,
        statuses: Optional[list[str]] = None,
        hard_delete: bool = False,
    ) -> dict[str, Any]:
        domains = [
            str(item or "").strip().lower()
            for item in (root_domains or [])
            if str(item or "").strip()
        ]
        normalized_statuses: list[str] = []
        for item in (statuses or []):
            text = str(item or "").strip().lower()
            if not text:
                continue
            if text in {"errored", "error"}:
                text = "failed"
            if text not in {"pending", "ready", "running", "completed", "failed"}:
                continue
            if text not in normalized_statuses:
                normalized_statuses.append(text)
        where_sql = ["1=1"]
        params: list[Any] = []
        if domains:
            where_sql.append("root_domain = ANY(%s)")
            params.append(domains)
        if normalized_statuses:
            where_sql.append("status = ANY(%s)")
            params.append(normalized_statuses)
        where_clause = " AND ".join(where_sql)
        if hard_delete:
            sql = f"""
DELETE FROM coordinator_targets
WHERE {where_clause};
"""
        else:
            sql = f"""
UPDATE coordinator_targets
SET status = 'pending',
    error = NULL,
    exit_code = NULL,
    worker_id = NULL,
    lease_expires_at = NULL,
    started_at_utc = NULL,
    completed_at_utc = NULL,
    heartbeat_at_utc = NULL,
    attempt_count = 0,
    updated_at_utc = NOW()
WHERE {where_clause};
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                affected = int(cur.rowcount or 0)
            conn.commit()
        self.record_system_event(
            "target.reset",
            "targets",
            {
                "source": "coordinator_store.reset_targets",
                "root_domains": domains,
                "statuses": normalized_statuses,
                "hard_delete": bool(hard_delete),
                "affected_rows": affected,
            },
        )
        return {
            "ok": True,
            "root_domains": domains,
            "statuses": normalized_statuses,
            "hard_delete": bool(hard_delete),
            "affected_rows": affected,
            "reset_at_utc": _iso_now(),
        }

    def delete_artifacts(
        self,
        *,
        root_domain: str,
        artifact_types: Optional[list[str]] = None,
    ) -> int:
        rd = str(root_domain or "").strip().lower()
        if not rd:
            return 0
        types = [
            str(item or "").strip().lower()
            for item in (artifact_types or [])
            if str(item or "").strip()
        ]
        where_sql = ["root_domain = %s"]
        params: list[Any] = [rd]
        if types:
            where_sql.append("artifact_type = ANY(%s)")
            params.append(types)
        where_clause = " AND ".join(where_sql)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM coordinator_artifact_manifest_entries WHERE {where_clause};", tuple(params))
                cur.execute(f"DELETE FROM coordinator_artifacts WHERE {where_clause};", tuple(params))
                deleted = int(cur.rowcount or 0)
            conn.commit()
        self.record_system_event(
            "artifact.deleted",
            f"artifact:{rd}",
            {
                "source": "coordinator_store.delete_artifacts",
                "root_domain": rd,
                "artifact_types": types,
                "deleted_rows": deleted,
            },
        )
        return deleted

    def delete_sessions(
        self,
        *,
        root_domains: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        domains = [
            str(item or "").strip().lower()
            for item in (root_domains or [])
            if str(item or "").strip()
        ]
        if not domains:
            return {
                "ok": True,
                "root_domains": [],
                "affected_rows": 0,
            }
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM coordinator_sessions WHERE root_domain = ANY(%s);", (domains,))
                deleted = int(cur.rowcount or 0)
            conn.commit()
        self.record_system_event(
            "session.deleted",
            "sessions",
            {
                "source": "coordinator_store.delete_sessions",
                "root_domains": domains,
                "deleted_rows": deleted,
            },
        )
        return {
            "ok": True,
            "root_domains": domains,
            "affected_rows": deleted,
        }

    def upload_artifact(
        self,
        *,
        root_domain: str,
        artifact_type: str,
        content: bytes | None = None,
        source_worker: str = "",
        content_encoding: str = "identity",
        manifest: Optional[dict[str, Any]] = None,
        retention_class: str = "derived_rebuildable",
        media_type: str = "application/octet-stream",
    ) -> bool:
        rd = str(root_domain or "").strip().lower()
        at = str(artifact_type or "").strip().lower()
        if not rd or not at:
            return False
        data = bytes(content or b"")
        manifest_dict = dict(manifest or {}) if isinstance(manifest, dict) else {}
        if data:
            metadata = self._artifact_store.put_bytes(
                artifact_type=at,
                root_domain=rd,
                payload=data,
                media_type=media_type or "application/octet-stream",
                encoding=content_encoding or "binary",
                worker_id=source_worker or "",
            )
            inline_content = data if len(data) <= self._db_inline_artifact_max_bytes and at.endswith(("_json", "_txt", "_summary")) else None
            sha256 = metadata.sha256
            storage_backend = metadata.storage_backend
            storage_uri = metadata.storage_uri
            compression = metadata.compression
            media_type_text = metadata.media_type
            size_bytes = len(data)
        else:
            sha256 = str(manifest_dict.get("content_sha256") or hashlib.sha256(json.dumps(manifest_dict, sort_keys=True, default=str).encode("utf-8")).hexdigest())
            storage_backend = str(manifest_dict.get("storage_backend") or "manifest").strip() or "manifest"
            storage_uri = str(manifest_dict.get("storage_uri") or "").strip()
            compression = str(manifest_dict.get("compression") or "identity").strip() or "identity"
            media_type_text = str(manifest_dict.get("media_type") or media_type or "application/octet-stream")
            size_bytes = int(manifest_dict.get("content_size_bytes") or 0)
            inline_content = None
        summary_match_count = int(manifest_dict.get("match_count") or manifest_dict.get("summary_match_count") or 0)
        summary_anomaly_count = int(manifest_dict.get("anomaly_count") or manifest_dict.get("summary_anomaly_count") or 0)
        summary_request_count = int(manifest_dict.get("request_count") or manifest_dict.get("summary_request_count") or 0)
        sql = """
INSERT INTO coordinator_artifacts(
    root_domain, artifact_type, source_worker, content, content_encoding, content_sha256, content_size_bytes,
    storage_backend, storage_uri, media_type, compression, schema_version, manifest_json, retention_class,
    summary_match_count, summary_anomaly_count, summary_request_count, updated_at_utc
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, NOW())
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
    manifest_json = EXCLUDED.manifest_json,
    retention_class = EXCLUDED.retention_class,
    summary_match_count = EXCLUDED.summary_match_count,
    summary_anomaly_count = EXCLUDED.summary_anomaly_count,
    summary_request_count = EXCLUDED.summary_request_count,
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
                        sha256,
                        size_bytes,
                        storage_backend,
                        storage_uri,
                        media_type_text,
                        compression,
                        registry.current_version("artifact_metadata"),
                        json.dumps(manifest_dict, ensure_ascii=False, default=str),
                        str(retention_class or "derived_rebuildable")[:64],
                        summary_match_count,
                        summary_anomaly_count,
                        summary_request_count,
                    ),
                )
                if storage_uri:
                    cur.execute(
                        """
INSERT INTO coordinator_artifact_objects(
    content_sha256, storage_backend, storage_uri, content_size_bytes, media_type, compression, ref_count, updated_at_utc
)
VALUES (%s, %s, %s, %s, %s, %s, 1, NOW())
ON CONFLICT (content_sha256) DO UPDATE
SET ref_count = coordinator_artifact_objects.ref_count + 1,
    updated_at_utc = NOW();
""",
                        (sha256, storage_backend, storage_uri, size_bytes, media_type_text, compression),
                    )
                manifest_entries = manifest_dict.get("entries")
                if not isinstance(manifest_entries, list):
                    manifest_entries = manifest_dict.get("files") if isinstance(manifest_dict.get("files"), list) else []
                for entry in manifest_entries:
                    if not isinstance(entry, dict):
                        continue
                    entry_path = str(entry.get("path") or entry.get("entry_path") or entry.get("name") or "").strip()
                    entry_hash = str(entry.get("content_sha256") or entry.get("sha256") or "").strip().lower()
                    if not entry_path:
                        continue
                    if not entry_hash:
                        entry_hash = hashlib.sha256(entry_path.encode("utf-8")).hexdigest()
                    entry_size = int(entry.get("content_size_bytes") or entry.get("size_bytes") or entry.get("size") or 0)
                    entry_uri = str(entry.get("storage_uri") or "").strip()
                    entry_media = str(entry.get("media_type") or media_type_text or "application/octet-stream")
                    shard_key = str(entry.get("shard_key") or entry.get("bucket") or "").strip()
                    logical_role = str(entry.get("logical_role") or entry.get("role") or "").strip()
                    cur.execute(
                        """
INSERT INTO coordinator_artifact_manifest_entries(
    root_domain, artifact_type, entry_path, content_sha256, content_size_bytes,
    media_type, storage_uri, shard_key, logical_role, metadata_json
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (root_domain, artifact_type, entry_path) DO UPDATE
SET content_sha256 = EXCLUDED.content_sha256,
    content_size_bytes = EXCLUDED.content_size_bytes,
    media_type = EXCLUDED.media_type,
    storage_uri = EXCLUDED.storage_uri,
    shard_key = EXCLUDED.shard_key,
    logical_role = EXCLUDED.logical_role,
    metadata_json = EXCLUDED.metadata_json;
""",
                        (
                            rd,
                            at,
                            entry_path,
                            entry_hash,
                            entry_size,
                            entry_media,
                            entry_uri,
                            shard_key,
                            logical_role,
                            json.dumps(entry, ensure_ascii=False, default=str),
                        ),
                    )
                summary_payload = json.dumps(
                    {
                        "schema_version": registry.current_version("summary_envelope"),
                        "stage": "artifact_upload",
                        "status": "completed",
                        "root_domain": rd,
                        "counts": {"artifacts": 1},
                        "metrics": {"artifact_bytes": float(size_bytes), "risk_score": self._risk_score_for_artifact(at, size_bytes)},
                        "output_artifacts": {at: sha256},
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
        # New artifacts may unblock waiting stage tasks that require files/data
        # produced by earlier plugins.
        try:
            self.refresh_stage_task_readiness(root_domain=rd, limit=1000)
        except Exception:
            pass
        self.record_system_event(
            "artifact.uploaded",
            f"artifact:{rd}:{at}",
            {
                "source": "coordinator_store.upload_artifact",
                "root_domain": rd,
                "artifact_type": at,
                "worker_id": str(source_worker or "")[:200],
                "sha256": sha256,
                "size_bytes": size_bytes,
                "storage_backend": storage_backend,
                "storage_uri": storage_uri,
                "compression": compression,
                "retention_class": str(retention_class or "derived_rebuildable"),
                "risk_score": self._risk_score_for_artifact(at, size_bytes),
                "table": "coordinator_artifacts",
            },
        )
        return True

    def list_artifact_manifest_entries(
        self,
        root_domain: str,
        artifact_type: str,
        *,
        shard_key: str = "",
        logical_role: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        at = str(artifact_type or "").strip().lower()
        if not rd or not at:
            return []
        filters = ["root_domain = %s", "artifact_type = %s"]
        params: list[Any] = [rd, at]
        if shard_key:
            filters.append("shard_key = %s")
            params.append(str(shard_key).strip())
        if logical_role:
            filters.append("logical_role = %s")
            params.append(str(logical_role).strip())
        params.append(max(1, min(10000, int(limit or 1000))))
        sql = f"""
SELECT entry_path, content_sha256, content_size_bytes, media_type, storage_uri, shard_key, logical_role, metadata_json
FROM coordinator_artifact_manifest_entries
WHERE {' AND '.join(filters)}
ORDER BY entry_path ASC
LIMIT %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            conn.commit()
        return [
            {
                "path": str(row[0] or ""),
                "content_sha256": str(row[1] or ""),
                "content_size_bytes": int(row[2] or 0),
                "media_type": str(row[3] or "application/octet-stream"),
                "storage_uri": str(row[4] or ""),
                "shard_key": str(row[5] or ""),
                "logical_role": str(row[6] or ""),
                "metadata": row[7] if isinstance(row[7], dict) else {},
            }
            for row in rows
        ]

    def get_artifact_metadata(self, root_domain: str, artifact_type: str) -> Optional[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        at = str(artifact_type or "").strip().lower()
        if not rd or not at:
            return None
        sql = """
SELECT root_domain, artifact_type, source_worker, content_encoding, content_sha256, content_size_bytes,
       storage_backend, storage_uri, media_type, compression, schema_version, updated_at_utc,
       manifest_json, retention_class, summary_match_count, summary_anomaly_count, summary_request_count
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
            "content_encoding": row[3],
            "content_sha256": row[4],
            "content_size_bytes": int(row[5] or 0),
            "storage_backend": row[6] or "filesystem",
            "storage_uri": row[7] or "",
            "media_type": row[8] or "application/octet-stream",
            "compression": row[9] or "identity",
            "schema_version": int(row[10] or 1),
            "updated_at_utc": row[11].isoformat() if row[11] else None,
            "manifest": row[12] if isinstance(row[12], dict) else {},
            "retention_class": str(row[13] or "derived_rebuildable"),
            "summary_match_count": int(row[14] or 0),
            "summary_anomaly_count": int(row[15] or 0),
            "summary_request_count": int(row[16] or 0),
        }

    def get_artifact(self, root_domain: str, artifact_type: str, *, include_content: bool = True) -> Optional[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        at = str(artifact_type or "").strip().lower()
        if not rd or not at:
            return None
        sql = """
SELECT root_domain, artifact_type, source_worker, content, content_encoding, content_sha256, content_size_bytes,
       storage_backend, storage_uri, media_type, compression, schema_version, updated_at_utc,
       manifest_json, retention_class, summary_match_count, summary_anomaly_count, summary_request_count
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
        payload = {
            "root_domain": row[0],
            "artifact_type": row[1],
            "source_worker": row[2],
            "content_encoding": row[4],
            "content_sha256": row[5],
            "content_size_bytes": int(row[6] or 0),
            "storage_backend": row[7] or "filesystem",
            "storage_uri": row[8] or "",
            "media_type": row[9] or "application/octet-stream",
            "compression": row[10] or "identity",
            "schema_version": int(row[11] or 1),
            "updated_at_utc": row[12].isoformat() if row[12] else None,
            "manifest": row[13] if isinstance(row[13], dict) else {},
            "retention_class": str(row[14] or "derived_rebuildable"),
            "summary_match_count": int(row[15] or 0),
            "summary_anomaly_count": int(row[16] or 0),
            "summary_request_count": int(row[17] or 0),
        }
        if not include_content:
            return payload
        content = bytes(row[3] or b"")
        if payload["storage_uri"] and (not content):
            try:
                content = self._artifact_store.get_bytes(payload["storage_uri"], compression=payload["compression"])
            except Exception:
                content = b""
        payload["content"] = content
        return payload

    def get_artifact_stream_path(self, root_domain: str, artifact_type: str) -> Optional[dict[str, Any]]:
        meta = self.get_artifact_metadata(root_domain, artifact_type)
        if not meta:
            return None
        storage_uri = str(meta.get("storage_uri") or "").strip()
        if not storage_uri:
            return None
        try:
            path = self._artifact_store.resolve_uri(storage_uri)
        except Exception:
            return None
        if not path.exists():
            return None
        return {"path": str(path), "compression": str(meta.get("compression") or "identity"), "metadata": meta}

    def list_artifacts(self, root_domain: str) -> list[dict[str, Any]]:
        rd = str(root_domain or "").strip().lower()
        if not rd:
            return []
        sql = """
SELECT artifact_type, source_worker, content_encoding, content_sha256, content_size_bytes,
       storage_backend, storage_uri, media_type, compression, schema_version, updated_at_utc,
       manifest_json, retention_class, summary_match_count, summary_anomaly_count, summary_request_count
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
                    "storage_backend": row[5] or "filesystem",
                    "storage_uri": row[6] or "",
                    "media_type": row[7] or "application/octet-stream",
                    "compression": row[8] or "identity",
                    "schema_version": int(row[9] or 1),
                    "updated_at_utc": row[10].isoformat() if row[10] else None,
                    "manifest": row[11] if isinstance(row[11], dict) else {},
                    "retention_class": str(row[12] or "derived_rebuildable"),
                    "summary_match_count": int(row[13] or 0),
                    "summary_anomaly_count": int(row[14] or 0),
                    "summary_request_count": int(row[15] or 0),
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
