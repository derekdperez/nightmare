#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import os
import random
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from http_client import get_shared_client, request_capped


@dataclass
class QueuedHttpRequest:
    request_id: str
    method: str
    url: str
    headers: dict[str, str]
    body: bytes | None
    timeout_seconds: float
    read_limit: int
    metadata: dict[str, Any]
    attempts: int
    max_attempts: int


class HttpRequestQueue:
    def __init__(
        self,
        db_path: str | os.PathLike[str],
        spool_dir: str | os.PathLike[str],
        *,
        lease_seconds: int = 90,
        retry_base_seconds: float = 1.0,
        retry_max_seconds: float = 60.0,
        client: httpx.Client | None = None,
        worker_id: str | None = None,
        queue_mode: str = "durable",
    ) -> None:
        self.db_path = Path(db_path)
        self.spool_dir = Path(spool_dir)
        self.lease_seconds = max(5, int(lease_seconds))
        self.retry_base_seconds = max(0.25, float(retry_base_seconds))
        self.retry_max_seconds = max(self.retry_base_seconds, float(retry_max_seconds))
        self.worker_id = worker_id or f"worker-{os.getpid()}-{threading.get_ident()}"
        self.queue_mode = str(queue_mode or os.getenv("NIGHTMARE_HTTP_QUEUE_MODE", "durable")).strip().lower() or "durable"
        if self.queue_mode not in {"durable", "fast"}:
            self.queue_mode = "durable"
        self._result_events: dict[str, threading.Event] = {}
        self._event_lock = threading.Lock()
        self._client = client or get_shared_client()
        self._spool_lock = threading.Lock()
        self._db_lock = threading.Lock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS http_request_queue (
                    request_id TEXT PRIMARY KEY,
                    dedupe_key TEXT,
                    status TEXT NOT NULL,
                    method TEXT NOT NULL,
                    url TEXT NOT NULL,
                    headers_json TEXT NOT NULL,
                    body_base64 TEXT NOT NULL,
                    timeout_seconds REAL NOT NULL,
                    read_limit INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 100,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    scheduled_at REAL NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 5,
                    leased_by TEXT,
                    lease_expires_at REAL,
                    last_error TEXT,
                    last_status_code INTEGER,
                    response_json TEXT,
                    spool_file TEXT,
                    spool_offset INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_http_request_queue_claim
                ON http_request_queue(status, scheduled_at, priority, created_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_http_request_queue_lease
                ON http_request_queue(status, lease_expires_at)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS http_request_attempt (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    started_at REAL NOT NULL,
                    finished_at REAL,
                    outcome TEXT NOT NULL,
                    status_code INTEGER,
                    error_text TEXT,
                    response_json TEXT
                )
                """
            )

    def _append_spool(self, payload: dict[str, Any]) -> tuple[str, int]:
        spool_path = self.spool_dir / f"{time.strftime('%Y%m%d-%H')}.ndjson"
        encoded = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        with self._spool_lock:
            with open(spool_path, "ab") as fh:
                offset = fh.tell()
                fh.write(encoded)
                fh.flush()
                os.fsync(fh.fileno())
        return (str(spool_path), int(offset))

    def _get_event(self, request_id: str) -> threading.Event:
        with self._event_lock:
            event = self._result_events.get(request_id)
            if event is None:
                event = threading.Event()
                self._result_events[request_id] = event
            return event

    def _notify_result(self, request_id: str) -> None:
        with self._event_lock:
            event = self._result_events.get(request_id)
            if event is not None:
                event.set()

    def _discard_event(self, request_id: str) -> None:
        with self._event_lock:
            self._result_events.pop(request_id, None)

    def enqueue(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        timeout_seconds: float = 30.0,
        read_limit: int = 4096,
        metadata: dict[str, Any] | None = None,
        priority: int = 100,
        scheduled_at: float | None = None,
        dedupe_key: str | None = None,
        max_attempts: int = 5,
    ) -> str:
        request_id = str(uuid.uuid4())
        now = time.time()
        payload = {
            "event": "enqueue",
            "request_id": request_id,
            "created_at": now,
            "method": method.upper(),
            "url": url,
            "headers": headers or {},
            "body_base64": base64.b64encode(body or b"").decode("ascii"),
            "timeout_seconds": float(timeout_seconds),
            "read_limit": int(read_limit),
            "metadata": metadata or {},
            "priority": int(priority),
            "scheduled_at": float(scheduled_at if scheduled_at is not None else now),
            "dedupe_key": dedupe_key,
            "max_attempts": int(max_attempts),
        }
        spool_file, spool_offset = self._append_spool(payload)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if dedupe_key:
                existing = conn.execute(
                    "SELECT request_id FROM http_request_queue WHERE dedupe_key = ? AND status IN ('queued','leased','retry_wait') LIMIT 1",
                    (dedupe_key,),
                ).fetchone()
                if existing:
                    conn.execute("COMMIT")
                    return str(existing["request_id"])
            conn.execute(
                """
                INSERT INTO http_request_queue (
                    request_id, dedupe_key, status, method, url, headers_json, body_base64,
                    timeout_seconds, read_limit, metadata_json, priority, created_at, updated_at,
                    scheduled_at, attempts, max_attempts, leased_by, lease_expires_at, last_error,
                    last_status_code, response_json, spool_file, spool_offset
                ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, NULL, NULL, NULL, NULL, ?, ?)
                """,
                (
                    request_id,
                    dedupe_key,
                    method.upper(),
                    url,
                    json.dumps(headers or {}, ensure_ascii=False),
                    base64.b64encode(body or b"").decode("ascii"),
                    float(timeout_seconds),
                    int(read_limit),
                    json.dumps(metadata or {}, ensure_ascii=False),
                    int(priority),
                    now,
                    now,
                    float(scheduled_at if scheduled_at is not None else now),
                    int(max_attempts),
                    spool_file,
                    int(spool_offset),
                ),
            )
            conn.execute("COMMIT")
        return request_id

    def requeue_expired_leases(self) -> int:
        now = time.time()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE http_request_queue
                   SET status='queued',
                       leased_by=NULL,
                       lease_expires_at=NULL,
                       updated_at=?,
                       last_error=COALESCE(last_error, 'lease expired')
                 WHERE status='leased' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
                """,
                (now, now),
            )
            return int(cur.rowcount or 0)

    def claim_next(self) -> QueuedHttpRequest | None:
        now = time.time()
        lease_expires_at = now + float(self.lease_seconds)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT request_id, method, url, headers_json, body_base64, timeout_seconds,
                       read_limit, metadata_json, attempts, max_attempts
                  FROM http_request_queue
                 WHERE status IN ('queued','retry_wait')
                   AND scheduled_at <= ?
                 ORDER BY priority ASC, scheduled_at ASC, created_at ASC
                 LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            conn.execute(
                """
                UPDATE http_request_queue
                   SET status='leased',
                       leased_by=?,
                       lease_expires_at=?,
                       updated_at=?,
                       attempts=attempts+1
                 WHERE request_id=?
                """,
                (self.worker_id, lease_expires_at, now, row["request_id"]),
            )
            attempt_no = int(row["attempts"] or 0) + 1
            conn.execute(
                """
                INSERT INTO http_request_attempt(request_id, worker_id, attempt_no, started_at, outcome)
                VALUES (?, ?, ?, ?, 'leased')
                """,
                (row["request_id"], self.worker_id, attempt_no, now),
            )
            conn.execute("COMMIT")
            return QueuedHttpRequest(
                request_id=str(row["request_id"]),
                method=str(row["method"]),
                url=str(row["url"]),
                headers=json.loads(row["headers_json"] or "{}"),
                body=base64.b64decode(row["body_base64"] or ""),
                timeout_seconds=float(row["timeout_seconds"] or 30.0),
                read_limit=int(row["read_limit"] or 4096),
                metadata=json.loads(row["metadata_json"] or "{}"),
                attempts=attempt_no,
                max_attempts=int(row["max_attempts"] or 5),
            )

    def claim_request_if_ready(self, request_id: str) -> QueuedHttpRequest | None:
        """Claim a specific request_id if it is queued/retry_wait and ready (not behind global FIFO).

        Used by :meth:`submit_and_wait` so a waiter is not starved behind unrelated jobs when many
        requests are outstanding (parallel Fozzy / Nightmare workers sharing one queue DB).
        """
        rid = str(request_id or "").strip()
        if not rid:
            return None
        now = time.time()
        lease_expires_at = now + float(self.lease_seconds)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT request_id, method, url, headers_json, body_base64, timeout_seconds,
                       read_limit, metadata_json, attempts, max_attempts
                  FROM http_request_queue
                 WHERE request_id = ?
                   AND status IN ('queued','retry_wait')
                   AND scheduled_at <= ?
                """,
                (rid, now),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            conn.execute(
                """
                UPDATE http_request_queue
                   SET status='leased',
                       leased_by=?,
                       lease_expires_at=?,
                       updated_at=?,
                       attempts=attempts+1
                 WHERE request_id=?
                """,
                (self.worker_id, lease_expires_at, now, rid),
            )
            attempt_no = int(row["attempts"] or 0) + 1
            conn.execute(
                """
                INSERT INTO http_request_attempt(request_id, worker_id, attempt_no, started_at, outcome)
                VALUES (?, ?, ?, ?, 'leased')
                """,
                (rid, self.worker_id, attempt_no, now),
            )
            conn.execute("COMMIT")
        return QueuedHttpRequest(
            request_id=str(row["request_id"]),
            method=str(row["method"]),
            url=str(row["url"]),
            headers=json.loads(row["headers_json"] or "{}"),
            body=base64.b64decode(row["body_base64"] or ""),
            timeout_seconds=float(row["timeout_seconds"] or 30.0),
            read_limit=int(row["read_limit"] or 4096),
            metadata=json.loads(row["metadata_json"] or "{}"),
            attempts=attempt_no,
            max_attempts=int(row["max_attempts"] or 5),
        )

    def load_result(self, request_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status, response_json, last_error, last_status_code FROM http_request_queue WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        if row is None:
            return None
        status = str(row["status"])
        if status in {"succeeded", "dead_letter", "failed"}:
            response = json.loads(row["response_json"] or "null")
            if isinstance(response, dict):
                return response
            return {
                "ok": status == "succeeded",
                "status_code": row["last_status_code"],
                "note": row["last_error"] or status,
                "response": None,
            }
        return None

    def mark_success(self, request_id: str, result: dict[str, Any], status_code: int | None = None) -> None:
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE http_request_queue
                   SET status='succeeded',
                       updated_at=?,
                       leased_by=NULL,
                       lease_expires_at=NULL,
                       last_status_code=?,
                       response_json=?,
                       last_error=NULL
                 WHERE request_id=?
                """,
                (now, status_code, json.dumps(result, ensure_ascii=False), request_id),
            )
            conn.execute(
                """
                UPDATE http_request_attempt
                   SET finished_at=?, outcome='succeeded', status_code=?, response_json=?
                 WHERE request_id=? AND worker_id=? AND finished_at IS NULL
                """,
                (now, status_code, json.dumps(result, ensure_ascii=False), request_id, self.worker_id),
            )

    def mark_retry(self, request_id: str, result: dict[str, Any], status_code: int | None, error_text: str | None, attempt_no: int, max_attempts: int) -> None:
        now = time.time()
        if attempt_no >= max_attempts:
            self.mark_dead_letter(request_id, result, status_code, error_text)
            return
        delay_cap = min(self.retry_max_seconds, self.retry_base_seconds * (2 ** max(0, attempt_no - 1)))
        backoff = random.uniform(0.0, delay_cap)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE http_request_queue
                   SET status='retry_wait',
                       updated_at=?,
                       scheduled_at=?,
                       leased_by=NULL,
                       lease_expires_at=NULL,
                       last_status_code=?,
                       response_json=?,
                       last_error=?
                 WHERE request_id=?
                """,
                (now, now + backoff, status_code, json.dumps(result, ensure_ascii=False), error_text, request_id),
            )
            conn.execute(
                """
                UPDATE http_request_attempt
                   SET finished_at=?, outcome='retry_wait', status_code=?, error_text=?, response_json=?
                 WHERE request_id=? AND worker_id=? AND finished_at IS NULL
                """,
                (now, status_code, error_text, json.dumps(result, ensure_ascii=False), request_id, self.worker_id),
            )
        self._notify_result(request_id)

    def mark_dead_letter(self, request_id: str, result: dict[str, Any], status_code: int | None, error_text: str | None) -> None:
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE http_request_queue
                   SET status='dead_letter',
                       updated_at=?,
                       leased_by=NULL,
                       lease_expires_at=NULL,
                       last_status_code=?,
                       response_json=?,
                       last_error=?
                 WHERE request_id=?
                """,
                (now, status_code, json.dumps(result, ensure_ascii=False), error_text, request_id),
            )
            conn.execute(
                """
                UPDATE http_request_attempt
                   SET finished_at=?, outcome='dead_letter', status_code=?, error_text=?, response_json=?
                 WHERE request_id=? AND worker_id=? AND finished_at IS NULL
                """,
                (now, status_code, error_text, json.dumps(result, ensure_ascii=False), request_id, self.worker_id),
            )
        self._notify_result(request_id)

    @staticmethod
    def is_retryable(status_code: int | None, error_text: str | None) -> bool:
        if status_code is None:
            return True
        return status_code in {408, 425, 429} or 500 <= int(status_code) <= 599

    def execute_claimed(self, job: QueuedHttpRequest) -> dict[str, Any]:
        status_code: int | None = None
        try:
            rsp = request_capped(
                job.method,
                job.url,
                headers=job.headers,
                content=job.body,
                timeout_seconds=job.timeout_seconds,
                read_limit=job.read_limit,
                client=self._client,
            )
            status_code = int(rsp.status_code)
            result = {
                "ok": status_code < 400,
                "status_code": status_code,
                "request_id": job.request_id,
                "request": {
                    "method": job.method,
                    "url": job.url,
                    "headers": job.headers,
                    "body_base64": base64.b64encode(job.body or b"").decode("ascii"),
                },
                "response": {
                    "status": status_code,
                    "url": rsp.url,
                    "headers": rsp.headers,
                    "body_text_preview": rsp.body.decode("utf-8", errors="replace"),
                    "body_base64": base64.b64encode(rsp.body).decode("ascii"),
                    "body_size": len(rsp.body),
                    "elapsed_ms": rsp.elapsed_ms,
                },
                "note": "HTTP request completed",
            }
            if status_code < 400:
                self.mark_success(job.request_id, result, status_code=status_code)
            elif self.is_retryable(status_code, None):
                self.mark_retry(job.request_id, result, status_code, f"HTTP {status_code}", job.attempts, job.max_attempts)
            else:
                self.mark_dead_letter(job.request_id, result, status_code, f"HTTP {status_code}")
            return result
        except httpx.HTTPError as exc:
            result = {
                "ok": False,
                "status_code": status_code,
                "request_id": job.request_id,
                "request": {
                    "method": job.method,
                    "url": job.url,
                    "headers": job.headers,
                    "body_base64": base64.b64encode(job.body or b"").decode("ascii"),
                },
                "response": None,
                "note": f"HTTP request failed: {exc}",
            }
            if self.is_retryable(status_code, str(exc)):
                self.mark_retry(job.request_id, result, status_code, str(exc), job.attempts, job.max_attempts)
            else:
                self.mark_dead_letter(job.request_id, result, status_code, str(exc))
            return result


    def execute_inline(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        timeout_seconds: float = 30.0,
        read_limit: int = 4096,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        request_id = uuid.uuid4().hex
        request_summary = {
            "method": str(method or "GET").upper(),
            "url": str(url or ""),
            "headers": dict(headers or {}),
            "body_base64": base64.b64encode(body or b"").decode("ascii"),
            "metadata": dict(metadata or {}),
        }
        try:
            response = request_capped(
                request_summary["method"],
                request_summary["url"],
                headers=request_summary["headers"],
                body=body,
                timeout_seconds=float(timeout_seconds or 30.0),
                read_limit=int(read_limit or 4096),
                client=self._client,
            )
            result = {
                "ok": 200 <= int(response.status_code) < 400,
                "status_code": int(response.status_code),
                "request_id": request_id,
                "request": request_summary,
                "response": {
                    "url": str(response.url),
                    "headers": dict(response.headers or {}),
                    "body_base64": base64.b64encode(response.body or b"").decode("ascii"),
                    "elapsed_ms": int(response.elapsed_ms or 0),
                },
                "note": None,
            }
            if self.is_retryable(result["status_code"], None) and not result["ok"]:
                result["ok"] = False
            return result
        except Exception as exc:
            return {
                "ok": False,
                "status_code": None,
                "request_id": request_id,
                "request": request_summary,
                "response": None,
                "note": f"HTTP request failed: {exc}",
            }

    def submit_and_wait(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        timeout_seconds: float = 30.0,
        read_limit: int = 4096,
        metadata: dict[str, Any] | None = None,
        priority: int = 100,
        dedupe_key: str | None = None,
        max_attempts: int = 5,
        wait_timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        if self.queue_mode == "fast":
            return self.execute_inline(
                method=method,
                url=url,
                headers=headers,
                body=body,
                timeout_seconds=timeout_seconds,
                read_limit=read_limit,
                metadata=metadata,
            )
        request_id = self.enqueue(
            method=method,
            url=url,
            headers=headers,
            body=body,
            timeout_seconds=timeout_seconds,
            read_limit=read_limit,
            metadata=metadata,
            priority=priority,
            dedupe_key=dedupe_key,
            max_attempts=max_attempts,
        )
        if wait_timeout_seconds is not None:
            wait_budget = float(wait_timeout_seconds)
        else:
            ts = float(timeout_seconds or 30.0)
            ma = max(1, int(max_attempts or 5))
            wait_budget = max(180.0, ts * float(ma) * 2.0 + 90.0)
        deadline = time.time() + wait_budget
        event = self._get_event(request_id)
        while time.time() < deadline:
            self.requeue_expired_leases()
            result = self.load_result(request_id)
            if isinstance(result, dict):
                self._discard_event(request_id)
                return result
            own = self.claim_request_if_ready(request_id)
            if own is not None:
                self.execute_claimed(own)
                continue
            job = self.claim_next()
            if job is not None:
                self.execute_claimed(job)
                continue
            remaining = max(0.0, deadline - time.time())
            event.wait(min(0.5, remaining))
            event.clear()
        self._discard_event(request_id)
        return {
            "ok": False,
            "status_code": None,
            "request_id": request_id,
            "request": {"method": method.upper(), "url": url, "headers": headers or {}, "body_base64": base64.b64encode(body or b'').decode('ascii')},
            "response": None,
            "note": "Timed out waiting for queued HTTP request result",
        }


    def stats(self) -> dict[str, Any]:
        with self._connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS count FROM http_request_queue GROUP BY status").fetchall()
        counts = {str(row["status"]): int(row["count"] or 0) for row in rows}
        return {"db_path": str(self.db_path), "spool_dir": str(self.spool_dir), "queue_mode": self.queue_mode, "counts": counts}
