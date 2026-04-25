#!/usr/bin/env python3
"""Coordinator client and runtime helpers."""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import subprocess
import threading
import time
import zipfile
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import httpx
from http_client import request_json
from nightmare_shared.config import CoordinatorSettings, atomic_write_json, load_env_file_into_os, merged_value, read_json_dict, safe_float, safe_int
from nightmare_shared.error_reporting import report_error


def _read_log_tail(log_path: Path, *, lines: int = 120, max_chars: int = 16000) -> str:
    try:
        with log_path.open("r", encoding="utf-8", errors="ignore") as reader:
            tail = "".join(reader.readlines()[-lines:])
    except Exception:
        return ""
    tail = str(tail or "").strip()
    if max_chars > 0 and len(tail) > max_chars:
        tail = tail[-max_chars:]
    return tail


def summarize_subprocess_failure(program_name: str, exit_code: int | Path, log_path: Path | int) -> str:
    """Return a compact failure summary.

    Accept both the legacy argument order ``(program_name, log_path, exit_code)``
    and the corrected order ``(program_name, exit_code, log_path)`` for backward compatibility.
    """
    if isinstance(exit_code, Path) and isinstance(log_path, int):
        actual_log_path = exit_code
        actual_exit_code = log_path
    else:
        actual_exit_code = int(exit_code)
        actual_log_path = Path(log_path)
    tail = _read_log_tail(actual_log_path)
    if tail:
        lines = [line.strip() for line in tail.splitlines() if str(line).strip()]
        interesting = ""
        for line in reversed(lines):
            if any(token in line for token in ("Error:", "Exception", "Traceback", "NameError:", "RuntimeError:", "ValueError:")):
                interesting = line
                break
        if interesting:
            return f"{program_name} exit code {int(actual_exit_code)}; {interesting}"
    return f"{program_name} exit code {int(actual_exit_code)}"
from nightmare_shared.logging_utils import get_logger

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH_DEFAULT = BASE_DIR / "config" / "coordinator.json"
OUTPUT_ROOT_DEFAULT = BASE_DIR / "output"


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "true" if default else "false") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int = 0) -> int:
    try:
        return int(str(os.getenv(name, str(default)) or str(default)).strip())
    except Exception:
        return int(default)


@dataclass
class CoordinatorConfig:
    server_base_url: str
    api_token: str
    insecure_tls: bool
    output_root: Path
    heartbeat_interval_seconds: float
    lease_seconds: int
    poll_interval_seconds: float
    nightmare_workers: int
    fozzy_workers: int
    extractor_workers: int
    auth0r_workers: int
    python_executable: str
    nightmare_config: Path
    fozzy_config: Path
    extractor_config: Path
    auth0r_config: Path
    upload_session_every_seconds: float
    enable_nightmare: bool
    enable_fozzy: bool
    enable_extractor: bool
    enable_auth0r: bool
    fozzy_process_workers: int
    extractor_process_workers: int
    workflow_config: Path
    workflow_scheduler_enabled: bool
    workflow_scheduler_interval_seconds: float
    plugin_workers: int
    plugin_allowlist: list[str]


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class CoordinatorClient:
    def __init__(self, base_url: str, token: str, timeout_seconds: float = 20.0, verify_ssl: bool = True):
        self.base_url = base_url.rstrip("/")
        self.token = token.strip()
        self.timeout_seconds = timeout_seconds
        self.verify_ssl = verify_ssl
        self.http_log_details = _env_truthy("COORDINATOR_HTTP_LOG_DETAILS", default=True)
        self.http_log_payloads = _env_truthy("COORDINATOR_HTTP_LOG_PAYLOADS", default=True)
        self.http_log_max_chars = _env_int("COORDINATOR_HTTP_LOG_MAX_CHARS", default=0)
        self.http_redact_auth_header = _env_truthy("COORDINATOR_HTTP_REDACT_AUTH_HEADER", default=True)
        self.logger = get_logger(
            "coordinator_client",
            component="coordinator_http_client",
            coordinator_base_url=self.base_url,
        )

    def _headers(self) -> dict[str, str]:
        out = {"Content-Type": "application/json"}
        if self.token:
            out["Authorization"] = f"Bearer {self.token}"
        return out

    def _request_json(self, method: str, path: str, payload: Optional[dict[str, Any] ] = None) -> dict[str, Any]:
        headers = self._headers()
        url = f"{self.base_url}{path}"
        return request_json(
            method,
            url,
            headers=headers,
            json_payload=payload,
            timeout_seconds=self.timeout_seconds,
            user_agent="nightmare-coordinator/1.0",
            verify=self.verify_ssl,
            logger=self.logger,
            log_details=self.http_log_details,
            include_payloads=self.http_log_payloads,
            max_logged_body_chars=(self.http_log_max_chars if self.http_log_max_chars > 0 else None),
            redact_authorization_header=self.http_redact_auth_header,
        )


    def _stream_headers(self) -> dict[str, str]:
        out = {}
        if self.token:
            out["Authorization"] = f"Bearer {self.token}"
        return out

    def upload_artifact_manifest(
        self,
        root_domain: str,
        artifact_type: str,
        manifest: dict[str, Any],
        *,
        source_worker: str,
        content_encoding: str = "identity",
        retention_class: str = "derived_rebuildable",
        media_type: str = "application/octet-stream",
    ) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/artifact",
            {
                "root_domain": root_domain,
                "artifact_type": artifact_type,
                "source_worker": source_worker,
                "content_encoding": content_encoding,
                "retention_class": retention_class,
                "media_type": media_type,
                "manifest": dict(manifest or {}),
            },
        )
        return bool(rsp.get("ok"))

    def upload_artifact_from_file(
        self,
        root_domain: str,
        artifact_type: str,
        path: Path,
        *,
        source_worker: str,
        content_encoding: str = "identity",
        retention_class: str = "derived_rebuildable",
        media_type: str = "application/octet-stream",
    ) -> bool:
        upload_url = f"{self.base_url}/api/coord/artifact/stream"
        with path.open("rb") as handle:
            with httpx.Client(timeout=self.timeout_seconds, verify=self.verify_ssl, follow_redirects=True) as client:
                response = client.post(
                    upload_url,
                    headers=self._stream_headers(),
                    params={
                        "root_domain": root_domain,
                        "artifact_type": artifact_type,
                        "source_worker": source_worker,
                        "content_encoding": content_encoding,
                        "retention_class": retention_class,
                        "media_type": media_type,
                    },
                    content=handle,
                )
                response.raise_for_status()
                payload = response.json()
        return bool(payload.get("ok"))

    def download_artifact_metadata(self, root_domain: str, artifact_type: str) -> Optional[dict[str, Any]]:
        query = urlencode({"root_domain": root_domain, "artifact_type": artifact_type, "include_content": "false"})
        rsp = self._request_json("GET", f"/api/coord/artifact?{query}")
        if not bool(rsp.get("found")):
            return None
        artifact = rsp.get("artifact")
        return artifact if isinstance(artifact, dict) else None

    def list_artifact_manifest_entries(
        self,
        root_domain: str,
        artifact_type: str,
        *,
        shard_key: str = "",
        logical_role: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        query = urlencode(
            {
                "root_domain": root_domain,
                "artifact_type": artifact_type,
                "shard_key": shard_key,
                "logical_role": logical_role,
                "limit": max(1, int(limit or 1000)),
            }
        )
        rsp = self._request_json("GET", f"/api/coord/artifact/manifest-entries?{query}")
        entries = rsp.get("entries")
        return entries if isinstance(entries, list) else []

    def download_artifact_to_file(self, root_domain: str, artifact_type: str, target_path: Path) -> bool:
        url = f"{self.base_url}/api/coord/artifact/stream"
        with httpx.Client(timeout=None, verify=self.verify_ssl, follow_redirects=True) as client:
            with client.stream(
                "GET",
                url,
                headers=self._stream_headers(),
                params={"root_domain": root_domain, "artifact_type": artifact_type},
            ) as response:
                if response.status_code == 404:
                    return False
                response.raise_for_status()
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with target_path.open("wb") as handle:
                    for chunk in response.iter_bytes():
                        if chunk:
                            handle.write(chunk)
        return True

    def get_workflow_domain(self, root_domain: str) -> dict[str, Any]:
        query = urlencode({"root_domain": root_domain})
        return self._request_json("GET", f"/api/coord/workflow-domain?{query}")

    def claim_target(self, worker_id: str, lease_seconds: int) -> Optional[dict[str, Any] ]:
        rsp = self._request_json(
            "POST",
            "/api/coord/claim",
            {"worker_id": worker_id, "lease_seconds": int(lease_seconds)},
        )
        return rsp.get("entry") if isinstance(rsp.get("entry"), dict) else None

    def heartbeat_target(self, entry_id: str, worker_id: str, lease_seconds: int) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/heartbeat",
            {"entry_id": entry_id, "worker_id": worker_id, "lease_seconds": int(lease_seconds)},
        )
        return bool(rsp.get("ok"))

    def complete_target(self, entry_id: str, worker_id: str, exit_code: int, error: str = "") -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/complete",
            {"entry_id": entry_id, "worker_id": worker_id, "exit_code": int(exit_code), "error": str(error)},
        )
        return bool(rsp.get("ok"))

    def enqueue_stage(
        self,
        root_domain: str,
        stage: str,
        *,
        workflow_id: str = "default",
        checkpoint: Optional[dict[str, Any]] = None,
        progress: Optional[dict[str, Any]] = None,
        progress_artifact_type: str = "",
        resume_mode: str = "exact",
    ) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/enqueue",
            {
                "root_domain": root_domain,
                "stage": stage,
                "workflow_id": "",
                "checkpoint": (dict(checkpoint) if isinstance(checkpoint, dict) else None),
                "progress": (dict(progress) if isinstance(progress, dict) else None),
                "progress_artifact_type": str(progress_artifact_type or "").strip().lower(),
                "resume_mode": str(resume_mode or "exact").strip().lower() or "exact",
            },
        )
        if not bool(rsp.get("ok")):
            return False
        return bool(rsp.get("scheduled", rsp.get("ok")))

    def enqueue_stage_detailed(
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
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/enqueue",
            {
                "root_domain": root_domain,
                "stage": stage,
                "workflow_id": str(workflow_id or "default").strip().lower() or "default",
                "worker_id": str(worker_id or "").strip(),
                "reason": str(reason or "").strip(),
                "allow_retry_failed": bool(allow_retry_failed),
                "max_attempts": int(max_attempts or 0),  # optional; server supplies the default retry policy when omitted/zero
                "checkpoint": (dict(checkpoint) if isinstance(checkpoint, dict) else None),
                "progress": (dict(progress) if isinstance(progress, dict) else None),
                "progress_artifact_type": str(progress_artifact_type or "").strip().lower(),
                "resume_mode": str(resume_mode or "exact").strip().lower() or "exact",
            },
        )
        if not isinstance(rsp, dict):
            return {"ok": False, "scheduled": False}
        return {
            "ok": bool(rsp.get("ok")),
            "scheduled": bool(rsp.get("scheduled", False)),
            "workflow_id": str(rsp.get("workflow_id", workflow_id) or workflow_id),
            "status": str(rsp.get("status", "") or ""),
            "reason": str(rsp.get("reason", "") or ""),
            "attempt_count": safe_int(rsp.get("attempt_count"), 0),
            "root_domain": str(rsp.get("root_domain", root_domain) or root_domain),
            "stage": str(rsp.get("stage", stage) or stage),
            "plugin_name": str(rsp.get("plugin_name", stage) or stage),
        }

    def claim_stage(
        self,
        worker_id: str,
        stage: str,
        lease_seconds: int,
        *,
        workflow_id: str = "default",
    ) -> Optional[dict[str, Any] ]:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/claim",
            {
                "worker_id": worker_id,
                "stage": stage,
                # Workflow is task metadata only; stage acquisition is global.
                "workflow_id": "",
                "lease_seconds": int(lease_seconds),
            },
        )
        return rsp.get("entry") if isinstance(rsp.get("entry"), dict) else None

    def claim_next_stage(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        workflow_id: str = "",
        plugin_allowlist: Optional[list[str]] = None,
    ) -> Optional[dict[str, Any]]:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/claim-next",
            {
                "worker_id": worker_id,
                "workflow_id": str(workflow_id or "").strip().lower(),
                "lease_seconds": int(lease_seconds),
                "plugin_allowlist": [
                    str(item or "").strip().lower()
                    for item in (plugin_allowlist or [])
                    if str(item or "").strip()
                ],
            },
        )
        return rsp.get("entry") if isinstance(rsp.get("entry"), dict) else None

    def heartbeat_stage(
        self,
        worker_id: str,
        root_domain: str,
        stage: str,
        lease_seconds: int,
        *,
        workflow_id: str = "default",
        checkpoint: Optional[dict[str, Any]] = None,
        progress: Optional[dict[str, Any]] = None,
        progress_artifact_type: str = "",
    ) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/heartbeat",
            {
                "worker_id": worker_id,
                "root_domain": root_domain,
                "stage": stage,
                "workflow_id": str(workflow_id or "default").strip().lower() or "default",
                "lease_seconds": int(lease_seconds),
                "checkpoint": (dict(checkpoint) if isinstance(checkpoint, dict) else None),
                "progress": (dict(progress) if isinstance(progress, dict) else None),
                "progress_artifact_type": str(progress_artifact_type or "").strip().lower(),
            },
        )
        return bool(rsp.get("ok"))

    def update_stage_progress(
        self,
        *,
        worker_id: str,
        root_domain: str,
        stage: str,
        workflow_id: str = "default",
        checkpoint: Optional[dict[str, Any]] = None,
        progress: Optional[dict[str, Any]] = None,
        progress_artifact_type: str = "",
    ) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/progress",
            {
                "worker_id": worker_id,
                "root_domain": root_domain,
                "stage": stage,
                "workflow_id": str(workflow_id or "default").strip().lower() or "default",
                "checkpoint": (dict(checkpoint) if isinstance(checkpoint, dict) else None),
                "progress": (dict(progress) if isinstance(progress, dict) else None),
                "progress_artifact_type": str(progress_artifact_type or "").strip().lower(),
            },
        )
        return bool(rsp.get("ok"))

    def complete_stage(
        self,
        worker_id: str,
        root_domain: str,
        stage: str,
        exit_code: int,
        error: str = "",
        *,
        workflow_id: str = "default",
        checkpoint: Optional[dict[str, Any]] = None,
        progress: Optional[dict[str, Any]] = None,
        progress_artifact_type: str = "",
        resume_mode: str = "",
    ) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/complete",
            {
                "worker_id": worker_id,
                "root_domain": root_domain,
                "stage": stage,
                "workflow_id": str(workflow_id or "default").strip().lower() or "default",
                "exit_code": int(exit_code),
                "error": str(error),
                "checkpoint": (dict(checkpoint) if isinstance(checkpoint, dict) else None),
                "progress": (dict(progress) if isinstance(progress, dict) else None),
                "progress_artifact_type": str(progress_artifact_type or "").strip().lower(),
                "resume_mode": str(resume_mode or "").strip().lower(),
            },
        )
        return bool(rsp.get("ok"))

    def reset_stage_tasks(
        self,
        *,
        workflow_id: str = "",
        root_domains: Optional[list[str]] = None,
        plugins: Optional[list[str]] = None,
        hard_delete: bool = False,
    ) -> dict[str, Any]:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/reset",
            {
                "workflow_id": str(workflow_id or "").strip().lower(),
                "root_domains": [
                    str(item or "").strip().lower()
                    for item in (root_domains or [])
                    if str(item or "").strip()
                ],
                "plugins": [
                    str(item or "").strip().lower()
                    for item in (plugins or [])
                    if str(item or "").strip()
                ],
                "hard_delete": bool(hard_delete),
            },
        )
        return rsp if isinstance(rsp, dict) else {"ok": False}

    def load_session(self, root_domain: str) -> Optional[dict[str, Any] ]:
        query = urlencode({"root_domain": root_domain})
        rsp = self._request_json("GET", f"/api/coord/session?{query}")
        if not bool(rsp.get("found")):
            return None
        session = rsp.get("session")
        return session if isinstance(session, dict) else None

    def save_session(self, session_payload: dict[str, Any]) -> bool:
        rsp = self._request_json("POST", "/api/coord/session", {"session": session_payload})
        return bool(rsp.get("ok"))

    def upload_artifact(
        self,
        root_domain: str,
        artifact_type: str,
        content: bytes,
        *,
        source_worker: str,
        content_encoding: str = "identity",
    ) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/artifact",
            {
                "root_domain": root_domain,
                "artifact_type": artifact_type,
                "source_worker": source_worker,
                "content_encoding": content_encoding,
                "content_base64": base64.b64encode(bytes(content)).decode("ascii"),
            },
        )
        return bool(rsp.get("ok"))

    def download_artifact(self, root_domain: str, artifact_type: str) -> Optional[dict[str, Any] ]:
        query = urlencode({"root_domain": root_domain, "artifact_type": artifact_type})
        rsp = self._request_json("GET", f"/api/coord/artifact?{query}")
        if not bool(rsp.get("found")):
            return None
        artifact = rsp.get("artifact")
        if not isinstance(artifact, dict):
            return None
        data_b64 = str(artifact.get("content_base64", "") or "")
        try:
            raw = base64.b64decode(data_b64.encode("ascii"), validate=True)
        except Exception:
            return None
        return {
            **artifact,
            "content": raw,
        }

    def get_fleet_settings(self) -> dict[str, Any]:
        return self._request_json("GET", "/api/coord/fleet-settings")


    def get_workflow_domains(self, *, limit: int = 2000) -> list[str]:
        query = urlencode({"limit": max(1, int(limit or 1))})
        rsp = self._request_json("GET", f"/api/coord/workflow-domains?{query}")
        domains = rsp.get("root_domains")
        if not isinstance(domains, list):
            return []
        return [str(item or "").strip().lower() for item in domains if str(item or "").strip()]

    def get_workflow_snapshot(self, *, limit: int = 2000) -> dict[str, Any]:
        query = urlencode({"limit": max(1, int(limit or 1))})
        return self._request_json("GET", f"/api/coord/workflow-snapshot?{query}")

    def claim_worker_command(self, worker_id: str, *, worker_state: str = "idle") -> Optional[dict[str, Any]]:
        rsp = self._request_json(
            "POST",
            "/api/coord/worker-command/claim",
            {"worker_id": worker_id, "worker_state": worker_state},
        )
        command = rsp.get("command")
        return command if isinstance(command, dict) else None

    def complete_worker_command(
        self,
        worker_id: str,
        command_id: int,
        *,
        success: bool,
        error: str = "",
    ) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/worker-command/complete",
            {
                "worker_id": worker_id,
                "command_id": int(command_id),
                "success": bool(success),
                "error": str(error or ""),
            },
        )
        return bool(rsp.get("ok"))

class SessionUploader(threading.Thread):
    def __init__(
        self,
        client: CoordinatorClient,
        *,
        root_domain: str,
        session_path: Path,
        interval_seconds: float,
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True)
        self.client = client
        self.root_domain = root_domain
        self.session_path = session_path
        self.interval = max(5.0, float(interval_seconds))
        self.stop_event = stop_event
        self.last_mtime_ns = 0
        self.logger = get_logger(
            "coordinator_session_uploader",
            component="session_uploader",
            root_domain=root_domain,
            session_path=str(session_path),
        )

    def run(self) -> None:
        while not self.stop_event.wait(self.interval):
            self.upload_once()

    def upload_once(self) -> None:
        if not self.session_path.is_file():
            return
        try:
            st = self.session_path.stat()
        except OSError:
            return
        if st.st_mtime_ns <= self.last_mtime_ns:
            return
        data = _read_json_dict(self.session_path)
        if not data:
            return
        data["root_domain"] = self.root_domain
        data["saved_at_utc"] = str(data.get("saved_at_utc") or _now_iso())
        try:
            if self.client.save_session(data):
                self.last_mtime_ns = st.st_mtime_ns
                self.logger.info(
                    "session_upload_succeeded",
                    root_domain=self.root_domain,
                    session_path=str(self.session_path),
                )
            else:
                self.logger.error(
                    "session_upload_failed",
                    root_domain=self.root_domain,
                    session_path=str(self.session_path),
                )
        except Exception as exc:
            self.logger.error(
                "session_upload_error",
                root_domain=self.root_domain,
                session_path=str(self.session_path),
                error=str(exc),
            )

class LeaseHeartbeat(threading.Thread):
    def __init__(self, tick_fn, interval_seconds: float, *, logger: Any = None, heartbeat_kind: str = "unknown"):
        super().__init__(daemon=True)
        self._tick_fn = tick_fn
        self._interval = max(5.0, float(interval_seconds))
        self._stop = threading.Event()
        self._logger = logger or get_logger("coordinator_lease_heartbeat", component="lease_heartbeat")
        self._heartbeat_kind = str(heartbeat_kind or "unknown")

    def run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._tick_fn()
                self._logger.info("lease_heartbeat_tick_ok", heartbeat_kind=self._heartbeat_kind)
            except Exception as exc:
                self._logger.error(
                    "lease_heartbeat_tick_failed",
                    heartbeat_kind=self._heartbeat_kind,
                    error=str(exc),
                )
                continue

    def stop(self) -> None:
        self._stop.set()

def _zip_directory_bytes(path: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for file_path in sorted(path.rglob("*")):
            if file_path.is_dir():
                continue
            rel = file_path.relative_to(path).as_posix()
            zf.writestr(rel, file_path.read_bytes())
    return buf.getvalue()

def _unzip_bytes_to_directory(content: bytes, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(content), mode="r") as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            member_name = member.filename.replace("\\", "/").lstrip("/")
            out_path = (target_dir / member_name).resolve()
            try:
                out_path.relative_to(target_dir.resolve())
            except ValueError:
                continue
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(zf.read(member))

def run_subprocess(cmd: list[str], *, cwd: Path, log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_handle:
        log_handle.write(f"\n=== RUN {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n$ {' '.join(cmd)}\n")
        log_handle.flush()
        run_cmd = list(cmd)
        try:
            proc = subprocess.Popen(
                run_cmd,
                cwd=str(cwd),
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except FileNotFoundError as exc:
            command_name = str(run_cmd[0] if run_cmd else "").strip().lower()
            if command_name in {"python", "python3"} and str(sys.executable or "").strip():
                fallback_cmd = [sys.executable, *run_cmd[1:]]
                log_handle.write(
                    f"[coordinator] startup fallback: command '{run_cmd[0]}' was not found; "
                    f"retrying with sys.executable '{sys.executable}'\n"
                )
                log_handle.write(f"$ {' '.join(fallback_cmd)}\n")
                log_handle.flush()
                proc = subprocess.Popen(
                    fallback_cmd,
                    cwd=str(cwd),
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            else:
                log_handle.write(f"[coordinator] subprocess failed to start: {exc}\n")
                log_handle.flush()
                raise
        except Exception as exc:
            log_handle.write(f"[coordinator] subprocess failed to start: {exc}\n")
            log_handle.flush()
            report_error(
                "Subprocess failed to start",
                program_name="coordinator",
                component_name="run_subprocess",
                source_type="worker",
                exception=exc,
                raw_line=str(exc),
                metadata={"command": run_cmd, "log_path": str(log_path)},
            )
            raise
        exit_code = int(proc.wait())
        if exit_code != 0:
            try:
                tail = _read_log_tail(log_path)
                report_error(
                    f"Subprocess exited with non-zero status {exit_code}",
                    program_name="coordinator",
                    component_name="run_subprocess",
                    source_type="worker",
                    raw_line=tail,
                    metadata={"command": run_cmd, "log_path": str(log_path), "exit_code": exit_code},
                )
            except Exception:
                pass
        return exit_code

def load_config(args: argparse.Namespace) -> CoordinatorConfig:
    load_env_file_into_os(BASE_DIR / "deploy" / ".env", override=False)
    config_path = Path(str(args.config)).expanduser()
    if not config_path.is_absolute():
        config_path = (BASE_DIR / config_path).resolve()
    cfg = read_json_dict(config_path)
    settings = CoordinatorSettings.model_validate(
        {
            "server_base_url": merged_value(args.server_base_url, cfg, "server_base_url", os.getenv("COORDINATOR_BASE_URL", "")),
            "api_token": merged_value(args.api_token, cfg, "api_token", os.getenv("COORDINATOR_API_TOKEN", "")),
            "insecure_tls": merged_value(None, cfg, "insecure_tls", os.getenv("COORDINATOR_INSECURE_TLS", False)),
            "output_root": merged_value(args.output_root, cfg, "output_root", str(OUTPUT_ROOT_DEFAULT)),
            "heartbeat_interval_seconds": cfg.get("heartbeat_interval_seconds", 20.0),
            "lease_seconds": cfg.get("lease_seconds", 180),
            "poll_interval_seconds": cfg.get("poll_interval_seconds", 5.0),
            "nightmare_workers": cfg.get("nightmare_workers", 2),
            "fozzy_workers": cfg.get("fozzy_workers", 2),
            "extractor_workers": cfg.get("extractor_workers", 2),
            "auth0r_workers": cfg.get("auth0r_workers", 2),
            "python_executable": merged_value(None, cfg, "python_executable", sys.executable),
            "nightmare_config": BASE_DIR / str(cfg.get("nightmare_config", "config/nightmare.json")),
            "fozzy_config": BASE_DIR / str(cfg.get("fozzy_config", "config/fozzy.json")),
            "extractor_config": BASE_DIR / str(cfg.get("extractor_config", "config/extractor.json")),
            "auth0r_config": BASE_DIR / str(cfg.get("auth0r_config", "config/auth0r.json")),
            "upload_session_every_seconds": cfg.get("upload_session_every_seconds", 15.0),
            "enable_nightmare": bool(cfg.get("enable_nightmare", True)),
            "enable_fozzy": bool(cfg.get("enable_fozzy", True)),
            "enable_extractor": bool(cfg.get("enable_extractor", True)),
            "enable_auth0r": bool(cfg.get("enable_auth0r", True)),
            "fozzy_process_workers": cfg.get("fozzy_process_workers", 1),
            "extractor_process_workers": cfg.get("extractor_process_workers", 1),
            "workflow_config": BASE_DIR / str(cfg.get("workflow_config", "workflows")),
            "workflow_scheduler_enabled": cfg.get("workflow_scheduler_enabled", True),
            "workflow_scheduler_interval_seconds": cfg.get("workflow_scheduler_interval_seconds", 15.0),
            "plugin_workers": cfg.get("plugin_workers", 0),
            "plugin_allowlist": cfg.get("plugin_allowlist", []),
        }
    )
    plugin_allowlist = (
        [str(item or "").strip().lower() for item in (settings.plugin_allowlist or []) if str(item or "").strip()]
        if isinstance(settings.plugin_allowlist, list)
        else []
    )
    plugin_workers_value = max(0, safe_int(settings.plugin_workers, 0))
    output_root = Path(settings.output_root).expanduser()
    if not output_root.is_absolute():
        output_root = (BASE_DIR / output_root).resolve()
    return CoordinatorConfig(
        server_base_url=settings.server_base_url,
        api_token=settings.api_token,
        insecure_tls=settings.insecure_tls,
        output_root=output_root,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
        lease_seconds=settings.lease_seconds,
        poll_interval_seconds=settings.poll_interval_seconds,
        nightmare_workers=settings.nightmare_workers,
        fozzy_workers=settings.fozzy_workers,
        extractor_workers=settings.extractor_workers,
        auth0r_workers=settings.auth0r_workers,
        python_executable=settings.python_executable,
        nightmare_config=Path(settings.nightmare_config).resolve(),
        fozzy_config=Path(settings.fozzy_config).resolve(),
        extractor_config=Path(settings.extractor_config).resolve(),
        auth0r_config=Path(settings.auth0r_config).resolve(),
        upload_session_every_seconds=settings.upload_session_every_seconds,
        enable_nightmare=settings.enable_nightmare,
        enable_fozzy=settings.enable_fozzy,
        enable_extractor=settings.enable_extractor,
        enable_auth0r=settings.enable_auth0r,
        fozzy_process_workers=settings.fozzy_process_workers,
        extractor_process_workers=settings.extractor_process_workers,
        workflow_config=Path(settings.workflow_config).resolve(),
        workflow_scheduler_enabled=bool(settings.workflow_scheduler_enabled),
        workflow_scheduler_interval_seconds=float(settings.workflow_scheduler_interval_seconds),
        plugin_workers=plugin_workers_value,
        plugin_allowlist=plugin_allowlist,
    )
