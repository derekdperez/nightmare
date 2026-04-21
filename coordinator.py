#!/usr/bin/env python3
"""Distributed worker coordinator for Nightmare/Fozzy/Extractor.

This process runs on worker VMs and coordinates work through the central server APIs:
- claims next Nightmare target,
- heartbeats active leases,
- checkpoints crawl session state to Postgres via server APIs,
- enqueues and runs Fozzy + Extractor stage jobs,
- uploads key artifacts so any VM can resume the pipeline,
- polls fleet output_clear_generation and wipes local output/ when the operator bumps it on the central host.
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

from http_client import request_json

from output_cleanup import FLEET_GEN_APPLIED_FILENAME, clear_output_root_children
from nightmare_shared.config import CoordinatorSettings, atomic_write_json, load_env_file_into_os, merged_value, read_json_dict, safe_float, safe_int
from nightmare_shared.error_reporting import install_error_reporting, report_error
from nightmare_shared.logging_utils import configure_logging, get_logger
from coordinator_app.runtime import CoordinatorClient, CoordinatorConfig, SessionUploader, LeaseHeartbeat, _zip_directory_bytes, _unzip_bytes_to_directory, run_subprocess, summarize_subprocess_failure, load_config

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH_DEFAULT = BASE_DIR / "config" / "coordinator.json"
OUTPUT_ROOT_DEFAULT = BASE_DIR / "output"


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _merged_value(cli_value: Any, cfg: dict[str, Any], key: str, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if key in cfg:
        value = cfg[key]
        if value is not None and value != "":
            return value
    return default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _domain_output_dir(root_domain: str, output_root: Path) -> Path:
    return output_root / root_domain


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _default_workflow_entries() -> list[dict[str, Any]]:
    return [
        {
            "name": "auth0r",
            "display_name": "Auth0r",
            "description": "Run auth0r once nightmare session data is available.",
            "handler": "legacy_step_adapter",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "auth0r",
            "enabled": True,
            "prerequisites": {"artifacts_all": ["nightmare_session_json"]},
            "retry_failed": True,
            "max_attempts": 3,
            "parameters": {
                "min_delay_seconds": 0.25,
                "max_seed_actions": 200,
                "timeout_seconds": 20.0,
            },
        },
        {
            "name": "fozzy",
            "display_name": "Fozzy",
            "description": "Run fozzy once nightmare parameter data is available.",
            "handler": "legacy_step_adapter",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "fozzy",
            "enabled": True,
            "prerequisites": {"artifacts_all": ["nightmare_parameters_json"]},
            "retry_failed": True,
            "max_attempts": 3,
            "parameters": {
                "max_background_workers": 1,
                "max_workers_per_domain": 1,
                "max_workers_per_subdomain": 1,
            },
        },
        {
            "name": "extractor",
            "display_name": "Extractor",
            "description": "Run extractor after fozzy output exists.",
            "handler": "legacy_step_adapter",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "extractor",
            "enabled": True,
            "prerequisites": {"artifacts_all": ["fozzy_results_zip"]},
            "retry_failed": True,
            "max_attempts": 3,
            "parameters": {"force": True},
        },
    ]


def _normalize_workflow_entry(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    name = str(raw.get("name") or raw.get("type") or raw.get("stage") or "").strip().lower()
    stage = str(raw.get("stage") or name).strip().lower()
    if not stage:
        return None
    prereq = raw.get("prerequisites", {})
    if not isinstance(prereq, dict):
        prereq = {}
    artifacts_all = prereq.get("artifacts_all", raw.get("requires_artifacts_all", []))
    artifacts_any = prereq.get("artifacts_any", raw.get("requires_artifacts_any", []))
    params = raw.get("parameters", raw.get("config", {}))
    return {
        "name": name or stage,
        "display_name": str(raw.get("display_name") or raw.get("name") or stage).strip(),
        "description": str(raw.get("description") or "").strip(),
        "handler": str(raw.get("handler") or "legacy_step_adapter").strip(),
        "config_schema": (
            dict(raw.get("config_schema"))
            if isinstance(raw.get("config_schema"), dict)
            else {"type": "object", "additionalProperties": True}
        ),
        "stage": stage,
        "enabled": bool(raw.get("enabled", True)),
        "prerequisites": {
            "artifacts_all": [str(item).strip().lower() for item in (artifacts_all if isinstance(artifacts_all, list) else []) if str(item).strip()],
            "artifacts_any": [str(item).strip().lower() for item in (artifacts_any if isinstance(artifacts_any, list) else []) if str(item).strip()],
        },
        "retry_failed": bool(raw.get("retry_failed", True)),
        "max_attempts": max(0, _safe_int(raw.get("max_attempts", 0), 0)),
        "parameters": dict(params) if isinstance(params, dict) else {},
    }


def _load_workflow_entries(path: Path, logger: Any) -> list[dict[str, Any]]:
    payload = _read_json_dict(path)
    candidates: list[Any] = []
    if isinstance(payload.get("stages"), list):
        candidates = list(payload.get("stages") or [])
    elif isinstance(payload.get("steps"), list):
        candidates = list(payload.get("steps") or [])
    if not candidates:
        logger.warning(
            "workflow_config_missing_or_empty_using_defaults",
            workflow_config=str(path),
        )
        return _default_workflow_entries()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in candidates:
        normalized = _normalize_workflow_entry(raw)
        if not normalized:
            continue
        stage = str(normalized.get("stage") or "").strip().lower()
        if not stage or stage in seen:
            continue
        seen.add(stage)
        out.append(normalized)
    if not out:
        logger.warning(
            "workflow_config_entries_invalid_using_defaults",
            workflow_config=str(path),
        )
        return _default_workflow_entries()
    return out


class DistributedCoordinator:
    def __init__(self, cfg: CoordinatorConfig, *, logger: Any | None = None):
        self.cfg = cfg
        # Allow opt-out for local/self-signed coordinator deployments.
        verify_ssl = not bool(cfg.insecure_tls)
        self.client = CoordinatorClient(cfg.server_base_url, cfg.api_token, verify_ssl=verify_ssl)
        host = socket.gethostname().strip() or "worker"
        self.worker_prefix = f"{host}-{uuid.uuid4().hex[:8]}"
        base_logger = logger or get_logger("coordinator_runner", component="distributed_coordinator")
        self.logger = base_logger.bind(worker_prefix=self.worker_prefix)
        self.stop_event = threading.Event()
        self._job_lock = threading.Lock()
        self._active_jobs = 0
        self._fleet_lock = threading.Lock()
        self._worker_state_lock = threading.Lock()
        self._worker_states: dict[str, str] = {}
        self._workflow_entries = _load_workflow_entries(self.cfg.workflow_config, self.logger)
        self._workflow_stage_map: dict[str, dict[str, Any]] = {
            str(item.get("stage") or "").strip().lower(): item for item in self._workflow_entries
        }
        self.logger.info(
            "workflow_scheduler_config_loaded",
            workflow_config=str(self.cfg.workflow_config),
            workflow_scheduler_enabled=bool(self.cfg.workflow_scheduler_enabled),
            workflow_scheduler_interval_seconds=float(self.cfg.workflow_scheduler_interval_seconds),
            workflow_stage_count=len(self._workflow_entries),
            workflow_stages=[str(item.get("stage") or "") for item in self._workflow_entries],
        )

    def _begin_job(self) -> None:
        with self._job_lock:
            self._active_jobs += 1

    def _end_job(self) -> None:
        with self._job_lock:
            self._active_jobs = max(0, self._active_jobs - 1)

    def _set_worker_state(self, worker_id: str, state: str) -> None:
        safe_state = str(state or "").strip().lower()
        if safe_state not in {"running", "paused", "stopped", "errored", "idle"}:
            safe_state = "idle"
        with self._worker_state_lock:
            self._worker_states[str(worker_id)] = safe_state

    def _get_worker_state(self, worker_id: str) -> str:
        with self._worker_state_lock:
            return str(self._worker_states.get(str(worker_id), "running") or "running")

    def _poll_worker_commands(self, worker_id: str) -> str:
        state = self._get_worker_state(worker_id)
        while not self.stop_event.is_set():
            try:
                command_entry = self.client.claim_worker_command(worker_id, worker_state=state)
            except Exception as exc:
                self.logger.error(
                    "worker_command_poll_failed",
                    worker_id=worker_id,
                    worker_state=state,
                    error=str(exc),
                )
                return state
            if not command_entry:
                return state

            command_id = _safe_int(command_entry.get("id", 0), 0)
            command = str(command_entry.get("command", "") or "").strip().lower()
            self.logger.info(
                "worker_command_claimed",
                worker_id=worker_id,
                command_id=command_id,
                command=command,
                prior_state=state,
            )
            ok = True
            err = ""
            if command == "pause":
                state = "paused"
            elif command == "stop":
                state = "stopped"
            elif command == "start":
                state = "running"
            else:
                ok = False
                err = f"unsupported command: {command!r}"
                state = "errored"
            self._set_worker_state(worker_id, state)
            self.logger.info(
                "worker_command_applied",
                worker_id=worker_id,
                command_id=command_id,
                command=command,
                success=ok,
                error=err,
                next_state=state,
            )
            if command_id > 0:
                try:
                    self.client.complete_worker_command(worker_id, command_id, success=ok, error=err)
                except Exception as exc:
                    self.logger.error(
                        "worker_command_complete_failed",
                        worker_id=worker_id,
                        command_id=command_id,
                        command=command,
                        error=str(exc),
                    )
            if not ok:
                return state

    def _read_local_fleet_generation(self) -> int:
        path = self.cfg.output_root / FLEET_GEN_APPLIED_FILENAME
        try:
            text = path.read_text(encoding="utf-8").strip()
            return max(0, int((text.splitlines() or ["0"])[0]))
        except Exception:
            return 0

    def _write_local_fleet_generation(self, generation: int) -> None:
        path = self.cfg.output_root / FLEET_GEN_APPLIED_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{max(0, int(generation))}\n", encoding="utf-8")

    def _maybe_apply_fleet_output_clear(self) -> None:
        """When central bumps output_clear_generation, delete local output/ children (best-effort).

        Intended for drained or idle periods; clearing while jobs run can delete active work.
        """
        with self._fleet_lock:
            with self._job_lock:
                if self._active_jobs > 0:
                    return
            try:
                remote = int(self.client.get_fleet_settings().get("output_clear_generation", 0))
            except Exception as exc:
                self.logger.error(
                    "fleet_settings_fetch_failed",
                    output_root=str(self.cfg.output_root),
                    error=str(exc),
                )
                return
            local = self._read_local_fleet_generation()
            if remote <= local:
                return
            with self._job_lock:
                if self._active_jobs > 0:
                    return
            self.logger.warning(
                "fleet_output_clear_generation_applied",
                previous_generation=local,
                next_generation=remote,
                output_root=str(self.cfg.output_root),
            )
            clear_output_root_children(
                self.cfg.output_root,
                preserve_names=frozenset({FLEET_GEN_APPLIED_FILENAME}),
            )
            self._write_local_fleet_generation(remote)

    def _is_stage_runtime_enabled(self, stage: str) -> bool:
        stg = str(stage or "").strip().lower()
        if stg == "fozzy":
            return bool(self.cfg.enable_fozzy)
        if stg == "extractor":
            return bool(self.cfg.enable_extractor)
        if stg == "auth0r":
            return bool(self.cfg.enable_auth0r)
        return False

    def _workflow_stage_parameters(self, stage: str) -> dict[str, Any]:
        entry = self._workflow_stage_map.get(str(stage or "").strip().lower(), {})
        params = entry.get("parameters") if isinstance(entry, dict) else {}
        return dict(params) if isinstance(params, dict) else {}

    @staticmethod
    def _has_stage_prerequisites(artifacts: set[str], entry: dict[str, Any]) -> bool:
        prereq = entry.get("prerequisites") if isinstance(entry, dict) else {}
        if not isinstance(prereq, dict):
            prereq = {}
        required_all = {
            str(item or "").strip().lower()
            for item in (prereq.get("artifacts_all") or [])
            if str(item or "").strip()
        }
        required_any = {
            str(item or "").strip().lower()
            for item in (prereq.get("artifacts_any") or [])
            if str(item or "").strip()
        }
        if required_all and not required_all.issubset(artifacts):
            return False
        if required_any and artifacts.isdisjoint(required_any):
            return False
        return True

    def _schedule_domain_workflows(self, domain_row: dict[str, Any], *, worker_id: str) -> int:
        root_domain = str(domain_row.get("root_domain", "") or "").strip().lower()
        if not root_domain:
            return 0
        artifacts = {
            str(item or "").strip().lower()
            for item in (domain_row.get("artifact_types") if isinstance(domain_row.get("artifact_types"), list) else [])
            if str(item or "").strip()
        }
        stage_tasks = domain_row.get("stage_tasks") if isinstance(domain_row.get("stage_tasks"), dict) else {}
        scheduled_count = 0
        for entry in self._workflow_entries:
            stage = str(entry.get("stage") or "").strip().lower()
            if not stage or not bool(entry.get("enabled", True)):
                continue
            if not self._is_stage_runtime_enabled(stage):
                continue
            if not self._has_stage_prerequisites(artifacts, entry):
                continue
            task_row = stage_tasks.get(stage) if isinstance(stage_tasks.get(stage), dict) else {}
            status = str(task_row.get("status", "") or "").strip().lower()
            attempt_count = _safe_int(task_row.get("attempt_count", 0), 0)
            retry_failed = bool(entry.get("retry_failed", True))
            max_attempts = max(0, _safe_int(entry.get("max_attempts", 0), 0))
            if status in {"pending", "running", "completed"}:
                continue
            allow_retry_failed = status == "failed" and retry_failed
            if status == "failed" and max_attempts > 0 and attempt_count >= max_attempts:
                continue
            if status == "failed" and not allow_retry_failed:
                continue
            details = self.client.enqueue_stage_detailed(
                root_domain,
                stage,
                worker_id=worker_id,
                reason=f"workflow:{entry.get('name', stage)}",
                allow_retry_failed=allow_retry_failed,
                max_attempts=max_attempts,
            )
            if bool(details.get("scheduled")):
                scheduled_count += 1
                self.logger.info(
                    "workflow_task_scheduled",
                    worker_id=worker_id,
                    root_domain=root_domain,
                    stage=stage,
                    prior_status=status or "none",
                    attempt_count=attempt_count,
                    max_attempts=max_attempts,
                    artifacts_available=sorted(artifacts),
                )
        return scheduled_count

    def _workflow_scheduler_loop(self) -> None:
        worker_id = f"{self.worker_prefix}-scheduler-1"
        self._set_worker_state(worker_id, "running")
        self.logger.info(
            "workflow_scheduler_loop_started",
            worker_id=worker_id,
            workflow_config=str(self.cfg.workflow_config),
            interval_seconds=float(self.cfg.workflow_scheduler_interval_seconds),
        )
        while not self.stop_event.is_set():
            state = self._poll_worker_commands(worker_id)
            if state in {"paused", "stopped", "errored"}:
                self.logger.warning(
                    "workflow_scheduler_waiting_due_to_state",
                    worker_id=worker_id,
                    worker_state=state,
                    poll_interval_seconds=self.cfg.workflow_scheduler_interval_seconds,
                )
                self.stop_event.wait(self.cfg.workflow_scheduler_interval_seconds)
                continue
            try:
                snapshot = self.client.get_workflow_snapshot(limit=5000)
                domain_rows = snapshot.get("domains") if isinstance(snapshot.get("domains"), list) else []
            except Exception as exc:
                self.logger.error("workflow_scheduler_snapshot_failed", worker_id=worker_id, error=str(exc))
                self.stop_event.wait(self.cfg.workflow_scheduler_interval_seconds)
                continue
            scheduled = 0
            for row in domain_rows:
                if not isinstance(row, dict):
                    continue
                try:
                    scheduled += self._schedule_domain_workflows(row, worker_id=worker_id)
                except Exception as exc:
                    self.logger.error(
                        "workflow_scheduler_domain_failed",
                        worker_id=worker_id,
                        root_domain=str(row.get("root_domain", "") or "").strip().lower(),
                        error=str(exc),
                    )
            self.logger.info(
                "workflow_scheduler_cycle_complete",
                worker_id=worker_id,
                domains_checked=len(domain_rows),
                tasks_scheduled=scheduled,
            )
            self.stop_event.wait(self.cfg.workflow_scheduler_interval_seconds)

    def _artifact_paths(self, root_domain: str) -> dict[str, Path]:
        domain_dir = _domain_output_dir(root_domain, self.cfg.output_root)
        fozzy_domain_dir = domain_dir / "fozzy-output" / root_domain
        high_value_dir = self.cfg.output_root / "high_value" / root_domain
        return {
            "nightmare_session_json": domain_dir / f"{root_domain}_crawl_session.json",
            "nightmare_url_inventory_json": domain_dir / f"{root_domain}_url_inventory.json",
            "nightmare_requests_json": domain_dir / "requests.json",
            "nightmare_parameters_json": domain_dir / f"{root_domain}.parameters.json",
            "nightmare_post_requests_json": domain_dir / f"{root_domain}.post.requests.json",
            "nightmare_parameters_txt": domain_dir / f"{root_domain}.parameters.txt",
            "nightmare_source_of_truth_json": domain_dir / f"{root_domain}_source_of_truth.json",
            "nightmare_report_html": domain_dir / "report.html",
            "nightmare_log": domain_dir / f"{root_domain}_nightmare.log",
            "nightmare_scrapy_log": domain_dir / f"{root_domain}_scrapy.log",
            "fozzy_summary_json": fozzy_domain_dir / f"{root_domain}.fozzy.summary.json",
            "fozzy_inventory_json": fozzy_domain_dir / f"{root_domain}.fozzy.inventory.json",
            "fozzy_log": fozzy_domain_dir / f"{root_domain}.fozzy.log",
            "fozzy_results_dir": fozzy_domain_dir / "results",
            "extractor_summary_json": fozzy_domain_dir / "extractor" / "summary.json",
            "extractor_matches_dir": fozzy_domain_dir / "extractor" / "matches",
            "auth0r_summary_json": domain_dir / "auth0r.summary.json",
            "auth0r_log": domain_dir / "coordinator.auth0r.log",
            "nightmare_high_value_dir": high_value_dir,
        }

    def _upload_file_artifact(self, root_domain: str, artifact_type: str, path: Path, worker_id: str) -> None:
        if not path.is_file():
            self.logger.info(
                "artifact_file_not_found",
                root_domain=root_domain,
                worker_id=worker_id,
                artifact_type=artifact_type,
                artifact_path=str(path),
            )
            return
        content = path.read_bytes()
        self.logger.info(
            "artifact_file_upload_start",
            root_domain=root_domain,
            worker_id=worker_id,
            artifact_type=artifact_type,
            artifact_path=str(path),
            bytes=len(content),
        )
        self.client.upload_artifact(root_domain, artifact_type, content, source_worker=worker_id)
        self.logger.info(
            "artifact_file_upload_complete",
            root_domain=root_domain,
            worker_id=worker_id,
            artifact_type=artifact_type,
            artifact_path=str(path),
            bytes=len(content),
        )

    def _upload_zip_artifact(self, root_domain: str, artifact_type: str, path: Path, worker_id: str) -> None:
        if not path.is_dir():
            self.logger.info(
                "artifact_zip_source_not_found",
                root_domain=root_domain,
                worker_id=worker_id,
                artifact_type=artifact_type,
                source_path=str(path),
            )
            return
        self.logger.info(
            "artifact_zip_upload_start",
            root_domain=root_domain,
            worker_id=worker_id,
            artifact_type=artifact_type,
            source_path=str(path),
        )
        payload = _zip_directory_bytes(path)
        self.client.upload_artifact(
            root_domain,
            artifact_type,
            payload,
            source_worker=worker_id,
            content_encoding="zip",
        )
        self.logger.info(
            "artifact_zip_upload_complete",
            root_domain=root_domain,
            worker_id=worker_id,
            artifact_type=artifact_type,
            source_path=str(path),
            bytes=len(payload),
        )

    def _download_file_artifact(self, root_domain: str, artifact_type: str, path: Path) -> bool:
        self.logger.info(
            "artifact_file_download_start",
            root_domain=root_domain,
            artifact_type=artifact_type,
            target_path=str(path),
        )
        artifact = self.client.download_artifact(root_domain, artifact_type)
        if artifact is None:
            self.logger.warning(
                "artifact_file_download_missing",
                root_domain=root_domain,
                artifact_type=artifact_type,
                target_path=str(path),
            )
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        content = bytes(artifact["content"])
        path.write_bytes(content)
        self.logger.info(
            "artifact_file_download_complete",
            root_domain=root_domain,
            artifact_type=artifact_type,
            target_path=str(path),
            bytes=len(content),
        )
        return True

    def _download_zip_artifact(self, root_domain: str, artifact_type: str, target_dir: Path) -> bool:
        self.logger.info(
            "artifact_zip_download_start",
            root_domain=root_domain,
            artifact_type=artifact_type,
            target_dir=str(target_dir),
        )
        artifact = self.client.download_artifact(root_domain, artifact_type)
        if artifact is None:
            self.logger.warning(
                "artifact_zip_download_missing",
                root_domain=root_domain,
                artifact_type=artifact_type,
                target_dir=str(target_dir),
            )
            return False
        content = bytes(artifact["content"])
        _unzip_bytes_to_directory(content, target_dir)
        self.logger.info(
            "artifact_zip_download_complete",
            root_domain=root_domain,
            artifact_type=artifact_type,
            target_dir=str(target_dir),
            bytes=len(content),
        )
        return True

    def _nightmare_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-nightmare-{idx}"
        self._set_worker_state(worker_id, "running")
        self.logger.info("nightmare_worker_loop_started", worker_id=worker_id, worker_index=idx)
        while not self.stop_event.is_set():
            state = self._poll_worker_commands(worker_id)
            if state in {"paused", "stopped", "errored"}:
                self.logger.warning(
                    "nightmare_worker_waiting_due_to_state",
                    worker_id=worker_id,
                    worker_state=state,
                    poll_interval_seconds=self.cfg.poll_interval_seconds,
                )
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            self._maybe_apply_fleet_output_clear()
            try:
                entry = self.client.claim_target(worker_id, self.cfg.lease_seconds)
            except Exception as exc:
                self.logger.error("nightmare_claim_failed", worker_id=worker_id, error=str(exc))
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            if not entry:
                self.logger.info("nightmare_claim_empty", worker_id=worker_id)
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue

            entry_id = str(entry.get("entry_id", "") or "")
            start_url = str(entry.get("start_url", "") or "")
            root_domain = str(entry.get("root_domain", "") or "").strip().lower()
            if not entry_id or not start_url or not root_domain:
                self.logger.error(
                    "nightmare_claim_invalid_entry",
                    worker_id=worker_id,
                    entry=entry,
                )
                self.stop_event.wait(1.0)
                continue
            self._begin_job()
            try:
                self.logger.info(
                    "nightmare_job_start",
                    worker_id=worker_id,
                    entry_id=entry_id,
                    root_domain=root_domain,
                    start_url=start_url,
                )
                paths = self._artifact_paths(root_domain)
                domain_dir = _domain_output_dir(root_domain, self.cfg.output_root)
                domain_dir.mkdir(parents=True, exist_ok=True)

                try:
                    remote_session = self.client.load_session(root_domain)
                    if isinstance(remote_session, dict) and remote_session:
                        session_payload = dict(remote_session.get("payload", remote_session))
                        if session_payload:
                            _atomic_write_json(paths["nightmare_session_json"], session_payload)
                            self.logger.info(
                                "nightmare_session_restored",
                                worker_id=worker_id,
                                entry_id=entry_id,
                                root_domain=root_domain,
                                session_path=str(paths["nightmare_session_json"]),
                            )
                except Exception as exc:
                    self.logger.error(
                        "nightmare_session_restore_failed",
                        worker_id=worker_id,
                        entry_id=entry_id,
                        root_domain=root_domain,
                        error=str(exc),
                    )

                heartbeat = LeaseHeartbeat(
                    tick_fn=lambda: self.client.heartbeat_target(entry_id, worker_id, self.cfg.lease_seconds),
                    interval_seconds=self.cfg.heartbeat_interval_seconds,
                    logger=self.logger,
                    heartbeat_kind="nightmare_target",
                )
                heartbeat.start()

                uploader_stop = threading.Event()
                session_uploader = SessionUploader(
                    self.client,
                    root_domain=root_domain,
                    session_path=paths["nightmare_session_json"],
                    interval_seconds=self.cfg.upload_session_every_seconds,
                    stop_event=uploader_stop,
                )
                session_uploader.start()

                cmd = [
                    self.cfg.python_executable,
                    "nightmare.py",
                    start_url,
                    "--config",
                    str(self.cfg.nightmare_config),
                    "--resume",
                ]
                log_path = domain_dir / "coordinator.nightmare.log"
                exit_code = 1
                err_text = ""
                self.logger.info(
                    "nightmare_subprocess_start",
                    worker_id=worker_id,
                    entry_id=entry_id,
                    root_domain=root_domain,
                    command=cmd,
                    log_path=str(log_path),
                )
                try:
                    exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
                    if exit_code != 0:
                        err_text = summarize_subprocess_failure("nightmare", log_path, exit_code)
                    self.logger.info(
                        "nightmare_subprocess_complete",
                        worker_id=worker_id,
                        entry_id=entry_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=err_text,
                    )
                except Exception as exc:
                    err_text = str(exc)
                    exit_code = 1
                    self.logger.error(
                        "nightmare_subprocess_error",
                        worker_id=worker_id,
                        entry_id=entry_id,
                        root_domain=root_domain,
                        error=err_text,
                    )
                finally:
                    uploader_stop.set()
                    session_uploader.upload_once()
                    heartbeat.stop()

                try:
                    self._upload_file_artifact(root_domain, "nightmare_session_json", paths["nightmare_session_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_url_inventory_json", paths["nightmare_url_inventory_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_requests_json", paths["nightmare_requests_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_parameters_json", paths["nightmare_parameters_json"], worker_id)
                    self._upload_file_artifact(
                        root_domain, "nightmare_post_requests_json", paths["nightmare_post_requests_json"], worker_id
                    )
                    self._upload_file_artifact(root_domain, "nightmare_parameters_txt", paths["nightmare_parameters_txt"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_source_of_truth_json", paths["nightmare_source_of_truth_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_report_html", paths["nightmare_report_html"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_log", paths["nightmare_log"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_scrapy_log", paths["nightmare_scrapy_log"], worker_id)
                    hv_dir = paths.get("nightmare_high_value_dir")
                    if hv_dir is not None and hv_dir.is_dir():
                        self._upload_zip_artifact(
                            root_domain, "nightmare_high_value_zip", hv_dir, worker_id
                        )
                except Exception as exc:
                    self.logger.error(
                        "nightmare_artifact_upload_failed",
                        worker_id=worker_id,
                        entry_id=entry_id,
                        root_domain=root_domain,
                        error=str(exc),
                    )

                if exit_code == 0 and not self.cfg.workflow_scheduler_enabled:
                    try:
                        self.client.enqueue_stage(root_domain, "auth0r")
                        self.logger.info(
                            "nightmare_stage_enqueued",
                            worker_id=worker_id,
                            entry_id=entry_id,
                            root_domain=root_domain,
                            stage="auth0r",
                        )
                    except Exception as exc:
                        self.logger.error(
                            "nightmare_stage_enqueue_failed",
                            worker_id=worker_id,
                            entry_id=entry_id,
                            root_domain=root_domain,
                            stage="auth0r",
                            error=str(exc),
                        )
                    try:
                        self.client.enqueue_stage(root_domain, "fozzy")
                        self.logger.info(
                            "nightmare_stage_enqueued",
                            worker_id=worker_id,
                            entry_id=entry_id,
                            root_domain=root_domain,
                            stage="fozzy",
                        )
                    except Exception as exc:
                        self.logger.error(
                            "nightmare_stage_enqueue_failed",
                            worker_id=worker_id,
                            entry_id=entry_id,
                            root_domain=root_domain,
                            stage="fozzy",
                            error=str(exc),
                        )
                try:
                    self.client.complete_target(entry_id, worker_id, exit_code, err_text)
                    self.logger.info(
                        "nightmare_job_complete",
                        worker_id=worker_id,
                        entry_id=entry_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=err_text,
                    )
                except Exception as exc:
                    self.logger.error(
                        "nightmare_complete_failed",
                        worker_id=worker_id,
                        entry_id=entry_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=str(exc),
                    )
            finally:
                self._end_job()

    def _fozzy_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-fozzy-{idx}"
        self._set_worker_state(worker_id, "running")
        self.logger.info("fozzy_worker_loop_started", worker_id=worker_id, worker_index=idx)
        while not self.stop_event.is_set():
            state = self._poll_worker_commands(worker_id)
            if state in {"paused", "stopped", "errored"}:
                self.logger.warning(
                    "fozzy_worker_waiting_due_to_state",
                    worker_id=worker_id,
                    worker_state=state,
                    poll_interval_seconds=self.cfg.poll_interval_seconds,
                )
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            self._maybe_apply_fleet_output_clear()
            try:
                entry = self.client.claim_stage(worker_id, "fozzy", self.cfg.lease_seconds)
            except Exception as exc:
                self.logger.error("fozzy_claim_failed", worker_id=worker_id, error=str(exc))
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            if not entry:
                self.logger.info("fozzy_claim_empty", worker_id=worker_id)
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            root_domain = str(entry.get("root_domain", "") or "").strip().lower()
            if not root_domain:
                self.logger.error("fozzy_claim_invalid_entry", worker_id=worker_id, entry=entry)
                self.stop_event.wait(1.0)
                continue
            self.logger.info(
                "fozzy_job_start",
                worker_id=worker_id,
                root_domain=root_domain,
            )
            paths = self._artifact_paths(root_domain)
            params_path = paths["nightmare_parameters_json"]
            if not params_path.is_file():
                self._download_file_artifact(root_domain, "nightmare_parameters_json", params_path)
            if not params_path.is_file():
                self.client.complete_stage(worker_id, root_domain, "fozzy", 1, "missing parameters artifact")
                self.logger.error(
                    "fozzy_parameters_missing",
                    worker_id=worker_id,
                    root_domain=root_domain,
                    parameters_path=str(params_path),
                )
                continue

            hv_dir = paths.get("nightmare_high_value_dir")
            if hv_dir is not None:
                hv_dir.mkdir(parents=True, exist_ok=True)
                self._download_zip_artifact(root_domain, "nightmare_high_value_zip", hv_dir)
            self._download_file_artifact(root_domain, "nightmare_post_requests_json", paths["nightmare_post_requests_json"])

            self._begin_job()
            try:
                heartbeat = LeaseHeartbeat(
                    tick_fn=lambda: self.client.heartbeat_stage(worker_id, root_domain, "fozzy", self.cfg.lease_seconds),
                    interval_seconds=self.cfg.heartbeat_interval_seconds,
                    logger=self.logger,
                    heartbeat_kind="fozzy_stage",
                )
                heartbeat.start()
                fozzy_params = self._workflow_stage_parameters("fozzy")
                max_background_workers = max(
                    1,
                    _safe_int(
                        fozzy_params.get("max_background_workers", self.cfg.fozzy_process_workers),
                        max(1, int(self.cfg.fozzy_process_workers)),
                    ),
                )
                max_workers_per_domain = max(
                    1,
                    _safe_int(fozzy_params.get("max_workers_per_domain", 1), 1),
                )
                max_workers_per_subdomain = max(
                    1,
                    _safe_int(fozzy_params.get("max_workers_per_subdomain", 1), 1),
                )
                cmd = [
                    self.cfg.python_executable,
                    "fozzy.py",
                    str(params_path),
                    "--config",
                    str(self.cfg.fozzy_config),
                    "--max-background-workers",
                    str(max_background_workers),
                    "--max-workers-per-domain",
                    str(max_workers_per_domain),
                    "--max-workers-per-subdomain",
                    str(max_workers_per_subdomain),
                ]
                log_path = _domain_output_dir(root_domain, self.cfg.output_root) / "coordinator.fozzy.log"
                exit_code = 1
                err_text = ""
                self.logger.info(
                    "fozzy_subprocess_start",
                    worker_id=worker_id,
                    root_domain=root_domain,
                    command=cmd,
                    log_path=str(log_path),
                )
                try:
                    exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
                    if exit_code != 0:
                        err_text = summarize_subprocess_failure("fozzy", log_path, exit_code)
                    self.logger.info(
                        "fozzy_subprocess_complete",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=err_text,
                    )
                except Exception as exc:
                    err_text = str(exc)
                    exit_code = 1
                    self.logger.error(
                        "fozzy_subprocess_error",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        error=err_text,
                    )
                finally:
                    heartbeat.stop()

                try:
                    self._upload_file_artifact(root_domain, "fozzy_summary_json", paths["fozzy_summary_json"], worker_id)
                    self._upload_file_artifact(root_domain, "fozzy_inventory_json", paths["fozzy_inventory_json"], worker_id)
                    self._upload_file_artifact(root_domain, "fozzy_log", paths["fozzy_log"], worker_id)
                    self._upload_zip_artifact(root_domain, "fozzy_results_zip", paths["fozzy_results_dir"], worker_id)
                except Exception as exc:
                    self.logger.error(
                        "fozzy_artifact_upload_failed",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        error=str(exc),
                    )

                if exit_code == 0 and not self.cfg.workflow_scheduler_enabled:
                    try:
                        self.client.enqueue_stage(root_domain, "extractor")
                        self.logger.info(
                            "fozzy_stage_enqueued",
                            worker_id=worker_id,
                            root_domain=root_domain,
                            stage="extractor",
                        )
                    except Exception as exc:
                        self.logger.error(
                            "fozzy_stage_enqueue_failed",
                            worker_id=worker_id,
                            root_domain=root_domain,
                            stage="extractor",
                            error=str(exc),
                        )
                try:
                    self.client.complete_stage(worker_id, root_domain, "fozzy", exit_code, err_text)
                    self.logger.info(
                        "fozzy_job_complete",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=err_text,
                    )
                except Exception as exc:
                    self.logger.error(
                        "fozzy_complete_failed",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=str(exc),
                    )
            finally:
                self._end_job()


    def _auth0r_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-auth0r-{idx}"
        self._set_worker_state(worker_id, "running")
        self.logger.info("auth0r_worker_loop_started", worker_id=worker_id, worker_index=idx)
        while not self.stop_event.is_set():
            state = self._poll_worker_commands(worker_id)
            if state in {"paused", "stopped", "errored"}:
                self.logger.warning(
                    "auth0r_worker_waiting_due_to_state",
                    worker_id=worker_id,
                    worker_state=state,
                    poll_interval_seconds=self.cfg.poll_interval_seconds,
                )
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            self._maybe_apply_fleet_output_clear()
            try:
                entry = self.client.claim_stage(worker_id, "auth0r", self.cfg.lease_seconds)
            except Exception as exc:
                self.logger.error("auth0r_claim_failed", worker_id=worker_id, error=str(exc))
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            if not entry:
                self.logger.info("auth0r_claim_empty", worker_id=worker_id)
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            root_domain = str(entry.get("root_domain", "") or "").strip().lower()
            if not root_domain:
                self.logger.error("auth0r_claim_invalid_entry", worker_id=worker_id, entry=entry)
                self.stop_event.wait(1.0)
                continue
            paths = self._artifact_paths(root_domain)
            nightmare_session_path = paths["nightmare_session_json"]
            if not nightmare_session_path.is_file():
                self._download_file_artifact(root_domain, "nightmare_session_json", nightmare_session_path)
            if not nightmare_session_path.is_file():
                self.client.complete_stage(worker_id, root_domain, "auth0r", 1, "missing nightmare session artifact")
                self.logger.error(
                    "auth0r_session_missing",
                    worker_id=worker_id,
                    root_domain=root_domain,
                    session_path=str(nightmare_session_path),
                )
                continue

            self._begin_job()
            try:
                heartbeat = LeaseHeartbeat(
                    tick_fn=lambda: self.client.heartbeat_stage(worker_id, root_domain, "auth0r", self.cfg.lease_seconds),
                    interval_seconds=self.cfg.heartbeat_interval_seconds,
                    logger=self.logger,
                    heartbeat_kind="auth0r_stage",
                )
                heartbeat.start()
                cmd = [
                    self.cfg.python_executable,
                    "auth0r.py",
                    root_domain,
                    "--nightmare-session",
                    str(nightmare_session_path),
                    "--summary-json",
                    str(paths["auth0r_summary_json"]),
                ]
                auth0r_database_url = os.getenv("AUTH0R_DATABASE_URL", "") or os.getenv("DATABASE_URL", "") or os.getenv("COORDINATOR_DATABASE_URL", "")
                if auth0r_database_url:
                    cmd.extend(["--database-url", auth0r_database_url])
                if not self.client.verify_ssl:
                    cmd.append("--insecure-tls")
                auth0r_cfg = _read_json_dict(self.cfg.auth0r_config)
                auth0r_params = self._workflow_stage_parameters("auth0r")
                min_delay = auth0r_params.get("min_delay_seconds", auth0r_cfg.get("min_delay_seconds", 0.25))
                max_seed_actions = auth0r_params.get("max_seed_actions", auth0r_cfg.get("max_seed_actions", 200))
                timeout_seconds = auth0r_params.get("timeout_seconds", auth0r_cfg.get("timeout_seconds", 20.0))
                cmd.extend(["--min-delay-seconds", str(min_delay)])
                cmd.extend(["--max-seed-actions", str(max_seed_actions)])
                cmd.extend(["--timeout-seconds", str(timeout_seconds)])
                log_path = paths["auth0r_log"]
                exit_code = 1
                err_text = ""
                self.logger.info(
                    "auth0r_subprocess_start",
                    worker_id=worker_id,
                    root_domain=root_domain,
                    command=cmd,
                    log_path=str(log_path),
                )
                try:
                    exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
                    if exit_code != 0:
                        err_text = summarize_subprocess_failure("auth0r", log_path, exit_code)
                    self.logger.info(
                        "auth0r_subprocess_complete",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=err_text,
                    )
                except Exception as exc:
                    err_text = str(exc)
                    exit_code = 1
                    self.logger.error(
                        "auth0r_subprocess_error",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        error=err_text,
                    )
                finally:
                    heartbeat.stop()

                try:
                    self._upload_file_artifact(root_domain, "auth0r_summary_json", paths["auth0r_summary_json"], worker_id)
                    self._upload_file_artifact(root_domain, "auth0r_log", paths["auth0r_log"], worker_id)
                except Exception as exc:
                    self.logger.error(
                        "auth0r_artifact_upload_failed",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        error=str(exc),
                    )
                try:
                    self.client.complete_stage(worker_id, root_domain, "auth0r", exit_code, err_text)
                except Exception as exc:
                    self.logger.error(
                        "auth0r_complete_failed",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=str(exc),
                    )
            finally:
                self._end_job()
    def _extractor_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-extractor-{idx}"
        self._set_worker_state(worker_id, "running")
        self.logger.info("extractor_worker_loop_started", worker_id=worker_id, worker_index=idx)
        while not self.stop_event.is_set():
            state = self._poll_worker_commands(worker_id)
            if state in {"paused", "stopped", "errored"}:
                self.logger.warning(
                    "extractor_worker_waiting_due_to_state",
                    worker_id=worker_id,
                    worker_state=state,
                    poll_interval_seconds=self.cfg.poll_interval_seconds,
                )
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            self._maybe_apply_fleet_output_clear()
            try:
                entry = self.client.claim_stage(worker_id, "extractor", self.cfg.lease_seconds)
            except Exception as exc:
                self.logger.error("extractor_claim_failed", worker_id=worker_id, error=str(exc))
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            if not entry:
                self.logger.info("extractor_claim_empty", worker_id=worker_id)
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            root_domain = str(entry.get("root_domain", "") or "").strip().lower()
            if not root_domain:
                self.logger.error("extractor_claim_invalid_entry", worker_id=worker_id, entry=entry)
                self.stop_event.wait(1.0)
                continue
            self.logger.info(
                "extractor_job_start",
                worker_id=worker_id,
                root_domain=root_domain,
            )
            paths = self._artifact_paths(root_domain)
            domain_dir = _domain_output_dir(root_domain, self.cfg.output_root)
            fozzy_results_dir = paths["fozzy_results_dir"]
            if not fozzy_results_dir.is_dir():
                self._download_zip_artifact(root_domain, "fozzy_results_zip", fozzy_results_dir)
            if not fozzy_results_dir.is_dir():
                self.client.complete_stage(worker_id, root_domain, "extractor", 1, "missing fozzy results artifact")
                self.logger.error(
                    "extractor_results_missing",
                    worker_id=worker_id,
                    root_domain=root_domain,
                    results_dir=str(fozzy_results_dir),
                )
                continue

            self._begin_job()
            try:
                heartbeat = LeaseHeartbeat(
                    tick_fn=lambda: self.client.heartbeat_stage(worker_id, root_domain, "extractor", self.cfg.lease_seconds),
                    interval_seconds=self.cfg.heartbeat_interval_seconds,
                    logger=self.logger,
                    heartbeat_kind="extractor_stage",
                )
                heartbeat.start()
                extractor_params = self._workflow_stage_parameters("extractor")
                extractor_workers = max(
                    1,
                    _safe_int(
                        extractor_params.get("workers", self.cfg.extractor_process_workers),
                        max(1, int(self.cfg.extractor_process_workers)),
                    ),
                )
                force_extractor = bool(extractor_params.get("force", True))
                cmd = [
                    self.cfg.python_executable,
                    "extractor.py",
                    root_domain,
                    "--config",
                    str(self.cfg.extractor_config),
                    "--scan-root",
                    str(self.cfg.output_root),
                    "--workers",
                    str(extractor_workers),
                ]
                if force_extractor:
                    cmd.append("--force")
                log_path = domain_dir / "coordinator.extractor.log"
                exit_code = 1
                err_text = ""
                self.logger.info(
                    "extractor_subprocess_start",
                    worker_id=worker_id,
                    root_domain=root_domain,
                    command=cmd,
                    log_path=str(log_path),
                )
                try:
                    exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
                    if exit_code != 0:
                        err_text = summarize_subprocess_failure("extractor", log_path, exit_code)
                    self.logger.info(
                        "extractor_subprocess_complete",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=err_text,
                    )
                except Exception as exc:
                    err_text = str(exc)
                    exit_code = 1
                    self.logger.error(
                        "extractor_subprocess_error",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        error=err_text,
                    )
                finally:
                    heartbeat.stop()

                try:
                    self._upload_file_artifact(root_domain, "extractor_summary_json", paths["extractor_summary_json"], worker_id)
                    self._upload_zip_artifact(root_domain, "extractor_matches_zip", paths["extractor_matches_dir"], worker_id)
                except Exception as exc:
                    self.logger.error(
                        "extractor_artifact_upload_failed",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        error=str(exc),
                    )

                try:
                    self.client.complete_stage(worker_id, root_domain, "extractor", exit_code, err_text)
                    self.logger.info(
                        "extractor_job_complete",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=err_text,
                    )
                except Exception as exc:
                    self.logger.error(
                        "extractor_complete_failed",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        exit_code=exit_code,
                        error=str(exc),
                    )
            finally:
                self._end_job()

    def run(self) -> int:
        threads: list[threading.Thread] = []
        if self.cfg.workflow_scheduler_enabled:
            threads.append(threading.Thread(target=self._workflow_scheduler_loop, daemon=True))
        if self.cfg.enable_nightmare:
            for idx in range(1, max(1, self.cfg.nightmare_workers) + 1):
                t = threading.Thread(target=self._nightmare_worker_loop, args=(idx,), daemon=True)
                threads.append(t)
        if self.cfg.enable_fozzy:
            for idx in range(1, max(1, self.cfg.fozzy_workers) + 1):
                t = threading.Thread(target=self._fozzy_worker_loop, args=(idx,), daemon=True)
                threads.append(t)
        if self.cfg.enable_auth0r:
            for idx in range(1, max(1, self.cfg.auth0r_workers) + 1):
                t = threading.Thread(target=self._auth0r_worker_loop, args=(idx,), daemon=True)
                threads.append(t)
        if self.cfg.enable_extractor:
            for idx in range(1, max(1, self.cfg.extractor_workers) + 1):
                t = threading.Thread(target=self._extractor_worker_loop, args=(idx,), daemon=True)
                threads.append(t)

        for t in threads:
            t.start()
        self.logger.info(
            "coordinator_workers_started",
            worker_prefix=self.worker_prefix,
            workflow_scheduler_enabled=bool(self.cfg.workflow_scheduler_enabled),
            workflow_scheduler_interval_seconds=float(self.cfg.workflow_scheduler_interval_seconds),
            nightmare_workers=(self.cfg.nightmare_workers if self.cfg.enable_nightmare else 0),
            fozzy_workers=(self.cfg.fozzy_workers if self.cfg.enable_fozzy else 0),
            auth0r_workers=(self.cfg.auth0r_workers if self.cfg.enable_auth0r else 0),
            extractor_workers=(self.cfg.extractor_workers if self.cfg.enable_extractor else 0),
        )
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            self.logger.warning("coordinator_interrupt_received")
            self.stop_event.set()
            return 0


def parse_args(argv: Optional[list[str] ] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Distributed coordinator worker for Nightmare/Fozzy/Auth0r/Extractor")
    p.add_argument("--config", default=str(CONFIG_PATH_DEFAULT), help="Path to coordinator config JSON")
    p.add_argument("--server-base-url", default=None, help="Coordinator server base URL (e.g. https://coord.example.com)")
    p.add_argument("--api-token", default=None, help="Coordinator API bearer token")
    p.add_argument("--output-root", default=None, help="Output root (default: ./output)")
    return p.parse_args(argv)



def main(argv: Optional[list[str] ] = None) -> int:
    install_error_reporting(program_name="coordinator", component_name="distributed_worker", source_type="worker")
    configure_logging()
    logger = get_logger("coordinator")
    args = parse_args(argv)
    cfg = load_config(args)
    logger.info(
        "coordinator_starting",
        server_base_url=cfg.server_base_url,
        output_root=str(cfg.output_root),
        workflow_config=str(cfg.workflow_config),
        workflow_scheduler_enabled=bool(cfg.workflow_scheduler_enabled),
        workflow_scheduler_interval_seconds=float(cfg.workflow_scheduler_interval_seconds),
        nightmare_workers=cfg.nightmare_workers,
        fozzy_workers=cfg.fozzy_workers,
        auth0r_workers=cfg.auth0r_workers,
        extractor_workers=cfg.extractor_workers,
    )
    runner = DistributedCoordinator(cfg, logger=logger)
    return runner.run()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        report_error(
            "Unhandled coordinator exception",
            program_name="coordinator",
            component_name="distributed_worker",
            source_type="worker",
            exception=exc,
            raw_line=str(exc),
        )
        raise
