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
import hashlib
import io
import json
import os
import re
import socket
import subprocess
import sys
import threading
import queue
import time
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode, urlparse

from http_client import request_capped, request_json

from output_cleanup import FLEET_GEN_APPLIED_FILENAME, clear_output_root_children
from plugins.base import PluginExecutionContext
from plugins.registry import resolve_plugin
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


def _normalize_subdomain_start_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        return text
    return f"https://{text}"


def _default_workflow_entries() -> list[dict[str, Any]]:
    return [
        {
            "name": "nightmare_crawl_core",
            "display_name": "Nightmare Crawl Core",
            "description": "Marks crawl completion once target queue intake finishes for a domain.",
            "handler": "nightmare_artifact_gate",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "nightmare_crawl_core",
            "enabled": True,
            "prerequisites": {"target_statuses": ["completed"]},
            "retry_failed": False,
            "max_attempts": 1,
            "resume_mode": "exact",
            "parameters": {},
        },
        {
            "name": "nightmare_ai_discovery",
            "display_name": "Nightmare AI Discovery",
            "description": "Marks AI discovery-ready state once nightmare session data exists.",
            "handler": "nightmare_artifact_gate",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "nightmare_ai_discovery",
            "enabled": True,
            "prerequisites": {
                "artifacts_all": ["nightmare_session_json"],
                "requires_plugins_all": ["nightmare_crawl_core"],
            },
            "retry_failed": False,
            "max_attempts": 1,
            "resume_mode": "exact",
            "parameters": {},
        },
        {
            "name": "nightmare_ai_probe_execution",
            "display_name": "Nightmare AI Probe Execution",
            "description": "Marks probe execution readiness once URL inventory exists.",
            "handler": "nightmare_artifact_gate",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "nightmare_ai_probe_execution",
            "enabled": True,
            "prerequisites": {
                "artifacts_all": ["nightmare_url_inventory_json"],
                "requires_plugins_all": ["nightmare_ai_discovery"],
            },
            "retry_failed": False,
            "max_attempts": 1,
            "resume_mode": "exact",
            "parameters": {},
        },
        {
            "name": "nightmare_url_verification",
            "display_name": "Nightmare URL Verification",
            "description": "Marks URL verification readiness once request inventory exists.",
            "handler": "nightmare_artifact_gate",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "nightmare_url_verification",
            "enabled": True,
            "prerequisites": {
                "artifacts_all": ["nightmare_requests_json"],
                "requires_plugins_all": ["nightmare_ai_probe_execution"],
            },
            "retry_failed": False,
            "max_attempts": 1,
            "resume_mode": "exact",
            "parameters": {},
        },
        {
            "name": "nightmare_inventory_export",
            "display_name": "Nightmare Inventory Export",
            "description": "Marks export readiness once nightmare parameter inventory exists.",
            "handler": "nightmare_artifact_gate",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "nightmare_inventory_export",
            "enabled": True,
            "prerequisites": {
                "artifacts_all": ["nightmare_parameters_json"],
                "requires_plugins_all": ["nightmare_url_verification"],
            },
            "retry_failed": False,
            "max_attempts": 1,
            "resume_mode": "exact",
            "parameters": {},
        },
        {
            "name": "nightmare_report_generation",
            "display_name": "Nightmare Report Generation",
            "description": "Marks report generation readiness once report artifact exists.",
            "handler": "nightmare_artifact_gate",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "nightmare_report_generation",
            "enabled": True,
            "prerequisites": {
                "artifacts_all": ["nightmare_report_html"],
                "requires_plugins_all": ["nightmare_inventory_export"],
            },
            "retry_failed": False,
            "max_attempts": 1,
            "resume_mode": "exact",
            "parameters": {},
        },
        {
            "name": "auth0r",
            "display_name": "Auth0r",
            "description": "Run auth0r once nightmare session data is available.",
            "handler": "legacy_step_adapter",
            "config_schema": {"type": "object", "additionalProperties": True},
            "stage": "auth0r",
            "enabled": True,
            "prerequisites": {
                "artifacts_all": ["nightmare_session_json"],
                "requires_plugins_all": ["nightmare_crawl_core"],
            },
            "retry_failed": False,
            "max_attempts": 1,
            "resume_mode": "exact",
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
            "prerequisites": {
                "artifacts_all": ["nightmare_parameters_json"],
                "requires_plugins_all": ["nightmare_inventory_export"],
            },
            "retry_failed": False,
            "max_attempts": 1,
            "resume_mode": "exact",
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
            "prerequisites": {
                "artifacts_all": ["fozzy_results_zip"],
                "requires_plugins_all": ["fozzy"],
            },
            "retry_failed": False,
            "max_attempts": 1,
            "resume_mode": "exact",
            "parameters": {"force": True},
        },
    ]


def _normalize_workflow_entry(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    name = str(raw.get("name") or raw.get("type") or raw.get("plugin_name") or raw.get("stage") or "").strip().lower()
    plugin_name = str(raw.get("plugin_name") or raw.get("stage") or raw.get("name") or name).strip().lower()
    if not plugin_name:
        return None
    prereq = raw.get("prerequisites", raw.get("preconditions", {}))
    if not isinstance(prereq, dict):
        prereq = {}
    inputs = raw.get("inputs") if isinstance(raw.get("inputs"), dict) else {}
    artifacts_all = (
        list(prereq.get("artifacts_all", []) if isinstance(prereq.get("artifacts_all"), list) else [])
        + list(raw.get("requires_artifacts_all", []) if isinstance(raw.get("requires_artifacts_all"), list) else [])
        + list(inputs.get("artifacts_all", []) if isinstance(inputs.get("artifacts_all"), list) else [])
    )
    artifacts_any = (
        list(prereq.get("artifacts_any", []) if isinstance(prereq.get("artifacts_any"), list) else [])
        + list(raw.get("requires_artifacts_any", []) if isinstance(raw.get("requires_artifacts_any"), list) else [])
        + list(inputs.get("artifacts_any", []) if isinstance(inputs.get("artifacts_any"), list) else [])
    )
    requires_plugins_all = prereq.get("requires_plugins_all", prereq.get("plugins_all", []))
    requires_plugins_any = prereq.get("requires_plugins_any", prereq.get("plugins_any", []))
    target_statuses = prereq.get("target_statuses", raw.get("target_statuses", []))
    require_target_completed = bool(
        prereq.get("require_target_completed", raw.get("require_target_completed", False))
    )
    params = raw.get("parameters", raw.get("config", {}))
    return {
        "name": name or plugin_name,
        "display_name": str(raw.get("display_name") or raw.get("name") or plugin_name).strip(),
        "description": str(raw.get("description") or "").strip(),
        "handler": str(raw.get("handler") or "legacy_step_adapter").strip().lower(),
        "config_schema": (
            dict(raw.get("config_schema"))
            if isinstance(raw.get("config_schema"), dict)
            else {"type": "object", "additionalProperties": True}
        ),
        "stage": plugin_name,
        "plugin_name": plugin_name,
        "enabled": bool(raw.get("enabled", True)),
        "prerequisites": {
            "artifacts_all": [str(item).strip().lower() for item in (artifacts_all if isinstance(artifacts_all, list) else []) if str(item).strip()],
            "artifacts_any": [str(item).strip().lower() for item in (artifacts_any if isinstance(artifacts_any, list) else []) if str(item).strip()],
            "requires_plugins_all": [str(item).strip().lower() for item in (requires_plugins_all if isinstance(requires_plugins_all, list) else []) if str(item).strip()],
            "requires_plugins_any": [str(item).strip().lower() for item in (requires_plugins_any if isinstance(requires_plugins_any, list) else []) if str(item).strip()],
            "target_statuses": [str(item).strip().lower() for item in (target_statuses if isinstance(target_statuses, list) else []) if str(item).strip()],
            "require_target_completed": bool(require_target_completed),
        },
        "retry_failed": bool(raw.get("retry_failed", False)),
        "max_attempts": max(0, _safe_int(raw.get("max_attempts", 0), 0)),
        "inputs": dict(raw.get("inputs") or {}) if isinstance(raw.get("inputs"), dict) else {},
        "outputs": dict(raw.get("outputs") or {}) if isinstance(raw.get("outputs"), dict) else {},
        "resume_mode": str(raw.get("resume_mode") or "exact").strip().lower() or "exact",
        "parameters": dict(params) if isinstance(params, dict) else {},
    }



def _discover_workflow_paths(path: Path) -> list[Path]:
    if path.is_dir():
        return sorted(candidate.resolve() for candidate in path.glob(WORKFLOW_FILE_GLOB) if candidate.is_file())
    if path.is_file():
        return [path.resolve()]
    raw = str(path or "")
    if any(token in raw for token in ("*", "?", "[")):
        parent = path.parent if str(path.parent) not in {"", "."} else BASE_DIR
        try:
            return sorted(candidate.resolve() for candidate in parent.glob(path.name) if candidate.is_file())
        except Exception:
            return []
    return []


def _load_workflow_catalog(path: Path, logger: Any) -> tuple[str, dict[str, list[dict[str, Any]]]]:
    workflow_paths = _discover_workflow_paths(path)
    if not workflow_paths:
        logger.warning(
            "workflow_config_missing_or_empty_using_defaults",
            workflow_config=str(path),
        )
        return "default", {"default": _default_workflow_entries()}

    catalog: dict[str, list[dict[str, Any]]] = {}
    primary_workflow_id = "default"
    for idx, workflow_path in enumerate(workflow_paths):
        payload = _read_json_dict(workflow_path)
        workflow_id = str(payload.get("workflow_id") or payload.get("id") or workflow_path.stem).strip().lower() or workflow_path.stem.lower()
        candidates: list[Any] = []
        if isinstance(payload.get("plugins"), list):
            candidates = list(payload.get("plugins") or [])
        elif isinstance(payload.get("stages"), list):
            candidates = list(payload.get("stages") or [])
        elif isinstance(payload.get("steps"), list):
            candidates = list(payload.get("steps") or [])
        if not candidates:
            logger.warning(
                "workflow_config_missing_or_empty_using_defaults",
                workflow_config=str(workflow_path),
            )
            catalog.setdefault(workflow_id, _default_workflow_entries())
            continue
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in candidates:
            normalized = _normalize_workflow_entry(raw)
            if not normalized:
                continue
            plugin_name = str(normalized.get("plugin_name") or normalized.get("stage") or "").strip().lower()
            if not plugin_name or plugin_name in seen:
                continue
            seen.add(plugin_name)
            out.append(normalized)
        if not out:
            logger.warning(
                "workflow_config_entries_invalid_using_defaults",
                workflow_config=str(workflow_path),
            )
            out = _default_workflow_entries()
        catalog[workflow_id] = out
        if idx == 0:
            primary_workflow_id = workflow_id
    return primary_workflow_id, catalog


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
        self._workflow_lock = threading.Lock()
        self._workflow_reschedule_queue: "queue.Queue[str]" = queue.Queue()
        self._workflow_reschedule_pending: set[str] = set()
        self._workflow_reschedule_lock = threading.Lock()
        self._workflow_id = "default"
        self._workflow_catalog: dict[str, list[dict[str, Any]]] = {}
        self._workflow_entries: list[dict[str, Any]] = []
        self._workflow_stage_map: dict[str, dict[str, dict[str, Any]]] = {}
        self._reload_workflow_config(reason="startup")

    def _reload_workflow_config(self, *, worker_id: str = "", reason: str = "") -> bool:
        try:
            workflow_id, workflow_catalog = _load_workflow_catalog(self.cfg.workflow_config, self.logger)
            workflow_stage_map: dict[str, dict[str, dict[str, Any]]] = {}
            flattened_entries: list[dict[str, Any]] = []
            workflow_ids: list[str] = []
            for catalog_workflow_id, workflow_entries in workflow_catalog.items():
                workflow_ids.append(catalog_workflow_id)
                stage_map = {
                    str(item.get("plugin_name") or item.get("stage") or "").strip().lower(): item
                    for item in workflow_entries
                }
                workflow_stage_map[catalog_workflow_id] = stage_map
                flattened_entries.extend(workflow_entries)
            with self._workflow_lock:
                self._workflow_id = str(workflow_id or "default").strip().lower() or "default"
                self._workflow_catalog = {str(k): list(v) for k, v in workflow_catalog.items()}
                self._workflow_entries = list(flattened_entries)
                self._workflow_stage_map = workflow_stage_map
            self.logger.info(
                "workflow_scheduler_config_loaded",
                workflow_config=str(self.cfg.workflow_config),
                workflow_id=self._workflow_id,
                workflow_ids=workflow_ids,
                workflow_scheduler_enabled=bool(self.cfg.workflow_scheduler_enabled),
                workflow_scheduler_interval_seconds=float(self.cfg.workflow_scheduler_interval_seconds),
                workflow_stage_count=len(self._workflow_entries),
                workflow_stages=[str(item.get("plugin_name") or item.get("stage") or "") for item in self._workflow_entries],
                reload_reason=str(reason or ""),
                requested_by_worker=worker_id,
            )
            return True
        except Exception as exc:
            self.logger.error(
                "workflow_scheduler_config_reload_failed",
                workflow_config=str(self.cfg.workflow_config),
                worker_id=worker_id,
                reload_reason=str(reason or ""),
                error=str(exc),
            )
            return False

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
            return str(self._worker_states.get(str(worker_id), "idle") or "idle")

    def _record_worker_error(
        self,
        *,
        worker_id: str,
        description: str,
        exception: BaseException | None = None,
        metadata: Optional[dict[str, Any]] = None,
        mark_errored: bool = True,
    ) -> None:
        safe_worker_id = str(worker_id or "").strip() or "unknown-worker"
        safe_description = str(description or "Worker error").strip() or "Worker error"
        if mark_errored:
            self._set_worker_state(safe_worker_id, "errored")
        try:
            report_error(
                safe_description,
                program_name="coordinator",
                component_name="distributed_worker",
                source_type="worker",
                exception=exception,
                raw_line=str(exception or safe_description),
                metadata={
                    "worker_id": safe_worker_id,
                    **(dict(metadata) if isinstance(metadata, dict) else {}),
                },
                severity="error",
            )
        except Exception:
            pass

    def _enqueue_domain_workflow_schedule(self, root_domain: str) -> None:
        safe_domain = str(root_domain or "").strip().lower()
        if not safe_domain:
            return
        with self._workflow_reschedule_lock:
            if safe_domain in self._workflow_reschedule_pending:
                return
            self._workflow_reschedule_pending.add(safe_domain)
            self._workflow_reschedule_queue.put(safe_domain)

    def _refresh_domain_workflow_schedule(self, *, root_domain: str, worker_id: str) -> int:
        safe_domain = str(root_domain or "").strip().lower()
        if not safe_domain:
            return 0
        try:
            snapshot = self.client.get_workflow_domain(safe_domain)
        except Exception as exc:
            self.logger.error(
                "workflow_domain_snapshot_refresh_failed",
                worker_id=worker_id,
                root_domain=safe_domain,
                error=str(exc),
            )
            self._record_worker_error(
                worker_id=worker_id,
                description=f"Failed to refresh workflow snapshot for {safe_domain}",
                exception=exc,
                metadata={"root_domain": safe_domain, "source": "_refresh_domain_workflow_schedule"},
                mark_errored=False,
            )
            return 0
        if not bool(snapshot.get("found")):
            return 0
        try:
            return self._schedule_domain_workflows(snapshot, worker_id=worker_id)
        except Exception as exc:
            self.logger.error(
                "workflow_domain_reschedule_failed",
                worker_id=worker_id,
                root_domain=safe_domain,
                error=str(exc),
            )
            self._record_worker_error(
                worker_id=worker_id,
                description=f"Failed to schedule downstream workflow tasks for {safe_domain}",
                exception=exc,
                metadata={"root_domain": safe_domain, "source": "_refresh_domain_workflow_schedule"},
                mark_errored=False,
            )
            return 0

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
                # "start" resumes availability. The worker is marked running only
                # while it has a leased task.
                state = "idle"
            elif command == "reload":
                ok = self._reload_workflow_config(worker_id=worker_id, reason="worker_command")
                if not ok:
                    err = "workflow config reload failed"
                    state = "errored"
                    self._record_worker_error(
                        worker_id=worker_id,
                        description="Worker command reload failed",
                        metadata={"command": command, "command_id": command_id, "error": err},
                        mark_errored=True,
                    )
            else:
                ok = False
                err = f"unsupported command: {command!r}"
                state = "errored"
                self._record_worker_error(
                    worker_id=worker_id,
                    description="Worker received unsupported command",
                    metadata={"command": command, "command_id": command_id, "error": err},
                    mark_errored=True,
                )
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
        _ = str(stage or "").strip().lower()
        # Unified workflow runtime: all plugin execution is handled by generic plugin workers.
        # Per-tool enable flags are legacy/deprecated and intentionally ignored.
        return True

    def _workflow_stage_parameters(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Backward-compatible stage parameter lookup.

        Supported call styles:
          - self._workflow_stage_parameters(stage_name)
          - self._workflow_stage_parameters(workflow_id, stage_name)
          - self._workflow_stage_parameters(workflow_id=<wid>, stage=<stage>)
          - self._workflow_stage_parameters(stage=<stage>)  # workflow inferred
        """
        workflow_id = kwargs.get("workflow_id")
        stage = kwargs.get("stage")

        if len(args) == 1:
            if stage is None and workflow_id is None:
                stage = args[0]
            elif workflow_id is None:
                workflow_id = args[0]
        elif len(args) >= 2:
            workflow_id, stage = args[0], args[1]

        wid = str(workflow_id or "").strip().lower()
        stage_name = str(stage or "").strip().lower()

        # Backward compatibility: if only one positional argument was provided,
        # treat it as the stage/plugin name and search across all workflows.
        if not stage_name and wid:
            stage_name, wid = wid, ""

        entry: dict[str, Any] = {}
        stage_map = self._workflow_stage_map.get(wid) if wid and isinstance(self._workflow_stage_map.get(wid), dict) else {}
        if isinstance(stage_map, dict):
            maybe_entry = stage_map.get(stage_name)
            if isinstance(maybe_entry, dict):
                entry = maybe_entry
        if not entry and stage_name:
            for fallback_map in self._workflow_stage_map.values():
                if isinstance(fallback_map, dict) and isinstance(fallback_map.get(stage_name), dict):
                    entry = fallback_map.get(stage_name) or {}
                    break
        params = entry.get("parameters") if isinstance(entry, dict) else {}
        return dict(params) if isinstance(params, dict) else {}

    @staticmethod
    def _has_stage_prerequisites(
        artifacts: set[str],
        entry: dict[str, Any],
        *,
        workflow_tasks: dict[str, dict[str, Any]],
        target_counts: dict[str, Any],
    ) -> bool:
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
        required_plugins_all = {
            str(item or "").strip().lower()
            for item in (prereq.get("requires_plugins_all") or [])
            if str(item or "").strip()
        }
        required_plugins_any = {
            str(item or "").strip().lower()
            for item in (prereq.get("requires_plugins_any") or [])
            if str(item or "").strip()
        }
        required_target_statuses = {
            str(item or "").strip().lower()
            for item in (prereq.get("target_statuses") or [])
            if str(item or "").strip()
        }
        require_target_completed = bool(prereq.get("require_target_completed", False))

        def _task_status(name: str) -> str:
            row = workflow_tasks.get(name) if isinstance(workflow_tasks.get(name), dict) else {}
            return str(row.get("status") or "").strip().lower()

        pending_targets = _safe_int(target_counts.get("pending"), 0)
        running_targets = _safe_int(target_counts.get("running"), 0)
        completed_targets = _safe_int(target_counts.get("completed"), 0)
        failed_targets = _safe_int(target_counts.get("failed"), 0)
        if running_targets > 0:
            current_target_status = "running"
        elif pending_targets > 0:
            current_target_status = "pending"
        elif failed_targets > 0 and completed_targets <= 0:
            current_target_status = "failed"
        elif completed_targets > 0:
            current_target_status = "completed"
        else:
            current_target_status = "unknown"

        if required_all and not required_all.issubset(artifacts):
            return False
        if required_any and artifacts.isdisjoint(required_any):
            return False
        if required_plugins_all and any(_task_status(item) != "completed" for item in required_plugins_all):
            return False
        if required_plugins_any and not any(_task_status(item) == "completed" for item in required_plugins_any):
            return False
        if required_target_statuses and current_target_status not in required_target_statuses:
            return False
        if require_target_completed and completed_targets <= 0:
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
        plugin_tasks_all = domain_row.get("plugin_tasks") if isinstance(domain_row.get("plugin_tasks"), dict) else {}
        target_counts = domain_row.get("targets") if isinstance(domain_row.get("targets"), dict) else {}
        scheduled_count = 0
        workflow_catalog = dict(self._workflow_catalog)
        for workflow_id, workflow_entries in workflow_catalog.items():
            workflow_tasks = plugin_tasks_all.get(workflow_id) if isinstance(plugin_tasks_all.get(workflow_id), dict) else {}
            for entry in workflow_entries:
                plugin_name = str(entry.get("plugin_name") or entry.get("stage") or "").strip().lower()
                if not plugin_name or not bool(entry.get("enabled", True)):
                    continue
                if not self._is_stage_runtime_enabled(plugin_name):
                    continue
                if not self._has_stage_prerequisites(
                    artifacts,
                    entry,
                    workflow_tasks=workflow_tasks,
                    target_counts=target_counts,
                ):
                    continue
                task_row = workflow_tasks.get(plugin_name) if isinstance(workflow_tasks.get(plugin_name), dict) else {}
                status = str(task_row.get("status", "") or "").strip().lower()
                attempt_count = _safe_int(task_row.get("attempt_count", 0), 0)
                retry_failed = bool(entry.get("retry_failed", False))
                max_attempts = max(0, _safe_int(entry.get("max_attempts", 0), 0))
                if status in {"pending", "running", "completed"}:
                    continue
                allow_retry_failed = False
                if status == "failed":
                    if not retry_failed:
                        continue
                    if max_attempts > 0 and attempt_count >= max_attempts:
                        continue
                    allow_retry_failed = True
                resume_mode = str(entry.get("resume_mode") or "exact").strip().lower() or "exact"
                details = self.client.enqueue_stage_detailed(
                    root_domain,
                    plugin_name,
                    workflow_id=workflow_id,
                    worker_id=worker_id,
                    reason=f"workflow:{entry.get('name', plugin_name)}",
                    allow_retry_failed=allow_retry_failed,
                    max_attempts=max_attempts,
                    checkpoint={"schema_version": 1, "resume_mode": resume_mode, "state": "queued"},
                    progress={"status": "queued", "plugin_name": plugin_name},
                    progress_artifact_type=f"workflow_progress_{plugin_name}",
                    resume_mode=resume_mode,
                )
                if bool(details.get("scheduled")):
                    scheduled_count += 1
                    self.logger.info(
                        "workflow_task_scheduled",
                        worker_id=worker_id,
                        workflow_id=workflow_id,
                        root_domain=root_domain,
                        stage=plugin_name,
                        prior_status=status or "none",
                        attempt_count=attempt_count,
                        max_attempts=max_attempts,
                        retry_failed=retry_failed,
                        artifacts_available=sorted(artifacts),
                    )
        return scheduled_count

    def _rescan_workflow_domains(self, *, worker_id: str, reason: str) -> int:
        try:
            snapshot = self.client.get_workflow_snapshot(limit=5000)
        except Exception as exc:
            self.logger.error(
                "workflow_scheduler_snapshot_scan_failed",
                worker_id=worker_id,
                reason=reason,
                error=str(exc),
            )
            self._record_worker_error(
                worker_id=worker_id,
                description="Workflow scheduler failed to scan workflow snapshot",
                exception=exc,
                metadata={"reason": reason, "source": "_rescan_workflow_domains"},
                mark_errored=False,
            )
            return 0
        domains = snapshot.get("domains") if isinstance(snapshot.get("domains"), list) else []
        scheduled_total = 0
        scanned_domains = 0
        for domain_row in domains:
            if not isinstance(domain_row, dict):
                continue
            scanned_domains += 1
            try:
                scheduled_total += int(self._schedule_domain_workflows(domain_row, worker_id=worker_id) or 0)
            except Exception as exc:
                self.logger.error(
                    "workflow_scheduler_domain_scan_failed",
                    worker_id=worker_id,
                    reason=reason,
                    root_domain=str(domain_row.get("root_domain") or "").strip().lower(),
                    error=str(exc),
                )
        self.logger.info(
            "workflow_scheduler_snapshot_scan_complete",
            worker_id=worker_id,
            reason=reason,
            scanned_domains=scanned_domains,
            tasks_scheduled=scheduled_total,
        )
        return scheduled_total

    def _workflow_scheduler_loop(self) -> None:
        worker_id = f"{self.worker_prefix}-scheduler-1"
        self._set_worker_state(worker_id, "idle")
        idle_rescan_interval_seconds = max(30.0, float(self.cfg.workflow_scheduler_interval_seconds))
        last_idle_rescan_at = 0.0
        self.logger.info(
            "workflow_scheduler_loop_started",
            worker_id=worker_id,
            workflow_id=self._workflow_id,
            workflow_ids=sorted(self._workflow_catalog.keys()),
            workflow_config=str(self.cfg.workflow_config),
            interval_seconds=float(self.cfg.workflow_scheduler_interval_seconds),
            mode="domain_event_queue",
        )
        self._rescan_workflow_domains(worker_id=worker_id, reason="startup")
        last_idle_rescan_at = time.time()
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
                root_domain = self._workflow_reschedule_queue.get(timeout=float(self.cfg.workflow_scheduler_interval_seconds))
            except queue.Empty:
                now_epoch = time.time()
                if (now_epoch - last_idle_rescan_at) >= idle_rescan_interval_seconds:
                    self._rescan_workflow_domains(worker_id=worker_id, reason="periodic_idle")
                    last_idle_rescan_at = now_epoch
                continue
            with self._workflow_reschedule_lock:
                self._workflow_reschedule_pending.discard(root_domain)
            try:
                scheduled = self._refresh_domain_workflow_schedule(root_domain=root_domain, worker_id=worker_id)
            except Exception as exc:
                self.logger.error(
                    "workflow_scheduler_domain_failed",
                    worker_id=worker_id,
                    root_domain=root_domain,
                    error=str(exc),
                )
                self._record_worker_error(
                    worker_id=worker_id,
                    description=f"Workflow scheduler failed while processing {root_domain or 'unknown-domain'}",
                    exception=exc,
                    metadata={"root_domain": root_domain, "source": "workflow_scheduler_loop"},
                    mark_errored=False,
                )
                scheduled = 0
            self.logger.info(
                "workflow_scheduler_domain_complete",
                worker_id=worker_id,
                workflow_id=self._workflow_id,
                root_domain=root_domain,
                tasks_scheduled=scheduled,
            )


    def _artifact_paths(self, root_domain: str) -> dict[str, Path]:
        domain_dir = _domain_output_dir(root_domain, self.cfg.output_root)
        fozzy_domain_dir = domain_dir / "fozzy-output" / root_domain
        high_value_dir = self.cfg.output_root / "high_value" / root_domain
        recon_dir = domain_dir / "recon"
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
            "recon_dir": recon_dir,
            "recon_subdomains_json": recon_dir / "subdomains.json",
            "recon_subdomain_enumeration_log": recon_dir / "subdomain_enumeration.log",
            "recon_subdomain_enumeration_progress_json": recon_dir / "subdomain_enumeration.progress.json",
            "recon_subdomain_enumeration_complete_flag": recon_dir / "subdomain_enumeration.complete.json",
            "recon_spider_source_tags_log": recon_dir / "spider_source_tags.log",
            "recon_spider_source_tags_progress_json": recon_dir / "spider_source_tags.progress.json",
            "recon_spider_source_tags_complete_flag": recon_dir / "spider_source_tags.complete.json",
            "recon_spider_script_links_log": recon_dir / "spider_script_links.log",
            "recon_spider_script_links_progress_json": recon_dir / "spider_script_links.progress.json",
            "recon_spider_script_links_complete_flag": recon_dir / "spider_script_links.complete.json",
            "recon_spider_wordlist_log": recon_dir / "spider_wordlist.log",
            "recon_spider_wordlist_progress_json": recon_dir / "spider_wordlist.progress.json",
            "recon_spider_wordlist_complete_flag": recon_dir / "spider_wordlist.complete.json",
            "recon_spider_ai_log": recon_dir / "spider_ai.log",
            "recon_spider_ai_progress_json": recon_dir / "spider_ai.progress.json",
            "recon_spider_ai_complete_flag": recon_dir / "spider_ai.complete.json",
            "recon_extractor_high_value_log": recon_dir / "extractor_high_value.log",
            "recon_extractor_high_value_summary_json": recon_dir / "extractor_high_value.summary.json",
            "recon_extractor_high_value_matches_dir": recon_dir / "extractor_high_value.matches",
            "recon_extractor_high_value_progress_json": recon_dir / "extractor_high_value.progress.json",
            "recon_extractor_high_value_complete_flag": recon_dir / "extractor_high_value.complete.json",
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
        self.logger.info(
            "artifact_file_upload_start",
            root_domain=root_domain,
            worker_id=worker_id,
            artifact_type=artifact_type,
            artifact_path=str(path),
            bytes=path.stat().st_size,
        )
        manifest = {
            "local_path": str(path),
            "size_bytes": int(path.stat().st_size),
            "mtime_ns": int(path.stat().st_mtime_ns),
        }
        self.client.upload_artifact_from_file(
            root_domain,
            artifact_type,
            path,
            source_worker=worker_id,
            content_encoding="identity",
            retention_class="derived_rebuildable",
            media_type="application/octet-stream",
        )
        self.client.upload_artifact_manifest(
            root_domain,
            f"{artifact_type}_manifest",
            manifest,
            source_worker=worker_id,
            media_type="application/json",
            retention_class="summary_index",
        )
        self.logger.info(
            "artifact_file_upload_complete",
            root_domain=root_domain,
            worker_id=worker_id,
            artifact_type=artifact_type,
            artifact_path=str(path),
            bytes=path.stat().st_size,
        )

    def _upload_directory_manifest_artifact(self, root_domain: str, artifact_type: str, path: Path, worker_id: str) -> None:
        if not path.is_dir():
            self.logger.info(
                "artifact_manifest_source_not_found",
                root_domain=root_domain,
                worker_id=worker_id,
                artifact_type=artifact_type,
                source_path=str(path),
            )
            return
        entries: list[dict[str, Any]] = []
        for file_path in sorted(path.rglob("*")):
            if not file_path.is_file():
                continue
            rel = file_path.relative_to(path).as_posix()
            digest_hash = hashlib.sha256()
            with file_path.open("rb") as digest_handle:
                for chunk in iter(lambda: digest_handle.read(1024 * 1024), b""):
                    digest_hash.update(chunk)
            digest = digest_hash.hexdigest()
            shard_artifact_type = f"{artifact_type}__shard__{digest[:24]}"
            self.client.upload_artifact_from_file(
                root_domain,
                shard_artifact_type,
                file_path,
                source_worker=worker_id,
                content_encoding="binary",
                retention_class="derived_rebuildable",
                media_type="application/octet-stream",
            )
            entries.append(
                {
                    "path": rel,
                    "artifact_type": shard_artifact_type,
                    "content_sha256": digest,
                    "size_bytes": int(file_path.stat().st_size),
                    "mtime_ns": int(file_path.stat().st_mtime_ns),
                    "shard_key": rel.split("/", 1)[0] if "/" in rel else rel,
                    "logical_role": artifact_type,
                }
            )
        self.client.upload_artifact_manifest(
            root_domain,
            artifact_type,
            {
                "format": "content_addressed_directory_manifest",
                "root": str(path),
                "file_count": len(entries),
                "entries": entries,
            },
            source_worker=worker_id,
            media_type="application/json",
            retention_class="summary_index",
        )
        self.logger.info(
            "artifact_manifest_upload_complete",
            root_domain=root_domain,
            worker_id=worker_id,
            artifact_type=artifact_type,
            source_path=str(path),
            file_count=len(entries),
        )

    def _download_file_artifact(self, root_domain: str, artifact_type: str, path: Path) -> bool:
        self.logger.info(
            "artifact_file_download_start",
            root_domain=root_domain,
            artifact_type=artifact_type,
            target_path=str(path),
        )
        ok = self.client.download_artifact_to_file(root_domain, artifact_type, path)
        if not ok:
            self.logger.warning(
                "artifact_file_download_missing",
                root_domain=root_domain,
                artifact_type=artifact_type,
                target_path=str(path),
            )
            return False
        self.logger.info(
            "artifact_file_download_complete",
            root_domain=root_domain,
            artifact_type=artifact_type,
            target_path=str(path),
            bytes=path.stat().st_size if path.exists() else 0,
        )
        return True

    def _materialize_manifest_artifact(self, root_domain: str, artifact_type: str, target_dir: Path) -> bool:
        self.logger.info(
            "artifact_manifest_materialize_start",
            root_domain=root_domain,
            artifact_type=artifact_type,
            target_dir=str(target_dir),
        )
        artifact = self.client.get_artifact_metadata(root_domain, artifact_type)
        manifest = artifact.get("manifest") if isinstance(artifact, dict) else None
        entries = manifest.get("entries") if isinstance(manifest, dict) else None
        if not isinstance(entries, list):
            entries = manifest.get("files") if isinstance(manifest, dict) and isinstance(manifest.get("files"), list) else None
        if not entries:
            self.logger.warning(
                "artifact_manifest_missing",
                root_domain=root_domain,
                artifact_type=artifact_type,
                target_dir=str(target_dir),
            )
            return False
        target_dir.mkdir(parents=True, exist_ok=True)
        materialized = 0
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            rel = str(entry.get("path") or entry.get("entry_path") or "").strip().lstrip("/")
            shard_type = str(entry.get("artifact_type") or "").strip().lower()
            if not rel or not shard_type:
                continue
            if self.client.download_artifact_to_file(root_domain, shard_type, target_dir / rel):
                materialized += 1
        self.logger.info(
            "artifact_manifest_materialize_complete",
            root_domain=root_domain,
            artifact_type=artifact_type,
            target_dir=str(target_dir),
            files=materialized,
        )
        return materialized > 0

    @staticmethod
    def _recon_progress_artifact_type(plugin_name: str) -> str:
        return f"{str(plugin_name or '').strip().lower()}_progress_json"

    @staticmethod
    def _recon_complete_artifact_type(plugin_name: str) -> str:
        return f"{str(plugin_name or '').strip().lower()}_complete_flag"

    def _recon_progress_path_for_plugin(self, root_domain: str, plugin_name: str) -> Path:
        key = f"{str(plugin_name or '').strip().lower()}_progress_json"
        return self._artifact_paths(root_domain).get(key) or (_domain_output_dir(root_domain, self.cfg.output_root) / "recon" / f"{plugin_name}.progress.json")

    def _recon_complete_flag_path_for_plugin(self, root_domain: str, plugin_name: str) -> Path:
        key = f"{str(plugin_name or '').strip().lower()}_complete_flag"
        return self._artifact_paths(root_domain).get(key) or (_domain_output_dir(root_domain, self.cfg.output_root) / "recon" / f"{plugin_name}.complete.json")

    def _load_json_file_or_artifact(self, *, root_domain: str, artifact_type: str, path: Path) -> dict[str, Any]:
        if not path.is_file():
            self._download_file_artifact(root_domain, artifact_type, path)
        return _read_json_dict(path)

    def _persist_recon_progress(
        self,
        *,
        worker_id: str,
        root_domain: str,
        plugin_name: str,
        payload: dict[str, Any],
    ) -> None:
        path = self._recon_progress_path_for_plugin(root_domain, plugin_name)
        payload["updated_at_utc"] = _now_iso()
        _atomic_write_json(path, payload)
        self._upload_file_artifact(
            root_domain,
            self._recon_progress_artifact_type(plugin_name),
            path,
            worker_id,
        )

    def _write_recon_completion_flag(
        self,
        *,
        worker_id: str,
        root_domain: str,
        plugin_name: str,
        details: dict[str, Any],
    ) -> None:
        path = self._recon_complete_flag_path_for_plugin(root_domain, plugin_name)
        payload = {
            "plugin_name": plugin_name,
            "root_domain": root_domain,
            "completed_at_utc": _now_iso(),
            "details": details,
        }
        _atomic_write_json(path, payload)
        self._upload_file_artifact(
            root_domain,
            self._recon_complete_artifact_type(plugin_name),
            path,
            worker_id,
        )

    def _upload_nightmare_artifacts(self, *, worker_id: str, root_domain: str) -> None:
        paths = self._artifact_paths(root_domain)
        self._upload_file_artifact(root_domain, "nightmare_session_json", paths["nightmare_session_json"], worker_id)
        self._upload_file_artifact(root_domain, "nightmare_url_inventory_json", paths["nightmare_url_inventory_json"], worker_id)
        self._upload_file_artifact(root_domain, "nightmare_requests_json", paths["nightmare_requests_json"], worker_id)
        self._upload_file_artifact(root_domain, "nightmare_parameters_json", paths["nightmare_parameters_json"], worker_id)
        self._upload_file_artifact(root_domain, "nightmare_post_requests_json", paths["nightmare_post_requests_json"], worker_id)
        self._upload_file_artifact(root_domain, "nightmare_parameters_txt", paths["nightmare_parameters_txt"], worker_id)
        self._upload_file_artifact(root_domain, "nightmare_source_of_truth_json", paths["nightmare_source_of_truth_json"], worker_id)
        self._upload_file_artifact(root_domain, "nightmare_report_html", paths["nightmare_report_html"], worker_id)
        self._upload_file_artifact(root_domain, "nightmare_log", paths["nightmare_log"], worker_id)
        self._upload_file_artifact(root_domain, "nightmare_scrapy_log", paths["nightmare_scrapy_log"], worker_id)
        hv_dir = paths.get("nightmare_high_value_dir")
        if hv_dir is not None and hv_dir.is_dir():
            self._upload_directory_manifest_artifact(root_domain, "nightmare_high_value_zip", hv_dir, worker_id)

    def _nightmare_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-nightmare-{idx}"
        self._set_worker_state(worker_id, "idle")
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
                self._set_worker_state(worker_id, "idle")
                self.logger.info("nightmare_claim_empty", worker_id=worker_id)
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue

            self._set_worker_state(worker_id, "running")
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
                    self._upload_nightmare_artifacts(worker_id=worker_id, root_domain=root_domain)
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
                        self.client.enqueue_stage(root_domain, "auth0r", workflow_id=self._workflow_id)
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
                        self.client.enqueue_stage(root_domain, "fozzy", workflow_id=self._workflow_id)
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
                if self._get_worker_state(worker_id) == "running":
                    self._set_worker_state(worker_id, "idle")

    def _update_plugin_progress(
        self,
        *,
        worker_id: str,
        workflow_id: str,
        root_domain: str,
        plugin_name: str,
        checkpoint: dict[str, Any],
        progress: dict[str, Any],
    ) -> None:
        try:
            self.client.update_stage_progress(
                worker_id=worker_id,
                workflow_id=workflow_id,
                root_domain=root_domain,
                stage=plugin_name,
                checkpoint=checkpoint,
                progress=progress,
                progress_artifact_type=f"workflow_progress_{plugin_name}",
            )
        except Exception as exc:
            self.logger.error(
                "workflow_task_progress_update_failed",
                worker_id=worker_id,
                workflow_id=workflow_id,
                root_domain=root_domain,
                stage=plugin_name,
                error=str(exc),
            )

    def _run_nightmare_artifact_gate_plugin(
        self,
        *,
        root_domain: str,
        plugin_name: str,
    ) -> tuple[int, str]:
        entry: dict[str, Any] = {}
        stage_name = str(plugin_name or "").strip().lower()
        for stage_map in self._workflow_stage_map.values():
            if isinstance(stage_map, dict) and isinstance(stage_map.get(stage_name), dict):
                entry = stage_map.get(stage_name) or {}
                break
        prereq = entry.get("prerequisites") if isinstance(entry, dict) else {}
        if not isinstance(prereq, dict):
            prereq = {}
        required_artifacts = [
            str(item or "").strip().lower()
            for item in (prereq.get("artifacts_all") or [])
            if str(item or "").strip()
        ]
        for artifact_type in required_artifacts:
            artifact = self.client.download_artifact(root_domain, artifact_type)
            if artifact is None:
                return 1, f"missing required artifact {artifact_type}"
        return 0, ""

    def _enumerate_subdomains_passive(self, root_domain: str) -> tuple[list[str], str]:
        query_url = f"https://crt.sh/?q=%.{root_domain}&output=json"
        try:
            response = request_capped(
                "GET",
                query_url,
                timeout_seconds=30.0,
                read_limit=4 * 1024 * 1024,
                follow_redirects=True,
                verify=True,
            )
        except Exception as exc:
            return [], f"passive enumeration request failed: {exc}"
        if int(response.status_code) >= 400:
            return [], f"passive enumeration failed with HTTP {int(response.status_code)}"
        text = bytes(response.body or b"").decode("utf-8", errors="replace").strip()
        if not text:
            return [], ""
        try:
            parsed = json.loads(text)
        except Exception as exc:
            return [], f"passive enumeration JSON parse failed: {exc}"
        found: set[str] = set()
        if isinstance(parsed, list):
            for row in parsed:
                if not isinstance(row, dict):
                    continue
                name_value = str(row.get("name_value", "") or "").strip().lower()
                if not name_value:
                    continue
                for candidate in name_value.splitlines():
                    host = str(candidate or "").strip().lstrip("*.").lower().rstrip(".")
                    if host and (host == root_domain or host.endswith(f".{root_domain}")):
                        found.add(host)
        return sorted(found), ""

    def _run_recon_subdomain_enumeration_plugin_task(
        self,
        *,
        worker_id: str,
        root_domain: str,
        plugin_name: str,
    ) -> tuple[int, str]:
        paths = self._artifact_paths(root_domain)
        recon_dir = paths["recon_dir"]
        recon_dir.mkdir(parents=True, exist_ok=True)
        progress_path = self._recon_progress_path_for_plugin(root_domain, plugin_name)
        progress_state = self._load_json_file_or_artifact(
            root_domain=root_domain,
            artifact_type=self._recon_progress_artifact_type(plugin_name),
            path=progress_path,
        )
        if not progress_state:
            progress_state = {
                "schema_version": 1,
                "plugin_name": plugin_name,
                "root_domain": root_domain,
                "status": "running",
                "phase": "enumeration",
                "started_at_utc": _now_iso(),
                "requests_made": [],
                "subdomains": [],
                "probe_results": {},
                "accessible_subdomains": [],
            }

        params = self._workflow_stage_parameters(plugin_name)
        sublist3r_command_raw = params.get("sublist3r_command", "sublist3r")
        if isinstance(sublist3r_command_raw, list):
            sublist3r_cmd = [str(item).strip() for item in sublist3r_command_raw if str(item).strip()]
        else:
            sublist3r_cmd = [str(sublist3r_command_raw or "sublist3r").strip()]
        sublist3r_extra_args = [
            str(item).strip()
            for item in (params.get("sublist3r_extra_args") if isinstance(params.get("sublist3r_extra_args"), list) else [])
            if str(item).strip()
        ]
        allow_passive_fallback = bool(params.get("allow_passive_fallback", True))
        probe_timeout = max(1.0, _safe_float(params.get("probe_timeout_seconds", 8.0), 8.0))
        probe_verify_tls = bool(params.get("probe_verify_tls", True))
        max_logged_requests = max(100, _safe_int(params.get("max_request_log_entries", 4000), 4000))

        subdomains_file = paths["recon_subdomains_json"]
        raw_output_file = recon_dir / "subdomain_enumeration.raw.txt"
        log_path = paths["recon_subdomain_enumeration_log"]

        existing_subdomains = [
            str(item).strip().lower().lstrip("*.").rstrip(".")
            for item in (progress_state.get("subdomains") if isinstance(progress_state.get("subdomains"), list) else [])
            if str(item).strip()
        ]
        found_subdomains: set[str] = set(existing_subdomains)
        found_subdomains.add(root_domain)

        if not found_subdomains or progress_state.get("phase") in {"enumeration", "running"}:
            enumeration_cmd = [*sublist3r_cmd, "-d", root_domain, "-o", str(raw_output_file), *sublist3r_extra_args]
            progress_state["phase"] = "enumeration"
            progress_state.setdefault("requests_made", []).append(
                {
                    "type": "sublist3r",
                    "command": enumeration_cmd,
                    "started_at_utc": _now_iso(),
                }
            )
            progress_state["requests_made"] = list(progress_state["requests_made"])[-max_logged_requests:]
            self._persist_recon_progress(
                worker_id=worker_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                payload=progress_state,
            )

            sublist3r_exit = 0
            sublist3r_error = ""
            try:
                sublist3r_exit = run_subprocess(enumeration_cmd, cwd=BASE_DIR, log_path=log_path)
                if int(sublist3r_exit) != 0:
                    sublist3r_error = summarize_subprocess_failure("sublist3r", log_path, sublist3r_exit)
            except Exception as exc:
                sublist3r_exit = 1
                sublist3r_error = str(exc)

            if int(sublist3r_exit) == 0 and raw_output_file.is_file():
                for line in raw_output_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                    host = str(line or "").strip().lower().lstrip("*.").rstrip(".")
                    if host and (host == root_domain or host.endswith(f".{root_domain}")):
                        found_subdomains.add(host)
            elif allow_passive_fallback:
                passive_subdomains, passive_error = self._enumerate_subdomains_passive(root_domain)
                progress_state.setdefault("requests_made", []).append(
                    {
                        "type": "passive_fallback",
                        "endpoint": f"https://crt.sh/?q=%.{root_domain}&output=json",
                        "started_at_utc": _now_iso(),
                        "error": passive_error,
                    }
                )
                progress_state["requests_made"] = list(progress_state["requests_made"])[-max_logged_requests:]
                for host in passive_subdomains:
                    if host and (host == root_domain or host.endswith(f".{root_domain}")):
                        found_subdomains.add(host)
                if not passive_subdomains and sublist3r_error:
                    progress_state["status"] = "failed"
                    progress_state["error"] = sublist3r_error
                    self._persist_recon_progress(
                        worker_id=worker_id,
                        root_domain=root_domain,
                        plugin_name=plugin_name,
                        payload=progress_state,
                    )
                    return 1, sublist3r_error
            elif sublist3r_error:
                progress_state["status"] = "failed"
                progress_state["error"] = sublist3r_error
                self._persist_recon_progress(
                    worker_id=worker_id,
                    root_domain=root_domain,
                    plugin_name=plugin_name,
                    payload=progress_state,
                )
                return 1, sublist3r_error

        progress_state["subdomains"] = sorted(found_subdomains)
        progress_state["phase"] = "probe"
        progress_state["status"] = "running"
        self._persist_recon_progress(
            worker_id=worker_id,
            root_domain=root_domain,
            plugin_name=plugin_name,
            payload=progress_state,
        )

        probe_results = dict(progress_state.get("probe_results") or {}) if isinstance(progress_state.get("probe_results"), dict) else {}
        for subdomain in progress_state["subdomains"]:
            if not subdomain:
                continue
            prior = probe_results.get(subdomain)
            if isinstance(prior, dict) and bool(prior.get("probed")):
                continue
            attempts: list[dict[str, Any]] = []
            selected_start_url = ""
            selected_status_code: int | None = None
            for scheme in ("https", "http"):
                probe_url = f"{scheme}://{subdomain}/"
                attempt_payload = {
                    "type": "probe",
                    "url": probe_url,
                    "started_at_utc": _now_iso(),
                }
                try:
                    probe_rsp = request_capped(
                        "GET",
                        probe_url,
                        timeout_seconds=probe_timeout,
                        read_limit=2048,
                        follow_redirects=True,
                        verify=probe_verify_tls,
                    )
                    attempt_payload["status_code"] = int(probe_rsp.status_code)
                    attempt_payload["resolved_url"] = str(probe_rsp.url or probe_url)
                    if selected_start_url == "" and 200 <= int(probe_rsp.status_code) < 500:
                        selected_start_url = str(probe_rsp.url or probe_url)
                        selected_status_code = int(probe_rsp.status_code)
                except Exception as exc:
                    attempt_payload["error"] = str(exc)
                attempts.append(attempt_payload)
                progress_state.setdefault("requests_made", []).append(attempt_payload)
                progress_state["requests_made"] = list(progress_state["requests_made"])[-max_logged_requests:]
                self._persist_recon_progress(
                    worker_id=worker_id,
                    root_domain=root_domain,
                    plugin_name=plugin_name,
                    payload=progress_state,
                )

            probe_results[subdomain] = {
                "subdomain": subdomain,
                "probed": True,
                "accessible": bool(selected_start_url),
                "start_url": selected_start_url,
                "status_code": selected_status_code,
                "attempts": attempts,
            }
            progress_state["probe_results"] = probe_results
            self._persist_recon_progress(
                worker_id=worker_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                payload=progress_state,
            )

        entries = []
        accessible_subdomains: list[str] = []
        for subdomain in progress_state["subdomains"]:
            row = probe_results.get(subdomain) if isinstance(probe_results.get(subdomain), dict) else {}
            if bool(row.get("accessible")) and str(row.get("start_url", "")).strip():
                accessible_subdomains.append(str(row.get("start_url")).strip())
            entries.append(
                {
                    "subdomain": subdomain,
                    "accessible": bool(row.get("accessible")),
                    "start_url": str(row.get("start_url", "")).strip(),
                    "status_code": row.get("status_code"),
                    "attempts": row.get("attempts") if isinstance(row.get("attempts"), list) else [],
                }
            )

        subdomain_payload = {
            "schema_version": 1,
            "plugin_name": plugin_name,
            "root_domain": root_domain,
            "generated_at_utc": _now_iso(),
            "subdomains": progress_state["subdomains"],
            "entries": entries,
            "accessible_subdomains": sorted(set(accessible_subdomains)),
        }
        _atomic_write_json(subdomains_file, subdomain_payload)
        self._upload_file_artifact(root_domain, "recon_subdomains_json", subdomains_file, worker_id)

        progress_state["phase"] = "complete"
        progress_state["status"] = "completed"
        progress_state["accessible_subdomains"] = subdomain_payload["accessible_subdomains"]
        self._persist_recon_progress(
            worker_id=worker_id,
            root_domain=root_domain,
            plugin_name=plugin_name,
            payload=progress_state,
        )
        self._write_recon_completion_flag(
            worker_id=worker_id,
            root_domain=root_domain,
            plugin_name=plugin_name,
            details={
                "subdomain_count": len(progress_state["subdomains"]),
                "accessible_count": len(subdomain_payload["accessible_subdomains"]),
            },
        )
        return 0, ""

    def _extract_recon_start_urls(self, *, root_domain: str, payload: dict[str, Any]) -> list[str]:
        urls: list[str] = []
        entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
        for row in entries:
            if not isinstance(row, dict):
                continue
            if not bool(row.get("accessible")):
                continue
            start_url = _normalize_subdomain_start_url(str(row.get("start_url", "") or "").strip())
            if start_url:
                urls.append(start_url)
        if not urls:
            for item in (payload.get("accessible_subdomains") if isinstance(payload.get("accessible_subdomains"), list) else []):
                start_url = _normalize_subdomain_start_url(str(item or "").strip())
                if start_url:
                    urls.append(start_url)
        if not urls:
            urls.append(f"https://{root_domain}")
        deduped: list[str] = []
        seen: set[str] = set()
        for url in urls:
            clean = str(url or "").strip()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            deduped.append(clean)
        return deduped

    def _run_recon_spider_plugin_task(
        self,
        *,
        worker_id: str,
        root_domain: str,
        plugin_name: str,
    ) -> tuple[int, str]:
        paths = self._artifact_paths(root_domain)
        subdomains_payload = self._load_json_file_or_artifact(
            root_domain=root_domain,
            artifact_type="recon_subdomains_json",
            path=paths["recon_subdomains_json"],
        )
        if not subdomains_payload:
            return 1, "missing recon_subdomains_json artifact"
        start_urls = self._extract_recon_start_urls(root_domain=root_domain, payload=subdomains_payload)
        if not start_urls:
            return 1, "no accessible subdomain start URLs were found"

        progress_path = self._recon_progress_path_for_plugin(root_domain, plugin_name)
        progress_state = self._load_json_file_or_artifact(
            root_domain=root_domain,
            artifact_type=self._recon_progress_artifact_type(plugin_name),
            path=progress_path,
        )
        if not progress_state:
            progress_state = {
                "schema_version": 1,
                "plugin_name": plugin_name,
                "root_domain": root_domain,
                "status": "running",
                "started_at_utc": _now_iso(),
                "requests_made": [],
                "subdomains": [],
                "last_frontier_size": None,
            }

        params = self._workflow_stage_parameters(plugin_name)
        no_ai = bool(params.get("no_ai", plugin_name != "recon_spider_ai"))
        force_wordlist = bool(params.get("force_wordlist", plugin_name == "recon_spider_wordlist"))
        verify_urls = bool(params.get("verify_urls", False))
        max_pages = _safe_int(params.get("max_pages", 0), 0)
        crawl_delay = _safe_float(
            params.get("spider_throttle_seconds", params.get("crawl_delay", 0.5)),
            0.5,
        )
        if crawl_delay < 0:
            crawl_delay = 0.0
        openai_timeout = _safe_float(params.get("openai_timeout", 0.0), 0.0)
        ai_probe_max_requests = _safe_int(params.get("ai_probe_max_requests", 0), 0)
        ai_probe_per_host_max = _safe_int(params.get("ai_probe_per_host_max", 0), 0)
        crawl_wordlist_raw = str(params.get("crawl_wordlist", "resources/wordlists/file_path_list.txt") or "").strip()
        extra_args = [
            str(item).strip()
            for item in (params.get("extra_args") if isinstance(params.get("extra_args"), list) else [])
            if str(item).strip()
        ]
        max_logged_requests = max(100, _safe_int(params.get("max_request_log_entries", 4000), 4000))
        strict_frontier_empty = bool(params.get("require_empty_frontier", True))
        log_path = paths.get(f"{plugin_name}_log") or (paths["recon_dir"] / f"{plugin_name}.log")

        prior_rows = progress_state.get("subdomains") if isinstance(progress_state.get("subdomains"), list) else []
        state_by_url: dict[str, dict[str, Any]] = {}
        for row in prior_rows:
            if not isinstance(row, dict):
                continue
            key = str(row.get("start_url", "") or "").strip()
            if key:
                state_by_url[key] = dict(row)
        for url in start_urls:
            if url not in state_by_url:
                state_by_url[url] = {
                    "start_url": url,
                    "host": str(urlparse(url).hostname or "").lower(),
                    "status": "pending",
                    "attempt_count": 0,
                    "last_exit_code": None,
                    "last_error": "",
                    "last_run_started_at_utc": None,
                    "last_run_completed_at_utc": None,
                }
        progress_state["subdomains"] = [state_by_url[url] for url in start_urls]
        progress_state["status"] = "running"
        progress_state["spider_throttle_seconds"] = crawl_delay
        self._persist_recon_progress(
            worker_id=worker_id,
            root_domain=root_domain,
            plugin_name=plugin_name,
            payload=progress_state,
        )

        empty_wordlist_path = paths["recon_dir"] / "empty-wordlist.txt"
        if not empty_wordlist_path.is_file():
            empty_wordlist_path.parent.mkdir(parents=True, exist_ok=True)
            empty_wordlist_path.write_text("", encoding="utf-8")

        for start_url in start_urls:
            row = state_by_url[start_url]
            if str(row.get("status", "")).strip().lower() == "completed":
                continue
            cmd = [
                self.cfg.python_executable,
                "nightmare.py",
                start_url,
                "--config",
                str(self.cfg.nightmare_config),
                "--resume",
            ]
            if no_ai:
                cmd.append("--no-ai")
            if force_wordlist and crawl_wordlist_raw:
                cmd.extend(["--crawl-wordlist", crawl_wordlist_raw])
            elif no_ai:
                cmd.extend(["--crawl-wordlist", str(empty_wordlist_path)])
            if verify_urls:
                cmd.append("--verify-urls")
            if max_pages > 0:
                cmd.extend(["--max-pages", str(max_pages)])
            if crawl_delay > 0:
                cmd.extend(["--crawl-delay", str(crawl_delay)])
            if openai_timeout > 0:
                cmd.extend(["--openai-timeout", str(openai_timeout)])
            if ai_probe_max_requests > 0:
                cmd.extend(["--ai-probe-max-requests", str(ai_probe_max_requests)])
            if ai_probe_per_host_max > 0:
                cmd.extend(["--ai-probe-per-host-max", str(ai_probe_per_host_max)])
            cmd.extend(extra_args)

            row["status"] = "running"
            row["attempt_count"] = int(row.get("attempt_count", 0) or 0) + 1
            row["last_run_started_at_utc"] = _now_iso()
            row["last_command"] = cmd
            progress_state.setdefault("requests_made", []).append(
                {
                    "type": "nightmare_spider_run",
                    "start_url": start_url,
                    "command": cmd,
                    "started_at_utc": row["last_run_started_at_utc"],
                }
            )
            progress_state["requests_made"] = list(progress_state["requests_made"])[-max_logged_requests:]
            progress_state["subdomains"] = [state_by_url[url] for url in start_urls]
            self._persist_recon_progress(
                worker_id=worker_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                payload=progress_state,
            )

            exit_code = 1
            err_text = ""
            try:
                exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
                if exit_code != 0:
                    err_text = summarize_subprocess_failure("nightmare", log_path, exit_code)
            except Exception as exc:
                exit_code = 1
                err_text = str(exc)

            row["last_exit_code"] = int(exit_code)
            row["last_error"] = str(err_text or "")
            row["last_run_completed_at_utc"] = _now_iso()
            row["status"] = "completed" if int(exit_code) == 0 else "failed"
            progress_state["subdomains"] = [state_by_url[url] for url in start_urls]
            self._persist_recon_progress(
                worker_id=worker_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                payload=progress_state,
            )

            if int(exit_code) != 0:
                progress_state["status"] = "failed"
                self._persist_recon_progress(
                    worker_id=worker_id,
                    root_domain=root_domain,
                    plugin_name=plugin_name,
                    payload=progress_state,
                )
                return int(exit_code), str(err_text or f"{plugin_name} failed for {start_url}")

            try:
                self._upload_nightmare_artifacts(worker_id=worker_id, root_domain=root_domain)
            except Exception as exc:
                self.logger.error(
                    "recon_spider_artifact_upload_failed",
                    worker_id=worker_id,
                    root_domain=root_domain,
                    plugin_name=plugin_name,
                    start_url=start_url,
                    error=str(exc),
                )

        session_payload = self._load_json_file_or_artifact(
            root_domain=root_domain,
            artifact_type="nightmare_session_json",
            path=paths["nightmare_session_json"],
        )
        frontier = session_payload.get("frontier") if isinstance(session_payload.get("frontier"), list) else []
        progress_state["last_frontier_size"] = len(frontier)
        if strict_frontier_empty and len(frontier) > 0:
            progress_state["status"] = "failed"
            self._persist_recon_progress(
                worker_id=worker_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                payload=progress_state,
            )
            return 1, f"{plugin_name} finished runs but nightmare frontier still has {len(frontier)} pending URL(s)"

        progress_state["status"] = "completed"
        self._persist_recon_progress(
            worker_id=worker_id,
            root_domain=root_domain,
            plugin_name=plugin_name,
            payload=progress_state,
        )
        self._write_recon_completion_flag(
            worker_id=worker_id,
            root_domain=root_domain,
            plugin_name=plugin_name,
            details={
                "start_url_count": len(start_urls),
                "frontier_size": len(frontier),
            },
        )
        return 0, ""

    def _run_recon_extractor_high_value_plugin_task(
        self,
        *,
        worker_id: str,
        root_domain: str,
        plugin_name: str,
    ) -> tuple[int, str]:
        paths = self._artifact_paths(root_domain)
        domain_dir = _domain_output_dir(root_domain, self.cfg.output_root)
        recon_dir = paths["recon_dir"]
        recon_dir.mkdir(parents=True, exist_ok=True)
        matches_dir = paths["recon_extractor_high_value_matches_dir"]
        matches_dir.mkdir(parents=True, exist_ok=True)
        summary_path = paths["recon_extractor_high_value_summary_json"]
        progress_path = self._recon_progress_path_for_plugin(root_domain, plugin_name)
        progress_state = self._load_json_file_or_artifact(
            root_domain=root_domain,
            artifact_type=self._recon_progress_artifact_type(plugin_name),
            path=progress_path,
        )
        if not progress_state:
            progress_state = {
                "schema_version": 1,
                "plugin_name": plugin_name,
                "root_domain": root_domain,
                "status": "running",
                "started_at_utc": _now_iso(),
                "requests_made": [],
            }
        params = self._workflow_stage_parameters(plugin_name)
        wordlist_raw = str(params.get("wordlist", "resources/wordlists/high_value_extractor_list.txt") or "").strip()
        wordlist_path = Path(wordlist_raw).expanduser()
        if not wordlist_path.is_absolute():
            wordlist_path = (BASE_DIR / wordlist_path).resolve()
        if not wordlist_path.is_file():
            progress_state["status"] = "failed"
            progress_state["error"] = f"high-value extractor wordlist not found: {wordlist_path}"
            self._persist_recon_progress(
                worker_id=worker_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                payload=progress_state,
            )
            return 1, str(progress_state["error"])

        raw_rules = []
        try:
            raw_text = wordlist_path.read_text(encoding="utf-8-sig")
            if raw_text.strip():
                parsed_rules = json.loads(raw_text)
                if isinstance(parsed_rules, list):
                    raw_rules = parsed_rules
        except Exception as exc:
            progress_state["status"] = "failed"
            progress_state["error"] = f"failed to parse high-value extractor wordlist: {exc}"
            self._persist_recon_progress(
                worker_id=worker_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                payload=progress_state,
            )
            return 1, str(progress_state["error"])

        compiled_rules: list[dict[str, Any]] = []
        for idx, row in enumerate(raw_rules):
            if not isinstance(row, dict):
                continue
            rule_name = str(row.get("name", f"rule_{idx + 1}") or f"rule_{idx + 1}").strip()
            pattern = str(row.get("pattern", row.get("regex", "")) or "").strip()
            if not rule_name or not pattern:
                continue
            flags = 0
            raw_flags = row.get("flags")
            flag_tokens: list[str] = []
            if isinstance(raw_flags, str):
                flag_tokens = [raw_flags]
            elif isinstance(raw_flags, list):
                flag_tokens = [str(item).strip() for item in raw_flags if str(item).strip()]
            if bool(row.get("ignore_case", False)):
                flag_tokens.append("IGNORECASE")
            for token in flag_tokens:
                token_upper = str(token).strip().upper()
                if token_upper in {"I", "IGNORECASE", "RE.IGNORECASE"}:
                    flags |= re.IGNORECASE
                elif token_upper in {"M", "MULTILINE", "RE.MULTILINE"}:
                    flags |= re.MULTILINE
                elif token_upper in {"S", "DOTALL", "RE.DOTALL"}:
                    flags |= re.DOTALL
            try:
                compiled_rules.append(
                    {
                        "name": rule_name,
                        "compiled": re.compile(pattern, flags=flags),
                        "importance_score": _safe_int(row.get("importance_score", 0), 0),
                    }
                )
            except re.error:
                continue

        max_file_bytes = max(4096, _safe_int(params.get("max_file_bytes", 2 * 1024 * 1024), 2 * 1024 * 1024))
        max_matches_per_rule_per_file = max(1, _safe_int(params.get("max_matches_per_rule_per_file", 100), 100))
        persist_every_files = max(1, _safe_int(params.get("persist_every_files", 10), 10))
        skip_ext = {
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
            ".ico",
            ".bmp",
            ".pdf",
            ".zip",
            ".gz",
            ".7z",
            ".rar",
            ".tar",
            ".mp4",
            ".mp3",
            ".wav",
            ".woff",
            ".woff2",
            ".ttf",
            ".eot",
        }
        excluded_dirs = {"recon", "fozzy-output", "__pycache__", ".git"}
        candidate_files: list[Path] = []
        if domain_dir.is_dir():
            for path in domain_dir.rglob("*"):
                if not path.is_file():
                    continue
                rel_parts = {str(part).lower() for part in path.relative_to(domain_dir).parts}
                if rel_parts & excluded_dirs:
                    continue
                if path.suffix.lower() in skip_ext:
                    continue
                candidate_files.append(path)
        candidate_files.sort(key=lambda p: str(p).lower())

        processed_files = {
            str(item).strip()
            for item in (progress_state.get("files_processed") if isinstance(progress_state.get("files_processed"), list) else [])
            if str(item).strip()
        }
        rows_out = list(progress_state.get("rows", [])) if isinstance(progress_state.get("rows"), list) else []
        rule_counts = dict(progress_state.get("rule_counts", {})) if isinstance(progress_state.get("rule_counts"), dict) else {}
        match_count = _safe_int(progress_state.get("match_count", len(rows_out)), len(rows_out))

        progress_state["status"] = "running"
        progress_state.setdefault("requests_made", []).append(
            {
                "type": "extractor_run",
                "started_at_utc": _now_iso(),
                "wordlist": str(wordlist_path),
                "compiled_rule_count": len(compiled_rules),
            }
        )
        progress_state["requests_made"] = list(progress_state["requests_made"])[-2000:]
        progress_state["files_total"] = len(candidate_files)
        self._persist_recon_progress(
            worker_id=worker_id,
            root_domain=root_domain,
            plugin_name=plugin_name,
            payload=progress_state,
        )

        files_since_persist = 0
        for file_path in candidate_files:
            relative_path = file_path.relative_to(domain_dir).as_posix()
            if relative_path in processed_files:
                continue
            try:
                raw_bytes = file_path.read_bytes()
            except Exception:
                processed_files.add(relative_path)
                continue
            body = raw_bytes[:max_file_bytes]
            if b"\x00" in body[:2048]:
                processed_files.add(relative_path)
                continue
            text = body.decode("utf-8", errors="replace")
            for rule in compiled_rules:
                rule_name = str(rule["name"])
                per_rule_hits = 0
                for idx, match in enumerate(rule["compiled"].finditer(text)):
                    matched_text = str(match.group(0) or "")
                    if not matched_text:
                        continue
                    digest_input = f"{relative_path}\0{rule_name}\0{idx}\0{matched_text[:512]}"
                    digest = hashlib.sha256(digest_input.encode("utf-8", errors="ignore")).hexdigest()[:24]
                    match_file = matches_dir / f"m_{digest}.json"
                    detail = {
                        "root_domain": root_domain,
                        "filter_name": rule_name,
                        "importance_score": int(rule.get("importance_score", 0) or 0),
                        "result_file": str(file_path.resolve()),
                        "relative_path": relative_path,
                        "match_index": idx,
                        "matched_text": matched_text,
                        "captured_at_utc": _now_iso(),
                    }
                    _atomic_write_json(match_file, detail)
                    rows_out.append(
                        {
                            "filter_name": rule_name,
                            "importance_score": int(rule.get("importance_score", 0) or 0),
                            "result_file": str(file_path.resolve()),
                            "match_file": str(match_file.resolve()),
                            "match_preview": matched_text[:240].replace("\n", " ").strip(),
                            "scope": "file_body",
                        }
                    )
                    rule_counts[rule_name] = int(rule_counts.get(rule_name, 0) or 0) + 1
                    match_count += 1
                    per_rule_hits += 1
                    if per_rule_hits >= max_matches_per_rule_per_file:
                        break
            processed_files.add(relative_path)
            files_since_persist += 1
            if files_since_persist >= persist_every_files:
                files_since_persist = 0
                progress_state["files_processed"] = sorted(processed_files)
                progress_state["rows"] = rows_out
                progress_state["rule_counts"] = rule_counts
                progress_state["match_count"] = int(match_count)
                progress_state["last_file"] = relative_path
                self._persist_recon_progress(
                    worker_id=worker_id,
                    root_domain=root_domain,
                    plugin_name=plugin_name,
                    payload=progress_state,
                )

        summary_payload = {
            "root_domain": root_domain,
            "generated_at_utc": _now_iso(),
            "wordlist_path": str(wordlist_path),
            "match_count": int(match_count),
            "files_total": len(candidate_files),
            "files_scanned": len(processed_files),
            "rule_counts": rule_counts,
            "rows": rows_out,
        }
        _atomic_write_json(summary_path, summary_payload)

        try:
            self._upload_file_artifact(
                root_domain,
                "recon_extractor_high_value_summary_json",
                summary_path,
                worker_id,
            )
            self._upload_file_artifact(root_domain, "extractor_summary_json", summary_path, worker_id)
            self._upload_directory_manifest_artifact(
                root_domain,
                "recon_extractor_high_value_matches_zip",
                matches_dir,
                worker_id,
            )
            self._upload_directory_manifest_artifact(root_domain, "extractor_matches_zip", matches_dir, worker_id)
        except Exception as exc:
            progress_state["status"] = "failed"
            progress_state["error"] = f"failed to upload extractor artifacts: {exc}"
            self._persist_recon_progress(
                worker_id=worker_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                payload=progress_state,
            )
            return 1, str(progress_state["error"])

        progress_state["status"] = "completed"
        progress_state["error"] = ""
        progress_state["files_processed"] = sorted(processed_files)
        progress_state["rows"] = rows_out
        progress_state["rule_counts"] = rule_counts
        progress_state["match_count"] = int(match_count)
        self._persist_recon_progress(
            worker_id=worker_id,
            root_domain=root_domain,
            plugin_name=plugin_name,
            payload=progress_state,
        )
        self._write_recon_completion_flag(
            worker_id=worker_id,
            root_domain=root_domain,
            plugin_name=plugin_name,
            details={
                "extractor_completed": True,
                "match_count": int(match_count),
                "files_scanned": len(processed_files),
            },
        )
        return 0, ""

    def _run_fozzy_plugin_task(
        self,
        *,
        worker_id: str,
        root_domain: str,
        plugin_name: str,
    ) -> tuple[int, str]:
        paths = self._artifact_paths(root_domain)
        params_path = paths["nightmare_parameters_json"]
        if not params_path.is_file():
            self._download_file_artifact(root_domain, "nightmare_parameters_json", params_path)
        if not params_path.is_file():
            return 1, "missing parameters artifact"

        hv_dir = paths.get("nightmare_high_value_dir")
        if hv_dir is not None:
            hv_dir.mkdir(parents=True, exist_ok=True)
            self._materialize_manifest_artifact(root_domain, "nightmare_high_value_zip", hv_dir)
        self._download_file_artifact(root_domain, "nightmare_post_requests_json", paths["nightmare_post_requests_json"])

        fozzy_params = self._workflow_stage_parameters(plugin_name)
        max_background_workers = max(
            1,
            _safe_int(
                fozzy_params.get("max_background_workers", self.cfg.fozzy_process_workers),
                max(1, int(self.cfg.fozzy_process_workers)),
            ),
        )
        max_workers_per_domain = max(1, _safe_int(fozzy_params.get("max_workers_per_domain", 1), 1))
        max_workers_per_subdomain = max(1, _safe_int(fozzy_params.get("max_workers_per_subdomain", 1), 1))

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
        self.logger.info(
            "fozzy_subprocess_start",
            worker_id=worker_id,
            root_domain=root_domain,
            stage=plugin_name,
            command=cmd,
            log_path=str(log_path),
        )
        try:
            exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
            err_text = summarize_subprocess_failure("fozzy", log_path, exit_code) if exit_code != 0 else ""
        except Exception as exc:
            return 1, str(exc)

        try:
            self._upload_file_artifact(root_domain, "fozzy_summary_json", paths["fozzy_summary_json"], worker_id)
            self._upload_file_artifact(root_domain, "fozzy_inventory_json", paths["fozzy_inventory_json"], worker_id)
            self._upload_file_artifact(root_domain, "fozzy_log", paths["fozzy_log"], worker_id)
            self._upload_directory_manifest_artifact(root_domain, "fozzy_results_zip", paths["fozzy_results_dir"], worker_id)
        except Exception as exc:
            self.logger.error(
                "fozzy_artifact_upload_failed",
                worker_id=worker_id,
                root_domain=root_domain,
                stage=plugin_name,
                error=str(exc),
            )
        return int(exit_code), str(err_text or "")

    def _run_auth0r_plugin_task(
        self,
        *,
        worker_id: str,
        root_domain: str,
        plugin_name: str,
    ) -> tuple[int, str]:
        paths = self._artifact_paths(root_domain)
        nightmare_session_path = paths["nightmare_session_json"]
        if not nightmare_session_path.is_file():
            self._download_file_artifact(root_domain, "nightmare_session_json", nightmare_session_path)
        if not nightmare_session_path.is_file():
            return 1, "missing nightmare session artifact"

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
        auth0r_params = self._workflow_stage_parameters(plugin_name)
        min_delay = auth0r_params.get("min_delay_seconds", auth0r_cfg.get("min_delay_seconds", 0.25))
        max_seed_actions = auth0r_params.get("max_seed_actions", auth0r_cfg.get("max_seed_actions", 200))
        timeout_seconds = auth0r_params.get("timeout_seconds", auth0r_cfg.get("timeout_seconds", 20.0))
        cmd.extend(["--min-delay-seconds", str(min_delay)])
        cmd.extend(["--max-seed-actions", str(max_seed_actions)])
        cmd.extend(["--timeout-seconds", str(timeout_seconds)])
        log_path = paths["auth0r_log"]
        self.logger.info(
            "auth0r_subprocess_start",
            worker_id=worker_id,
            root_domain=root_domain,
            stage=plugin_name,
            command=cmd,
            log_path=str(log_path),
        )
        try:
            exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
            err_text = summarize_subprocess_failure("auth0r", log_path, exit_code) if exit_code != 0 else ""
        except Exception as exc:
            return 1, str(exc)
        try:
            self._upload_file_artifact(root_domain, "auth0r_summary_json", paths["auth0r_summary_json"], worker_id)
            self._upload_file_artifact(root_domain, "auth0r_log", paths["auth0r_log"], worker_id)
        except Exception as exc:
            self.logger.error(
                "auth0r_artifact_upload_failed",
                worker_id=worker_id,
                root_domain=root_domain,
                stage=plugin_name,
                error=str(exc),
            )
        return int(exit_code), str(err_text or "")

    def _run_extractor_plugin_task(
        self,
        *,
        worker_id: str,
        root_domain: str,
        plugin_name: str,
    ) -> tuple[int, str]:
        paths = self._artifact_paths(root_domain)
        domain_dir = _domain_output_dir(root_domain, self.cfg.output_root)
        fozzy_results_dir = paths["fozzy_results_dir"]
        if not fozzy_results_dir.is_dir():
            self._materialize_manifest_artifact(root_domain, "fozzy_results_zip", fozzy_results_dir)
        if not fozzy_results_dir.is_dir():
            return 1, "missing fozzy results artifact"

        extractor_params = self._workflow_stage_parameters(plugin_name)
        extractor_workers = max(
            1,
            _safe_int(
                extractor_params.get("workers", self.cfg.extractor_process_workers),
                max(1, int(self.cfg.extractor_process_workers)),
            ),
        )
        force_extractor = bool(extractor_params.get("force", True))
        wordlist_raw = str(extractor_params.get("wordlist", "") or "").strip()
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
        if wordlist_raw:
            cmd.extend(["--wordlist", wordlist_raw])
        if force_extractor:
            cmd.append("--force")
        log_path = paths.get(f"{plugin_name}_log") or (domain_dir / "coordinator.extractor.log")
        self.logger.info(
            "extractor_subprocess_start",
            worker_id=worker_id,
            root_domain=root_domain,
            stage=plugin_name,
            command=cmd,
            log_path=str(log_path),
        )
        try:
            exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
            err_text = summarize_subprocess_failure("extractor", log_path, exit_code) if exit_code != 0 else ""
        except Exception as exc:
            return 1, str(exc)
        try:
            self._upload_file_artifact(root_domain, "extractor_summary_json", paths["extractor_summary_json"], worker_id)
            self._upload_directory_manifest_artifact(root_domain, "extractor_matches_zip", paths["extractor_matches_dir"], worker_id)
        except Exception as exc:
            self.logger.error(
                "extractor_artifact_upload_failed",
                worker_id=worker_id,
                root_domain=root_domain,
                stage=plugin_name,
                error=str(exc),
            )
        return int(exit_code), str(err_text or "")

    def _run_plugin_task(self, *, worker_id: str, entry: dict[str, Any]) -> None:
        workflow_id = str(entry.get("workflow_id") or self._workflow_id or "default").strip().lower() or "default"
        root_domain = str(entry.get("root_domain", "") or "").strip().lower()
        plugin_name = str(entry.get("plugin_name") or entry.get("stage") or "").strip().lower()
        if not root_domain or not plugin_name:
            self.logger.error(
                "workflow_task_invalid_entry",
                worker_id=worker_id,
                entry=entry,
            )
            return
        checkpoint = dict(entry.get("checkpoint") or {}) if isinstance(entry.get("checkpoint"), dict) else {}
        progress = dict(entry.get("progress") or {}) if isinstance(entry.get("progress"), dict) else {}
        checkpoint.update({"status": "running", "started_at_utc": _now_iso(), "plugin_name": plugin_name})
        progress.update({"status": "running", "plugin_name": plugin_name, "root_domain": root_domain})
        self._begin_job()
        heartbeat = LeaseHeartbeat(
            tick_fn=lambda: self.client.heartbeat_stage(
                worker_id,
                root_domain,
                plugin_name,
                self.cfg.lease_seconds,
                workflow_id=workflow_id,
                checkpoint=checkpoint,
                progress=progress,
                progress_artifact_type=f"workflow_progress_{plugin_name}",
            ),
            interval_seconds=self.cfg.heartbeat_interval_seconds,
            logger=self.logger,
            heartbeat_kind=f"{plugin_name}_stage",
        )
        heartbeat.start()
        try:
            self._update_plugin_progress(
                worker_id=worker_id,
                workflow_id=workflow_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                checkpoint=checkpoint,
                progress=progress,
            )
            plugin_runner = resolve_plugin(plugin_name)
            if plugin_runner is None:
                exit_code, err_text = 1, f"unsupported plugin: {plugin_name}"
            else:
                exit_code, err_text = plugin_runner.run(
                    PluginExecutionContext(
                        coordinator=self,
                        worker_id=worker_id,
                        root_domain=root_domain,
                        workflow_id=workflow_id,
                        plugin_name=plugin_name,
                        entry=entry,
                    )
                )

            checkpoint["completed_at_utc"] = _now_iso()
            checkpoint["status"] = "completed" if int(exit_code) == 0 else "failed"
            progress["status"] = checkpoint["status"]
            progress["error"] = str(err_text or "")
            self._update_plugin_progress(
                worker_id=worker_id,
                workflow_id=workflow_id,
                root_domain=root_domain,
                plugin_name=plugin_name,
                checkpoint=checkpoint,
                progress=progress,
            )
            self.client.complete_stage(
                worker_id,
                root_domain,
                plugin_name,
                int(exit_code),
                str(err_text or ""),
                workflow_id=workflow_id,
                checkpoint=checkpoint,
                progress=progress,
                progress_artifact_type=f"workflow_progress_{plugin_name}",
                resume_mode="exact",
            )
            self.logger.info(
                "workflow_task_complete",
                worker_id=worker_id,
                workflow_id=workflow_id,
                root_domain=root_domain,
                stage=plugin_name,
                exit_code=int(exit_code),
                error=str(err_text or ""),
            )
            try:
                self._enqueue_domain_workflow_schedule(root_domain)
                scheduled_count = self._refresh_domain_workflow_schedule(root_domain=root_domain, worker_id=worker_id)
                self.logger.info(
                    "workflow_domain_rescheduled_after_task_completion",
                    worker_id=worker_id,
                    workflow_id=workflow_id,
                    root_domain=root_domain,
                    stage=plugin_name,
                    tasks_scheduled=int(scheduled_count),
                )
            except Exception as exc:
                self.logger.error(
                    "workflow_domain_reschedule_after_task_completion_failed",
                    worker_id=worker_id,
                    workflow_id=workflow_id,
                    root_domain=root_domain,
                    stage=plugin_name,
                    error=str(exc),
                )
                self._record_worker_error(
                    worker_id=worker_id,
                    description=f"Failed to schedule follow-on tasks after {plugin_name} for {root_domain}",
                    exception=exc,
                    metadata={"workflow_id": workflow_id, "root_domain": root_domain, "stage": plugin_name},
                    mark_errored=False,
                )
        except Exception as exc:
            self.logger.error(
                "workflow_task_run_failed",
                worker_id=worker_id,
                workflow_id=workflow_id,
                root_domain=root_domain,
                stage=plugin_name,
                error=str(exc),
            )
            self._record_worker_error(
                worker_id=worker_id,
                description=f"Workflow task failed: {plugin_name} for {root_domain}",
                exception=exc,
                metadata={"workflow_id": workflow_id, "root_domain": root_domain, "stage": plugin_name},
                mark_errored=False,
            )
            try:
                self.client.complete_stage(
                    worker_id,
                    root_domain,
                    plugin_name,
                    1,
                    str(exc),
                    workflow_id=workflow_id,
                    checkpoint={"status": "failed", "error": str(exc), "completed_at_utc": _now_iso()},
                    progress={"status": "failed", "error": str(exc)},
                    progress_artifact_type=f"workflow_progress_{plugin_name}",
                    resume_mode="exact",
                )
            except Exception as complete_exc:
                self.logger.error(
                    "workflow_task_complete_failed_after_exception",
                    worker_id=worker_id,
                    workflow_id=workflow_id,
                    root_domain=root_domain,
                    stage=plugin_name,
                    error=str(complete_exc),
                )
        finally:
            heartbeat.stop()
            self._end_job()

    def _plugin_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-plugin-{idx}"
        self._set_worker_state(worker_id, "idle")
        self.logger.info(
            "workflow_plugin_worker_loop_started",
            worker_id=worker_id,
            worker_index=idx,
            workflow_id=self._workflow_id,
            workflow_ids=sorted(self._workflow_catalog.keys()),
            plugin_allowlist=self.cfg.plugin_allowlist,
        )
        while not self.stop_event.is_set():
            state = self._poll_worker_commands(worker_id)
            if state in {"paused", "stopped", "errored"}:
                self.logger.warning(
                    "workflow_plugin_worker_waiting_due_to_state",
                    worker_id=worker_id,
                    worker_state=state,
                    poll_interval_seconds=self.cfg.poll_interval_seconds,
                )
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            self._maybe_apply_fleet_output_clear()
            try:
                entry = self.client.claim_next_stage(
                    worker_id=worker_id,
                    lease_seconds=self.cfg.lease_seconds,
                    workflow_id="",
                    plugin_allowlist=self.cfg.plugin_allowlist,
                )
            except Exception as exc:
                self.logger.error("workflow_plugin_claim_failed", worker_id=worker_id, error=str(exc))
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            if not entry:
                self._set_worker_state(worker_id, "idle")
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            self._set_worker_state(worker_id, "running")
            try:
                self._run_plugin_task(worker_id=worker_id, entry=entry)
            finally:
                if self._get_worker_state(worker_id) == "running":
                    self._set_worker_state(worker_id, "idle")

    def _fozzy_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-fozzy-{idx}"
        self._set_worker_state(worker_id, "idle")
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
                entry = self.client.claim_stage(
                    worker_id,
                    "fozzy",
                    self.cfg.lease_seconds,
                    workflow_id="",
                )
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
                self.client.complete_stage(
                    worker_id,
                    root_domain,
                    "fozzy",
                    1,
                    "missing parameters artifact",
                    workflow_id=self._workflow_id,
                )
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
                self._materialize_manifest_artifact(root_domain, "nightmare_high_value_zip", hv_dir)
            self._download_file_artifact(root_domain, "nightmare_post_requests_json", paths["nightmare_post_requests_json"])

            self._begin_job()
            try:
                heartbeat = LeaseHeartbeat(
                    tick_fn=lambda: self.client.heartbeat_stage(
                        worker_id,
                        root_domain,
                        "fozzy",
                        self.cfg.lease_seconds,
                        workflow_id=self._workflow_id,
                    ),
                    interval_seconds=self.cfg.heartbeat_interval_seconds,
                    logger=self.logger,
                    heartbeat_kind="fozzy_stage",
                )
                heartbeat.start()
                fozzy_params = self._workflow_stage_parameters(self._workflow_id, "fozzy")
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
                    self._upload_directory_manifest_artifact(root_domain, "fozzy_results_zip", paths["fozzy_results_dir"], worker_id)
                except Exception as exc:
                    self.logger.error(
                        "fozzy_artifact_upload_failed",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        error=str(exc),
                    )

                if exit_code == 0 and not self.cfg.workflow_scheduler_enabled:
                    try:
                        self.client.enqueue_stage(root_domain, "extractor", workflow_id=self._workflow_id)
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
                    self.client.complete_stage(
                        worker_id,
                        root_domain,
                        "fozzy",
                        exit_code,
                        err_text,
                        workflow_id=self._workflow_id,
                    )
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
        self._set_worker_state(worker_id, "idle")
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
                entry = self.client.claim_stage(
                    worker_id,
                    "auth0r",
                    self.cfg.lease_seconds,
                    workflow_id="",
                )
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
                self.client.complete_stage(
                    worker_id,
                    root_domain,
                    "auth0r",
                    1,
                    "missing nightmare session artifact",
                    workflow_id=self._workflow_id,
                )
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
                    tick_fn=lambda: self.client.heartbeat_stage(
                        worker_id,
                        root_domain,
                        "auth0r",
                        self.cfg.lease_seconds,
                        workflow_id=self._workflow_id,
                    ),
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
                    self.client.complete_stage(
                        worker_id,
                        root_domain,
                        "auth0r",
                        exit_code,
                        err_text,
                        workflow_id=self._workflow_id,
                    )
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
        self._set_worker_state(worker_id, "idle")
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
                entry = self.client.claim_stage(
                    worker_id,
                    "extractor",
                    self.cfg.lease_seconds,
                    workflow_id="",
                )
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
                self._materialize_manifest_artifact(root_domain, "fozzy_results_zip", fozzy_results_dir)
            if not fozzy_results_dir.is_dir():
                self.client.complete_stage(
                    worker_id,
                    root_domain,
                    "extractor",
                    1,
                    "missing fozzy results artifact",
                    workflow_id=self._workflow_id,
                )
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
                    tick_fn=lambda: self.client.heartbeat_stage(
                        worker_id,
                        root_domain,
                        "extractor",
                        self.cfg.lease_seconds,
                        workflow_id=self._workflow_id,
                    ),
                    interval_seconds=self.cfg.heartbeat_interval_seconds,
                    logger=self.logger,
                    heartbeat_kind="extractor_stage",
                )
                heartbeat.start()
                extractor_params = self._workflow_stage_parameters(self._workflow_id, "extractor")
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
                    self._upload_directory_manifest_artifact(root_domain, "extractor_matches_zip", paths["extractor_matches_dir"], worker_id)
                except Exception as exc:
                    self.logger.error(
                        "extractor_artifact_upload_failed",
                        worker_id=worker_id,
                        root_domain=root_domain,
                        error=str(exc),
                    )

                try:
                    self.client.complete_stage(
                        worker_id,
                        root_domain,
                        "extractor",
                        exit_code,
                        err_text,
                        workflow_id=self._workflow_id,
                    )
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

    def _thread_target_with_error_capture(self, *, worker_id: str, target: Any, metadata: Optional[dict[str, Any]] = None) -> None:
        try:
            target()
        except Exception as exc:
            self.logger.error(
                "worker_thread_unhandled_exception",
                worker_id=worker_id,
                error=str(exc),
            )
            self._record_worker_error(
                worker_id=worker_id,
                description=f"Unhandled worker thread exception for {worker_id}",
                exception=exc,
                metadata=metadata or {},
                mark_errored=True,
            )
            raise

    def run(self) -> int:
        threads: list[threading.Thread] = []
        if self.cfg.workflow_scheduler_enabled:
            scheduler_worker_id = f"{self.worker_prefix}-scheduler-1"
            threads.append(
                threading.Thread(
                    target=lambda wid=scheduler_worker_id: self._thread_target_with_error_capture(
                        worker_id=wid,
                        target=self._workflow_scheduler_loop,
                        metadata={"loop": "workflow_scheduler"},
                    ),
                    daemon=True,
                    name=f"{scheduler_worker_id}-thread",
                )
            )
        plugin_worker_count = max(0, int(self.cfg.plugin_workers or 0))
        for idx in range(1, plugin_worker_count + 1):
            worker_id = f"{self.worker_prefix}-plugin-{idx}"
            t = threading.Thread(
                target=lambda index=idx, wid=worker_id: self._thread_target_with_error_capture(
                    worker_id=wid,
                    target=lambda: self._plugin_worker_loop(index),
                    metadata={"loop": "plugin_worker", "worker_index": index},
                ),
                daemon=True,
                name=f"{worker_id}-thread",
            )
            threads.append(t)

        for t in threads:
            t.start()
        self.logger.info(
            "coordinator_workers_started",
            worker_prefix=self.worker_prefix,
            workflow_scheduler_enabled=bool(self.cfg.workflow_scheduler_enabled),
            workflow_scheduler_interval_seconds=float(self.cfg.workflow_scheduler_interval_seconds),
            workflow_id=self._workflow_id,
            plugin_workers=plugin_worker_count,
            plugin_allowlist=self.cfg.plugin_allowlist,
            legacy_workers_disabled=True,
            ignored_legacy_settings={
                "enable_nightmare": bool(self.cfg.enable_nightmare),
                "enable_fozzy": bool(self.cfg.enable_fozzy),
                "enable_extractor": bool(self.cfg.enable_extractor),
                "enable_auth0r": bool(self.cfg.enable_auth0r),
                "nightmare_workers": int(self.cfg.nightmare_workers or 0),
                "fozzy_workers": int(self.cfg.fozzy_workers or 0),
                "extractor_workers": int(self.cfg.extractor_workers or 0),
                "auth0r_workers": int(self.cfg.auth0r_workers or 0),
            },
        )
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            self.logger.warning("coordinator_interrupt_received")
            self.stop_event.set()
            return 0


def parse_args(argv: Optional[list[str] ] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Distributed coordinator worker for workflow plugins")
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
        plugin_workers=max(0, int(cfg.plugin_workers or 0)),
        plugin_allowlist=cfg.plugin_allowlist,
        legacy_workers_disabled=True,
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
