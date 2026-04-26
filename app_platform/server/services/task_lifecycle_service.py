#!/usr/bin/env python3
"""Heartbeat, progress, completion, and resource-lease refresh for stage tasks."""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

from shared.schemas import ExecutionResultSchema


class TaskLifecycleService:
    """Mutations for running tasks: leases, heartbeats, progress, completion."""

    def __init__(
        self,
        *,
        connect_factory: Callable[[], Any],
        default_lease_seconds: int,
        touch_worker_presence: Callable[[Any, str, str], None],
        sync_step_run_for_stage: Callable[
            [Any, str, str, str, str, str, str],
            None,
        ],
        record_system_event: Callable[[str, str, dict[str, Any]], None],
        telemetry: Any,
        refresh_stage_task_readiness: Callable[..., Any],
    ) -> None:
        self._connect = connect_factory
        self._default_lease_seconds = int(default_lease_seconds)
        self._touch_worker_presence = touch_worker_presence
        self._sync_step_run_for_stage = sync_step_run_for_stage
        self._record_system_event = record_system_event
        self._telemetry = telemetry
        self._refresh_stage_task_readiness = refresh_stage_task_readiness

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
        lease = max(15, int(lease_seconds or self._default_lease_seconds))
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
        if updated > 0:
            self._record_system_event(
                "workflow.task.lease_renewed",
                f"workflow_task:{widf}:{rd}:{stg}",
                {
                    "source": "coordinator_store.heartbeat_stage_with_workflow",
                    "workflow_id": widf,
                    "root_domain": rd,
                    "stage": stg,
                    "plugin_id": stg,
                    "worker_id": wid,
                    "lease_seconds": lease,
                    "status": "running",
                },
            )
            self._record_system_event(
                "worker.heartbeat",
                f"worker:{wid}",
                {
                    "source": "coordinator_store.heartbeat_stage_with_workflow",
                    "worker_id": wid,
                    "workflow_id": widf,
                    "root_domain": rd,
                    "stage": stg,
                    "status": "running",
                },
            )
        return updated > 0

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
        """Refresh volatile liveness and lease on the stage task and mirror leases row."""
        _ = checkpoint, progress, progress_artifact_type
        rd = str(root_domain or "").strip().lower()
        stg = str(stage or "").strip().lower()
        wid = str(worker_id or "").strip()
        widf = str(workflow_id or "").strip().lower() or "default"
        lease = max(15, int(lease_seconds or self._default_lease_seconds))
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
        if updated > 0:
            self._telemetry.incr(
                "coordinator.workflow.tasks.heartbeat",
                tags={"workflow_id": widf, "stage": stg, "worker_id": wid},
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
            self._record_system_event(
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
        try:
            ExecutionResultSchema.model_validate(
                {
                    "status": next_status,
                    "exit_code": int(exit_code),
                    "error": str(error or ""),
                    "checkpoint": dict(checkpoint or {}) if isinstance(checkpoint, dict) else {},
                    "progress": dict(progress or {}) if isinstance(progress, dict) else {},
                }
            )
        except Exception:
            return False
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
                        """
UPDATE coordinator_task_attempts
SET status = %s,
    error = %s,
    completed_at_utc = NOW(),
    duration_ms = GREATEST(0, EXTRACT(EPOCH FROM (NOW() - started_at_utc)) * 1000)::BIGINT
WHERE task_id = %s
  AND attempt_number = (
      SELECT COALESCE(MAX(attempt_number), 0)
      FROM coordinator_task_attempts
      WHERE task_id = %s
  );
""",
                        (next_status, str(error or "")[:2000], f"{widf}:{rd}:{stg}", f"{widf}:{rd}:{stg}"),
                    )
                    cur.execute(
                        "DELETE FROM coordinator_resource_leases WHERE workflow_id = %s AND root_domain = %s AND stage = %s AND worker_id = %s;",
                        (widf, rd, stg, wid),
                    )
                    self._sync_step_run_for_stage(
                        cur,
                        widf,
                        rd,
                        stg,
                        next_status,
                        wid,
                        str(error or "")[:2000],
                    )
            conn.commit()
        if updated > 0:
            self._telemetry.incr(
                "coordinator.workflow.tasks.completed",
                tags={"workflow_id": widf, "stage": stg, "status": next_status, "worker_id": wid},
            )
            try:
                self._refresh_stage_task_readiness(root_domain=rd, workflow_id=widf, limit=1000)
            except Exception:
                pass
            self._record_system_event(
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
            if next_status == "completed":
                self._record_system_event(
                    "workflow.task.succeeded",
                    f"workflow_task:{widf}:{rd}:{stg}",
                    {
                        "source": "coordinator_store.complete_stage",
                        "workflow_id": widf,
                        "root_domain": rd,
                        "stage": stg,
                        "plugin_id": stg,
                        "worker_id": wid,
                        "status": "succeeded",
                    },
                )
            else:
                self._record_system_event(
                    "workflow.task.failed",
                    f"workflow_task:{widf}:{rd}:{stg}",
                    {
                        "source": "coordinator_store.complete_stage",
                        "workflow_id": widf,
                        "root_domain": rd,
                        "stage": stg,
                        "plugin_id": stg,
                        "worker_id": wid,
                        "status": "failed",
                        "error": str(error or "")[:2000],
                    },
                )
        return updated > 0
