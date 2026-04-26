#!/usr/bin/env python3
"""Task claim and lease orchestration for coordinator workers."""

from __future__ import annotations

import json
from typing import Any, Callable, Optional


class TaskClaimService:
    """Handle ready-task claiming with resource lease coordination."""

    def __init__(
        self,
        *,
        connect_factory: Callable[[], Any],
        default_lease_seconds: int,
        refresh_stage_task_readiness: Callable[..., Any],
        touch_worker_presence: Callable[[Any, str, str], None],
        cleanup_orphaned_leases_cur: Callable[[Any], None],
        stage_prerequisites_satisfied: Callable[[Any, str, str, str], tuple[bool, str]],
        sync_workflow_step_run: Callable[[Any, str, str, str, str, str, str], None],
        stage_lease_key: Callable[[dict[str, Any]], str],
        resource_conflict_exists: Callable[[Any, str, str, str, int], bool],
        record_system_event: Callable[[str, str, dict[str, Any]], None],
        telemetry: Any,
    ) -> None:
        self._connect = connect_factory
        self._default_lease_seconds = int(default_lease_seconds)
        self._refresh_stage_task_readiness = refresh_stage_task_readiness
        self._touch_worker_presence = touch_worker_presence
        self._cleanup_orphaned_leases_cur = cleanup_orphaned_leases_cur
        self._stage_prerequisites_satisfied = stage_prerequisites_satisfied
        self._sync_workflow_step_run = sync_workflow_step_run
        self._stage_lease_key = stage_lease_key
        self._resource_conflict_exists = resource_conflict_exists
        self._record_system_event = record_system_event
        self._telemetry = telemetry

    def try_claim_stage_with_resources(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        workflow_id: str = "",
        plugin_allowlist: Optional[list[str]] = None,
    ) -> Optional[dict[str, Any]]:
        wid = str(worker_id or "").strip()
        if not wid:
            raise ValueError("worker_id is required")
        lease = max(15, int(lease_seconds or self._default_lease_seconds))
        allowlist = [
            str(item or "").strip().lower()
            for item in (plugin_allowlist or [])
            if str(item or "").strip()
        ]
        self._refresh_stage_task_readiness(limit=1000)
        # Target-domain lock: skip collision check for workflow-run tasks (checkpoint
        # carries workflow_run_id) so ready steps are not starved by a legacy running
        # coordinator_targets lease on the same root_domain.
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
      OR (COALESCE(checkpoint_json, '{}'::jsonb) ? 'force_run_override')
      OR (COALESCE(checkpoint_json, '{}'::jsonb) ? 'workflow_run_id')
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
                self._cleanup_orphaned_leases_cur(cur)
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
                            row_map["workflow_id"],
                            row_map["root_domain"],
                            row_map["stage"],
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
                        self._sync_workflow_step_run(
                            cur,
                            row_map["workflow_id"],
                            row_map["root_domain"],
                            row_map["stage"],
                            "pending",
                            str(blocked_reason)[:2000],
                            "",
                        )
                        continue
                    lease_key = self._stage_lease_key(row_map)
                    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (f"{row_map['root_domain']}:{lease_key}",))
                    if self._resource_conflict_exists(
                        cur,
                        row_map["root_domain"],
                        lease_key,
                        row_map["access_mode"],
                        row_map["max_parallelism"],
                    ):
                        continue
                    cur.execute(update_sql, (wid, lease, row_map["workflow_id"], row_map["root_domain"], row_map["stage"]))
                    updated = cur.fetchone()
                    if updated is None:
                        continue
                    self._sync_workflow_step_run(
                        cur,
                        row_map["workflow_id"],
                        row_map["root_domain"],
                        row_map["stage"],
                        "running",
                        "",
                        wid,
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
                    cur.execute(
                        """
INSERT INTO coordinator_task_attempts(
  task_id, workflow_id, root_domain, stage, attempt_number, worker_id, status, metadata_json
)
VALUES (%s,%s,%s,%s,%s,%s,'running',%s::jsonb)
ON CONFLICT (task_id, attempt_number) DO NOTHING;
""",
                        (
                            f"{updated[0]}:{updated[1]}:{updated[2]}",
                            str(updated[0] or ""),
                            str(updated[1] or ""),
                            str(updated[2] or ""),
                            int(updated[5] or 0),
                            str(wid or ""),
                            json.dumps(
                                {
                                    "resource_class": row_map["resource_class"],
                                    "access_mode": row_map["access_mode"],
                                    "concurrency_group": row_map["concurrency_group"],
                                    "max_parallelism": int(row_map["max_parallelism"] or 1),
                                    "max_attempts": int(row_map["max_attempts"] or 1),
                                },
                                ensure_ascii=False,
                            ),
                        ),
                    )
                    claimed = updated
                    break
            conn.commit()
        if claimed is None:
            self._telemetry.incr("coordinator.workflow.claim.empty", tags={"worker_id": wid})
            return None
        row = claimed
        self._telemetry.incr(
            "coordinator.workflow.tasks.claimed",
            tags={"workflow_id": str(row[0] or "default"), "stage": str(row[2] or ""), "worker_id": wid},
        )
        self._record_system_event(
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
        self._record_system_event(
            "workflow.task.started",
            f"workflow_task:{row[0]}:{row[1]}:{row[2]}",
            {
                "source": "coordinator_store.try_claim_stage_with_resources",
                "workflow_id": row[0],
                "root_domain": row[1],
                "stage": row[2],
                "plugin_id": row[2],
                "status": "running",
                "worker_id": row[4],
                "attempt_count": int(row[5] or 0),
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
