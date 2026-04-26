#!/usr/bin/env python3
"""Task control mutations for workflow-run cancellation and task retries."""

from __future__ import annotations

from typing import Any, Callable


class TaskControlService:
    """Handle operational task control actions with store-provided callbacks."""

    def __init__(
        self,
        *,
        connect_factory: Callable[[], Any],
        now_iso: Callable[[], str],
        record_system_event: Callable[[str, str, dict[str, Any]], None],
        sync_step_run_status: Callable[[Any, str, str, str, str, str], None],
        stage_prerequisites_satisfied: Callable[[Any, str, str, str], tuple[bool, str]],
        telemetry: Any,
    ) -> None:
        self._connect = connect_factory
        self._now_iso = now_iso
        self._record_system_event = record_system_event
        self._sync_step_run_status = sync_step_run_status
        self._stage_prerequisites_satisfied = stage_prerequisites_satisfied
        self._telemetry = telemetry

    def cancel_workflow_run(self, *, workflow_run_id: str, actor: str = "", reason: str = "") -> dict[str, Any]:
        """Cancel all active tasks that belong to a workflow run."""
        run_id = str(workflow_run_id or "").strip()
        if not run_id:
            return {"ok": False, "error": "workflow_run_id is required"}
        actor_text = str(actor or "").strip()[:200]
        reason_text = str(reason or "").strip()[:1000]
        affected = 0
        canceled_rows: list[tuple[str, str, str]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
SELECT workflow_id, root_domain, stage
FROM coordinator_stage_tasks
WHERE COALESCE(checkpoint_json->>'workflow_run_id','') = %s
  AND status IN ('pending', 'ready', 'running', 'paused')
FOR UPDATE;
""",
                    (run_id,),
                )
                rows = cur.fetchall()
                for row in rows:
                    widf = str(row[0] or "default").strip().lower() or "default"
                    rd = str(row[1] or "").strip().lower()
                    stg = str(row[2] or "").strip().lower()
                    if not rd or not stg:
                        continue
                    cur.execute(
                        """
UPDATE coordinator_stage_tasks
SET status = 'canceled',
    worker_id = NULL,
    lease_expires_at = NULL,
    heartbeat_at_utc = NULL,
    completed_at_utc = NOW(),
    error = %s,
    updated_at_utc = NOW()
WHERE workflow_id = %s AND root_domain = %s AND stage = %s;
""",
                        (reason_text or "Canceled by operator", widf, rd, stg),
                    )
                    changed = int(cur.rowcount or 0)
                    if changed <= 0:
                        continue
                    affected += changed
                    canceled_rows.append((widf, rd, stg))
                    cur.execute(
                        """
DELETE FROM coordinator_resource_leases
WHERE workflow_id = %s AND root_domain = %s AND stage = %s;
""",
                        (widf, rd, stg),
                    )
                    self._sync_step_run_status(
                        cur,
                        widf,
                        rd,
                        stg,
                        "canceled",
                        reason_text or "Canceled by operator",
                    )
            conn.commit()

        for widf, rd, stg in canceled_rows[:5000]:
            self._record_system_event(
                "workflow.task.canceled",
                f"workflow_task:{widf}:{rd}:{stg}",
                {
                    "source": "coordinator_store.cancel_workflow_run",
                    "workflow_id": widf,
                    "workflow_run_id": run_id,
                    "root_domain": rd,
                    "stage": stg,
                    "plugin_id": stg,
                    "status": "canceled",
                    "actor": actor_text,
                    "reason": reason_text,
                },
            )
        self._record_system_event(
            "workflow.run.canceled",
            f"workflow_run:{run_id}",
            {
                "source": "coordinator_store.cancel_workflow_run",
                "workflow_run_id": run_id,
                "status": "canceled",
                "actor": actor_text,
                "reason": reason_text,
                "affected_rows": affected,
            },
        )
        self._telemetry.incr("coordinator.workflow.runs.canceled")
        self._telemetry.gauge("coordinator.workflow.run.cancel_affected", float(affected))
        return {
            "ok": True,
            "workflow_run_id": run_id,
            "affected_rows": affected,
            "actor": actor_text,
            "reason": reason_text,
            "updated_at_utc": self._now_iso(),
        }

    def retry_failed_workflow_tasks(
        self,
        *,
        workflow_id: str = "",
        workflow_run_id: str = "",
        actor: str = "",
        reason: str = "",
        limit: int = 5000,
    ) -> dict[str, Any]:
        """Requeue failed tasks for retry with audit events."""
        wid_filter = str(workflow_id or "").strip().lower()
        run_filter = str(workflow_run_id or "").strip()
        if not wid_filter and not run_filter:
            return {"ok": False, "error": "workflow_id or workflow_run_id is required"}
        actor_text = str(actor or "").strip()[:200]
        reason_text = str(reason or "").strip()[:1000]
        max_rows = max(1, min(5000, int(limit or 5000)))
        retried = 0
        skipped_max_attempts = 0
        skipped_not_failed = 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                where_sql = ["status = 'failed'"]
                params: list[Any] = []
                if wid_filter:
                    where_sql.append("workflow_id = %s")
                    params.append(wid_filter)
                if run_filter:
                    where_sql.append("COALESCE(checkpoint_json->>'workflow_run_id','') = %s")
                    params.append(run_filter)
                params.append(max_rows)
                cur.execute(
                    f"""
SELECT workflow_id, root_domain, stage, attempt_count, max_attempts
FROM coordinator_stage_tasks
WHERE {' AND '.join(where_sql)}
ORDER BY updated_at_utc DESC
LIMIT %s
FOR UPDATE;
""",
                    params,
                )
                rows = cur.fetchall()
                for row in rows:
                    widf = str(row[0] or "default").strip().lower() or "default"
                    rd = str(row[1] or "").strip().lower()
                    stg = str(row[2] or "").strip().lower()
                    attempts = int(row[3] or 0)
                    max_attempts = max(1, int(row[4] or 1))
                    if attempts >= max_attempts:
                        skipped_max_attempts += 1
                        continue
                    ready, prereq_reason = self._stage_prerequisites_satisfied(cur, widf, rd, stg)
                    next_status = "ready" if ready else "pending"
                    cur.execute(
                        """
UPDATE coordinator_stage_tasks
SET status = %s,
    worker_id = NULL,
    lease_expires_at = NULL,
    heartbeat_at_utc = NULL,
    completed_at_utc = NULL,
    error = %s,
    updated_at_utc = NOW()
WHERE workflow_id = %s AND root_domain = %s AND stage = %s AND status = 'failed';
""",
                        (
                            next_status,
                            "" if next_status == "ready" else prereq_reason[:2000],
                            widf,
                            rd,
                            stg,
                        ),
                    )
                    changed = int(cur.rowcount or 0)
                    if changed <= 0:
                        skipped_not_failed += 1
                        continue
                    retried += changed
                    self._sync_step_run_status(
                        cur,
                        widf,
                        rd,
                        stg,
                        next_status,
                        "" if next_status == "ready" else prereq_reason[:2000],
                    )
            conn.commit()
        self._record_system_event(
            "workflow.task.retry_bulk",
            f"workflow:{wid_filter or 'all'}",
            {
                "source": "coordinator_store.retry_failed_workflow_tasks",
                "workflow_id": wid_filter,
                "workflow_run_id": run_filter,
                "actor": actor_text,
                "reason": reason_text,
                "retried": retried,
                "skipped_max_attempts": skipped_max_attempts,
                "skipped_not_failed": skipped_not_failed,
            },
        )
        self._telemetry.incr("coordinator.workflow.tasks.retry_requested")
        self._telemetry.gauge("coordinator.workflow.tasks.retried", float(retried))
        return {
            "ok": True,
            "workflow_id": wid_filter,
            "workflow_run_id": run_filter,
            "retried": retried,
            "skipped_max_attempts": skipped_max_attempts,
            "skipped_not_failed": skipped_not_failed,
            "actor": actor_text,
            "reason": reason_text,
            "updated_at_utc": self._now_iso(),
        }
