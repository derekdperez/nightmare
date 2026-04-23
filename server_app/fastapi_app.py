from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from reporting.server_pages import render_workers_html, render_workflows_html
from server_app.store import CoordinatorStore
from logging_app.store import LogStore


class TokenGuard:
    def __init__(self, token: str):
        self.token = str(token or "").strip()

    def __call__(self, authorization: Optional[str] = Header(default=None)) -> None:
        if not self.token:
            return
        supplied = str(authorization or "").strip()
        expected = f"Bearer {self.token}"
        if supplied != expected:
            raise HTTPException(status_code=401, detail="unauthorized")


class TargetClaimRequest(BaseModel):
    worker_id: str
    lease_seconds: int


class TargetHeartbeatRequest(BaseModel):
    entry_id: str
    worker_id: str
    lease_seconds: int


class TargetCompleteRequest(BaseModel):
    entry_id: str
    worker_id: str
    exit_code: int
    error: str = ""


class StageEnqueueRequest(BaseModel):
    root_domain: str
    stage: str
    workflow_id: str = "default"
    worker_id: str = ""
    reason: str = ""
    checkpoint: Optional[dict[str, Any]] = None
    progress: Optional[dict[str, Any]] = None
    progress_artifact_type: str = ""
    resume_mode: str = "exact"
    allow_retry_failed: bool = False
    max_attempts: int = 0


class StageClaimRequest(BaseModel):
    worker_id: str
    stage: str
    workflow_id: str = "default"
    lease_seconds: int


class StageClaimNextRequest(BaseModel):
    worker_id: str
    workflow_id: str = ""
    lease_seconds: int
    plugin_allowlist: list[str] = []


class StageHeartbeatRequest(BaseModel):
    worker_id: str
    root_domain: str
    stage: str
    workflow_id: str = "default"
    lease_seconds: int
    checkpoint: Optional[dict[str, Any]] = None
    progress: Optional[dict[str, Any]] = None
    progress_artifact_type: str = ""


class StageProgressRequest(BaseModel):
    worker_id: str
    root_domain: str
    stage: str
    workflow_id: str = "default"
    checkpoint: Optional[dict[str, Any]] = None
    progress: Optional[dict[str, Any]] = None
    progress_artifact_type: str = ""


class StageCompleteRequest(BaseModel):
    worker_id: str
    root_domain: str
    stage: str
    workflow_id: str = "default"
    exit_code: int
    error: str = ""
    checkpoint: Optional[dict[str, Any]] = None
    progress: Optional[dict[str, Any]] = None
    progress_artifact_type: str = ""
    resume_mode: str = ""


class SessionRequest(BaseModel):
    session: dict[str, Any]


class ArtifactRequest(BaseModel):
    root_domain: str
    artifact_type: str
    source_worker: str = ""
    content_encoding: str = "identity"
    content_base64: str
    retention_class: str = "important_raw"
    manifest: Optional[dict[str, Any]] = None
    local_path: str = ""


class WorkerCommandClaimRequest(BaseModel):
    worker_id: str
    worker_state: str = "idle"


class WorkerCommandCompleteRequest(BaseModel):
    worker_id: str
    command_id: int
    success: bool
    error: str = ""


def create_app(
    *,
    output_root: str | Path,
    database_url: str,
    log_database_url: str = "",
    coordinator_token: str = "",
) -> FastAPI:
    store = CoordinatorStore(database_url)
    log_store = LogStore(log_database_url) if str(log_database_url or "").strip() else None
    auth = TokenGuard(coordinator_token)
    app = FastAPI(title="Nightmare Coordinator", version="2.0")

    @app.get("/", response_class=HTMLResponse)
    def root_page() -> str:
        return render_workers_html()

    @app.get("/workers", response_class=HTMLResponse)
    def workers_page() -> str:
        return render_workers_html()

    @app.get("/workflows", response_class=HTMLResponse)
    def workflows_page() -> str:
        return render_workflows_html()

    @app.get("/api/coord/workers")
    def worker_statuses(_: None = Depends(auth)) -> dict[str, Any]:
        return store.worker_statuses()

    @app.post("/api/coord/claim")
    def claim_target(payload: TargetClaimRequest, _: None = Depends(auth)) -> dict[str, Any]:
        return {"ok": True, "entry": store.claim_target(payload.worker_id, payload.lease_seconds)}

    @app.post("/api/coord/heartbeat")
    def heartbeat_target(payload: TargetHeartbeatRequest, _: None = Depends(auth)) -> dict[str, Any]:
        return {"ok": store.heartbeat(payload.entry_id, payload.worker_id, payload.lease_seconds)}

    @app.post("/api/coord/complete")
    def complete_target(payload: TargetCompleteRequest, _: None = Depends(auth)) -> dict[str, Any]:
        return {"ok": store.finish(payload.entry_id, payload.worker_id, exit_code=payload.exit_code, error=payload.error)}

    @app.post("/api/coord/stage/enqueue")
    def enqueue_stage(payload: StageEnqueueRequest, _: None = Depends(auth)) -> dict[str, Any]:
        result = store.schedule_stage(
            payload.root_domain,
            payload.stage,
            workflow_id=payload.workflow_id,
            worker_id=payload.worker_id,
            reason=payload.reason,
            checkpoint=payload.checkpoint,
            progress=payload.progress,
            progress_artifact_type=payload.progress_artifact_type,
            resume_mode=payload.resume_mode,
            allow_retry_failed=payload.allow_retry_failed,
            max_attempts=payload.max_attempts,
        )
        return {"ok": True, **result}

    @app.post("/api/coord/stage/claim")
    def claim_stage(payload: StageClaimRequest, _: None = Depends(auth)) -> dict[str, Any]:
        return {"ok": True, "entry": store.claim_stage(payload.stage, payload.worker_id, payload.lease_seconds, workflow_id=payload.workflow_id)}

    @app.post("/api/coord/stage/claim-next")
    def claim_next_stage(payload: StageClaimNextRequest, _: None = Depends(auth)) -> dict[str, Any]:
        entry = store.claim_next_stage(
            worker_id=payload.worker_id,
            lease_seconds=payload.lease_seconds,
            workflow_id=payload.workflow_id,
            plugin_allowlist=payload.plugin_allowlist,
        )
        return {"ok": True, "entry": entry}

    @app.post("/api/coord/stage/heartbeat")
    def heartbeat_stage(payload: StageHeartbeatRequest, _: None = Depends(auth)) -> dict[str, Any]:
        return {
            "ok": store.heartbeat_stage_with_workflow(
                root_domain=payload.root_domain,
                stage=payload.stage,
                worker_id=payload.worker_id,
                lease_seconds=payload.lease_seconds,
                workflow_id=payload.workflow_id,
                checkpoint=payload.checkpoint,
                progress=payload.progress,
                progress_artifact_type=payload.progress_artifact_type,
            )
        }

    @app.post("/api/coord/stage/progress")
    def stage_progress(payload: StageProgressRequest, _: None = Depends(auth)) -> dict[str, Any]:
        return {
            "ok": store.update_stage_progress(
                root_domain=payload.root_domain,
                stage=payload.stage,
                worker_id=payload.worker_id,
                workflow_id=payload.workflow_id,
                checkpoint=payload.checkpoint,
                progress=payload.progress,
                progress_artifact_type=payload.progress_artifact_type,
            )
        }

    @app.post("/api/coord/stage/complete")
    def stage_complete(payload: StageCompleteRequest, _: None = Depends(auth)) -> dict[str, Any]:
        return {
            "ok": store.complete_stage(
                payload.root_domain,
                payload.stage,
                payload.worker_id,
                workflow_id=payload.workflow_id,
                exit_code=payload.exit_code,
                error=payload.error,
                checkpoint=payload.checkpoint,
                progress=payload.progress,
                progress_artifact_type=payload.progress_artifact_type,
                resume_mode=payload.resume_mode,
            )
        }

    @app.post("/api/coord/stage/reset")
    def stage_reset(
        workflow_id: str = Query(default=""),
        hard_delete: bool = Query(default=False),
        root_domains: list[str] = Query(default=[]),
        plugins: list[str] = Query(default=[]),
        _: None = Depends(auth),
    ) -> dict[str, Any]:
        return store.reset_stage_tasks(
            workflow_id=workflow_id,
            hard_delete=hard_delete,
            root_domains=root_domains,
            plugins=plugins,
        )

    @app.get("/api/coord/session")
    def get_session(root_domain: str = Query(...), _: None = Depends(auth)) -> dict[str, Any]:
        session = store.load_session(root_domain)
        return {"found": session is not None, "session": session}

    @app.post("/api/coord/session")
    def save_session(payload: SessionRequest, _: None = Depends(auth)) -> dict[str, Any]:
        session = dict(payload.session or {})
        ok = store.save_session(
            root_domain=str(session.get("root_domain") or ""),
            start_url=str(session.get("start_url") or ""),
            max_pages=int(session.get("max_pages") or 0),
            payload=session,
            saved_at_utc=str(session.get("saved_at_utc") or ""),
        )
        return {"ok": ok}

    @app.get("/api/coord/artifact")
    def get_artifact(root_domain: str = Query(...), artifact_type: str = Query(...), _: None = Depends(auth)) -> dict[str, Any]:
        artifact = store.get_artifact(root_domain, artifact_type)
        if not artifact:
            return {"found": False}
        body = dict(artifact)
        raw = bytes(body.pop("content", b""))
        body["content_base64"] = base64.b64encode(raw).decode("ascii")
        return {"found": True, "artifact": body}

    @app.post("/api/coord/artifact")
    def upload_artifact(payload: ArtifactRequest, _: None = Depends(auth)) -> dict[str, Any]:
        raw = base64.b64decode(payload.content_base64.encode("ascii")) if payload.content_base64 else b""
        ok = store.upload_artifact(
            root_domain=payload.root_domain,
            artifact_type=payload.artifact_type,
            content=raw,
            source_worker=payload.source_worker,
            content_encoding=payload.content_encoding,
            retention_class=payload.retention_class,
            manifest=payload.manifest,
            local_path=payload.local_path,
        )
        return {"ok": ok}

    @app.get("/api/coord/fleet-settings")
    def fleet_settings(_: None = Depends(auth)) -> dict[str, Any]:
        return store.get_fleet_settings()

    @app.get("/api/coord/workflow-snapshot")
    def workflow_snapshot(limit: int = Query(default=2000), _: None = Depends(auth)) -> dict[str, Any]:
        return store.workflow_scheduler_snapshot(limit=limit)

    @app.get("/api/coord/workflow-domain")
    def workflow_domain(root_domain: str = Query(...), _: None = Depends(auth)) -> dict[str, Any]:
        return store.workflow_domain_snapshot(root_domain)

    @app.post("/api/coord/worker-command/claim")
    def worker_command_claim(payload: WorkerCommandClaimRequest, _: None = Depends(auth)) -> dict[str, Any]:
        return {"ok": True, "command": store.claim_worker_command(payload.worker_id, worker_state=payload.worker_state)}

    @app.post("/api/coord/worker-command/complete")
    def worker_command_complete(payload: WorkerCommandCompleteRequest, _: None = Depends(auth)) -> dict[str, Any]:
        return {"ok": store.complete_worker_command(payload.worker_id, payload.command_id, success=payload.success, error=payload.error)}

    @app.get("/api/coord/crawl-progress")
    def crawl_progress(_: None = Depends(auth)) -> dict[str, Any]:
        return store.workflow_scheduler_snapshot(limit=500)

    @app.get("/api/coord/events")
    def list_events(limit: int = 250, offset: int = 0, q: str = "", _: None = Depends(auth)) -> dict[str, Any]:
        return store.list_events(limit=limit, offset=offset, search=q)

    @app.get("/api/coord/log-events")
    def list_log_events(limit: int = 250, offset: int = 0, q: str = "", _: None = Depends(auth)) -> dict[str, Any]:
        if log_store is None:
            return {"total": 0, "events": [], "limit": limit, "offset": offset}
        return log_store.query_events(limit=limit, offset=offset, search=q)

    return app
