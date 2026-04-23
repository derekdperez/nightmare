#!/usr/bin/env python3
"""FastAPI coordinator API and lightweight server app."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, Optional

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse

from server_app.store import CoordinatorStore, _stream_file_chunks


def _bearer_token(header_value: str | None) -> str:
    raw = str(header_value or "").strip()
    if raw.lower().startswith("bearer "):
        return raw[7:].strip()
    return raw


def create_app(*, coordinator_store: CoordinatorStore | None = None, coordinator_api_token: str = "") -> FastAPI:
    app = FastAPI(title="Nightmare Coordinator", version="2.0.0")
    app.state.coordinator_store = coordinator_store
    app.state.coordinator_api_token = str(coordinator_api_token or "").strip()

    def get_store() -> CoordinatorStore:
        store = app.state.coordinator_store
        if store is None:
            raise HTTPException(status_code=503, detail="coordinator is not configured (database_url missing)")
        return store

    def require_auth(authorization: str | None = Header(default=None)) -> None:
        expected = str(app.state.coordinator_api_token or "").strip()
        if not expected:
            return
        provided = _bearer_token(authorization)
        if provided != expected:
            raise HTTPException(status_code=401, detail="unauthorized")

    @app.get("/", response_class=HTMLResponse)
    def root() -> str:
        return "<html><body><h1>Nightmare Coordinator</h1><p>FastAPI coordinator API is running.</p></body></html>"

    @app.get("/healthz", response_class=PlainTextResponse)
    def healthz() -> str:
        return "ok"

    @app.post("/api/coord/claim")
    def claim_target(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        entry = store.claim_target(str(body.get("worker_id") or ""), int(body.get("lease_seconds") or 0))
        return {"ok": bool(entry), "entry": entry}

    @app.post("/api/coord/heartbeat")
    def heartbeat_target(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        ok = store.heartbeat(str(body.get("entry_id") or ""), str(body.get("worker_id") or ""), int(body.get("lease_seconds") or 0))
        return {"ok": bool(ok)}

    @app.post("/api/coord/complete")
    def complete_target(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        ok = store.finish(
            str(body.get("entry_id") or ""),
            str(body.get("worker_id") or ""),
            exit_code=int(body.get("exit_code") or 0),
            error=str(body.get("error") or ""),
        )
        return {"ok": bool(ok)}

    @app.get("/api/coord/session")
    def get_session(
        root_domain: str = Query(default=""),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        session = store.load_session(root_domain)
        return {"found": bool(session), "session": session}

    @app.post("/api/coord/session")
    def save_session(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        payload = body.get("session")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="session payload is required")
        ok = store.save_session(
            root_domain=str(payload.get("root_domain") or ""),
            start_url=str(payload.get("start_url") or ""),
            max_pages=int(payload.get("max_pages") or 0),
            payload=payload,
            saved_at_utc=str(payload.get("saved_at_utc") or "") or None,
        )
        return {"ok": bool(ok)}

    @app.post("/api/coord/stage/enqueue")
    def enqueue_stage(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        details = store.schedule_stage(
            str(body.get("root_domain") or ""),
            str(body.get("stage") or ""),
            workflow_id=str(body.get("workflow_id") or "default"),
            worker_id=str(body.get("worker_id") or ""),
            reason=str(body.get("reason") or ""),
            allow_retry_failed=bool(body.get("allow_retry_failed")),
            max_attempts=int(body.get("max_attempts") or 0),
            checkpoint=(dict(body.get("checkpoint")) if isinstance(body.get("checkpoint"), dict) else None),
            progress=(dict(body.get("progress")) if isinstance(body.get("progress"), dict) else None),
            progress_artifact_type=str(body.get("progress_artifact_type") or ""),
            resume_mode=str(body.get("resume_mode") or "exact"),
        )
        return {"ok": True, **details}

    @app.post("/api/coord/stage/claim")
    def claim_stage(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        entry = store.claim_stage(
            str(body.get("stage") or ""),
            str(body.get("worker_id") or ""),
            int(body.get("lease_seconds") or 0),
            workflow_id=str(body.get("workflow_id") or "default"),
        )
        return {"ok": bool(entry), "entry": entry}

    @app.post("/api/coord/stage/claim-next")
    def claim_next_stage(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        entry = store.claim_next_stage(
            worker_id=str(body.get("worker_id") or ""),
            lease_seconds=int(body.get("lease_seconds") or 0),
            workflow_id=str(body.get("workflow_id") or ""),
            plugin_allowlist=list(body.get("plugin_allowlist") or []),
        )
        return {"ok": bool(entry), "entry": entry}

    @app.post("/api/coord/stage/heartbeat")
    def heartbeat_stage(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        ok = store.heartbeat_stage_with_workflow(
            root_domain=str(body.get("root_domain") or ""),
            stage=str(body.get("stage") or ""),
            worker_id=str(body.get("worker_id") or ""),
            lease_seconds=int(body.get("lease_seconds") or 0),
            workflow_id=str(body.get("workflow_id") or "default"),
            checkpoint=(dict(body.get("checkpoint")) if isinstance(body.get("checkpoint"), dict) else None),
            progress=(dict(body.get("progress")) if isinstance(body.get("progress"), dict) else None),
            progress_artifact_type=str(body.get("progress_artifact_type") or ""),
        )
        return {"ok": bool(ok)}

    @app.post("/api/coord/stage/progress")
    def progress_stage(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        ok = store.update_stage_progress(
            root_domain=str(body.get("root_domain") or ""),
            stage=str(body.get("stage") or ""),
            worker_id=str(body.get("worker_id") or ""),
            workflow_id=str(body.get("workflow_id") or "default"),
            checkpoint=(dict(body.get("checkpoint")) if isinstance(body.get("checkpoint"), dict) else None),
            progress=(dict(body.get("progress")) if isinstance(body.get("progress"), dict) else None),
            progress_artifact_type=str(body.get("progress_artifact_type") or ""),
        )
        return {"ok": bool(ok)}

    @app.post("/api/coord/stage/complete")
    def complete_stage(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        ok = store.complete_stage(
            str(body.get("root_domain") or ""),
            str(body.get("stage") or ""),
            str(body.get("worker_id") or ""),
            workflow_id=str(body.get("workflow_id") or "default"),
            exit_code=int(body.get("exit_code") or 0),
            error=str(body.get("error") or ""),
            checkpoint=(dict(body.get("checkpoint")) if isinstance(body.get("checkpoint"), dict) else None),
            progress=(dict(body.get("progress")) if isinstance(body.get("progress"), dict) else None),
            progress_artifact_type=str(body.get("progress_artifact_type") or ""),
            resume_mode=str(body.get("resume_mode") or ""),
        )
        return {"ok": bool(ok)}

    @app.post("/api/coord/stage/reset")
    def reset_stage_tasks(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return store.reset_stage_tasks(
            workflow_id=str(body.get("workflow_id") or ""),
            root_domains=list(body.get("root_domains") or []),
            plugins=list(body.get("plugins") or []),
            hard_delete=bool(body.get("hard_delete")),
        )

    @app.post("/api/coord/artifact")
    def upload_artifact(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        content_b64 = str(body.get("content_base64") or "")
        content = b""
        if content_b64:
            try:
                content = base64.b64decode(content_b64.encode("ascii"), validate=True)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"invalid content_base64: {exc}") from exc
        ok = store.upload_artifact(
            root_domain=str(body.get("root_domain") or ""),
            artifact_type=str(body.get("artifact_type") or ""),
            content=content,
            source_worker=str(body.get("source_worker") or ""),
            content_encoding=str(body.get("content_encoding") or "identity"),
            manifest=(dict(body.get("manifest")) if isinstance(body.get("manifest"), dict) else None),
            retention_class=str(body.get("retention_class") or "derived_rebuildable"),
            media_type=str(body.get("media_type") or "application/octet-stream"),
        )
        return {"ok": bool(ok)}

    @app.post("/api/coord/artifact/stream")
    async def upload_artifact_stream(
        request: Request,
        root_domain: str = Query(default=""),
        artifact_type: str = Query(default=""),
        source_worker: str = Query(default=""),
        content_encoding: str = Query(default="identity"),
        retention_class: str = Query(default="derived_rebuildable"),
        media_type: str = Query(default="application/octet-stream"),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        payload = await request.body()
        ok = store.upload_artifact(
            root_domain=root_domain,
            artifact_type=artifact_type,
            content=payload,
            source_worker=source_worker,
            content_encoding=content_encoding,
            retention_class=retention_class,
            media_type=media_type,
        )
        return {"ok": bool(ok)}

    @app.get("/api/coord/artifact")
    def get_artifact(
        root_domain: str = Query(default=""),
        artifact_type: str = Query(default=""),
        include_content: bool = Query(default=True),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        artifact = store.get_artifact(root_domain, artifact_type, include_content=include_content)
        if artifact is None:
            return {"found": False, "root_domain": root_domain, "artifact_type": artifact_type}
        payload = dict(artifact)
        if include_content:
            payload["content_base64"] = base64.b64encode(bytes(payload.pop("content", b""))).decode("ascii")
        return {"found": True, "artifact": payload}

    @app.get("/api/coord/artifact/stream")
    def get_artifact_stream(
        root_domain: str = Query(default=""),
        artifact_type: str = Query(default=""),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ):
        stream_info = store.get_artifact_stream_path(root_domain, artifact_type)
        if not stream_info:
            artifact = store.get_artifact(root_domain, artifact_type, include_content=True)
            if artifact is None:
                raise HTTPException(status_code=404, detail="artifact not found")
            return StreamingResponse(iter([bytes(artifact.get("content") or b"")]), media_type=str(artifact.get("media_type") or "application/octet-stream"))
        metadata = stream_info["metadata"]
        return StreamingResponse(
            _stream_file_chunks(Path(stream_info["path"])),
            media_type=str(metadata.get("media_type") or "application/octet-stream"),
            headers={
                "X-Artifact-Sha256": str(metadata.get("content_sha256") or ""),
                "X-Artifact-Size": str(metadata.get("content_size_bytes") or 0),
            },
        )

    @app.get("/api/coord/artifacts")
    def list_artifacts(
        root_domain: str = Query(default=""),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return {"root_domain": root_domain, "artifacts": store.list_artifacts(root_domain)}

    @app.get("/api/coord/fleet-settings")
    def fleet_settings(
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return store.get_fleet_settings()

    @app.get("/api/coord/workflow-snapshot")
    def workflow_snapshot(
        limit: int = Query(default=2000),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return store.workflow_scheduler_snapshot(limit=limit)

    @app.get("/api/coord/workflow-domain")
    def workflow_domain(
        root_domain: str = Query(default=""),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return store.workflow_domain_scheduler_state(root_domain)

    @app.post("/api/coord/worker-command/claim")
    def claim_worker_command(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        command = store.claim_worker_command(str(body.get("worker_id") or ""), worker_state=str(body.get("worker_state") or "idle"))
        return {"ok": bool(command), "command": command}

    @app.post("/api/coord/worker-command/complete")
    def complete_worker_command(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        ok = store.complete_worker_command(
            str(body.get("worker_id") or ""),
            int(body.get("command_id") or 0),
            success=bool(body.get("success")),
            error=str(body.get("error") or ""),
        )
        return {"ok": bool(ok)}

    return app
