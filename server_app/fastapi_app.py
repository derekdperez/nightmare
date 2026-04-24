#!/usr/bin/env python3
"""FastAPI coordinator API and lightweight server app."""

from __future__ import annotations

import base64
import json
import re
from pathlib import Path
from typing import Any, Optional

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse

from server_app.store import CoordinatorStore, _stream_file_chunks


BASE_DIR = Path(__file__).resolve().parents[1]
WORKFLOW_FILE_SUFFIX = ".workflow.json"
WORKFLOW_FILE_GLOB = f"*{WORKFLOW_FILE_SUFFIX}"


def _bearer_token(header_value: str | None) -> str:
    raw = str(header_value or "").strip()
    if raw.lower().startswith("bearer "):
        return raw[7:].strip()
    return raw


def _parse_status_filters(payload: dict[str, Any]) -> list[str]:
    raw_statuses = payload.get("statuses", payload.get("status", []))
    if isinstance(raw_statuses, str):
        values = [item.strip().lower() for item in raw_statuses.split(",") if item.strip()]
    elif isinstance(raw_statuses, list):
        values = [str(item or "").strip().lower() for item in raw_statuses if str(item or "").strip()]
    else:
        values = []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        status = "failed" if value in {"errored", "error"} else value
        if status not in {"pending", "ready", "running", "completed", "failed", "paused"}:
            continue
        if not status or status in seen:
            continue
        seen.add(status)
        normalized.append(status)
    return normalized


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_workflow_id(value: Any, *, default: str = "") -> str:
    raw = str(value or "").strip().lower()
    safe = re.sub(r"[^a-z0-9._-]+", "-", raw).strip("-")
    if safe:
        return safe
    fallback = str(default or "").strip().lower()
    return re.sub(r"[^a-z0-9._-]+", "-", fallback).strip("-")


def _workflow_id_from_path(path: Path) -> str:
    name = str(path.name or "")
    lowered = name.lower()
    if lowered.endswith(WORKFLOW_FILE_SUFFIX):
        return _normalize_workflow_id(name[:-len(WORKFLOW_FILE_SUFFIX)])
    return _normalize_workflow_id(path.stem)


def _iter_workflow_paths() -> list[Path]:
    workflows_dir = BASE_DIR / "workflows"
    if not workflows_dir.is_dir():
        return []
    return sorted(path.resolve() for path in workflows_dir.glob(WORKFLOW_FILE_GLOB) if path.is_file())


def _load_workflow_payload(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
        parsed = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    workflow_id = _normalize_workflow_id(parsed.get("workflow_id"), default=_workflow_id_from_path(path))
    parsed["workflow_id"] = workflow_id or _workflow_id_from_path(path)
    plugins = parsed.get("plugins")
    parsed["plugins"] = [item for item in plugins if isinstance(item, dict)] if isinstance(plugins, list) else []
    return parsed


def _resolve_workflow_path(workflow_id: str) -> Path | None:
    safe_id = _normalize_workflow_id(workflow_id)
    if not safe_id:
        return None
    direct = (BASE_DIR / "workflows" / f"{safe_id}{WORKFLOW_FILE_SUFFIX}").resolve()
    if direct.is_file():
        return direct
    for path in _iter_workflow_paths():
        payload = _load_workflow_payload(path)
        payload_id = _normalize_workflow_id(payload.get("workflow_id"), default=_workflow_id_from_path(path))
        if payload_id and payload_id == safe_id:
            return path
    return None


def _workflow_index_payload() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in _iter_workflow_paths():
        payload = _load_workflow_payload(path)
        workflow_id = _normalize_workflow_id(payload.get("workflow_id"), default=_workflow_id_from_path(path))
        if not workflow_id:
            continue
        out.append(
            {
                "workflow_id": workflow_id,
                "description": str(payload.get("description") or "").strip(),
                "plugin_count": len(payload.get("plugins") if isinstance(payload.get("plugins"), list) else []),
                "path_rel": str(path.relative_to(BASE_DIR)).replace("\\", "/"),
            }
        )
    out.sort(key=lambda item: str(item.get("workflow_id") or ""))
    return out


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

    @app.get("/api/coord/database-status")
    def database_status(
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return store.database_status()

    @app.post("/api/coord/register-targets")
    def register_targets(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        raw_targets = body.get("targets")
        if not isinstance(raw_targets, list):
            raise HTTPException(status_code=400, detail="targets list is required")
        targets = [str(item or "").strip() for item in raw_targets if str(item or "").strip()]
        result = store.register_targets(targets, replace_existing=bool(body.get("replace_existing")))
        return {"ok": True, **result}

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
            workflow_id="",
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
            workflow_id="",
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
            statuses=_parse_status_filters(body),
            hard_delete=bool(body.get("hard_delete")),
        )

    @app.post("/api/coord/stage/control")
    def control_stage_task(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        result = store.control_stage_task(
            workflow_id=str(body.get("workflow_id") or "default"),
            root_domain=str(body.get("root_domain") or ""),
            stage=str(body.get("stage", body.get("plugin", "")) or ""),
            action=str(body.get("action") or ""),
        )
        if not bool(result.get("ok")):
            raise HTTPException(status_code=404, detail=str(result.get("error") or "task control failed"))
        return result

    @app.post("/api/coord/targets/reset")
    def reset_targets(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return store.reset_targets(
            root_domains=list(body.get("root_domains") or []),
            statuses=_parse_status_filters(body),
            hard_delete=bool(body.get("hard_delete")),
        )

    @app.post("/api/coord/tasks/reset")
    def reset_tasks(
        body: dict[str, Any] = Body(default_factory=dict),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        raw_scopes = body.get("scopes", body.get("scope", []))
        if isinstance(raw_scopes, str):
            scope_values = [raw_scopes.strip().lower()] if raw_scopes.strip() else []
        elif isinstance(raw_scopes, list):
            scope_values = [str(item or "").strip().lower() for item in raw_scopes if str(item or "").strip()]
        else:
            scope_values = []
        scopes: list[str] = []
        for scope in scope_values:
            if scope in {"all", "*"}:
                scopes = ["stage_tasks", "targets"]
                break
            if scope in {"stage", "stages", "stage_task", "stage_tasks"} and "stage_tasks" not in scopes:
                scopes.append("stage_tasks")
            if scope in {"target", "targets", "target_queue", "coordinator_targets"} and "targets" not in scopes:
                scopes.append("targets")
        if not scopes:
            scopes = ["stage_tasks"]

        status_filters = _parse_status_filters(body)
        workflow_id = str(body.get("workflow_id") or "")
        root_domains = list(body.get("root_domains") or [])
        plugins = list(body.get("plugins") or [])
        hard_delete = bool(body.get("hard_delete"))

        stage_result: dict[str, Any] | None = None
        target_result: dict[str, Any] | None = None
        if "stage_tasks" in scopes:
            stage_result = store.reset_stage_tasks(
                workflow_id=workflow_id,
                root_domains=root_domains,
                plugins=plugins,
                statuses=status_filters,
                hard_delete=hard_delete,
            )
        if "targets" in scopes:
            target_result = store.reset_targets(
                root_domains=root_domains,
                statuses=status_filters,
                hard_delete=hard_delete,
            )
        total_affected = int((stage_result or {}).get("affected_rows") or 0) + int((target_result or {}).get("affected_rows") or 0)
        return {
            "ok": True,
            "scopes": scopes,
            "workflow_id": workflow_id,
            "root_domains": root_domains,
            "plugins": plugins,
            "statuses": status_filters,
            "hard_delete": hard_delete,
            "total_affected_rows": total_affected,
            "stage_tasks": stage_result,
            "targets": target_result,
        }

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

    @app.get("/api/coord/artifact/manifest-entries")
    def artifact_manifest_entries(
        root_domain: str = Query(default=""),
        artifact_type: str = Query(default=""),
        shard_key: str = Query(default=""),
        logical_role: str = Query(default=""),
        limit: int = Query(default=1000),
        _auth: None = Depends(require_auth),
        store: CoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return {
            "root_domain": root_domain,
            "artifact_type": artifact_type,
            "entries": store.list_artifact_manifest_entries(
                root_domain,
                artifact_type,
                shard_key=shard_key,
                logical_role=logical_role,
                limit=limit,
            ),
        }

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
