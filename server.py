#!/usr/bin/env python3
"""FastAPI server entrypoint for the coordinator API."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import uvicorn

from server_app.fastapi_app import create_app
from server_app.store import CoordinatorStore

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "server.json"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _resolve_config_path(raw_path: str | None) -> Path:
    value = str(raw_path or "").strip()
    if not value:
        return DEFAULT_CONFIG_PATH
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (BASE_DIR / value).resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Nightmare coordinator API with FastAPI.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--host", default="")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--database-url", default="")
    parser.add_argument("--coordinator-api-token", default="")
    parser.add_argument("--artifact-store-root", default="")
    parser.add_argument("--reload", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = _resolve_config_path(args.config)
    config = _read_json_dict(config_path)
    host = str(args.host or config.get("host") or os.getenv("HOST") or DEFAULT_HOST)
    port = int(args.port or config.get("http_port") or os.getenv("PORT") or DEFAULT_PORT)
    database_url = str(args.database_url or config.get("database_url") or os.getenv("DATABASE_URL") or "").strip()
    coordinator_api_token = str(
        args.coordinator_api_token
        or config.get("coordinator_api_token")
        or os.getenv("COORDINATOR_API_TOKEN")
        or ""
    ).strip()
    artifact_store_root = str(
        args.artifact_store_root
        or config.get("artifact_store_root")
        or os.getenv("NIGHTMARE_ARTIFACT_STORE_ROOT")
        or ""
    ).strip()

    store = CoordinatorStore(database_url, artifact_store_root=artifact_store_root) if database_url else None
    app = create_app(coordinator_store=store, coordinator_api_token=coordinator_api_token)
    uvicorn.run(app, host=host, port=port, reload=bool(args.reload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
