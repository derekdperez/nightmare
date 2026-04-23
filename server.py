#!/usr/bin/env python3
"""FastAPI server entrypoint for the coordinator API."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

try:
    import uvicorn
except ModuleNotFoundError:  # pragma: no cover - exercised in minimal test envs
    uvicorn = None

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
    parser.add_argument("--http-port", type=int, default=0)
    parser.add_argument("--https-port", type=int, default=0)
    parser.add_argument("--cert-file", default="")
    parser.add_argument("--key-file", default="")
    parser.add_argument("--database-url", default="")
    parser.add_argument("--coordinator-api-token", default="")
    parser.add_argument("--artifact-store-root", default="")
    parser.add_argument("--reload", action="store_true")
    return parser.parse_args()


def _resolve_optional_path(raw_path: str | None) -> Path | None:
    value = str(raw_path or "").strip()
    if not value:
        return None
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (BASE_DIR / value).resolve()


def main() -> int:
    args = parse_args()

    if uvicorn is None:
        raise SystemExit("uvicorn is required to run server.py; install the server dependencies.")

    from server_app.fastapi_app import create_app
    from server_app.store import CoordinatorStore

    config_path = _resolve_config_path(args.config)
    config = _read_json_dict(config_path)
    host = str(args.host or config.get("host") or os.getenv("HOST") or DEFAULT_HOST)
    legacy_port = int(args.port or config.get("port") or os.getenv("PORT") or 0)
    http_port = int(args.http_port or config.get("http_port") or os.getenv("HTTP_PORT") or 0)
    https_port = int(args.https_port or config.get("https_port") or os.getenv("HTTPS_PORT") or 0)

    cert_file = str(
        args.cert_file
        or config.get("cert_file")
        or os.getenv("CERT_FILE")
        or os.getenv("TLS_CERT_FILE")
        or ""
    ).strip()
    key_file = str(
        args.key_file
        or config.get("key_file")
        or os.getenv("KEY_FILE")
        or os.getenv("TLS_KEY_FILE")
        or ""
    ).strip()

    port = int(legacy_port or https_port or http_port or DEFAULT_PORT)
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
    uvicorn_kwargs: dict[str, Any] = {}

    if https_port:
        cert_path = _resolve_optional_path(cert_file)
        key_path = _resolve_optional_path(key_file)
        if cert_path is None or key_path is None:
            raise SystemExit("--https-port requires both --cert-file and --key-file.")
        if not cert_path.is_file():
            raise SystemExit(f"TLS cert file not found: {cert_path}")
        if not key_path.is_file():
            raise SystemExit(f"TLS key file not found: {key_path}")
        uvicorn_kwargs["ssl_certfile"] = str(cert_path)
        uvicorn_kwargs["ssl_keyfile"] = str(key_path)

    uvicorn.run(app, host=host, port=port, reload=bool(args.reload), **uvicorn_kwargs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
