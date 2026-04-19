#!/usr/bin/env python3
"""Register targets into the central coordinator queue."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from http_client import request_json
from nightmare_shared.config import load_env_file_into_os, normalize_server_base_url


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DEPLOY_ENV_FILE = BASE_DIR / "deploy" / ".env"


def _read_targets(path: Path) -> list[str]:
    raw = path.read_text(encoding="utf-8-sig")
    out: list[str] = []
    for line in raw.splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        out.append(value)
    return out


def _post_json(base_url: str, token: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if token.strip():
        headers["Authorization"] = f"Bearer {token.strip()}"
    return request_json(
        "POST",
        f"{base_url.rstrip('/')}{path}",
        headers=headers,
        json_payload=payload,
        timeout_seconds=30.0,
        verify=False,
        user_agent="nightmare-register-targets/1.0",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    # Keep CLI defaults aligned with central deploy conventions.
    load_env_file_into_os(DEFAULT_DEPLOY_ENV_FILE, override=False)
    p = argparse.ArgumentParser(description="Register targets into coordinator queue")
    p.add_argument(
        "--server-base-url",
        default=os.getenv("COORDINATOR_BASE_URL", ""),
        help="Coordinator server base URL (defaults to COORDINATOR_BASE_URL or deploy/.env)",
    )
    p.add_argument(
        "--api-token",
        default=os.getenv("COORDINATOR_API_TOKEN", ""),
        help="Coordinator API token (defaults to COORDINATOR_API_TOKEN or deploy/.env)",
    )
    p.add_argument("--targets-file", default="targets.txt", help="Targets text file")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    base_url = normalize_server_base_url(args.server_base_url)
    if not base_url:
        raise ValueError("server base URL is required (use --server-base-url or COORDINATOR_BASE_URL in deploy/.env)")
    targets = _read_targets(Path(args.targets_file).expanduser().resolve())
    rsp = _post_json(
        base_url,
        args.api_token,
        "/api/coord/register-targets",
        {"targets": targets},
    )
    print(json.dumps(rsp, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

