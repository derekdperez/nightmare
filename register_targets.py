#!/usr/bin/env python3
"""Register targets into the central coordinator queue."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from http_client import request_json


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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Register targets into coordinator queue")
    p.add_argument("--server-base-url", required=True, help="Coordinator server base URL")
    p.add_argument("--api-token", default="", help="Coordinator API token")
    p.add_argument("--targets-file", default="targets.txt", help="Targets text file")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    targets = _read_targets(Path(args.targets_file).expanduser().resolve())
    rsp = _post_json(
        args.server_base_url,
        args.api_token,
        "/api/coord/register-targets",
        {"targets": targets},
    )
    print(json.dumps(rsp, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

