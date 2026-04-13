#!/usr/bin/env python3
"""Operator client for quick centralized status checks."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).resolve().parent


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    out: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def _http_get_json(base_url: str, path: str, token: str = "", query: dict[str, Any] | None = None) -> dict[str, Any]:
    q = urlencode(query or {})
    suffix = f"{path}?{q}" if q else path
    url = f"{base_url.rstrip('/')}{suffix}"
    headers: dict[str, str] = {}
    if token.strip():
        headers["Authorization"] = f"Bearer {token.strip()}"
    req = Request(url=url, method="GET", headers=headers)
    try:
        with urlopen(req, timeout=30) as rsp:
            raw = rsp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} GET {suffix}: {body[:400]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error GET {suffix}: {exc}") from exc
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        parsed = {}
    return parsed if isinstance(parsed, dict) else {}


def _run_aws_json(args: list[str]) -> dict[str, Any]:
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(err or f"aws command failed ({proc.returncode})")
    text = (proc.stdout or "").strip() or "{}"
    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise RuntimeError(f"aws returned non-JSON output: {text[:300]}") from exc
    return parsed if isinstance(parsed, dict) else {}


def _run_aws_text(args: list[str]) -> str:
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(err or f"aws command failed ({proc.returncode})")
    return (proc.stdout or "").strip()


def _print_coordinator_worker_status(payload: dict[str, Any]) -> None:
    counts = payload.get("counts", {}) if isinstance(payload.get("counts"), dict) else {}
    workers = payload.get("workers", []) if isinstance(payload.get("workers"), list) else []
    print("[coordinator]")
    print(
        "workers total={total} online={online} stale={stale} generated_at={ts}".format(
            total=int(counts.get("total_workers", 0) or 0),
            online=int(counts.get("online_workers", 0) or 0),
            stale=int(counts.get("stale_workers", 0) or 0),
            ts=str(payload.get("generated_at_utc", "")),
        )
    )
    print("worker_id | status | age_s | run_targets | run_stage_tasks | active_stages")
    for item in workers:
        if not isinstance(item, dict):
            continue
        wid = str(item.get("worker_id", "") or "")
        status = str(item.get("status", "unknown") or "unknown")
        age = item.get("seconds_since_heartbeat")
        age_text = str(int(age)) if isinstance(age, int) else "-"
        run_targets = int(item.get("running_targets", 0) or 0)
        run_stage_tasks = int(item.get("running_stage_tasks", 0) or 0)
        active_stages_raw = item.get("active_stages", [])
        active_stages = ",".join(str(x) for x in active_stages_raw) if isinstance(active_stages_raw, list) else ""
        print(f"{wid} | {status} | {age_text} | {run_targets} | {run_stage_tasks} | {active_stages}")


def _parse_compose_ps_stdout(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return "no output"
    try:
        parsed = json.loads(text)
    except Exception:
        one_line = " ".join(text.split())
        return one_line[:220]

    entries: list[dict[str, Any]]
    if isinstance(parsed, list):
        entries = [x for x in parsed if isinstance(x, dict)]
    elif isinstance(parsed, dict):
        entries = [parsed]
    else:
        return "unexpected output"
    if not entries:
        return "no containers"

    parts: list[str] = []
    for row in entries:
        name = str(row.get("Name", row.get("name", "?")) or "?")
        state = str(row.get("State", row.get("state", "?")) or "?")
        service = str(row.get("Service", row.get("service", "?")) or "?")
        parts.append(f"{service}:{name}:{state}")
    return "; ".join(parts)


def _run_ssm_worker_status(
    *,
    region: str,
    target_key: str,
    target_values: str,
    timeout_seconds: int,
) -> int:
    if not shutil.which("aws"):
        print("[ssm]")
        print("aws CLI not found; skipping VM container status checks.")
        return 0

    commands_payload = 'commands=["cd /opt/nightmare/deploy && docker compose -f docker-compose.worker.yml --env-file .env ps --format json"]'
    send_cmd = [
        "aws",
        "ssm",
        "send-command",
        "--document-name",
        "AWS-RunShellScript",
        "--targets",
        f"Key={target_key},Values={target_values}",
        "--parameters",
        commands_payload,
        "--comment",
        "nightmare worker status check",
        "--region",
        region,
        "--query",
        "Command.CommandId",
        "--output",
        "text",
    ]
    command_id = _run_aws_text(send_cmd)
    if not command_id:
        raise RuntimeError("send-command did not return a command id")

    deadline = time.time() + max(10, int(timeout_seconds))
    terminal = {
        "Success",
        "Cancelled",
        "TimedOut",
        "Failed",
        "Cancelling",
        "Undeliverable",
        "Terminated",
        "InvalidPlatform",
        "AccessDenied",
        "Delivery Timed Out",
        "Execution Timed Out",
    }
    list_inv_cmd = [
        "aws",
        "ssm",
        "list-command-invocations",
        "--command-id",
        command_id,
        "--details",
        "--region",
        region,
        "--output",
        "json",
    ]

    invocations: list[dict[str, Any]] = []
    while time.time() < deadline:
        payload = _run_aws_json(list_inv_cmd)
        rows = payload.get("CommandInvocations", [])
        invocations = rows if isinstance(rows, list) else []
        if invocations and all(str(x.get("Status", "")) in terminal for x in invocations if isinstance(x, dict)):
            break
        time.sleep(2.0)

    print("[ssm]")
    print(f"command_id={command_id} region={region} targets={target_key}:{target_values}")
    if not invocations:
        print("no invocations returned yet")
        return 0
    print("instance_id | ssm_status | response_code | worker_container_state")

    for inv in invocations:
        if not isinstance(inv, dict):
            continue
        instance_id = str(inv.get("InstanceId", "") or "")
        status = str(inv.get("Status", "unknown") or "unknown")
        plugin_rows = inv.get("CommandPlugins", [])
        response_code = ""
        raw_out = ""
        if isinstance(plugin_rows, list) and plugin_rows and isinstance(plugin_rows[0], dict):
            response_code = str(plugin_rows[0].get("ResponseCode", ""))
            raw_out = str(plugin_rows[0].get("Output", "") or "")
        parsed_summary = _parse_compose_ps_stdout(raw_out)
        print(f"{instance_id} | {status} | {response_code} | {parsed_summary}")
    return 0


def _run_ssm_rollout(
    *,
    region: str,
    target_key: str,
    target_values: str,
    timeout_seconds: int,
    repo_dir: str,
    branch: str,
) -> int:
    if not shutil.which("aws"):
        print("[rollout]")
        print("aws CLI not found; cannot run remote worker rollout.")
        return 1

    safe_repo_dir = str(repo_dir or "").strip() or "/opt/nightmare"
    safe_branch = str(branch or "").strip() or "main"
    repo_dir_q = shlex.quote(safe_repo_dir)
    branch_q = shlex.quote(safe_branch)
    rollout_script = (
        f"set -e; "
        f"cd {repo_dir_q}; "
        f"git fetch --all --prune; "
        f"git checkout {branch_q}; "
        f"git pull --ff-only origin {branch_q}; "
        f"cd deploy; "
        f"if docker compose version >/dev/null 2>&1; then COMPOSE_CMD='docker compose'; elif command -v docker-compose >/dev/null 2>&1; then COMPOSE_CMD='docker-compose'; else echo 'docker compose not found'; exit 1; fi; "
        f"$COMPOSE_CMD -f docker-compose.worker.yml --env-file .env up -d --build; "
        f"$COMPOSE_CMD -f docker-compose.worker.yml --env-file .env ps --format json"
    )
    commands_payload = json.dumps({"commands": [rollout_script]}, ensure_ascii=False, separators=(",", ":"))
    send_cmd = [
        "aws",
        "ssm",
        "send-command",
        "--document-name",
        "AWS-RunShellScript",
        "--targets",
        f"Key={target_key},Values={target_values}",
        "--parameters",
        commands_payload,
        "--comment",
        f"nightmare worker rollout branch={safe_branch}",
        "--region",
        region,
        "--query",
        "Command.CommandId",
        "--output",
        "text",
    ]
    command_id = _run_aws_text(send_cmd)
    if not command_id:
        raise RuntimeError("send-command did not return a command id")

    deadline = time.time() + max(10, int(timeout_seconds))
    terminal = {
        "Success",
        "Cancelled",
        "TimedOut",
        "Failed",
        "Cancelling",
        "Undeliverable",
        "Terminated",
        "InvalidPlatform",
        "AccessDenied",
        "Delivery Timed Out",
        "Execution Timed Out",
    }
    list_inv_cmd = [
        "aws",
        "ssm",
        "list-command-invocations",
        "--command-id",
        command_id,
        "--details",
        "--region",
        region,
        "--output",
        "json",
    ]

    invocations: list[dict[str, Any]] = []
    while time.time() < deadline:
        payload = _run_aws_json(list_inv_cmd)
        rows = payload.get("CommandInvocations", [])
        invocations = rows if isinstance(rows, list) else []
        if invocations and all(str(x.get("Status", "")) in terminal for x in invocations if isinstance(x, dict)):
            break
        time.sleep(2.0)

    print("[rollout]")
    print(
        f"command_id={command_id} region={region} targets={target_key}:{target_values} "
        f"repo_dir={safe_repo_dir} branch={safe_branch}"
    )
    if not invocations:
        print("no invocations returned yet")
        return 1
    print("instance_id | ssm_status | response_code | worker_container_state")

    all_ok = True
    for inv in invocations:
        if not isinstance(inv, dict):
            continue
        instance_id = str(inv.get("InstanceId", "") or "")
        status = str(inv.get("Status", "unknown") or "unknown")
        plugin_rows = inv.get("CommandPlugins", [])
        response_code = ""
        raw_out = ""
        if isinstance(plugin_rows, list) and plugin_rows and isinstance(plugin_rows[0], dict):
            response_code = str(plugin_rows[0].get("ResponseCode", ""))
            raw_out = str(plugin_rows[0].get("Output", "") or "")
        parsed_summary = _parse_compose_ps_stdout(raw_out)
        print(f"{instance_id} | {status} | {response_code} | {parsed_summary}")
        if status != "Success" or response_code not in {"0", ""}:
            all_ok = False
    return 0 if all_ok else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env_from_file = _read_env_file(BASE_DIR / "deploy" / ".env")
    p = argparse.ArgumentParser(description="Central operator client for worker and VM status checks")
    p.add_argument("action", nargs="?", default="status", choices=["status", "rollout"], help="Action to run")
    p.add_argument("--server-base-url", default=os.getenv("COORDINATOR_BASE_URL", env_from_file.get("COORDINATOR_BASE_URL", "")), help="Coordinator server base URL")
    p.add_argument("--api-token", default=os.getenv("COORDINATOR_API_TOKEN", env_from_file.get("COORDINATOR_API_TOKEN", "")), help="Coordinator API token")
    p.add_argument("--stale-after-seconds", type=int, default=180, help="Heartbeat freshness window for online/stale worker status")
    p.add_argument("--skip-ssm", action="store_true", help="Skip AWS SSM per-VM docker status checks")
    p.add_argument("--aws-region", default=os.getenv("AWS_REGION", env_from_file.get("AWS_REGION", "us-east-1")), help="AWS region for SSM checks")
    p.add_argument("--ssm-target-key", default="tag:Name", help="SSM target key")
    p.add_argument("--ssm-target-values", default="nightmare-worker*", help="SSM target values expression")
    p.add_argument("--ssm-timeout-seconds", type=int, default=60, help="SSM status poll timeout")
    p.add_argument("--repo-dir", default="/opt/nightmare", help="Worker repository directory for rollout")
    p.add_argument("--branch", default="main", help="Git branch to deploy during rollout")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.action not in {"status", "rollout"}:
        raise ValueError(f"unsupported action: {args.action}")

    base_url = str(args.server_base_url or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("server base URL is required (use --server-base-url or COORDINATOR_BASE_URL)")

    token = str(args.api_token or "").strip()
    workers_payload = _http_get_json(
        base_url=base_url,
        path="/api/coord/workers",
        token=token,
        query={"stale_after_seconds": max(15, int(args.stale_after_seconds or 180))},
    )
    _print_coordinator_worker_status(workers_payload)

    if args.action == "status" and not args.skip_ssm:
        _run_ssm_worker_status(
            region=str(args.aws_region or "us-east-1"),
            target_key=str(args.ssm_target_key or "tag:Name"),
            target_values=str(args.ssm_target_values or "nightmare-worker*"),
            timeout_seconds=max(10, int(args.ssm_timeout_seconds or 60)),
        )
    if args.action == "rollout":
        return _run_ssm_rollout(
            region=str(args.aws_region or "us-east-1"),
            target_key=str(args.ssm_target_key or "tag:Name"),
            target_values=str(args.ssm_target_values or "nightmare-worker*"),
            timeout_seconds=max(10, int(args.ssm_timeout_seconds or 60)),
            repo_dir=str(args.repo_dir or "/opt/nightmare"),
            branch=str(args.branch or "main"),
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
