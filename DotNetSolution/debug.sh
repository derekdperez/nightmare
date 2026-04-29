#!/usr/bin/env bash
# Run from DotNetSolution/: ./debug.sh
# Executes lines from .ai-debug/debug_commands.sh and writes debug_results.json
# Requires python3 on PATH (used for safe JSON encoding of arbitrary command output).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

COMMANDS_FILE="${REPO_ROOT}/.ai-debug/debug_commands.sh"
OUTPUT_FILE="${REPO_ROOT}/debug_results.json"

if ! command -v python3 >/dev/null 2>&1; then
  cat >&2 <<'EOF'
debug.sh: python3 is required to generate safe JSON in debug_results.json.
Install Python 3 and ensure 'python3' is on PATH, then re-run ./debug.sh
EOF
  exit 1
fi

export DEBUG_COMMANDS_FILE="$COMMANDS_FILE"
export DEBUG_OUTPUT_FILE="$OUTPUT_FILE"

exec python3 - <<'PY'
import json
import os
import subprocess
from datetime import datetime, timezone

def utc_iso_ms(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    ms = dt.microsecond // 1000
    return f"{base}.{ms:03d}Z"

commands_path = os.environ.get("DEBUG_COMMANDS_FILE", "")
output_path = os.environ.get("DEBUG_OUTPUT_FILE", "")
repo_root = os.path.dirname(output_path) or "."

results: list[dict] = []
lines: list[str] = []
if commands_path and os.path.isfile(commands_path):
    with open(commands_path, encoding="utf-8", errors="replace") as handle:
        lines = handle.read().splitlines()

for raw in lines:
    stripped = raw.strip()
    if not stripped or stripped.lstrip().startswith("#"):
        continue
    cmd = raw.rstrip("\n")
    t0 = datetime.now(timezone.utc)
    try:
        proc = subprocess.run(
            ["/bin/sh", "-lc", cmd],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=600,
            env=os.environ.copy(),
        )
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        code = int(proc.returncode)
    except subprocess.TimeoutExpired as exc:
        if isinstance(exc.stdout, str):
            stdout = exc.stdout or ""
        else:
            stdout = exc.stdout.decode("utf-8", errors="replace") if exc.stdout else ""
        if isinstance(exc.stderr, str):
            stderr = exc.stderr or ""
        else:
            stderr = exc.stderr.decode("utf-8", errors="replace") if exc.stderr else ""
        stderr += "\n[debug.sh] command timed out after 600s\n"
        code = 124
    except Exception as exc:  # noqa: BLE001
        stdout = ""
        stderr = f"[debug.sh] failed to run command: {exc!s}\n"
        code = 125
    t1 = datetime.now(timezone.utc)
    duration_ms = int((t1 - t0).total_seconds() * 1000)
    results.append(
        {
            "command": cmd,
            "exit_code": code,
            "stdout": stdout,
            "stderr": stderr,
            "started_at": utc_iso_ms(t0),
            "finished_at": utc_iso_ms(t1),
            "duration_ms": duration_ms,
        }
    )

with open(output_path, "w", encoding="utf-8") as outf:
    json.dump(results, outf, ensure_ascii=False, indent=2)
    outf.write("\n")
PY
