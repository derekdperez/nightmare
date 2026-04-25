"""Command execution actions for system workflows."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any


def run_command(command: list[str], *, cwd: str | Path | None = None, timeout_seconds: int = 300) -> dict[str, Any]:
    """Execute a command and return a structured result object."""
    if not isinstance(command, list) or not command:
        raise ValueError("command must be a non-empty list")
    completed = subprocess.run(
        [str(part) for part in command],
        cwd=str(cwd) if cwd else None,
        timeout=max(1, int(timeout_seconds or 300)),
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "ok": int(completed.returncode) == 0,
        "exit_code": int(completed.returncode),
        "stdout": str(completed.stdout or ""),
        "stderr": str(completed.stderr or ""),
    }
