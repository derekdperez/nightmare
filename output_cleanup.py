"""Remove crawl output under a directory (optional preserved entry names)."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

# Persisted on workers so local disk clear survives wiping output/.
FLEET_GEN_APPLIED_FILENAME = ".coordinator_fleet_generation_applied"


def clear_output_root_children(
    output_root: Path,
    *,
    preserve_names: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Remove direct children under output_root. Keeps output_root itself."""
    removed_dirs = 0
    removed_files = 0
    errors: list[str] = []
    if not output_root.is_dir():
        return {"skipped": True, "reason": "not_a_directory", "path": str(output_root)}
    preserve = preserve_names or frozenset()
    for child in sorted(output_root.iterdir(), key=lambda p: p.name):
        if child.name in preserve:
            continue
        try:
            if child.is_dir():
                shutil.rmtree(child)
                removed_dirs += 1
            elif child.is_file() or child.is_symlink():
                child.unlink(missing_ok=True)
                removed_files += 1
        except OSError as exc:
            errors.append(f"{child.name}: {exc}")
    return {
        "output_root": str(output_root.resolve()),
        "removed_directories": removed_dirs,
        "removed_files": removed_files,
        "errors": errors or None,
    }
