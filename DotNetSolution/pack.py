#!/usr/bin/env python3
"""Pack this project into one JSON file and one ZIP archive.

Creates ``packed.json`` and ``packed.zip`` by default, containing:
- JSON transport envelope with:
  - directory paths
  - file paths
  - file metadata
  - file contents (base64)
- ZIP archive of the actual project files

Excludes the ``output/`` directory tree.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PACK_SCHEMA_VERSION = 1
DEFAULT_OUTPUT_NAME = "packed.json"
DEFAULT_ZIP_NAME = "packed.zip"
EXCLUDED_DIR_NAMES = {"output", ".git", "bin", "obj", "debug"}


def _path_posix_rel(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _is_excluded_dir_name(name: str) -> bool:
    return name.strip().lower() in EXCLUDED_DIR_NAMES


def _should_skip_file(path: Path, root: Path, generated_outputs: set[Path]) -> bool:
    resolved = path.resolve()

    if resolved in generated_outputs:
        return True

    rel_parts = resolved.relative_to(root.resolve()).parts
    return any(_is_excluded_dir_name(part) for part in rel_parts[:-1])


def _file_record(path: Path, root: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    st = path.stat()
    return {
        "path": _path_posix_rel(path, root),
        "size_bytes": int(st.st_size),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "mode": int(st.st_mode & 0o777),
        "encoding": "base64",
        "content": base64.b64encode(raw).decode("ascii"),
    }


def build_pack_payload(root: Path, output_path: Path, zip_path: Path) -> dict[str, Any]:
    root = root.resolve()
    output_path = output_path.resolve()
    zip_path = zip_path.resolve()

    if not root.is_dir():
        raise FileNotFoundError(f"Root directory not found: {root}")

    generated_outputs = {output_path, zip_path}

    directories: list[str] = []
    files: list[dict[str, Any]] = []

    for current, dirnames, filenames in os.walk(root, topdown=True):
        current_path = Path(current)
        dirnames[:] = sorted(
            [d for d in dirnames if not _is_excluded_dir_name(d)],
            key=str.lower,
        )
        filenames.sort(key=str.lower)

        rel_current = current_path.resolve().relative_to(root)
        if rel_current.as_posix() != ".":
            directories.append(rel_current.as_posix())

        for filename in filenames:
            file_path = (current_path / filename).resolve()
            if _should_skip_file(file_path, root, generated_outputs):
                continue
            files.append(_file_record(file_path, root))

    directories = sorted(set(directories), key=str.lower)
    files.sort(key=lambda item: str(item.get("path", "")).lower())

    return {
        "pack_schema_version": PACK_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root": ".",
        "excluded_directories": sorted(EXCLUDED_DIR_NAMES),
        "directory_count": len(directories),
        "file_count": len(files),
        "directories": directories,
        "files": files,
    }


def write_zip_archive(root: Path, zip_path: Path, output_path: Path) -> tuple[int, int]:
    root = root.resolve()
    zip_path = zip_path.resolve()
    output_path = output_path.resolve()
    generated_outputs = {output_path, zip_path}

    zip_path.parent.mkdir(parents=True, exist_ok=True)

    file_count = 0
    total_bytes = 0

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for current, dirnames, filenames in os.walk(root, topdown=True):
            current_path = Path(current)
            dirnames[:] = sorted(
                [d for d in dirnames if not _is_excluded_dir_name(d)],
                key=str.lower,
            )
            filenames.sort(key=str.lower)

            for filename in filenames:
                file_path = (current_path / filename).resolve()
                if _should_skip_file(file_path, root, generated_outputs):
                    continue

                arcname = _path_posix_rel(file_path, root)
                zf.write(file_path, arcname=arcname)

                st = file_path.stat()
                file_count += 1
                total_bytes += int(st.st_size)

    return file_count, total_bytes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pack this project into one JSON file and one ZIP archive (excluding output/)."
    )
    parser.add_argument(
        "--root",
        default=".",
        help="Project root to pack (default: current directory).",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_NAME,
        help=f"Output JSON file path (default: {DEFAULT_OUTPUT_NAME}).",
    )
    parser.add_argument(
        "--zip-output",
        default=DEFAULT_ZIP_NAME,
        help=f"Output ZIP file path (default: {DEFAULT_ZIP_NAME}).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).expanduser().resolve()

    output_path = Path(args.output).expanduser()
    if not output_path.is_absolute():
        output_path = (root / output_path).resolve()
    else:
        output_path = output_path.resolve()

    zip_path = Path(args.zip_output).expanduser()
    if not zip_path.is_absolute():
        zip_path = (root / zip_path).resolve()
    else:
        zip_path = zip_path.resolve()

    payload = build_pack_payload(root, output_path, zip_path)
    inner_json_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    inner_json_bytes = inner_json_text.encode("utf-8")
    outer_payload = {
        "transport_schema_version": 1,
        "transport_encoding": "base64-json",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "payload_sha256": hashlib.sha256(inner_json_bytes).hexdigest(),
        "payload_base64": base64.b64encode(inner_json_bytes).decode("ascii"),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(outer_payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    zip_file_count, zip_total_bytes = write_zip_archive(root, zip_path, output_path)
    zip_size_bytes = zip_path.stat().st_size if zip_path.exists() else 0

    total_bytes = sum(int(item.get("size_bytes", 0) or 0) for item in payload.get("files", []))
    print(f"Packed root: {root}")
    print(f"Packed JSON: {output_path}")
    print(f"Packed ZIP: {zip_path}")
    print(f"Directories: {payload['directory_count']}")
    print(f"Files in JSON: {payload['file_count']}")
    print(f"Files in ZIP: {zip_file_count}")
    print(f"Original bytes (JSON payload source): {total_bytes}")
    print(f"Original bytes (ZIP source): {zip_total_bytes}")
    print(f"ZIP size bytes: {zip_size_bytes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())