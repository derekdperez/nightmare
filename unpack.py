#!/usr/bin/env python3
"""Unpack a ``packed.json`` project archive into folders/files."""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path, PurePosixPath
from typing import Any


DEFAULT_INPUT_NAME = "packed.json"


def _safe_rel_path(path_text: str) -> PurePosixPath:
    rel = PurePosixPath(str(path_text or "").strip())
    if rel.is_absolute():
        raise ValueError(f"Absolute paths are not allowed in pack: {path_text!r}")
    if any(part == ".." for part in rel.parts):
        raise ValueError(f"Parent path traversal is not allowed in pack: {path_text!r}")
    if str(rel) in {"", "."}:
        return PurePosixPath(".")
    return rel


def _read_pack(path: Path) -> dict[str, Any]:
    payload_outer = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload_outer, dict):
        raise ValueError(f"Packed file must contain a JSON object: {path}")
    encoding = str(payload_outer.get("transport_encoding", "") or "").strip().lower()
    if encoding == "base64-json":
        b64 = str(payload_outer.get("payload_base64", "") or "")
        if not b64:
            raise ValueError("Packed transport envelope is missing payload_base64.")
        inner_raw = base64.b64decode(b64.encode("ascii"), validate=False)
        inner_payload = json.loads(inner_raw.decode("utf-8"))
        if not isinstance(inner_payload, dict):
            raise ValueError("Decoded packed payload must be a JSON object.")
        return inner_payload
    # Backward-compatible fallback for older non-envelope packed files.
    return payload_outer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unpack a packed.json archive into project files.")
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_NAME,
        help=f"Packed JSON file path (default: {DEFAULT_INPUT_NAME}).",
    )
    parser.add_argument(
        "--target",
        default=".",
        help="Destination directory to unpack into (default: current directory).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    packed_path = Path(args.input).expanduser().resolve()
    target_root = Path(args.target).expanduser().resolve()
    if not packed_path.is_file():
        raise FileNotFoundError(f"Packed file not found: {packed_path}")
    target_root.mkdir(parents=True, exist_ok=True)

    payload = _read_pack(packed_path)
    directories = payload.get("directories", [])
    files = payload.get("files", [])
    if not isinstance(directories, list) or not isinstance(files, list):
        raise ValueError("Packed JSON is missing required 'directories'/'files' lists.")

    created_dirs = 0
    written_files = 0

    for item in directories:
        rel = _safe_rel_path(str(item))
        if str(rel) in {"", "."}:
            continue
        out_dir = (target_root / Path(*rel.parts)).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        created_dirs += 1

    for record in files:
        if not isinstance(record, dict):
            continue
        rel = _safe_rel_path(str(record.get("path", "")))
        if str(rel) in {"", "."}:
            continue
        encoding = str(record.get("encoding", "") or "").strip().lower()
        if encoding != "base64":
            raise ValueError(f"Unsupported file encoding in pack: {encoding!r}")
        content_b64 = str(record.get("content", "") or "")
        raw = base64.b64decode(content_b64.encode("ascii"), validate=False)
        out_path = (target_root / Path(*rel.parts)).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(raw)

        mode_raw = record.get("mode")
        try:
            mode = int(mode_raw)
        except (TypeError, ValueError):
            mode = None
        if mode is not None:
            try:
                os.chmod(out_path, mode)
            except OSError:
                pass
        written_files += 1

    print(f"Unpacked from: {packed_path}")
    print(f"Target root: {target_root}")
    print(f"Directories created: {created_dirs}")
    print(f"Files written: {written_files}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
