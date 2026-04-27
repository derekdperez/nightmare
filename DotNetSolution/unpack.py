#!/usr/bin/env python3
"""Unpack a ``packed.json`` project archive into folders/files."""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
from pathlib import Path, PurePosixPath
from typing import Any


DEFAULT_INPUT_NAME = "packed.json"


def _escape_unquoted_controls_in_json_strings(text: str) -> str:
    """Escape raw control characters found inside JSON string literals."""
    out: list[str] = []
    in_string = False
    escaped = False
    for ch in text:
        if in_string:
            if escaped:
                out.append(ch)
                escaped = False
                continue
            if ch == "\\":
                out.append(ch)
                escaped = True
                continue
            if ch == '"':
                out.append(ch)
                in_string = False
                continue
            if ch == "\n":
                out.append("\\n")
                continue
            if ch == "\r":
                out.append("\\r")
                continue
            if ch == "\t":
                out.append("\\t")
                continue
            out.append(ch)
            continue
        out.append(ch)
        if ch == '"':
            in_string = True
    return "".join(out)


def _load_json_resilient(raw_text: str, context: str) -> dict[str, Any]:
    """Load JSON text and recover from common malformed control-character issues."""
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        repaired = _escape_unquoted_controls_in_json_strings(raw_text)
        try:
            payload = json.loads(repaired)
        except json.JSONDecodeError:
            if "payload_base64" in context:
                payload = _load_legacy_inner_payload(raw_text, context, exc)
            else:
                raise ValueError(f"Failed to decode JSON payload for {context}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Packed file must contain a JSON object: {context}")
    return payload


def _escape_json_string_value(value: str) -> str:
    """Return a JSON-escaped string body (without surrounding quotes)."""
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def _load_legacy_inner_payload(raw_text: str, context: str, original_exc: json.JSONDecodeError) -> dict[str, Any]:
    """Recover malformed packed payloads where file content is unescaped source text."""
    marker = '"content":"'
    if marker not in raw_text:
        raise ValueError(f"Failed to decode JSON payload for {context}: {original_exc}") from original_exc

    rebuilt: list[str] = []
    cursor = 0
    while True:
        start = raw_text.find(marker, cursor)
        if start < 0:
            rebuilt.append(raw_text[cursor:])
            break
        value_start = start + len(marker)
        rebuilt.append(raw_text[cursor:value_start])

        next_file = raw_text.find('"},{"path":"', value_start)
        next_end = raw_text.find('"}]}', value_start)
        candidates = [pos for pos in (next_file, next_end) if pos >= 0]
        if not candidates:
            raise ValueError(f"Failed to decode JSON payload for {context}: {original_exc}") from original_exc
        value_end = min(candidates)

        raw_value = raw_text[value_start:value_end]
        rebuilt.append(_escape_json_string_value(raw_value))
        cursor = value_end

    repaired = "".join(rebuilt)
    try:
        payload = json.loads(repaired)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to decode JSON payload for {context}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Packed file must contain a JSON object: {context}")
    return payload


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
    payload_outer = _load_json_resilient(path.read_text(encoding="utf-8-sig"), str(path))
    encoding = str(payload_outer.get("transport_encoding", "") or "").strip().lower()
    if encoding == "base64-json":
        b64 = str(payload_outer.get("payload_base64", "") or "")
        if not b64:
            raise ValueError("Packed transport envelope is missing payload_base64.")
        inner_raw = base64.b64decode(b64.encode("ascii"), validate=False)
        return _load_json_resilient(inner_raw.decode("utf-8"), f"{path}::payload_base64")
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
        try:
            raw = base64.b64decode(content_b64.encode("ascii"), validate=True)
        except (UnicodeEncodeError, binascii.Error):
            # Some legacy packed payloads incorrectly store raw text here.
            raw = content_b64.encode("utf-8")
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
