from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class _PendingFile:
    path: Path
    mode: str
    records: list[Any] = field(default_factory=list)
    last_updated: float = field(default_factory=time.time)


class BatchedArtifactWriter:
    def __init__(self, *, flush_count: int = 100, flush_bytes: int = 1_000_000, flush_interval_seconds: float = 2.0):
        self.flush_count = max(1, int(flush_count))
        self.flush_bytes = max(1, int(flush_bytes))
        self.flush_interval_seconds = max(0.1, float(flush_interval_seconds))
        self._lock = threading.RLock()
        self._pending: dict[str, _PendingFile] = {}

    def queue_json(self, path: str | os.PathLike[str], payload: dict[str, Any]) -> None:
        self._queue(Path(path), "json", payload)

    def queue_ndjson(self, path: str | os.PathLike[str], row: dict[str, Any]) -> None:
        self._queue(Path(path), "ndjson", row)

    def _queue(self, path: Path, mode: str, item: Any) -> None:
        with self._lock:
            entry = self._pending.setdefault(str(path), _PendingFile(path=path, mode=mode))
            if entry.mode != mode:
                raise ValueError(f"Mismatched queued write mode for {path}")
            entry.records.append(item)
            entry.last_updated = time.time()
            if self._should_flush(entry):
                self._flush_one(entry)
                self._pending.pop(str(path), None)

    def _should_flush(self, entry: _PendingFile) -> bool:
        approx_bytes = sum(len(json.dumps(item, ensure_ascii=False)) for item in entry.records)
        return len(entry.records) >= self.flush_count or approx_bytes >= self.flush_bytes or (time.time() - entry.last_updated) >= self.flush_interval_seconds

    def flush(self) -> None:
        with self._lock:
            items = list(self._pending.values())
            self._pending.clear()
        for entry in items:
            self._flush_one(entry)

    def _flush_one(self, entry: _PendingFile) -> None:
        entry.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=entry.path.name + ".", suffix=".tmp", dir=str(entry.path.parent))
        try:
            existing = b""
            if entry.mode == "ndjson" and entry.path.exists():
                existing = entry.path.read_bytes()
            with os.fdopen(fd, "wb") as handle:
                if entry.mode == "json":
                    payload = entry.records[-1] if entry.records else {}
                    handle.write(json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"))
                else:
                    handle.write(existing)
                    for row in entry.records:
                        handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                        handle.write(b"\n")
            os.replace(tmp_name, entry.path)
        finally:
            try:
                if os.path.exists(tmp_name):
                    os.unlink(tmp_name)
            except Exception:
                pass
