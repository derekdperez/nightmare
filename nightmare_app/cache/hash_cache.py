from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Iterable


def compute_signature(*, content: bytes | None = None, path: str | os.PathLike[str] | None = None, dependencies: Iterable[str] | None = None) -> str:
    hasher = hashlib.sha256()
    if content is not None:
        hasher.update(content)
    if path is not None:
        p = Path(path)
        if p.exists():
            stat = p.stat()
            hasher.update(str(p.resolve()).encode("utf-8"))
            hasher.update(str(stat.st_size).encode("ascii"))
            hasher.update(str(stat.st_mtime_ns).encode("ascii"))
    for item in sorted(str(dep) for dep in (dependencies or [])):
        hasher.update(item.encode("utf-8"))
    return hasher.hexdigest()


class HashCache:
    def __init__(self, path: str | os.PathLike[str], *, ttl_seconds: float | None = None):
        self.path = Path(path)
        self.ttl_seconds = float(ttl_seconds) if ttl_seconds is not None else None
        self._lock = threading.RLock()
        self._cache = self._load()
        self.metrics = {"hit": 0, "miss": 0, "eviction": 0}

    def _load(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._cache, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.path)

    def get(self, key: str, signature: str) -> Any | None:
        with self._lock:
            entry = self._cache.get(key)
            if not isinstance(entry, dict):
                self.metrics["miss"] += 1
                return None
            if str(entry.get("signature") or "") != str(signature):
                self.metrics["miss"] += 1
                return None
            created_at = float(entry.get("created_at") or 0.0)
            if self.ttl_seconds is not None and created_at and (time.time() - created_at) > self.ttl_seconds:
                self.metrics["eviction"] += 1
                self._cache.pop(key, None)
                return None
            self.metrics["hit"] += 1
            return entry.get("value")

    def put(self, key: str, signature: str, value: Any) -> None:
        with self._lock:
            self._cache[key] = {"signature": signature, "value": value, "created_at": time.time()}
            self._save()

    def debug_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {"entries": len(self._cache), "metrics": dict(self.metrics)}
