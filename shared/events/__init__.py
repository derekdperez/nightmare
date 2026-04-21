from __future__ import annotations

import json
import threading
from collections import deque
from pathlib import Path
from typing import Any, Iterable

from shared.models import EventRecord


class EventStream:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = threading.RLock()

    def append(self, event: EventRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(event.to_dict(), ensure_ascii=False, separators=(",", ":")) + "\n"
        with self._lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line)

    def read(self, *, limit: int | None = None, reverse: bool = False) -> list[dict[str, Any]]:
        if not self.path.is_file():
            return []
        rows: list[dict[str, Any]] = []
        if limit is not None and limit > 0:
            bucket: deque[dict[str, Any]] = deque(maxlen=int(limit))
            with self._lock:
                with self.path.open("r", encoding="utf-8", errors="ignore") as handle:
                    for raw in handle:
                        line = raw.strip()
                        if not line:
                            continue
                        try:
                            item = json.loads(line)
                        except Exception:
                            continue
                        if isinstance(item, dict):
                            bucket.append(item)
            rows = list(bucket)
        else:
            with self._lock:
                with self.path.open("r", encoding="utf-8", errors="ignore") as handle:
                    for raw in handle:
                        line = raw.strip()
                        if not line:
                            continue
                        try:
                            item = json.loads(line)
                        except Exception:
                            continue
                        if isinstance(item, dict):
                            rows.append(item)
        if reverse:
            rows.reverse()
        return rows


def build_projection(events: Iterable[dict[str, Any]], *, key_field: str = "aggregate_key") -> dict[str, dict[str, Any]]:
    projection: dict[str, dict[str, Any]] = {}
    for event in events:
        aggregate = str(event.get(key_field) or "").strip()
        if not aggregate:
            continue
        bucket = projection.setdefault(aggregate, {})
        bucket["last_event_type"] = str(event.get("event_type") or "")
        bucket["updated_at"] = event.get("created_at")
        payload = event.get("payload")
        if isinstance(payload, dict):
            bucket.update(payload)
    return projection
