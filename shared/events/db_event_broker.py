"""Database-backed central event broker for append-only event fanout."""

from __future__ import annotations

import json
from typing import Any

from shared.schemas import EventSchema


class DbEventBroker:
    """Append validated events to the immutable event log and query fanout pages."""

    def __init__(self, connect_factory: Any) -> None:
        """Store a callable that returns a database connection context manager."""
        self._connect_factory = connect_factory

    def publish(self, event: EventSchema) -> str:
        """Persist one event idempotently and return its event identifier."""
        payload = json.dumps(event.payload, ensure_ascii=False, default=str)
        with self._connect_factory() as conn, conn.cursor() as cur:
            cur.execute(
                """
INSERT INTO coordinator_event_log(
    event_id, created_at_utc, event_type, aggregate_key, schema_version,
    source, message, idempotency_key, correlation_id, causation_id, payload_json
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
ON CONFLICT (event_id) DO NOTHING;
""",
                (
                    event.event_id,
                    event.created_at_utc,
                    event.event_type,
                    event.aggregate_key,
                    event.schema_version,
                    event.source,
                    event.message,
                    event.idempotency_key,
                    str(getattr(event, "correlation_id", "") or ""),
                    str(getattr(event, "causation_id", "") or ""),
                    payload,
                ),
            )
            conn.commit()
        return event.event_id

    def list_recent(self, *, limit: int = 250, after_sequence: int = 0) -> list[dict[str, Any]]:
        """Return recent events ordered by append sequence for SSE/WebSocket feeds."""
        requested = max(1, min(5000, int(limit or 250)))
        with self._connect_factory() as conn, conn.cursor() as cur:
            cur.execute(
                """
SELECT event_sequence, event_id, created_at_utc, event_type, aggregate_key,
       schema_version, source, message, idempotency_key, correlation_id, causation_id, payload_json
FROM coordinator_event_log
WHERE event_sequence > %s
ORDER BY event_sequence ASC
LIMIT %s;
""",
                (max(0, int(after_sequence or 0)), requested),
            )
            rows = cur.fetchall()
            conn.commit()
        return [
            {
                "event_sequence": int(row[0] or 0),
                "event_id": str(row[1] or ""),
                "created_at_utc": row[2].isoformat() if row[2] else None,
                "event_type": str(row[3] or ""),
                "aggregate_key": str(row[4] or ""),
                "schema_version": int(row[5] or 1),
                "source": str(row[6] or ""),
                "message": str(row[7] or ""),
                "idempotency_key": str(row[8] or ""),
                "correlation_id": str(row[9] or ""),
                "causation_id": str(row[10] or ""),
                "payload": row[11] if isinstance(row[11], dict) else {},
            }
            for row in rows
        ]
