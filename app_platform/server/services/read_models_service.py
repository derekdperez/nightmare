#!/usr/bin/env python3
"""Read-model queries for coordinator dashboards and APIs."""

from __future__ import annotations

from typing import Any, Callable


class ReadModelsService:
    """Encapsulate read-only coordinator projections and metrics queries."""

    def __init__(
        self,
        *,
        connect_factory: Callable[[], Any],
        now_iso: Callable[[], str],
        json_safe_db_value: Callable[[Any], Any],
        quote_ident: Callable[[str], str],
        build_database_preview_expr: Callable[[str, str, int], str],
        pick_recent_order_column: Callable[[list[tuple[str, str, str]]], str | None],
    ) -> None:
        self._connect = connect_factory
        self._now_iso = now_iso
        self._json_safe_db_value = json_safe_db_value
        self._quote_ident = quote_ident
        self._build_database_preview_expr = build_database_preview_expr
        self._pick_recent_order_column = pick_recent_order_column

    def list_events(
        self,
        *,
        limit: int = 250,
        offset: int = 0,
        search: str = "",
        event_type: str = "",
        aggregate_key: str = "",
        source: str = "",
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        requested = max(1, min(5000, int(limit or 250)))
        skip = max(0, int(offset or 0))
        filters: list[str] = []
        params: list[Any] = []
        if event_type:
            filters.append("LOWER(event_type) LIKE %s")
            params.append(f"%{str(event_type).strip().lower()}%")
        if aggregate_key:
            filters.append("LOWER(aggregate_key) LIKE %s")
            params.append(f"%{str(aggregate_key).strip().lower()}%")
        if source:
            filters.append("LOWER(source) LIKE %s")
            params.append(f"%{str(source).strip().lower()}%")
        if search:
            needle = f"%{str(search).strip().lower()}%"
            filters.append(
                "(LOWER(event_type) LIKE %s OR LOWER(aggregate_key) LIKE %s OR LOWER(source) LIKE %s OR LOWER(message) LIKE %s OR LOWER(payload_json::text) LIKE %s)"
            )
            params.extend([needle, needle, needle, needle, needle])
        where = "WHERE " + " AND ".join(filters) if filters else ""
        order = "ASC" if str(sort_dir or "desc").strip().lower() == "asc" else "DESC"
        count_sql = f"SELECT COUNT(*) FROM coordinator_recent_events {where};"
        list_sql = f"""
SELECT event_id, created_at_utc, event_type, aggregate_key, schema_version, source, message, payload_json
FROM coordinator_recent_events
{where}
ORDER BY created_at_utc {order}, event_id {order}
LIMIT %s OFFSET %s;
"""
        page: list[dict[str, Any]] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total_row = cur.fetchone()
                cur.execute(list_sql, [*params, requested, skip])
                rows = cur.fetchall()
            conn.commit()
        for row in rows:
            page.append(
                {
                    "event_id": str(row[0] or ""),
                    "created_at": row[1].isoformat() if row[1] else None,
                    "event_type": str(row[2] or ""),
                    "aggregate_key": str(row[3] or ""),
                    "schema_version": int(row[4] or 1),
                    "source": str(row[5] or ""),
                    "message": str(row[6] or ""),
                    "payload": row[7] if isinstance(row[7], dict) else {},
                }
            )
        return {
            "generated_at_utc": self._now_iso(),
            "total": int(total_row[0] or 0) if total_row else 0,
            "offset": skip,
            "limit": requested,
            "events": page,
            "source": "coordinator_recent_events",
        }

    def database_status(self) -> dict[str, Any]:
        max_rows_per_table = 20
        max_text_preview_chars = 4096
        tables_sql = """
SELECT
  n.nspname AS table_schema,
  c.relname AS table_name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname;
"""
        columns_sql = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
ORDER BY ordinal_position;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user, version(), NOW();")
                db_row = cur.fetchone()
                cur.execute(tables_sql)
                table_rows = cur.fetchall()
                tables: list[dict[str, Any]] = []
                for schema_name, table_name in table_rows:
                    safe_ident = f'{self._quote_ident(schema_name)}.{self._quote_ident(table_name)}'
                    try:
                        cur.execute(columns_sql, (schema_name, table_name))
                        column_rows = cur.fetchall()
                        columns = [
                            {
                                "name": col_name,
                                "data_type": data_type,
                                "nullable": str(is_nullable).upper() == "YES",
                            }
                            for col_name, data_type, is_nullable in column_rows
                        ]
                        select_exprs = [
                            self._build_database_preview_expr(str(col_name), str(data_type), max_text_preview_chars)
                            for col_name, data_type, _is_nullable in column_rows
                        ]
                        select_list = ", ".join(select_exprs) if select_exprs else "NULL"
                        order_col = self._pick_recent_order_column(column_rows)
                        order_clause = f" ORDER BY {self._quote_ident(order_col)} DESC" if order_col else ""
                        cur.execute(f"SELECT COUNT(*) FROM {safe_ident};")
                        row_count_value = int((cur.fetchone() or [0])[0] or 0)
                        cur.execute(
                            f"SELECT {select_list} FROM {safe_ident}{order_clause} LIMIT %s;",
                            (max_rows_per_table,),
                        )
                        rows = cur.fetchall()
                        colnames = [desc[0] for desc in cur.description]
                        serialized_rows: list[dict[str, Any]] = []
                        for row in rows:
                            serialized_rows.append(
                                {colnames[idx]: self._json_safe_db_value(value) for idx, value in enumerate(row)}
                            )
                        tables.append(
                            {
                                "schema": schema_name,
                                "name": table_name,
                                "row_count": row_count_value,
                                "row_count_is_estimate": False,
                                "rows_returned": len(serialized_rows),
                                "rows_limited": row_count_value > len(serialized_rows),
                                "columns": columns,
                                "rows": serialized_rows,
                            }
                        )
                    except Exception as exc:
                        tables.append(
                            {
                                "schema": schema_name,
                                "name": table_name,
                                "row_count": 0,
                                "row_count_is_estimate": False,
                                "rows_returned": 0,
                                "rows_limited": False,
                                "columns": [],
                                "rows": [],
                                "table_error": str(exc),
                            }
                        )
            conn.commit()
        return {
            "database": {
                "name": db_row[0],
                "current_user": db_row[1],
                "version": db_row[2],
                "server_time_utc": self._json_safe_db_value(db_row[3]),
            },
            "table_count": len(tables),
            "max_rows_per_table": max_rows_per_table,
            "max_text_preview_chars": max_text_preview_chars,
            "tables": tables,
            "generated_at_utc": self._now_iso(),
        }

    def database_activity(self, *, limit: int = 500) -> dict[str, Any]:
        requested = max(1, min(2000, int(limit or 500)))
        sql = """
SELECT
  n.nspname AS schema_name,
  c.relname AS table_name,
  COALESCE(s.n_live_tup, 0)::bigint AS rows_estimate,
  COALESCE(col.column_count, 0)::int AS column_count,
  pg_total_relation_size(c.oid) AS table_size_bytes,
  GREATEST(
    COALESCE(s.last_vacuum, 'epoch'::timestamptz),
    COALESCE(s.last_autovacuum, 'epoch'::timestamptz),
    COALESCE(s.last_analyze, 'epoch'::timestamptz),
    COALESCE(s.last_autoanalyze, 'epoch'::timestamptz)
  ) AS last_update_utc
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
LEFT JOIN (
  SELECT table_schema, table_name, COUNT(*) AS column_count
  FROM information_schema.columns
  GROUP BY table_schema, table_name
) col ON col.table_schema = n.nspname AND col.table_name = c.relname
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(c.oid) DESC, n.nspname ASC, c.relname ASC
LIMIT %s;
"""
        rows: list[Any] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (requested,))
                rows = cur.fetchall()
            conn.commit()
        tables: list[dict[str, Any]] = []
        for row in rows:
            size_bytes = int(row[4] or 0)
            updated = row[5]
            tables.append(
                {
                    "schema": str(row[0] or ""),
                    "name": str(row[1] or ""),
                    "row_count": int(row[2] or 0),
                    "column_count": int(row[3] or 0),
                    "table_size_bytes": size_bytes,
                    "table_size_mb": round(float(size_bytes) / (1024.0 * 1024.0), 2),
                    "last_update_utc": (updated.isoformat() if updated else None),
                }
            )
        return {
            "generated_at_utc": self._now_iso(),
            "limit": requested,
            "tables": tables,
        }

    def observability_core(self) -> dict[str, Any]:
        """Return DB-backed observability counters excluding worker presence."""
        status_counts: dict[str, int] = {}
        lease_counts: dict[str, int] = {"active_leases": 0, "expired_leases": 0}
        event_counts: dict[str, int] = {"last_5m": 0, "last_1h": 0}
        active_workflow_runs = 0
        oldest_ready_task_age_seconds = 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
SELECT status, COUNT(*)
FROM coordinator_stage_tasks
GROUP BY status;
"""
                )
                for status, count in cur.fetchall():
                    key = str(status or "").strip().lower()
                    if key:
                        status_counts[key] = int(count or 0)
                cur.execute(
                    """
SELECT
  COUNT(*) FILTER (WHERE lease_expires_at IS NOT NULL AND lease_expires_at > NOW()) AS active_leases,
  COUNT(*) FILTER (WHERE lease_expires_at IS NOT NULL AND lease_expires_at <= NOW()) AS expired_leases
FROM coordinator_stage_tasks;
"""
                )
                lease_row = cur.fetchone() or (0, 0)
                lease_counts["active_leases"] = int(lease_row[0] or 0)
                lease_counts["expired_leases"] = int(lease_row[1] or 0)
                cur.execute(
                    """
SELECT COUNT(*)
FROM coordinator_event_log
WHERE created_at_utc >= NOW() - INTERVAL '5 minutes';
"""
                )
                event_counts["last_5m"] = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    """
SELECT COUNT(*)
FROM coordinator_event_log
WHERE created_at_utc >= NOW() - INTERVAL '1 hour';
"""
                )
                event_counts["last_1h"] = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    """
SELECT COUNT(DISTINCT COALESCE(checkpoint_json->>'workflow_run_id',''))
FROM coordinator_stage_tasks
WHERE COALESCE(checkpoint_json->>'workflow_run_id','') <> ''
  AND status IN ('pending', 'ready', 'running', 'paused');
"""
                )
                active_workflow_runs = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    """
SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(updated_at_utc))), 0)
FROM coordinator_stage_tasks
WHERE status = 'ready';
"""
                )
                oldest_ready_task_age_seconds = int(float((cur.fetchone() or [0])[0] or 0.0))
            conn.commit()
        return {
            "generated_at_utc": self._now_iso(),
            "stage_task_status_counts": status_counts,
            "leases": lease_counts,
            "event_volume": event_counts,
            "active_workflow_runs": active_workflow_runs,
            "oldest_ready_task_age_seconds": oldest_ready_task_age_seconds,
        }
