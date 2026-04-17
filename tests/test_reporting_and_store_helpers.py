from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import pytest

from reporting.extractor_reports import build_javascript_extractor_matches_report_html
from reporting.server_pages import render_dashboard_html, render_workers_html
from server_app.store import CoordinatorStore, _get_root_domain, _make_target_entry_id, _normalize_target_url


def test_render_dashboard_html_contains_expected_heading():
    html = render_dashboard_html()
    assert "Nightmare Live Dashboard" in html


def test_render_workers_html_contains_expected_heading():
    html = render_workers_html()
    assert "Worker Control Center" in html


def test_extractor_report_html_escapes_script_content():
    html = build_javascript_extractor_matches_report_html(
        "example.com",
        [
            {
                "rule_name": "r1",
                "regex": "<script>",
                "match_text": "<script>alert(1)</script>",
                "score": 5,
                "url": "https://example.com/a.js",
                "source_file": "a.js",
            }
        ],
    )
    assert "JavaScript extractor" in html
    assert "\\u003cscript>" in html


def test_get_root_domain_extracts_last_two_labels():
    assert _get_root_domain("a.b.example.com") == "example.com"
    assert _get_root_domain("localhost") == "localhost"
    assert _get_root_domain("") == ""


def test_normalize_target_url_accepts_host_and_strips_fragment():
    normalized, root_domain = _normalize_target_url("Example.COM/path#frag")
    assert normalized == "https://example.com/path"
    assert root_domain == "example.com"


def test_normalize_target_url_rejects_invalid_target():
    with pytest.raises(ValueError):
        _normalize_target_url("://bad target")


def test_make_target_entry_id_is_stable_and_short():
    a = _make_target_entry_id(12, "https://example.com")
    b = _make_target_entry_id(12, "https://example.com")
    c = _make_target_entry_id(13, "https://example.com")
    assert a == b
    assert a != c
    assert re.fullmatch(r"[0-9a-f]{16}", a)


def test_database_status_limits_rows_per_table():
    now = datetime(2026, 4, 16, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchone = None
            self._fetchall = []
            self.description = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "SELECT current_database(), current_user, version(), NOW();" in compact:
                self._fetchone = ("nightmare", "nightmare", "PostgreSQL 16", now)
                return
            if "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace" in compact:
                self._fetchall = [("public", "coordinator_targets", 50)]
                return
            if "FROM information_schema.columns" in compact:
                self._fetchall = [
                    ("entry_id", "text", "NO"),
                    ("raw", "text", "YES"),
                ]
                return
            if 'FROM "public"."coordinator_targets" LIMIT %s;' in compact:
                assert params == (20,)
                self.description = [("entry_id",), ("raw",)]
                self._fetchall = [(f"id-{idx}", f"raw-{idx}") for idx in range(20)]
                return
            raise AssertionError(f"Unexpected SQL in test: {compact}")

        def fetchone(self):
            return self._fetchone

        def fetchall(self):
            return self._fetchall

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    store = CoordinatorStore.__new__(CoordinatorStore)
    store._connect = lambda: FakeConnection()  # type: ignore[method-assign]

    data = CoordinatorStore.database_status(store)
    assert data["max_rows_per_table"] == 20
    assert data["max_text_preview_chars"] == 4096
    assert data["table_count"] == 1
    table = data["tables"][0]
    assert table["row_count"] == 50
    assert table["row_count_is_estimate"] is True
    assert table["rows_returned"] == 20
    assert table["rows_limited"] is True
    assert len(table["rows"]) == 20


def test_database_status_tolerates_single_table_query_failure():
    now = datetime(2026, 4, 16, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchone = None
            self._fetchall = []
            self.description = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "SELECT current_database(), current_user, version(), NOW();" in compact:
                self._fetchone = ("nightmare", "nightmare", "PostgreSQL 16", now)
                return
            if "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace" in compact:
                self._fetchall = [("public", "problem_table", 7)]
                return
            if "FROM information_schema.columns" in compact:
                self._fetchall = [("raw", "text", "YES")]
                return
            if 'FROM "public"."problem_table" LIMIT %s;' in compact:
                raise RuntimeError("simulated table read failure")
            raise AssertionError(f"Unexpected SQL in test: {compact}")

        def fetchone(self):
            return self._fetchone

        def fetchall(self):
            return self._fetchall

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    store = CoordinatorStore.__new__(CoordinatorStore)
    store._connect = lambda: FakeConnection()  # type: ignore[method-assign]

    data = CoordinatorStore.database_status(store)
    assert data["table_count"] == 1
    table = data["tables"][0]
    assert table["schema"] == "public"
    assert table["name"] == "problem_table"
    assert table["rows_returned"] == 0
    assert table["rows_limited"] is False
    assert "simulated table read failure" in table["table_error"]


def test_worker_control_snapshot_includes_presence_only_worker():
    now = datetime.now(timezone.utc) - timedelta(seconds=5)

    class FakeCursor:
        def __init__(self):
            self._fetchall = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "FROM worker_ids w" in compact and "queued_commands" in compact:
                self._fetchall = [("presence-worker-1", now, 0, 0, 0, [], 0)]
                return
            raise AssertionError(f"Unexpected SQL in test: {compact}")

        def fetchall(self):
            return self._fetchall

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    store = CoordinatorStore.__new__(CoordinatorStore)
    store._connect = lambda: FakeConnection()  # type: ignore[method-assign]

    data = CoordinatorStore.worker_control_snapshot(store, stale_after_seconds=120)
    assert data["counts"]["total_workers"] == 1
    worker = data["workers"][0]
    assert worker["worker_id"] == "presence-worker-1"
    assert worker["status"] == "online"
    assert worker["running_targets"] == 0
    assert worker["running_stage_tasks"] == 0
    assert worker["urls_scanned_session"] == 0


def test_worker_statuses_includes_presence_only_worker():
    now = datetime.now(timezone.utc) - timedelta(seconds=5)

    class FakeCursor:
        def __init__(self):
            self._fetchall = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "FROM worker_ids w" in compact and "active_target_leases" in compact:
                self._fetchall = [("presence-worker-2", now, 0, 0, 0, 0, [])]
                return
            raise AssertionError(f"Unexpected SQL in test: {compact}")

        def fetchall(self):
            return self._fetchall

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    store = CoordinatorStore.__new__(CoordinatorStore)
    store._connect = lambda: FakeConnection()  # type: ignore[method-assign]

    data = CoordinatorStore.worker_statuses(store, stale_after_seconds=120)
    assert data["counts"]["total_workers"] == 1
    worker = data["workers"][0]
    assert worker["worker_id"] == "presence-worker-2"
    assert worker["status"] == "online"
    assert worker["running_targets"] == 0
    assert worker["running_stage_tasks"] == 0
