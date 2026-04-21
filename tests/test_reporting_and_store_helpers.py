from __future__ import annotations

import re
import io
import zipfile
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pytest

from reporting.extractor_reports import build_javascript_extractor_matches_report_html
from reporting.server_pages import (
    render_crawl_progress_html,
    render_dashboard_html,
    render_discovered_files_html,
    render_docker_status_html,
    render_discovered_targets_html,
    render_extractor_matches_html,
    render_fuzzing_html,
    render_view_logs_html,
    render_workers_html,
)
from server import (
    _apply_extractor_row_query,
    _apply_fuzzing_row_query,
    _extractor_match_stats_from_zip_bytes,
    _top_extractor_filters,
    collect_dashboard_data,
)
from server_app.store import CoordinatorStore, _get_root_domain, _make_target_entry_id, _normalize_target_url


def test_render_dashboard_html_contains_expected_heading():
    html = render_dashboard_html()
    assert "Nightmare Live Dashboard" in html


def test_render_workers_html_contains_expected_heading():
    html = render_workers_html()
    assert "Worker Control Center" in html


def test_render_crawl_progress_html_contains_expected_heading():
    html = render_crawl_progress_html()
    assert "Crawl Progress" in html




def test_render_discovered_targets_html_contains_expected_heading():
    html = render_discovered_targets_html()
    assert "Discovered Targets" in html
    assert "/api/coord/discovered-targets" in html
    assert "/api/coord/discovered-target-sitemap" in html


def test_render_discovered_files_html_contains_expected_heading():
    html = render_discovered_files_html()
    assert "Discovered Files" in html
    assert "/api/coord/discovered-files" in html
    assert "payload.rows" in html
    assert "payload.files" in html


def test_render_extractor_matches_html_contains_expected_heading():
    html = render_extractor_matches_html()
    assert "Extractor Matches" in html
    assert "Top Filters (Top 10)" in html
    assert "/api/coord/extractor-patterns" in html
    assert "/api/coord/extractor-patterns/save" in html


def test_render_fuzzing_html_contains_expected_heading():
    html = render_fuzzing_html()
    assert "Fuzzing" in html
    assert "/api/coord/fuzzing/domains" in html
    assert "/api/coord/fuzzing?" in html
    assert "/api/coord/ui-preferences" in html
    assert "NightmareGridControls" in html
    assert "fuzzing_findings_table_v1" in html


def test_render_docker_status_html_contains_expected_heading():
    html = render_docker_status_html()
    assert "Docker Status" in html
    assert "/api/coord/docker-status" in html


def test_render_view_logs_html_contains_expected_heading():
    html = render_view_logs_html()
    assert "View Logs" in html
    assert "/api/coord/log-sources" in html


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
                self._fetchall = [("public", "coordinator_targets")]
                return
            if "FROM information_schema.columns" in compact:
                self._fetchall = [
                    ("entry_id", "text", "NO"),
                    ("raw", "text", "YES"),
                ]
                return
            if 'SELECT COUNT(*) FROM "public"."coordinator_targets";' in compact:
                self._fetchone = (50,)
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
    assert table["row_count_is_estimate"] is False
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
                self._fetchall = [("public", "problem_table")]
                return
            if "FROM information_schema.columns" in compact:
                self._fetchall = [("raw", "text", "YES")]
                return
            if 'SELECT COUNT(*) FROM "public"."problem_table";' in compact:
                self._fetchone = (7,)
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


def test_get_ui_preference_returns_default_when_missing():
    now = datetime(2026, 4, 16, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchone = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            assert "FROM coordinator_ui_preferences" in compact
            assert params == ("fuzzing", "table_columns_v1")
            self._fetchone = None

        def fetchone(self):
            return self._fetchone

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
    payload = CoordinatorStore.get_ui_preference(store, page="fuzzing", pref_key="table_columns_v1")
    assert payload["found"] is False
    assert payload["pref_value"] == {}
    assert payload["updated_at_utc"] is None


def test_set_ui_preference_upserts_json_payload():
    now = datetime(2026, 4, 16, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchone = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            assert "INSERT INTO coordinator_ui_preferences" in compact
            assert params[0] == "fuzzing"
            assert params[1] == "table_columns_v1"
            assert isinstance(params[2], str)
            self._fetchone = (now,)

        def fetchone(self):
            return self._fetchone

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
    value = {"hidden_columns": ["url"], "column_widths": {"url": 180}}
    payload = CoordinatorStore.set_ui_preference(store, page="fuzzing", pref_key="table_columns_v1", pref_value=value)
    assert payload["page"] == "fuzzing"
    assert payload["pref_key"] == "table_columns_v1"
    assert payload["pref_value"] == value
    assert payload["updated_at_utc"] == now.isoformat()


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
                self._fetchall = [("presence-worker-1", now, 0, 0, 0, [], 0, "state_running")]
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
    assert worker["status"] == "running"
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
                self._fetchall = [("presence-worker-2", now, 0, 0, 0, 0, [], "state_running")]
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
    assert worker["status"] == "running"
    assert worker["running_targets"] == 0
    assert worker["running_stage_tasks"] == 0


def test_crawl_progress_snapshot_reports_domain_counts():
    now = datetime.now(timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchall = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "FROM domain_set d" in compact and "discovered_urls_count" in compact:
                assert params == (200,)
                self._fetchall = [
                    (
                        "example.com",  # root_domain
                        "https://example.com",  # start_url
                        12,  # discovered_urls_count
                        5,  # visited_urls_count
                        7,  # frontier_count
                        now,  # session_saved_at_utc
                        now,  # last_activity_at_utc
                        0,  # pending_targets
                        1,  # running_targets
                        0,  # completed_targets
                        0,  # failed_targets
                        0,  # pending_stage_tasks
                        0,  # running_stage_tasks
                        0,  # completed_stage_tasks
                        0,  # failed_stage_tasks
                        [],  # active_stages
                        ["worker-1"],  # target_workers
                        [],  # stage_workers
                    )
                ]
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

    data = CoordinatorStore.crawl_progress_snapshot(store, limit=200)
    assert data["counts"]["total_domains"] == 1
    assert data["counts"]["running_domains"] == 1
    domain = data["domains"][0]
    assert domain["root_domain"] == "example.com"
    assert domain["phase"] == "nightmare_running"
    assert domain["discovered_urls_count"] == 12
    assert domain["visited_urls_count"] == 5
    assert domain["frontier_count"] == 7
    assert domain["active_workers"] == ["worker-1"]


def test_collect_dashboard_data_uses_coordinator_progress_when_output_empty(tmp_path: Path):
    class DummyStore:
        def crawl_progress_snapshot(self, *, limit: int = 2000):
            assert limit == 2000
            return {
                "counts": {"total_domains": 1, "running_domains": 1},
                "domains": [
                    {
                        "root_domain": "example.com",
                        "phase": "nightmare_running",
                        "discovered_urls_count": 42,
                        "visited_urls_count": 11,
                    }
                ],
            }

    data = collect_dashboard_data(tmp_path, DummyStore())
    assert data["totals"]["domains"] == 1
    assert data["totals"]["running"] == 1
    domain = data["domains"][0]
    assert domain["domain"] == "example.com"
    assert domain["status"] == "nightmare_running"
    assert domain["unique_urls"] == 42
    assert domain["requested_urls"] == 11


def test_list_extractor_match_domains_uses_summary_count():
    now = datetime(2026, 4, 17, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchall = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "FROM coordinator_artifacts m LEFT JOIN coordinator_artifacts s" in compact:
                assert params == (5000,)
                self._fetchall = [
                    (
                        "example.com",
                        "worker-1",
                        "sha123",
                        100,
                        now,
                        b'{"match_count": 7, "rows": []}',
                        "identity",
                        now,
                    )
                ]
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

    data = CoordinatorStore.list_extractor_match_domains(store, limit=5000)
    assert len(data) == 1
    row = data[0]
    assert row["root_domain"] == "example.com"
    assert row["summary_match_count"] == 7
    assert row["content_sha256"] == "sha123"


def test_list_fozzy_summary_domains_uses_summary_totals():
    now = datetime(2026, 4, 17, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchall = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "FROM coordinator_artifacts s LEFT JOIN coordinator_artifacts z" in compact:
                assert params == (5000,)
                self._fetchall = [
                    (
                        "example.com",
                        "worker-1",
                        "sha-summary",
                        300,
                        now,
                        b'{"totals": {"groups": 2, "anomalies": 9, "reflections": 4, "baseline_requests": 10, "fuzz_requests": 99}}',
                        "identity",
                        "sha-zip",
                        1234,
                        now,
                    )
                ]
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

    data = CoordinatorStore.list_fozzy_summary_domains(store, limit=5000)
    assert len(data) == 1
    row = data[0]
    assert row["root_domain"] == "example.com"
    assert row["totals"]["anomalies"] == 9
    assert row["totals"]["reflections"] == 4
    assert row["has_results_zip"] is True


def test_top_extractor_filters_ranks_descending_and_limits_top_10():
    rows = []
    for idx in range(12):
        name = f"rule-{idx:02d}"
        for _ in range(idx + 1):
            rows.append({"filter_name": name})
    rows.append({"filter_name": ""})
    rows.append({})

    top = _top_extractor_filters(rows, top_n=10)
    assert len(top) == 10
    assert top[0]["filter_name"] == "rule-11"
    assert top[0]["match_count"] == 12
    assert top[1]["filter_name"] == "rule-10"
    assert top[-1]["filter_name"] == "rule-02"


def test_extractor_match_stats_from_zip_bytes_returns_count_and_max_score():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("m_1.json", '{"importance_score": 3}')
        zf.writestr("m_2.json", '{"importance_score": 12}')
        zf.writestr("misc.json", '{"importance_score": 999}')
    stats = _extractor_match_stats_from_zip_bytes(buf.getvalue())
    assert stats["match_count"] == 2
    assert stats["max_importance_score"] == 12


def test_apply_extractor_row_query_filters_sorts_and_pages():
    rows = [
        {"filter_name": "alpha", "importance_score": 1, "source_http_status": 200, "url": "https://a.example/x"},
        {"filter_name": "beta", "importance_score": 10, "source_http_status": 500, "url": "https://b.example/y"},
        {"filter_name": "alpha", "importance_score": 7, "source_http_status": 404, "url": "https://a.example/z"},
    ]

    result = _apply_extractor_row_query(
        rows,
        search_text="example",
        column_filters={"filter_name": "alpha"},
        sort_key="importance_score",
        sort_dir="desc",
        offset=0,
        limit=1,
    )
    page_rows = result["rows"]
    filtered_rows = result["filtered_rows_for_stats"]
    assert result["total_rows"] == 2
    assert len(page_rows) == 1
    assert page_rows[0]["importance_score"] == 7
    assert len(filtered_rows) == 2
    assert result["has_more"] is True
    assert result["next_offset"] == 1
    assert result["prev_offset"] is None


def test_apply_fuzzing_row_query_filters_sorts_and_pages():
    rows = [
        {"root_domain": "a.com", "result_type": "anomaly", "anomaly_type": "header_change", "size_difference": 10, "url": "https://a.com/x"},
        {"root_domain": "a.com", "result_type": "reflection", "anomaly_type": "", "size_difference": 2, "url": "https://a.com/y"},
        {"root_domain": "b.com", "result_type": "anomaly", "anomaly_type": "status_or_size_change", "size_difference": 99, "url": "https://b.com/z"},
    ]
    result = _apply_fuzzing_row_query(
        rows,
        search_text="a.com",
        column_filters={"result_type": "anomaly"},
        sort_key="size_difference",
        sort_dir="desc",
        offset=0,
        limit=1,
    )
    assert result["total_rows"] == 1
    assert len(result["rows"]) == 1
    assert result["rows"][0]["root_domain"] == "a.com"
    assert result["rows"][0]["result_type"] == "anomaly"
    assert result["has_more"] is False


def test_list_discovered_target_domains_uses_session_inventory_counts():
    now = datetime(2026, 4, 19, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchall = []
            self._fetchone = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if (
                "FROM coordinator_sessions" in compact
                and "jsonb_array_length(payload #> '{state,discovered_urls}')" in compact
            ) or (
                "FROM domain_rows" in compact
                and "SELECT root_domain, start_url, saved_at_utc, payload" in compact
            ):
                assert params == (5000,)
                self._fetchall = [
                    (
                        "example.com",
                        "https://example.com",
                        now,
                        {
                            "state": {
                                "discovered_urls": ["https://example.com/", "https://example.com/app.js"],
                                "url_inventory": {
                                    "https://example.com/": {"discovered_via": ["seed_input", "internal_link"]},
                                    "https://example.com/app.js": {"discovered_via": ["src_reference", "observed_in_script_file"]},
                                },
                            }
                        },
                        0,  # pending_targets
                        0,  # running_targets
                        1,  # completed_targets
                        0,  # failed_targets
                    )
                ]
                return
            if "FROM coordinator_sessions" in compact and "WHERE root_domain = %s" in compact:
                assert params == ("example.com",)
                self._fetchone = (
                    "example.com",
                    "https://example.com",
                    1000,
                    now,
                    {
                        "state": {
                            "discovered_urls": ["https://example.com/", "https://example.com/admin"],
                            "link_graph": {"https://example.com/": ["https://example.com/admin"]},
                            "url_inventory": {
                                "https://example.com/": {"discovered_via": ["seed_input"], "was_crawled": True},
                                "https://example.com/admin": {"discovered_via": ["internal_link"], "exists_confirmed": True},
                            },
                        }
                    },
                )
                return
            raise AssertionError(f"Unexpected SQL in test: {compact}")

        def fetchall(self):
            return self._fetchall

        def fetchone(self):
            return self._fetchone

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

    domains = CoordinatorStore.list_discovered_target_domains(store, limit=5000)
    assert len(domains) == 1
    row = domains[0]
    assert row["root_domain"] == "example.com"
    assert row["discovered_urls_count"] == 2
    assert row["method_counts"]["seed_input"] == 1
    assert row["method_counts"]["internal_link"] == 1
    assert row["method_counts"]["src_reference"] == 1
    assert row["method_counts"]["observed_in_script_file"] == 1

    sitemap = CoordinatorStore.get_discovered_target_sitemap(store, "example.com")
    assert sitemap is not None
    assert sitemap["root_domain"] == "example.com"
    assert sitemap["page_count"] == 2
    pages = {row["url"]: row for row in sitemap["pages"]}
    assert pages["https://example.com/"]["outbound_count"] == 1
    assert pages["https://example.com/admin"]["inbound_count"] == 1


def test_auth0r_overview_completed_only_uses_type_safe_ordering():
    now = datetime(2026, 4, 21, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchall = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "FROM domain_rows" in compact and "discovered_urls_count > 0" in compact:
                assert "ORDER BY root_domain ASC" in compact
                assert "CASE WHEN (%s = TRUE) THEN root_domain ELSE NULL END DESC" not in compact
                assert params == (True, 25)
                self._fetchall = [("example.com", "https://example.com/", 9, "completed", now)]
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

    store = CoordinatorStore.__new__(CoordinatorStore)
    store._connect = lambda: FakeConnection()  # type: ignore[method-assign]

    payload = CoordinatorStore.auth0r_overview(store, completed_only=True, limit=25)
    assert payload["completed_only"] is True
    assert payload["total_domains"] == 1
    assert payload["domains"][0]["root_domain"] == "example.com"
    assert payload["domains"][0]["status"] == "completed"


def test_list_discovered_files_returns_template_compatible_keys():
    now = datetime(2026, 4, 21, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self):
            self._fetchall = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "FROM coordinator_artifacts a LEFT JOIN coordinator_sessions s ON s.root_domain = a.root_domain" in compact:
                assert params == (5000,)
                self._fetchall = [("Example.COM", "nightmare_log", 321, now, "https://example.com/")]
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

    rows = CoordinatorStore.list_discovered_files(store, limit=5000)
    assert len(rows) == 1
    row = rows[0]
    assert row["root_domain"] == "example.com"
    assert row["artifact_type"] == "nightmare_log"
    assert row["updated_at_utc"] == now.isoformat()
    assert row["discovered_at_utc"] == now.isoformat()
    assert row["content_size_bytes"] == 321
    assert row["file_size"] == 321


def test_list_high_value_files_returns_template_compatible_keys():
    now = datetime(2026, 4, 21, tzinfo=timezone.utc)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("captures/login.txt", "hello")
        zf.writestr("captures/ignore.json", "{}")
    zip_bytes = zip_buffer.getvalue()

    class FakeCursor:
        def __init__(self):
            self._fetchall = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if "FROM coordinator_artifacts WHERE artifact_type = 'nightmare_high_value_zip'" in compact:
                assert params == (5000,)
                self._fetchall = [("example.com", zip_bytes, "zip", now)]
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

    rows = CoordinatorStore.list_high_value_files(store, limit=5000)
    assert len(rows) == 1
    row = rows[0]
    assert row["root_domain"] == "example.com"
    assert row["saved_relative"] == "captures/login.txt"
    assert row["captured_at_utc"] == now.isoformat()
    assert row["updated_at_utc"] == now.isoformat()
    assert row["discovered_at_utc"] == now.isoformat()
    assert row["content_size_bytes"] == 5
    assert row["file_size"] == 5
