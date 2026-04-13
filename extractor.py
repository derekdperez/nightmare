#!/usr/bin/env python3
"""Run regex extractors over Fozzy anomaly/reflection JSON (response bodies and metadata).

Scans ``--scan-root`` (default: ``output`` next to this repo) for Fozzy domain trees, the same way
the master report does. When a domain's ``results/`` tree has newer files than recorded in
``.extractor_incremental_state.json``, that domain is re-processed: each match is written under
``<domain_output>/extractor/matches/`` and a ``summary.json`` is refreshed for the master HTML report.

After the Fozzy pass, optional rules from ``javascript_extractor_list.txt`` are applied to each domain's
``collected_data/scripts`` tree; matches go under ``collected_data/javascript_extractor/``, and progress is
stored in ``<scan-root>/javascript_extractor_state.json`` for incremental runs. An interactive
``collected_data/javascript_extractor/matches_report.html`` is written with the JS match table.

Usage:
    python extractor.py
    python extractor.py --scan-root ./output --force
    python extractor.py lillylibrary.org
    python extractor.py --wordlist path/to/extractor_list.txt
    python extractor.py --trim
    python extractor.py --trim --trim-remove "noisy_rule,other_rule" --trim-yes
    python extractor.py --trim --trim-disk-scan
    python extractor.py --rebuild-trim-stats
    python extractor.py -v
"""

from __future__ import annotations

import argparse
import atexit
import hashlib
import html
import json
import os
import re
import shutil
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Repo root (directory containing this file)
EXTRACTOR_BASE = Path(__file__).resolve().parent
DEFAULT_WORDLIST = EXTRACTOR_BASE / "resources" / "wordlists" / "extractor_list.txt"
DEFAULT_JAVASCRIPT_WORDLIST = EXTRACTOR_BASE / "resources" / "wordlists" / "javascript_extractor_list.txt"
DEFAULT_SCAN_ROOT = EXTRACTOR_BASE / "output"
DEFAULT_CONFIG_PATH = EXTRACTOR_BASE / "config" / "extractor.json"
STATE_FILE_NAME = ".extractor_incremental_state.json"
JAVASCRIPT_STATE_FILE_NAME = "javascript_extractor_state.json"
JAVASCRIPT_EXTRACTOR_REPORT_HTML = "matches_report.html"
MAX_MATCHES_PER_RULE_PER_RESULT_FILE = 300
TRIM_ENUM_PROGRESS_EVERY = 100_000
TRIM_REBUILD_PROGRESS_EVERY = 50_000
# Tiny sidecar so --trim never has to json.load() multi-GB summary.json (see trim_stats.json).
TRIM_STATS_FILENAME = "trim_stats.json"
TRIM_MAX_SUMMARY_JSON_BYTES = 48 * 1024 * 1024

_EXTRACTOR_LOG_HANDLE: Any = None


class _StreamTee:
    def __init__(self, original: Any, mirror_handle: Any):
        self._original = original
        self._mirror = mirror_handle

    def write(self, data: str) -> int:
        text = str(data)
        wrote = self._original.write(text)
        try:
            self._mirror.write(text)
        except Exception:
            pass
        return wrote

    def flush(self) -> None:
        try:
            self._original.flush()
        except Exception:
            pass
        try:
            self._mirror.flush()
        except Exception:
            pass

    def isatty(self) -> bool:
        try:
            return bool(self._original.isatty())
        except Exception:
            return False

    @property
    def encoding(self) -> str:
        return getattr(self._original, "encoding", "utf-8")


def install_extractor_log_tee(log_path: Path) -> None:
    global _EXTRACTOR_LOG_HANDLE
    ensure_directory(log_path.parent)
    handle = log_path.open("a", encoding="utf-8")
    _EXTRACTOR_LOG_HANDLE = handle
    sys.stdout = _StreamTee(sys.stdout, handle)  # type: ignore[assignment]
    sys.stderr = _StreamTee(sys.stderr, handle)  # type: ignore[assignment]
    print(
        f"[extractor] logging to {log_path} (pid={os.getpid()}, started_utc={datetime.now(timezone.utc).isoformat()})",
        flush=True,
    )


def _close_extractor_log_tee() -> None:
    global _EXTRACTOR_LOG_HANDLE
    h = _EXTRACTOR_LOG_HANDLE
    if h is None:
        return
    _EXTRACTOR_LOG_HANDLE = None
    try:
        h.flush()
    except Exception:
        pass
    try:
        h.close()
    except Exception:
        pass


atexit.register(_close_extractor_log_tee)

# Import shared discovery + mtime helpers from fozzy (fozzy does not import extractor).
from fozzy import (  # noqa: E402
    discover_fozzy_domain_output_pairs,
    discover_fozzy_domain_output_pairs_nested,
    ensure_directory,
    folder_tree_max_mtime_ns,
    load_result_entries_from_folders,
    master_report_uses_nested_layout,
    read_json,
)


def load_extractor_incremental_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"version": 1, "domains": {}}
    try:
        data = read_json(path)
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "domains": {}}
    if not isinstance(data, dict):
        return {"version": 1, "domains": {}}
    domains = data.get("domains")
    if not isinstance(domains, dict):
        domains = {}
    return {"version": 1, "domains": domains}


def save_extractor_incremental_state(path: Path, state: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def load_javascript_extractor_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"version": 1, "wordlist": {}, "domains": {}}
    try:
        data = read_json(path)
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "wordlist": {}, "domains": {}}
    if not isinstance(data, dict):
        return {"version": 1, "wordlist": {}, "domains": {}}
    domains = data.get("domains")
    if not isinstance(domains, dict):
        domains = {}
    wl = data.get("wordlist")
    if not isinstance(wl, dict):
        wl = {}
    return {"version": 1, "wordlist": wl, "domains": domains}


def save_javascript_extractor_state(path: Path, state: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def wordlist_fingerprint(wordlist_path: Path) -> str:
    """Stable id for wordlist contents: path, size, and mtime (fast; re-run when list file changes)."""
    try:
        st = wordlist_path.stat()
    except OSError:
        return ""
    base = f"{wordlist_path.resolve()}\0{st.st_size}\0{st.st_mtime_ns}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]


def collected_scripts_root(domain_output: Path) -> Path:
    return domain_output / "collected_data" / "scripts"


def javascript_extractor_root(domain_output: Path) -> Path:
    return domain_output / "collected_data" / "javascript_extractor"


def iter_collected_script_files(scripts_root: Path) -> list[Path]:
    if not scripts_root.is_dir():
        return []
    out: list[Path] = []
    for p in scripts_root.rglob("*"):
        if p.is_file():
            out.append(p)
    out.sort(key=lambda x: str(x).lower())
    return out


def script_file_to_result_entry(script_path: Path, scripts_root: Path) -> dict[str, Any]:
    rel = script_path.relative_to(scripts_root).as_posix()
    try:
        text = script_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        text = ""
    pseudo_url = f"file://collected_data/scripts/{rel}"
    rf = str(script_path.resolve())
    return {
        "url": pseudo_url,
        "host": "",
        "path": f"/{rel}",
        "mutated_parameter": "",
        "mutated_value": "",
        "result_type": "javascript_script",
        "result_file": rf,
        "baseline_response": {"body_preview": text},
        "anomaly_response": {"body_preview": text},
    }


def _unlink_match_paths(paths: list[str]) -> None:
    for s in paths:
        p = Path(s)
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass


def apply_extractor_rules_to_entry(
    entry: dict[str, Any],
    domain_label: str,
    rules: list[dict[str, Any]],
    matches_dir: Path,
) -> tuple[int, list[dict[str, Any]]]:
    """Run every rule against one Fozzy- or synthetic result entry; write ``m_<hash>.json`` files."""
    rows_out: list[dict[str, Any]] = []
    match_count = 0
    result_file = str(entry.get("result_file", "") or "")
    if not result_file:
        return 0, rows_out
    rf_path = Path(result_file)
    result_file_abs = str(rf_path.resolve()) if rf_path.is_file() else result_file

    for rule in rules:
        scope = rule["scope"]
        blobs = build_search_text_for_scope(entry, scope)
        text_key = scope if scope in blobs else "request_response"
        combined = blobs.get(text_key, blobs.get("request_response", ""))
        if scope == "response_body":
            parts: list[tuple[str, str]] = [
                ("baseline", blobs.get("baseline_body", "")),
                ("anomaly", blobs.get("anomaly_body", "")),
            ]
        elif scope in ("request_headers", "response_headers"):
            parts = [("combined", combined)]
        elif scope == "request_headers_response_body":
            parts = [("combined", combined)]
        else:
            parts = [("combined", combined)]

        rule_hits = 0
        for side_label, text in parts:
            if not text.strip():
                continue
            try:
                it = rule["compiled"].finditer(text)
            except re.error:
                break
            for idx, m in enumerate(it):
                if rule_hits >= MAX_MATCHES_PER_RULE_PER_RESULT_FILE:
                    break
                matched = m.group(0)
                if not matched:
                    continue
                rule_hits += 1
                match_count += 1
                mid = match_fingerprint(domain_label, result_file_abs, rule["name"], idx, matched)
                detail_path = matches_dir / f"m_{mid}.json"
                detail = {
                    "domain_label": domain_label,
                    "filter_name": rule["name"],
                    "importance_score": int(rule.get("importance_score", 0) or 0),
                    "filter_description": rule["description"],
                    "scope": scope,
                    "response_side": side_label,
                    "regex": rule["regex"],
                    "result_file": result_file_abs,
                    "result_type": str(entry.get("result_type", "") or ""),
                    "url": str(entry.get("url", "") or ""),
                    "matched_text": matched,
                    "match_index": idx,
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                detail_path.write_text(
                    json.dumps(detail, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                preview = matched.replace("\n", " ").strip()
                if len(preview) > 160:
                    preview = preview[:157] + "..."
                rows_out.append(
                    {
                        "domain_label": domain_label,
                        "url": str(entry.get("url", "") or ""),
                        "filter_name": rule["name"],
                        "importance_score": int(rule.get("importance_score", 0) or 0),
                        "scope": scope,
                        "response_side": side_label,
                        "match_preview": preview,
                        "result_file": result_file_abs,
                        "match_file": str(detail_path.resolve()),
                        "result_type": str(entry.get("result_type", "") or ""),
                    }
                )
    return match_count, rows_out


def rebuild_javascript_extractor_summary_for_domain(
    domain_label: str,
    domain_output: Path,
    *,
    progress_every: int | None = TRIM_REBUILD_PROGRESS_EVERY,
) -> Path | None:
    """Rebuild ``collected_data/javascript_extractor/summary.json`` from ``matches/m_*.json``."""
    js_root = javascript_extractor_root(domain_output)
    matches_dir = js_root / "matches"
    rows_out: list[dict[str, Any]] = []
    if matches_dir.is_dir():
        paths = sorted(matches_dir.glob("m_*.json"))
        total_paths = len(paths)
        for idx, path in enumerate(paths, start=1):
            detail = _read_match_detail(path)
            if not detail:
                continue
            row = detail_record_to_summary_row(detail)
            row["match_file"] = str(path.resolve())
            rows_out.append(row)
            if progress_every and total_paths > 0 and idx > 0 and idx % progress_every == 0:
                print(
                    f"[extractor:js] rebuild {domain_label!r}: scanned {idx:,}/{total_paths:,} match file(s) …",
                    flush=True,
                )
    summary_path = js_root / "summary.json"
    summary_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "domain_label": domain_label,
        "domain_output": str(domain_output.resolve()),
        "source": "javascript_extractor",
        "match_count": len(rows_out),
        "rows": rows_out,
    }
    ensure_directory(js_root)
    summary_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    write_trim_stats_from_rows(js_root, domain_label, rows_out)
    write_javascript_extractor_matches_report_html(domain_label, domain_output)
    return summary_path


def collect_javascript_extractor_report_rows(domain_output: Path) -> list[dict[str, Any]]:
    """One dict per ``m_*.json`` under ``collected_data/javascript_extractor/matches`` for the HTML report."""
    js_root = javascript_extractor_root(domain_output)
    matches_dir = js_root / "matches"
    out: list[dict[str, Any]] = []
    if not matches_dir.is_dir():
        return out
    for path in sorted(matches_dir.glob("m_*.json")):
        detail = _read_match_detail(path)
        if not detail:
            continue
        out.append(
            {
                "rule": str(detail.get("filter_name", "") or ""),
                "regex": str(detail.get("regex", "") or ""),
                "matched": str(detail.get("matched_text", "") or ""),
                "score": int(detail.get("importance_score", 0) or 0),
            }
        )
    return out


def build_javascript_extractor_matches_report_html(domain_label: str, rows: list[dict[str, Any]]) -> str:
    """Self-contained HTML: sortable columns, per-column filters, global search (all fields)."""
    payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    payload = payload.replace("<", "\\u003c")
    title_esc = html.escape(f"JavaScript extractor — {domain_label}")
    gen_esc = html.escape(datetime.now(timezone.utc).isoformat())
    # Template: keep `{` in CSS/JS minimal — use {{ for literal braces in f-string... actually not using f-string for body
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>__TITLE__</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: system-ui, Segoe UI, Roboto, sans-serif; margin: 1rem 1.25rem; line-height: 1.45; }
  h1 { font-size: 1.25rem; margin: 0 0 0.25rem; }
  .meta { color: #666; font-size: 0.85rem; margin-bottom: 1rem; }
  #globalSearch { width: min(42rem, 100%); padding: 0.45rem 0.6rem; font-size: 1rem; margin-bottom: 0.75rem; }
  .table-wrap { overflow-x: auto; border: 1px solid #8884; border-radius: 6px; }
  table { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
  th, td { border: 1px solid #8883; padding: 0.35rem 0.5rem; vertical-align: top; text-align: left; }
  thead th { background: #8882; position: sticky; top: 0; z-index: 2; cursor: pointer; user-select: none; white-space: nowrap; }
  thead th:hover { background: #8884; }
  thead tr.filters th { position: sticky; top: 2.2rem; z-index: 2; cursor: default; background: #6661; padding: 0.25rem; }
  thead tr.filters input { width: 100%; box-sizing: border-box; font: inherit; padding: 0.25rem 0.35rem; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .col-rule { min-width: 9rem; max-width: 14rem; word-break: break-word; }
  .col-regex { min-width: 12rem; max-width: 28rem; word-break: break-all; font-family: ui-monospace, Consolas, monospace; font-size: 0.82rem; }
  .col-matched { min-width: 14rem; max-width: 36rem; white-space: pre-wrap; word-break: break-word; font-family: ui-monospace, Consolas, monospace; font-size: 0.82rem; }
  .col-score { width: 5rem; }
  th .sortind { font-size: 0.75rem; opacity: 0.7; margin-left: 0.25rem; }
  tbody tr:nth-child(even) { background: #8881; }
  .empty { padding: 1rem; color: #666; }
  .count { font-weight: 600; margin-bottom: 0.5rem; }
</style>
</head>
<body>
  <h1>__TITLE__</h1>
  <div class="meta">Generated UTC: __GEN__ · Rows: __N__</div>
  <div class="count" id="visibleCount"></div>
  <label for="globalSearch">Search all columns (each word must appear somewhere in the row)</label><br/>
  <input type="search" id="globalSearch" placeholder="Words — searched across rule, regex, matched text, score…" autocomplete="off"/>
  <div class="table-wrap">
    <table id="grid">
      <thead>
        <tr>
          <th data-col="0" scope="col">Rule name<span class="sortind" id="s0"></span></th>
          <th data-col="1" scope="col">Regex<span class="sortind" id="s1"></span></th>
          <th data-col="2" scope="col">Matched text<span class="sortind" id="s2"></span></th>
          <th data-col="3" scope="col">Importance<span class="sortind" id="s3"></span></th>
        </tr>
        <tr class="filters">
          <th><input type="text" data-col-filter="0" placeholder="Filter rule name…" autocomplete="off"/></th>
          <th><input type="text" data-col-filter="1" placeholder="Filter regex…" autocomplete="off"/></th>
          <th><input type="text" data-col-filter="2" placeholder="Filter matched text…" autocomplete="off"/></th>
          <th><input type="text" data-col-filter="3" placeholder="Filter score…" autocomplete="off"/></th>
        </tr>
      </thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
  <script type="application/json" id="report-data">__DATA__</script>
  <script>
(function () {
  const KEYS = ["rule", "regex", "matched", "score"];
  const ROWS = JSON.parse(document.getElementById("report-data").textContent);
  const tbody = document.getElementById("tbody");
  const globalSearch = document.getElementById("globalSearch");
  const visibleCount = document.getElementById("visibleCount");
  const colFilters = [0,1,2,3].map(function (i) {
    return document.querySelector('input[data-col-filter="' + i + '"]');
  });
  let sortCol = null;
  let sortAsc = true;

  function cellStr(row, i) {
    if (i === 3) return String(row.score);
    return String(row[KEYS[i]] || "");
  }
  function rowHaystack(row) {
    return (cellStr(row,0) + " " + cellStr(row,1) + " " + cellStr(row,2) + " " + cellStr(row,3)).toLowerCase();
  }
  function passesGlobal(row) {
    const raw = (globalSearch.value || "").trim().toLowerCase();
    if (!raw) return true;
    const hay = rowHaystack(row);
    const parts = raw.split(/\\s+/).filter(Boolean);
    for (let i = 0; i < parts.length; i++) {
      if (hay.indexOf(parts[i]) === -1) return false;
    }
    return true;
  }
  function passesColFilters(row) {
    for (let i = 0; i < 4; i++) {
      const f = (colFilters[i].value || "").trim().toLowerCase();
      if (!f) continue;
      if (cellStr(row, i).toLowerCase().indexOf(f) === -1) return false;
    }
    return true;
  }
  function filterRows() {
    return ROWS.filter(function (r) { return passesGlobal(r) && passesColFilters(r); });
  }
  function sortRows(arr) {
    if (sortCol === null) return arr.slice();
    const c = sortCol;
    const asc = sortAsc;
    return arr.slice().sort(function (a, b) {
      if (c === 3) {
        const na = Number(a.score) || 0, nb = Number(b.score) || 0;
        return asc ? na - nb : nb - na;
      }
      const va = cellStr(a, c).toLowerCase();
      const vb = cellStr(b, c).toLowerCase();
      const cmp = va.localeCompare(vb, undefined, { numeric: true, sensitivity: "base" });
      return asc ? cmp : -cmp;
    });
  }
  function esc(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }
  function render() {
    let data = filterRows();
    data = sortRows(data);
    let html = "";
    if (data.length === 0) {
      html = '<tr><td colspan="4" class="empty">No rows match the current filters.</td></tr>';
    } else {
      for (let i = 0; i < data.length; i++) {
        const r = data[i];
        html += "<tr>"
          + '<td class="col-rule">' + esc(r.rule) + "</td>"
          + '<td class="col-regex">' + esc(r.regex) + "</td>"
          + '<td class="col-matched">' + esc(r.matched) + "</td>"
          + '<td class="col-score num">' + esc(String(r.score)) + "</td>"
          + "</tr>";
      }
    }
    tbody.innerHTML = html;
    visibleCount.textContent = "Showing " + data.length + " of " + ROWS.length + " match(es)";
  }
  function updateSortIndicators() {
    for (let i = 0; i < 4; i++) {
      const el = document.getElementById("s" + i);
      if (sortCol === i) el.textContent = sortAsc ? " ▲" : " ▼";
      else el.textContent = "";
    }
  }
  document.querySelectorAll("thead tr:first-child th[data-col]").forEach(function (th) {
    th.addEventListener("click", function () {
      const c = parseInt(th.getAttribute("data-col"), 10);
      if (sortCol === c) sortAsc = !sortAsc;
      else { sortCol = c; sortAsc = c === 3 ? false : true; }
      updateSortIndicators();
      render();
    });
  });
  globalSearch.addEventListener("input", render);
  colFilters.forEach(function (inp) { inp.addEventListener("input", render); });
  updateSortIndicators();
  render();
})();
  </script>
</body>
</html>
""".replace("__TITLE__", title_esc).replace("__GEN__", gen_esc).replace("__N__", str(len(rows))).replace(
        "__DATA__", payload
    )


def write_javascript_extractor_matches_report_html(domain_label: str, domain_output: Path) -> Path | None:
    """Write ``collected_data/javascript_extractor/matches_report.html`` for interactive review."""
    rows = collect_javascript_extractor_report_rows(domain_output)
    js_root = javascript_extractor_root(domain_output)
    ensure_directory(js_root)
    out = js_root / JAVASCRIPT_EXTRACTOR_REPORT_HTML
    html = build_javascript_extractor_matches_report_html(domain_label, rows)
    out.write_text(html, encoding="utf-8")
    return out


def javascript_extractor_domain_needs_work(
    domain_label: str,
    domain_output: Path,
    js_state: dict[str, Any],
    wordlist_fp: str,
    force: bool,
) -> bool:
    scripts_root = collected_scripts_root(domain_output)
    if not scripts_root.is_dir():
        return False
    if force:
        return True
    block = js_state.get("domains", {}).get(domain_label, {})
    if not isinstance(block, dict):
        block = {}
    stored_fp = ""
    wl = js_state.get("wordlist")
    if isinstance(wl, dict):
        stored_fp = str(wl.get("fingerprint", "") or "")
    if wordlist_fp and stored_fp != wordlist_fp:
        return True
    files_state_raw = block.get("files")
    files_state: dict[str, Any] = files_state_raw if isinstance(files_state_raw, dict) else {}

    seen: set[str] = set()
    for path in iter_collected_script_files(scripts_root):
        rel = path.relative_to(scripts_root).as_posix()
        seen.add(rel)
        try:
            st = path.stat()
        except OSError:
            continue
        fi = files_state.get(rel)
        if not isinstance(fi, dict):
            return True
        if int(fi.get("mtime_ns", -1)) != int(st.st_mtime_ns) or int(fi.get("size", -1)) != int(st.st_size):
            return True

    for rel in list(files_state.keys()):
        if rel not in seen:
            return True
    return False


def run_javascript_extractor_for_domain(
    domain_label: str,
    domain_output: Path,
    rules: list[dict[str, Any]],
    js_state: dict[str, Any],
    *,
    wordlist_path: Path,
    wordlist_fp: str,
    force: bool,
) -> tuple[int, Path | None]:
    """Incremental regex pass over ``collected_data/scripts``; outputs under ``collected_data/javascript_extractor``."""
    scripts_root = collected_scripts_root(domain_output)
    if not scripts_root.is_dir():
        return 0, None

    js_root = javascript_extractor_root(domain_output)
    matches_dir = js_root / "matches"
    ensure_directory(matches_dir)

    if "domains" not in js_state or not isinstance(js_state["domains"], dict):
        js_state["domains"] = {}
    domain_block: dict[str, Any] = js_state["domains"].setdefault(domain_label, {})
    if "files" not in domain_block or not isinstance(domain_block["files"], dict):
        domain_block["files"] = {}

    files_state: dict[str, Any] = domain_block["files"]

    # Drop orphan state entries and match files for deleted scripts
    on_disk = {p.relative_to(scripts_root).as_posix() for p in iter_collected_script_files(scripts_root)}
    for rel in list(files_state.keys()):
        if rel not in on_disk:
            fi = files_state.pop(rel, {})
            if isinstance(fi, dict):
                prev = fi.get("match_files")
                if isinstance(prev, list):
                    _unlink_match_paths([str(x) for x in prev if x])

    stored_fp = ""
    wl = js_state.get("wordlist")
    if isinstance(wl, dict):
        stored_fp = str(wl.get("fingerprint", "") or "")

    to_scan: list[Path] = []
    for path in iter_collected_script_files(scripts_root):
        rel = path.relative_to(scripts_root).as_posix()
        try:
            st = path.stat()
        except OSError:
            continue
        fi = files_state.get(rel)
        dirty = (
            force
            or (wordlist_fp and stored_fp != wordlist_fp)
            or not isinstance(fi, dict)
            or int(fi.get("mtime_ns", -1)) != int(st.st_mtime_ns)
            or int(fi.get("size", -1)) != int(st.st_size)
        )
        if dirty:
            to_scan.append(path)

    total_new_matches = 0
    for path in to_scan:
        rel = path.relative_to(scripts_root).as_posix()
        fi_prev = files_state.get(rel) if isinstance(files_state.get(rel), dict) else {}
        prev_mf = fi_prev.get("match_files") if isinstance(fi_prev, dict) else []
        if isinstance(prev_mf, list):
            _unlink_match_paths([str(x) for x in prev_mf if x])

        entry = script_file_to_result_entry(path, scripts_root)
        n, _rows = apply_extractor_rules_to_entry(entry, domain_label, rules, matches_dir)
        total_new_matches += n
        try:
            st = path.stat()
        except OSError:
            st = None
        mf_list: list[str] = []
        if _rows:
            for row in _rows:
                mf = str(row.get("match_file", "") or "").strip()
                if mf:
                    mf_list.append(mf)
        files_state[rel] = {
            "mtime_ns": int(st.st_mtime_ns) if st else 0,
            "size": int(st.st_size) if st else 0,
            "match_files": mf_list,
        }

    js_state["wordlist"] = {
        "path": str(wordlist_path.resolve()),
        "fingerprint": wordlist_fp,
    }

    summary_path = rebuild_javascript_extractor_summary_for_domain(domain_label, domain_output)
    return total_new_matches, summary_path


def domain_results_dirty(domain_output: Path, state: dict[str, Any], domain_key: str) -> bool:
    results = domain_output / "results"
    if not results.is_dir():
        return False
    current = folder_tree_max_mtime_ns(results)
    block = state.get("domains", {}).get(domain_key)
    if not isinstance(block, dict):
        block = {}
    last = int(block.get("results_max_mtime_ns", 0))
    return current > last


def load_extractor_rules(wordlist_path: Path) -> list[dict[str, Any]]:
    if not wordlist_path.is_file():
        raise FileNotFoundError(f"Extractor wordlist not found: {wordlist_path}")
    raw = wordlist_path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"Extractor list must be a JSON array: {wordlist_path}")
    rules: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "") or "").strip()
        regex = str(item.get("regex", "") or "")
        scope = str(item.get("scope", "request_response") or "request_response").strip()
        if not name or not regex:
            continue
        try:
            compiled = re.compile(regex, re.IGNORECASE)
        except re.error as exc:
            print(f"[extractor] skip rule {name!r}: invalid regex ({exc})", flush=True)
            continue
        try:
            importance_score = int(item.get("importance_score", 0) or 0)
        except (TypeError, ValueError):
            importance_score = 0
        rules.append(
            {
                "name": name,
                "regex": regex,
                "compiled": compiled,
                "scope": scope,
                "description": str(item.get("description", "") or ""),
                "output_filename": str(item.get("output_filename", "") or ""),
                "importance_score": importance_score,
            }
        )
    return rules


def _text_response_body(entry: dict[str, Any]) -> tuple[str, str]:
    baseline = entry.get("baseline_response") if isinstance(entry.get("baseline_response"), dict) else {}
    anomaly = entry.get("anomaly_response") if isinstance(entry.get("anomaly_response"), dict) else {}
    b = str(baseline.get("body_preview", "") or "")
    a = str(anomaly.get("body_preview", "") or "")
    return b, a


def _headers_text(_headers: Any) -> str:
    # Fozzy result JSON does not persist response/request headers; placeholders for future data.
    return ""


def build_search_text_for_scope(entry: dict[str, Any], scope: str) -> dict[str, str]:
    """Map logical scope name to text blobs (baseline / anomaly / combined meta)."""
    url = str(entry.get("url", "") or "")
    host = str(entry.get("host", "") or "")
    path = str(entry.get("path", "") or "")
    mp = str(entry.get("mutated_parameter", "") or "")
    mv = str(entry.get("mutated_value", "") or "")
    meta = f"{url}\n{host}\n{path}\n{mp}\n{mv}\n"
    b_body, a_body = _text_response_body(entry)
    req_h = _headers_text(None)
    res_h_b = _headers_text(None)
    res_h_a = _headers_text(None)

    blobs: dict[str, str] = {
        "baseline_body": b_body,
        "anomaly_body": a_body,
        "request_response": f"{meta}\n{b_body}\n{a_body}",
        "response_body": f"{b_body}\n{a_body}",
        "request_headers": req_h,
        "response_headers": f"{res_h_b}\n{res_h_a}",
        "request_headers_response_body": f"{req_h}\n{b_body}\n{a_body}",
    }
    # Default unknown scopes to request_response-like blob
    if scope not in blobs and scope != "request_response":
        blobs.setdefault(scope, f"{meta}\n{b_body}\n{a_body}")
    return blobs


def match_fingerprint(domain_label: str, result_file: str, rule_name: str, idx: int, matched: str) -> str:
    key = f"{domain_label}\0{result_file}\0{rule_name}\0{idx}\0{matched[:500]}"
    return hashlib.sha256(key.encode("utf-8", errors="replace")).hexdigest()[:16]


def _trim_progress(msg: str) -> None:
    print(f"[extractor:trim] {msg}", flush=True)


def write_trim_stats_from_rows(extractor_root: Path, domain_label: str, rows: list[Any]) -> Path:
    """Write ``extractor/trim_stats.json`` (small) with per-rule counts; safe to read during ``--trim``."""
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        if not isinstance(row, dict):
            continue
        fn = str(row.get("filter_name", "") or "").strip()
        if fn:
            counts[fn] = counts.get(fn, 0) + 1
    path = extractor_root / TRIM_STATS_FILENAME
    payload = {
        "version": 1,
        "domain_label": domain_label,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": len(rows),
        "counts_by_filter_name": dict(counts),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def write_trim_stats_from_counts(
    extractor_root: Path,
    domain_label: str,
    *,
    counts_by_filter_name: dict[str, int],
    row_count: int,
) -> Path:
    path = extractor_root / TRIM_STATS_FILENAME
    payload = {
        "version": 1,
        "domain_label": domain_label,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": int(row_count),
        "counts_by_filter_name": dict(counts_by_filter_name),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def _merge_trim_stats_counts(merged: dict[str, int], data: dict[str, Any]) -> bool:
    raw = data.get("counts_by_filter_name")
    if not isinstance(raw, dict):
        return False
    any_n = False
    for k, v in raw.items():
        key = str(k or "").strip()
        if not key:
            continue
        try:
            n = int(v)
        except (TypeError, ValueError):
            continue
        any_n = True
        merged[key] = merged.get(key, 0) + n
    return any_n


def _trim_domain_pairs(
    scan_root: Path,
    *,
    domain_filter: str | None = None,
) -> list[tuple[str, Path]]:
    domain_filter_text = str(domain_filter or "").strip().lower()
    pairs: list[tuple[str, Path]] = []
    for domain_label, domain_output in discover_pairs(scan_root):
        if domain_filter_text:
            if (
                str(domain_label).strip().lower() != domain_filter_text
                and str(domain_output.name).strip().lower() != domain_filter_text
            ):
                continue
        pairs.append((domain_label, domain_output))
    return pairs


def _summary_rows_ok_for_trim_counts(data: dict[str, Any]) -> bool:
    """True when we can aggregate per-rule counts from ``rows`` without scanning every ``m_*.json``."""
    return isinstance(data.get("rows"), list)


def _matches_dir_has_any_m_json(matches_dir: Path) -> bool:
    """Return True if at least one ``m_*.json`` exists (stops after the first hit)."""
    if not matches_dir.is_dir():
        return False
    try:
        for p in matches_dir.iterdir():
            if p.is_file() and p.suffix.lower() == ".json" and p.name.startswith("m_"):
                return True
    except OSError:
        return False
    return False


def _collect_extractor_match_json_paths_for_domain(
    domain_output: Path,
    *,
    label: str | None = None,
) -> list[Path]:
    md = domain_output / "extractor" / "matches"
    out: list[Path] = []
    if not md.is_dir():
        return out
    lbl = label or f"Listing match files ({domain_output.name})"
    try:
        for p in md.iterdir():
            if p.is_file() and p.suffix.lower() == ".json" and p.name.startswith("m_"):
                out.append(p)
                n = len(out)
                if n > 0 and n % TRIM_ENUM_PROGRESS_EVERY == 0:
                    _trim_progress(f"{lbl}: {n:,} path(s) …")
    except OSError:
        return out
    if out:
        _trim_progress(f"{lbl}: done — {len(out):,} match file(s) to scan.")
    return out


def _merge_rule_counts(target: dict[str, int], part: dict[str, int]) -> None:
    for k, v in part.items():
        target[k] = target.get(k, 0) + int(v or 0)


def aggregate_rule_match_counts_hybrid(
    scan_root: Path,
    *,
    domain_filter: str | None = None,
    workers: int = 8,
    force_disk_scan: bool = False,
) -> tuple[dict[str, int], list[tuple[str, Path]]]:
    """Prefer ``trim_stats.json``, else bounded-size ``summary.json``, else scan ``m_*.json`` on disk."""

    if force_disk_scan:
        return aggregate_rule_match_counts_from_disk(
            scan_root, domain_filter=domain_filter, workers=workers
        ), []

    merged: dict[str, int] = {}
    slow_pairs: list[tuple[str, Path]] = []
    from_stats = 0
    from_summary = 0
    for domain_label, domain_output in _trim_domain_pairs(scan_root, domain_filter=domain_filter):
        ext = domain_output / "extractor"
        tsp = ext / TRIM_STATS_FILENAME
        sp = ext / "summary.json"
        mdir = ext / "matches"
        if not mdir.is_dir():
            continue

        if tsp.is_file():
            try:
                tdata = read_json(tsp)
            except (OSError, json.JSONDecodeError, TypeError):
                tdata = {}
            if isinstance(tdata, dict) and _merge_trim_stats_counts(merged, tdata):
                from_stats += 1
                continue

        if not sp.is_file():
            slow_pairs.append((domain_label, domain_output))
            continue

        try:
            sz = sp.stat().st_size
        except OSError:
            slow_pairs.append((domain_label, domain_output))
            continue

        if sz > TRIM_MAX_SUMMARY_JSON_BYTES:
            _trim_progress(
                f"{domain_label!r}: summary.json is {sz:,} B (cap {TRIM_MAX_SUMMARY_JSON_BYTES:,} B) — "
                f"run `python extractor.py --rebuild-trim-stats` (or re-run extract) to add {TRIM_STATS_FILENAME}, "
                f"else this tree will be disk-scanned for trim counts"
            )
            slow_pairs.append((domain_label, domain_output))
            continue

        try:
            data = read_json(sp)
        except (OSError, json.JSONDecodeError, TypeError):
            slow_pairs.append((domain_label, domain_output))
            continue
        if not isinstance(data, dict) or not _summary_rows_ok_for_trim_counts(data):
            slow_pairs.append((domain_label, domain_output))
            continue
        rows = data["rows"]
        if len(rows) == 0 and _matches_dir_has_any_m_json(mdir):
            slow_pairs.append((domain_label, domain_output))
            _trim_progress(
                f"{domain_label!r}: summary has 0 rows but matches/ is non-empty; disk scan needed for counts"
            )
            continue
        mc = data.get("match_count")
        if isinstance(mc, int) and mc != len(rows):
            _trim_progress(
                f"note: {domain_label!r} summary match_count ({mc}) != len(rows) ({len(rows)}); "
                f"using rows only for rule totals"
            )
        for row in rows:
            if not isinstance(row, dict):
                continue
            fn = str(row.get("filter_name", "") or "").strip()
            if fn:
                merged[fn] = merged.get(fn, 0) + 1
        from_summary += 1

    _trim_progress(
        f"count sources: {TRIM_STATS_FILENAME}={from_stats} domain(s), summary.json={from_summary} domain(s); "
        f"{len(slow_pairs)} tree(s) may need disk scan for counts"
    )

    if slow_pairs:
        for i, (domain_label, domain_output) in enumerate(slow_pairs, start=1):
            _trim_progress(f"disk count {i}/{len(slow_pairs)}: {domain_label!r} …")
            files = _collect_extractor_match_json_paths_for_domain(
                domain_output,
                label=f"Listing {domain_label!r} matches",
            )
            if not files:
                continue
            w = max(1, min(int(workers or 1), 32, len(files)))
            chunk_size = max(1, (len(files) + w - 1) // w)
            chunks = [files[j : j + chunk_size] for j in range(0, len(files), chunk_size)]
            n_chunks = len(chunks)
            _trim_progress(
                f"disk count {i}/{len(slow_pairs)} ({domain_label!r}): "
                f"reading {len(files):,} file(s) in {n_chunks} chunk(s) …"
            )
            done_chunks = 0
            with ThreadPoolExecutor(max_workers=n_chunks) as executor:
                futures = [executor.submit(_count_rules_in_chunk, ch) for ch in chunks]
                for fut in as_completed(futures):
                    _merge_rule_counts(merged, fut.result())
                    done_chunks += 1
                    _trim_progress(
                        f"disk count {domain_label!r}: chunk {done_chunks}/{n_chunks} done"
                    )

    return merged, slow_pairs


def _collect_extractor_match_json_paths(
    scan_root: Path,
    *,
    domain_filter: str | None = None,
    label: str = "Listing match files",
) -> list[Path]:
    out: list[Path] = []
    domain_filter_text = str(domain_filter or "").strip().lower()
    _trim_progress(f"{label}: scanning domain trees under {scan_root} …")
    for domain_label, domain_output in discover_pairs(scan_root):
        if domain_filter_text:
            if (
                str(domain_label).strip().lower() != domain_filter_text
                and str(domain_output.name).strip().lower() != domain_filter_text
            ):
                continue
        md = domain_output / "extractor" / "matches"
        if not md.is_dir():
            continue
        try:
            for p in md.iterdir():
                if p.is_file() and p.suffix.lower() == ".json" and p.name.startswith("m_"):
                    out.append(p)
                    n = len(out)
                    if n > 0 and n % TRIM_ENUM_PROGRESS_EVERY == 0:
                        _trim_progress(f"{label}: {n:,} file(s) found so far …")
        except OSError:
            continue
    _trim_progress(f"{label}: done — {len(out):,} match file(s).")
    return out


def _read_match_detail(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8-sig")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _count_rules_in_chunk(paths: list[Path]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for p in paths:
        detail = _read_match_detail(p)
        if not detail:
            continue
        fn = str(detail.get("filter_name", "") or "").strip()
        if fn:
            counts[fn] += 1
    return dict(counts)


def aggregate_rule_match_counts_from_disk(
    scan_root: Path,
    *,
    domain_filter: str | None = None,
    workers: int = 8,
) -> dict[str, int]:
    """Count extractor match files per ``filter_name`` (rule name) under ``scan_root``."""
    files = _collect_extractor_match_json_paths(
        scan_root, domain_filter=domain_filter, label="Listing match files (for counts)"
    )
    if not files:
        return {}
    workers = max(1, min(int(workers or 1), 32, len(files)))
    chunk_size = max(1, (len(files) + workers - 1) // workers)
    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]
    merged: dict[str, int] = defaultdict(int)
    n_chunks = len(chunks)
    _trim_progress(f"Counting rules: processing {len(files):,} file(s) in {n_chunks} parallel chunk(s) …")
    done_chunks = 0
    with ThreadPoolExecutor(max_workers=n_chunks) as executor:
        futures = [executor.submit(_count_rules_in_chunk, ch) for ch in chunks]
        for fut in as_completed(futures):
            part = fut.result()
            done_chunks += 1
            for k, v in part.items():
                merged[k] += v
            _trim_progress(f"Counting rules: chunk {done_chunks}/{n_chunks} done")
    return dict(merged)


def _delete_matches_chunk(paths: list[Path], rule_names: set[str]) -> int:
    removed = 0
    for p in paths:
        detail = _read_match_detail(p)
        if not detail:
            continue
        fn = str(detail.get("filter_name", "") or "").strip()
        if fn not in rule_names:
            continue
        try:
            p.unlink()
            removed += 1
        except OSError:
            pass
    return removed


def delete_extractor_matches_for_rules(
    scan_root: Path,
    rule_names: set[str],
    *,
    domain_filter: str | None = None,
    workers: int = 8,
) -> int:
    """Remove on-disk ``m_*.json`` match records whose ``filter_name`` is in ``rule_names``."""
    if not rule_names:
        return 0
    files = _collect_extractor_match_json_paths(
        scan_root, domain_filter=domain_filter, label="Listing match files (for delete)"
    )
    if not files:
        return 0
    workers = max(1, min(int(workers or 1), 32, len(files)))
    chunk_size = max(1, (len(files) + workers - 1) // workers)
    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]
    total = 0
    n_chunks = len(chunks)
    _trim_progress(
        f"Deleting matches for {len(rule_names)} rule name(s): scanning {len(files):,} file(s) "
        f"in {n_chunks} parallel chunk(s) …"
    )
    done_chunks = 0
    with ThreadPoolExecutor(max_workers=n_chunks) as executor:
        futs = [executor.submit(_delete_matches_chunk, ch, rule_names) for ch in chunks]
        for fut in as_completed(futs):
            total += int(fut.result() or 0)
            done_chunks += 1
            _trim_progress(f"Deleting matches: chunk {done_chunks}/{n_chunks} done ({total:,} file(s) removed so far)")
    return total


def delete_extractor_matches_for_rules_on_tree(
    domain_output: Path,
    rule_names: set[str],
    workers: int,
) -> int:
    """Like :func:`delete_extractor_matches_for_rules` but for a single Fozzy domain output tree."""
    if not rule_names:
        return 0
    files = _collect_extractor_match_json_paths_for_domain(domain_output)
    if not files:
        return 0
    w = max(1, min(int(workers or 1), 32, len(files)))
    chunk_size = max(1, (len(files) + w - 1) // w)
    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]
    total = 0
    n_chunks = len(chunks)
    with ThreadPoolExecutor(max_workers=n_chunks) as executor:
        futs = [executor.submit(_delete_matches_chunk, ch, rule_names) for ch in chunks]
        for fut in as_completed(futs):
            total += int(fut.result() or 0)
    return total


def trim_domain_using_summary_file(
    domain_label: str,
    domain_output: Path,
    remove_names: set[str],
) -> tuple[int, bool, bool]:
    """Remove rows for ``remove_names``, delete their ``match_file`` paths, rewrite summary. No full-disk scan.

    Returns ``(files_removed, success, summary_rewritten)``.
    """
    if not remove_names:
        return 0, True, False
    sp = domain_output / "extractor" / "summary.json"
    if not sp.is_file():
        return 0, False, False
    try:
        data = read_json(sp)
    except (OSError, json.JSONDecodeError, TypeError):
        return 0, False, False
    if not isinstance(data, dict):
        return 0, False, False
    rows_in = data.get("rows")
    if not isinstance(rows_in, list):
        return 0, False, False

    kept: list[dict[str, Any]] = []
    to_unlink: list[Path] = []
    for row in rows_in:
        if not isinstance(row, dict):
            continue
        fn = str(row.get("filter_name", "") or "").strip()
        mf = str(row.get("match_file", "") or "").strip()
        if fn in remove_names:
            if mf:
                to_unlink.append(Path(mf))
            continue
        kept.append(row)

    if len(kept) == len(rows_in) and not to_unlink:
        return 0, True, False

    removed_files = 0
    for p in to_unlink:
        try:
            existed = p.is_file()
            p.unlink(missing_ok=True)
            if existed:
                removed_files += 1
        except OSError:
            pass

    data["rows"] = kept
    data["match_count"] = len(kept)
    data["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    sp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    write_trim_stats_from_rows(domain_output / "extractor", domain_label, kept)
    _trim_progress(
        f"updated summary for {domain_label!r}: removed {removed_files:,} match file(s); {len(kept):,} row(s) left"
    )
    return removed_files, True, True


def detail_record_to_summary_row(detail: dict[str, Any]) -> dict[str, Any]:
    matched = str(detail.get("matched_text", "") or "")
    preview = matched.replace("\n", " ").strip()
    if len(preview) > 160:
        preview = preview[:157] + "..."
    match_file = str(detail.get("match_file", "") or "").strip()
    if not match_file:
        match_file = ""
    return {
        "domain_label": str(detail.get("domain_label", "") or ""),
        "url": str(detail.get("url", "") or ""),
        "filter_name": str(detail.get("filter_name", "") or ""),
        "importance_score": int(detail.get("importance_score", 0) or 0),
        "scope": str(detail.get("scope", "") or ""),
        "response_side": str(detail.get("response_side", "") or ""),
        "match_preview": preview,
        "result_file": str(detail.get("result_file", "") or ""),
        "match_file": match_file,
        "result_type": str(detail.get("result_type", "") or ""),
    }


def rebuild_extractor_summary_for_domain(
    domain_label: str,
    domain_output: Path,
    *,
    progress_every: int | None = TRIM_REBUILD_PROGRESS_EVERY,
) -> Path | None:
    """Rebuild ``extractor/summary.json`` from remaining ``extractor/matches/m_*.json`` files."""
    extractor_root = domain_output / "extractor"
    matches_dir = extractor_root / "matches"
    rows_out: list[dict[str, Any]] = []
    if matches_dir.is_dir():
        paths = sorted(matches_dir.glob("m_*.json"))
        total_paths = len(paths)
        for idx, path in enumerate(paths, start=1):
            detail = _read_match_detail(path)
            if not detail:
                continue
            row = detail_record_to_summary_row(detail)
            row["match_file"] = str(path.resolve())
            rows_out.append(row)
            if (
                progress_every
                and total_paths > 0
                and idx > 0
                and idx % progress_every == 0
            ):
                _trim_progress(
                    f"rebuild {domain_label!r}: scanned {idx:,}/{total_paths:,} match file(s) …"
                )
    match_count = len(rows_out)
    summary_path = extractor_root / "summary.json"
    summary_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "domain_label": domain_label,
        "domain_output": str(domain_output.resolve()),
        "match_count": match_count,
        "rows": rows_out,
    }
    ensure_directory(extractor_root)
    summary_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    write_trim_stats_from_rows(extractor_root, domain_label, rows_out)
    return summary_path


def run_rebuild_trim_stats(
    scan_root: Path,
    *,
    domain_filter: str | None = None,
    workers: int = 8,
) -> int:
    """Create ``extractor/trim_stats.json`` per domain so ``--trim`` never loads huge ``summary.json``."""
    scan_root = scan_root.resolve()
    print(f"[extractor] rebuilding {TRIM_STATS_FILENAME} under {scan_root} …", flush=True)
    n = 0
    for domain_label, domain_output in _trim_domain_pairs(scan_root, domain_filter=domain_filter):
        ext = domain_output / "extractor"
        sp = ext / "summary.json"
        tsp = ext / TRIM_STATS_FILENAME
        mdir = ext / "matches"
        if not mdir.is_dir():
            continue
        if tsp.is_file():
            continue
        if sp.is_file():
            try:
                sz = sp.stat().st_size
            except OSError:
                continue
            if sz <= TRIM_MAX_SUMMARY_JSON_BYTES:
                try:
                    data = read_json(sp)
                except (OSError, json.JSONDecodeError, TypeError):
                    continue
                rows = data.get("rows") if isinstance(data, dict) else None
                if not isinstance(rows, list):
                    continue
                write_trim_stats_from_rows(ext, domain_label, rows)
                print(
                    f"[extractor] {domain_label!r}: wrote {TRIM_STATS_FILENAME} from summary ({len(rows):,} rows)",
                    flush=True,
                )
                n += 1
                continue
            print(
                f"[extractor] {domain_label!r}: large summary ({sz:,} B); counting match files …",
                flush=True,
            )
        elif not _matches_dir_has_any_m_json(mdir):
            continue
        else:
            print(f"[extractor] {domain_label!r}: no summary.json; counting match files …", flush=True)

        files = _collect_extractor_match_json_paths_for_domain(
            domain_output,
            label=f"listing {domain_label!r}",
        )
        if not files:
            continue
        w = max(1, min(int(workers or 1), 32, len(files)))
        chunk_size = max(1, (len(files) + w - 1) // w)
        chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]
        sub: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
            for part in executor.map(_count_rules_in_chunk, chunks):
                _merge_rule_counts(sub, part)
        total_hits = sum(sub.values())
        write_trim_stats_from_counts(
            ext,
            domain_label,
            counts_by_filter_name=sub,
            row_count=total_hits,
        )
        print(
            f"[extractor] {domain_label!r}: wrote {TRIM_STATS_FILENAME} from disk ({total_hits:,} match file(s))",
            flush=True,
        )
        n += 1
    print(f"[extractor] wrote {n} {TRIM_STATS_FILENAME} file(s).", flush=True)
    return 0


def rebuild_all_extractor_summaries_after_trim(scan_root: Path, *, domain_filter: str | None = None) -> int:
    domain_filter_text = str(domain_filter or "").strip().lower()
    to_rebuild: list[tuple[str, Path]] = []
    for domain_label, domain_output in discover_pairs(scan_root):
        if domain_filter_text:
            if (
                str(domain_label).strip().lower() != domain_filter_text
                and str(domain_output.name).strip().lower() != domain_filter_text
            ):
                continue
        ext = domain_output / "extractor"
        if not ext.is_dir():
            continue
        to_rebuild.append((domain_label, domain_output))
    n_total = len(to_rebuild)
    if n_total == 0:
        return 0
    _trim_progress(f"Rebuilding extractor/summary.json for {n_total} domain tree(s) …")
    for i, (domain_label, domain_output) in enumerate(to_rebuild, start=1):
        _trim_progress(f"Rebuilding summaries: {i}/{n_total} — {domain_label!r}")
        rebuild_extractor_summary_for_domain(domain_label, domain_output)
    return n_total


def load_wordlist_rule_names(wordlist_path: Path) -> set[str]:
    if not wordlist_path.is_file():
        return set()
    try:
        raw = wordlist_path.read_text(encoding="utf-8-sig")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(data, list):
        return set()
    names: set[str] = set()
    for item in data:
        if isinstance(item, dict):
            nm = str(item.get("name", "") or "").strip()
            if nm:
                names.add(nm)
    return names


def remove_rules_from_wordlist(wordlist_path: Path, names_to_remove: set[str], *, backup: bool = True) -> tuple[int, int]:
    """Drop rules whose ``name`` is in ``names_to_remove``. Returns (removed_rule_count, remaining_rule_count)."""
    if not names_to_remove:
        return 0, 0
    raw = wordlist_path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"Wordlist must be a JSON array: {wordlist_path}")
    if backup:
        bak = wordlist_path.with_suffix(wordlist_path.suffix + ".trim.bak")
        bak.write_text(raw, encoding="utf-8")
    kept: list[Any] = []
    removed = 0
    for item in data:
        if not isinstance(item, dict):
            kept.append(item)
            continue
        nm = str(item.get("name", "") or "").strip()
        if nm in names_to_remove:
            removed += 1
            continue
        kept.append(item)
    tmp = wordlist_path.with_suffix(wordlist_path.suffix + ".tmp")
    tmp.write_text(json.dumps(kept, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(wordlist_path)
    return removed, len([x for x in kept if isinstance(x, dict) and str(x.get("name", "")).strip()])


def parse_trim_selection(line: str, ordered_rules: list[tuple[str, int]]) -> set[str]:
    """Map user input (1-based indices or rule names) to rule names."""
    names_out: set[str] = set()
    parts = [p.strip() for p in line.replace(";", ",").split(",") if p.strip()]
    if not parts:
        return names_out
    by_index = {str(i + 1): ordered_rules[i][0] for i in range(len(ordered_rules))}
    known_names = {name for name, _ in ordered_rules}
    for p in parts:
        low = p.lower()
        if p.isdigit() or (low.startswith("#") and low[1:].isdigit()):
            key = low[1:] if low.startswith("#") else p
            nm = by_index.get(key)
            if nm:
                names_out.add(nm)
            continue
        if p in known_names:
            names_out.add(p)
            continue
        hits = [n for n in known_names if n.lower() == low]
        if len(hits) == 1:
            names_out.add(hits[0])
    return names_out


def parse_trim_remove_cli(spec: str) -> set[str]:
    out: set[str] = set()
    for part in spec.replace(";", ",").split(","):
        t = part.strip()
        if t:
            out.add(t)
    return out


def apply_trim_effects(
    scan_root: Path,
    remove_names: set[str],
    *,
    domain_filter: str | None,
    workers: int,
    trim_disk_scan: bool,
    slow_pairs: list[tuple[str, Path]],
) -> tuple[int, int]:
    """Delete match JSON and refresh summaries. Returns ``(files_removed, domain_trees_updated)``."""
    if not remove_names:
        return 0, 0

    if trim_disk_scan:
        deleted = delete_extractor_matches_for_rules(
            scan_root, remove_names, domain_filter=domain_filter, workers=workers
        )
        rebuilt = rebuild_all_extractor_summaries_after_trim(scan_root, domain_filter=domain_filter)
        return deleted, rebuilt

    slow_set = {lbl for lbl, _ in slow_pairs}
    deleted = 0
    touched = 0
    for domain_label, domain_output in _trim_domain_pairs(scan_root, domain_filter=domain_filter):
        ext = domain_output / "extractor"
        if not ext.is_dir():
            continue
        if domain_label in slow_set:
            _trim_progress(f"delete + rebuild (disk): {domain_label!r} …")
            deleted += delete_extractor_matches_for_rules_on_tree(domain_output, remove_names, workers)
            rebuild_extractor_summary_for_domain(domain_label, domain_output)
            touched += 1
            continue
        n, ok, wrote = trim_domain_using_summary_file(domain_label, domain_output, remove_names)
        if ok:
            deleted += n
            if wrote:
                touched += 1
            continue
        _trim_progress(f"summary-based trim failed for {domain_label!r}; using disk delete …")
        deleted += delete_extractor_matches_for_rules_on_tree(domain_output, remove_names, workers)
        rebuild_extractor_summary_for_domain(domain_label, domain_output)
        touched += 1

    return deleted, touched


def run_trim_mode(
    scan_root: Path,
    wordlist_path: Path,
    *,
    domain_filter: str | None = None,
    workers: int = 8,
    trim_remove: str | None = None,
    trim_yes: bool = False,
    no_backup: bool = False,
    trim_disk_scan: bool = False,
) -> int:
    scan_root = scan_root.resolve()
    _trim_progress(f"starting — scan_root={scan_root} workers={workers}")
    if trim_disk_scan:
        _trim_progress("mode: full disk scan (per-match JSON read) — use only if summaries are untrustworthy")
    else:
        _trim_progress("mode: fast (per-domain extractor/summary.json + targeted deletes)")
    if domain_filter:
        _trim_progress(f"domain filter={domain_filter!r}")
    wl_names = load_wordlist_rule_names(wordlist_path)

    if trim_disk_scan:
        counts = aggregate_rule_match_counts_from_disk(
            scan_root, domain_filter=domain_filter, workers=workers
        )
        slow_pairs: list[tuple[str, Path]] = []
    else:
        counts, slow_pairs = aggregate_rule_match_counts_hybrid(
            scan_root, domain_filter=domain_filter, workers=workers, force_disk_scan=False
        )
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))

    total_matches = sum(counts.values())
    _trim_progress(
        f"aggregate counts done: {total_matches:,} match file(s) across {len(counts)} rule name(s)"
    )
    if not ordered and not trim_remove:
        print("[extractor:trim] No extractor match files found; nothing to trim.", flush=True)
        return 0

    # Table: rank, matches, rule name
    width = len(str(len(ordered))) if ordered else 1
    for i, (name, n) in enumerate(ordered, start=1):
        in_wl = "yes" if name in wl_names else "no"
        print(f"[extractor:trim] {i:>{width}}  {n:>10}  {name!r}  (in wordlist: {in_wl})", flush=True)

    remove_names: set[str] = set()
    if trim_remove:
        remove_names = parse_trim_remove_cli(trim_remove)
        unknown = sorted(nm for nm in remove_names if nm not in counts and nm not in wl_names)
        if unknown:
            print(f"[extractor:trim] warning: not found in match counts or wordlist (will still try wordlist delete): {unknown}", flush=True)
    else:
        if not sys.stdin.isatty():
            print(
                "[extractor:trim] stdin is not a TTY; specify rules to remove with --trim-remove 'name1,name2' "
                "or use --trim-yes with --trim-remove for non-interactive use.",
                flush=True,
            )
            return 1
        print(
            "[extractor:trim] Enter rules to remove: comma-separated numbers (from the list above) and/or exact rule names.",
            flush=True,
        )
        print("[extractor:trim] Empty line cancels.", flush=True)
        try:
            line = input("[extractor:trim] > ").strip()
        except EOFError:
            line = ""
        if not line:
            print("[extractor:trim] cancelled.", flush=True)
            return 0
        remove_names = parse_trim_selection(line, ordered)

    if not remove_names:
        print("[extractor:trim] No rules selected; nothing to do.", flush=True)
        return 0

    print(f"[extractor:trim] will remove {len(remove_names)} rule name(s) from wordlist and delete their match files:", flush=True)
    for nm in sorted(remove_names):
        print(f"  - {nm!r} ({counts.get(nm, 0)} match file(s) on disk)", flush=True)

    if not trim_yes:
        if not sys.stdin.isatty():
            print("[extractor:trim] add --trim-yes to confirm when using --trim-remove non-interactively.", flush=True)
            return 1
        try:
            ans = input("[extractor:trim] Proceed? [y/N]: ").strip().lower()
        except EOFError:
            ans = ""
        if ans not in {"y", "yes"}:
            print("[extractor:trim] aborted.", flush=True)
            return 0

    removed_rules, remaining_rules = remove_rules_from_wordlist(
        wordlist_path, remove_names, backup=not no_backup
    )
    print(f"[extractor:trim] wordlist updated: removed {removed_rules} rule block(s); ~{remaining_rules} named rules remain.", flush=True)
    if not no_backup:
        print(f"[extractor:trim] wordlist backup: {wordlist_path.with_suffix(wordlist_path.suffix + '.trim.bak')}", flush=True)

    _trim_progress("applying deletions and updating extractor summaries …")
    deleted, rebuilt = apply_trim_effects(
        scan_root,
        remove_names,
        domain_filter=domain_filter,
        workers=workers,
        trim_disk_scan=trim_disk_scan,
        slow_pairs=slow_pairs if not trim_disk_scan else [],
    )
    print(f"[extractor:trim] deleted {deleted} match file(s).", flush=True)

    print(f"[extractor:trim] updated extractor summary.json for {rebuilt} domain tree(s).", flush=True)
    print(
        "[extractor:trim] done. Re-run `python extractor.py --force` if you want to re-scan Fozzy results with the new wordlist.",
        flush=True,
    )
    return 0


def run_extractors_for_domain(
    domain_label: str,
    domain_output: Path,
    rules: list[dict[str, Any]],
) -> tuple[int, Path]:
    """Rewrite ``extractor/matches`` and ``extractor/summary.json`` for one Fozzy tree."""
    results_dir = domain_output / "results"
    legacy = domain_output / "anomalies"
    scan_dirs = [results_dir]
    if legacy.is_dir():
        scan_dirs.append(legacy)

    entries = load_result_entries_from_folders(scan_dirs, source_domain=domain_label)
    extractor_root = domain_output / "extractor"
    matches_dir = extractor_root / "matches"
    if matches_dir.exists():
        shutil.rmtree(matches_dir, ignore_errors=True)
    ensure_directory(matches_dir)

    rows_out: list[dict[str, Any]] = []
    match_count = 0

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        n, rows = apply_extractor_rules_to_entry(entry, domain_label, rules, matches_dir)
        match_count += n
        rows_out.extend(rows)

    summary_path = extractor_root / "summary.json"
    summary_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "domain_label": domain_label,
        "domain_output": str(domain_output.resolve()),
        "match_count": match_count,
        "rows": rows_out,
    }
    ensure_directory(extractor_root)
    summary_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    write_trim_stats_from_rows(extractor_root, domain_label, rows_out)
    return match_count, summary_path


def discover_pairs(scan_root: Path) -> list[tuple[str, Path]]:
    scan_root = scan_root.resolve()
    nested = master_report_uses_nested_layout(scan_root)
    if nested:
        return discover_fozzy_domain_output_pairs_nested(scan_root)
    return discover_fozzy_domain_output_pairs(scan_root)


def default_extractor_config() -> dict[str, Any]:
    return {
        "scan_root": "output",
        "wordlist": "resources/wordlists/extractor_list.txt",
        "javascript_wordlist": "resources/wordlists/javascript_extractor_list.txt",
        "skip_javascript_extractor": False,
        "verbose": False,
        "workers": 4,
        "force": False,
        "domain": None,
        "log_file": None,
    }


def read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(parsed, dict):
        raise ValueError(f"Extractor config file must contain a JSON object: {path}")
    return parsed


def resolve_config_path(path_value: str) -> Path:
    p = Path(str(path_value or "").strip() or "extractor.json")
    if p.is_absolute():
        return p
    if p.parts and p.parts[0].lower() == "config":
        return EXTRACTOR_BASE / p
    return EXTRACTOR_BASE / "config" / p


def merged_value(cli_value: Any, config: dict[str, Any], key: str, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if key in config:
        return config[key]
    return default


def normalize_workers(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 4
    return max(1, parsed)


def _extractor_vprint(verbose: bool, message: str) -> None:
    """Status line when ``--verbose`` is set (mirrored to extractor.log via tee)."""
    if not verbose:
        return
    print(f"[extractor:v] {message}", flush=True)


def run_extractor_scan(
    scan_root: Path,
    wordlist_path: Path,
    *,
    force: bool = False,
    domain_filter: str | None = None,
    workers: int = 1,
    javascript_wordlist_path: Path | None = None,
    skip_javascript_extractor: bool = False,
    verbose: bool = False,
) -> int:
    scan_root = scan_root.resolve()
    state_path = scan_root / STATE_FILE_NAME
    state = load_extractor_incremental_state(state_path)
    rules = load_extractor_rules(wordlist_path)
    if not rules:
        print("[extractor] No valid rules loaded; exiting.", flush=True)
        return 1

    _extractor_vprint(
        verbose,
        f"loaded {len(rules)} Fozzy rule(s) from {wordlist_path}",
    )
    pairs = discover_pairs(scan_root)
    domain_filter_text = str(domain_filter or "").strip().lower()
    if domain_filter_text:
        pairs = [
            (domain_label, domain_output)
            for domain_label, domain_output in pairs
            if str(domain_label).strip().lower() == domain_filter_text
            or str(domain_output.name).strip().lower() == domain_filter_text
        ]
        if not pairs:
            print(
                f"[extractor] No matching domain output found for domain filter: {domain_filter_text}",
                flush=True,
            )
            return 1
        _extractor_vprint(verbose, f"domain filter {domain_filter_text!r} → {len(pairs)} tree(s)")
    if not pairs:
        print(
            f"[extractor] No domain output trees under {scan_root} "
            "(expected Fozzy results/extractor, or Nightmare ``collected_data/`` per site).",
            flush=True,
        )
        return 0

    _extractor_vprint(verbose, f"discovered {len(pairs)} domain tree(s) under {scan_root}")
    if verbose and pairs:
        max_list = 25
        for idx, (lbl, outp) in enumerate(pairs[:max_list], start=1):
            _extractor_vprint(verbose, f"  {idx}. {lbl!r} → {outp}")
        if len(pairs) > max_list:
            _extractor_vprint(verbose, f"  … and {len(pairs) - max_list} more")

    processed = 0
    skipped = 0
    to_process: list[tuple[str, Path]] = []
    fozzy_skip_v_shown = 0
    fozzy_skip_v_cap = 30
    for domain_label, domain_output in pairs:
        key = domain_label
        if not force and not domain_results_dirty(domain_output, state, key):
            skipped += 1
            if verbose and fozzy_skip_v_shown < fozzy_skip_v_cap:
                rdir = domain_output / "results"
                _extractor_vprint(
                    verbose,
                    f"Fozzy skip (unchanged): {domain_label!r} results_dir={rdir} exists={rdir.is_dir()}",
                )
                fozzy_skip_v_shown += 1
            continue
        to_process.append((domain_label, domain_output))
    if verbose and skipped > fozzy_skip_v_shown:
        _extractor_vprint(
            verbose,
            f"Fozzy skip: … and {skipped - fozzy_skip_v_shown} more unchanged domain(s) not listed",
        )

    print(
        f"[extractor] Starting scan: domains_to_process={len(to_process)}, skipped_unchanged={skipped}, workers={workers}",
        flush=True,
    )
    if to_process and verbose:
        _extractor_vprint(
            verbose,
            f"Fozzy phase: processing {len(to_process)} domain(s) with workers={workers}",
        )

    failures = 0
    if workers <= 1:
        for domain_label, domain_output in to_process:
            _extractor_vprint(verbose, f"Fozzy run starting: {domain_label!r}")
            print(f"[extractor] {domain_label} … ({domain_output})", flush=True)
            n, summary_path = run_extractors_for_domain(domain_label, domain_output, rules)
            print(f"[extractor]   wrote {n} match file(s); {summary_path}", flush=True)
            results = domain_output / "results"
            if results.is_dir():
                if "domains" not in state or not isinstance(state["domains"], dict):
                    state["domains"] = {}
                state["domains"][domain_label] = {
                    "results_max_mtime_ns": folder_tree_max_mtime_ns(results),
                }
            save_extractor_incremental_state(state_path, state)
            processed += 1
            _extractor_vprint(
                verbose,
                f"Fozzy done: {domain_label!r} → {n} match file(s), state saved",
            )
    else:

        def _run_one(domain_label: str, domain_output: Path) -> tuple[str, Path, int, Path]:
            n, summary_path = run_extractors_for_domain(domain_label, domain_output, rules)
            return domain_label, domain_output, n, summary_path

        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_map = {
                    executor.submit(_run_one, domain_label, domain_output): (domain_label, domain_output)
                    for domain_label, domain_output in to_process
                }
                n_total = len(future_map)
                done_n = 0
                for fut in as_completed(future_map):
                    domain_label, domain_output = future_map[fut]
                    try:
                        done_domain, done_output, n, summary_path = fut.result()
                    except Exception as exc:
                        failures += 1
                        print(f"[extractor] {domain_label} worker error: {exc}", flush=True)
                        _extractor_vprint(verbose, f"Fozzy worker error on {domain_label!r}: {exc}")
                        continue
                    done_n += 1
                    _extractor_vprint(
                        verbose,
                        f"Fozzy progress {done_n}/{n_total}: {done_domain!r} → {n} match file(s)",
                    )
                    print(f"[extractor] {done_domain} … ({done_output})", flush=True)
                    print(f"[extractor]   wrote {n} match file(s); {summary_path}", flush=True)
                    results = done_output / "results"
                    if results.is_dir():
                        if "domains" not in state or not isinstance(state["domains"], dict):
                            state["domains"] = {}
                        state["domains"][done_domain] = {
                            "results_max_mtime_ns": folder_tree_max_mtime_ns(results),
                        }
                    save_extractor_incremental_state(state_path, state)
                    processed += 1
                    _extractor_vprint(
                        verbose,
                        f"incremental state updated for {done_domain!r}",
                    )
        except KeyboardInterrupt:
            print("[extractor] Interrupt received; stopping worker pool with partial outputs.", flush=True)
            return 130

        if failures > 0:
            print(f"[extractor] Completed with worker failures={failures}", flush=True)

    js_state_path = scan_root / JAVASCRIPT_STATE_FILE_NAME
    jwp = javascript_wordlist_path or DEFAULT_JAVASCRIPT_WORDLIST
    jwp = jwp.expanduser()
    if not jwp.is_absolute():
        jwp = (EXTRACTOR_BASE / jwp).resolve()
    else:
        jwp = jwp.resolve()

    if skip_javascript_extractor:
        print("[extractor:js] skipped (--skip-javascript-extractor / config).", flush=True)
        _extractor_vprint(verbose, "JavaScript pass disabled by flag/config")
    elif not jwp.is_file():
        print(f"[extractor:js] wordlist not found ({jwp}); skipping JavaScript pass.", flush=True)
        _extractor_vprint(verbose, f"missing JS wordlist path: {jwp}")
    else:
        try:
            js_rules = load_extractor_rules(jwp)
        except (ValueError, json.JSONDecodeError) as exc:
            print(f"[extractor:js] invalid wordlist ({jwp}): {exc}; skipping.", flush=True)
            _extractor_vprint(verbose, f"JS wordlist parse error: {exc}")
        else:
            if not js_rules:
                print("[extractor:js] no rules passed validation; skipping.", flush=True)
            else:
                _extractor_vprint(
                    verbose,
                    f"JavaScript phase: {len(js_rules)} rule(s), wordlist fp from {jwp.name}",
                )
                js_fp = wordlist_fingerprint(jwp)
                _extractor_vprint(verbose, f"JS incremental state: {js_state_path}")
                js_state = load_javascript_extractor_state(js_state_path)
                js_skipped = 0
                js_processed = 0
                js_skip_v_shown = 0
                js_skip_v_cap = 30
                for domain_label, domain_output in pairs:
                    if not javascript_extractor_domain_needs_work(
                        domain_label, domain_output, js_state, js_fp, force
                    ):
                        js_skipped += 1
                        if verbose and js_skip_v_shown < js_skip_v_cap:
                            scripts_root = collected_scripts_root(domain_output)
                            if not scripts_root.is_dir():
                                _extractor_vprint(
                                    verbose,
                                    f"JS skip: {domain_label!r} (no {scripts_root})",
                                )
                            else:
                                _extractor_vprint(
                                    verbose,
                                    f"JS skip: {domain_label!r} (scripts up to date vs state/wordlist)",
                                )
                            js_skip_v_shown += 1
                        continue
                    _extractor_vprint(verbose, f"JS run starting: {domain_label!r}")
                    print(f"[extractor:js] {domain_label} … ({domain_output})", flush=True)
                    n_js, js_summary = run_javascript_extractor_for_domain(
                        domain_label,
                        domain_output,
                        js_rules,
                        js_state,
                        wordlist_path=jwp,
                        wordlist_fp=js_fp,
                        force=force,
                    )
                    save_javascript_extractor_state(js_state_path, js_state)
                    js_processed += 1
                    summ = js_summary or ""
                    print(
                        f"[extractor:js]   wrote {n_js} match(es) from re-scanned script(s); {summ}",
                        flush=True,
                    )
                    _extractor_vprint(
                        verbose,
                        f"JS done: {domain_label!r} → {n_js} new match(es); state written",
                    )
                if verbose and js_skipped > js_skip_v_shown:
                    _extractor_vprint(
                        verbose,
                        f"JS skip: … and {js_skipped - js_skip_v_shown} more domain(s) not listed",
                    )
                print(
                    f"[extractor:js] Done. domains_processed={js_processed}, skipped_unchanged={js_skipped}, "
                    f"state={js_state_path}",
                    flush=True,
                )

    print(
        f"[extractor] Done. Domains processed={processed}, skipped (unchanged)={skipped}, "
        f"state={state_path}",
        flush=True,
    )
    if workers > 1 and failures > 0:
        return 1
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Regex extractor over Fozzy result JSON (incremental by domain).")
    p.add_argument(
        "domain",
        nargs="?",
        default=None,
        help="Optional domain label to process only one domain output tree (for example lillylibrary.org).",
    )
    p.add_argument(
        "--config",
        default="extractor.json",
        help="Extractor config file path (default: config/extractor.json). Relative paths resolve under config/.",
    )
    p.add_argument(
        "--scan-root",
        default=None,
        help=f"Root to scan for Fozzy outputs (default: {DEFAULT_SCAN_ROOT})",
    )
    p.add_argument(
        "--wordlist",
        default=None,
        help=f"JSON array of extractor rules (default: {DEFAULT_WORDLIST})",
    )
    p.add_argument(
        "--javascript-wordlist",
        default=None,
        metavar="PATH",
        help=(
            f"JSON array of rules for collected_data/scripts (default: {DEFAULT_JAVASCRIPT_WORDLIST}). "
            "State: <scan-root>/javascript_extractor_state.json"
        ),
    )
    p.add_argument(
        "--skip-javascript-extractor",
        action="store_true",
        default=None,
        help="Do not run the JavaScript/collected_data/scripts regex pass.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel extractor domain workers.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        default=None,
        help="Re-run extractors for every domain regardless of incremental state.",
    )
    p.add_argument(
        "--log-file",
        default=None,
        help=(
            "Optional extractor log file path. Relative paths resolve from repo root. "
            "Default: <scan-root>/extractor.log"
        ),
    )
    p.add_argument(
        "--trim",
        action="store_true",
        help=(
            "List each rule's on-disk match file count (highest first), then remove chosen rules from the "
            "wordlist, delete their match JSON files, and rebuild per-domain extractor/summary.json."
        ),
    )
    p.add_argument(
        "--trim-remove",
        default=None,
        metavar="NAMES",
        help="With --trim: comma-separated rule names to remove without typing (use --trim-yes to confirm).",
    )
    p.add_argument(
        "--trim-yes",
        action="store_true",
        help="With --trim --trim-remove: skip the final confirmation prompt.",
    )
    p.add_argument(
        "--trim-no-backup",
        action="store_true",
        help="With --trim: do not write <wordlist>.trim.bak before saving the wordlist.",
    )
    p.add_argument(
        "--trim-disk-scan",
        action="store_true",
        help=(
            "With --trim: read every extractor/matches/m_*.json (slow, legacy). "
            "Default uses per-domain extractor/summary.json for counts and deletes."
        ),
    )
    p.add_argument(
        "--rebuild-trim-stats",
        action="store_true",
        help=(
            "Write extractor/trim_stats.json for each domain (fast rule totals for --trim) without re-running "
            "regex extraction. Skips trees that already have trim_stats.json."
        ),
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=None,
        help="Print detailed progress (discovered trees, skips, per-phase status).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config_path = resolve_config_path(str(args.config))
    file_config = read_json_file(config_path)
    effective_config = {**default_extractor_config(), **file_config}

    scan_root_raw = str(merged_value(args.scan_root, effective_config, "scan_root", str(DEFAULT_SCAN_ROOT)))
    wordlist_raw = str(merged_value(args.wordlist, effective_config, "wordlist", str(DEFAULT_WORDLIST)))
    js_wordlist_raw = str(
        merged_value(
            getattr(args, "javascript_wordlist", None),
            effective_config,
            "javascript_wordlist",
            str(DEFAULT_JAVASCRIPT_WORDLIST),
        )
    )
    workers = normalize_workers(merged_value(args.workers, effective_config, "workers", 4))
    force = bool(merged_value(args.force, effective_config, "force", False))
    domain_filter = merged_value(args.domain, effective_config, "domain", None)
    log_file_raw = str(merged_value(args.log_file, effective_config, "log_file", "") or "").strip()
    skip_js = bool(
        merged_value(
            getattr(args, "skip_javascript_extractor", None),
            effective_config,
            "skip_javascript_extractor",
            False,
        )
    )
    verbose = bool(
        merged_value(getattr(args, "verbose", None), effective_config, "verbose", False),
    )

    scan_root = Path(scan_root_raw).expanduser()
    if not scan_root.is_absolute():
        scan_root = (EXTRACTOR_BASE / scan_root).resolve()
    wordlist = Path(wordlist_raw).expanduser()
    if not wordlist.is_absolute():
        wordlist = (EXTRACTOR_BASE / wordlist).resolve()
    javascript_wordlist = Path(js_wordlist_raw).expanduser()
    if not javascript_wordlist.is_absolute():
        javascript_wordlist = (EXTRACTOR_BASE / javascript_wordlist).resolve()
    else:
        javascript_wordlist = javascript_wordlist.resolve()

    if bool(getattr(args, "rebuild_trim_stats", False)):
        domain_filter_text = str(domain_filter).strip() if domain_filter is not None else ""
        return run_rebuild_trim_stats(
            scan_root,
            domain_filter=domain_filter_text or None,
            workers=workers,
        )

    if bool(getattr(args, "trim", False)):
        domain_filter_text = str(domain_filter).strip() if domain_filter is not None else ""
        return run_trim_mode(
            scan_root,
            wordlist,
            domain_filter=domain_filter_text or None,
            workers=workers,
            trim_remove=str(args.trim_remove).strip() if args.trim_remove else None,
            trim_yes=bool(getattr(args, "trim_yes", False)),
            no_backup=bool(getattr(args, "trim_no_backup", False)),
            trim_disk_scan=bool(getattr(args, "trim_disk_scan", False)),
        )

    if log_file_raw:
        log_path = Path(log_file_raw).expanduser()
        if not log_path.is_absolute():
            log_path = (EXTRACTOR_BASE / log_path).resolve()
        else:
            log_path = log_path.resolve()
    else:
        log_path = (scan_root / "extractor.log").resolve()
    install_extractor_log_tee(log_path)
    print(
        f"[extractor] config={config_path.resolve()} scan_root={scan_root} wordlist={wordlist} "
        f"javascript_wordlist={javascript_wordlist} skip_js={skip_js} verbose={verbose} "
        f"workers={workers} force={force} domain={domain_filter}",
        flush=True,
    )
    try:
        return run_extractor_scan(
            scan_root,
            wordlist,
            force=force,
            domain_filter=str(domain_filter).strip() if domain_filter is not None else None,
            workers=workers,
            javascript_wordlist_path=javascript_wordlist,
            skip_javascript_extractor=skip_js,
            verbose=verbose,
        )
    except FileNotFoundError as exc:
        print(f"[extractor] {exc}", flush=True)
        return 1
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"[extractor] {exc}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
