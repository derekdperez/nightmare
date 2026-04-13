#!/usr/bin/env python3
"""Run regex extractors over Fozzy anomaly/reflection JSON (response bodies and metadata).

Scans ``--scan-root`` (default: ``output`` next to this repo) for Fozzy domain trees, the same way
the master report does. When a domain's ``results/`` tree has newer files than recorded in
``.extractor_incremental_state.json``, that domain is re-processed: each match is written under
``<domain_output>/extractor/matches/`` and a ``summary.json`` is refreshed for the master HTML report.

Usage:
    python extractor.py
    python extractor.py --scan-root ./output --force
    python extractor.py lillylibrary.org
    python extractor.py --wordlist path/to/extractor_list.txt
    python extractor.py --trim
    python extractor.py --trim --trim-remove "noisy_rule,other_rule" --trim-yes
"""

from __future__ import annotations

import argparse
import atexit
import hashlib
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
DEFAULT_SCAN_ROOT = EXTRACTOR_BASE / "output"
DEFAULT_CONFIG_PATH = EXTRACTOR_BASE / "config" / "extractor.json"
STATE_FILE_NAME = ".extractor_incremental_state.json"
MAX_MATCHES_PER_RULE_PER_RESULT_FILE = 300

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
            compiled = re.compile(regex)
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


def _collect_extractor_match_json_paths(
    scan_root: Path,
    *,
    domain_filter: str | None = None,
) -> list[Path]:
    out: list[Path] = []
    domain_filter_text = str(domain_filter or "").strip().lower()
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
        except OSError:
            continue
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
    files = _collect_extractor_match_json_paths(scan_root, domain_filter=domain_filter)
    if not files:
        return {}
    workers = max(1, min(int(workers or 1), 32, len(files)))
    chunk_size = max(1, (len(files) + workers - 1) // workers)
    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]
    merged: dict[str, int] = defaultdict(int)
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        for part in executor.map(_count_rules_in_chunk, chunks):
            for k, v in part.items():
                merged[k] += v
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
    files = _collect_extractor_match_json_paths(scan_root, domain_filter=domain_filter)
    if not files:
        return 0
    workers = max(1, min(int(workers or 1), 32, len(files)))
    chunk_size = max(1, (len(files) + workers - 1) // workers)
    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]
    total = 0
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        futs = [executor.submit(_delete_matches_chunk, ch, rule_names) for ch in chunks]
        for fut in as_completed(futs):
            total += int(fut.result() or 0)
    return total


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


def rebuild_extractor_summary_for_domain(domain_label: str, domain_output: Path) -> Path | None:
    """Rebuild ``extractor/summary.json`` from remaining ``extractor/matches/m_*.json`` files."""
    extractor_root = domain_output / "extractor"
    matches_dir = extractor_root / "matches"
    rows_out: list[dict[str, Any]] = []
    if matches_dir.is_dir():
        for path in sorted(matches_dir.glob("m_*.json")):
            detail = _read_match_detail(path)
            if not detail:
                continue
            row = detail_record_to_summary_row(detail)
            row["match_file"] = str(path.resolve())
            rows_out.append(row)
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
    return summary_path


def rebuild_all_extractor_summaries_after_trim(scan_root: Path, *, domain_filter: str | None = None) -> int:
    n = 0
    domain_filter_text = str(domain_filter or "").strip().lower()
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
        rebuild_extractor_summary_for_domain(domain_label, domain_output)
        n += 1
    return n


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


def run_trim_mode(
    scan_root: Path,
    wordlist_path: Path,
    *,
    domain_filter: str | None = None,
    workers: int = 8,
    trim_remove: str | None = None,
    trim_yes: bool = False,
    no_backup: bool = False,
) -> int:
    scan_root = scan_root.resolve()
    counts = aggregate_rule_match_counts_from_disk(scan_root, domain_filter=domain_filter, workers=workers)
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))
    wl_names = load_wordlist_rule_names(wordlist_path)

    print(f"[extractor:trim] scan_root={scan_root}", flush=True)
    if domain_filter:
        print(f"[extractor:trim] domain filter={domain_filter!r}", flush=True)
    print(f"[extractor:trim] match files counted: {sum(counts.values())} across {len(counts)} rule name(s)", flush=True)
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

    deleted = delete_extractor_matches_for_rules(
        scan_root, remove_names, domain_filter=domain_filter, workers=workers
    )
    print(f"[extractor:trim] deleted {deleted} match file(s).", flush=True)

    rebuilt = rebuild_all_extractor_summaries_after_trim(scan_root, domain_filter=domain_filter)
    print(f"[extractor:trim] rebuilt extractor summary.json for {rebuilt} domain tree(s).", flush=True)
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
        result_file = str(entry.get("result_file", "") or "")
        if not result_file:
            continue
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
                # request_response and unknown: one combined pass
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


def run_extractor_scan(
    scan_root: Path,
    wordlist_path: Path,
    *,
    force: bool = False,
    domain_filter: str | None = None,
    workers: int = 1,
) -> int:
    scan_root = scan_root.resolve()
    state_path = scan_root / STATE_FILE_NAME
    state = load_extractor_incremental_state(state_path)
    rules = load_extractor_rules(wordlist_path)
    if not rules:
        print("[extractor] No valid rules loaded; exiting.", flush=True)
        return 1

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
    if not pairs:
        print(f"[extractor] No Fozzy domain trees under {scan_root}", flush=True)
        return 0

    processed = 0
    skipped = 0
    to_process: list[tuple[str, Path]] = []
    for domain_label, domain_output in pairs:
        key = domain_label
        if not force and not domain_results_dirty(domain_output, state, key):
            skipped += 1
            continue
        to_process.append((domain_label, domain_output))

    print(
        f"[extractor] Starting scan: domains_to_process={len(to_process)}, skipped_unchanged={skipped}, workers={workers}",
        flush=True,
    )

    if workers <= 1:
        for domain_label, domain_output in to_process:
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
    else:
        failures = 0

        def _run_one(domain_label: str, domain_output: Path) -> tuple[str, Path, int, Path]:
            n, summary_path = run_extractors_for_domain(domain_label, domain_output, rules)
            return domain_label, domain_output, n, summary_path

        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_map = {
                    executor.submit(_run_one, domain_label, domain_output): (domain_label, domain_output)
                    for domain_label, domain_output in to_process
                }
                for fut in as_completed(future_map):
                    domain_label, domain_output = future_map[fut]
                    try:
                        done_domain, done_output, n, summary_path = fut.result()
                    except Exception as exc:
                        failures += 1
                        print(f"[extractor] {domain_label} worker error: {exc}", flush=True)
                        continue
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
        except KeyboardInterrupt:
            print("[extractor] Interrupt received; stopping worker pool with partial outputs.", flush=True)
            return 130

        if failures > 0:
            print(f"[extractor] Completed with worker failures={failures}", flush=True)
            return 1

    print(
        f"[extractor] Done. Domains processed={processed}, skipped (unchanged)={skipped}, "
        f"state={state_path}",
        flush=True,
    )
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
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config_path = resolve_config_path(str(args.config))
    file_config = read_json_file(config_path)
    effective_config = {**default_extractor_config(), **file_config}

    scan_root_raw = str(merged_value(args.scan_root, effective_config, "scan_root", str(DEFAULT_SCAN_ROOT)))
    wordlist_raw = str(merged_value(args.wordlist, effective_config, "wordlist", str(DEFAULT_WORDLIST)))
    workers = normalize_workers(merged_value(args.workers, effective_config, "workers", 4))
    force = bool(merged_value(args.force, effective_config, "force", False))
    domain_filter = merged_value(args.domain, effective_config, "domain", None)
    log_file_raw = str(merged_value(args.log_file, effective_config, "log_file", "") or "").strip()

    scan_root = Path(scan_root_raw).expanduser()
    if not scan_root.is_absolute():
        scan_root = (EXTRACTOR_BASE / scan_root).resolve()
    wordlist = Path(wordlist_raw).expanduser()
    if not wordlist.is_absolute():
        wordlist = (EXTRACTOR_BASE / wordlist).resolve()

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
        )
    except FileNotFoundError as exc:
        print(f"[extractor] {exc}", flush=True)
        return 1
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"[extractor] {exc}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
