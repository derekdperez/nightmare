#!/usr/bin/env python3
"""Run regex extractors over Fozzy anomaly/reflection JSON (response bodies and metadata).

Scans ``--scan-root`` (default: ``output`` next to this repo) for Fozzy domain trees, the same way
the master report does. When a domain's ``results/`` tree has newer files than recorded in
``.extractor_incremental_state.json``, that domain is re-processed: each match is written under
``<domain_output>/extractor/matches/`` and a ``summary.json`` is refreshed for the master HTML report.

Usage:
    python extractor.py
    python extractor.py --scan-root ./output --force
    python extractor.py --wordlist path/to/extractor_list.txt
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Repo root (directory containing this file)
EXTRACTOR_BASE = Path(__file__).resolve().parent
DEFAULT_WORDLIST = EXTRACTOR_BASE / "resources" / "wordlists" / "extractor_list.txt"
DEFAULT_SCAN_ROOT = EXTRACTOR_BASE / "output"
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


def run_extractor_scan(
    scan_root: Path,
    wordlist_path: Path,
    *,
    force: bool = False,
) -> int:
    scan_root = scan_root.resolve()
    state_path = scan_root / STATE_FILE_NAME
    state = load_extractor_incremental_state(state_path)
    rules = load_extractor_rules(wordlist_path)
    if not rules:
        print("[extractor] No valid rules loaded; exiting.", flush=True)
        return 1

    pairs = discover_pairs(scan_root)
    if not pairs:
        print(f"[extractor] No Fozzy domain trees under {scan_root}", flush=True)
        return 0

    processed = 0
    skipped = 0
    for domain_label, domain_output in pairs:
        key = domain_label
        if not force and not domain_results_dirty(domain_output, state, key):
            skipped += 1
            continue
        print(f"[extractor] {domain_label} … ({domain_output})", flush=True)
        n, summary_path = run_extractors_for_domain(domain_label, domain_output, rules)
        print(f"[extractor]   wrote {n} match file(s); {summary_path}", flush=True)
        results = domain_output / "results"
        if results.is_dir():
            if "domains" not in state or not isinstance(state["domains"], dict):
                state["domains"] = {}
            state["domains"][key] = {
                "results_max_mtime_ns": folder_tree_max_mtime_ns(results),
            }
        save_extractor_incremental_state(state_path, state)
        processed += 1

    print(
        f"[extractor] Done. Domains processed={processed}, skipped (unchanged)={skipped}, "
        f"state={state_path}",
        flush=True,
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Regex extractor over Fozzy result JSON (incremental by domain).")
    p.add_argument(
        "--scan-root",
        default=str(DEFAULT_SCAN_ROOT),
        help=f"Root to scan for Fozzy outputs (default: {DEFAULT_SCAN_ROOT})",
    )
    p.add_argument(
        "--wordlist",
        default=str(DEFAULT_WORDLIST),
        help=f"JSON array of extractor rules (default: {DEFAULT_WORDLIST})",
    )
    p.add_argument(
        "--force",
        action="store_true",
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
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    scan_root = Path(args.scan_root).expanduser()
    if not scan_root.is_absolute():
        scan_root = (EXTRACTOR_BASE / scan_root).resolve()
    wordlist = Path(args.wordlist).expanduser()
    if not wordlist.is_absolute():
        wordlist = (EXTRACTOR_BASE / wordlist).resolve()
    log_file_raw = str(args.log_file or "").strip()
    if log_file_raw:
        log_path = Path(log_file_raw).expanduser()
        if not log_path.is_absolute():
            log_path = (EXTRACTOR_BASE / log_path).resolve()
        else:
            log_path = log_path.resolve()
    else:
        log_path = (scan_root / "extractor.log").resolve()
    install_extractor_log_tee(log_path)
    try:
        return run_extractor_scan(scan_root, wordlist, force=bool(args.force))
    except FileNotFoundError as exc:
        print(f"[extractor] {exc}", flush=True)
        return 1
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"[extractor] {exc}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
