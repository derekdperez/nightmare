#!/usr/bin/env python3
"""Parameter permutation and fuzz runner for Nightmare parameter inventories.

Usage:
    python fozzy.py output/example.com/example.com.parameters.json
    python fozzy.py --write-master-report path/to/output_or_fozzy-output
    python fozzy.py --generate-master-report
        Regenerates all_domains.results_summary.json/html under --scan-root (default ./output).
    python fozzy.py
        With no parameters file, scans --scan-root (default: ./output) for Nightmare domain
        folders, runs Fozzy for any domain whose folder has new/changed files since the last
        run (see .fozzy_incremental_state.json). Live runs do not prompt; --incremental-force
        to run every domain that has a parameters file.

    Repo-wide master report (all domains): output/all_domains.results_summary.html when the
    layout is output/<domain>/fozzy-output/<domain>/results/ (see --write-master-report or --generate-master-report).

Settings load from config/fozzy.json next to this script (override with --config).
CLI flags only override individual values when passed.
"""

from __future__ import annotations

import argparse
import copy
import difflib
import hashlib
import html
import itertools
import json
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable

# Semaphore / RLock misuse or runtime issues should warn, not abort a long fuzz run.
_FOZZY_LOCK_ERRORS: tuple[type[BaseException], ...] = (RuntimeError, ValueError, AttributeError)


def _fozzy_safe_release(label: str, release_fn: Callable[[], None]) -> None:
    try:
        release_fn()
    except _FOZZY_LOCK_ERRORS as exc:
        print(f"Warning: Fozzy lock release ({label}) skipped: {exc}", flush=True)


def _fozzy_with_rlock(lock: threading.RLock | None, context: str, fn: Callable[[], Any]) -> Any:
    if lock is None:
        return fn()
    try:
        with lock:
            return fn()
    except _FOZZY_LOCK_ERRORS as exc:
        print(f"Warning: Fozzy {context}: {exc}", flush=True)
        return fn()


try:
    import tldextract

    _TLD_EXTRACT_FOZZY = tldextract.TLDExtract(suffix_list_urls=None)
except Exception:  # pragma: no cover - optional dependency fallback
    tldextract = None
    _TLD_EXTRACT_FOZZY = None


FOZZY_BASE_DIR = Path(__file__).resolve().parent
FOZZY_DEFAULT_CONFIG_PATH = FOZZY_BASE_DIR / "config" / "fozzy.json"

_FOZZY_CONFIG_NESTED_MERGE_KEYS = frozenset({"request_headers", "default_generic_by_type"})


def default_fozzy_config() -> dict[str, Any]:
    """Built-in defaults; overridden by config/fozzy.json and CLI flags."""
    return {
        "timeout_seconds": 10.0,
        "delay_seconds": 0.1,
        "max_permutations": 512,
        "quick_fuzz_list": "resources/quick_fuzz_list.txt",
        "output_dir": None,
        "dry_run": False,
        "max_background_workers": 8,
        "max_workers_per_domain": 8,
        "max_workers_per_subdomain": 1,
        "max_requests_per_endpoint": None,
        "live_report_interval_seconds": 5.0,
        "body_preview_limit": 2048,
        "reflection_alert_ignore_exact": ["test"],
        "default_generic_by_type": {
            "bool": "true",
            "int": "1",
            "float": "1.0",
            "uuid": "00000000-0000-4000-8000-000000000000",
            "date": "2024-01-01",
            "datetime": "2024-01-01T00:00:00Z",
            "email": "test@example.com",
            "url": "https://example.com",
            "hex": "deadbeef",
            "token": "sampletoken",
            "empty": "",
            "string": "test",
        },
        "type_priority": [
            "uuid",
            "datetime",
            "date",
            "email",
            "url",
            "bool",
            "float",
            "int",
            "hex",
            "token",
            "empty",
            "string",
        ],
        "request_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
        },
    }


def merge_fozzy_user_into_base(base: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in user.items():
        if key in _FOZZY_CONFIG_NESTED_MERGE_KEYS and isinstance(value, dict) and isinstance(out.get(key), dict):
            merged = {**out[key], **value}
            out[key] = merged
        elif isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = {**out[key], **copy.deepcopy(value)}
        else:
            out[key] = copy.deepcopy(value)
    return out


def normalize_fozzy_config(cfg: dict[str, Any]) -> None:
    """Coerce types and sane bounds in place."""
    cfg["timeout_seconds"] = max(0.1, float(cfg.get("timeout_seconds", 10.0)))
    cfg["delay_seconds"] = max(0.0, float(cfg.get("delay_seconds", 0.1)))
    cfg["max_permutations"] = max(1, int(cfg.get("max_permutations", 512)))
    cfg["max_background_workers"] = max(1, int(cfg.get("max_background_workers", 8)))
    cfg["max_workers_per_domain"] = max(1, int(cfg.get("max_workers_per_domain", 8)))
    cfg["max_workers_per_subdomain"] = max(1, int(cfg.get("max_workers_per_subdomain", 1)))
    if cfg["max_background_workers"] <= 1:
        cfg["max_workers_per_domain"] = 1
        cfg["max_workers_per_subdomain"] = 1
    # None / missing / 0 / negative: no per-endpoint cap (all planned requests; throttle via delay_seconds).
    # Positive int: optional cap on plan lines per host+path.
    raw_mre = cfg.get("max_requests_per_endpoint", None)
    if raw_mre is None:
        cfg["max_requests_per_endpoint"] = None
    else:
        try:
            mre = int(raw_mre)
        except (TypeError, ValueError):
            cfg["max_requests_per_endpoint"] = None
        else:
            cfg["max_requests_per_endpoint"] = None if mre <= 0 else mre
    cfg["live_report_interval_seconds"] = max(0.5, float(cfg.get("live_report_interval_seconds", 5.0)))
    cfg["body_preview_limit"] = max(256, int(cfg.get("body_preview_limit", 2048)))
    raw_ign = cfg.get("reflection_alert_ignore_exact", ["test"])
    if not isinstance(raw_ign, list):
        raw_ign = ["test"]
    cfg["reflection_alert_ignore_exact"] = [str(x) for x in raw_ign if str(x).strip() != ""]
    if not cfg["reflection_alert_ignore_exact"]:
        cfg["reflection_alert_ignore_exact"] = ["test"]
    if not isinstance(cfg.get("type_priority"), list) or not cfg["type_priority"]:
        cfg["type_priority"] = list(default_fozzy_config()["type_priority"])
    else:
        cfg["type_priority"] = [str(x).strip().lower() for x in cfg["type_priority"] if str(x).strip()]
    dft = cfg.get("default_generic_by_type")
    if not isinstance(dft, dict):
        cfg["default_generic_by_type"] = copy.deepcopy(default_fozzy_config()["default_generic_by_type"])
    else:
        base_g = default_fozzy_config()["default_generic_by_type"]
        cfg["default_generic_by_type"] = {**base_g, **{str(k): str(v) for k, v in dft.items()}}
    hdrs = cfg.get("request_headers")
    if not isinstance(hdrs, dict):
        cfg["request_headers"] = copy.deepcopy(default_fozzy_config()["request_headers"])
    else:
        cfg["request_headers"] = {str(k): str(v) for k, v in hdrs.items()}
    cfg["dry_run"] = bool(cfg.get("dry_run", False))
    qfl = cfg.get("quick_fuzz_list")
    cfg["quick_fuzz_list"] = str(qfl or "resources/quick_fuzz_list.txt").strip() or "resources/quick_fuzz_list.txt"
    od = cfg.get("output_dir")
    cfg["output_dir"] = str(od).strip() if od not in (None, "") else None


_ACTIVE_FOZZY_CONFIG: dict[str, Any] = copy.deepcopy(default_fozzy_config())
normalize_fozzy_config(_ACTIVE_FOZZY_CONFIG)


def active_fozzy_config() -> dict[str, Any]:
    return _ACTIVE_FOZZY_CONFIG


def _http_status_phrase(code: int) -> str:
    try:
        return HTTPStatus(code).phrase
    except ValueError:
        return "Unknown"


def is_low_signal_reflection_fuzz_value(fuzz_value: Any) -> bool:
    """True if substring-matching this value in a response body is not a useful reflection signal."""
    text = str(fuzz_value).strip()
    if not text:
        return True
    ignored = active_fozzy_config().get("reflection_alert_ignore_exact", ["test"])
    lowered = {str(x).strip().lower() for x in ignored if isinstance(x, str) and str(x).strip()}
    if text.lower() in lowered:
        return True
    return False


def registrable_domain(hostname: str) -> str:
    """eTLD+1 for rate-limit grouping (e.g. api.example.com -> example.com)."""
    host = (hostname or "").lower().strip(".")
    if not host:
        return ""
    if _TLD_EXTRACT_FOZZY is not None:
        try:
            extracted = _TLD_EXTRACT_FOZZY(host)
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}".lower()
        except Exception:
            pass
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:]).lower()
    return host


class HostDomainFuzzGate:
    """Limits concurrent fuzz jobs: global pool, per registrable domain (eTLD+1), per exact hostname."""

    def __init__(
        self,
        max_workers: int,
        max_workers_per_domain: int,
        max_workers_per_subdomain: int,
    ):
        self._max_workers = max(1, int(max_workers))
        self._max_per_domain = max(1, int(max_workers_per_domain))
        self._max_per_subdomain = max(1, int(max_workers_per_subdomain))
        self._global_sem = threading.Semaphore(self._max_workers)
        self._domain_sems: dict[str, threading.Semaphore] = {}
        self._host_sems: dict[str, threading.Semaphore] = {}
        self._meta_lock = threading.Lock()

    def _domain_sem(self, reg_domain: str) -> threading.Semaphore:
        try:
            with self._meta_lock:
                if reg_domain not in self._domain_sems:
                    self._domain_sems[reg_domain] = threading.Semaphore(self._max_per_domain)
                return self._domain_sems[reg_domain]
        except _FOZZY_LOCK_ERRORS as exc:
            print(f"Warning: Fozzy gate meta-lock (domain map): {exc}", flush=True)
            if reg_domain not in self._domain_sems:
                self._domain_sems[reg_domain] = threading.Semaphore(self._max_per_domain)
            return self._domain_sems[reg_domain]

    def _host_sem(self, host: str) -> threading.Semaphore:
        try:
            with self._meta_lock:
                if host not in self._host_sems:
                    self._host_sems[host] = threading.Semaphore(self._max_per_subdomain)
                return self._host_sems[host]
        except _FOZZY_LOCK_ERRORS as exc:
            print(f"Warning: Fozzy gate meta-lock (host map): {exc}", flush=True)
            if host not in self._host_sems:
                self._host_sems[host] = threading.Semaphore(self._max_per_subdomain)
            return self._host_sems[host]

    def acquire(self, host: str, reg_domain: str) -> None:
        self._global_sem.acquire()
        try:
            self._domain_sem(reg_domain).acquire()
            try:
                self._host_sem(host).acquire()
            except BaseException:
                _fozzy_safe_release(
                    "gate domain (after host acquire failure)",
                    lambda: self._domain_sem(reg_domain).release(),
                )
                raise
        except BaseException:
            _fozzy_safe_release(
                "gate global (after domain acquire failure)",
                lambda: self._global_sem.release(),
            )
            raise

    def release(self, host: str, reg_domain: str) -> None:
        _fozzy_safe_release("gate host", lambda: self._host_sem(host).release())
        _fozzy_safe_release("gate domain", lambda: self._domain_sem(reg_domain).release())
        _fozzy_safe_release("gate global", lambda: self._global_sem.release())

    def try_acquire_nonblocking(self, host: str, reg_domain: str) -> bool:
        """Acquire global + domain + host slots, all non-blocking; release any partial on failure.

        Used by the parallel scheduler so a thread is not assigned a job that blocks on host
        while still holding a global worker slot (which would serialize unrelated subdomains).
        """
        if not self._global_sem.acquire(blocking=False):
            return False
        dom = self._domain_sem(reg_domain)
        if not dom.acquire(blocking=False):
            _fozzy_safe_release("gate global (try_acquire domain busy)", lambda: self._global_sem.release())
            return False
        try:
            hsem = self._host_sem(host)
            if not hsem.acquire(blocking=False):
                _fozzy_safe_release("gate domain (try_acquire host busy)", lambda: dom.release())
                _fozzy_safe_release("gate global (try_acquire host busy)", lambda: self._global_sem.release())
                return False
        except BaseException:
            _fozzy_safe_release("gate domain (try_acquire host exception)", lambda: dom.release())
            _fozzy_safe_release("gate global (try_acquire host exception)", lambda: self._global_sem.release())
            raise
        return True


def _resume_get(
    resume: dict[str, Any] | None,
    key: str,
    lock: threading.RLock | None,
) -> Any:
    if not isinstance(resume, dict):
        return None
    return _fozzy_with_rlock(lock, "resume read lock", lambda: resume.get(key))


@dataclass
class ParameterMeta:
    name: str
    data_type: str = "string"
    observed_values: set[str] = field(default_factory=set)


@dataclass
class RouteGroup:
    host: str
    path: str
    scheme: str
    observed_param_sets: set[frozenset[str]] = field(default_factory=set)
    params: dict[str, ParameterMeta] = field(default_factory=dict)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read a Nightmare parameters.json file, derive valid/optional parameter combinations, "
            "generate placeholder permutations, and run baseline-vs-fuzz requests. "
            "With no parameters file, scans --scan-root and runs domains that have new or changed files "
            "since the last run (see .fozzy_incremental_state.json in the scan root)."
        )
    )
    parser.add_argument(
        "parameters_file",
        nargs="?",
        default=None,
        help=(
            "Path to <domain>.parameters.json. Omit (with no master-report-only flag) to scan --scan-root "
            "and run every domain folder that has new/changed files since the last run."
        ),
    )
    default_cfg_path = str(FOZZY_DEFAULT_CONFIG_PATH)
    parser.add_argument(
        "--config",
        "--fozzy-config",
        dest="fozzy_config",
        type=str,
        default=default_cfg_path,
        help=(
            f"Fozzy settings JSON (default: {default_cfg_path}). "
            "Relative paths resolve against the directory containing fozzy.py."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Override fozzy.json request timeout_seconds.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        help="Override fozzy.json delay_seconds between requests.",
    )
    parser.add_argument(
        "--max-permutations",
        type=int,
        default=None,
        help="Override fozzy.json max_permutations per host/path.",
    )
    parser.add_argument(
        "--max-requests-per-endpoint",
        type=int,
        default=None,
        help=(
            "Live run only: optional cap on HTTP requests per host+path (plan lines; baseline + mutations per block). "
            "Default is no cap (run all planned requests); config may set max_requests_per_endpoint. "
            "Use 0 for no cap when overriding a capped config."
        ),
    )
    parser.add_argument(
        "--quick-fuzz-list",
        default=None,
        help=(
            "Override fozzy.json quick_fuzz_list path. "
            "If omitted and not found, also tries resources/wordlists/quick_fuzz_list.txt."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Override fozzy.json output_dir; default artifact layout if unset in both.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan only; overrides fozzy.json dry_run.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Force live HTTP requests even if fozzy.json sets dry_run true.",
    )
    parser.add_argument(
        "--max-background-workers",
        type=int,
        default=None,
        help=(
            "Override fozzy.json max_background_workers (default 8, same idea as nightmare.json batch_workers). "
            "Per eTLD+1 and per-host caps also apply (max_workers_per_domain / max_workers_per_subdomain)."
        ),
    )
    parser.add_argument(
        "--max-workers-per-domain",
        type=int,
        default=None,
        help=(
            "Override fozzy.json max_workers_per_domain (concurrent fuzz jobs per registrable domain / eTLD+1). "
            "Set high enough to fan out across subdomains; ignored when max_background_workers is 1."
        ),
    )
    parser.add_argument(
        "--max-workers-per-subdomain",
        type=int,
        default=None,
        help=(
            "Override fozzy.json max_workers_per_subdomain (per exact hostname). "
            "Default 1. Ignored when max_background_workers is 1."
        ),
    )
    parser.add_argument(
        "--write-master-report",
        metavar="AGGREGATE_ROOT",
        type=str,
        default=None,
        help=(
            "Only aggregate anomaly_*.json and reflection_*.json into all_domains.results_summary.json/html "
            "under AGGREGATE_ROOT, then exit. Use the Nightmare output directory (e.g. output/) for all domains, "
            "or a single fozzy-output folder; layout is detected automatically."
        ),
    )
    parser.add_argument(
        "--generate-master-report",
        nargs="?",
        const=True,
        default=None,
        metavar="AGGREGATE_ROOT",
        help=(
            "Regenerate all_domains.results_summary.json/html from on-disk results and exit. "
            "With no argument, uses --scan-root (default: output). "
            "Same aggregation as --write-master-report; if both are set, --write-master-report wins."
        ),
    )
    parser.add_argument(
        "--scan-root",
        type=str,
        default="output",
        help=(
            "When no parameters file is given: directory containing per-domain Nightmare folders "
            "(default: output next to fozzy.py). Incremental state is stored as .fozzy_incremental_state.json here. "
            "Also the default root for --generate-master-report when that flag is used without a path."
        ),
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="No-op (kept for compatibility). Fozzy never prompts after startup.",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="No-op (kept for compatibility). Same as --batch.",
    )
    parser.add_argument(
        "--incremental-force",
        action="store_true",
        help="When scanning --scan-root: run Fozzy for every domain that has a parameters file, ignoring incremental state.",
    )
    return parser.parse_args()


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def build_effective_fozzy_config(args: argparse.Namespace) -> dict[str, Any]:
    cfg = copy.deepcopy(default_fozzy_config())
    config_path = Path(args.fozzy_config).expanduser()
    if not config_path.is_absolute():
        config_path = (FOZZY_BASE_DIR / config_path).resolve()
    else:
        config_path = config_path.resolve()
    if config_path.is_file():
        loaded = read_json(config_path)
        if isinstance(loaded, dict):
            cfg = merge_fozzy_user_into_base(cfg, loaded)
        else:
            print(f"Warning: {config_path} must contain a JSON object; ignoring.", flush=True)
    else:
        print(f"Warning: fozzy config file not found ({config_path}); using built-in defaults.", flush=True)

    if args.timeout is not None:
        cfg["timeout_seconds"] = float(args.timeout)
    if args.delay is not None:
        cfg["delay_seconds"] = float(args.delay)
    if args.max_permutations is not None:
        cfg["max_permutations"] = int(args.max_permutations)
    if args.max_requests_per_endpoint is not None:
        cfg["max_requests_per_endpoint"] = int(args.max_requests_per_endpoint)
    if args.quick_fuzz_list is not None:
        cfg["quick_fuzz_list"] = str(args.quick_fuzz_list).strip()
    if args.output_dir is not None:
        cfg["output_dir"] = str(args.output_dir).strip() or None
    if args.max_background_workers is not None:
        cfg["max_background_workers"] = int(args.max_background_workers)
    if args.max_workers_per_domain is not None:
        cfg["max_workers_per_domain"] = int(args.max_workers_per_domain)
    if args.max_workers_per_subdomain is not None:
        cfg["max_workers_per_subdomain"] = int(args.max_workers_per_subdomain)
    if args.dry_run:
        cfg["dry_run"] = True
    if getattr(args, "live", False):
        cfg["dry_run"] = False

    normalize_fozzy_config(cfg)
    return cfg


def merge_data_type(existing: str, new_type: str) -> str:
    if not existing:
        return new_type or "string"
    if not new_type:
        return existing
    existing = existing.strip().lower()
    new_type = new_type.strip().lower()
    if existing == new_type:
        return existing
    if existing == "string" or new_type == "string":
        return "string"
    if {"float", "int"} == {existing, new_type}:
        return "float"
    priority = active_fozzy_config().get("type_priority") or default_fozzy_config()["type_priority"]
    for candidate in priority:
        if candidate in {existing, new_type}:
            return candidate
    return "string"


def infer_value_type(value: str) -> str:
    text = (value or "").strip()
    if text == "":
        return "empty"
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return "bool"
    if re.fullmatch(r"[+-]?\d+", text):
        return "int"
    if re.fullmatch(r"[+-]?\d+\.\d+", text):
        return "float"
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}", text):
        return "uuid"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return "date"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T[^ ]+", text):
        return "datetime"
    if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", text):
        return "email"
    if re.fullmatch(r"https?://\S+", text, flags=re.IGNORECASE):
        return "url"
    if re.fullmatch(r"[0-9a-fA-F]+", text) and len(text) % 2 == 0:
        return "hex"
    if re.fullmatch(r"[A-Za-z0-9_-]{8,}={0,2}", text):
        return "token"
    return "string"


def load_route_groups(parameters_payload: dict[str, Any]) -> list[RouteGroup]:
    grouped: dict[tuple[str, str], RouteGroup] = {}
    entries = parameters_payload.get("entries", [])
    if not isinstance(entries, list):
        return []

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        raw_url = str(entry.get("url", "")).strip()
        if not raw_url:
            continue
        parsed = urllib.parse.urlparse(raw_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        host = parsed.netloc.lower()
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/") or "/"
        key = (host, path)
        if key not in grouped:
            grouped[key] = RouteGroup(host=host, path=path, scheme=parsed.scheme.lower())
        group = grouped[key]

        # Observed parameter set from URL query.
        query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        observed_keys = frozenset(key for key, _ in query_items if key)
        if observed_keys:
            group.observed_param_sets.add(observed_keys)

        parameters = entry.get("parameters", [])
        if isinstance(parameters, list):
            for param in parameters:
                if not isinstance(param, dict):
                    continue
                name = str(param.get("name", "")).strip()
                if not name:
                    continue
                dtype = str(param.get("canonical_data_type", "string")).strip().lower() or "string"
                meta = group.params.get(name)
                if meta is None:
                    meta = ParameterMeta(name=name, data_type=dtype)
                    group.params[name] = meta
                else:
                    meta.data_type = merge_data_type(meta.data_type, dtype)
                observed_values = param.get("observed_values", [])
                if isinstance(observed_values, list):
                    for value in observed_values:
                        meta.observed_values.add(str(value))

        # Backfill param metadata from URL query values.
        for key_name, value in query_items:
            if not key_name:
                continue
            meta = group.params.get(key_name)
            inferred = infer_value_type(value)
            if meta is None:
                meta = ParameterMeta(name=key_name, data_type=inferred)
                group.params[key_name] = meta
            else:
                meta.data_type = merge_data_type(meta.data_type, inferred)
            meta.observed_values.add(value)

    return [grouped[key] for key in sorted(grouped.keys())]


def generic_value_for(meta: ParameterMeta) -> str:
    table = active_fozzy_config().get("default_generic_by_type") or {}
    return str(table.get(meta.data_type, "test"))


def baseline_seed_value_for(meta: ParameterMeta) -> str:
    observed = sorted({str(value) for value in meta.observed_values if str(value) != ""}, key=lambda item: (len(item), item))
    if observed:
        return observed[0]
    return generic_value_for(meta)


def load_quick_fuzz_values(quick_fuzz_path: str) -> list[str]:
    candidates = [Path(quick_fuzz_path)]
    if quick_fuzz_path == "resources/quick_fuzz_list.txt":
        candidates.append(Path("resources/wordlists/quick_fuzz_list.txt"))
    resolved: Path | None = None
    for candidate in candidates:
        if candidate.exists():
            resolved = candidate
            break
    if resolved is None:
        raise FileNotFoundError(
            f"Quick fuzz list not found. Tried: {', '.join(str(path) for path in candidates)}"
        )
    lines = resolved.read_text(encoding="utf-8-sig").splitlines()
    if not lines:
        raise ValueError(f"Quick fuzz list is empty: {resolved}")
    return lines


def build_url(scheme: str, host: str, path: str, values: dict[str, str]) -> str:
    query = urllib.parse.urlencode([(name, values[name]) for name in sorted(values.keys())], doseq=True)
    return urllib.parse.urlunparse((scheme, host, path, "", query, ""))


def build_placeholder_url(scheme: str, host: str, path: str, params: list[ParameterMeta]) -> str:
    counters: dict[str, int] = defaultdict(int)
    pairs: list[str] = []
    for param in sorted(params, key=lambda item: item.name):
        dtype = param.data_type or "string"
        counters[dtype] += 1
        placeholder = f"{{{dtype}{counters[dtype]}}}"
        key_encoded = urllib.parse.quote_plus(param.name, safe="[]")
        pairs.append(f"{key_encoded}={placeholder}")
    query = "&".join(pairs)
    return urllib.parse.urlunparse((scheme, host, path, "", query, ""))


def request_url(url: str, timeout: float) -> dict[str, Any]:
    cfg = active_fozzy_config()
    headers = cfg.get("request_headers") or {}
    preview_limit = int(cfg.get("body_preview_limit", 2048))
    req = urllib.request.Request(
        url,
        headers=headers,
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read(preview_limit)
            elapsed_ms = int(round((time.perf_counter() - t0) * 1000.0))
            return {
                "url": url,
                "status": int(getattr(response, "status", 0) or 0),
                "size": len(body),
                "body_preview": body.decode("utf-8", errors="replace"),
                "elapsed_ms": elapsed_ms,
            }
    except urllib.error.HTTPError as exc:
        body = b""
        try:
            body = exc.read(preview_limit)
        except Exception:
            body = b""
        elapsed_ms = int(round((time.perf_counter() - t0) * 1000.0))
        return {
            "url": url,
            "status": int(getattr(exc, "code", 0) or 0),
            "size": len(body),
            "body_preview": body.decode("utf-8", errors="replace"),
            "elapsed_ms": elapsed_ms,
        }
    except Exception as exc:
        elapsed_ms = int(round((time.perf_counter() - t0) * 1000.0))
        return {
            "url": url,
            "status": 0,
            "size": 0,
            "body_preview": "",
            "error": str(exc),
            "elapsed_ms": elapsed_ms,
        }


def response_differs(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return int(a.get("status", 0)) != int(b.get("status", 0)) or int(a.get("size", 0)) != int(b.get("size", 0))


def curl_command_for_url(url: str, timeout: float) -> str:
    timeout_token = str(int(timeout)) if timeout > 0 else "10"
    parts = ["curl", "-sS", "-L", "--max-time", timeout_token]
    headers = active_fozzy_config().get("request_headers") or {}
    for key, value in headers.items():
        parts.extend(["-H", f"\"{key}: {value}\""])
    parts.append(f"\"{url}\"")
    return " ".join(parts)


def empty_expected_response() -> dict[str, Any]:
    return {"status": None, "size": None, "body_preview": None}


def make_request_key(
    *,
    phase: str,
    host: str,
    path: str,
    permutation: tuple[str, ...] | list[str] | None,
    mutated_parameter: str | None,
    mutated_value: str | None,
    url: str,
) -> str:
    payload = {
        "phase": str(phase or "").strip(),
        "host": str(host or "").strip().lower(),
        "path": str(path or "").strip() or "/",
        "permutation": list(permutation or ()),
        "mutated_parameter": str(mutated_parameter or ""),
        "mutated_value": str(mutated_value or ""),
        "url": str(url or "").strip(),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def request_key_from_plan_line(line: dict[str, Any]) -> str:
    return make_request_key(
        phase=str(line.get("phase", "")),
        host=str(line.get("host", "")),
        path=str(line.get("path", "")),
        permutation=list(line.get("permutation", [])) if isinstance(line.get("permutation"), list) else [],
        mutated_parameter=str(line.get("mutated_parameter", "") or ""),
        mutated_value=str(line.get("mutated_value", "") or ""),
        url=str(line.get("url", "")),
    )


def load_resume_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"requested": {}, "queued": [], "meta": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {"requested": {}, "queued": [], "meta": {}}
    if not isinstance(payload, dict):
        return {"requested": {}, "queued": [], "meta": {}}
    requested = payload.get("requested", {})
    queued = payload.get("queued", [])
    meta = payload.get("meta", {})
    return {
        "requested": requested if isinstance(requested, dict) else {},
        "queued": queued if isinstance(queued, list) else [],
        "meta": meta if isinstance(meta, dict) else {},
    }


def save_resume_state(path: Path, state: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def queue_dry_run_request(
    plan_lines: list[dict[str, Any]] | None,
    phase: str,
    url: str,
    timeout: float,
    host: str,
    path: str,
    permutation: tuple[str, ...] | None,
    mutated_parameter: str | None,
    mutated_value: str | None,
    expected_baseline_response: dict[str, Any],
) -> None:
    if plan_lines is None:
        return
    request_id = len(plan_lines) + 1
    line = {
        "request_id": request_id,
        "phase": phase,
        "host": host,
        "path": path,
        "permutation": list(permutation or ()),
        "mutated_parameter": mutated_parameter,
        "mutated_value": mutated_value,
        "url": url,
        "curl": curl_command_for_url(url, timeout=timeout),
        "expected_baseline_response": expected_baseline_response,
        "actual_response": {},
    }
    line["request_key"] = request_key_from_plan_line(line)
    plan_lines.append(line)


def format_duration(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def build_endpoint_request_counts(plan_lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for line in plan_lines:
        host = str(line.get("host", "")).strip().lower()
        path = str(line.get("path", "")).strip() or "/"
        if not host:
            continue
        endpoint = f"{host}{path}"
        counts[endpoint] = counts.get(endpoint, 0) + 1
    rows = [{"endpoint": endpoint, "request_count": count} for endpoint, count in counts.items()]
    rows.sort(key=lambda item: (-int(item["request_count"]), str(item["endpoint"])))
    return rows


def plan_line_endpoint_key(line: dict[str, Any]) -> str:
    host = str(line.get("host", "")).strip().lower()
    path = str(line.get("path", "")).strip() or "/"
    return f"{host}{path}"


def _plan_lines_to_permutation_blocks(ep_lines: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Split one endpoint's plan lines into blocks starting at ``fuzz_baseline`` (baseline + its mutations)."""
    blocks: list[list[dict[str, Any]]] = []
    cur: list[dict[str, Any]] = []
    for line in ep_lines:
        if str(line.get("phase", "")) == "fuzz_baseline":
            if cur:
                blocks.append(cur)
            cur = [line]
        else:
            if not cur:
                cur = [line]
            else:
                cur.append(line)
    if cur:
        blocks.append(cur)
    return blocks


def cap_plan_lines_by_endpoint(plan_lines: list[dict[str, Any]], max_per_endpoint: int) -> list[dict[str, Any]]:
    """Keep plan lines in file order, at most ``max_per_endpoint`` lines per host+path, cutting only on block boundaries."""
    if max_per_endpoint <= 0:
        return list(plan_lines)
    out: list[dict[str, Any]] = []
    for _ep, ep_iter in itertools.groupby(plan_lines, key=plan_line_endpoint_key):
        ep_lines = list(ep_iter)
        if not ep_lines:
            continue
        used = 0
        for block in _plan_lines_to_permutation_blocks(ep_lines):
            blen = len(block)
            if used + blen > max_per_endpoint:
                break
            out.extend(block)
            used += blen
    return out


def list_results_folder_files(results_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not results_dir.exists():
        return records
    for path in sorted(results_dir.iterdir(), key=lambda item: item.name.lower()):
        if not path.is_file():
            continue
        stat = path.stat()
        records.append(
            {
                "name": path.name,
                "path": str(path),
                "extension": path.suffix.lower(),
                "size_bytes": int(stat.st_size),
                "modified_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    return records


def infer_required_and_optional(
    group: RouteGroup,
    timeout: float,
    delay: float,
    dry_run: bool,
    dry_run_plan_lines: list[dict[str, Any]] | None = None,
) -> tuple[set[str], set[str]]:
    all_params = set(group.params.keys())
    if not all_params:
        return set(), set()

    if group.observed_param_sets:
        observed_list = list(group.observed_param_sets)
        required = set(observed_list[0])
        for values in observed_list[1:]:
            required &= set(values)
    else:
        required = set()
    optional = set(all_params - required)

    baseline_values = {name: baseline_seed_value_for(group.params[name]) for name in sorted(all_params)}
    baseline_url = build_url(group.scheme, group.host, group.path, baseline_values)
    baseline_ref = {
        **empty_expected_response(),
        "source": "requiredness_baseline",
        "note": "Populate from the matching baseline request response when executing this plan.",
    }
    if dry_run:
        queue_dry_run_request(
            plan_lines=dry_run_plan_lines,
            phase="requiredness_baseline",
            url=baseline_url,
            timeout=timeout,
            host=group.host,
            path=group.path,
            permutation=tuple(sorted(all_params)),
            mutated_parameter=None,
            mutated_value=None,
            expected_baseline_response=baseline_ref,
        )
        for candidate in sorted(optional):
            trial_values = dict(baseline_values)
            trial_values.pop(candidate, None)
            trial_url = build_url(group.scheme, group.host, group.path, trial_values)
            queue_dry_run_request(
                plan_lines=dry_run_plan_lines,
                phase="requiredness_probe_remove_parameter",
                url=trial_url,
                timeout=timeout,
                host=group.host,
                path=group.path,
                permutation=tuple(sorted(trial_values.keys())),
                mutated_parameter=candidate,
                mutated_value=None,
                expected_baseline_response=baseline_ref,
            )
        return required, optional

    if not optional:
        return required, optional

    baseline = request_url(baseline_url, timeout=timeout)
    time.sleep(delay)

    if int(baseline.get("status", 0)) == 0:
        return required, optional

    for candidate in sorted(optional):
        trial_values = dict(baseline_values)
        trial_values.pop(candidate, None)
        trial_url = build_url(group.scheme, group.host, group.path, trial_values)
        trial = request_url(trial_url, timeout=timeout)
        time.sleep(delay)
        # If response materially changes, treat as required for permutation generation.
        if response_differs(baseline, trial):
            required.add(candidate)

    optional = set(all_params - required)
    return required, optional


def generate_permutations(
    observed_sets: set[frozenset[str]],
    required: set[str],
    optional: set[str],
    max_permutations: int,
) -> list[tuple[str, ...]]:
    ordered: list[tuple[str, ...]] = []
    seen: set[tuple[str, ...]] = set()

    for observed in sorted(observed_sets, key=lambda items: (len(items), sorted(items))):
        combo = tuple(sorted(observed))
        if combo not in seen:
            seen.add(combo)
            ordered.append(combo)
            if len(ordered) >= max_permutations:
                return ordered

    opt_list = sorted(optional)
    for count in range(0, len(opt_list) + 1):
        for subset in itertools.combinations(opt_list, count):
            combo = tuple(sorted(set(required) | set(subset)))
            if combo not in seen:
                seen.add(combo)
                ordered.append(combo)
                if len(ordered) >= max_permutations:
                    return ordered
    return ordered


def infer_required_optional_from_observed(group: RouteGroup) -> tuple[set[str], set[str]]:
    all_params = set(group.params.keys())
    if not all_params:
        return set(), set()
    if group.observed_param_sets:
        observed_list = list(group.observed_param_sets)
        required = set(observed_list[0])
        for values in observed_list[1:]:
            required &= set(values)
    else:
        required = set()
    optional = set(all_params - required)
    return required, optional


def select_max_parameter_permutations(permutations: list[tuple[str, ...]]) -> list[tuple[str, ...]]:
    non_empty = [item for item in permutations if item]
    if not non_empty:
        return []
    max_len = max(len(item) for item in non_empty)
    selected: list[tuple[str, ...]] = []
    seen: set[tuple[str, ...]] = set()
    for item in non_empty:
        if len(item) != max_len:
            continue
        if item in seen:
            continue
        seen.add(item)
        selected.append(item)
    return selected


def estimate_group_request_count(
    permutations: list[tuple[str, ...]],
    quick_fuzz_values_count: int,
) -> int:
    fuzz_requests = 0
    for permutation in permutations:
        if not permutation:
            continue
        fuzz_requests += 1 + (len(permutation) * quick_fuzz_values_count)
    return fuzz_requests


def count_live_requests_for_group_with_allowlist(
    group: RouteGroup,
    permutations: list[tuple[str, ...]],
    quick_fuzz_values: list[str],
    allowed_request_keys: set[str] | None,
) -> int:
    """How many baseline + mutation requests would run for this group under an allowlist (or full count if None)."""
    if allowed_request_keys is None:
        return estimate_group_request_count(permutations, len(quick_fuzz_values))
    total = 0
    for permutation in permutations:
        if not permutation:
            continue
        baseline_values = {name: baseline_seed_value_for(group.params[name]) for name in permutation}
        baseline_url = build_url(group.scheme, group.host, group.path, baseline_values)
        baseline_key = make_request_key(
            phase="fuzz_baseline",
            host=group.host,
            path=group.path,
            permutation=permutation,
            mutated_parameter=None,
            mutated_value=None,
            url=baseline_url,
        )
        if baseline_key not in allowed_request_keys:
            continue
        total += 1
        for name in permutation:
            for fuzz_value in quick_fuzz_values:
                trial_values = dict(baseline_values)
                trial_values[name] = fuzz_value
                trial_url = build_url(group.scheme, group.host, group.path, trial_values)
                trial_key = make_request_key(
                    phase="fuzz_mutation",
                    host=group.host,
                    path=group.path,
                    permutation=permutation,
                    mutated_parameter=name,
                    mutated_value=fuzz_value,
                    url=trial_url,
                )
                if trial_key in allowed_request_keys:
                    total += 1
    return total


def anomaly_file_name(host: str, path: str, permutation: tuple[str, ...], parameter: str, value: str) -> str:
    seed = f"{host}|{path}|{','.join(permutation)}|{parameter}|{value}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    path_token = re.sub(r"[^A-Za-z0-9_-]+", "_", path.strip("/")) or "root"
    return f"anomaly_{host}_{path_token}_{digest}.json"


def reflection_file_name(host: str, path: str, permutation: tuple[str, ...], parameter: str, value: str) -> str:
    seed = f"{host}|{path}|{','.join(permutation)}|{parameter}|{value}|reflection"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    path_token = re.sub(r"[^A-Za-z0-9_-]+", "_", path.strip("/")) or "root"
    return f"reflection_{host}_{path_token}_{digest}.json"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _response_elapsed_ms(response: dict[str, Any] | None) -> int | None:
    if not isinstance(response, dict):
        return None
    v = response.get("elapsed_ms")
    if v is None:
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _pair_elapsed_ms_summary(
    baseline: dict[str, Any], anomaly: dict[str, Any]
) -> tuple[int | None, int | None, int | None]:
    b = _response_elapsed_ms(baseline)
    a = _response_elapsed_ms(anomaly)
    if b is None and a is None:
        return None, None, None
    if b is not None and a is not None:
        return b, a, a - b
    return b, a, None


def load_result_entries_from_folders(
    result_dirs: list[Path],
    *,
    source_domain: str | None = None,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    domain_label = str(source_domain).strip() if source_domain else ""
    for result_dir in result_dirs:
        if not result_dir.exists():
            continue
        for path in sorted(result_dir.rglob("*.json")):
            name = path.name.lower()
            path_key = str(path.resolve())
            if path_key in seen_paths:
                continue
            seen_paths.add(path_key)
            try:
                payload = json.loads(path.read_text(encoding="utf-8-sig"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            is_anomaly_name = name.startswith("anomaly_") or name.startswith("anomoly_")
            is_reflection_name = name.startswith("reflection_")
            has_result_shape = isinstance(payload.get("baseline"), dict) and (
                isinstance(payload.get("anomaly"), dict) or isinstance(payload.get("response"), dict)
            )
            if not (is_anomaly_name or is_reflection_name or has_result_shape):
                continue
            baseline = payload.get("baseline", {}) if isinstance(payload.get("baseline"), dict) else {}
            response = payload.get("anomaly", {}) if isinstance(payload.get("anomaly"), dict) else {}
            if not response and isinstance(payload.get("response"), dict):
                response = payload.get("response", {})
            url = str(payload.get("requested_url") or payload.get("url") or "").strip()
            requested_url = str(payload.get("requested_url", "") or "").strip()
            target_url = requested_url or url
            baseline_url = str(baseline.get("url", "") or "").strip()
            response_url = str(response.get("url", "") or "").strip()
            # Some historical files were saved with baseline/anomaly reversed.
            # If baseline points to the mutated requested URL while anomaly does not, swap them.
            if target_url and baseline_url == target_url and response_url and response_url != target_url:
                baseline, response = response, baseline
                baseline_url, response_url = response_url, baseline_url
            # Legacy baseline values that used sampletoken are often invalid.
            # If baseline is an error with sampletoken and anomaly maps to requested_url success, treat as reversed.
            baseline_status_hint = _safe_int(baseline.get("status", 0))
            response_status_hint = _safe_int(response.get("status", 0))
            if (
                target_url
                and response_url == target_url
                and "sampletoken" in baseline_url.lower()
                and baseline_status_hint >= 400
                and 200 <= response_status_hint < 400
            ):
                baseline, response = response, baseline
            captured_at = str(payload.get("captured_at_utc", "")).strip()
            baseline_status = _safe_int(baseline.get("status", 0))
            new_status = _safe_int(response.get("status", 0))
            baseline_size = _safe_int(baseline.get("size", 0))
            new_size = _safe_int(response.get("size", 0))
            baseline_body = str(baseline.get("body_preview", "") or "")
            anomaly_body = str(response.get("body_preview", "") or "")
            diff_lines = list(
                difflib.unified_diff(
                    baseline_body.splitlines(),
                    anomaly_body.splitlines(),
                    fromfile="baseline",
                    tofile="anomaly",
                    lineterm="",
                )
            )
            result_type = "reflection" if is_reflection_name else "anomaly"
            entries.append(
                {
                    "result_type": result_type,
                    "source_domain": domain_label,
                    "result_file": str(path),
                    "captured_at_utc": captured_at,
                    "url": url,
                    "host": str(payload.get("host", "")),
                    "path": str(payload.get("path", "")),
                    "mutated_parameter": str(payload.get("mutated_parameter", "")),
                    "mutated_value": str(payload.get("mutated_value", "")),
                    "baseline_status": baseline_status,
                    "new_status": new_status,
                    "baseline_size": baseline_size,
                    "new_size": new_size,
                    "size_difference": new_size - baseline_size,
                    "baseline_response": baseline,
                    "anomaly_response": response,
                    "response_diff_text": "\n".join(diff_lines[:400]),
                }
            )
    entries.sort(
        key=lambda item: (
            item.get("captured_at_utc", ""),
            item.get("source_domain", ""),
            item.get("result_file", ""),
        )
    )
    return entries


def discover_fozzy_domain_output_pairs(master_root: Path) -> list[tuple[str, Path]]:
    """Each immediate child directory with a ``results/`` subfolder is treated as one domain output tree."""
    if not master_root.is_dir():
        return []
    pairs: list[tuple[str, Path]] = []
    for child in sorted(master_root.iterdir(), key=lambda p: p.name.lower()):
        if not child.is_dir() or child.name.startswith("."):
            continue
        results_sub = child / "results"
        if results_sub.is_dir():
            pairs.append((child.name, child.resolve()))
    return pairs


def discover_fozzy_domain_output_pairs_nested(scan_root: Path) -> list[tuple[str, Path]]:
    """Discover ``output/<registrable>/fozzy-output/<domain>/results`` style trees under ``scan_root``."""
    if not scan_root.is_dir():
        return []
    pairs: list[tuple[str, Path]] = []
    used_labels: set[str] = set()
    for domain_dir in sorted(scan_root.iterdir(), key=lambda p: p.name.lower()):
        if not domain_dir.is_dir() or domain_dir.name.startswith("."):
            continue
        fozzy_out = domain_dir / "fozzy-output"
        if not fozzy_out.is_dir():
            continue
        for inner in sorted(fozzy_out.iterdir(), key=lambda p: p.name.lower()):
            if not inner.is_dir() or inner.name.startswith("."):
                continue
            if not (inner / "results").is_dir():
                continue
            label = inner.name
            if label in used_labels:
                label = f"{domain_dir.name}/{inner.name}"
            used_labels.add(label)
            pairs.append((label, inner.resolve()))
    return pairs


def master_report_uses_nested_layout(aggregate_root: Path) -> bool:
    """True if ``aggregate_root`` looks like Nightmare's multi-domain ``output/`` directory."""
    if not aggregate_root.is_dir():
        return False
    for child in aggregate_root.iterdir():
        if child.is_dir() and not child.name.startswith(".") and (child / "fozzy-output").is_dir():
            return True
    return False


def list_results_files_for_master(pairs: list[tuple[str, Path]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for domain_name, domain_output in pairs:
        for rec in list_results_folder_files(domain_output / "results"):
            row = dict(rec)
            row["domain_output"] = domain_name
            records.append(row)
    records.sort(key=lambda item: (str(item.get("domain_output", "")), str(item.get("name", ""))))
    return records


def count_master_discrepancy_json_files(pairs: list[tuple[str, Path]]) -> dict[str, int]:
    """Count anomaly/reflection JSON files on disk (same rules as load_result_entries_from_folders)."""
    anomalies = 0
    reflections = 0
    for _domain_name, domain_output in pairs:
        scan_dirs = [domain_output / "results"]
        legacy = domain_output / "anomalies"
        if legacy.is_dir():
            scan_dirs.append(legacy)
        for base in scan_dirs:
            if not base.is_dir():
                continue
            for path in base.rglob("*.json"):
                low = path.name.lower()
                if low.startswith("anomaly_") or low.startswith("anomoly_"):
                    anomalies += 1
                elif low.startswith("reflection_"):
                    reflections += 1
    return {
        "filesystem_anomaly_json": anomalies,
        "filesystem_reflection_json": reflections,
        "filesystem_total_discrepancy_json": anomalies + reflections,
    }


def resolve_site_directory_from_domain_output(domain_output: Path) -> Path | None:
    """Nightmare site folder (e.g. ``output/example.com``) from a Fozzy output tree."""
    p = domain_output.resolve()
    parent = p.parent
    if parent.name == "fozzy-output":
        grand = parent.parent
        return grand if grand.is_dir() else None
    return parent if parent.is_dir() else None


def find_parameters_json_for_domain_output(domain_output: Path) -> Path | None:
    site = resolve_site_directory_from_domain_output(domain_output)
    if site and site.is_dir():
        cand = site / f"{site.name}.parameters.json"
        if cand.is_file():
            return cand
    for anc in (domain_output.parent, domain_output.parent.parent):
        if anc.is_dir():
            cand = anc / f"{domain_output.name}.parameters.json"
            if cand.is_file():
                return cand
    return None


def find_fozzy_inventory_json(domain_output: Path) -> Path | None:
    matches = sorted(domain_output.glob("*.fozzy.inventory.json"))
    return matches[0] if matches else None


def _route_parameter_count(route: dict[str, Any]) -> int:
    raw = route.get("parameter_count")
    try:
        pc = int(raw) if raw is not None else 0
    except (TypeError, ValueError):
        pc = 0
    params = route.get("parameters")
    if isinstance(params, list):
        pc = max(pc, len(params))
    return max(0, pc)


def aggregate_inventory_route_stats(routes: list[dict[str, Any]]) -> dict[str, Any]:
    seen_keys: set[tuple[str, str]] = set()
    routes_with_parameters = 0
    total_parameter_slots = 0
    max_parameters_on_route = 0
    hosts: set[str] = set()
    for r in routes:
        if not isinstance(r, dict):
            continue
        h = str(r.get("host", "")).strip().lower()
        pth = str(r.get("path", "")).strip() or "/"
        if not h:
            continue
        seen_keys.add((h, pth))
        pc = _route_parameter_count(r)
        if pc > 0:
            routes_with_parameters += 1
        total_parameter_slots += pc
        max_parameters_on_route = max(max_parameters_on_route, pc)
        hosts.add(h)
    n_routes = len(seen_keys)
    avg = 0.0
    if routes_with_parameters > 0:
        avg = total_parameter_slots / routes_with_parameters
    return {
        "unique_routes": n_routes,
        "routes_with_parameters": routes_with_parameters,
        "total_parameter_slots": total_parameter_slots,
        "max_parameters_on_route": max_parameters_on_route,
        "unique_hosts": len(hosts),
        "avg_parameters_on_parameterized_routes": round(avg, 2),
    }


def aggregate_route_groups_stats(groups: list[RouteGroup]) -> dict[str, Any]:
    seen_keys: set[tuple[str, str]] = set()
    routes_with_parameters = 0
    total_parameter_slots = 0
    max_parameters_on_route = 0
    hosts: set[str] = set()
    for g in groups:
        seen_keys.add((g.host.lower(), g.path or "/"))
        n = len(g.params)
        if n > 0:
            routes_with_parameters += 1
        total_parameter_slots += n
        max_parameters_on_route = max(max_parameters_on_route, n)
        hosts.add(g.host.lower())
    n_routes = len(seen_keys)
    avg = 0.0
    if routes_with_parameters > 0:
        avg = total_parameter_slots / routes_with_parameters
    return {
        "unique_routes": n_routes,
        "routes_with_parameters": routes_with_parameters,
        "total_parameter_slots": total_parameter_slots,
        "max_parameters_on_route": max_parameters_on_route,
        "unique_hosts": len(hosts),
        "avg_parameters_on_parameterized_routes": round(avg, 2),
    }


def build_master_domain_inventory_tables(
    pairs: list[tuple[str, Path]],
    *,
    per_route_row_cap: int = 2000,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    """Domain-level and per-route rows for the master HTML report (from inventory or parameters JSON)."""
    domain_rows: list[dict[str, Any]] = []
    per_route_rows: list[dict[str, Any]] = []
    truncated = False
    for domain_label, domain_output in pairs:
        site_dir = resolve_site_directory_from_domain_output(domain_output)
        site_folder_str = str(site_dir) if site_dir else ""
        inv_path = find_fozzy_inventory_json(domain_output)
        source = "unavailable"
        stats: dict[str, Any] | None = None
        routes_for_detail: list[dict[str, Any]] = []
        if inv_path and inv_path.is_file():
            try:
                inv_data = read_json(inv_path)
            except (OSError, json.JSONDecodeError):
                inv_data = {}
            if isinstance(inv_data, dict):
                raw_routes = inv_data.get("routes", [])
                routes_for_detail = [r for r in raw_routes if isinstance(r, dict)]
                stats = aggregate_inventory_route_stats(routes_for_detail)
                source = "fozzy.inventory"
        if stats is None:
            params_path = find_parameters_json_for_domain_output(domain_output)
            if params_path and params_path.is_file():
                try:
                    payload = read_json(params_path)
                except (OSError, json.JSONDecodeError):
                    payload = {}
                if isinstance(payload, dict):
                    groups = load_route_groups(payload)
                    stats = aggregate_route_groups_stats(groups)
                    source = "parameters.json"
                    routes_for_detail = [
                        {
                            "host": g.host,
                            "path": g.path,
                            "scheme": g.scheme,
                            "parameter_count": len(g.params),
                        }
                        for g in groups
                    ]
        if stats is None:
            domain_rows.append(
                {
                    "domain_label": domain_label,
                    "site_folder": site_folder_str,
                    "unique_routes": 0,
                    "routes_with_parameters": 0,
                    "total_parameter_slots": 0,
                    "max_parameters_on_route": 0,
                    "unique_hosts": 0,
                    "avg_parameters_on_parameterized_routes": 0.0,
                    "source": "unavailable",
                }
            )
            continue
        domain_rows.append(
            {
                "domain_label": domain_label,
                "site_folder": site_folder_str,
                "source": source,
                **stats,
            }
        )
        for r in routes_for_detail:
            h = str(r.get("host", "")).strip().lower()
            pth = str(r.get("path", "")).strip() or "/"
            if not h:
                continue
            sch = str(r.get("scheme", "") or "https").strip().lower() or "https"
            per_route_rows.append(
                {
                    "domain_label": domain_label,
                    "host": h,
                    "path": pth,
                    "scheme": sch,
                    "parameter_count": _route_parameter_count(r) if isinstance(r, dict) else 0,
                }
            )
    per_route_rows.sort(
        key=lambda row: (
            -int(row.get("parameter_count", 0) or 0),
            str(row.get("domain_label", "")),
            str(row.get("host", "")),
            str(row.get("path", "")),
        )
    )
    if len(per_route_rows) > per_route_row_cap:
        truncated = True
        per_route_rows = per_route_rows[:per_route_row_cap]
    return domain_rows, per_route_rows, truncated


def build_results_summary_payload(
    *,
    root_domain: str,
    parameters_path: Path,
    result_entries: list[dict[str, Any]],
    interrupted: bool,
    interrupted_group: str | None,
    results_folder_files: list[dict[str, Any]],
    report_heading: str | None = None,
    summary_scope: str = "single_domain",
    master_scan_root: str | None = None,
    domains_scanned: int | None = None,
    payload_extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    anomaly_entries = [item for item in result_entries if str(item.get("result_type", "")) == "anomaly"]
    reflection_entries = [item for item in result_entries if str(item.get("result_type", "")) == "reflection"]
    unique_urls = sorted({str(item.get("url", "")).strip() for item in result_entries if str(item.get("url", "")).strip()})
    by_url_counter: dict[str, dict[str, Any]] = {}
    status_code_changes = 0
    size_changes = 0
    for item in result_entries:
        baseline_status = _safe_int(item.get("baseline_status", 0))
        new_status = _safe_int(item.get("new_status", 0))
        baseline_size = _safe_int(item.get("baseline_size", 0))
        new_size = _safe_int(item.get("new_size", 0))
        size_diff = new_size - baseline_size
        if baseline_status != new_status:
            status_code_changes += 1
        if baseline_size != new_size:
            size_changes += 1
        url_key = str(item.get("url", "")).strip()
        if url_key not in by_url_counter:
            by_url_counter[url_key] = {
                "url": url_key,
                "count": 0,
                "status_change_count": 0,
                "max_abs_size_difference": 0,
                "baseline_response_codes": set(),
                "anomaly_response_codes": set(),
            }
        by_url_counter[url_key]["count"] += 1
        if baseline_status != new_status:
            by_url_counter[url_key]["status_change_count"] += 1
        by_url_counter[url_key]["max_abs_size_difference"] = max(
            _safe_int(by_url_counter[url_key]["max_abs_size_difference"], 0), abs(size_diff)
        )
        by_url_counter[url_key]["baseline_response_codes"].add(baseline_status)
        by_url_counter[url_key]["anomaly_response_codes"].add(new_status)
    for entry in by_url_counter.values():
        entry["baseline_response_codes"] = sorted(int(code) for code in entry["baseline_response_codes"])
        entry["anomaly_response_codes"] = sorted(int(code) for code in entry["anomaly_response_codes"])
    by_url = sorted(
        by_url_counter.values(),
        key=lambda item: (-_safe_int(item.get("count", 0)), str(item.get("url", ""))),
    )
    totals: dict[str, Any] = {
        "total_results": len(result_entries),
        "total_anomalies": len(anomaly_entries),
        "total_reflections": len(reflection_entries),
        "unique_discrepancy_urls_count": len(unique_urls),
        "status_code_changes": status_code_changes,
        "size_changes": size_changes,
    }
    if domains_scanned is not None:
        totals["domains_scanned"] = int(domains_scanned)
    input_pf = str(parameters_path)
    if summary_scope == "master":
        root_hint = master_scan_root or str(parameters_path)
        input_pf = f"(multi-domain aggregate; scanned folder: {root_hint})"
    anomaly_summary_payload: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root_domain": root_domain,
        "input_parameters_file": input_pf,
        "interrupted": interrupted,
        "interrupted_group": interrupted_group,
        "summary_scope": summary_scope,
        "totals": totals,
        "unique_discrepancy_urls": unique_urls,
        "by_url": by_url,
        "discrepancies": result_entries,
        "results_folder_files": list(results_folder_files),
    }
    if report_heading:
        anomaly_summary_payload["report_heading"] = report_heading
    if master_scan_root:
        anomaly_summary_payload["master_scan_root"] = master_scan_root
    extras = payload_extras if isinstance(payload_extras, dict) else {}
    master_inv = extras.get("master_domain_inventory")
    if isinstance(master_inv, list):
        anomaly_summary_payload["master_domain_inventory"] = master_inv
    master_routes = extras.get("master_per_route_inventory")
    if isinstance(master_routes, list):
        anomaly_summary_payload["master_per_route_inventory"] = master_routes
    if extras.get("master_per_route_inventory_truncated"):
        anomaly_summary_payload["master_per_route_inventory_truncated"] = True
    return anomaly_summary_payload


def write_master_results_summary(
    master_root: Path,
    *,
    prefer_nested: bool | None = None,
) -> tuple[Path, Path, int]:
    """Aggregate every domain's ``results/`` (and legacy ``anomalies/``) into one report under ``master_root``.

    If ``prefer_nested`` is None, chooses nested (``output/<site>/fozzy-output/...``) when that
    layout is detected under ``master_root``, otherwise the flat layout (immediate children with
    ``results/``).
    """
    master_root = master_root.expanduser().resolve()
    nested = master_report_uses_nested_layout(master_root) if prefer_nested is None else bool(prefer_nested)
    pairs = (
        discover_fozzy_domain_output_pairs_nested(master_root)
        if nested
        else discover_fozzy_domain_output_pairs(master_root)
    )
    all_entries: list[dict[str, Any]] = []
    for domain_name, domain_output in pairs:
        legacy_anomaly_dir = domain_output / "anomalies"
        scan_dirs = [domain_output / "results"]
        if legacy_anomaly_dir.is_dir():
            scan_dirs.append(legacy_anomaly_dir)
        all_entries.extend(load_result_entries_from_folders(scan_dirs, source_domain=domain_name))
    all_entries.sort(
        key=lambda item: (
            item.get("captured_at_utc", ""),
            item.get("source_domain", ""),
            item.get("result_file", ""),
        )
    )
    folder_files = list_results_files_for_master(pairs)
    domain_inv_rows, per_route_rows, per_route_truncated = build_master_domain_inventory_tables(pairs)
    payload = build_results_summary_payload(
        root_domain="all_domains",
        parameters_path=master_root,
        result_entries=all_entries,
        interrupted=False,
        interrupted_group=None,
        results_folder_files=folder_files,
        report_heading="Master report — all domains (anomalies & reflections)",
        summary_scope="master",
        master_scan_root=str(master_root),
        domains_scanned=len(pairs),
        payload_extras={
            "master_domain_inventory": domain_inv_rows,
            "master_per_route_inventory": per_route_rows,
            "master_per_route_inventory_truncated": per_route_truncated,
        },
    )
    json_path = master_root / "all_domains.results_summary.json"
    html_path = master_root / "all_domains.results_summary.html"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    html_path.write_text(render_anomaly_summary_html(payload), encoding="utf-8")
    return json_path, html_path, len(pairs)


def write_results_summary(
    *,
    root_domain: str,
    parameters_path: Path,
    output_dir: Path,
    results_dir: Path,
    interrupted: bool,
    interrupted_group: str | None,
) -> tuple[Path, Path]:
    legacy_anomaly_dir = output_dir / "anomalies"
    scan_dirs = [results_dir]
    if legacy_anomaly_dir.exists():
        scan_dirs.append(legacy_anomaly_dir)
    result_entries = load_result_entries_from_folders(scan_dirs, source_domain=None)
    anomaly_summary_payload = build_results_summary_payload(
        root_domain=root_domain,
        parameters_path=parameters_path,
        result_entries=result_entries,
        interrupted=interrupted,
        interrupted_group=interrupted_group,
        results_folder_files=[],
        summary_scope="single_domain",
    )
    anomaly_summary_path = results_dir / f"{root_domain}.results_summary.json"
    anomaly_summary_path.write_text(json.dumps(anomaly_summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    anomaly_summary_html_path = results_dir / f"{root_domain}.results_summary.html"
    anomaly_summary_html_path.write_text(render_anomaly_summary_html(anomaly_summary_payload), encoding="utf-8")

    anomaly_summary_payload["results_folder_files"] = list_results_folder_files(results_dir)
    anomaly_summary_path.write_text(json.dumps(anomaly_summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    anomaly_summary_html_path.write_text(render_anomaly_summary_html(anomaly_summary_payload), encoding="utf-8")
    return anomaly_summary_path, anomaly_summary_html_path


def build_body_side_by_side_diff_html(baseline_body: str, anomaly_body: str) -> str:
    """HTML table: baseline vs anomaly body_preview with line-level add/remove/change highlighting (difflib.HtmlDiff)."""
    left = str(baseline_body or "")
    right = str(anomaly_body or "")
    if not left.strip() and not right.strip():
        return "<p class='diff-empty-note'>No body previews available to compare.</p>"
    left_lines = left.splitlines()
    right_lines = right.splitlines()
    if not left_lines:
        left_lines = [""]
    if not right_lines:
        right_lines = [""]
    differ = difflib.HtmlDiff(tabsize=4, wrapcolumn=96)
    return differ.make_table(
        left_lines,
        right_lines,
        "Baseline body",
        "Anomaly body",
        context=False,
        numlines=5,
    )


def render_anomaly_summary_html(payload: dict[str, Any]) -> str:
    rows = payload.get("discrepancies", []) if isinstance(payload.get("discrepancies"), list) else []
    include_domain = any(
        str((item.get("source_domain") or "")).strip() for item in rows if isinstance(item, dict)
    )
    report_heading = str(payload.get("report_heading") or "All discrepancies")
    doc_title = "Master results" if str(payload.get("summary_scope", "")) == "master" else "results summary"
    column_labels: list[str] = ["Type"]
    if include_domain:
        column_labels.append("Domain")
    column_labels.extend(
        [
            "Time",
            "URL",
            "Parameter",
            "Value",
            "Baseline response",
            "Anomaly response",
            "Baseline time (ms)",
            "Anomaly time (ms)",
            "Time diff (ms)",
            "Baseline size (bytes)",
            "Anomaly size (bytes)",
            "Size diff (bytes)",
            "Tools",
            "Result file",
        ]
    )
    table_col_count = len(column_labels)
    detail_column_labels_json = json.dumps(column_labels, ensure_ascii=False).replace("</", "<\\/")
    group_select_options = ['<option value="-1">(None — flat list)</option>']
    for i, lab in enumerate(column_labels):
        group_select_options.append(f'<option value="{i}">{html.escape(lab)}</option>')
    detail_group_by_select_html = (
        '<label class="group-by-label" for="detailGroupBy">Group by</label> '
        '<select id="detailGroupBy" title="Show collapsible sections for each distinct value in this column">'
        + "".join(group_select_options)
        + "</select>"
    )
    header_cells: list[str] = ['<th data-type="string">Type</th>']
    if include_domain:
        header_cells.append('<th data-type="string">Domain</th>')
    header_cells.extend(
        [
            '<th data-type="string">Time</th>',
            '<th data-type="string">URL</th>',
            '<th data-type="string">Parameter</th>',
            '<th data-type="string">Value</th>',
            '<th data-type="number">Baseline response</th>',
            '<th data-type="number">Anomaly response</th>',
            '<th data-type="number">Baseline time (ms)</th>',
            '<th data-type="number">Anomaly time (ms)</th>',
            '<th data-type="number">Time diff (ms)</th>',
            '<th data-type="number">Baseline size (bytes)</th>',
            '<th data-type="number">Anomaly size (bytes)</th>',
            '<th data-type="number">Size diff (bytes)</th>',
            '<th data-type="string">Tools</th>',
            '<th data-type="string">Result file</th>',
        ]
    )
    thead_row_main = "<tr>" + "".join(header_cells) + "</tr>"
    filter_placeholders = [
        "Filter type",
        *(
            [
                "Filter domain",
            ]
            if include_domain
            else []
        ),
        "Filter time",
        "Filter URL",
        "Filter param",
        "Filter value",
        "Filter baseline (code or phrase)",
        "Filter anomaly (code or phrase)",
        "Filter baseline ms",
        "Filter anomaly ms",
        "Filter time diff ms (anomaly − baseline)",
        "Filter baseline bytes",
        "Filter anomaly bytes",
        "Filter diff bytes",
        "Filter tools",
        "Filter file",
    ]
    filter_cells: list[str] = []
    for col_idx, ph in enumerate(filter_placeholders):
        filter_cells.append(
            f'<th><input data-col="{col_idx}" type="text" placeholder="{html.escape(ph)}"></th>'
        )
    thead_filters = '<tr class="filters">\n          ' + "\n          ".join(filter_cells) + "\n        </tr>"

    def clip_cell(value: Any) -> tuple[str, str]:
        raw = str(value or "")
        return (
            f"<span class='clip-cell' title='{html.escape(raw)}'>{html.escape(raw)}</span>",
            raw,
        )

    def file_href(path_value: Any) -> str:
        raw = str(path_value or "").strip()
        if not raw:
            return ""
        normalized = raw.replace("\\", "/")
        return f"file:///{urllib.parse.quote(normalized, safe='/:._-()')}"

    def size_bytes_text(size_bytes: Any) -> str:
        return str(_safe_int(size_bytes, 0))

    def format_display_time(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt_utc = dt.astimezone(timezone.utc)
            return dt_utc.strftime("%H:%M:%S")
        except Exception:
            return raw

    response_records: list[dict[str, Any]] = []
    detail_rows_html: list[str] = []
    for idx, item in enumerate(rows):
        if not isinstance(item, dict):
            continue
        response_id = f"r{idx + 1}"
        baseline_response = item.get("baseline_response") if isinstance(item.get("baseline_response"), dict) else {}
        anomaly_response = item.get("anomaly_response") if isinstance(item.get("anomaly_response"), dict) else {}
        response_diff_text = str(item.get("response_diff_text", "") or "")
        baseline_body = str(baseline_response.get("body_preview", "") or "")
        anomaly_body = str(anomaly_response.get("body_preview", "") or "")
        diff_side_by_side_html = build_body_side_by_side_diff_html(baseline_body, anomaly_body)
        response_records.append(
            {
                "id": response_id,
                "baseline": baseline_response,
                "anomaly": anomaly_response,
                "diff": response_diff_text,
                "diff_side_by_side_html": diff_side_by_side_html,
            }
        )

        captured_raw_value = str(item.get("captured_at_utc", "") or "")
        captured_display, captured_raw = clip_cell(format_display_time(captured_raw_value))
        url_display, url_raw = clip_cell(item.get("url", ""))
        param_display, param_raw = clip_cell(item.get("mutated_parameter", ""))
        value_display, value_raw = clip_cell(item.get("mutated_value", ""))
        result_raw = str(item.get("result_file", "") or "")
        result_link = (
            f"<a class='clip-cell file-link' title='{html.escape(result_raw)}' href='{html.escape(file_href(result_raw))}' "
            f"target='_blank' rel='noopener noreferrer'>{html.escape(result_raw)}</a>"
            if result_raw
            else ""
        )

        baseline_status = _safe_int(item.get("baseline_status", 0))
        anomaly_status = _safe_int(item.get("new_status", 0))
        baseline_size = _safe_int(item.get("baseline_size", 0))
        anomaly_size = _safe_int(item.get("new_size", 0))
        size_diff = anomaly_size - baseline_size

        baseline_label = f"{baseline_status} {_http_status_phrase(baseline_status)}"
        anomaly_label = f"{anomaly_status} {_http_status_phrase(anomaly_status)}"
        baseline_display, _ = clip_cell(baseline_label)
        anomaly_display, _ = clip_cell(anomaly_label)
        baseline_raw = str(baseline_status)
        anomaly_raw = str(anomaly_status)

        elapsed_b_ms, elapsed_a_ms, elapsed_diff_ms = _pair_elapsed_ms_summary(baseline_response, anomaly_response)
        if elapsed_b_ms is None:
            baseline_ms_cell = "<td data-raw=''>—</td>"
        else:
            baseline_ms_cell = f"<td data-raw='{elapsed_b_ms}'>{elapsed_b_ms}</td>"
        if elapsed_a_ms is None:
            anomaly_ms_cell = "<td data-raw=''>—</td>"
        else:
            anomaly_ms_cell = f"<td data-raw='{elapsed_a_ms}'>{elapsed_a_ms}</td>"
        if elapsed_diff_ms is None:
            time_diff_cell = "<td data-raw=''>—</td>"
        else:
            td_cls = "size-diff-zero"
            if elapsed_diff_ms > 0:
                td_cls = "size-diff-pos"
            elif elapsed_diff_ms < 0:
                td_cls = "size-diff-neg"
            time_diff_cell = (
                f"<td data-raw='{elapsed_diff_ms}'><span class='{td_cls}'>{elapsed_diff_ms:+d}</span></td>"
            )

        diff_class = "size-diff-zero"
        if size_diff > 0:
            diff_class = "size-diff-pos"
        elif size_diff < 0:
            diff_class = "size-diff-neg"
        diff_text = f"{size_diff:+d}"

        result_type_raw = str(item.get("result_type", "") or "").strip() or "unknown"
        type_display, type_raw = clip_cell(result_type_raw)
        source_domain_raw = str(item.get("source_domain", "") or "").strip()
        domain_cell = ""
        if include_domain:
            domain_display, domain_raw_cell = clip_cell(source_domain_raw)
            domain_cell = f"<td data-raw='{html.escape(domain_raw_cell)}'>{domain_display}</td>"

        body_search = (
            f"{result_type_raw}\n{source_domain_raw}\n{baseline_body}\n{anomaly_body}\n"
            f"{elapsed_b_ms if elapsed_b_ms is not None else ''}\n"
            f"{elapsed_a_ms if elapsed_a_ms is not None else ''}\n"
            f"{elapsed_diff_ms if elapsed_diff_ms is not None else ''}"
        ).lower()

        detail_rows_html.append(
            f"<tr class='detail-data-row' data-search-extra='{html.escape(body_search)}'>"
            f"<td data-raw='{html.escape(type_raw)}'>{type_display}</td>"
            f"{domain_cell}"
            f"<td data-raw='{html.escape(captured_raw)}'>{captured_display}</td>"
            f"<td data-raw='{html.escape(url_raw)}'>{url_display}</td>"
            f"<td data-raw='{html.escape(param_raw)}'>{param_display}</td>"
            f"<td data-raw='{html.escape(value_raw)}'>{value_display}</td>"
            f"<td data-raw='{html.escape(baseline_raw)}'><a href='#' class='resp-link' data-kind='baseline' data-id='{html.escape(response_id)}'>{baseline_display}</a></td>"
            f"<td data-raw='{html.escape(anomaly_raw)}'><a href='#' class='resp-link' data-kind='anomaly' data-id='{html.escape(response_id)}'>{anomaly_display}</a></td>"
            f"{baseline_ms_cell}"
            f"{anomaly_ms_cell}"
            f"{time_diff_cell}"
            f"<td data-raw='{html.escape(size_bytes_text(baseline_size))}'>{html.escape(size_bytes_text(baseline_size))}</td>"
            f"<td data-raw='{html.escape(size_bytes_text(anomaly_size))}'>{html.escape(size_bytes_text(anomaly_size))}</td>"
            f"<td data-raw='{html.escape(diff_text)}'><span class='{diff_class}'>{html.escape(diff_text)}</span></td>"
            f"<td data-raw='diff'><a href='#' class='tool-diff' data-id='{html.escape(response_id)}'>diff</a></td>"
            f"<td data-raw='{html.escape(result_raw)}'>{result_link}</td>"
            "</tr>"
        )
    detail_table = (
        "\n".join(detail_rows_html)
        if detail_rows_html
        else f"<tr><td colspan='{table_col_count}'>No discrepancies</td></tr>"
    )
    response_data_json = json.dumps(response_records, ensure_ascii=False).replace("</", "<\\/")

    inventory_sections_html = ""
    inventory_script_html = ""
    master_inv_rows = payload.get("master_domain_inventory")
    master_route_rows = payload.get("master_per_route_inventory")
    if str(payload.get("summary_scope", "")) == "master" and isinstance(master_inv_rows, list) and master_inv_rows:
        dom_lines: list[str] = []
        sum_unique = 0
        sum_with_p = 0
        sum_slots = 0
        sum_hosts = 0
        max_max = 0
        for inv in master_inv_rows:
            if not isinstance(inv, dict):
                continue
            lbl = str(inv.get("domain_label", "") or "")
            src = str(inv.get("source", "") or "")
            ur = int(inv.get("unique_routes", 0) or 0)
            rwp = int(inv.get("routes_with_parameters", 0) or 0)
            tps = int(inv.get("total_parameter_slots", 0) or 0)
            mx = int(inv.get("max_parameters_on_route", 0) or 0)
            uh = int(inv.get("unique_hosts", 0) or 0)
            avg = inv.get("avg_parameters_on_parameterized_routes", 0)
            try:
                avg_f = float(avg) if avg is not None else 0.0
            except (TypeError, ValueError):
                avg_f = 0.0
            site = str(inv.get("site_folder", "") or "")
            sum_unique += ur
            sum_with_p += rwp
            sum_slots += tps
            sum_hosts += uh
            max_max = max(max_max, mx)
            dom_lines.append(
                "<tr>"
                f"<td data-raw='{html.escape(lbl)}'><span class='clip-cell' title='{html.escape(lbl)}'>{html.escape(lbl)}</span></td>"
                f"<td data-raw='{html.escape(src)}'>{html.escape(src)}</td>"
                f"<td data-raw='{ur}'>{ur}</td>"
                f"<td data-raw='{rwp}'>{rwp}</td>"
                f"<td data-raw='{tps}'>{tps}</td>"
                f"<td data-raw='{mx}'>{mx}</td>"
                f"<td data-raw='{avg_f}'>{avg_f}</td>"
                f"<td data-raw='{uh}'>{uh}</td>"
                f"<td data-raw='{html.escape(site)}'><span class='clip-cell' title='{html.escape(site)}'>{html.escape(site)}</span></td>"
                "</tr>"
            )
        dom_tbody = "\n".join(dom_lines) if dom_lines else "<tr><td colspan='9'>No rows</td></tr>"
        dom_tfoot = (
            "<tfoot><tr>"
            "<td data-raw='totals'><strong>Totals / max</strong></td>"
            "<td data-raw=''></td>"
            f"<td data-raw='{sum_unique}'><strong>{sum_unique}</strong></td>"
            f"<td data-raw='{sum_with_p}'><strong>{sum_with_p}</strong></td>"
            f"<td data-raw='{sum_slots}'><strong>{sum_slots}</strong></td>"
            f"<td data-raw='{max_max}'><strong>{max_max}</strong></td>"
            "<td data-raw=''></td>"
            f"<td data-raw='{sum_hosts}'><strong>{sum_hosts}</strong></td>"
            "<td data-raw=''></td>"
            "</tr></tfoot>"
        )
        n_dom = len([r for r in master_inv_rows if isinstance(r, dict)])
        route_note = ""
        route_block = ""
        if isinstance(master_route_rows, list) and master_route_rows:
            trunc = bool(payload.get("master_per_route_inventory_truncated"))
            route_note = (
                "<p class='inventory-note'>Per-route rows (scheme + host + path), sorted by parameter count. "
                + ("List truncated for report size; re-run with a smaller workspace or inspect each domain's "
                   "<code>*.fozzy.inventory.json</code> for the full list. "
                   if trunc
                   else "")
                + "</p>"
            )
            rlines: list[str] = []
            for rr in master_route_rows:
                if not isinstance(rr, dict):
                    continue
                dl = str(rr.get("domain_label", "") or "")
                ho = str(rr.get("host", "") or "")
                pa = str(rr.get("path", "") or "")
                sc = str(rr.get("scheme", "") or "https")
                pc = int(rr.get("parameter_count", 0) or 0)
                rlines.append(
                    "<tr>"
                    f"<td data-raw='{html.escape(dl)}'><span class='clip-cell' title='{html.escape(dl)}'>{html.escape(dl)}</span></td>"
                    f"<td data-raw='{html.escape(ho)}'>{html.escape(ho)}</td>"
                    f"<td data-raw='{html.escape(pa)}'><span class='clip-cell' title='{html.escape(pa)}'>{html.escape(pa)}</span></td>"
                    f"<td data-raw='{html.escape(sc)}'>{html.escape(sc)}</td>"
                    f"<td data-raw='{pc}'>{pc}</td>"
                    "</tr>"
                )
            rt_body = "\n".join(rlines) if rlines else "<tr><td colspan='5'>No routes</td></tr>"
            route_block = f"""
    <h3>Routes and parameter counts</h3>
    {route_note}
    <div class="count-note" id="masterRouteInventoryCount"></div>
    <div class="scroll-inventory">
      <table id="masterRouteInventoryTable" class="data-table">
        <thead>
          <tr>
            <th data-type="string">Domain</th>
            <th data-type="string">Host</th>
            <th data-type="string">Path</th>
            <th data-type="string">Scheme</th>
            <th data-type="number">Parameters</th>
          </tr>
        </thead>
        <tbody>{rt_body}</tbody>
      </table>
    </div>"""
        inventory_sections_html = f"""
  <section class="inventory-section">
    <h3>Domain inventory (Nightmare / Fozzy)</h3>
    <p class="inventory-note">
      {n_dom} domain output folder(s). Unique routes are distinct scheme+host+path groups from
      <code>*.fozzy.inventory.json</code> when present, otherwise <code>&lt;domain&gt;.parameters.json</code>.
      &ldquo;Routes with parameters&rdquo; have at least one tracked query parameter; &ldquo;Total parameter names&rdquo;
      sums parameter slots across those routes (same name on one route counts once).
    </p>
    <div class="count-note" id="masterDomainInventoryCount"></div>
    <div class="scroll-inventory">
      <table id="masterDomainInventoryTable" class="data-table">
        <thead>
          <tr>
            <th data-type="string">Domain</th>
            <th data-type="string">Source</th>
            <th data-type="number">Unique routes</th>
            <th data-type="number">Routes w/ params</th>
            <th data-type="number">Total param names</th>
            <th data-type="number">Max params (one route)</th>
            <th data-type="number">Avg params (routes w/ params)</th>
            <th data-type="number">Unique hosts</th>
            <th data-type="string">Site folder</th>
          </tr>
        </thead>
        <tbody>{dom_tbody}</tbody>
        {dom_tfoot}
      </table>
    </div>
    {route_block}
  </section>"""
        inventory_script_html = """
      setupTable("masterDomainInventoryTable", "masterDomainInventoryCount");
      enableResizableColumns("masterDomainInventoryTable");
"""
        if isinstance(master_route_rows, list) and master_route_rows:
            inventory_script_html += """
      setupTable("masterRouteInventoryTable", "masterRouteInventoryCount");
      enableResizableColumns("masterRouteInventoryTable");
"""

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(doc_title)}</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 20px; background: #f6f8fb; color: #1f2937; }}
    h2 {{ margin: 0 0 12px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d1d5db; margin-bottom: 18px; table-layout: fixed; }}
    th, td {{ text-align: left; border: 1px solid #d1d5db; padding: 6px 8px; font-size: 12px; vertical-align: top; }}
    th {{ background: #eef2f7; top: 0; cursor: pointer; min-width: 80px; max-width: 280px; position: sticky; }}
    tr.filters th {{ top: 29px; background: #f8fafc; cursor: default; }}
    tr.filters input {{ width: 100%; box-sizing: border-box; padding: 4px 6px; border: 1px solid #cbd5e1; border-radius: 4px; font-size: 11px; }}
    .clip-cell {{ display: block; max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; user-select: text; }}
    .file-link {{ color: #1d4ed8; text-decoration: underline; }}
    th.resizable {{ position: sticky; }}
    .resize-handle {{ position: absolute; top: 0; right: -2px; width: 6px; height: 100%; cursor: col-resize; user-select: none; }}
    .scroll {{ max-height: 640px; overflow: auto; border: 1px solid #d1d5db; }}
    .count-note {{ font-size: 12px; color: #6b7280; margin-bottom: 8px; }}
    .size-diff-pos {{ color: #15803d; font-weight: 600; }}
    .size-diff-neg {{ color: #b91c1c; font-weight: 600; }}
    .size-diff-zero {{ color: #374151; }}
    .modal-backdrop {{ display: none; position: fixed; inset: 0; background: rgba(0, 0, 0, 0.45); z-index: 1000; }}
    .modal {{ display: none; position: fixed; left: 50%; top: 50%; transform: translate(-50%, -50%); width: min(1400px, 96vw); height: min(85vh, 900px); background: #fff; border: 1px solid #d1d5db; border-radius: 8px; z-index: 1001; box-shadow: 0 10px 30px rgba(0,0,0,0.25); }}
    .modal-header {{ display: flex; justify-content: space-between; align-items: center; padding: 10px 12px; border-bottom: 1px solid #e5e7eb; }}
    .modal-title {{ font-size: 14px; font-weight: 600; }}
    .modal-close {{ border: 1px solid #cbd5e1; background: #fff; border-radius: 4px; padding: 4px 8px; cursor: pointer; }}
    .modal-body {{ padding: 10px; overflow: auto; height: calc(100% - 48px); }}
    #modalContent pre {{ margin: 0; white-space: pre-wrap; word-break: break-word; font-family: Consolas, 'Courier New', monospace; font-size: 12px; }}
    .modal-diff-root .diff-sbs-wrap {{ overflow: auto; max-height: min(58vh, 560px); border: 1px solid #e5e7eb; border-radius: 6px; background: #fafafa; }}
    .modal-diff-root table.diff {{ width: 100%; border-collapse: collapse; font-family: Consolas, 'Courier New', monospace; font-size: 11px; table-layout: fixed; }}
    .modal-diff-root table.diff td {{ padding: 4px 8px; vertical-align: top; border: 1px solid #e5e7eb; white-space: pre-wrap; word-break: break-word; }}
    .modal-diff-root table.diff td.diff_header {{ background: #e5e7eb; color: #374151; font-weight: 600; }}
    .modal-diff-root .diff_add {{ background: #d1fae5; }}
    .modal-diff-root .diff_chg {{ background: #fef3c7; }}
    .modal-diff-root .diff_sub {{ background: #fee2e2; }}
    .modal-diff-root .diff_next {{ background: #f9fafb; color: #6b7280; }}
    .modal-diff-root .diff-empty-note {{ margin: 0; font-size: 13px; color: #6b7280; }}
    .modal-diff-root .diff-unified-raw {{ margin-top: 12px; }}
    .modal-diff-root .diff-unified-raw summary {{ cursor: pointer; font-size: 12px; color: #374151; }}
    .modal-diff-root .diff-unified-raw pre {{ margin-top: 8px; font-size: 11px; max-height: 240px; overflow: auto; }}
    .modal-diff-root .diff-viewer-toolbar {{ display: flex; flex-wrap: wrap; gap: 12px 18px; align-items: center; margin-bottom: 8px; font-size: 12px; color: #374151; }}
    .modal-diff-root .diff-tb-opt {{ display: inline-flex; align-items: center; gap: 6px; cursor: pointer; user-select: none; }}
    .modal-diff-root .diff-row-hidden {{ display: none; }}
    .modal-diff-root table.diff.diff-hide-nav td.diff_next,
    .modal-diff-root table.diff.diff-hide-nav th.diff_next {{ display: none; }}
    .detail-toolbar {{ display: flex; flex-wrap: wrap; align-items: center; gap: 10px 16px; margin: 10px 0 6px; font-size: 13px; }}
    .detail-toolbar .group-by-label {{ font-weight: 600; color: #374151; }}
    .detail-toolbar select#detailGroupBy {{ min-width: 200px; padding: 5px 8px; border: 1px solid #cbd5e1; border-radius: 6px; background: #fff; }}
    .detail-toolbar-hint {{ color: #6b7280; font-size: 12px; }}
    tr.group-header-row td {{ background: #e8eeff; font-weight: 600; cursor: pointer; user-select: none; padding: 8px 10px; }}
    tr.group-header-row button.group-toggle {{ margin-right: 8px; cursor: pointer; border: 1px solid #94a3b8; background: #fff; border-radius: 4px; width: 28px; height: 24px; line-height: 1; vertical-align: middle; }}
    .inventory-section {{ margin-top: 20px; }}
    .inventory-section h3 {{ font-size: 15px; margin: 16px 0 8px; }}
    .inventory-note {{ font-size: 12px; color: #4b5563; margin: 0 0 10px; max-width: 960px; line-height: 1.45; }}
    .scroll-inventory {{ max-height: 420px; overflow: auto; border: 1px solid #d1d5db; margin-bottom: 18px; background: #fff; }}
    .inventory-section table.data-table tfoot td {{ background: #f1f5f9; font-weight: 600; }}
  </style>
</head>
<body>
  <h2>{html.escape(report_heading)}</h2>
  {inventory_sections_html}
  <div class="detail-toolbar">
    {detail_group_by_select_html}
    <span class="detail-toolbar-hint">Click a group header to collapse or expand rows.</span>
  </div>
  <div class="count-note" id="detailCount"></div>
  <div class="scroll">
    <table id="detailTable" class="data-table">
      <thead>
        {thead_row_main}
        {thead_filters}
      </thead>
      <tbody>{detail_table}</tbody>
    </table>
  </div>

  <div class="modal-backdrop" id="modalBackdrop"></div>
  <div class="modal" id="viewerModal">
    <div class="modal-header">
      <div class="modal-title" id="modalTitle">Viewer</div>
      <button class="modal-close" id="modalClose" type="button">Close</button>
    </div>
    <div class="modal-body"><div id="modalContent"></div></div>
  </div>

  <script>
    (function() {{
      const responseData = {response_data_json};
      const responseDataById = new Map(responseData.map((item) => [String(item.id), item]));
      const stateStorageKey = "fozzy-report-state:" + String(window.location.href.split("#")[0]);
      const stateNamePrefix = "fozzy-report-state:";

      function readPersistedState() {{
        try {{
          const raw = localStorage.getItem(stateStorageKey);
          if (raw) return JSON.parse(raw);
        }} catch {{
        }}
        try {{
          const name = String(window.name || "");
          if (name.startsWith(stateNamePrefix)) {{
            return JSON.parse(name.slice(stateNamePrefix.length));
          }}
        }} catch {{
        }}
        return {{}};
      }}

      function writePersistedState(nextState) {{
        const safeState = nextState && typeof nextState === "object" ? nextState : {{}};
        const encoded = JSON.stringify(safeState);
        try {{
          localStorage.setItem(stateStorageKey, encoded);
          return;
        }} catch {{
        }}
        try {{
          window.name = `${{stateNamePrefix}}${{encoded}}`;
        }} catch {{
        }}
      }}

      function tableRawRowText(row) {{
        const base = Array.from(row.cells).map((cell) => (cell.dataset.raw || cell.textContent || "").toLowerCase()).join(" ");
        const extra = String(row.dataset.searchExtra || "").toLowerCase();
        return `${{base}} ${{extra}}`;
      }}

      function matchesFilter(text, filterValue) {{
        const rawText = String(text || "").toLowerCase();
        const query = String(filterValue || "").trim().toLowerCase();
        if (!query) return true;
        const tokens = query.split(/\\s+/).filter(Boolean);
        for (const token of tokens) {{
          if (token.startsWith("!")) {{
            const neg = token.slice(1);
            if (neg && rawText.includes(neg)) return false;
          }} else if (!rawText.includes(token)) {{
            return false;
          }}
        }}
        return true;
      }}

      function setupTable(tableId, countId) {{
        const table = document.getElementById(tableId);
        const count = document.getElementById(countId);
        if (!table) return;
        const tbody = table.querySelector("tbody");
        const headers = Array.from(table.querySelectorAll("thead tr:first-child th"));
        const filterInputs = Array.from(table.querySelectorAll("thead tr.filters input"));
        let sortIndex = -1;
        let sortAsc = true;

        function saveState() {{
          const state = readPersistedState();
          state.sortIndex = sortIndex;
          state.sortAsc = sortAsc;
          state.filters = filterInputs.map((input) => String(input.value || ""));
          const scrollEl = table.closest(".scroll");
          state.scrollTop = scrollEl ? scrollEl.scrollTop : 0;
          writePersistedState(state);
        }}

        function apply() {{
          const columnFilters = {{}};
          filterInputs.forEach((input) => {{
            const idx = Number(input.dataset.col || "-1");
            if (idx >= 0) columnFilters[idx] = (input.value || "");
          }});

          const rows = Array.from(tbody.querySelectorAll("tr"));
          let visible = 0;
          rows.forEach((row) => {{
            let show = true;
            const rowText = tableRawRowText(row);
            if (show) {{
              for (const [idxStr, value] of Object.entries(columnFilters)) {{
                if (!value) continue;
                const idx = Number(idxStr);
                const cell = row.cells[idx];
                const cellRaw = ((cell && cell.dataset.raw) || (cell && cell.textContent) || "").toLowerCase();
                if (!matchesFilter(cellRaw + " " + rowText, value)) {{
                  show = false;
                  break;
                }}
              }}
            }}
            row.style.display = show ? "" : "none";
            if (show) visible += 1;
          }});
          if (count) count.textContent = `Visible rows: ${{visible}}`;
          saveState();
        }}

        function sortBy(index) {{
          const th = headers[index];
          const isNumber = (th.dataset.type || "string") === "number";
          if (sortIndex === index) sortAsc = !sortAsc; else {{ sortIndex = index; sortAsc = true; }}
          const rows = Array.from(tbody.querySelectorAll("tr"));
          rows.sort((a, b) => {{
            const av = (a.cells[index] && (a.cells[index].dataset.raw || a.cells[index].textContent) || "").trim();
            const bv = (b.cells[index] && (b.cells[index].dataset.raw || b.cells[index].textContent) || "").trim();
            let cmp = 0;
            if (isNumber) {{
              const an = Number(av);
              const bn = Number(bv);
              cmp = (isNaN(an) ? 0 : an) - (isNaN(bn) ? 0 : bn);
            }} else {{
              cmp = av.localeCompare(bv);
            }}
            return sortAsc ? cmp : -cmp;
          }});
          rows.forEach((row) => tbody.appendChild(row));
          apply();
          saveState();
        }}

        headers.forEach((header, index) => {{
          header.addEventListener("click", () => sortBy(index));
        }});
        filterInputs.forEach((input) => input.addEventListener("input", apply));
        window.addEventListener("beforeunload", saveState);
        window.addEventListener("pagehide", saveState);

        const restored = readPersistedState();
        if (Array.isArray(restored.filters)) {{
          filterInputs.forEach((input, index) => {{
            if (index < restored.filters.length) {{
              input.value = String(restored.filters[index] || "");
            }}
          }});
        }}
        if (typeof restored.sortIndex === "number" && restored.sortIndex >= 0 && restored.sortIndex < headers.length) {{
          sortIndex = restored.sortIndex;
          sortAsc = typeof restored.sortAsc === "boolean" ? restored.sortAsc : true;
          const rows = Array.from(tbody.querySelectorAll("tr"));
          rows.sort((a, b) => {{
            const av = (a.cells[sortIndex] && (a.cells[sortIndex].dataset.raw || a.cells[sortIndex].textContent) || "").trim();
            const bv = (b.cells[sortIndex] && (b.cells[sortIndex].dataset.raw || b.cells[sortIndex].textContent) || "").trim();
            const isNumber = (headers[sortIndex].dataset.type || "string") === "number";
            let cmp = 0;
            if (isNumber) {{
              const an = Number(av);
              const bn = Number(bv);
              cmp = (isNaN(an) ? 0 : an) - (isNaN(bn) ? 0 : bn);
            }} else {{
              cmp = av.localeCompare(bv);
            }}
            return sortAsc ? cmp : -cmp;
          }});
          rows.forEach((row) => tbody.appendChild(row));
        }}
        apply();
        const scrollEl = table.closest(".scroll");
        if (typeof restored.scrollTop === "number" && scrollEl) {{
          scrollEl.scrollTop = restored.scrollTop;
          scrollEl.addEventListener("scroll", saveState, {{ passive: true }});
        }}
      }}

      function enableResizableColumns(tableId) {{
        const table = document.getElementById(tableId);
        if (!table) return;
        const headers = Array.from(table.querySelectorAll("thead tr:first-child th"));
        const restored = readPersistedState();
        const restoredWidths = Array.isArray(restored.columnWidths) ? restored.columnWidths : [];
        headers.forEach((header, index) => {{
          const restoredWidth = Number(restoredWidths[index]);
          if (restoredWidth > 0) {{
            header.style.width = `${{restoredWidth}}px`;
            header.style.maxWidth = `${{restoredWidth}}px`;
            table.querySelectorAll(`tbody tr td:nth-child(${{index + 1}}) .clip-cell`).forEach((node) => {{
              node.style.maxWidth = `${{restoredWidth}}px`;
            }});
          }}
        }});

        function saveColumnWidths() {{
          const state = readPersistedState();
          state.columnWidths = headers.map((header) => {{
            const width = Number.parseFloat(header.style.width || "0");
            return Number.isFinite(width) ? width : 0;
          }});
          writePersistedState(state);
        }}

        headers.forEach((header, index) => {{
          header.classList.add("resizable");
          if (header.querySelector(".resize-handle")) return;
          const handle = document.createElement("span");
          handle.className = "resize-handle";
          header.appendChild(handle);
          let startX = 0;
          let startWidth = 0;
          const onMove = (event) => {{
            const width = Math.max(80, startWidth + (event.clientX - startX));
            header.style.width = `${{width}}px`;
            header.style.maxWidth = `${{width}}px`;
            table.querySelectorAll(`tbody tr td:nth-child(${{index + 1}}) .clip-cell`).forEach((node) => {{
              node.style.maxWidth = `${{width}}px`;
            }});
          }};
          const onUp = () => {{
            window.removeEventListener("mousemove", onMove);
            window.removeEventListener("mouseup", onUp);
            saveColumnWidths();
          }};
          handle.addEventListener("mousedown", (event) => {{
            event.preventDefault();
            event.stopPropagation();
            startX = event.clientX;
            startWidth = header.getBoundingClientRect().width;
            window.addEventListener("mousemove", onMove);
            window.addEventListener("mouseup", onUp);
          }});
        }});
      }}

      const modalBackdrop = document.getElementById("modalBackdrop");
      const viewerModal = document.getElementById("viewerModal");
      const modalTitle = document.getElementById("modalTitle");
      const modalContent = document.getElementById("modalContent");
      const modalClose = document.getElementById("modalClose");
      const diffViewerOptsKey = "fozzy-diff-viewer-options";

      function loadDiffViewerOptions() {{
        try {{
          const raw = localStorage.getItem(diffViewerOptsKey);
          if (raw) return JSON.parse(raw);
        }} catch {{
        }}
        return {{}};
      }}

      function saveDiffViewerOptions(obj) {{
        try {{
          localStorage.setItem(diffViewerOptsKey, JSON.stringify(obj));
        }} catch {{
        }}
      }}

      function setupDiffViewerToolbar(rootEl, wrapEl) {{
        const table = wrapEl.querySelector("table.diff");
        if (!table) return;
        const saved = loadDiffViewerOptions();
        const bar = document.createElement("div");
        bar.className = "diff-viewer-toolbar";
        bar.setAttribute("role", "toolbar");
        function addOpt(key, label, inputId) {{
          const lab = document.createElement("label");
          lab.className = "diff-tb-opt";
          const inp = document.createElement("input");
          inp.type = "checkbox";
          inp.id = inputId;
          inp.dataset.diffToggle = key;
          inp.checked = Boolean(saved[key]);
          lab.appendChild(inp);
          lab.appendChild(document.createTextNode(" " + label));
          bar.appendChild(lab);
        }}
        addOpt("hideUnchanged", "Hide unchanged lines", "diffOptHideUnchanged");
        addOpt("hideNav", "Hide jump link columns", "diffOptHideNav");
        rootEl.insertBefore(bar, wrapEl);

        function apply() {{
          const hideU = Boolean(bar.querySelector('[data-diff-toggle="hideUnchanged"]')?.checked);
          const hideN = Boolean(bar.querySelector('[data-diff-toggle="hideNav"]')?.checked);
          saveDiffViewerOptions({{ hideUnchanged: hideU, hideNav: hideN }});
          table.querySelectorAll("tbody tr").forEach((tr) => {{
            const hasChange = tr.querySelector(".diff_sub, .diff_add, .diff_chg");
            tr.classList.toggle("diff-row-hidden", hideU && !hasChange);
          }});
          table.classList.toggle("diff-hide-nav", hideN);
        }}

        bar.addEventListener("change", (ev) => {{
          if (ev.target && ev.target.matches && ev.target.matches("input[type=checkbox]")) apply();
        }});
        apply();
      }}

      function openModal(title, textContent, options) {{
        modalTitle.textContent = title;
        options = options || {{}};
        modalContent.className = "";
        modalContent.replaceChildren();
        if (options.htmlDiff) {{
          modalContent.className = "modal-diff-root";
          const wrap = document.createElement("div");
          wrap.className = "diff-sbs-wrap";
          wrap.innerHTML = options.htmlDiff;
          modalContent.appendChild(wrap);
          setupDiffViewerToolbar(modalContent, wrap);
          const unified = String(options.unifiedText || "").trim();
          if (unified) {{
            const details = document.createElement("details");
            details.className = "diff-unified-raw";
            const summary = document.createElement("summary");
            summary.textContent = "Unified diff (text)";
            const pre = document.createElement("pre");
            pre.textContent = unified;
            details.appendChild(summary);
            details.appendChild(pre);
            modalContent.appendChild(details);
          }}
        }} else {{
          const pre = document.createElement("pre");
          pre.textContent = textContent || "";
          modalContent.appendChild(pre);
        }}
        modalBackdrop.style.display = "block";
        viewerModal.style.display = "block";
      }}

      function closeModal() {{
        modalBackdrop.style.display = "none";
        viewerModal.style.display = "none";
      }}

      modalClose.addEventListener("click", closeModal);
      modalBackdrop.addEventListener("click", closeModal);

      document.addEventListener("click", (event) => {{
        const baselineLink = event.target.closest(".resp-link");
        if (baselineLink) {{
          event.preventDefault();
          const id = String(baselineLink.dataset.id || "");
          const kind = String(baselineLink.dataset.kind || "");
          const record = responseDataById.get(id);
          if (!record) return;
          if (kind === "baseline") {{
            openModal("Baseline response", JSON.stringify(record.baseline || {{}}, null, 2));
          }} else {{
            openModal("Anomaly response", JSON.stringify(record.anomaly || {{}}, null, 2));
          }}
          return;
        }}

        const diffLink = event.target.closest(".tool-diff");
        if (diffLink) {{
          event.preventDefault();
          const id = String(diffLink.dataset.id || "");
          const record = responseDataById.get(id);
          if (!record) return;
          const diffText = String(record.diff || "").trim() || "No textual diff available.";
          const sbs = String(record.diff_side_by_side_html || "").trim();
          if (sbs) {{
            openModal("Baseline vs anomaly (body)", null, {{ htmlDiff: sbs, unifiedText: diffText }});
          }} else {{
            openModal("Baseline vs anomaly diff", diffText);
          }}
        }}
      }});

      setupTable("detailTable", "detailCount");
      enableResizableColumns("detailTable");
    }})();
  </script>
</body>
</html>"""


def _sleep_with_stop(seconds: float, stop_event: threading.Event | None) -> None:
    """Sleep up to ``seconds`` but return early if ``stop_event`` is set (Ctrl+C / cooperative shutdown)."""
    if seconds <= 0:
        return
    if stop_event is None:
        time.sleep(seconds)
        return
    deadline = time.monotonic() + float(seconds)
    while True:
        if stop_event.is_set():
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(0.2, remaining))


def fuzz_group(
    group: RouteGroup,
    permutations: list[tuple[str, ...]],
    timeout: float,
    delay: float,
    quick_fuzz_values: list[str],
    results_dir: Path,
    dry_run: bool,
    dry_run_plan_lines: list[dict[str, Any]] | None = None,
    progress_callback: Callable[[str], None] | None = None,
    live_report_callback: Callable[[], None] | None = None,
    resume_requested: dict[str, Any] | None = None,
    mark_requested_callback: Callable[[dict[str, Any]], None] | None = None,
    stop_event: threading.Event | None = None,
    resume_access_lock: threading.RLock | None = None,
    allowed_request_keys: set[str] | None = None,
) -> dict[str, Any]:
    total_requests = count_live_requests_for_group_with_allowlist(
        group, permutations, quick_fuzz_values, allowed_request_keys if not dry_run else None
    )
    summary = {
        "host": group.host,
        "path": group.path,
        "permutation_count": len(permutations),
        "total_requests": total_requests,
        "baseline_requests": 0,
        "fuzz_requests": 0,
        "anomalies": 0,
        "reflections": 0,
        "skipped_requests": 0,
        "skipped_by_endpoint_cap": 0,
        "anomaly_entries": [],
        "reflection_entries": [],
        "interrupted_by_user": False,
    }
    issued_requests = 0
    last_progress_at = 0.0

    def _stop_requested() -> bool:
        return stop_event is not None and stop_event.is_set()

    if progress_callback:
        progress_callback(
            f"[{group.host}{group.path}] execution started (planned_requests={total_requests})"
        )
    try:
        for permutation in permutations:
            if _stop_requested():
                summary["interrupted_by_user"] = True
                break
            if not permutation:
                continue
            baseline_values = {name: baseline_seed_value_for(group.params[name]) for name in permutation}
            baseline_url = build_url(group.scheme, group.host, group.path, baseline_values)
            baseline_request_key = make_request_key(
                phase="fuzz_baseline",
                host=group.host,
                path=group.path,
                permutation=permutation,
                mutated_parameter=None,
                mutated_value=None,
                url=baseline_url,
            )
            baseline_ref = {
                **empty_expected_response(),
                "source": "fuzz_baseline",
                "note": "Populate from the matching baseline request response when executing this plan.",
            }
            if dry_run:
                queue_dry_run_request(
                    plan_lines=dry_run_plan_lines,
                    phase="fuzz_baseline",
                    url=baseline_url,
                    timeout=timeout,
                    host=group.host,
                    path=group.path,
                    permutation=permutation,
                    mutated_parameter=None,
                    mutated_value=None,
                    expected_baseline_response=baseline_ref,
                )
                summary["baseline_requests"] += 1
                if live_report_callback:
                    try:
                        live_report_callback()
                    except Exception:
                        pass
                if progress_callback and (summary["baseline_requests"] + summary["fuzz_requests"]) % 100 == 0:
                    progress_callback(
                        f"[{group.host}{group.path}] progress baseline={summary['baseline_requests']} "
                        f"fuzz={summary['fuzz_requests']} anomalies={summary['anomalies']}"
                    )
            else:
                if allowed_request_keys is not None and baseline_request_key not in allowed_request_keys:
                    summary["skipped_by_endpoint_cap"] += 1 + (len(permutation) * len(quick_fuzz_values))
                    continue
                stored_baseline = _resume_get(resume_requested, baseline_request_key, resume_access_lock)
                if isinstance(stored_baseline, dict) and isinstance(stored_baseline.get("response"), dict):
                    baseline_response = stored_baseline.get("response", {})
                    summary["skipped_requests"] += 1
                else:
                    issued_requests += 1
                    if progress_callback:
                        now = time.monotonic()
                        if issued_requests == 1 or now - last_progress_at >= 5.0:
                            progress_callback(
                                f"[{group.host}{group.path}] sending request {issued_requests}/{total_requests}"
                            )
                            last_progress_at = now
                    baseline_response = request_url(baseline_url, timeout=timeout)
                    if mark_requested_callback:
                        mark_requested_callback(
                            {
                                "request_key": baseline_request_key,
                                "phase": "fuzz_baseline",
                                "host": group.host,
                                "path": group.path,
                                "permutation": list(permutation),
                                "mutated_parameter": None,
                                "mutated_value": None,
                                "url": baseline_url,
                                "response": baseline_response,
                                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                            }
                        )
                    _sleep_with_stop(delay, stop_event)
                    if _stop_requested():
                        break
                if live_report_callback:
                    try:
                        live_report_callback()
                    except Exception:
                        pass
                summary["baseline_requests"] += 1
                if progress_callback and (summary["baseline_requests"] + summary["fuzz_requests"]) % 100 == 0:
                    progress_callback(
                        f"[{group.host}{group.path}] progress baseline={summary['baseline_requests']} "
                        f"fuzz={summary['fuzz_requests']} anomalies={summary['anomalies']}"
                    )

            for name in permutation:
                if _stop_requested():
                    break
                inner_stopped = False
                for fuzz_value in quick_fuzz_values:
                    if _stop_requested():
                        inner_stopped = True
                        break
                    trial_values = dict(baseline_values)
                    trial_values[name] = fuzz_value
                    trial_url = build_url(group.scheme, group.host, group.path, trial_values)
                    trial_request_key = make_request_key(
                        phase="fuzz_mutation",
                        host=group.host,
                        path=group.path,
                        permutation=permutation,
                        mutated_parameter=name,
                        mutated_value=fuzz_value,
                        url=trial_url,
                    )
                    if dry_run:
                        queue_dry_run_request(
                            plan_lines=dry_run_plan_lines,
                            phase="fuzz_mutation",
                            url=trial_url,
                            timeout=timeout,
                            host=group.host,
                            path=group.path,
                            permutation=permutation,
                            mutated_parameter=name,
                            mutated_value=fuzz_value,
                            expected_baseline_response=baseline_ref,
                        )
                        summary["fuzz_requests"] += 1
                        if live_report_callback:
                            try:
                                live_report_callback()
                            except Exception:
                                pass
                        if progress_callback and (summary["baseline_requests"] + summary["fuzz_requests"]) % 100 == 0:
                            progress_callback(
                                f"[{group.host}{group.path}] progress baseline={summary['baseline_requests']} "
                                f"fuzz={summary['fuzz_requests']} anomalies={summary['anomalies']}"
                            )
                        continue
                    if allowed_request_keys is not None and trial_request_key not in allowed_request_keys:
                        summary["skipped_by_endpoint_cap"] += 1
                        continue
                    stored_trial = _resume_get(resume_requested, trial_request_key, resume_access_lock)
                    if isinstance(stored_trial, dict) and isinstance(stored_trial.get("response"), dict):
                        trial_response = stored_trial.get("response", {})
                        summary["skipped_requests"] += 1
                    else:
                        issued_requests += 1
                        if progress_callback:
                            now = time.monotonic()
                            if issued_requests == 1 or now - last_progress_at >= 5.0:
                                progress_callback(
                                    f"[{group.host}{group.path}] sending request {issued_requests}/{total_requests}"
                                )
                                last_progress_at = now
                        trial_response = request_url(trial_url, timeout=timeout)
                        if mark_requested_callback:
                            mark_requested_callback(
                                {
                                    "request_key": trial_request_key,
                                    "phase": "fuzz_mutation",
                                    "host": group.host,
                                    "path": group.path,
                                    "permutation": list(permutation),
                                    "mutated_parameter": name,
                                    "mutated_value": fuzz_value,
                                    "url": trial_url,
                                    "response": trial_response,
                                    "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                                }
                            )
                        _sleep_with_stop(delay, stop_event)
                        if _stop_requested():
                            inner_stopped = True
                            break
                    summary["fuzz_requests"] += 1
                    if live_report_callback:
                        try:
                            live_report_callback()
                        except Exception:
                            pass
                    if progress_callback and (summary["baseline_requests"] + summary["fuzz_requests"]) % 100 == 0:
                        progress_callback(
                            f"[{group.host}{group.path}] progress baseline={summary['baseline_requests']} "
                            f"fuzz={summary['fuzz_requests']} anomalies={summary['anomalies']}"
                        )

                    if response_differs(baseline_response, trial_response):
                        payload = {
                            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                            "host": group.host,
                            "path": group.path,
                            "permutation": list(permutation),
                            "mutated_parameter": name,
                            "mutated_value": fuzz_value,
                            "requested_url": trial_url,
                            "baseline": baseline_response,
                            "anomaly": trial_response,
                        }
                        anomaly_path = results_dir / anomaly_file_name(group.host, group.path, permutation, name, fuzz_value)
                        anomaly_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
                        summary["anomalies"] += 1
                        summary["anomaly_entries"].append(
                            {
                                "result_type": "anomaly",
                                "result_file": str(anomaly_path),
                                "url": trial_url,
                                "host": group.host,
                                "path": group.path,
                                "mutated_parameter": name,
                                "mutated_value": fuzz_value,
                                "baseline_status": int(baseline_response.get("status", 0) or 0),
                                "new_status": int(trial_response.get("status", 0) or 0),
                                "baseline_size": int(baseline_response.get("size", 0) or 0),
                                "new_size": int(trial_response.get("size", 0) or 0),
                                "size_difference": int(trial_response.get("size", 0) or 0)
                                - int(baseline_response.get("size", 0) or 0),
                            }
                        )
                        if live_report_callback:
                            try:
                                live_report_callback()
                            except Exception:
                                pass
                    body_preview = str(trial_response.get("body_preview", "") or "")
                    fuzz_needle = str(fuzz_value)
                    if (
                        body_preview
                        and fuzz_needle
                        and not is_low_signal_reflection_fuzz_value(fuzz_value)
                        and fuzz_needle in body_preview
                    ):
                        reflection_payload = {
                            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                            "host": group.host,
                            "path": group.path,
                            "permutation": list(permutation),
                            "mutated_parameter": name,
                            "mutated_value": fuzz_value,
                            "requested_url": trial_url,
                            "reflection_detected": True,
                            "baseline": baseline_response,
                            "response": trial_response,
                        }
                        reflection_path = results_dir / reflection_file_name(
                            group.host, group.path, permutation, name, fuzz_value
                        )
                        reflection_path.write_text(json.dumps(reflection_payload, indent=2, ensure_ascii=False), encoding="utf-8")
                        summary["reflections"] += 1
                        summary["reflection_entries"].append(
                            {
                                "result_type": "reflection",
                                "result_file": str(reflection_path),
                                "url": trial_url,
                                "host": group.host,
                                "path": group.path,
                                "mutated_parameter": name,
                                "mutated_value": fuzz_value,
                                "baseline_status": int(baseline_response.get("status", 0) or 0),
                                "new_status": int(trial_response.get("status", 0) or 0),
                                "baseline_size": int(baseline_response.get("size", 0) or 0),
                                "new_size": int(trial_response.get("size", 0) or 0),
                                "size_difference": int(trial_response.get("size", 0) or 0)
                                - int(baseline_response.get("size", 0) or 0),
                            }
                        )
                        if live_report_callback:
                            try:
                                live_report_callback()
                            except Exception:
                                pass
                if inner_stopped:
                    break

    except KeyboardInterrupt:
        summary["interrupted_by_user"] = True
        if stop_event is not None:
            stop_event.set()
        if progress_callback:
            progress_callback(
                f"[{group.host}{group.path}] keyboard interrupt — stopping this group with partial results "
                f"(baseline={summary['baseline_requests']} fuzz={summary['fuzz_requests']} "
                f"anomalies={summary['anomalies']} reflections={summary['reflections']} "
                f"skipped={summary['skipped_requests']})"
            )

    if progress_callback:
        tail = " (interrupted)" if summary.get("interrupted_by_user") else ""
        progress_callback(
            f"[{group.host}{group.path}] execution finished{tail} "
            f"(baseline={summary['baseline_requests']} fuzz={summary['fuzz_requests']} "
            f"anomalies={summary['anomalies']} reflections={summary['reflections']} "
            f"skipped={summary['skipped_requests']})"
        )
    return summary


def resolve_fuzz_worker_settings(cfg: dict[str, Any]) -> tuple[int, int, int]:
    max_bg = max(1, int(cfg.get("max_background_workers", 8)))
    max_dom = max(1, int(cfg.get("max_workers_per_domain", 8)))
    max_sub = max(1, int(cfg.get("max_workers_per_subdomain", 1)))
    if max_bg <= 1:
        max_dom = 1
        max_sub = 1
    return max_bg, max_dom, max_sub


def resolve_scan_root_for_incremental(raw: str) -> Path:
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (FOZZY_BASE_DIR / p).resolve()
    else:
        p = p.resolve()
    return p


def nightmare_output_root_from_parameters(parameters_path: Path) -> Path | None:
    """If parameters live at ``output/<domain>/<domain>.parameters.json``, return the ``output`` directory."""
    name = parameters_path.name
    if not name.endswith(".parameters.json"):
        return None
    domain_from_file = name[: -len(".parameters.json")]
    parent = parameters_path.parent
    if parent.name != domain_from_file:
        return None
    grand = parent.parent
    return grand.resolve() if grand.is_dir() else None


def folder_tree_max_mtime_ns(root: Path) -> int:
    max_ns = 0
    if not root.is_dir():
        return 0
    try:
        for path in root.rglob("*"):
            try:
                if path.is_file():
                    max_ns = max(max_ns, path.stat().st_mtime_ns)
            except OSError:
                continue
    except OSError:
        pass
    return max_ns


def load_incremental_state(path: Path) -> dict[str, Any]:
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


def save_incremental_state(path: Path, state: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def iter_domain_parameter_paths(scan_root: Path) -> list[tuple[Path, Path]]:
    """``(domain_dir, parameters_path)`` for each ``<domain>/<domain>.parameters.json`` under ``scan_root``."""
    out: list[tuple[Path, Path]] = []
    if not scan_root.is_dir():
        return out
    for domain_dir in sorted(scan_root.iterdir(), key=lambda p: p.name.lower()):
        if not domain_dir.is_dir() or domain_dir.name.startswith("."):
            continue
        params = domain_dir / f"{domain_dir.name}.parameters.json"
        if params.is_file():
            out.append((domain_dir.resolve(), params.resolve()))
    return out


def domain_folder_is_dirty_for_incremental(domain_dir: Path, state: dict[str, Any]) -> bool:
    block = state.get("domains", {}).get(domain_dir.name)
    if not isinstance(block, dict):
        block = {}
    current = folder_tree_max_mtime_ns(domain_dir)
    last = int(block.get("folder_max_mtime_ns", 0))
    return current > last


def run_fozzy_for_parameters(
    parameters_path: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    global _ACTIVE_FOZZY_CONFIG
    if not parameters_path.exists():
        raise FileNotFoundError(f"Parameters file not found: {parameters_path}")

    cfg = build_effective_fozzy_config(args)
    _ACTIVE_FOZZY_CONFIG = cfg

    cfg_display_path = Path(args.fozzy_config).expanduser()
    if not cfg_display_path.is_absolute():
        cfg_display_path = (FOZZY_BASE_DIR / cfg_display_path).resolve()
    else:
        cfg_display_path = cfg_display_path.resolve()

    parameters_path = parameters_path.resolve()

    payload = read_json(parameters_path)
    root_domain = str(payload.get("root_domain", parameters_path.stem)).strip().lower() or parameters_path.stem
    max_background_workers, max_workers_per_domain, max_workers_per_subdomain = resolve_fuzz_worker_settings(cfg)
    quick_fuzz_values = load_quick_fuzz_values(cfg["quick_fuzz_list"])

    out_raw = cfg.get("output_dir")
    if out_raw:
        output_dir = Path(str(out_raw)).expanduser().resolve()
    else:
        output_dir = (parameters_path.parent / "fozzy-output" / root_domain).resolve()
    results_dir = output_dir / "results"
    ensure_directory(output_dir)
    ensure_directory(results_dir)

    groups = load_route_groups(payload)
    if not groups:
        print("No parameterized routes were found in the input file.")
        return {"interrupted": False, "cancelled_by_user": False}

    print(f"Loaded {len(groups)} host/path groups from: {parameters_path}")
    print(f"Fozzy config: {cfg_display_path}")
    print(f"Output directory: {output_dir}")
    print(
        f"Fuzz parallelism: max_background_workers={max_background_workers}, "
        f"max_workers_per_domain={max_workers_per_domain} (concurrent jobs per eTLD+1), "
        f"max_workers_per_subdomain={max_workers_per_subdomain} (concurrent jobs per exact hostname)"
    )
    print("")

    host_listing: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for group in groups:
        host_listing[group.host].append((group.path, len(group.params)))

    print("Host -> path parameter counts:")
    for host in sorted(host_listing.keys()):
        print(f"- {host}")
        for path, count in sorted(host_listing[host], key=lambda item: item[0]):
            print(f"  {path} ({count} parameters)")
    print("")

    permutations_lines: list[str] = []
    baseline_permutation_urls: list[str] = []
    run_summary: list[dict[str, Any]] = []
    planned_summary: list[dict[str, Any]] = []
    route_inventory: list[dict[str, Any]] = []
    dry_run_plan_lines: list[dict[str, Any]] = []
    group_jobs: list[dict[str, Any]] = []
    interrupted = False
    interrupted_group: str | None = None
    cancelled_by_user = False

    for group in groups:
        try:
            required, optional = infer_required_optional_from_observed(group)
            generated_permutations = generate_permutations(
                observed_sets=group.observed_param_sets,
                required=set(group.params.keys()),
                optional=set(),
                max_permutations=int(cfg["max_permutations"]),
            )
            permutations = select_max_parameter_permutations(generated_permutations)

            estimated_requests = estimate_group_request_count(
                permutations=permutations,
                quick_fuzz_values_count=len(quick_fuzz_values),
            )
            print(
                f"[{group.host}{group.path}] observed_valid={len(group.observed_param_sets)} "
                f"required={len(required)} optional={len(optional)} "
                f"permutations_generated={len(generated_permutations)} permutations_used={len(permutations)} "
                f"estimated_requests={estimated_requests}"
            )

            for permutation in permutations:
                if not permutation:
                    continue
                params_meta = [group.params[name] for name in permutation]
                permutations_lines.append(build_placeholder_url(group.scheme, group.host, group.path, params_meta))
                baseline_values = {name: baseline_seed_value_for(group.params[name]) for name in permutation}
                baseline_permutation_urls.append(build_url(group.scheme, group.host, group.path, baseline_values))

            route_inventory.append(
                {
                    "host": group.host,
                    "path": group.path,
                    "scheme": group.scheme,
                    "parameter_count": len(group.params),
                    "parameters": [
                        {"name": name, "data_type": group.params[name].data_type}
                        for name in sorted(group.params.keys())
                    ],
                    "observed_valid_parameter_sets": [sorted(list(item)) for item in sorted(group.observed_param_sets)],
                    "inferred_required_parameters": sorted(required),
                    "inferred_optional_parameters": sorted(optional),
                    "permutations_generated_count": len(generated_permutations),
                    "permutations_used_count": len(permutations),
                    "permutations": [list(item) for item in permutations],
                    "placeholder_urls": [
                        build_placeholder_url(group.scheme, group.host, group.path, [group.params[name] for name in item])
                        for item in permutations
                        if item
                    ],
                    "baseline_urls": [
                        build_url(
                            group.scheme,
                            group.host,
                            group.path,
                            {name: baseline_seed_value_for(group.params[name]) for name in item},
                        )
                        for item in permutations
                        if item
                    ],
                }
            )
            group_jobs.append(
                {
                    "group": group,
                    "permutations": permutations,
                }
            )
        except KeyboardInterrupt:
            interrupted = True
            interrupted_group = f"{group.host}{group.path}"
            print(f"Interrupted by user during group: {interrupted_group}. Saving partial results...")
            break

    for job in group_jobs:
        plan_summary = fuzz_group(
            group=job["group"],
            permutations=job["permutations"],
            timeout=float(cfg["timeout_seconds"]),
            delay=float(cfg["delay_seconds"]),
            quick_fuzz_values=quick_fuzz_values,
            results_dir=results_dir,
            dry_run=True,
            dry_run_plan_lines=dry_run_plan_lines,
            progress_callback=None,
            live_report_callback=None,
        )
        planned_summary.append(plan_summary)
        if plan_summary.get("interrupted_by_user"):
            interrupted = True
            interrupted_group = interrupted_group or "dry_run_planning"
            print("\nInterrupted during dry-run request planning. Saving partial results...", flush=True)
            break

    dry_run_plan_path = output_dir / f"{root_domain}.fozzy.requests.jsonl"
    with dry_run_plan_path.open("w", encoding="utf-8", newline="\n") as handle:
        for line in dry_run_plan_lines:
            handle.write(json.dumps(line, ensure_ascii=False))
            handle.write("\n")
    endpoint_counts = build_endpoint_request_counts(dry_run_plan_lines)
    endpoint_counts_json_path = output_dir / f"{root_domain}.fozzy.endpoint_request_counts.json"
    endpoint_counts_json_path.write_text(json.dumps(endpoint_counts, indent=2, ensure_ascii=False), encoding="utf-8")
    endpoint_counts_txt_path = output_dir / f"{root_domain}.fozzy.endpoint_request_counts.txt"
    endpoint_counts_txt_path.write_text(
        "\n".join(
            f"{item['request_count']}\t{item['endpoint']}"
            for item in endpoint_counts
        )
        + ("\n" if endpoint_counts else ""),
        encoding="utf-8",
    )

    planned_requests = len(dry_run_plan_lines)
    planned_delay = float(cfg["delay_seconds"])
    estimated_runtime_seconds = planned_requests * max(0.0, planned_delay)
    print(
        f"Request plan ready: {dry_run_plan_path}\n"
        f"Planned requests: {planned_requests}\n"
        f"Configured delay: {planned_delay:.2f}s between requests\n"
        f"Estimated minimum runtime at that delay: {format_duration(estimated_runtime_seconds)}"
    )
    print("Requests by endpoint (most to least):")
    for item in endpoint_counts:
        print(f"  {item['request_count']:>6}  {item['endpoint']}")
    print(f"Endpoint request counts JSON: {endpoint_counts_json_path}")
    print(f"Endpoint request counts TXT: {endpoint_counts_txt_path}")

    max_req = cfg.get("max_requests_per_endpoint")
    selected_group_jobs = list(group_jobs)
    selected_request_cap: int | None
    if not cfg["dry_run"] and max_req is not None:
        selected_request_cap = int(max_req)
        selected_plan_lines = cap_plan_lines_by_endpoint(dry_run_plan_lines, selected_request_cap)
        selected_planned_requests = len(selected_plan_lines)
        selected_endpoint_count = len(endpoint_counts)
        sel_est = selected_planned_requests * max(0.0, planned_delay)
        print(
            f"Live cap: at most {selected_request_cap} HTTP requests per endpoint "
            f"(full baseline+mutation blocks only); live planned requests {selected_planned_requests} "
            f"of {planned_requests} listed in the JSONL.\n"
            f"Estimated minimum runtime at configured delay: {format_duration(sel_est)}",
            flush=True,
        )
    else:
        selected_request_cap = None
        selected_plan_lines = list(dry_run_plan_lines)
        selected_planned_requests = len(selected_plan_lines)
        selected_endpoint_count = len(endpoint_counts)
        if not cfg["dry_run"]:
            print(
                f"Live: all {planned_requests} planned requests (no per-endpoint cap); "
                f"throttle: {planned_delay:.2f}s between requests.",
                flush=True,
            )

    allowed_request_keys_live: set[str] | None = None
    resume_state_path = output_dir / f"{root_domain}.fozzy.resume_state.json"
    for line in selected_plan_lines:
        if "request_key" not in line:
            line["request_key"] = request_key_from_plan_line(line)

    if not cfg["dry_run"] and selected_request_cap is not None:
        allowed_request_keys_live = {
            str(line.get("request_key", ""))
            for line in selected_plan_lines
            if str(line.get("request_key", ""))
        }

    resume_state = load_resume_state(resume_state_path)
    existing_requested = resume_state.get("requested", {}) if isinstance(resume_state.get("requested"), dict) else {}
    selected_keys = {str(line.get("request_key", "")) for line in selected_plan_lines if str(line.get("request_key", ""))}
    requested_for_selection: dict[str, Any] = {
        key: value for key, value in existing_requested.items() if key in selected_keys
    }

    queued_for_selection = [
        {
            "request_key": str(line.get("request_key", "")),
            "phase": str(line.get("phase", "")),
            "host": str(line.get("host", "")),
            "path": str(line.get("path", "")),
            "permutation": list(line.get("permutation", [])) if isinstance(line.get("permutation"), list) else [],
            "mutated_parameter": str(line.get("mutated_parameter", "") or ""),
            "mutated_value": str(line.get("mutated_value", "") or ""),
            "url": str(line.get("url", "")),
            "curl": str(line.get("curl", "")),
        }
        for line in selected_plan_lines
        if str(line.get("request_key", "")) and str(line.get("request_key", "")) not in requested_for_selection
    ]
    resume_state = {
        "requested": requested_for_selection,
        "queued": queued_for_selection,
        "meta": {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "root_domain": root_domain,
            "input_parameters_file": str(parameters_path),
            "selected_request_cap": selected_request_cap,
            "selected_planned_requests": len(selected_plan_lines),
            "requested_count": len(requested_for_selection),
            "queued_count": len(queued_for_selection),
        },
    }
    save_resume_state(resume_state_path, resume_state)
    if requested_for_selection:
        print(
            f"Resume state loaded: requested={len(requested_for_selection)} queued={len(queued_for_selection)} "
            f"file={resume_state_path}"
        )

    if not cfg["dry_run"] and not interrupted:
        if not cancelled_by_user:
            last_live_report_refresh = 0.0
            requested_cache = dict(resume_state.get("requested", {}))
            resume_save_last_mono = 0.0
            resume_save_interval = 2.0

            def maybe_refresh_live_report(force: bool = False) -> None:
                nonlocal last_live_report_refresh
                now = time.monotonic()
                if not force and (now - last_live_report_refresh) < float(
                    cfg["live_report_interval_seconds"]
                ):
                    return
                write_results_summary(
                    root_domain=root_domain,
                    parameters_path=parameters_path,
                    output_dir=output_dir,
                    results_dir=results_dir,
                    interrupted=interrupted,
                    interrupted_group=interrupted_group,
                )
                last_live_report_refresh = now

            def persist_resume_state_to_disk(*, force: bool) -> None:
                """Write resume JSON; throttled unless ``force`` (avoids O(plan_lines) disk work per HTTP)."""
                nonlocal resume_save_last_mono
                now = time.monotonic()
                if not force and (now - resume_save_last_mono) < resume_save_interval:
                    return
                resume_save_last_mono = now
                pending = [
                    {
                        "request_key": str(line.get("request_key", "")),
                        "phase": str(line.get("phase", "")),
                        "host": str(line.get("host", "")),
                        "path": str(line.get("path", "")),
                        "permutation": list(line.get("permutation", []))
                        if isinstance(line.get("permutation"), list)
                        else [],
                        "mutated_parameter": str(line.get("mutated_parameter", "") or ""),
                        "mutated_value": str(line.get("mutated_value", "") or ""),
                        "url": str(line.get("url", "")),
                        "curl": str(line.get("curl", "")),
                    }
                    for line in selected_plan_lines
                    if str(line.get("request_key", "")) and str(line.get("request_key", "")) not in requested_cache
                ]
                resume_payload = {
                    "requested": requested_cache,
                    "queued": pending,
                    "meta": {
                        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                        "root_domain": root_domain,
                        "input_parameters_file": str(parameters_path),
                        "selected_request_cap": selected_request_cap,
                        "selected_planned_requests": len(selected_plan_lines),
                        "requested_count": len(requested_cache),
                        "queued_count": len(pending),
                    },
                }
                save_resume_state(resume_state_path, resume_payload)

            def mark_request_completed(entry: dict[str, Any]) -> None:
                key = str(entry.get("request_key", "")).strip()
                if not key:
                    return
                requested_cache[key] = entry
                persist_resume_state_to_disk(force=False)

            parallel_fuzz = max_background_workers > 1
            state_lock = threading.RLock()

            def safe_progress(msg: str) -> None:
                def _do_print() -> None:
                    print(msg, flush=True)

                _fozzy_with_rlock(state_lock, "parallel progress lock", _do_print)

            def mark_request_completed_safe(entry: dict[str, Any]) -> None:
                def _do_mark() -> None:
                    mark_request_completed(entry)

                _fozzy_with_rlock(state_lock, "parallel resume-save lock", _do_mark)

            def maybe_refresh_live_report_safe(force: bool = False) -> None:
                nonlocal last_live_report_refresh

                def _throttle() -> bool:
                    nonlocal last_live_report_refresh
                    now = time.monotonic()
                    if not force and (now - last_live_report_refresh) < float(
                        cfg["live_report_interval_seconds"]
                    ):
                        return False
                    last_live_report_refresh = now
                    return True

                should_write = _fozzy_with_rlock(state_lock, "parallel live-report throttle lock", _throttle)
                if not should_write:
                    return
                write_results_summary(
                    root_domain=root_domain,
                    parameters_path=parameters_path,
                    output_dir=output_dir,
                    results_dir=results_dir,
                    interrupted=interrupted,
                    interrupted_group=interrupted_group,
                )

            maybe_refresh_live_report_safe(force=True)

            if not parallel_fuzz:
                for job in selected_group_jobs:
                    group = job["group"]
                    group_summary = fuzz_group(
                        group=group,
                        permutations=job["permutations"],
                        timeout=float(cfg["timeout_seconds"]),
                        delay=float(cfg["delay_seconds"]),
                        quick_fuzz_values=quick_fuzz_values,
                        results_dir=results_dir,
                        dry_run=False,
                        dry_run_plan_lines=None,
                        progress_callback=print,
                        live_report_callback=maybe_refresh_live_report,
                        resume_requested=requested_cache,
                        mark_requested_callback=mark_request_completed,
                        resume_access_lock=None,
                        allowed_request_keys=allowed_request_keys_live,
                    )
                    run_summary.append(group_summary)
                    if group_summary.get("interrupted_by_user"):
                        interrupted = True
                        interrupted_group = f"{group.host}{group.path}"
                        print(
                            f"Interrupted by user during group: {interrupted_group}. Saving partial results...",
                            flush=True,
                        )
                        maybe_refresh_live_report(force=True)
                        break
                persist_resume_state_to_disk(force=True)
            else:
                stop_event = threading.Event()
                gate = HostDomainFuzzGate(
                    max_background_workers,
                    max_workers_per_domain,
                    max_workers_per_subdomain,
                )
                results_by_index: dict[int, dict[str, Any]] = {}

                def run_after_gate_acquired(
                    index: int,
                    job: dict[str, Any],
                    host_key: str,
                    reg: str,
                ) -> tuple[int, dict[str, Any]]:
                    """Run ``fuzz_group``; caller must have acquired the gate for ``host_key``/``reg``."""
                    grp = job["group"]
                    try:
                        if stop_event.is_set():
                            return index, {
                                "host": grp.host,
                                "path": grp.path,
                                "permutation_count": len(job["permutations"]),
                                "total_requests": estimate_group_request_count(
                                    permutations=job["permutations"],
                                    quick_fuzz_values_count=len(quick_fuzz_values),
                                ),
                                "baseline_requests": 0,
                                "fuzz_requests": 0,
                                "anomalies": 0,
                                "reflections": 0,
                                "skipped_requests": 0,
                                "skipped_by_endpoint_cap": 0,
                                "anomaly_entries": [],
                                "reflection_entries": [],
                                "cancelled_before_start": True,
                            }
                        summary = fuzz_group(
                            group=grp,
                            permutations=job["permutations"],
                            timeout=float(cfg["timeout_seconds"]),
                            delay=float(cfg["delay_seconds"]),
                            quick_fuzz_values=quick_fuzz_values,
                            results_dir=results_dir,
                            dry_run=False,
                            dry_run_plan_lines=None,
                            progress_callback=safe_progress,
                            live_report_callback=maybe_refresh_live_report_safe,
                            resume_requested=requested_cache,
                            mark_requested_callback=mark_request_completed_safe,
                            stop_event=stop_event,
                            resume_access_lock=state_lock,
                            allowed_request_keys=allowed_request_keys_live,
                        )
                        return index, summary
                    finally:
                        gate.release(host_key, reg)

                try:
                    pending_jobs: list[tuple[int, dict[str, Any]]] = list(
                        enumerate(selected_group_jobs)
                    )
                    in_flight: dict[Any, int] = {}
                    with ThreadPoolExecutor(max_workers=max_background_workers) as executor:
                        while pending_jobs or in_flight:
                            if not stop_event.is_set():
                                scan_again = True
                                while (
                                    scan_again
                                    and len(in_flight) < max_background_workers
                                    and pending_jobs
                                ):
                                    scan_again = False
                                    for pos, (idx, job) in enumerate(pending_jobs):
                                        grp = job["group"]
                                        host_key = grp.host.lower()
                                        reg = registrable_domain(host_key) or host_key
                                        if gate.try_acquire_nonblocking(host_key, reg):
                                            pending_jobs.pop(pos)
                                            fut = executor.submit(
                                                run_after_gate_acquired,
                                                idx,
                                                job,
                                                host_key,
                                                reg,
                                            )
                                            in_flight[fut] = idx
                                            scan_again = True
                                            break
                            if not in_flight:
                                if pending_jobs and not stop_event.is_set():
                                    safe_progress(
                                        "Warning: Fozzy parallel scheduler stalled "
                                        f"(pending={len(pending_jobs)} jobs, none runnable)."
                                    )
                                break
                            done, _pending_f = wait(
                                in_flight.keys(),
                                return_when=FIRST_COMPLETED,
                            )
                            for fut in done:
                                idx = in_flight.pop(fut)
                                try:
                                    i, group_summary = fut.result()
                                    results_by_index[i] = group_summary
                                    if group_summary.get("interrupted_by_user"):
                                        interrupted = True
                                        gj = selected_group_jobs[i]["group"]
                                        interrupted_group = f"{gj.host}{gj.path}"
                                        stop_event.set()
                                except KeyboardInterrupt:
                                    stop_event.set()
                                    raise
                                except Exception as exc:
                                    grp = selected_group_jobs[idx]["group"]
                                    safe_progress(f"[{grp.host}{grp.path}] worker error: {exc}")
                                    results_by_index[idx] = {
                                        "host": grp.host,
                                        "path": grp.path,
                                        "permutation_count": len(
                                            selected_group_jobs[idx]["permutations"]
                                        ),
                                        "total_requests": 0,
                                        "baseline_requests": 0,
                                        "fuzz_requests": 0,
                                        "anomalies": 0,
                                        "reflections": 0,
                                        "skipped_requests": 0,
                                        "skipped_by_endpoint_cap": 0,
                                        "anomaly_entries": [],
                                        "reflection_entries": [],
                                        "worker_error": str(exc),
                                    }
                except KeyboardInterrupt:
                    interrupted = True
                    interrupted_group = interrupted_group or "parallel_fuzz"
                    stop_event.set()
                    safe_progress("Interrupt received; stopping parallel fuzz workers...")
                    maybe_refresh_live_report_safe(force=True)

                persist_resume_state_to_disk(force=True)
                run_summary.extend(results_by_index[i] for i in sorted(results_by_index.keys()))

    if cfg["dry_run"]:
        run_summary = planned_summary

    previous_sigint_handler: Any = None
    sigint_ignored = False
    try:
        previous_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        sigint_ignored = True
    except Exception:
        previous_sigint_handler = None
        sigint_ignored = False

    permutations_txt_path = output_dir / f"{root_domain}.fozzy.permutations.txt"
    permutations_txt_path.write_text("\n".join(permutations_lines).strip() + ("\n" if permutations_lines else ""), encoding="utf-8")
    baseline_urls_txt_path = output_dir / f"{root_domain}.fozzy.baseline-urls.txt"
    baseline_urls_txt_path.write_text(
        "\n".join(baseline_permutation_urls).strip() + ("\n" if baseline_permutation_urls else ""),
        encoding="utf-8",
    )
    inventory_path = output_dir / f"{root_domain}.fozzy.inventory.json"
    inventory_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root_domain": root_domain,
        "input_parameters_file": str(parameters_path),
        "routes": route_inventory,
    }
    inventory_path.write_text(json.dumps(inventory_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    summary_path = output_dir / f"{root_domain}.fozzy.summary.json"
    summary_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root_domain": root_domain,
        "input_parameters_file": str(parameters_path),
        "dry_run": bool(cfg["dry_run"]),
        "fozzy_config_path": str(cfg_display_path),
        "max_background_workers": max_background_workers,
        "max_workers_per_domain": max_workers_per_domain,
        "max_workers_per_subdomain": max_workers_per_subdomain,
        "parallel_fuzz_enabled": max_background_workers > 1,
        "interrupted": interrupted,
        "interrupted_group": interrupted_group,
        "cancelled_by_user": cancelled_by_user,
        "max_requests_per_endpoint": cfg.get("max_requests_per_endpoint"),
        "selected_request_cap": selected_request_cap,
        "selected_planned_requests": selected_planned_requests,
        "selected_endpoint_count": selected_endpoint_count,
        "groups": run_summary,
        "totals": {
            "groups": len(run_summary),
            "baseline_requests": sum(int(item.get("baseline_requests", 0)) for item in run_summary),
            "fuzz_requests": sum(int(item.get("fuzz_requests", 0)) for item in run_summary),
            "anomalies": sum(int(item.get("anomalies", 0)) for item in run_summary),
            "reflections": sum(int(item.get("reflections", 0)) for item in run_summary),
            "skipped_by_endpoint_cap": sum(int(item.get("skipped_by_endpoint_cap", 0)) for item in run_summary),
            "placeholder_permutations": len(permutations_lines),
            "planned_requests": len(dry_run_plan_lines),
        },
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    anomaly_summary_path, anomaly_summary_html_path = write_results_summary(
        root_domain=root_domain,
        parameters_path=parameters_path,
        output_dir=output_dir,
        results_dir=results_dir,
        interrupted=interrupted,
        interrupted_group=interrupted_group,
    )

    try:
        repo_root = nightmare_output_root_from_parameters(parameters_path)
        if repo_root is not None:
            master_json, master_html, n_dom = write_master_results_summary(repo_root, prefer_nested=None)
            print(
                f"Master results (all domains under {repo_root}; {n_dom} output tree(s)): "
                f"{master_json} | {master_html}"
            )
        else:
            master_parent = output_dir.parent.resolve()
            if master_parent.is_dir():
                master_json, master_html, n_dom = write_master_results_summary(master_parent, prefer_nested=None)
                print(f"Master results summary JSON: {master_json}")
                print(f"Master results summary HTML: {master_html} ({n_dom} domain output tree(s))")
    except OSError as exc:
        print(f"Warning: could not write master results summary: {exc}", flush=True)

    if sigint_ignored:
        try:
            signal.signal(signal.SIGINT, previous_sigint_handler)
        except Exception:
            pass
    print("")
    print(f"Permutations list: {permutations_txt_path}")
    print(f"Baseline URL list: {baseline_urls_txt_path}")
    print(f"Route inventory JSON: {inventory_path}")
    print(f"Run summary JSON: {summary_path}")
    print(f"Results summary JSON: {anomaly_summary_path}")
    print(f"Results summary HTML: {anomaly_summary_html_path}")
    print(f"Request plan JSONL: {dry_run_plan_path}")
    print(f"Results directory: {results_dir}")
    print(
        "Request totals: "
        f"baseline={summary_payload['totals']['baseline_requests']} "
        f"fuzz={summary_payload['totals']['fuzz_requests']} "
        f"anomalies={summary_payload['totals']['anomalies']} "
        f"reflections={summary_payload['totals']['reflections']}"
        + (
            f" skipped_by_endpoint_cap={summary_payload['totals']['skipped_by_endpoint_cap']}"
            if summary_payload["totals"].get("skipped_by_endpoint_cap")
            else ""
        )
    )
    if interrupted:
        print("Run completed with interrupt; outputs contain partial results.")

    return {"interrupted": interrupted, "cancelled_by_user": cancelled_by_user}


def run_incremental_domains(args: argparse.Namespace) -> None:
    scan_root = resolve_scan_root_for_incremental(str(args.scan_root))
    if not scan_root.is_dir():
        raise FileNotFoundError(f"Incremental scan root not found or not a directory: {scan_root}")
    state_path = scan_root / ".fozzy_incremental_state.json"
    state = load_incremental_state(state_path)
    pairs = iter_domain_parameter_paths(scan_root)
    if not pairs:
        print(f"No domain parameters files found under {scan_root} (expected <domain>/<domain>.parameters.json).")
        master_json, master_html, n_dom = write_master_results_summary(scan_root, prefer_nested=None)
        print(f"Master results summary JSON: {master_json}")
        print(f"Master results summary HTML: {master_html} ({n_dom} domain output tree(s))")
        return

    to_run = [
        (domain_dir, params_path)
        for domain_dir, params_path in pairs
        if args.incremental_force or domain_folder_is_dirty_for_incremental(domain_dir, state)
    ]

    if not to_run:
        print(
            f"No domain folders with new or changed files since the last Fozzy run under {scan_root}.\n"
            "Use --incremental-force to run every domain, or update files under a domain folder to trigger a run."
        )
        master_json, master_html, n_dom = write_master_results_summary(scan_root, prefer_nested=None)
        print(f"Master results summary JSON: {master_json}")
        print(f"Master results summary HTML: {master_html} ({n_dom} domain output tree(s))")
        return

    print(f"Incremental Fozzy: running {len(to_run)} domain(s) under {scan_root}")

    for domain_dir, params_path in to_run:
        print(f"\n=== {domain_dir.name} ===\nParameters: {params_path}", flush=True)
        result = run_fozzy_for_parameters(params_path, args)
        if result.get("interrupted") or result.get("cancelled_by_user"):
            print(f"Stopped after {domain_dir.name}; incremental state not updated for this domain.", flush=True)
            break
        if "domains" not in state or not isinstance(state["domains"], dict):
            state["domains"] = {}
        state["domains"][domain_dir.name] = {
            "folder_max_mtime_ns": folder_tree_max_mtime_ns(domain_dir),
            "last_completed_run_utc": datetime.now(timezone.utc).isoformat(),
            "parameters_path": str(params_path),
        }
        save_incremental_state(state_path, state)

    master_json, master_html, n_dom = write_master_results_summary(scan_root, prefer_nested=None)
    print(f"\nMaster results summary JSON: {master_json}")
    print(f"Master results summary HTML: {master_html} ({n_dom} domain output tree(s))")


def main() -> None:
    args = parse_args()
    master_root: Path | None = None
    if args.write_master_report:
        master_root = Path(str(args.write_master_report)).expanduser().resolve()
    elif args.generate_master_report is not None:
        if args.generate_master_report is True:
            master_root = resolve_scan_root_for_incremental(str(args.scan_root))
        else:
            master_root = Path(str(args.generate_master_report)).expanduser().resolve()
    if master_root is not None:
        if not master_root.is_dir():
            raise FileNotFoundError(f"Master report root not found or not a directory: {master_root}")
        master_json, master_html, n_dom = write_master_results_summary(master_root, prefer_nested=None)
        print(f"Master report: aggregated {n_dom} domain output tree(s) under {master_root}")
        print(f"Master results summary JSON: {master_json}")
        print(f"Master results summary HTML: {master_html}")
        return
    if not args.parameters_file:
        run_incremental_domains(args)
        return
    run_fozzy_for_parameters(Path(args.parameters_file).resolve(), args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nFozzy interrupted by user (Ctrl+C). Partial outputs may have been saved.", flush=True)
        raise SystemExit(130)


