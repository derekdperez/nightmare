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
import atexit
import base64
import copy
import difflib
import hashlib
import html
import itertools
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable

from fozzy_app.fuzz_core import (
    ParameterMeta,
    RouteGroup,
    baseline_seed_value_for as _core_baseline_seed_value_for,
    build_fuzz_http_request as _core_build_fuzz_http_request,
    build_placeholder_request_summary as _core_build_placeholder_request_summary,
    build_placeholder_url as _core_build_placeholder_url,
    build_url as _core_build_url,
    build_urlencoded_form_body as _core_build_urlencoded_form_body,
    generic_value_for as _core_generic_value_for,
    merge_data_type as _core_merge_data_type,
    merge_request_headers as _core_merge_request_headers,
    request_body_fingerprint as _core_request_body_fingerprint,
    route_group_all_param_names as _core_route_group_all_param_names,
    route_group_param_meta as _core_route_group_param_meta,
)
from http_client import request_capped
from http_request_queue import HttpRequestQueue
from nightmare_shared.value_types import infer_observed_value_type

# Semaphore / RLock misuse or runtime issues should warn, not abort a long fuzz run.
_FOZZY_LOCK_ERRORS: tuple[type[BaseException], ...] = (RuntimeError, ValueError, AttributeError)
RESPONSE_TIME_DISCREPANCY_MULTIPLIER = 3.0
MASTER_REPORT_BODY_PREVIEW_MAX_CHARS = 240
MASTER_REPORT_DIFF_TEXT_MAX_LINES = 60
MASTER_REPORT_EXTRACTOR_ROWS_MAX = 10000

_HTTP_QUEUE_LOCK = threading.Lock()
_HTTP_QUEUE_INSTANCE: HttpRequestQueue | None = None


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


def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    """Write text atomically to avoid leaving truncated/empty reports on partial failures."""
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_name = f"{target.name}.tmp-{os.getpid()}-{threading.get_ident()}"
    temp_path = target.with_name(temp_name)
    temp_path.write_text(text, encoding=encoding)
    temp_path.replace(target)


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
        "log_file": None,
        "dry_run": False,
        "max_background_workers": 8,
        "incremental_domain_workers": 8,
        "max_workers_per_domain": 8,
        "max_workers_per_subdomain": 1,
        "max_requests_per_endpoint": None,
        "live_report_interval_seconds": 5.0,
        "body_preview_limit": 50000,
        "reflection_alert_ignore_exact": ["test"," ",""],
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
    cfg["incremental_domain_workers"] = max(
        1,
        int(cfg.get("incremental_domain_workers", cfg["max_background_workers"])),
    )
    cfg["max_workers_per_domain"] = max(1, int(cfg.get("max_workers_per_domain", 8)))
    cfg["max_workers_per_subdomain"] = max(1, int(cfg.get("max_workers_per_subdomain", 1)))
    if cfg["max_background_workers"] <= 1:
        cfg["max_workers_per_domain"] = 1
        cfg["max_workers_per_subdomain"] = 1
    # None / missing / 0 / negative: no per-endpoint cap (all planned requests; throttle via delay_seconds).
    # Positive int: optional cap on plan lines per method+host+path.
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
    lf = cfg.get("log_file")
    cfg["log_file"] = str(lf).strip() if lf not in (None, "") else None


_ACTIVE_FOZZY_CONFIG: dict[str, Any] = copy.deepcopy(default_fozzy_config())
normalize_fozzy_config(_ACTIVE_FOZZY_CONFIG)


_FOZZY_LOG_HANDLE: Any = None


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

    def fileno(self) -> int:
        return self._original.fileno()

    @property
    def encoding(self) -> str:
        return getattr(self._original, "encoding", "utf-8")


def _resolve_optional_log_file_path(raw: str | None) -> Path | None:
    text = str(raw or "").strip()
    if not text:
        return None
    p = Path(text).expanduser()
    if not p.is_absolute():
        p = (FOZZY_BASE_DIR / p).resolve()
    else:
        p = p.resolve()
    return p


def resolve_default_fozzy_log_path(args: argparse.Namespace, cfg_hint: dict[str, Any] | None = None) -> Path:
    if args.write_master_report:
        return Path(str(args.write_master_report)).expanduser().resolve() / "fozzy.master_report.log"
    if args.generate_master_report is not None:
        if args.generate_master_report is True:
            return resolve_scan_root_for_incremental(str(args.scan_root)) / "fozzy.master_report.log"
        return Path(str(args.generate_master_report)).expanduser().resolve() / "fozzy.master_report.log"
    if not args.parameters_file:
        return resolve_scan_root_for_incremental(str(args.scan_root)) / "fozzy.incremental.log"

    params_path = Path(str(args.parameters_file)).expanduser()
    if not params_path.is_absolute():
        params_path = params_path.resolve()
    else:
        params_path = params_path.resolve()
    root_domain = params_path.stem
    try:
        payload = read_json(params_path)
        root_domain = str(payload.get("root_domain", root_domain)).strip().lower() or root_domain
    except Exception:
        pass

    cfg_local = cfg_hint or {}
    out_raw = cfg_local.get("output_dir")
    if out_raw:
        output_dir = Path(str(out_raw)).expanduser().resolve()
    else:
        output_dir = (params_path.parent / "fozzy-output" / root_domain).resolve()
    return output_dir / f"{root_domain}.fozzy.log"


def install_fozzy_log_tee(log_path: Path) -> None:
    global _FOZZY_LOG_HANDLE
    ensure_directory(log_path.parent)
    handle = log_path.open("a", encoding="utf-8")
    _FOZZY_LOG_HANDLE = handle
    sys.stdout = _StreamTee(sys.stdout, handle)  # type: ignore[assignment]
    sys.stderr = _StreamTee(sys.stderr, handle)  # type: ignore[assignment]
    print(
        f"[fozzy] logging to {log_path} (pid={os.getpid()}, started_utc={datetime.now(timezone.utc).isoformat()})",
        flush=True,
    )


def _close_fozzy_log_tee() -> None:
    global _FOZZY_LOG_HANDLE
    h = _FOZZY_LOG_HANDLE
    if h is None:
        return
    _FOZZY_LOG_HANDLE = None
    try:
        h.flush()
    except Exception:
        pass
    try:
        h.close()
    except Exception:
        pass


atexit.register(_close_fozzy_log_tee)


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
            "Live run only: optional cap on HTTP requests per method+host+path (plan lines; baseline + mutations per block). "
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
        "--log-file",
        default=None,
        help=(
            "Optional log file path for this fozzy process. Relative paths resolve against the directory "
            "containing fozzy.py. If omitted, fozzy writes to a mode-specific default log path."
        ),
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
        "--incremental-domain-workers",
        type=int,
        default=None,
        help=(
            "Override fozzy.json incremental_domain_workers (concurrent domain processes in "
            "--scan-root incremental mode when no parameters file is given). "
            "Defaults to max_background_workers when unset."
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
    if args.log_file is not None:
        cfg["log_file"] = str(args.log_file).strip() or None
    if args.max_background_workers is not None:
        cfg["max_background_workers"] = int(args.max_background_workers)
    if args.incremental_domain_workers is not None:
        cfg["incremental_domain_workers"] = int(args.incremental_domain_workers)
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
    priority = active_fozzy_config().get("type_priority") or default_fozzy_config()["type_priority"]
    return _core_merge_data_type(existing, new_type, type_priority=priority)


def infer_value_type(value: str) -> str:
    return infer_observed_value_type(value)


def _merge_parameter_meta_dict(
    target: dict[str, ParameterMeta],
    name: str,
    dtype: str,
    observed_values: list[Any] | None,
) -> None:
    meta = target.get(name)
    if meta is None:
        meta = ParameterMeta(name=name, data_type=dtype)
        target[name] = meta
    else:
        meta.data_type = merge_data_type(meta.data_type, dtype)
    if isinstance(observed_values, list):
        for value in observed_values:
            meta.observed_values.add(str(value))


def load_route_groups(parameters_payload: dict[str, Any]) -> list[RouteGroup]:
    grouped: dict[tuple[str, str, str], RouteGroup] = {}
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
        method = str(entry.get("http_method", "GET") or "GET").strip().upper() or "GET"
        key = (host, path, method)
        if key not in grouped:
            grouped[key] = RouteGroup(host=host, path=path, scheme=parsed.scheme.lower(), http_method=method)
        group = grouped[key]

        query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        qk = frozenset(k for k, _ in query_items if k)
        if qk:
            group.observed_param_sets.add(qk)

        parameters = entry.get("parameters", [])
        if isinstance(parameters, list):
            for param in parameters:
                if not isinstance(param, dict):
                    continue
                name = str(param.get("name", "")).strip()
                if not name:
                    continue
                dtype = str(param.get("canonical_data_type", "string")).strip().lower() or "string"
                ovs = param.get("observed_values", [])
                ovs_list = ovs if isinstance(ovs, list) else []
                _merge_parameter_meta_dict(group.params, name, dtype, ovs_list)

        body_parameters = entry.get("body_parameters", [])
        body_names: set[str] = set()
        if isinstance(body_parameters, list):
            for param in body_parameters:
                if not isinstance(param, dict):
                    continue
                name = str(param.get("name", "")).strip()
                if not name:
                    continue
                body_names.add(name)
                dtype = str(param.get("canonical_data_type", "string")).strip().lower() or "string"
                ovs = param.get("observed_values", [])
                ovs_list = ovs if isinstance(ovs, list) else []
                _merge_parameter_meta_dict(group.body_params, name, dtype, ovs_list)

        bk = frozenset(body_names)
        if bk:
            group.observed_body_param_sets.add(bk)
        combined = qk | bk
        if combined:
            group.observed_combined_sets.add(combined)

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

    return [grouped[k] for k in sorted(grouped.keys())]


def load_post_requests_route_groups(post_payload: dict[str, Any]) -> list[RouteGroup]:
    raw_list = post_payload.get("requests", [])
    if not isinstance(raw_list, list):
        return []
    grouped: dict[tuple[str, str, str], RouteGroup] = {}
    for rec in raw_list:
        if not isinstance(rec, dict):
            continue
        if str(rec.get("http_method", "POST") or "POST").strip().upper() != "POST":
            continue
        raw_u = str(rec.get("target_url", "")).strip()
        if not raw_u:
            continue
        parsed = urllib.parse.urlparse(raw_u)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        host = parsed.netloc.lower()
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/") or "/"
        method = "POST"
        key = (host, path, method)
        if key not in grouped:
            g = RouteGroup(host=host, path=path, scheme=parsed.scheme.lower(), http_method=method)
            ct = str(rec.get("content_type") or rec.get("enctype") or "").strip()
            if ct:
                g.body_content_type = ct
            sug = rec.get("suggested_headers")
            if isinstance(sug, dict):
                for hk, hv in sug.items():
                    if str(hk).strip():
                        g.extra_request_headers[str(hk)] = str(hv)
            grouped[key] = g
        group = grouped[key]

        query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        qk = frozenset(k for k, _ in query_items if k)
        if qk:
            group.observed_param_sets.add(qk)
        for key_name, value in query_items:
            if not key_name:
                continue
            meta = group.params.get(key_name)
            inferred = infer_value_type(value)
            if meta is None:
                group.params[key_name] = ParameterMeta(name=key_name, data_type=inferred)
            else:
                meta.data_type = merge_data_type(meta.data_type, inferred)
            group.params[key_name].observed_values.add(value)

        body_fields = rec.get("body_fields")
        body_names: set[str] = set()
        if isinstance(body_fields, dict):
            for fname, finfo in body_fields.items():
                name = str(fname).strip()
                if not name:
                    continue
                body_names.add(name)
                if not isinstance(finfo, dict):
                    finfo = {}
                dtype = str(finfo.get("type") or "string").strip().lower() or "string"
                val = finfo.get("value")
                ovs = [str(val)] if val is not None else []
                ph = finfo.get("placeholder")
                if ph is not None:
                    ovs.append(str(ph))
                _merge_parameter_meta_dict(group.body_params, name, dtype, ovs)
        bk = frozenset(body_names)
        if bk:
            group.observed_body_param_sets.add(bk)
        combined = qk | bk
        if combined:
            group.observed_combined_sets.add(combined)

    return [grouped[k] for k in sorted(grouped.keys())]


def merge_route_groups_lists(primary: list[RouteGroup], extra: list[RouteGroup]) -> list[RouteGroup]:
    def _merge_meta_maps(
        dest: dict[str, ParameterMeta],
        src: dict[str, ParameterMeta],
    ) -> None:
        for name, sm in src.items():
            dm = dest.get(name)
            if dm is None:
                dest[name] = ParameterMeta(
                    name=sm.name,
                    data_type=sm.data_type,
                    observed_values=set(sm.observed_values),
                )
            else:
                dm.data_type = merge_data_type(dm.data_type, sm.data_type)
                dm.observed_values |= sm.observed_values

    idx: dict[tuple[str, str, str], RouteGroup] = {}
    for g in primary:
        idx[(g.host, g.path, g.http_method.upper())] = g
    for g in extra:
        k = (g.host, g.path, g.http_method.upper())
        if k not in idx:
            idx[k] = g
            continue
        t = idx[k]
        _merge_meta_maps(t.params, g.params)
        _merge_meta_maps(t.body_params, g.body_params)
        t.observed_param_sets |= g.observed_param_sets
        t.observed_body_param_sets |= g.observed_body_param_sets
        t.observed_combined_sets |= g.observed_combined_sets
        for hk, hv in g.extra_request_headers.items():
            t.extra_request_headers.setdefault(hk, hv)
        if g.body_content_type and not t.body_content_type:
            t.body_content_type = g.body_content_type
    return [idx[k] for k in sorted(idx.keys())]


def generic_value_for(meta: ParameterMeta) -> str:
    table = active_fozzy_config().get("default_generic_by_type") or {}
    return _core_generic_value_for(meta, default_generic_by_type=table)


def baseline_seed_value_for(meta: ParameterMeta) -> str:
    table = active_fozzy_config().get("default_generic_by_type") or {}
    return _core_baseline_seed_value_for(meta, default_generic_by_type=table)


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
    return _core_build_url(scheme, host, path, values)


def build_placeholder_url(scheme: str, host: str, path: str, params: list[ParameterMeta]) -> str:
    return _core_build_placeholder_url(scheme, host, path, params)


def route_group_param_meta(group: RouteGroup, name: str) -> ParameterMeta:
    return _core_route_group_param_meta(group, name)


def route_group_all_param_names(group: RouteGroup) -> set[str]:
    return _core_route_group_all_param_names(group)


def _merge_fozzy_headers(group: RouteGroup) -> dict[str, str]:
    base = active_fozzy_config().get("request_headers") or {}
    return _core_merge_request_headers(base, group.extra_request_headers)


def build_urlencoded_form_body(values: dict[str, str]) -> bytes:
    return _core_build_urlencoded_form_body(values)


def build_fuzz_http_request(
    group: RouteGroup,
    values: dict[str, str],
) -> tuple[str, str, dict[str, str], bytes]:
    return _core_build_fuzz_http_request(
        group,
        values,
        base_headers=active_fozzy_config().get("request_headers") or {},
    )


def _request_body_fingerprint(body: bytes | None) -> str:
    return _core_request_body_fingerprint(body)


def build_placeholder_request_summary(group: RouteGroup, permutation: tuple[str, ...]) -> str:
    return _core_build_placeholder_request_summary(group, permutation)


def _fozzy_queue_enabled() -> bool:
    cfg = active_fozzy_config()
    return bool(cfg.get("http_queue_enabled", False))


def _get_fozzy_http_queue() -> HttpRequestQueue:
    global _HTTP_QUEUE_INSTANCE
    with _HTTP_QUEUE_LOCK:
        if _HTTP_QUEUE_INSTANCE is None:
            cfg = active_fozzy_config()
            _HTTP_QUEUE_INSTANCE = HttpRequestQueue(
                db_path=str(cfg.get("http_queue_db_path") or "output/http_request_queue.sqlite3"),
                spool_dir=str(cfg.get("http_queue_spool_dir") or "output/http-request-spool"),
                lease_seconds=int(cfg.get("http_queue_lease_seconds", 90) or 90),
                retry_base_seconds=float(cfg.get("http_queue_retry_base_seconds", 1.0) or 1.0),
                retry_max_seconds=float(cfg.get("http_queue_retry_max_seconds", 60.0) or 60.0),
                worker_id=f"fozzy-{os.getpid()}-{threading.get_ident()}",
            )
        return _HTTP_QUEUE_INSTANCE


def _extract_response_header_names(headers_obj: Any) -> list[str]:
    if headers_obj is None:
        return []
    names: list[str] = []
    try:
        items = list(headers_obj.items())
    except Exception:
        items = []
    for key, _value in items:
        name = str(key or "").strip()
        if name:
            names.append(name)
    if not names:
        try:
            for key in headers_obj.keys():
                name = str(key or "").strip()
                if name:
                    names.append(name)
        except Exception:
            pass
    seen: set[str] = set()
    deduped: list[str] = []
    for name in names:
        lowered = name.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(name)
    return deduped


def perform_fozzy_http_request(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
    timeout: float,
) -> dict[str, Any]:
    cfg = active_fozzy_config()
    preview_limit = int(cfg.get("body_preview_limit", 2048))
    m = (method or "GET").strip().upper() or "GET"
    hdrs = dict(headers or {})

    if _fozzy_queue_enabled():
        result = _get_fozzy_http_queue().submit_and_wait(
            method=m,
            url=url,
            headers=hdrs,
            body=body if body else None,
            timeout_seconds=timeout,
            read_limit=preview_limit,
            metadata={"component": "fozzy", "operation": "perform_fozzy_http_request"},
            dedupe_key=None,
            max_attempts=int(cfg.get("http_queue_max_attempts", 5) or 5),
            wait_timeout_seconds=max(timeout * 6.0, 30.0),
        )
        response = result.get("response") if isinstance(result.get("response"), dict) else {}
        body_preview = str(response.get("body_text_preview", "") or "")
        header_names = _extract_response_header_names(response.get("headers"))
        elapsed_ms = int(response.get("elapsed_ms", 0) or 0)
        out: dict[str, Any] = {
            "url": url,
            "http_method": m,
            "status": int(result.get("status_code", 0) or 0),
            "size": len(body_preview.encode("utf-8", errors="ignore")),
            "body_preview": body_preview,
            "elapsed_ms": elapsed_ms,
            "response_header_names": header_names,
            "request_body_size": len(body or b""),
        }
        if not bool(result.get("ok", False)):
            out["error"] = str(result.get("note"))
        return out

    try:
        response = request_capped(
            m,
            url,
            headers=hdrs,
            content=body,
            timeout_seconds=timeout,
            read_limit=preview_limit,
            user_agent=str(hdrs.get("User-Agent") or "nightmare-fozzy/1.0"),
        )
        return {
            "url": url,
            "http_method": m,
            "status": int(response.status_code or 0),
            "size": len(response.body),
            "body_preview": response.body.decode("utf-8", errors="replace"),
            "elapsed_ms": int(response.elapsed_ms or 0),
            "response_header_names": _extract_response_header_names(response.headers),
            "request_body_size": len(body or b""),
        }
    except Exception as exc:
        return {
            "url": url,
            "http_method": m,
            "status": 0,
            "size": 0,
            "body_preview": "",
            "elapsed_ms": 0,
            "response_header_names": [],
            "request_body_size": len(body or b""),
            "error": str(exc),
        }


def request_url(url: str, timeout: float) -> dict[str, Any]:
    cfg = active_fozzy_config()
    headers = cfg.get("request_headers") or {}
    if not isinstance(headers, dict):
        headers = {}
    return perform_fozzy_http_request(
        method="GET",
        url=url,
        headers={str(k): str(v) for k, v in headers.items()},
        body=None,
        timeout=timeout,
    )


def response_differs(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return int(a.get("status", 0)) != int(b.get("status", 0)) or int(a.get("size", 0)) != int(b.get("size", 0))


def curl_command_for_url(url: str, timeout: float) -> str:
    return curl_command_for_fozzy_request(
        method="GET",
        url=url,
        headers={},
        body=None,
        timeout=timeout,
    )


def curl_command_for_fozzy_request(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
    *,
    timeout: float,
) -> str:
    timeout_token = str(int(timeout)) if timeout > 0 else "10"
    parts = ["curl", "-sS", "-L", "--max-time", timeout_token]
    cfg_headers = active_fozzy_config().get("request_headers") or {}
    if isinstance(cfg_headers, dict):
        for key, value in cfg_headers.items():
            parts.extend(["-H", f"\"{key}: {value}\""])
    for key, value in (headers or {}).items():
        parts.extend(["-H", f"\"{key}: {value}\""])
    m = (method or "GET").strip().upper() or "GET"
    if m != "GET":
        parts.extend(["-X", m])
    if body:
        body_s = body.decode("utf-8", errors="replace")
        parts.extend(["--data-binary", shlex.quote(body_s)])
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
    http_method: str = "GET",
    body_fingerprint: str = "",
) -> str:
    payload = {
        "phase": str(phase or "").strip(),
        "host": str(host or "").strip().lower(),
        "path": str(path or "").strip() or "/",
        "permutation": list(permutation or ()),
        "mutated_parameter": str(mutated_parameter or ""),
        "mutated_value": str(mutated_value or ""),
        "url": str(url or "").strip(),
        "http_method": str(http_method or "GET").strip().upper() or "GET",
        "body_fingerprint": str(body_fingerprint or ""),
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
        http_method=str(line.get("http_method", "GET") or "GET"),
        body_fingerprint=str(line.get("body_fingerprint", "") or ""),
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
    *,
    http_method: str = "GET",
    request_headers: dict[str, str] | None = None,
    request_body: bytes | None = None,
) -> None:
    if plan_lines is None:
        return
    request_id = len(plan_lines) + 1
    rh = dict(request_headers or {})
    rb = request_body or b""
    bf = _request_body_fingerprint(rb)
    line = {
        "request_id": request_id,
        "phase": phase,
        "host": host,
        "path": path,
        "permutation": list(permutation or ()),
        "mutated_parameter": mutated_parameter,
        "mutated_value": mutated_value,
        "url": url,
        "http_method": str(http_method or "GET").strip().upper() or "GET",
        "body_fingerprint": bf,
        "request_headers": rh,
        "request_body_base64": base64.b64encode(rb).decode("ascii") if rb else "",
        "curl": curl_command_for_fozzy_request(
            str(http_method or "GET"),
            url,
            rh,
            rb if rb else None,
            timeout=timeout,
        ),
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
        method = str(line.get("http_method", "GET") or "GET").strip().upper() or "GET"
        endpoint = f"{method} {host}{path}"
        counts[endpoint] = counts.get(endpoint, 0) + 1
    rows = [{"endpoint": endpoint, "request_count": count} for endpoint, count in counts.items()]
    rows.sort(key=lambda item: (-int(item["request_count"]), str(item["endpoint"])))
    return rows


def plan_line_endpoint_key(line: dict[str, Any]) -> str:
    host = str(line.get("host", "")).strip().lower()
    path = str(line.get("path", "")).strip() or "/"
    method = str(line.get("http_method", "GET") or "GET").strip().upper() or "GET"
    return f"{method}\t{host}\t{path}"


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
    """Keep plan lines in file order, at most ``max_per_endpoint`` lines per method+host+path, cutting only on block boundaries."""
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
    all_params = route_group_all_param_names(group)
    if not all_params:
        return set(), set()

    obs_sets: list[frozenset[str]] = list(group.observed_combined_sets) if group.observed_combined_sets else []
    if not obs_sets:
        for s in group.observed_param_sets:
            obs_sets.append(frozenset(s))
        for s in group.observed_body_param_sets:
            obs_sets.append(frozenset(s))
    if obs_sets:
        required = set(obs_sets[0])
        for s in obs_sets[1:]:
            required &= set(s)
    else:
        required = set()
    optional = set(all_params - required)

    baseline_values = {name: baseline_seed_value_for(route_group_param_meta(group, name)) for name in sorted(all_params)}
    bm, baseline_url, bh, bb = build_fuzz_http_request(group, baseline_values)
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
            http_method=bm,
            request_headers=bh,
            request_body=bb,
        )
        for candidate in sorted(optional):
            trial_values = dict(baseline_values)
            trial_values.pop(candidate, None)
            tm, trial_url, th, tb = build_fuzz_http_request(group, trial_values)
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
                http_method=tm,
                request_headers=th,
                request_body=tb,
            )
        return required, optional

    if not optional:
        return required, optional

    baseline = perform_fozzy_http_request(method=bm, url=baseline_url, headers=bh, body=bb, timeout=timeout)
    time.sleep(delay)

    if int(baseline.get("status", 0)) == 0:
        return required, optional

    for candidate in sorted(optional):
        trial_values = dict(baseline_values)
        trial_values.pop(candidate, None)
        tm, trial_url, th, tb = build_fuzz_http_request(group, trial_values)
        trial = perform_fozzy_http_request(method=tm, url=trial_url, headers=th, body=tb, timeout=timeout)
        time.sleep(delay)
        if response_differs(baseline, trial):
            required.add(candidate)

    optional = set(all_params - required)
    return required, optional


def route_group_observed_sets_for_permutations(group: RouteGroup) -> set[frozenset[str]]:
    """Prefer combined query∪body observations; otherwise union separate query/body observation sets."""
    if group.observed_combined_sets:
        return set(group.observed_combined_sets)
    out: set[frozenset[str]] = set()
    for s in group.observed_param_sets:
        out.add(frozenset(s))
    for s in group.observed_body_param_sets:
        out.add(frozenset(s))
    return out


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
    all_params = route_group_all_param_names(group)
    if not all_params:
        return set(), set()
    obs_sets: list[frozenset[str]] = []
    if group.observed_combined_sets:
        obs_sets = list(group.observed_combined_sets)
    else:
        for s in group.observed_param_sets:
            obs_sets.append(frozenset(s))
        for s in group.observed_body_param_sets:
            obs_sets.append(frozenset(s))
    if obs_sets:
        required = set(obs_sets[0])
        for s in obs_sets[1:]:
            required &= set(s)
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
        baseline_values = {
            name: baseline_seed_value_for(route_group_param_meta(group, name)) for name in permutation
        }
        _bm, baseline_url, _bh, baseline_body = build_fuzz_http_request(group, baseline_values)
        baseline_key = make_request_key(
            phase="fuzz_baseline",
            host=group.host,
            path=group.path,
            permutation=permutation,
            mutated_parameter=None,
            mutated_value=None,
            url=baseline_url,
            http_method=_bm,
            body_fingerprint=_request_body_fingerprint(baseline_body),
        )
        if baseline_key not in allowed_request_keys:
            continue
        total += 1
        for name in permutation:
            for fuzz_value in quick_fuzz_values:
                trial_values = dict(baseline_values)
                trial_values[name] = fuzz_value
                tm, trial_url, _th, trial_body = build_fuzz_http_request(group, trial_values)
                trial_key = make_request_key(
                    phase="fuzz_mutation",
                    host=group.host,
                    path=group.path,
                    permutation=permutation,
                    mutated_parameter=name,
                    mutated_value=fuzz_value,
                    url=trial_url,
                    http_method=tm,
                    body_fingerprint=_request_body_fingerprint(trial_body),
                )
                if trial_key in allowed_request_keys:
                    total += 1
    return total


def anomaly_file_name(
    host: str,
    path: str,
    permutation: tuple[str, ...],
    parameter: str,
    value: str,
    anomaly_type: str | None = None,
) -> str:
    seed = f"{host}|{path}|{','.join(permutation)}|{parameter}|{value}|{str(anomaly_type or '').strip().lower()}"
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


def _response_header_names(response: dict[str, Any] | None) -> set[str]:
    if not isinstance(response, dict):
        return set()
    raw = response.get("response_header_names", [])
    if not isinstance(raw, list):
        return set()
    names: set[str] = set()
    for item in raw:
        name = str(item or "").strip().lower()
        if name:
            names.add(name)
    return names


def detect_response_time_discrepancy(
    baseline: dict[str, Any],
    anomaly: dict[str, Any],
    *,
    multiplier: float = RESPONSE_TIME_DISCREPANCY_MULTIPLIER,
) -> tuple[bool, str]:
    baseline_ms = _response_elapsed_ms(baseline)
    anomaly_ms = _response_elapsed_ms(anomaly)
    if baseline_ms is None or anomaly_ms is None:
        return False, ""
    if baseline_ms <= 0:
        return False, ""
    threshold = baseline_ms * float(multiplier)
    if float(anomaly_ms) > threshold:
        return True, (
            f"Response elapsed time anomaly: baseline={baseline_ms}ms, "
            f"new={anomaly_ms}ms, ratio={float(anomaly_ms) / float(baseline_ms):.2f}x (> {multiplier:.1f}x)"
        )
    return False, ""


def detect_header_change(
    baseline: dict[str, Any],
    anomaly: dict[str, Any],
) -> tuple[bool, str]:
    baseline_names = _response_header_names(baseline)
    anomaly_names = _response_header_names(anomaly)
    if not baseline_names and not anomaly_names:
        return False, ""
    if baseline_names == anomaly_names:
        return False, ""
    added = sorted(anomaly_names - baseline_names)
    removed = sorted(baseline_names - anomaly_names)
    parts = [
        f"baseline_headers={len(baseline_names)}",
        f"new_headers={len(anomaly_names)}",
    ]
    if added:
        parts.append("added=" + ",".join(added))
    if removed:
        parts.append("removed=" + ",".join(removed))
    return True, "Header change anomaly: " + "; ".join(parts)


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
            anomaly_type = str(payload.get("anomaly_type", "") or "").strip()
            display_type = result_type
            if result_type == "anomaly" and anomaly_type:
                display_type = f"anomaly:{anomaly_type}"
            entries.append(
                {
                    "result_type": result_type,
                    "display_type": display_type,
                    "anomaly_type": anomaly_type,
                    "anomaly_note": str(payload.get("anomaly_note", "") or "").strip(),
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
    """Each immediate child directory with Fozzy output, extractor output, or crawl ``collected_data/``."""
    if not master_root.is_dir():
        return []
    pairs: list[tuple[str, Path]] = []
    for child in sorted(master_root.iterdir(), key=lambda p: p.name.lower()):
        if not child.is_dir() or child.name.startswith("."):
            continue
        results_sub = child / "results"
        if (
            results_sub.is_dir()
            or (child / "extractor" / "summary.json").is_file()
            or (child / "collected_data").is_dir()
        ):
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
            has_results = (inner / "results").is_dir()
            has_extractor_summary = (inner / "extractor" / "summary.json").is_file()
            has_collected_data = (inner / "collected_data").is_dir()
            if not has_results and not has_extractor_summary and not has_collected_data:
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
    qp = route.get("query_parameters")
    bp = route.get("body_parameters")
    if isinstance(qp, list) or isinstance(bp, list):
        nq = len(qp) if isinstance(qp, list) else 0
        nb = len(bp) if isinstance(bp, list) else 0
        pc = max(pc, nq + nb)
    return max(0, pc)


def aggregate_inventory_route_stats(routes: list[dict[str, Any]]) -> dict[str, Any]:
    seen_keys: set[tuple[str, str, str]] = set()
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
        method = str(r.get("http_method", "GET") or "GET").strip().upper() or "GET"
        seen_keys.add((h, pth, method))
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
    seen_keys: set[tuple[str, str, str]] = set()
    routes_with_parameters = 0
    total_parameter_slots = 0
    max_parameters_on_route = 0
    hosts: set[str] = set()
    for g in groups:
        method = (g.http_method or "GET").strip().upper() or "GET"
        seen_keys.add((g.host.lower(), g.path or "/", method))
        n = len(route_group_all_param_names(g))
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
                            "http_method": (g.http_method or "GET").strip().upper() or "GET",
                            "parameter_count": len(route_group_all_param_names(g)),
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
            method = str(r.get("http_method", "GET") or "GET").strip().upper() or "GET" if isinstance(r, dict) else "GET"
            per_route_rows.append(
                {
                    "domain_label": domain_label,
                    "host": h,
                    "path": pth,
                    "scheme": sch,
                    "http_method": method,
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


def _compact_result_entries_for_master_report(
    entries: list[dict[str, Any]],
    *,
    body_preview_max_chars: int = MASTER_REPORT_BODY_PREVIEW_MAX_CHARS,
    diff_text_max_lines: int = MASTER_REPORT_DIFF_TEXT_MAX_LINES,
) -> list[dict[str, Any]]:
    compact_entries: list[dict[str, Any]] = []
    body_limit = max(0, int(body_preview_max_chars))
    diff_limit = max(0, int(diff_text_max_lines))

    for item in entries:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        baseline = dict(row.get("baseline_response") or {})
        anomaly = dict(row.get("anomaly_response") or {})

        if body_limit > 0:
            baseline_preview = str(baseline.get("body_preview", "") or "")
            anomaly_preview = str(anomaly.get("body_preview", "") or "")
            if len(baseline_preview) > body_limit:
                baseline["body_preview"] = baseline_preview[:body_limit] + " … (truncated in master report)"
            if len(anomaly_preview) > body_limit:
                anomaly["body_preview"] = anomaly_preview[:body_limit] + " … (truncated in master report)"
        else:
            baseline.pop("body_preview", None)
            anomaly.pop("body_preview", None)

        row["baseline_response"] = baseline
        row["anomaly_response"] = anomaly

        diff_text = str(row.get("response_diff_text", "") or "")
        if diff_limit <= 0:
            row["response_diff_text"] = ""
        elif diff_text:
            diff_lines = diff_text.splitlines()
            if len(diff_lines) > diff_limit:
                row["response_diff_text"] = "\n".join(diff_lines[:diff_limit]) + "\n... (truncated in master report)"
        compact_entries.append(row)

    return compact_entries


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
    compact_discrepancies: bool = False,
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
    serialized_entries = (
        _compact_result_entries_for_master_report(result_entries) if compact_discrepancies else result_entries
    )
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
        "discrepancies": serialized_entries,
        "results_folder_files": list(results_folder_files),
    }
    if report_heading:
        anomaly_summary_payload["report_heading"] = report_heading
    if compact_discrepancies:
        anomaly_summary_payload["discrepancies_compacted"] = True
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
    ext_rows = extras.get("extractor_matches")
    if isinstance(ext_rows, list):
        anomaly_summary_payload["extractor_matches"] = ext_rows
    if extras.get("extractor_matches_truncated"):
        anomaly_summary_payload["extractor_matches_truncated"] = True
    ext_total = extras.get("extractor_matches_total")
    if isinstance(ext_total, int):
        anomaly_summary_payload["extractor_matches_total"] = max(0, ext_total)
    log_files = extras.get("log_files")
    if isinstance(log_files, list):
        anomaly_summary_payload["log_files"] = log_files
    return anomaly_summary_payload


def discover_extractor_summary_paths(domain_output: Path) -> list[Path]:
    """Resolve ``extractor/summary.json`` for a Fozzy domain output tree (canonical + alternates).

    Extractor output normally lives at ``<domain_output>/extractor/summary.json``. Some runs place
    it under the registrable folder (``<registrable>/extractor/``) instead; nested ``**/extractor/``
    is also checked so master aggregation still finds data.
    """
    candidates: list[Path] = []
    direct = domain_output / "extractor" / "summary.json"
    candidates.append(direct)

    parent = domain_output.parent
    if parent.name == "fozzy-output":
        registrable_root = parent.parent
        candidates.append(registrable_root / "extractor" / "summary.json")

    try:
        for p in domain_output.rglob("summary.json"):
            if p.parent.name == "extractor" and p.is_file():
                candidates.append(p)
    except OSError:
        pass

    seen: set[str] = set()
    out: list[Path] = []
    for p in candidates:
        try:
            key = str(p.resolve())
        except OSError:
            key = str(p)
        if key in seen:
            continue
        seen.add(key)
        if p.is_file():
            out.append(p)
    return out


def load_extractor_match_rows_for_master(
    _master_root: Path, pairs: list[tuple[str, Path]]
) -> tuple[list[dict[str, Any]], int, bool]:
    """Load per-domain ``extractor/summary.json`` rows for the master HTML table."""
    rows: list[dict[str, Any]] = []
    total_rows = 0
    truncated = False
    for _domain_label, domain_output in pairs:
        for summary_path in discover_extractor_summary_paths(domain_output):
            try:
                data = read_json(summary_path)
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(data, dict):
                continue
            for item in data.get("rows", []):
                if not isinstance(item, dict):
                    continue
                total_rows += 1
                if len(rows) < MASTER_REPORT_EXTRACTOR_ROWS_MAX:
                    rows.append(dict(item))
                else:
                    truncated = True
    rows.sort(
        key=lambda x: (
            str(x.get("domain_label", "")).lower(),
            str(x.get("url", "")).lower(),
            str(x.get("filter_name", "")).lower(),
            str(x.get("match_file", "")).lower(),
        )
    )
    return rows, total_rows, truncated


def _relative_path_text(path: Path, root: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(root.resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def discover_log_files_for_master(master_root: Path, pairs: list[tuple[str, Path]]) -> list[dict[str, Any]]:
    """Collect likely log files for display in the master HTML viewer."""
    root = master_root.resolve()
    candidates: set[Path] = set()

    for filename in ("fozzy.incremental.log", "fozzy.master_report.log", "extractor.log"):
        p = root / filename
        if p.is_file():
            candidates.add(p.resolve())

    for child in root.iterdir() if root.is_dir() else []:
        if not child.is_dir() or child.name.startswith("."):
            continue
        for p in child.glob("*.log"):
            if p.is_file():
                candidates.add(p.resolve())

    for _domain_name, domain_output in pairs:
        if not domain_output.is_dir():
            continue
        for p in domain_output.glob("*.log"):
            if p.is_file():
                candidates.add(p.resolve())
        site_dir = resolve_site_directory_from_domain_output(domain_output)
        if site_dir and site_dir.is_dir():
            for pattern in ("*_nightmare.log", "*_scrapy.log", "*.fozzy.log"):
                for p in site_dir.glob(pattern):
                    if p.is_file():
                        candidates.add(p.resolve())

    rows: list[dict[str, Any]] = []
    for p in sorted(candidates, key=lambda x: _relative_path_text(x, root).lower()):
        try:
            stat = p.stat()
            size_bytes = int(stat.st_size)
            mtime_utc = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
        except OSError:
            size_bytes = 0
            mtime_utc = ""
        rel = _relative_path_text(p, root)
        fetch_path = rel
        if rel == p.as_posix():
            fetch_path = p.as_posix()
        rows.append(
            {
                "label": rel,
                "path": fetch_path,
                "absolute_path": p.as_posix(),
                "size_bytes": size_bytes,
                "modified_utc": mtime_utc,
            }
        )
    return rows


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
    extractor_match_rows, extractor_match_total, extractor_matches_truncated = load_extractor_match_rows_for_master(
        master_root, pairs
    )
    log_files = discover_log_files_for_master(master_root, pairs)
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
        compact_discrepancies=True,
        payload_extras={
            "master_domain_inventory": domain_inv_rows,
            "master_per_route_inventory": per_route_rows,
            "master_per_route_inventory_truncated": per_route_truncated,
            "extractor_matches": extractor_match_rows,
            "extractor_matches_total": extractor_match_total,
            "extractor_matches_truncated": extractor_matches_truncated,
            "log_files": log_files,
        },
    )
    json_path = master_root / "all_domains.results_summary.json"
    html_path = master_root / "all_domains.results_summary.html"
    payload["summary_json_filename"] = json_path.name
    _atomic_write_text(json_path, json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    _atomic_write_text(html_path, render_anomaly_summary_html(payload), encoding="utf-8")
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
    anomaly_summary_payload["summary_json_filename"] = anomaly_summary_path.name
    _atomic_write_text(anomaly_summary_path, json.dumps(anomaly_summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    anomaly_summary_html_path = results_dir / f"{root_domain}.results_summary.html"
    _atomic_write_text(anomaly_summary_html_path, render_anomaly_summary_html(anomaly_summary_payload), encoding="utf-8")

    anomaly_summary_payload["results_folder_files"] = list_results_folder_files(results_dir)
    _atomic_write_text(anomaly_summary_path, json.dumps(anomaly_summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    _atomic_write_text(anomaly_summary_html_path, render_anomaly_summary_html(anomaly_summary_payload), encoding="utf-8")
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
    # Keep generated HTML lightweight: detailed discrepancy rows are loaded from the
    # companion summary JSON on page load instead of being embedded inline.
    rows: list[dict[str, Any]] = []
    include_domain = str(payload.get("summary_scope", "")) == "master"
    summary_json_filename = str(payload.get("summary_json_filename") or "").strip()
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

        result_type_raw = str(item.get("display_type", item.get("result_type", "")) or "").strip() or "unknown"
        anomaly_note_raw = str(item.get("anomaly_note", "") or "").strip()
        type_display, type_raw = clip_cell(result_type_raw)
        source_domain_raw = str(item.get("source_domain", "") or "").strip()
        domain_cell = ""
        if include_domain:
            domain_display, domain_raw_cell = clip_cell(source_domain_raw)
            domain_cell = f"<td data-raw='{html.escape(domain_raw_cell)}'>{domain_display}</td>"

        body_search = (
            f"{result_type_raw}\n{anomaly_note_raw}\n{source_domain_raw}\n{baseline_body}\n{anomaly_body}\n"
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
    master_log_files = payload.get("log_files")
    response_data_json = json.dumps(response_records, ensure_ascii=False).replace("</", "<\\/")
    summary_json_filename_js = json.dumps(summary_json_filename, ensure_ascii=False).replace("</", "<\\/")
    master_log_files_js = json.dumps(
        master_log_files if isinstance(master_log_files, list) else [],
        ensure_ascii=False,
    ).replace("</", "<\\/")
    embedded_payload = payload if str(payload.get("summary_scope", "")) == "single_domain" else None
    embedded_payload_js = json.dumps(embedded_payload, ensure_ascii=False).replace("</", "<\\/")

    inventory_sections_html = ""
    inventory_script_html = ""
    extractor_section_html = ""
    extractor_script_html = ""
    log_viewer_section_html = ""
    log_viewer_script_html = ""
    if str(payload.get("summary_scope", "")) == "master":
        inventory_sections_html = """
  <section class="inventory-section">
    <h3>Domain inventory (Nightmare / Fozzy)</h3>
    <p class="inventory-note" id="masterDomainInventoryNote">
      Loading domain inventory from summary JSON...
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
        <tbody><tr><td colspan='9'>Loading...</td></tr></tbody>
      </table>
    </div>
    <h3>Routes and parameter counts</h3>
    <p class="inventory-note" id="masterRouteInventoryNote">
      Per-route rows (scheme + host + path), sorted by parameter count.
    </p>
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
        <tbody><tr><td colspan='5'>Loading...</td></tr></tbody>
      </table>
    </div>
  </section>"""
        inventory_script_html = """
      setupTable("masterDomainInventoryTable", "masterDomainInventoryCount");
      enableResizableColumns("masterDomainInventoryTable");
      setupTable("masterRouteInventoryTable", "masterRouteInventoryCount");
      enableResizableColumns("masterRouteInventoryTable");
"""

        ext_filter_placeholders = [
            "Filter domain",
            "Filter URL",
            "Filter name",
            "Filter importance score",
            "Filter scope",
            "Filter side",
            "Filter match text",
            "Filter result file",
            "Filter match file",
        ]
        ext_filter_cells = []
        for col_idx, ph in enumerate(ext_filter_placeholders):
            ext_filter_cells.append(
                f'<th><input data-col="{col_idx}" type="text" placeholder="{html.escape(ph)}"></th>'
            )
        ext_thead_filters = '<tr class="filters">\n          ' + "\n          ".join(ext_filter_cells) + "\n        </tr>"

        extractor_section_html = f"""
  <section class="inventory-section">
    <h3>Extractor matches (regex vs Fozzy responses)</h3>
    <p class="inventory-note" id="extractorInventoryNote">
      Loading rows from per-domain <code>extractor/summary.json</code> artifacts.
      Each match is also stored under <code>extractor/matches/</code> as its own JSON file.
    </p>
    <div class="count-note" id="extractorMatchesCount"></div>
    <div class="scroll-inventory">
      <table id="extractorMatchesTable" class="data-table">
        <thead>
          <tr>
            <th data-type="string">Domain</th>
            <th data-type="string">URL</th>
            <th data-type="string">Filter name</th>
            <th data-type="number">Importance score</th>
            <th data-type="string">Scope</th>
            <th data-type="string">Side</th>
            <th data-type="string">Match preview</th>
            <th data-type="string">Result JSON</th>
            <th data-type="string">Match record</th>
          </tr>
          {ext_thead_filters}
        </thead>
        <tbody><tr><td colspan='9'>Loading...</td></tr></tbody>
      </table>
    </div>
  </section>"""
        extractor_script_html = """
      setupTable("extractorMatchesTable", "extractorMatchesCount");
      enableResizableColumns("extractorMatchesTable");
"""

    if str(payload.get("summary_scope", "")) == "master" and isinstance(master_log_files, list):
        log_viewer_section_html = """
  <section class="inventory-section">
    <h3>Log viewer</h3>
    <p class="inventory-note">
      Select a discovered log file and load it from disk. Very large logs are truncated client-side for display.
    </p>
    <div class="detail-toolbar">
      <label class="group-by-label" for="masterLogSelect">Log file</label>
      <select id="masterLogSelect" title="Discovered log files"></select>
      <button id="masterLogLoad" type="button">Load</button>
      <span class="detail-toolbar-hint" id="masterLogMeta"></span>
    </div>
    <div class="scroll-inventory">
      <pre id="masterLogViewer" style="margin:0;padding:10px;white-space:pre-wrap;word-break:break-word;font-size:12px;"></pre>
    </div>
  </section>"""
        log_viewer_script_html = """
      setupMasterLogViewer();
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
  {extractor_section_html}
  {log_viewer_section_html}
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
      let responseData = {response_data_json};
      let responseDataById = new Map(responseData.map((item) => [String(item.id), item]));
      const summaryJsonFilename = {summary_json_filename_js};
      const masterLogFiles = {master_log_files_js};
      const embeddedPayload = {embedded_payload_js};
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

      function escapeHtml(value) {{
        const s = String(value == null ? "" : value);
        return s
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }}

      function clipCellHtml(value) {{
        const raw = String(value == null ? "" : value);
        return `<span class='clip-cell' title='${{escapeHtml(raw)}}'>${{escapeHtml(raw)}}</span>`;
      }}

      function fileHref(pathValue) {{
        const raw = String(pathValue == null ? "" : pathValue).trim();
        if (!raw) return "";
        const normalized = raw.replaceAll("\\\\", "/");
        return "file:///" + encodeURI(normalized);
      }}

      function statusPhrase(code) {{
        const map = {{
          200: "OK",
          201: "Created",
          202: "Accepted",
          204: "No Content",
          301: "Moved Permanently",
          302: "Found",
          304: "Not Modified",
          307: "Temporary Redirect",
          308: "Permanent Redirect",
          400: "Bad Request",
          401: "Unauthorized",
          403: "Forbidden",
          404: "Not Found",
          405: "Method Not Allowed",
          408: "Request Timeout",
          409: "Conflict",
          410: "Gone",
          413: "Payload Too Large",
          415: "Unsupported Media Type",
          418: "I'm a teapot",
          422: "Unprocessable Entity",
          429: "Too Many Requests",
          500: "Internal Server Error",
          501: "Not Implemented",
          502: "Bad Gateway",
          503: "Service Unavailable",
          504: "Gateway Timeout"
        }};
        return map[Number(code)] || "";
      }}

      function extractElapsedMs(responseObj) {{
        const rsp = responseObj && typeof responseObj === "object" ? responseObj : {{}};
        const ms = Number(rsp.elapsed_ms);
        if (Number.isFinite(ms)) return Math.round(ms);
        const sec = Number(rsp.elapsed_seconds);
        if (Number.isFinite(sec)) return Math.round(sec * 1000);
        return null;
      }}

      function formatUtcTime(value) {{
        const raw = String(value == null ? "" : value).trim();
        if (!raw) return "";
        const dt = new Date(raw);
        if (Number.isNaN(dt.getTime())) return raw;
        const h = String(dt.getUTCHours()).padStart(2, "0");
        const m = String(dt.getUTCMinutes()).padStart(2, "0");
        const s = String(dt.getUTCSeconds()).padStart(2, "0");
        return `${{h}}:${{m}}:${{s}}`;
      }}

      function buildResponseDataFromDiscrepancies(discrepancies) {{
        const out = [];
        discrepancies.forEach((item, idx) => {{
          if (!item || typeof item !== "object") return;
          const id = `r${{idx + 1}}`;
          const baseline = item.baseline_response && typeof item.baseline_response === "object" ? item.baseline_response : {{}};
          const anomaly = item.anomaly_response && typeof item.anomaly_response === "object" ? item.anomaly_response : {{}};
          out.push({{
            id,
            baseline,
            anomaly,
            diff: String(item.response_diff_text || ""),
            diff_side_by_side_html: ""
          }});
        }});
        return out;
      }}

      function renderDetailRows(discrepancies) {{
        const includeDomain = {str(str(payload.get("summary_scope", "")) == "master").lower()};
        const out = [];
        discrepancies.forEach((item, idx) => {{
          if (!item || typeof item !== "object") return;
          const responseId = `r${{idx + 1}}`;
          const baseline = item.baseline_response && typeof item.baseline_response === "object" ? item.baseline_response : {{}};
          const anomaly = item.anomaly_response && typeof item.anomaly_response === "object" ? item.anomaly_response : {{}};
          const typeRaw = String(item.display_type || item.result_type || "").trim() || "unknown";
          const anomalyNote = String(item.anomaly_note || "");
          const sourceDomainRaw = String(item.source_domain || "").trim();
          const capturedRaw = String(item.captured_at_utc || "");
          const capturedDisp = formatUtcTime(capturedRaw);
          const urlRaw = String(item.url || "");
          const paramRaw = String(item.mutated_parameter || "");
          const valueRaw = String(item.mutated_value || "");
          const resultRaw = String(item.result_file || "");
          const baselineStatus = Number(item.baseline_status || 0) || 0;
          const anomalyStatus = Number(item.new_status || 0) || 0;
          const baselineSize = Number(item.baseline_size || 0) || 0;
          const anomalySize = Number(item.new_size || 0) || 0;
          const sizeDiff = anomalySize - baselineSize;
          const baselineMs = extractElapsedMs(baseline);
          const anomalyMs = extractElapsedMs(anomaly);
          const diffMs = baselineMs == null || anomalyMs == null ? null : (anomalyMs - baselineMs);
          const baselineLabel = `${{baselineStatus}} ${{statusPhrase(baselineStatus)}}`.trim();
          const anomalyLabel = `${{anomalyStatus}} ${{statusPhrase(anomalyStatus)}}`.trim();
          const bodySearch = `${{typeRaw}}\\n${{anomalyNote}}\\n${{sourceDomainRaw}}\\n${{String(baseline.body_preview || "")}}\\n${{String(anomaly.body_preview || "")}}\\n${{baselineMs == null ? "" : baselineMs}}\\n${{anomalyMs == null ? "" : anomalyMs}}\\n${{diffMs == null ? "" : diffMs}}`.toLowerCase();
          const diffClass = diffMs == null ? "size-diff-zero" : (diffMs > 0 ? "size-diff-pos" : (diffMs < 0 ? "size-diff-neg" : "size-diff-zero"));
          const sizeDiffClass = sizeDiff > 0 ? "size-diff-pos" : (sizeDiff < 0 ? "size-diff-neg" : "size-diff-zero");
          const resultCell = resultRaw
            ? `<a class='clip-cell file-link' title='${{escapeHtml(resultRaw)}}' href='${{escapeHtml(fileHref(resultRaw))}}' target='_blank' rel='noopener noreferrer'>${{escapeHtml(resultRaw)}}</a>`
            : "";
          const domainCell = includeDomain
            ? `<td data-raw='${{escapeHtml(sourceDomainRaw)}}'>${{clipCellHtml(sourceDomainRaw)}}</td>`
            : "";
          out.push(
            `<tr class='detail-data-row' data-search-extra='${{escapeHtml(bodySearch)}}'>`
            + `<td data-raw='${{escapeHtml(typeRaw)}}'>${{clipCellHtml(typeRaw)}}</td>`
            + domainCell
            + `<td data-raw='${{escapeHtml(capturedRaw)}}'>${{clipCellHtml(capturedDisp)}}</td>`
            + `<td data-raw='${{escapeHtml(urlRaw)}}'>${{clipCellHtml(urlRaw)}}</td>`
            + `<td data-raw='${{escapeHtml(paramRaw)}}'>${{clipCellHtml(paramRaw)}}</td>`
            + `<td data-raw='${{escapeHtml(valueRaw)}}'>${{clipCellHtml(valueRaw)}}</td>`
            + `<td data-raw='${{escapeHtml(String(baselineStatus))}}'><a href='#' class='resp-link' data-kind='baseline' data-id='${{escapeHtml(responseId)}}'>${{clipCellHtml(baselineLabel)}}</a></td>`
            + `<td data-raw='${{escapeHtml(String(anomalyStatus))}}'><a href='#' class='resp-link' data-kind='anomaly' data-id='${{escapeHtml(responseId)}}'>${{clipCellHtml(anomalyLabel)}}</a></td>`
            + (baselineMs == null ? "<td data-raw=''>—</td>" : `<td data-raw='${{baselineMs}}'>${{baselineMs}}</td>`)
            + (anomalyMs == null ? "<td data-raw=''>—</td>" : `<td data-raw='${{anomalyMs}}'>${{anomalyMs}}</td>`)
            + (diffMs == null ? "<td data-raw=''>—</td>" : `<td data-raw='${{diffMs}}'><span class='${{diffClass}}'>${{diffMs >= 0 ? "+" : ""}}${{diffMs}}</span></td>`)
            + `<td data-raw='${{baselineSize}}'>${{baselineSize}}</td>`
            + `<td data-raw='${{anomalySize}}'>${{anomalySize}}</td>`
            + `<td data-raw='${{sizeDiff}}'><span class='${{sizeDiffClass}}'>${{sizeDiff >= 0 ? "+" : ""}}${{sizeDiff}}</span></td>`
            + `<td data-raw='diff'><a href='#' class='tool-diff' data-id='${{escapeHtml(responseId)}}'>diff</a></td>`
            + `<td data-raw='${{escapeHtml(resultRaw)}}'>${{resultCell}}</td>`
            + "</tr>"
          );
        }});
        return out.join("\\n");
      }}

      function renderMasterDomainInventoryRows(rows) {{
        const out = [];
        let sumUnique = 0;
        let sumWithParams = 0;
        let sumSlots = 0;
        let sumHosts = 0;
        let maxParams = 0;
        (Array.isArray(rows) ? rows : []).forEach((inv) => {{
          if (!inv || typeof inv !== "object") return;
          const lbl = String(inv.domain_label || "");
          const src = String(inv.source || "");
          const ur = Number(inv.unique_routes || 0) || 0;
          const rwp = Number(inv.routes_with_parameters || 0) || 0;
          const tps = Number(inv.total_parameter_slots || 0) || 0;
          const mx = Number(inv.max_parameters_on_route || 0) || 0;
          const avg = Number(inv.avg_parameters_on_parameterized_routes || 0) || 0;
          const uh = Number(inv.unique_hosts || 0) || 0;
          const site = String(inv.site_folder || "");
          sumUnique += ur;
          sumWithParams += rwp;
          sumSlots += tps;
          sumHosts += uh;
          maxParams = Math.max(maxParams, mx);
          out.push(
            "<tr>"
            + `<td data-raw='${{escapeHtml(lbl)}}'>${{clipCellHtml(lbl)}}</td>`
            + `<td data-raw='${{escapeHtml(src)}}'>${{escapeHtml(src)}}</td>`
            + `<td data-raw='${{ur}}'>${{ur}}</td>`
            + `<td data-raw='${{rwp}}'>${{rwp}}</td>`
            + `<td data-raw='${{tps}}'>${{tps}}</td>`
            + `<td data-raw='${{mx}}'>${{mx}}</td>`
            + `<td data-raw='${{avg}}'>${{avg}}</td>`
            + `<td data-raw='${{uh}}'>${{uh}}</td>`
            + `<td data-raw='${{escapeHtml(site)}}'>${{clipCellHtml(site)}}</td>`
            + "</tr>"
          );
        }});
        const tfoot = (
          "<tfoot><tr>"
          + "<td data-raw='totals'><strong>Totals / max</strong></td>"
          + "<td data-raw=''></td>"
          + `<td data-raw='${{sumUnique}}'><strong>${{sumUnique}}</strong></td>`
          + `<td data-raw='${{sumWithParams}}'><strong>${{sumWithParams}}</strong></td>`
          + `<td data-raw='${{sumSlots}}'><strong>${{sumSlots}}</strong></td>`
          + `<td data-raw='${{maxParams}}'><strong>${{maxParams}}</strong></td>`
          + "<td data-raw=''></td>"
          + `<td data-raw='${{sumHosts}}'><strong>${{sumHosts}}</strong></td>`
          + "<td data-raw=''></td>"
          + "</tr></tfoot>"
        );
        return {{
          tbodyHtml: out.length ? out.join("\\n") : "<tr><td colspan='9'>No rows</td></tr>",
          tfootHtml: tfoot,
          count: out.length
        }};
      }}

      function renderMasterRouteInventoryRows(rows) {{
        const out = [];
        (Array.isArray(rows) ? rows : []).forEach((rr) => {{
          if (!rr || typeof rr !== "object") return;
          const dl = String(rr.domain_label || "");
          const ho = String(rr.host || "");
          const pa = String(rr.path || "");
          const sc = String(rr.scheme || "https");
          const pc = Number(rr.parameter_count || 0) || 0;
          out.push(
            "<tr>"
            + `<td data-raw='${{escapeHtml(dl)}}'>${{clipCellHtml(dl)}}</td>`
            + `<td data-raw='${{escapeHtml(ho)}}'>${{escapeHtml(ho)}}</td>`
            + `<td data-raw='${{escapeHtml(pa)}}'>${{clipCellHtml(pa)}}</td>`
            + `<td data-raw='${{escapeHtml(sc)}}'>${{escapeHtml(sc)}}</td>`
            + `<td data-raw='${{pc}}'>${{pc}}</td>`
            + "</tr>"
          );
        }});
        return out.length ? out.join("\\n") : "<tr><td colspan='5'>No routes</td></tr>";
      }}

      function renderExtractorRows(rows) {{
        const out = [];
        (Array.isArray(rows) ? rows : []).forEach((er) => {{
          if (!er || typeof er !== "object") return;
          const dl = String(er.domain_label || "");
          const u = String(er.url || "");
          const fn = String(er.filter_name || "");
          const imp = Number(er.importance_score || 0) || 0;
          const sc = String(er.scope || "");
          const side = String(er.response_side || "");
          const prev = String(er.match_preview || "");
          const rf = String(er.result_file || "");
          const mf = String(er.match_file || "");
          const rt = String(er.result_type || "");
          const extra = `${{prev}}\\n${{rt}}\\n${{fn}}\\n${{imp}}\\n${{sc}}\\n${{side}}`.toLowerCase();
          const rfCell = rf
            ? `<td data-raw='${{escapeHtml(rf)}}'><a class='file-link' title='${{escapeHtml(rf)}}' href='${{escapeHtml(fileHref(rf))}}' target='_blank' rel='noopener noreferrer'>result JSON</a></td>`
            : "<td data-raw=''>—</td>";
          const mfCell = mf
            ? `<td data-raw='${{escapeHtml(mf)}}'><a class='file-link' title='${{escapeHtml(mf)}}' href='${{escapeHtml(fileHref(mf))}}' target='_blank' rel='noopener noreferrer'>match record</a></td>`
            : "<td data-raw=''>—</td>";
          out.push(
            `<tr class='detail-data-row' data-search-extra='${{escapeHtml(extra)}}'>`
            + `<td data-raw='${{escapeHtml(dl)}}'>${{clipCellHtml(dl)}}</td>`
            + `<td data-raw='${{escapeHtml(u)}}'>${{clipCellHtml(u)}}</td>`
            + `<td data-raw='${{escapeHtml(fn)}}'>${{clipCellHtml(fn)}}</td>`
            + `<td data-raw='${{imp}}'>${{imp}}</td>`
            + `<td data-raw='${{escapeHtml(sc)}}'>${{escapeHtml(sc)}}</td>`
            + `<td data-raw='${{escapeHtml(side)}}'>${{escapeHtml(side)}}</td>`
            + `<td data-raw='${{escapeHtml(prev)}}'>${{clipCellHtml(prev)}}</td>`
            + rfCell
            + mfCell
            + "</tr>"
          );
        }});
        return out.length ? out.join("\\n") : "<tr><td colspan='9'>No extractor matches</td></tr>";
      }}

      function populateMasterSectionsFromPayload(loadedPayload) {{
        if (!loadedPayload || typeof loadedPayload !== "object") return;
        const masterInvRows = Array.isArray(loadedPayload.master_domain_inventory) ? loadedPayload.master_domain_inventory : [];
        const routeRows = Array.isArray(loadedPayload.master_per_route_inventory) ? loadedPayload.master_per_route_inventory : [];
        const extractorRows = Array.isArray(loadedPayload.extractor_matches) ? loadedPayload.extractor_matches : [];
        const extractorRowsTotal = Number(loadedPayload.extractor_matches_total || 0) || extractorRows.length;
        const extractorRowsTruncated = !!loadedPayload.extractor_matches_truncated;

        const domainTable = document.getElementById("masterDomainInventoryTable");
        if (domainTable) {{
          const tbody = domainTable.querySelector("tbody");
          if (tbody) {{
            const rendered = renderMasterDomainInventoryRows(masterInvRows);
            tbody.innerHTML = rendered.tbodyHtml;
            const oldTfoot = domainTable.querySelector("tfoot");
            if (oldTfoot) oldTfoot.remove();
            domainTable.insertAdjacentHTML("beforeend", rendered.tfootHtml);
            const note = document.getElementById("masterDomainInventoryNote");
            if (note) {{
              note.textContent =
                `${{rendered.count}} domain output folder(s). Unique routes are distinct scheme+host+path groups from *.fozzy.inventory.json when present, otherwise <domain>.parameters.json.`;
            }}
          }}
        }}

        const routeTable = document.getElementById("masterRouteInventoryTable");
        if (routeTable) {{
          const tbody = routeTable.querySelector("tbody");
          if (tbody) tbody.innerHTML = renderMasterRouteInventoryRows(routeRows);
          const routeNote = document.getElementById("masterRouteInventoryNote");
          if (routeNote && loadedPayload.master_per_route_inventory_truncated) {{
            routeNote.textContent = "Per-route rows are truncated for report size; inspect per-domain *.fozzy.inventory.json for full list.";
          }}
        }}

        const extractorTable = document.getElementById("extractorMatchesTable");
        if (extractorTable) {{
          const tbody = extractorTable.querySelector("tbody");
          if (tbody) tbody.innerHTML = renderExtractorRows(extractorRows);
          const exNote = document.getElementById("extractorInventoryNote");
          if (exNote) {{
            if (extractorRowsTruncated && extractorRowsTotal > extractorRows.length) {{
              exNote.textContent =
                `${{extractorRows.length}} of ${{extractorRowsTotal}} row(s) shown (truncated for report size). Inspect per-domain extractor/summary.json for full data.`;
            }} else {{
              exNote.textContent =
                `${{extractorRows.length}} row(s) from per-domain extractor/summary.json artifacts. Each match is also stored under extractor/matches/.`;
            }}
          }}
        }}
      }}

      function renderMasterSectionsLoadError(message) {{
        const msg = String(message || "Unable to load summary JSON payload.");
        const domainTable = document.getElementById("masterDomainInventoryTable");
        if (domainTable) {{
          const tbody = domainTable.querySelector("tbody");
          if (tbody) tbody.innerHTML = `<tr><td colspan="8">${{escapeHtml(msg)}}</td></tr>`;
        }}
        const routeTable = document.getElementById("masterRouteInventoryTable");
        if (routeTable) {{
          const tbody = routeTable.querySelector("tbody");
          if (tbody) tbody.innerHTML = `<tr><td colspan="5">${{escapeHtml(msg)}}</td></tr>`;
        }}
        const extractorTable = document.getElementById("extractorMatchesTable");
        if (extractorTable) {{
          const tbody = extractorTable.querySelector("tbody");
          if (tbody) tbody.innerHTML = `<tr><td colspan="9">${{escapeHtml(msg)}}</td></tr>`;
        }}
        const dNote = document.getElementById("masterDomainInventoryNote");
        if (dNote) dNote.textContent = msg;
        const rNote = document.getElementById("masterRouteInventoryNote");
        if (rNote) rNote.textContent = msg;
        const eNote = document.getElementById("extractorInventoryNote");
        if (eNote) eNote.textContent = msg;
      }}

      async function loadReportPayloadFromDisk() {{
        const isFileProtocol = String(window.location.protocol || "").toLowerCase() === "file:";
        if (isFileProtocol) return null;
        const fallback = String(window.location.pathname || "").replace(/\\.html?$/i, ".json");
        const target = String(summaryJsonFilename || "").trim() || fallback;
        if (!target) return null;
        try {{
          const rsp = await fetch(target, {{ cache: "no-store" }});
          if (rsp.ok) return await rsp.json();
        }} catch {{
        }}
        try {{
          const text = await new Promise((resolve, reject) => {{
            const xhr = new XMLHttpRequest();
            xhr.open("GET", target, true);
            xhr.onreadystatechange = () => {{
              if (xhr.readyState !== 4) return;
              if ((xhr.status >= 200 && xhr.status < 300) || xhr.status === 0) resolve(xhr.responseText || "");
              else reject(new Error(`HTTP ${{xhr.status}}`));
            }};
            xhr.onerror = () => reject(new Error("network error"));
            xhr.send();
          }});
          return JSON.parse(text || "{{}}");
        }} catch {{
          return null;
        }}
      }}

      async function loadTextFromPath(targetPath) {{
        const isFileProtocol = String(window.location.protocol || "").toLowerCase() === "file:";
        if (isFileProtocol) return null;
        const path = String(targetPath || "").trim();
        if (!path) return null;
        try {{
          const rsp = await fetch(path, {{ cache: "no-store" }});
          if (rsp.ok) return await rsp.text();
        }} catch {{
        }}
        try {{
          return await new Promise((resolve, reject) => {{
            const xhr = new XMLHttpRequest();
            xhr.open("GET", path, true);
            xhr.onreadystatechange = () => {{
              if (xhr.readyState !== 4) return;
              if ((xhr.status >= 200 && xhr.status < 300) || xhr.status === 0) resolve(xhr.responseText || "");
              else reject(new Error(`HTTP ${{xhr.status}}`));
            }};
            xhr.onerror = () => reject(new Error("network error"));
            xhr.send();
          }});
        }} catch {{
          return null;
        }}
      }}

      function setupMasterLogViewer() {{
        const select = document.getElementById("masterLogSelect");
        const btn = document.getElementById("masterLogLoad");
        const meta = document.getElementById("masterLogMeta");
        const pre = document.getElementById("masterLogViewer");
        if (!select || !btn || !pre) return;

        const rows = Array.isArray(masterLogFiles) ? masterLogFiles.filter((x) => x && typeof x === "object") : [];
        select.innerHTML = "";
        if (!rows.length) {{
          pre.textContent = "No log files discovered for this master report.";
          if (meta) meta.textContent = "";
          btn.disabled = true;
          return;
        }}

        rows.forEach((row, idx) => {{
          const path = String(row.path || "");
          const label = String(row.label || path || `log_${{idx + 1}}`);
          const opt = document.createElement("option");
          opt.value = path;
          opt.textContent = label;
          select.appendChild(opt);
        }});

        async function loadSelected() {{
          const path = String(select.value || "");
          const row = rows.find((x) => String(x.path || "") === path) || null;
          pre.textContent = "Loading log...";
          const txt = await loadTextFromPath(path);
          if (txt == null) {{
            pre.textContent =
              "Unable to load selected log file from this page context. " +
              "Try opening the report via a local HTTP server.";
            return;
          }}
          const maxChars = 2_000_000;
          if (txt.length > maxChars) {{
            pre.textContent = txt.slice(txt.length - maxChars);
          }} else {{
            pre.textContent = txt;
          }}
          if (meta) {{
            const size = row ? Number(row.size_bytes || 0) : 0;
            const mtime = row ? String(row.modified_utc || "") : "";
            const abs = row ? String(row.absolute_path || "") : "";
            const sizeText = Number.isFinite(size) ? `${{size}} bytes` : "";
            meta.textContent = [sizeText, mtime, abs].filter(Boolean).join("  ");
          }}
        }}

        btn.addEventListener("click", loadSelected);
        select.addEventListener("change", () => {{
          if (meta) meta.textContent = "";
        }});
        loadSelected();
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

      async function bootstrapTables() {{
        const detailTable = document.getElementById("detailTable");
        const detailBody = detailTable ? detailTable.querySelector("tbody") : null;
        const loadedPayload = await loadReportPayloadFromDisk();
        const activePayload = loadedPayload || embeddedPayload || null;
        if (activePayload && detailBody) {{
          populateMasterSectionsFromPayload(activePayload);
          const discrepancies = Array.isArray(activePayload.discrepancies) ? activePayload.discrepancies : [];
          responseData = buildResponseDataFromDiscrepancies(discrepancies);
          responseDataById = new Map(responseData.map((item) => [String(item.id), item]));
          const renderedRows = renderDetailRows(discrepancies);
          detailBody.innerHTML = renderedRows || "<tr><td colspan='{table_col_count}'>No discrepancies</td></tr>";
        }} else if (detailBody) {{
          renderMasterSectionsLoadError(
            "Unable to load summary JSON payload. Regenerate all_domains.results_summary.json or open via the dashboard server."
          );
          detailBody.innerHTML =
            "<tr><td colspan='{table_col_count}'>Unable to load summary JSON from disk at page load. " +
            "Open this report via a local web server or allow local-file XHR/fetch in your browser.</td></tr>";
        }}

        {inventory_script_html}
        {extractor_script_html}
        {log_viewer_script_html}
        setupTable("detailTable", "detailCount");
        enableResizableColumns("detailTable");
      }}

      bootstrapTables();
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
            baseline_values = {
                name: baseline_seed_value_for(route_group_param_meta(group, name)) for name in permutation
            }
            bm, baseline_url, bh, bb = build_fuzz_http_request(group, baseline_values)
            baseline_bf = _request_body_fingerprint(bb)
            baseline_request_key = make_request_key(
                phase="fuzz_baseline",
                host=group.host,
                path=group.path,
                permutation=permutation,
                mutated_parameter=None,
                mutated_value=None,
                url=baseline_url,
                http_method=bm,
                body_fingerprint=baseline_bf,
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
                    http_method=bm,
                    request_headers=bh,
                    request_body=bb,
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
                    baseline_response = perform_fozzy_http_request(
                        method=bm, url=baseline_url, headers=bh, body=bb, timeout=timeout
                    )
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
                                "http_method": bm,
                                "body_fingerprint": baseline_bf,
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
                    tm, trial_url, th, tb = build_fuzz_http_request(group, trial_values)
                    trial_bf = _request_body_fingerprint(tb)
                    trial_request_key = make_request_key(
                        phase="fuzz_mutation",
                        host=group.host,
                        path=group.path,
                        permutation=permutation,
                        mutated_parameter=name,
                        mutated_value=fuzz_value,
                        url=trial_url,
                        http_method=tm,
                        body_fingerprint=trial_bf,
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
                            http_method=tm,
                            request_headers=th,
                            request_body=tb,
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
                        trial_response = perform_fozzy_http_request(
                            method=tm, url=trial_url, headers=th, body=tb, timeout=timeout
                        )
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
                                    "http_method": tm,
                                    "body_fingerprint": trial_bf,
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

                    anomaly_signals: list[tuple[str, str]] = []
                    if response_differs(baseline_response, trial_response):
                        anomaly_signals.append(("status_or_size_change", "Response status or size differs from baseline"))
                    time_discrepancy, time_note = detect_response_time_discrepancy(
                        baseline_response,
                        trial_response,
                    )
                    if time_discrepancy:
                        anomaly_signals.append(("response_time_discrepancy", time_note))
                    header_change, header_note = detect_header_change(
                        baseline_response,
                        trial_response,
                    )
                    if header_change:
                        anomaly_signals.append(("header_change", header_note))

                    for anomaly_type, anomaly_note in anomaly_signals:
                        payload = {
                            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                            "result_type": "anomaly",
                            "anomaly_type": anomaly_type,
                            "anomaly_note": anomaly_note,
                            "host": group.host,
                            "path": group.path,
                            "http_method": tm,
                            "body_fingerprint": trial_bf,
                            "permutation": list(permutation),
                            "mutated_parameter": name,
                            "mutated_value": fuzz_value,
                            "requested_url": trial_url,
                            "baseline": baseline_response,
                            "anomaly": trial_response,
                        }
                        anomaly_path = results_dir / anomaly_file_name(
                            group.host,
                            group.path,
                            permutation,
                            name,
                            fuzz_value,
                            anomaly_type=anomaly_type,
                        )
                        anomaly_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
                        summary["anomalies"] += 1
                        summary["anomaly_entries"].append(
                            {
                                "result_type": "anomaly",
                                "anomaly_type": anomaly_type,
                                "anomaly_note": anomaly_note,
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
                            "http_method": tm,
                            "body_fingerprint": trial_bf,
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
    pending: list[str] = [os.fspath(root)]
    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as it:
                for entry in it:
                    try:
                        st = entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    if st.st_mtime_ns > max_ns:
                        max_ns = st.st_mtime_ns
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(entry.path)
                    except OSError:
                        continue
        except OSError:
            continue
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
    post_path = parameters_path.parent / f"{root_domain}.post.requests.json"
    if post_path.is_file():
        try:
            post_payload = read_json(post_path)
            post_groups = load_post_requests_route_groups(post_payload)
            if post_groups:
                groups = merge_route_groups_lists(groups, post_groups)
                print(f"Merged {len(post_groups)} POST route group(s) from: {post_path}", flush=True)
        except Exception as exc:
            print(f"Warning: could not load POST requests file {post_path}: {exc}", flush=True)
    if not groups:
        print("No parameterized routes were found in the input file.")
        return {"interrupted": False, "cancelled_by_user": False}

    print(f"Loaded {len(groups)} host/path/method groups from: {parameters_path}")
    print(f"Fozzy config: {cfg_display_path}")
    print(f"Output directory: {output_dir}")
    print(
        f"Fuzz parallelism: max_background_workers={max_background_workers}, "
        f"max_workers_per_domain={max_workers_per_domain} (concurrent jobs per eTLD+1), "
        f"max_workers_per_subdomain={max_workers_per_subdomain} (concurrent jobs per exact hostname)"
    )
    print("")

    host_listing: dict[str, list[tuple[str, str, int]]] = defaultdict(list)
    for group in groups:
        method = (group.http_method or "GET").strip().upper() or "GET"
        n_params = len(route_group_all_param_names(group))
        host_listing[group.host].append((method, group.path, n_params))

    print("Host -> [method] path parameter counts:")
    for host in sorted(host_listing.keys()):
        print(f"- {host}")
        for method, path, count in sorted(host_listing[host], key=lambda item: (item[0], item[1])):
            print(f"  [{method}] {path} ({count} parameters)")
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
                observed_sets=route_group_observed_sets_for_permutations(group),
                required=route_group_all_param_names(group),
                optional=set(),
                max_permutations=int(cfg["max_permutations"]),
            )
            permutations = select_max_parameter_permutations(generated_permutations)

            estimated_requests = estimate_group_request_count(
                permutations=permutations,
                quick_fuzz_values_count=len(quick_fuzz_values),
            )
            obs_n = len(route_group_observed_sets_for_permutations(group))
            print(
                f"[{(group.http_method or 'GET').upper()} {group.host}{group.path}] observed_valid={obs_n} "
                f"required={len(required)} optional={len(optional)} "
                f"permutations_generated={len(generated_permutations)} permutations_used={len(permutations)} "
                f"estimated_requests={estimated_requests}"
            )

            group_baseline_request_lines: list[str] = []
            for permutation in permutations:
                if not permutation:
                    continue
                permutations_lines.append(build_placeholder_request_summary(group, permutation))
                baseline_values = {
                    name: baseline_seed_value_for(route_group_param_meta(group, name)) for name in permutation
                }
                bm, baseline_u, _bh, baseline_b = build_fuzz_http_request(group, baseline_values)
                base_line = f"{bm} {baseline_u}"
                if baseline_b:
                    base_line += f"  body_sha256_16={_request_body_fingerprint(baseline_b)}"
                baseline_permutation_urls.append(base_line)
                group_baseline_request_lines.append(base_line)

            route_inventory.append(
                {
                    "host": group.host,
                    "path": group.path,
                    "scheme": group.scheme,
                    "http_method": (group.http_method or "GET").strip().upper() or "GET",
                    "body_content_type": group.body_content_type,
                    "parameter_count": len(route_group_all_param_names(group)),
                    "query_parameters": [
                        {"name": name, "data_type": group.params[name].data_type}
                        for name in sorted(group.params.keys())
                    ],
                    "body_parameters": [
                        {"name": name, "data_type": group.body_params[name].data_type}
                        for name in sorted(group.body_params.keys())
                    ],
                    "parameters": [
                        {"name": name, "data_type": route_group_param_meta(group, name).data_type, "location": "query" if name in group.params else "body"}
                        for name in sorted(route_group_all_param_names(group))
                    ],
                    "observed_valid_query_parameter_sets": [
                        sorted(list(item)) for item in sorted(group.observed_param_sets)
                    ],
                    "observed_valid_body_parameter_sets": [
                        sorted(list(item)) for item in sorted(group.observed_body_param_sets)
                    ],
                    "observed_valid_combined_parameter_sets": [
                        sorted(list(item)) for item in sorted(group.observed_combined_sets)
                    ],
                    "observed_valid_parameter_sets": [
                        sorted(list(item)) for item in sorted(route_group_observed_sets_for_permutations(group))
                    ],
                    "inferred_required_parameters": sorted(required),
                    "inferred_optional_parameters": sorted(optional),
                    "permutations_generated_count": len(generated_permutations),
                    "permutations_used_count": len(permutations),
                    "permutations": [list(item) for item in permutations],
                    "placeholder_request_summaries": [
                        build_placeholder_request_summary(group, item) for item in permutations if item
                    ],
                    "baseline_requests": group_baseline_request_lines,
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

    print("Finalizing artifacts (permutations, inventory, summaries)...", flush=True)

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

    print("Writing per-domain results summary...", flush=True)
    anomaly_summary_path, anomaly_summary_html_path = write_results_summary(
        root_domain=root_domain,
        parameters_path=parameters_path,
        output_dir=output_dir,
        results_dir=results_dir,
        interrupted=interrupted,
        interrupted_group=interrupted_group,
    )

    try:
        print(
            "Generating master results summary across output tree (can take longer on large datasets)...",
            flush=True,
        )
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
    except KeyboardInterrupt:
        interrupted = True
        interrupted_group = interrupted_group or "finalize_master_summary"
        print(
            "Interrupt received during master-results finalization; skipping master report and finishing with partial outputs.",
            flush=True,
        )
    except OSError as exc:
        print(f"Warning: could not write master results summary: {exc}", flush=True)
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


def run_incremental_domains(args: argparse.Namespace) -> dict[str, bool]:
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
        return {"interrupted": False}

    to_run: list[tuple[Path, Path]] = []
    if args.incremental_force:
        to_run = list(pairs)
    else:
        total = len(pairs)
        print(
            f"Incremental Fozzy: checking {total} domain folder(s) for changes under {scan_root}",
            flush=True,
        )
        for idx, (domain_dir, params_path) in enumerate(pairs, start=1):
            print(f"  [{idx}/{total}] {domain_dir.name}", flush=True)
            if domain_folder_is_dirty_for_incremental(domain_dir, state):
                to_run.append((domain_dir, params_path))

    if not to_run:
        print(
            f"No domain folders with new or changed files since the last Fozzy run under {scan_root}.\n"
            "Use --incremental-force to run every domain, or update files under a domain folder to trigger a run."
        )
        master_json, master_html, n_dom = write_master_results_summary(scan_root, prefer_nested=None)
        print(f"Master results summary JSON: {master_json}")
        print(f"Master results summary HTML: {master_html} ({n_dom} domain output tree(s))")
        return {"interrupted": False}

    cfg = build_effective_fozzy_config(args)
    domain_workers = max(1, int(cfg.get("incremental_domain_workers", cfg.get("max_background_workers", 1))))
    print(
        f"Incremental Fozzy: running {len(to_run)} domain(s) under {scan_root} "
        f"with {domain_workers} concurrent domain worker(s)"
    )
    if domain_workers > 1:
        print(
            "Each domain run is executed as an isolated child process to enable true multi-domain parallelism "
            "and avoid shared global-state contention."
        )

    if "domains" not in state or not isinstance(state["domains"], dict):
        state["domains"] = {}

    script_path = Path(__file__).resolve()
    pending: list[tuple[Path, Path]] = list(to_run)
    active: dict[str, tuple[subprocess.Popen, Path, Path]] = {}
    failed_domains: list[tuple[str, int]] = []
    interrupted = False

    def build_child_command(params_path: Path) -> list[str]:
        cmd = [sys.executable, str(script_path), str(params_path), "--config", str(args.fozzy_config), "--batch"]
        if args.timeout is not None:
            cmd += ["--timeout", str(args.timeout)]
        if args.delay is not None:
            cmd += ["--delay", str(args.delay)]
        if args.max_permutations is not None:
            cmd += ["--max-permutations", str(args.max_permutations)]
        if args.max_requests_per_endpoint is not None:
            cmd += ["--max-requests-per-endpoint", str(args.max_requests_per_endpoint)]
        if args.quick_fuzz_list is not None:
            cmd += ["--quick-fuzz-list", str(args.quick_fuzz_list)]
        if args.output_dir is not None:
            cmd += ["--output-dir", str(args.output_dir)]
        if args.dry_run:
            cmd.append("--dry-run")
        if args.live:
            cmd.append("--live")
        # Global incremental worker pool controls overall domain fan-out in this mode.
        # Keep each child domain runner single-worker to prevent N x M thread explosion.
        cmd += ["--max-background-workers", "1", "--max-workers-per-domain", "1", "--max-workers-per-subdomain", "1"]
        return cmd

    try:
        while pending or active:
            while pending and len(active) < domain_workers:
                domain_dir, params_path = pending.pop(0)
                print(f"\n=== {domain_dir.name} ===\nParameters: {params_path}", flush=True)
                child_cmd = build_child_command(params_path)
                proc = subprocess.Popen(child_cmd, cwd=str(FOZZY_BASE_DIR))
                active[domain_dir.name] = (proc, domain_dir, params_path)

            finished: list[str] = []
            for domain_name, (proc, domain_dir, params_path) in list(active.items()):
                code = proc.poll()
                if code is None:
                    continue
                finished.append(domain_name)
                if code == 0:
                    state["domains"][domain_dir.name] = {
                        "folder_max_mtime_ns": folder_tree_max_mtime_ns(domain_dir),
                        "last_completed_run_utc": datetime.now(timezone.utc).isoformat(),
                        "parameters_path": str(params_path),
                    }
                    save_incremental_state(state_path, state)
                else:
                    failed_domains.append((domain_dir.name, int(code)))
                    print(
                        f"Warning: domain run failed for {domain_dir.name} (exit={code}). "
                        "Incremental state was not updated for this domain.",
                        flush=True,
                    )
            for domain_name in finished:
                active.pop(domain_name, None)

            if active:
                time.sleep(0.2)
    except KeyboardInterrupt:
        interrupted = True
        print("\nInterrupt received; terminating active domain workers...", flush=True)
        for proc, _domain_dir, _params_path in active.values():
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
        for proc, _domain_dir, _params_path in active.values():
            try:
                proc.wait(timeout=5.0)
            except Exception:
                pass

    if failed_domains:
        print("\nIncremental failures:", flush=True)
        for domain_name, code in failed_domains:
            print(f"  - {domain_name}: exit={code}", flush=True)
    if interrupted:
        print("Incremental run interrupted; some domains may have partial outputs.", flush=True)

    master_json, master_html, n_dom = write_master_results_summary(scan_root, prefer_nested=None)
    print(f"\nMaster results summary JSON: {master_json}")
    print(f"Master results summary HTML: {master_html} ({n_dom} domain output tree(s))")
    return {"interrupted": interrupted}


def main() -> None:
    args = parse_args()
    cfg_for_log = build_effective_fozzy_config(args)
    explicit_log_path = _resolve_optional_log_file_path(cfg_for_log.get("log_file"))
    selected_log_path = explicit_log_path or resolve_default_fozzy_log_path(args, cfg_hint=cfg_for_log)
    install_fozzy_log_tee(selected_log_path)

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
        incremental_result = run_incremental_domains(args)
        if bool(incremental_result.get("interrupted")):
            raise SystemExit(130)
        return
    single_result = run_fozzy_for_parameters(Path(args.parameters_file).resolve(), args)
    if bool(single_result.get("interrupted")) or bool(single_result.get("cancelled_by_user")):
        raise SystemExit(130)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nFozzy interrupted by user (Ctrl+C). Partial outputs may have been saved.", flush=True)
        raise SystemExit(130)
