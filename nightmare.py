#!/usr/bin/env python3
"""Domain crawler + AI-assisted sitemap analyzer.

Runs entirely on your machine: by default all artifacts go under ./output/<domain>/.
Each crawl also maintains ``output/<domain>/collected_data/summary.json``, a single JSON rollup of
everything under ``collected_data/`` (forms, endpoints without raw base64 bodies, cookies, scripts, etc.).
There is no integration with the central coordinator or any fleet API — that lives in
coordinator.py (which simply subprocesses this script with a URL and --resume).

Standalone local usage (same idea as a local CLI tool such as nmap):

    python nightmare.py https://example.com --standalone
    python nightmare.py https://example.com --config nightmare.standalone.json

Optional external calls: target websites (the crawl), and OpenAI if you enable AI stages
and set OPENAI_API_KEY (--no-ai disables that).

Batch mode (no URL argument): parallel local subprocesses over a targets file:

    python nightmare.py --batch-workers 4
"""

from __future__ import annotations

import argparse
import base64
import ctypes
import gzip
import hashlib
import html
import json
import logging
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

import scrapy
from openai import APIError, APITimeoutError, BadRequestError, OpenAI, RateLimitError
from scrapy.crawler import CrawlerProcess


try:
    import tldextract

    _TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=None)
except Exception:  # pragma: no cover - optional dependency fallback
    tldextract = None
    _TLD_EXTRACT = None


DEFAULT_NOT_FOUND_STATUSES = {404, 410}
DEFAULT_SOFT_404_SMALL_BODY_MAX_BYTES = 64 * 1024
DEFAULT_SOFT_404_HEAD_SCAN_MAX_BYTES = 8192
DEFAULT_SOFT_404_BODY_SCAN_MAX_BYTES = 64 * 1024
DEFAULT_SOFT_404_TITLE_PATTERNS = [
    r"<title[^>]*>[^<]*(?:404|not found|page not found|does not exist|cannot be found|no such)[^<]*</title>",
]
DEFAULT_SOFT_404_PHRASES = [
    "page not found",
    "does not exist",
    "cannot be found",
    "no such page",
    "not found",
    "the page you requested could not be found",
    "the requested url was not found",
    "this page is unavailable",
    "the page you are looking for doesn't exist or has been moved",
]
DEFAULT_CLOUDFLARE_BLOCK_TITLE_PHRASE = "attention required! | cloudflare"
DEFAULT_CLOUDFLARE_BLOCK_BODY_PHRASE = "sorry, you have been blocked"
BASE_DIR = Path(__file__).resolve().parent
FILE_PATH_WORDLIST_PATH = BASE_DIR / "resources" / "wordlists" / "file_path_list.txt"
FILE_PATH_WORDLIST_DISCOVERED_FROM = "resources/wordlists/file_path_list.txt"
CONFIG_DIR = BASE_DIR / "config"
OUTPUT_DIR = BASE_DIR / "output"
HELP_CONFIG_PATH = CONFIG_DIR / "nightmare.help.json"
MESSAGE_CONFIG_PATH = CONFIG_DIR / "nightmare.messages.json"
PROMPT_CONFIG_PATH = CONFIG_DIR / "nightmare.prompts.json"
TEMPLATE_DIR = CONFIG_DIR / "templates"
STATIC_ASSET_EXTENSIONS: set[str] = set()
AI_EXCLUDED_URL_EXTENSIONS: set[str] = set()
_EXTENSION_CONFIG_LOADED = False
HELP_TEXTS: dict[str, Any] = {}
MESSAGE_TEMPLATES: dict[str, Any] = {}
PROMPT_TEMPLATES: dict[str, Any] = {}
HTML_TEMPLATES: dict[str, str] = {}
_STRING_RESOURCES_LOADED = False
_NONINTERACTIVE_STDIN_HANDLE: Any = None
PAGE_EXISTENCE_CRITERIA_CONFIG_PATH = CONFIG_DIR / "page_existence_criteria_config.json"
PAGE_EXISTENCE_CRITERIA: dict[str, Any] = {}
_PAGE_EXISTENCE_CRITERIA_LOADED = False
OPENAI_PROMPT_BUDGET_STEPS = [180000, 120000, 80000, 50000, 30000]
DEFAULT_OPENAI_REQUEST_TIMEOUT_SECONDS = 90.0
DEFAULT_EVIDENCE_BODY_MAX_BYTES = 4096
# urllib probe: never read unbounded bodies (large downloads otherwise look like a hang).
HTTP_PROBE_BODY_READ_MAX = DEFAULT_EVIDENCE_BODY_MAX_BYTES + 1
# Above this response size, skip expensive selectors (e.g. every non-link href, all src,
# regex over HTML, and scanning every URL-like attribute). Huge SPAs otherwise look "frozen"
# for minutes while lxml walks millions of attributes.
LINK_EXTRACTION_FULL_MAX_BYTES = 2 * 1024 * 1024
# Cap how much HTML we regex-scan for quoted paths/URLs (inline JS can be multi‑MB).
MAX_EMBEDDED_REGEX_SCAN_BYTES = 512 * 1024
# XPath union: only attributes that commonly carry URLs (never use //@* on large documents).
_URL_ATTRIBUTE_XPATH = (
    "//@href|//@src|//@poster|//@cite|//@data-src|//@data-href|//@data-url|//@data-link|"
    "//@data-action|//@data-background|//@data-full-url|//@data-uri"
)
EVIDENCE_FILE_EXTENSION = ".json.gz"
EVIDENCE_GZIP_COMPRESSLEVEL = 9
DEV_ENV_NAMES = {"dev", "development", "local", "test"}
DEV_TIMING_TOP_N_DEFAULT = 20
_DEV_TIMING_ENABLED = False
_DEV_TIMING_LOG_EACH_CALL = False
_DEV_TIMING_LOCK = threading.Lock()
_DEV_TIMING_STATS: dict[str, dict[str, float | int]] = {}
_DEV_TIMING_CALLS: list[dict[str, Any]] = []


def get_root_domain(hostname: str) -> str:
    hostname = (hostname or "").lower().strip(".")
    if not hostname:
        return ""

    if _TLD_EXTRACT is not None:
        extracted = _TLD_EXTRACT(hostname)
        if extracted.domain and extracted.suffix:
            return f"{extracted.domain}.{extracted.suffix}".lower()

    parts = hostname.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:]).lower()
    return hostname


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    normalized = parsed._replace(scheme=scheme, netloc=netloc, fragment="", path=path)

    if normalized.path != "/":
        normalized = normalized._replace(path=normalized.path.rstrip("/"))

    return urlunparse(normalized)


def coerce_http_https_start_url(raw: str) -> str:
    """Return an absolute http(s) URL. Bare hostnames (e.g. example.com) become https://…"""
    text = (raw or "").strip()
    if not text:
        return text
    parsed = urlparse(text)
    if parsed.scheme in {"http", "https"}:
        if parsed.hostname:
            return text
        raise ValueError("url must include a hostname (e.g. https://example.com/)")
    if parsed.scheme:
        raise ValueError(f"url must use http or https scheme (got {parsed.scheme!r})")
    if text.startswith("//"):
        return f"https:{text}"
    return f"https://{text}"


def infer_query_value_type(value: str) -> str:
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


def normalize_url_for_sitemap(url: str) -> str:
    normalized = normalize_url(url)
    parsed = urlparse(normalized)
    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    if not query_items:
        return normalized

    key_to_types: dict[str, set[str]] = defaultdict(set)
    for key, value in query_items:
        key_to_types[key].add(infer_query_value_type(value))

    normalized_query_items: list[tuple[str, str]] = []
    for key in sorted(key_to_types.keys()):
        sorted_types = sorted(key_to_types[key])
        if len(sorted_types) == 1:
            type_token = sorted_types[0]
        else:
            type_token = "mixed_" + "_".join(sorted_types)
        normalized_query_items.append((key, f"__{type_token}__"))

    normalized_query = urlencode(normalized_query_items, doseq=True)
    return urlunparse(parsed._replace(query=normalized_query))


def normalize_path_segment_for_condensed_tree(segment: str) -> str:
    text = (segment or "").strip()
    if not text:
        return text

    if re.fullmatch(r"[+-]?\d+", text):
        return "{int}"
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}", text):
        return "{uuid}"
    if re.fullmatch(r"[0-9a-fA-F]{10,}", text):
        return "{hex}"
    if len(text) >= 12 and re.fullmatch(r"[A-Za-z0-9_-]+", text) and any(char.isdigit() for char in text):
        return "{token}"

    return text


def is_allowed_domain(url: str, root_domain: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False

    hostname = (parsed.hostname or "").lower()
    return hostname == root_domain or hostname.endswith(f".{root_domain}")


def should_crawl_url(url: str) -> bool:
    ensure_extension_config_loaded()
    parsed = urlparse(url)
    path = parsed.path or "/"

    if path.endswith("/"):
        return True

    filename = path.rsplit("/", 1)[-1]
    if "." not in filename:
        return True

    extension = f".{filename.rsplit('.', 1)[-1].lower()}"
    return extension not in STATIC_ASSET_EXTENSIONS


def should_include_url_in_ai_payload(url: str) -> bool:
    ensure_extension_config_loaded()
    parsed = urlparse(url)
    path = parsed.path or "/"
    if path.endswith("/"):
        return True

    filename = path.rsplit("/", 1)[-1]
    if "." not in filename:
        return True

    extension = f".{filename.rsplit('.', 1)[-1].lower()}"
    return extension not in AI_EXCLUDED_URL_EXTENSIONS


def filter_urls_for_ai(urls: list[str]) -> list[str]:
    return [
        normalize_url(url)
        for url in urls
        if isinstance(url, str) and url.strip() and should_include_url_in_ai_payload(url)
    ]


def should_include_url_in_source_of_truth(url: str) -> bool:
    ensure_extension_config_loaded()
    parsed = urlparse(url)
    path = parsed.path or "/"
    if path.endswith("/"):
        return True

    filename = path.rsplit("/", 1)[-1]
    if "." not in filename:
        return True

    extension = f".{filename.rsplit('.', 1)[-1].lower()}"
    if extension == ".json":
        return True
    return extension not in STATIC_ASSET_EXTENSIONS


def filter_urls_for_source_of_truth(urls: list[str]) -> list[str]:
    return [
        normalize_url(url)
        for url in urls
        if isinstance(url, str) and url.strip() and should_include_url_in_source_of_truth(url)
    ]


def detect_soft_not_found_response(
    status_code: int | None,
    response_body: bytes | bytearray | None,
) -> tuple[bool, str | None]:
    """Heuristic detection for soft-404 pages that return HTTP 200."""
    ensure_page_existence_criteria_loaded()
    criteria = PAGE_EXISTENCE_CRITERIA or default_page_existence_criteria()
    if not bool(criteria.get("enabled", True)):
        return False, None
    if status_code != 200:
        return False, None
    if not response_body:
        return False, None

    body_bytes = bytes(response_body)
    head_scan_max_bytes = int(criteria.get("soft_404_head_scan_max_bytes", DEFAULT_SOFT_404_HEAD_SCAN_MAX_BYTES))
    body_scan_max_bytes = int(criteria.get("soft_404_body_scan_max_bytes", DEFAULT_SOFT_404_BODY_SCAN_MAX_BYTES))
    small_body_max_bytes = int(
        criteria.get("soft_404_small_body_max_bytes", DEFAULT_SOFT_404_SMALL_BODY_MAX_BYTES)
    )
    title_patterns = criteria.get("soft_404_title_patterns", DEFAULT_SOFT_404_TITLE_PATTERNS)
    body_phrases = criteria.get("soft_404_body_phrases", DEFAULT_SOFT_404_PHRASES)
    body_regexes = criteria.get("soft_404_body_regexes", [])
    cloudflare_title_phrase = str(
        criteria.get("cloudflare_block_title_phrase", DEFAULT_CLOUDFLARE_BLOCK_TITLE_PHRASE)
    ).strip().lower()
    cloudflare_body_phrase = str(
        criteria.get("cloudflare_block_body_phrase", DEFAULT_CLOUDFLARE_BLOCK_BODY_PHRASE)
    ).strip().lower()

    head_text = body_bytes[:head_scan_max_bytes].decode("utf-8", errors="replace")
    body_text = body_bytes[:body_scan_max_bytes].decode("utf-8", errors="replace")
    head_lower = head_text.lower()
    body_lower = body_text.lower()
    compact = re.sub(r"\s+", " ", body_lower)

    # Cloudflare block pages are never valid pages, even when returned with non-404 status codes.
    if cloudflare_title_phrase and cloudflare_body_phrase:
        title_found = cloudflare_title_phrase in re.sub(r"\s+", " ", head_lower)
        blocked_found = cloudflare_body_phrase in compact
        if title_found and blocked_found:
            return True, (
                "cloudflare block page detected "
                f"(title contains '{cloudflare_title_phrase}' and body contains '{cloudflare_body_phrase}')"
            )

    for pattern in title_patterns:
        try:
            if re.search(str(pattern), head_text, flags=re.IGNORECASE):
                return True, f"soft-404 marker in HTML title matched regex '{pattern}'"
        except re.error:
            continue

    for phrase in body_phrases:
        phrase_text = str(phrase).strip().lower()
        if phrase_text and phrase_text in compact and len(body_bytes) <= small_body_max_bytes:
            return True, f"soft-404 phrase '{phrase}' in small 200 response ({len(body_bytes)} bytes)"

    for pattern in body_regexes:
        try:
            if re.search(str(pattern), compact, flags=re.IGNORECASE) and len(body_bytes) <= small_body_max_bytes:
                return True, f"soft-404 body regex '{pattern}' in small 200 response ({len(body_bytes)} bytes)"
        except re.error:
            continue

    return False, None


def is_effective_existing_record(record: UrlInventoryRecord | None) -> bool:
    if record is None:
        return False
    if bool(getattr(record, "soft_404_detected", False)):
        return False
    not_found_statuses = configured_not_found_statuses()
    for raw_status in (getattr(record, "crawl_status_code", None), getattr(record, "existence_status_code", None)):
        try:
            code = int(raw_status) if raw_status is not None else None
        except (TypeError, ValueError):
            code = None
        if code is not None and code in not_found_statuses:
            return False
    crawl_note = str(getattr(record, "crawl_note", "") or "").lower()
    existence_note = str(getattr(record, "existence_check_note", "") or "").lower()
    if "soft-404" in crawl_note or "soft-404" in existence_note:
        return False
    return True


def _looks_like_hashed_bundle_filename(filename: str) -> bool:
    lowered = (filename or "").strip().lower()
    if not lowered:
        return False
    return bool(
        re.fullmatch(r"(?:[a-z0-9_-]+|\d+)(?:[.-][0-9a-f]{8,})+\.(?:js|mjs|map)$", lowered)
        or re.fullmatch(r"[0-9a-f]{8,}\.(?:js|mjs|map)$", lowered)
    )


def summarize_url_for_ai(url: str) -> str:
    normalized = normalize_url(url)
    parsed = urlparse(normalized)
    path = parsed.path or "/"
    filename = path.rsplit("/", 1)[-1]
    if filename and _looks_like_hashed_bundle_filename(filename):
        replaced_path = f"{path.rsplit('/', 1)[0]}/{{hashed_bundle}}.{filename.rsplit('.', 1)[-1].lower()}"
        if path.count("/") == 1:
            replaced_path = f"/{{hashed_bundle}}.{filename.rsplit('.', 1)[-1].lower()}"
        return urlunparse(parsed._replace(path=replaced_path))
    return normalized


def condense_urls_for_ai(urls: list[str]) -> list[str]:
    condensed: list[str] = []
    seen: set[str] = set()
    for raw_url in sorted(set(filter_urls_for_ai(urls))):
        summarized = summarize_url_for_ai(raw_url)
        if summarized in seen:
            continue
        seen.add(summarized)
        condensed.append(summarized)
    return condensed


def bytes_to_b64(value: bytes) -> str:
    if not value:
        return ""
    return base64.b64encode(value).decode("ascii")


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def force_noninteractive_stdin() -> None:
    """Prevent third-party runtime components from blocking on console stdin."""
    global _NONINTERACTIVE_STDIN_HANDLE
    if _NONINTERACTIVE_STDIN_HANDLE is not None:
        return
    try:
        stdin_handle = open(os.devnull, "r", encoding="utf-8", errors="ignore")
    except Exception:
        return
    _NONINTERACTIVE_STDIN_HANDLE = stdin_handle
    try:
        sys.stdin = stdin_handle
    except Exception:
        return


def restore_default_interrupt_handlers() -> None:
    """Reinstall Python's default Ctrl+C handling.

    The crawl temporarily installs its own signal bridge so Ctrl+C can stop the Scrapy/Twisted
    reactor on Windows without leaving modified handlers behind. Resetting SIGINT (and SIGBREAK
    on Windows) after ``process.start`` returns keeps later phases and further interrupts
    behaving normally.
    """
    try:
        signal.signal(signal.SIGINT, signal.default_int_handler)
    except (ValueError, OSError):
        pass
    sigbreak = getattr(signal, "SIGBREAK", None)
    if sigbreak is not None:
        try:
            signal.signal(sigbreak, signal.SIG_DFL)
        except (ValueError, OSError):
            pass
    if os.name == "posix" and hasattr(signal, "siginterrupt"):
        try:
            signal.siginterrupt(signal.SIGINT, True)
        except (ValueError, OSError):
            pass


@dataclass
class CrawlInterruptBridge:
    interrupt_event: threading.Event = field(default_factory=threading.Event)
    caught_signal: int | None = None
    _stop_event: threading.Event = field(default_factory=threading.Event)
    _monitor_thread: threading.Thread | None = None
    _previous_handlers: dict[int, Any] = field(default_factory=dict)

    def install(self) -> None:
        handled_signals = [signal.SIGINT]
        sigbreak = getattr(signal, "SIGBREAK", None)
        if sigbreak is not None:
            handled_signals.append(sigbreak)

        def _handle_interrupt(signum, _frame) -> None:
            if self.caught_signal is None:
                self.caught_signal = int(signum)
                self.interrupt_event.set()
                signal_name = getattr(signal.Signals(signum), "name", str(signum))
                print(
                    f"Interrupt received ({signal_name}); stopping crawl and finalizing available artifacts...",
                    file=sys.stderr,
                    flush=True,
                )
                return
            raise KeyboardInterrupt

        for sig in handled_signals:
            try:
                self._previous_handlers[sig] = signal.getsignal(sig)
                signal.signal(sig, _handle_interrupt)
            except (ValueError, OSError):
                continue

        self._monitor_thread = threading.Thread(target=self._monitor_reactor_stop, daemon=True, name="crawl-interrupt")
        self._monitor_thread.start()

    def cleanup(self) -> None:
        self._stop_event.set()
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=1.0)
        for sig, previous in self._previous_handlers.items():
            try:
                signal.signal(sig, previous)
            except (ValueError, OSError):
                continue
        self._previous_handlers.clear()

    def _monitor_reactor_stop(self) -> None:
        while not self._stop_event.is_set():
            if self.interrupt_event.is_set():
                try:
                    from twisted.internet import reactor as twisted_reactor
                except Exception:
                    interruptible_sleep(0.1)
                    continue

                if getattr(twisted_reactor, "running", False):
                    try:
                        twisted_reactor.callFromThread(self._crash_reactor_if_running)
                    except Exception:
                        self._crash_reactor_if_running()
                    return
            interruptible_sleep(0.1)

    @staticmethod
    def _crash_reactor_if_running() -> None:
        try:
            from twisted.internet import reactor as twisted_reactor
        except Exception:
            return
        try:
            if getattr(twisted_reactor, "running", False):
                twisted_reactor.crash()
        except Exception:
            return


def interruptible_sleep(seconds: float, chunk_seconds: float = 0.25) -> None:
    """Sleep in short slices so :exc:`KeyboardInterrupt` can land between blocking sleeps."""
    if seconds <= 0.0:
        return
    deadline = time.monotonic() + float(seconds)
    chunk = max(0.05, float(chunk_seconds))
    while True:
        now = time.monotonic()
        if now >= deadline:
            return
        time.sleep(min(chunk, deadline - now))


def _raise_keyboard_interrupt_if_reactor_stopped_by_signal() -> None:
    """If the Twisted reactor exited due to SIGINT/SIGTERM/SIGBREAK, surface that as :exc:`KeyboardInterrupt`.

    With Twisted signal handlers enabled, Ctrl+C stops the reactor without raising
    :exc:`KeyboardInterrupt` inside ``process.start``. Twisted records the signal in
    ``reactor._exitSignal`` so we can match the script's existing interrupt handling.
    """
    try:
        from twisted.internet import reactor as twisted_reactor
    except Exception:
        return
    if getattr(twisted_reactor, "_exitSignal", None) is None:
        return
    raise KeyboardInterrupt


def stop_twisted_threadpool() -> None:
    """Stop Twisted's reactor threadpool so interrupted runs can exit cleanly."""
    try:
        from twisted.internet import reactor as twisted_reactor
    except Exception:
        return

    threadpool = getattr(twisted_reactor, "threadpool", None)
    if threadpool is None:
        get_threadpool = getattr(twisted_reactor, "getThreadPool", None)
        if callable(get_threadpool):
            try:
                threadpool = get_threadpool()
            except Exception:
                threadpool = None
    if threadpool is None:
        return

    stop = getattr(threadpool, "stop", None)
    if callable(stop):
        try:
            stop()
        except Exception:
            return


def force_process_exit(exit_code: int) -> None:
    try:
        sys.stdout.flush()
    except Exception:
        pass
    try:
        sys.stderr.flush()
    except Exception:
        pass
    try:
        logging.shutdown()
    finally:
        os._exit(int(exit_code))


def start_crawl_wait_monitor(
    progress: ProgressReporter | None,
    state: CrawlState,
    initial_visited_count: int,
    interrupt_event: threading.Event | None = None,
    interval_seconds: float = 5.0,
) -> tuple[threading.Event, threading.Thread] | None:
    if progress is None:
        return None

    stop_event = threading.Event()

    def _run() -> None:
        started_at = time.monotonic()
        while not stop_event.wait(interval_seconds):
            if interrupt_event is not None and interrupt_event.is_set():
                return
            if len(state.visited_urls) > initial_visited_count:
                return
            elapsed = time.monotonic() - started_at
            progress.info(
                "Still waiting for the first crawl response "
                f"({elapsed:.0f}s elapsed); the target may be slow, timing out, or retrying."
            )

    thread = threading.Thread(target=_run, daemon=True, name="crawl-wait-monitor")
    thread.start()
    return stop_event, thread


def to_json_compatible(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, bytes):
        return bytes_to_b64(value)

    if isinstance(value, dict):
        return {str(key): to_json_compatible(inner_value) for key, inner_value in value.items()}

    if hasattr(value, "items"):
        return {str(key): to_json_compatible(inner_value) for key, inner_value in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [to_json_compatible(item) for item in value]

    return str(value)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(to_json_compatible(payload), indent=2, ensure_ascii=False), encoding="utf-8")


def write_json_gzip(path: Path, payload: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=EVIDENCE_GZIP_COMPRESSLEVEL) as handle:
        json.dump(to_json_compatible(payload), handle, ensure_ascii=False, separators=(",", ":"))


def compact_text_for_evidence(text: str, max_chars: int = 4000) -> dict[str, Any]:
    value = text or ""
    preview = value[:max_chars]
    return {
        "preview": preview,
        "preview_truncated": len(value) > len(preview),
        "original_char_count": len(value),
        "sha256": hashlib.sha256(value.encode("utf-8")).hexdigest() if value else "",
    }


def encode_body_for_evidence(body: bytes, max_bytes: int = DEFAULT_EVIDENCE_BODY_MAX_BYTES) -> dict[str, Any]:
    raw = body or b""
    original_size = len(raw)
    if original_size <= max_bytes:
        return {
            "body_base64": bytes_to_b64(raw),
            "body_size_bytes": original_size,
            "body_truncated": False,
        }

    truncated = raw[:max_bytes]
    return {
        "body_base64": bytes_to_b64(truncated),
        "body_size_bytes": original_size,
        "stored_body_size_bytes": len(truncated),
        "body_truncated": True,
        "body_sha256": hashlib.sha256(raw).hexdigest(),
    }


def read_http_body_capped(response: Any, max_bytes: int = HTTP_PROBE_BODY_READ_MAX) -> bytes:
    """Read at most max_bytes from a urllib response (or HTTPError) to avoid blocking on huge bodies."""
    read_fn = getattr(response, "read", None)
    if read_fn is None:
        return b""
    chunk = read_fn(max_bytes)
    return chunk if isinstance(chunk, (bytes, bytearray)) else bytes(chunk)


def read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(parsed, dict):
        raise ValueError(f"Configuration file must contain a JSON object: {path}")
    return parsed


def read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig")


def resolve_config_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0].lower() == "config":
        return BASE_DIR / path
    return CONFIG_DIR / path


def resolve_output_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0].lower() == "output":
        return BASE_DIR / path
    return OUTPUT_DIR / path


def resolve_wordlist_path(path_value: str) -> Path:
    """Resolve crawl wordlist paths relative to repo root by default.

    Bare filenames fall back to ``resources/wordlists`` for convenience.
    """
    path = Path(path_value)
    if path.is_absolute():
        return path
    repo_relative = BASE_DIR / path
    if repo_relative.exists():
        return repo_relative
    if len(path.parts) == 1:
        return BASE_DIR / "resources" / "wordlists" / path
    return repo_relative


def format_repo_relative_path(path: Path) -> str:
    resolved = path
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    try:
        return resolved.relative_to(BASE_DIR).as_posix()
    except ValueError:
        return resolved.as_posix()


def merged_value(cli_value: Any, config: dict[str, Any], key: str, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if key in config:
        return config[key]
    return default


def _is_development_environment(config: dict[str, Any]) -> bool:
    config_env = str(config.get("environment", "") or "").strip().lower()
    env_env = str(
        os.getenv("NIGHTMARE_ENV")
        or os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or ""
    ).strip().lower()
    effective = env_env or config_env
    return effective in DEV_ENV_NAMES


def _is_truthy_env_flag(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes", "on", "y"}


def configure_dev_timing(config: dict[str, Any]) -> bool:
    global _DEV_TIMING_ENABLED, _DEV_TIMING_LOG_EACH_CALL, _DEV_TIMING_STATS, _DEV_TIMING_CALLS
    dev_env = _is_development_environment(config)
    configured = config.get("dev_timing_logging")
    enabled = dev_env if configured is None else bool(configured)
    env_override = os.getenv("NIGHTMARE_DEV_TIMING")
    if env_override is not None:
        enabled = _is_truthy_env_flag(env_override)
    each_call = bool(config.get("dev_timing_log_each_call", False))
    env_each_call = os.getenv("NIGHTMARE_DEV_TIMING_EACH_CALL")
    if env_each_call is not None:
        each_call = _is_truthy_env_flag(env_each_call)
    _DEV_TIMING_ENABLED = bool(enabled)
    _DEV_TIMING_LOG_EACH_CALL = bool(each_call and _DEV_TIMING_ENABLED)
    with _DEV_TIMING_LOCK:
        _DEV_TIMING_STATS = {}
        _DEV_TIMING_CALLS = []
    return _DEV_TIMING_ENABLED


def _emit_dev_perf_line(message: str, progress: "ProgressReporter | None" = None) -> None:
    if progress is not None:
        progress.info(message)
        return
    logger = logging.getLogger("nightmare")
    if logger.handlers:
        logger.info(message)
    else:
        print(message, flush=True)


def _record_dev_timing(name: str, elapsed_seconds: float) -> None:
    if not _DEV_TIMING_ENABLED:
        return
    elapsed = max(0.0, float(elapsed_seconds))
    with _DEV_TIMING_LOCK:
        stats = _DEV_TIMING_STATS.get(name)
        if stats is None:
            stats = {"count": 0, "total_s": 0.0, "max_s": 0.0}
            _DEV_TIMING_STATS[name] = stats
        stats["count"] = int(stats["count"]) + 1
        stats["total_s"] = float(stats["total_s"]) + elapsed
        stats["max_s"] = max(float(stats["max_s"]), elapsed)
        _DEV_TIMING_CALLS.append(
            {
                "name": name,
                "elapsed_s": elapsed,
            }
        )


@contextmanager
def dev_timed_call(name: str, progress: "ProgressReporter | None" = None):
    started = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - started
        _record_dev_timing(name, elapsed)
        if _DEV_TIMING_ENABLED and _DEV_TIMING_LOG_EACH_CALL:
            _emit_dev_perf_line(f"[dev-perf] {name}: {elapsed:.3f}s", progress)


def emit_dev_timing_summary(progress: "ProgressReporter | None" = None, *, top_n: int = DEV_TIMING_TOP_N_DEFAULT) -> None:
    if not _DEV_TIMING_ENABLED:
        return
    with _DEV_TIMING_LOCK:
        stats_items = [(name, dict(values)) for name, values in _DEV_TIMING_STATS.items()]
        call_items = list(_DEV_TIMING_CALLS)
    if not stats_items:
        _emit_dev_perf_line("[dev-perf] no timed calls were recorded.", progress)
        return
    total_calls = sum(int(item[1].get("count", 0) or 0) for item in stats_items)
    total_time = sum(float(item[1].get("total_s", 0.0) or 0.0) for item in stats_items)
    _emit_dev_perf_line(
        f"[dev-perf] captured {total_calls} timed calls across {len(stats_items)} methods; cumulative={total_time:.3f}s",
        progress,
    )
    limit = max(1, int(top_n))
    by_total = sorted(stats_items, key=lambda item: float(item[1].get("total_s", 0.0) or 0.0), reverse=True)[:limit]
    for name, values in by_total:
        count = int(values.get("count", 0) or 0)
        total_s = float(values.get("total_s", 0.0) or 0.0)
        max_s = float(values.get("max_s", 0.0) or 0.0)
        avg_s = (total_s / count) if count else 0.0
        _emit_dev_perf_line(
            f"[dev-perf] method={name} calls={count} total={total_s:.3f}s avg={avg_s:.3f}s max={max_s:.3f}s",
            progress,
        )
    slowest_calls = sorted(call_items, key=lambda row: float(row.get("elapsed_s", 0.0) or 0.0), reverse=True)[:limit]
    for row in slowest_calls:
        _emit_dev_perf_line(
            f"[dev-perf] slow-call method={row.get('name')} elapsed={float(row.get('elapsed_s', 0.0) or 0.0):.3f}s",
            progress,
        )


def _safe_int_from_any(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _compute_worker_affinity_mask(cpu_count: int, desired_cores: int | None) -> int:
    total = max(1, int(cpu_count))
    if desired_cores is None:
        worker_cores = max(1, total // 2)
        if total > 1:
            worker_cores = min(worker_cores, total - 1)
    else:
        worker_cores = max(1, min(int(desired_cores), total))
    mask = 0
    for idx in range(worker_cores):
        mask |= (1 << idx)
    return mask if mask > 0 else 1


def _apply_windows_affinity_mask(mask: int) -> bool:
    if os.name != "nt":
        return False
    if mask <= 0:
        return False
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    except Exception:
        return False
    get_current_process = getattr(kernel32, "GetCurrentProcess", None)
    set_affinity = getattr(kernel32, "SetProcessAffinityMask", None)
    if get_current_process is None or set_affinity is None:
        return False
    try:
        current = get_current_process()
        ok = bool(set_affinity(current, ctypes.c_size_t(int(mask))))
    except Exception:
        return False
    return ok


def apply_worker_priority_hints_from_env() -> None:
    """Apply worker niceness/affinity hints when running as a spawned batch worker."""
    if os.name == "nt":
        mask_raw = os.getenv("NIGHTMARE_WORKER_AFFINITY_MASK")
        if mask_raw:
            try:
                mask = int(mask_raw, 0)
            except ValueError:
                mask = 0
            if mask > 0:
                _apply_windows_affinity_mask(mask)
        return

    nice_raw = os.getenv("NIGHTMARE_WORKER_NICE")
    if not nice_raw:
        return
    try:
        nice_delta = max(0, _safe_int_from_any(nice_raw, 10))
    except Exception:
        nice_delta = 10
    if nice_delta <= 0:
        return
    try:
        os.nice(nice_delta)
    except OSError:
        return


def optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Expected string value for path configuration")
    trimmed = value.strip()
    return trimmed or None


def normalize_extension_config(value: Any, field_name: str) -> set[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list of file extensions")

    normalized: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"{field_name} must contain only string file extensions")
        extension = item.strip().lower()
        if not extension:
            continue
        if not extension.startswith("."):
            extension = f".{extension}"
        normalized.add(extension)
    return normalized


def apply_extension_config(config: dict[str, Any]) -> None:
    global STATIC_ASSET_EXTENSIONS, AI_EXCLUDED_URL_EXTENSIONS, _EXTENSION_CONFIG_LOADED
    STATIC_ASSET_EXTENSIONS = normalize_extension_config(config.get("static_asset_extensions", []), "static_asset_extensions")
    AI_EXCLUDED_URL_EXTENSIONS = normalize_extension_config(
        config.get("ai_excluded_url_extensions", []),
        "ai_excluded_url_extensions",
    )
    _EXTENSION_CONFIG_LOADED = True


def default_page_existence_criteria() -> dict[str, Any]:
    return {
        "enabled": True,
        "not_found_statuses": sorted(DEFAULT_NOT_FOUND_STATUSES),
        "soft_404_small_body_max_bytes": DEFAULT_SOFT_404_SMALL_BODY_MAX_BYTES,
        "soft_404_head_scan_max_bytes": DEFAULT_SOFT_404_HEAD_SCAN_MAX_BYTES,
        "soft_404_body_scan_max_bytes": DEFAULT_SOFT_404_BODY_SCAN_MAX_BYTES,
        "soft_404_title_patterns": list(DEFAULT_SOFT_404_TITLE_PATTERNS),
        "soft_404_body_phrases": list(DEFAULT_SOFT_404_PHRASES),
        "soft_404_body_regexes": [],
        "cloudflare_block_title_phrase": DEFAULT_CLOUDFLARE_BLOCK_TITLE_PHRASE,
        "cloudflare_block_body_phrase": DEFAULT_CLOUDFLARE_BLOCK_BODY_PHRASE,
    }


def _sanitize_page_existence_criteria(raw: dict[str, Any]) -> dict[str, Any]:
    defaults = default_page_existence_criteria()
    merged = {**defaults, **(raw or {})}

    merged["enabled"] = bool(merged.get("enabled", True))

    statuses = merged.get("not_found_statuses", defaults["not_found_statuses"])
    if not isinstance(statuses, list):
        statuses = defaults["not_found_statuses"]
    normalized_statuses: set[int] = set()
    for value in statuses:
        try:
            code = int(value)
        except (TypeError, ValueError):
            continue
        if 100 <= code <= 599:
            normalized_statuses.add(code)
    merged["not_found_statuses"] = sorted(normalized_statuses or DEFAULT_NOT_FOUND_STATUSES)

    def _as_positive_int(value: Any, fallback: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = fallback
        return max(1, parsed)

    merged["soft_404_small_body_max_bytes"] = _as_positive_int(
        merged.get("soft_404_small_body_max_bytes"),
        DEFAULT_SOFT_404_SMALL_BODY_MAX_BYTES,
    )
    merged["soft_404_head_scan_max_bytes"] = _as_positive_int(
        merged.get("soft_404_head_scan_max_bytes"),
        DEFAULT_SOFT_404_HEAD_SCAN_MAX_BYTES,
    )
    merged["soft_404_body_scan_max_bytes"] = _as_positive_int(
        merged.get("soft_404_body_scan_max_bytes"),
        DEFAULT_SOFT_404_BODY_SCAN_MAX_BYTES,
    )

    def _as_string_list(value: Any, fallback: list[str]) -> list[str]:
        if not isinstance(value, list):
            value = fallback
        out = [str(item).strip() for item in value if str(item).strip()]
        return out if out else list(fallback)

    merged["soft_404_title_patterns"] = _as_string_list(
        merged.get("soft_404_title_patterns"),
        DEFAULT_SOFT_404_TITLE_PATTERNS,
    )
    merged["soft_404_body_phrases"] = _as_string_list(
        merged.get("soft_404_body_phrases"),
        DEFAULT_SOFT_404_PHRASES,
    )
    merged["soft_404_body_regexes"] = _as_string_list(
        merged.get("soft_404_body_regexes"),
        [],
    )
    merged["cloudflare_block_title_phrase"] = str(
        merged.get("cloudflare_block_title_phrase", DEFAULT_CLOUDFLARE_BLOCK_TITLE_PHRASE)
    ).strip() or DEFAULT_CLOUDFLARE_BLOCK_TITLE_PHRASE
    merged["cloudflare_block_body_phrase"] = str(
        merged.get("cloudflare_block_body_phrase", DEFAULT_CLOUDFLARE_BLOCK_BODY_PHRASE)
    ).strip() or DEFAULT_CLOUDFLARE_BLOCK_BODY_PHRASE
    return merged


def apply_page_existence_criteria_config(config: dict[str, Any]) -> None:
    global PAGE_EXISTENCE_CRITERIA, _PAGE_EXISTENCE_CRITERIA_LOADED
    PAGE_EXISTENCE_CRITERIA = _sanitize_page_existence_criteria(config)
    _PAGE_EXISTENCE_CRITERIA_LOADED = True


def ensure_page_existence_criteria_loaded() -> None:
    global _PAGE_EXISTENCE_CRITERIA_LOADED
    if _PAGE_EXISTENCE_CRITERIA_LOADED:
        return
    raw = read_json_file(PAGE_EXISTENCE_CRITERIA_CONFIG_PATH) if PAGE_EXISTENCE_CRITERIA_CONFIG_PATH.exists() else {}
    apply_page_existence_criteria_config(raw)


def configured_not_found_statuses() -> set[int]:
    ensure_page_existence_criteria_loaded()
    criteria = PAGE_EXISTENCE_CRITERIA or default_page_existence_criteria()
    statuses = criteria.get("not_found_statuses", sorted(DEFAULT_NOT_FOUND_STATUSES))
    if not isinstance(statuses, list):
        return set(DEFAULT_NOT_FOUND_STATUSES)
    out: set[int] = set()
    for value in statuses:
        try:
            code = int(value)
        except (TypeError, ValueError):
            continue
        if 100 <= code <= 599:
            out.add(code)
    return out or set(DEFAULT_NOT_FOUND_STATUSES)


def ensure_extension_config_loaded() -> None:
    global _EXTENSION_CONFIG_LOADED
    if _EXTENSION_CONFIG_LOADED:
        return
    default_config_path = CONFIG_DIR / "nightmare.json"
    if not default_config_path.exists():
        raise ValueError("Extension configuration is not loaded and config/nightmare.json was not found")
    apply_extension_config(read_json_file(default_config_path))


def load_string_resources() -> None:
    global HELP_TEXTS, MESSAGE_TEMPLATES, PROMPT_TEMPLATES, HTML_TEMPLATES, _STRING_RESOURCES_LOADED

    HELP_TEXTS = read_json_file(HELP_CONFIG_PATH) if HELP_CONFIG_PATH.exists() else {}
    MESSAGE_TEMPLATES = read_json_file(MESSAGE_CONFIG_PATH) if MESSAGE_CONFIG_PATH.exists() else {}
    PROMPT_TEMPLATES = read_json_file(PROMPT_CONFIG_PATH) if PROMPT_CONFIG_PATH.exists() else {}
    HTML_TEMPLATES = {}
    if TEMPLATE_DIR.exists():
        for path in TEMPLATE_DIR.glob("*.html"):
            HTML_TEMPLATES[path.name] = read_text_file(path)
    _STRING_RESOURCES_LOADED = True


def ensure_string_resources_loaded() -> None:
    if _STRING_RESOURCES_LOADED:
        return
    load_string_resources()


def get_string_config_value(payload: dict[str, Any], key: str, default: str = "") -> str:
    ensure_string_resources_loaded()
    value = payload.get(key, default)
    return str(value) if value is not None else default


_RESOURCE_TOKEN_PATTERN = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")


def render_resource_template(template: str, values: dict[str, Any]) -> str:
    rendered_values = {str(key).upper(): str(value) for key, value in values.items()}

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1).upper()
        return rendered_values.get(key, match.group(0))

    return _RESOURCE_TOKEN_PATTERN.sub(_replace, template)


def render_message(key: str, **values: Any) -> str:
    template = get_string_config_value(MESSAGE_TEMPLATES, key, key)
    return render_resource_template(template, values)


def render_prompt(key: str, **values: Any) -> str:
    template = get_string_config_value(PROMPT_TEMPLATES, key, "")
    if not template:
        raise ValueError(f"Missing prompt template: {key}")
    return render_resource_template(template, values)


def render_html_template(template_name: str, **values: Any) -> str:
    ensure_string_resources_loaded()
    template = HTML_TEMPLATES.get(template_name, "")
    if not template:
        raise ValueError(f"Missing HTML template: {template_name}")
    return render_resource_template(template, values)


def resolve_output_path_with_domain_default(
    cli_value: str | None,
    config: dict[str, Any],
    key: str,
    default_path: Path,
) -> Path:
    configured = cli_value
    if configured is None:
        configured = optional_string(config.get(key))
    if configured is None:
        return default_path
    return resolve_output_path(configured)


def parse_log_level(value: str, field_name: str) -> int:
    normalized = (value or "").strip().upper()
    if normalized in logging._nameToLevel:
        return logging._nameToLevel[normalized]
    raise ValueError(f"{field_name} must be one of: {', '.join(logging.getLevelNamesMapping().keys())}")


def configure_application_logger(log_file: Path, file_level: int, console_level: int) -> logging.Logger:
    ensure_directory(log_file.parent)
    logger = logging.getLogger("nightmare")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


class RequestThrottle:
    def __init__(self, interval_seconds: float):
        self.interval_seconds = max(0.0, float(interval_seconds))
        self._next_allowed_at = 0.0

    def wait(self) -> None:
        if self.interval_seconds <= 0.0:
            return

        now = time.monotonic()
        if now < self._next_allowed_at:
            interruptible_sleep(self._next_allowed_at - now)
        self._next_allowed_at = time.monotonic() + self.interval_seconds


@dataclass
class ProgressReporter:
    verbose: bool = False
    logger: logging.Logger | None = None

    def info(self, message: str) -> None:
        if self.logger is not None:
            self.logger.info(message)
        if self.verbose:
            print(f"[verbose] {message}", flush=True)

    def status(self, message: str) -> None:
        print(message, flush=True)


def open_browser_uri_nonblocking(
    uri: str,
    progress: ProgressReporter | None,
    opened_ok_message: str,
    opened_false_message: str,
    *,
    failure_message_prefix: str = "Failed to auto-open report",
) -> None:
    """Open a file:// or https:// URI without blocking the main thread (Windows browser launch can stall)."""

    def _run() -> None:
        try:
            opened = webbrowser.open(uri)
            if progress is None:
                return
            if opened:
                progress.info(opened_ok_message)
            else:
                progress.info(opened_false_message)
        except Exception as open_error:
            if progress is not None:
                progress.info(f"{failure_message_prefix}: {open_error}")

    threading.Thread(target=_run, daemon=True).start()


class ContextLengthExceededError(RuntimeError):
    pass


def make_evidence_path(evidence_dir: Path, url: str, source_type: str) -> Path:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    timestamp = int(time.time() * 1000)
    filename = f"{source_type}_{digest}_{timestamp}{EVIDENCE_FILE_EXTENSION}"
    return evidence_dir / filename


def save_evidence(evidence_dir: Path, url: str, source_type: str, payload: dict[str, Any]) -> str:
    ensure_directory(evidence_dir)
    evidence_path = make_evidence_path(evidence_dir, url, source_type)
    write_json_gzip(evidence_path, payload)
    return evidence_path.as_posix()


@dataclass
class UrlInventoryRecord:
    url: str
    discovered_via: set[str] = field(default_factory=set)
    discovered_from: set[str] = field(default_factory=set)
    discovery_evidence_files: list[str] = field(default_factory=list)
    crawl_requested: bool = False
    was_crawled: bool = False
    crawl_status_code: int | None = None
    crawl_note: str | None = None
    exists_confirmed: bool = False
    existence_status_code: int | None = None
    existence_check_method: str | None = None
    existence_check_note: str | None = None
    soft_404_detected: bool = False
    soft_404_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "discovered_via": sorted(self.discovered_via),
            "discovered_from": sorted(self.discovered_from),
            "discovery_evidence_files": self.discovery_evidence_files,
            "crawl_requested": self.crawl_requested,
            "was_crawled": self.was_crawled,
            "crawl_status_code": self.crawl_status_code,
            "crawl_note": self.crawl_note,
            "exists_confirmed": self.exists_confirmed,
            "existence_status_code": self.existence_status_code,
            "existence_check_method": self.existence_check_method,
            "existence_check_note": self.existence_check_note,
            "soft_404_detected": self.soft_404_detected,
            "soft_404_reason": self.soft_404_reason,
        }


@dataclass
class CrawlState:
    discovered_urls: set[str] = field(default_factory=set)
    visited_urls: set[str] = field(default_factory=set)
    link_graph: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    url_inventory: dict[str, UrlInventoryRecord] = field(default_factory=dict)
    #: Normalized script/asset URLs already saved under ``collected_data/scripts`` (resume-safe dedup).
    scripts_fetched_urls: set[str] = field(default_factory=set)
    #: Normalized artifact URLs copied under ``collected_data/artifacts`` (resume-safe dedup).
    artifacts_saved_urls: set[str] = field(default_factory=set)
    #: Normalized crawl seed URLs built from ``file_path_list.txt`` (for reporting under ``guessed_paths/``).
    wordlist_path_seeds: list[str] = field(default_factory=list)
    #: Source path used for wordlist path seed generation (repo-relative when possible).
    wordlist_source: str = FILE_PATH_WORDLIST_DISCOVERED_FROM


DIRECT_DISCOVERY_SOURCES = {
    "internal_link",
    "form_action",
    "href_reference",
    "src_reference",
    "redirect_location",
}
INFERENCE_DISCOVERY_SOURCES = {
    "embedded_route",
    "attribute_route",
    "guessed_url",
    "ai_probe_request",
    "crawl_response",
    "seed_input",
    "file_path_wordlist",
    "observed_in_script_file",
}
GUESSED_DISCOVERY_SOURCES = {"guessed_url", "file_path_wordlist"}


def count_urls_by_discovery_sources(
    state: CrawlState,
    preferred_sources: set[str],
    fallback_sources: set[str] | None = None,
) -> int:
    total = 0
    fallback = fallback_sources or set()
    for record in state.url_inventory.values():
        sources = set(record.discovered_via)
        if sources & preferred_sources:
            total += 1
            continue
        if fallback and sources & fallback:
            total += 1
    return total


@dataclass
class CondensedTreeNode:
    children: dict[str, "CondensedTreeNode"] = field(default_factory=dict)
    is_page: bool = False
    query_shapes: set[str] = field(default_factory=set)


def crawl_state_to_dict(state: CrawlState) -> dict[str, Any]:
    return {
        "discovered_urls": sorted(state.discovered_urls),
        "visited_urls": sorted(state.visited_urls),
        "link_graph": {source: sorted(targets) for source, targets in state.link_graph.items()},
        "url_inventory": {url: record.to_dict() for url, record in state.url_inventory.items()},
        "scripts_fetched_urls": sorted(state.scripts_fetched_urls),
        "artifacts_saved_urls": sorted(state.artifacts_saved_urls),
        "wordlist_path_seeds": list(state.wordlist_path_seeds),
        "wordlist_source": str(state.wordlist_source or FILE_PATH_WORDLIST_DISCOVERED_FROM),
    }


def crawl_state_from_dict(payload: dict[str, Any]) -> CrawlState:
    state = CrawlState()

    discovered_urls = payload.get("discovered_urls", [])
    if isinstance(discovered_urls, list):
        state.discovered_urls = {
            normalize_url(url) for url in discovered_urls if isinstance(url, str) and url.strip()
        }

    visited_urls = payload.get("visited_urls", [])
    if isinstance(visited_urls, list):
        state.visited_urls = {
            normalize_url(url) for url in visited_urls if isinstance(url, str) and url.strip()
        }

    link_graph = payload.get("link_graph", {})
    if isinstance(link_graph, dict):
        for source, targets in link_graph.items():
            if not isinstance(source, str):
                continue
            if not isinstance(targets, list):
                continue
            normalized_source = normalize_url(source)
            for target in targets:
                if isinstance(target, str) and target.strip():
                    state.link_graph[normalized_source].add(normalize_url(target))

    inventory = payload.get("url_inventory", {})
    if isinstance(inventory, dict):
        for url, record in inventory.items():
            if not isinstance(url, str):
                continue
            if not isinstance(record, dict):
                continue
            normalized_url = normalize_url(url)
            state.url_inventory[normalized_url] = UrlInventoryRecord(
                url=normalized_url,
                discovered_via=set(record.get("discovered_via", [])) if isinstance(record.get("discovered_via"), list) else set(),
                discovered_from=set(record.get("discovered_from", []))
                if isinstance(record.get("discovered_from"), list)
                else set(),
                discovery_evidence_files=list(record.get("discovery_evidence_files", []))
                if isinstance(record.get("discovery_evidence_files"), list)
                else [],
                crawl_requested=bool(record.get("crawl_requested", False)),
                was_crawled=bool(record.get("was_crawled", False)),
                crawl_status_code=record.get("crawl_status_code"),
                crawl_note=str(record.get("crawl_note", "")).strip() or None,
                exists_confirmed=bool(record.get("exists_confirmed", False)),
                existence_status_code=record.get("existence_status_code"),
                existence_check_method=record.get("existence_check_method"),
                existence_check_note=record.get("existence_check_note"),
                soft_404_detected=bool(record.get("soft_404_detected", False)),
                soft_404_reason=str(record.get("soft_404_reason", "")).strip() or None,
            )

    for url in state.discovered_urls:
        if url not in state.url_inventory:
            state.url_inventory[url] = UrlInventoryRecord(url=url)

    wordlist_seeds = payload.get("wordlist_path_seeds", [])
    if isinstance(wordlist_seeds, list):
        state.wordlist_path_seeds = [
            normalize_url(u) for u in wordlist_seeds if isinstance(u, str) and u.strip()
        ]
    wordlist_source = payload.get("wordlist_source")
    if isinstance(wordlist_source, str) and wordlist_source.strip():
        state.wordlist_source = wordlist_source.strip()

    scripts_fetched = payload.get("scripts_fetched_urls", [])
    if isinstance(scripts_fetched, list):
        state.scripts_fetched_urls = {
            normalize_url(u) for u in scripts_fetched if isinstance(u, str) and u.strip()
        }
    artifacts_saved = payload.get("artifacts_saved_urls", [])
    if isinstance(artifacts_saved, list):
        state.artifacts_saved_urls = {
            normalize_url(u) for u in artifacts_saved if isinstance(u, str) and u.strip()
        }

    return state


def build_resume_frontier(state: CrawlState, root_domain: str) -> list[str]:
    frontier: list[str] = []
    for url in sorted(state.discovered_urls):
        if url in state.visited_urls:
            continue
        if not is_allowed_domain(url, root_domain):
            continue
        if not should_crawl_url(url):
            continue
        frontier.append(url)
    return frontier


def save_session_state(
    session_state_path: Path,
    start_url: str,
    root_domain: str,
    max_pages: int,
    state: CrawlState,
) -> None:
    frontier = build_resume_frontier(state, root_domain)
    payload = {
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "start_url": normalize_url(start_url),
        "root_domain": root_domain,
        "max_pages": max_pages,
        "frontier": frontier,
        "state": crawl_state_to_dict(state),
    }
    ensure_directory(session_state_path.parent)
    write_json(session_state_path, payload)


def load_session_state(session_state_path: Path) -> dict[str, Any]:
    if not session_state_path.exists():
        return {}
    return read_json_file(session_state_path)


def ensure_inventory_record(state: CrawlState, url: str) -> UrlInventoryRecord:
    normalized = normalize_url(url)
    if normalized not in state.url_inventory:
        state.url_inventory[normalized] = UrlInventoryRecord(url=normalized)
    return state.url_inventory[normalized]


def register_url_discovery(
    state: CrawlState,
    url: str,
    source_type: str,
    discovered_from: str,
    evidence_file: str,
) -> None:
    normalized = normalize_url(url)
    state.discovered_urls.add(normalized)

    record = ensure_inventory_record(state, normalized)
    record.discovered_via.add(source_type)
    if discovered_from:
        record.discovered_from.add(discovered_from)
    if evidence_file and evidence_file not in record.discovery_evidence_files:
        record.discovery_evidence_files.append(evidence_file)


def build_scrapy_discovery_evidence(response: scrapy.http.Response, discovered_url: str, source_type: str) -> dict[str, Any]:
    req = response.request
    return {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_type": source_type,
        "discovered_url": discovered_url,
        "source_page_url": normalize_url(response.url),
        "request": {
            "method": req.method,
            "url": req.url,
            "headers": req.headers.to_unicode_dict(),
            **encode_body_for_evidence(req.body or b""),
        },
        "response": {
            "status": response.status,
            "url": response.url,
            "headers": response.headers.to_unicode_dict(),
            **encode_body_for_evidence(response.body or b""),
        },
    }


def build_scrapy_request_failure_evidence(request: scrapy.http.Request, source_type: str, note: str) -> dict[str, Any]:
    return {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_type": source_type,
        "discovered_url": normalize_url(request.url),
        "source_page_url": normalize_url(request.url),
        "request": {
            "method": request.method,
            "url": request.url,
            "headers": request.headers.to_unicode_dict(),
            **encode_body_for_evidence(request.body or b""),
        },
        "response": None,
        "failure_note": note,
    }


def get_response_text(response: scrapy.http.Response) -> str:
    if hasattr(response, "text"):
        return response.text
    return response.body.decode("utf-8", errors="ignore")


def extract_embedded_urls(response: scrapy.http.Response) -> set[str]:
    response_text = get_response_text(response)
    if len(response_text) > MAX_EMBEDDED_REGEX_SCAN_BYTES:
        response_text = response_text[:MAX_EMBEDDED_REGEX_SCAN_BYTES]
    discovered_urls: set[str] = set()

    for match in re.finditer(r"""["'](/(?!/)[^"'<>\s]{1,2048})["']""", response_text):
        discovered_urls.add(normalize_url(urljoin(response.url, match.group(1))))

    for match in re.finditer(r"""["'](\\/(?!/)[^"'<>\s]{1,2048})["']""", response_text):
        unescaped_path = match.group(1).replace("\\/", "/")
        discovered_urls.add(normalize_url(urljoin(response.url, unescaped_path)))

    for match in re.finditer(r"""["'](https?://[^"'<>\s]{1,2048})["']""", response_text, flags=re.IGNORECASE):
        discovered_urls.add(normalize_url(match.group(1)))

    return discovered_urls


def extract_attribute_urls(response: scrapy.http.Response) -> set[str]:
    discovered_urls: set[str] = set()

    for raw_value in response.xpath(_URL_ATTRIBUTE_XPATH).getall():
        value = (raw_value or "").strip()
        if not value:
            continue

        if value.startswith("//"):
            discovered_urls.add(normalize_url(urljoin(response.url, f"https:{value}")))
            continue

        if value.startswith("/"):
            discovered_urls.add(normalize_url(urljoin(response.url, value)))
            continue

        if value.lower().startswith(("http://", "https://")):
            discovered_urls.add(normalize_url(value))

    return discovered_urls


def extract_discovery_candidates(response: scrapy.http.Response) -> tuple[dict[str, set[str]], bool]:
    """Return (candidates, used_lightweight_only). Lightweight mode skips very expensive passes."""
    candidates: dict[str, set[str]] = defaultdict(set)

    for href in response.css("a::attr(href)").getall():
        candidates["internal_link"].add(normalize_url(urljoin(response.url, href)))

    for action in response.css("form::attr(action)").getall():
        candidates["form_action"].add(normalize_url(urljoin(response.url, action)))

    body_len = len(response.body or b"")
    if body_len > LINK_EXTRACTION_FULL_MAX_BYTES:
        logging.getLogger("nightmare").info(
            "Large response (%d bytes) at %s: lightweight link extraction only "
            "(skipping heavy DOM scans and full-page embedded URL regex)",
            body_len,
            response.url,
        )
        return candidates, True

    for href in response.css("*:not(a):not(form)::attr(href)").getall():
        candidates["href_reference"].add(normalize_url(urljoin(response.url, href)))

    for src in response.css("*::attr(src)").getall():
        candidates["src_reference"].add(normalize_url(urljoin(response.url, src)))

    for embedded_url in extract_embedded_urls(response):
        candidates["embedded_route"].add(embedded_url)

    for attribute_url in extract_attribute_urls(response):
        candidates["attribute_route"].add(attribute_url)

    return candidates, False


COLLECTED_DATA_DIRNAME = "collected_data"
# Single JSON snapshot of everything under collected_data/ (see build_collected_data_rollup).
COLLECTED_DATA_SUMMARY_FILENAME = "summary.json"
COLLECTED_DATA_ROLLUP_MIN_INTERVAL_S = 12.0
COLLECTED_DATA_ROLLUP_MAX_ENDPOINT_JSON_BYTES = 6 * 1024 * 1024
COLLECTED_DATA_ROLLUP_MAX_SCRIPT_INLINE_BYTES = 256 * 1024
COLLECTED_DATA_ROLLUP_MAX_SCRIPT_HEAD_CHARS = 8000
COLLECTED_DATA_ROLLUP_MAX_JS_EXTRACTOR_SUMMARY_BYTES = 8 * 1024 * 1024
SCRIPT_SRC_EXTENSIONS = frozenset(
    {".js", ".mjs", ".cjs", ".jsx", ".ts", ".tsx", ".vue", ".svelte", ".coffee", ".dart"}
)
ARTIFACT_PATH_KEYWORDS = (
    "robots.txt",
    "sitemap.xml",
    "sitemap_index.xml",
    "openapi.json",
    "openapi.yaml",
    "swagger.json",
    "swagger.yaml",
    "postman_collection.json",
    "wsdl",
    ".well-known",
    "graphql",
    "favicon.ico",
)
DEFAULT_MAX_SCRIPT_ASSET_FETCHES = 400


def collected_data_root_from_evidence_dir(evidence_dir: Path) -> Path:
    return evidence_dir.resolve().parent / COLLECTED_DATA_DIRNAME


def ensure_collected_data_layout(root: Path) -> dict[str, Path]:
    sub = {
        "forms": root / "forms",
        "scripts": root / "scripts",
        "endpoints": root / "endpoints",
        "artifacts": root / "artifacts",
        "cookies": root / "cookies",
        "js_endpoints": root / "js_endpoints",
    }
    for path in sub.values():
        ensure_directory(path)
    return sub


def _rollup_redact_encoded_blobs(obj: Any) -> Any:
    """Drop huge base64 bodies from nested dicts for the aggregate summary file."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            ks = str(k)
            if ks == "body_base64" and isinstance(v, str):
                out["body_base64_omitted"] = True
                out["body_base64_length_chars"] = len(v)
                continue
            out[ks] = _rollup_redact_encoded_blobs(v)
        return out
    if isinstance(obj, list):
        return [_rollup_redact_encoded_blobs(x) for x in obj]
    return obj


def _load_json_object(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    if isinstance(raw, dict):
        return raw
    return None


def _read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        text = path.read_text(encoding="utf-8-sig", errors="replace")
    except OSError:
        return rows
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            rows.append(rec)
    return rows


def _script_file_rollup(path: Path, collected_root: Path) -> dict[str, Any]:
    rel = path.relative_to(collected_root).as_posix()
    try:
        st = path.stat()
    except OSError:
        return {"relative_path": rel, "error": "stat_failed"}
    ent: dict[str, Any] = {
        "relative_path": rel,
        "size_bytes": int(st.st_size),
        "modified_at_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
    }
    if st.st_size <= COLLECTED_DATA_ROLLUP_MAX_SCRIPT_INLINE_BYTES:
        try:
            ent["content"] = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            ent["read_error"] = str(exc)
        return ent
    if st.st_size <= 4 * 1024 * 1024:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            head = text[:COLLECTED_DATA_ROLLUP_MAX_SCRIPT_HEAD_CHARS]
            ent["head"] = head
            ent["head_truncated"] = len(text) > len(head)
            ent["total_chars"] = len(text)
        except OSError as exc:
            ent["read_error"] = str(exc)
    else:
        ent["note"] = "large file; content omitted from summary (open sibling file)"
    return ent


def _artifact_pair_rollup(blob_path: Path, collected_root: Path) -> dict[str, Any]:
    rel = blob_path.relative_to(collected_root).as_posix()
    try:
        st = blob_path.stat()
    except OSError:
        return {"relative_path": rel, "error": "stat_failed"}
    meta_path = blob_path.with_suffix(".meta.json")
    meta: dict[str, Any] | None = None
    if meta_path.is_file():
        meta = _load_json_object(meta_path)
    return {
        "relative_path": rel,
        "size_bytes": int(st.st_size),
        "meta_path": meta_path.relative_to(collected_root).as_posix() if meta_path.is_file() else None,
        "meta": meta,
    }


def build_collected_data_rollup(collected_data_root: Path) -> dict[str, Any]:
    """Single-structure view of all crawl-persisted files under ``collected_data`` (not full base64 bodies)."""
    root = collected_data_root.resolve()
    forms_out: list[dict[str, Any]] = []
    forms_dir = root / "forms"
    if forms_dir.is_dir():
        for p in sorted(forms_dir.glob("*.json")):
            data = _load_json_object(p)
            if data is None:
                continue
            forms_out.append(
                {
                    "relative_path": p.relative_to(root).as_posix(),
                    "record": data,
                }
            )

    endpoints_out: list[dict[str, Any]] = []
    ep_dir = root / "endpoints"
    if ep_dir.is_dir():
        for p in sorted(ep_dir.glob("*.json")):
            rel = p.relative_to(root).as_posix()
            try:
                sz = p.stat().st_size
            except OSError:
                continue
            if sz > COLLECTED_DATA_ROLLUP_MAX_ENDPOINT_JSON_BYTES:
                endpoints_out.append(
                    {
                        "relative_path": rel,
                        "file_size_bytes": sz,
                        "note": "endpoint JSON too large to embed; open file for full request/response bodies",
                    }
                )
                continue
            data = _load_json_object(p)
            if data is None:
                endpoints_out.append({"relative_path": rel, "error": "json_parse_failed"})
                continue
            endpoints_out.append(
                {
                    "relative_path": rel,
                    "record": _rollup_redact_encoded_blobs(data),
                }
            )

    cookies_out: dict[str, list[dict[str, Any]]] = {}
    cookies_dir = root / "cookies"
    if cookies_dir.is_dir():
        for p in sorted(cookies_dir.glob("*.jsonl")):
            cookies_out[p.name] = _read_jsonl_records(p)

    js_endpoints_out: list[dict[str, Any]] = []
    je_dir = root / "js_endpoints"
    if je_dir.is_dir():
        for p in sorted(je_dir.glob("*.json")):
            data = _load_json_object(p)
            if data is None:
                continue
            js_endpoints_out.append(
                {
                    "relative_path": p.relative_to(root).as_posix(),
                    "record": data,
                }
            )

    scripts_out: list[dict[str, Any]] = []
    scripts_dir = root / "scripts"
    if scripts_dir.is_dir():
        for p in sorted(scripts_dir.rglob("*")):
            if not p.is_file():
                continue
            scripts_out.append(_script_file_rollup(p, root))

    artifacts_out: list[dict[str, Any]] = []
    art_dir = root / "artifacts"
    if art_dir.is_dir():
        for p in sorted(art_dir.iterdir()):
            if not p.is_file():
                continue
            if p.name.endswith(".meta.json"):
                continue
            artifacts_out.append(_artifact_pair_rollup(p, root))

    js_ext_root = root / "javascript_extractor"
    js_ext_block: dict[str, Any] = {}
    if js_ext_root.is_dir():
        summ = js_ext_root / "summary.json"
        trim_c = js_ext_root / "trim_stats.json"
        if summ.is_file():
            try:
                ss = summ.stat().st_size
            except OSError:
                ss = 0
            if ss and ss <= COLLECTED_DATA_ROLLUP_MAX_JS_EXTRACTOR_SUMMARY_BYTES:
                data = _load_json_object(summ)
                if data is not None:
                    js_ext_block["summary"] = _rollup_redact_encoded_blobs(data)
            else:
                js_ext_block["summary_path"] = summ.relative_to(root).as_posix()
                js_ext_block["summary_note"] = "summary.json too large or missing; see on disk"
        if trim_c.is_file():
            tdata = _load_json_object(trim_c)
            if tdata is not None:
                js_ext_block["trim_stats"] = tdata
        mdir = js_ext_root / "matches"
        if mdir.is_dir():
            js_ext_block["match_json_files"] = sorted(
                mp.relative_to(root).as_posix() for mp in mdir.glob("m_*.json")
            )

    counts = {
        "forms": len(forms_out),
        "endpoints": len(endpoints_out),
        "cookie_jsonl_files": len(cookies_out),
        "cookie_records_total": sum(len(v) for v in cookies_out.values()),
        "js_endpoints": len(js_endpoints_out),
        "scripts": len(scripts_out),
        "artifacts": len(artifacts_out),
    }

    return {
        "version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "collected_data_root": str(root),
        "counts": counts,
        "forms": forms_out,
        "endpoints": endpoints_out,
        "cookies": cookies_out,
        "js_endpoints": js_endpoints_out,
        "scripts": scripts_out,
        "artifacts": artifacts_out,
        "javascript_extractor": js_ext_block,
    }


def write_collected_data_rollup(collected_data_root: Path) -> Path:
    """Write ``collected_data/summary.json`` with a full rollup of per-file crawl outputs."""
    root = collected_data_root.resolve()
    ensure_directory(root)
    out_path = root / COLLECTED_DATA_SUMMARY_FILENAME
    payload = build_collected_data_rollup(root)
    save_json_file(out_path, payload)
    return out_path


def _slug_file_component(raw: str, fallback: str) -> str:
    text = (raw or "").strip()
    if not text:
        text = fallback
    text = re.sub(r"[^a-zA-Z0-9._-]+", "_", text).strip("._-") or fallback
    return text[:120]


def split_script_stem_and_extension(path_tail: str) -> tuple[str, str]:
    """Last path segment -> (stem, ext) where ext is a known script extension or '.js'."""
    path_tail = (path_tail or "").strip() or "script"
    if "/" in path_tail:
        path_tail = path_tail.rsplit("/", 1)[-1]
    unq = unquote(path_tail)
    lower = unq.lower()
    for ext in sorted(SCRIPT_SRC_EXTENSIONS, key=len, reverse=True):
        if lower.endswith(ext):
            stem = unq[: -len(ext)].strip()
            return (stem or "script", ext)
    base, dot_ext = os.path.splitext(unq)
    if dot_ext.lower() in SCRIPT_SRC_EXTENSIONS:
        return (base.strip() or "script", dot_ext.lower())
    return (unq, ".js")


def infer_inline_script_extension(script_sel) -> str:
    """Prefer a real script extension; inline blocks default to .js unless type hints otherwise."""
    st = (script_sel.css("::attr(type)").get() or "").strip().lower()
    if "typescript" in st:
        return ".ts"
    lang = (script_sel.css("::attr(lang)").get() or "").strip().lower()
    if lang in {"ts", "typescript"}:
        return ".ts"
    if lang in {"tsx"}:
        return ".tsx"
    return ".js"


def _url_fingerprint_component(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()[:16]


def split_path_tail_stem_ext(tail: str) -> tuple[str, str]:
    """Last URL path segment -> (stem, extension_with_dot); extension may be empty."""
    tail = unquote((tail or "").strip() or "file")
    if "/" in tail:
        tail = tail.rsplit("/", 1)[-1]
    if "." not in tail:
        return tail, ""
    idx = tail.rfind(".")
    return tail[:idx], tail[idx:]


def url_filesystem_label(url: str, *, max_len: int = 120) -> str:
    """Stable, human-readable filesystem token from an http(s) URL (host + path + short query id)."""
    u = normalize_url(url)
    p = urlparse(u)
    host = _slug_file_component((p.hostname or "host").replace(":", "_"), "host")
    segments = [seg for seg in (p.path or "/").split("/") if seg]
    if not segments:
        path_s = "root"
    else:
        slugged = [_slug_file_component(seg, "p")[:48] for seg in segments[:12]]
        path_s = "_".join(slugged)
    q = ""
    if p.query:
        q = "_q" + _url_fingerprint_component(p.query)[:10]
    merged = f"{host}_{path_s}{q}"
    return _slug_file_component(merged, "url")[:max_len]


def build_collected_endpoint_basename(response_url: str, role: str) -> str:
    label = url_filesystem_label(response_url)
    rslug = _slug_file_component(role, "role")[:32]
    key = _url_fingerprint_component(normalize_url(response_url) + "|" + role)[:12]
    return f"{label}_{rslug}_{key}"


def save_json_file(path: Path, payload: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def append_jsonl_file(path: Path, record: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)


def extract_form_payloads_from_response(response: scrapy.http.Response, page_url: str) -> list[dict[str, Any]]:
    forms_out: list[dict[str, Any]] = []
    for idx, form in enumerate(response.css("form"), start=1):
        action = (form.css("::attr(action)").get() or "").strip()
        method = (form.css("::attr(method)").get() or "get").strip().upper() or "GET"
        name = (form.css("::attr(name)").get() or "").strip()
        fid = (form.css("::attr(id)").get() or "").strip()
        enctype = (form.css("::attr(enctype)").get() or "").strip().lower()
        autocomplete = (form.css("::attr(autocomplete)").get() or "").strip().lower()
        action_abs = normalize_url(urljoin(page_url, action)) if action else normalize_url(page_url)
        inputs: list[dict[str, Any]] = []
        for field in form.css("input, select, textarea, button"):
            tag = getattr(field.root, "tag", None) or "node"
            if isinstance(tag, str):
                tag_name = tag.lower()
            else:
                tag_name = "node"
            inputs.append(
                {
                    "tag": tag_name,
                    "type": (field.css("::attr(type)").get() or "").strip().lower(),
                    "name": (field.css("::attr(name)").get() or "").strip(),
                    "id": (field.css("::attr(id)").get() or "").strip(),
                    "value": (field.css("::attr(value)").get() or "").strip(),
                    "placeholder": (field.css("::attr(placeholder)").get() or "").strip(),
                    "disabled": bool(field.css("::attr(disabled)").get()),
                    "required": field.css("::attr(required)").get() is not None,
                }
            )
        form_slug = _slug_file_component(name or fid, f"form_{idx}")
        forms_out.append(
            {
                "form_file_key": form_slug,
                "source_page_url": normalize_url(page_url),
                "action_url": action_abs,
                "http_method": method,
                "enctype": enctype or "application/x-www-form-urlencoded",
                "name_attr": name or None,
                "id_attr": fid or None,
                "autocomplete": autocomplete or None,
                "inputs": inputs,
                "note": "Captured from HTML at crawl time; submit traffic was not synthesized.",
            }
        )
    return forms_out


def looks_like_downloadable_script_url(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if not path or path.endswith("/"):
        return False
    ext = f".{path.rsplit('.', 1)[-1]}" if "." in path.rsplit('/', 1)[-1] else ""
    return ext in SCRIPT_SRC_EXTENSIONS


def looks_like_standard_artifact_url(url: str) -> bool:
    u = (url or "").lower()
    return any(k in u for k in ARTIFACT_PATH_KEYWORDS)


def extract_urls_from_javascript_for_discovery(text: str, base_url: str) -> set[str]:
    """Extract http(s) URLs from JS/TS text using the same literal/link heuristics as HTML."""
    from scrapy.http import TextResponse

    fake = TextResponse(url=base_url, body=(text or "").encode("utf-8", errors="ignore"), encoding="utf-8")
    out: set[str] = set()
    out |= extract_embedded_urls(fake)
    out |= extract_attribute_urls(fake)
    return {u for u in out if u.startswith("http://") or u.startswith("https://")}


def is_markup_response(response: scrapy.http.Response) -> bool:
    content_type = (response.headers.get("Content-Type") or b"").decode("latin-1", errors="ignore").lower()
    if any(marker in content_type for marker in ("text/html", "application/xhtml", "application/xml", "text/xml")):
        return True

    selector = getattr(response, "selector", None)
    selector_type = getattr(selector, "type", "")
    return selector_type in {"html", "xml"}


class AdaptiveBackoffController:
    """Adjust per-slot crawl delay when rate limiting is detected."""

    def __init__(
        self,
        crawler,
        min_delay: float,
        max_delay: float,
        backoff_factor: float,
        recovery_factor: float,
        rate_limit_statuses: set[int],
    ):
        self.crawler = crawler
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.recovery_factor = recovery_factor
        self.rate_limit_statuses = rate_limit_statuses

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        statuses = settings.get("RATE_LIMIT_HTTP_CODES", [429, 503])
        status_set = {int(code) for code in statuses}
        return cls(
            crawler=crawler,
            min_delay=float(settings.getfloat("ADAPTIVE_BACKOFF_MIN_DELAY", 0.25)),
            max_delay=float(settings.getfloat("ADAPTIVE_BACKOFF_MAX_DELAY", 30.0)),
            backoff_factor=float(settings.getfloat("ADAPTIVE_BACKOFF_FACTOR", 1.7)),
            recovery_factor=float(settings.getfloat("ADAPTIVE_RECOVERY_FACTOR", 0.92)),
            rate_limit_statuses=status_set,
        )

    def process_response(self, request, response, spider=None):
        slot = self._get_slot(request)
        if slot is None:
            return response

        active_spider = spider
        if active_spider is None:
            active_spider = getattr(self.crawler, "spider", None)

        if response.status in self.rate_limit_statuses:
            self._increase_delay(slot, active_spider, response.status, request.url)
        elif response.status < 400:
            self._recover_delay(slot)

        return response

    def _get_slot(self, request):
        slot_key = request.meta.get("download_slot")
        if not slot_key:
            slot_key = (urlparse(request.url).hostname or "").lower()
        if not slot_key:
            return None
        return self.crawler.engine.downloader.slots.get(slot_key)

    def _increase_delay(self, slot, spider, status_code: int, url: str) -> None:
        current_delay = max(float(getattr(slot, "delay", 0.0)), self.min_delay)
        new_delay = min(self.max_delay, current_delay * self.backoff_factor)
        if new_delay > current_delay:
            slot.delay = new_delay
            if spider is not None:
                spider.logger.warning(
                    "Rate limit signal (HTTP %s) from %s; increased crawl delay %.2fs -> %.2fs",
                    status_code,
                    url,
                    current_delay,
                    new_delay,
                )

    def _recover_delay(self, slot) -> None:
        current_delay = max(float(getattr(slot, "delay", 0.0)), self.min_delay)
        new_delay = max(self.min_delay, current_delay * self.recovery_factor)
        if new_delay < current_delay - 0.01:
            slot.delay = new_delay


class DomainSpider(scrapy.Spider):
    name = "domain_spider"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "LOG_ENABLED": True,
        "TELNETCONSOLE_ENABLED": False,
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
        },
        "DOWNLOAD_TIMEOUT": 20,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.25,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.25,
        "AUTOTHROTTLE_MAX_DELAY": 30.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        "AUTOTHROTTLE_DEBUG": False,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
        "ADAPTIVE_BACKOFF_MIN_DELAY": 0.25,
        "ADAPTIVE_BACKOFF_MAX_DELAY": 30.0,
        "ADAPTIVE_BACKOFF_FACTOR": 1.7,
        "ADAPTIVE_RECOVERY_FACTOR": 0.92,
        "RATE_LIMIT_HTTP_CODES": [429, 503],
        "DOWNLOADER_MIDDLEWARES": {
            f"{__name__}.AdaptiveBackoffController": 540,
        },
        "REDIRECT_ENABLED": True,
    }

    def __init__(
        self,
        start_url: str,
        start_urls: list[str] | None,
        root_domain: str,
        max_pages: int,
        state: CrawlState,
        evidence_dir: str,
        session_state_path: str | None = None,
        verbose: bool = False,
        progress: ProgressReporter | None = None,
        wordlist_path_seeds: frozenset[str] | set[str] | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.start_url = normalize_url(start_url)
        # Empty list means "resume found nothing to crawl" — do not substitute ``start_url`` here,
        # or we'd re-queue an already-visited seed and Scrapy would exit with zero requests.
        if start_urls is None:
            initial_start_urls = [self.start_url]
        else:
            initial_start_urls = list(start_urls)
        self.start_urls = [normalize_url(url) for url in initial_start_urls if isinstance(url, str) and url.strip()]
        self.root_domain = root_domain
        self.max_pages = max_pages
        self.state = state
        self.evidence_dir = Path(evidence_dir)
        self.session_state_path = Path(session_state_path) if session_state_path else None
        self.verbose = verbose
        self.progress = progress
        self.wordlist_path_seeds = frozenset(wordlist_path_seeds or ())
        self.initial_discovered_url_count = len(self.state.discovered_urls)
        self._last_nonverbose_progress_at = time.monotonic()
        self._last_nonverbose_new_url_count = 0
        self._last_nonverbose_total_url_count = len(self.state.discovered_urls)
        self._has_emitted_nonverbose_progress = False
        self._nonverbose_progress_interval_seconds = 10.0
        self._nonverbose_progress_new_url_step = 10
        self.collected_data_root = collected_data_root_from_evidence_dir(self.evidence_dir)
        self._collected = ensure_collected_data_layout(self.collected_data_root)
        self._max_script_asset_fetches = DEFAULT_MAX_SCRIPT_ASSET_FETCHES
        self._script_asset_fetch_count = 0
        self._last_collected_rollup_at = 0.0

    def _maybe_refresh_collected_data_rollup(self, *, force: bool = False) -> None:
        """Refresh ``collected_data/summary.json`` (debounced during crawl; always on spider close)."""
        now = time.monotonic()
        if (
            not force
            and self._last_collected_rollup_at > 0
            and (now - self._last_collected_rollup_at) < COLLECTED_DATA_ROLLUP_MIN_INTERVAL_S
        ):
            return
        self._last_collected_rollup_at = now
        try:
            out = write_collected_data_rollup(self.collected_data_root)
            self.logger.debug("collected_data summary rollup: %s", out)
        except Exception as exc:
            self.logger.warning("collected_data summary rollup failed: %s", exc)

    def _build_crawl_request(self, url: str, *, wordlist_path_guess: bool = False) -> scrapy.Request:
        meta: dict[str, Any] = {"handle_httpstatus_all": True}
        if wordlist_path_guess:
            meta["wordlist_path_guess"] = True
        return scrapy.Request(
            url,
            callback=self.parse,
            errback=self.handle_request_failure,
            meta=meta,
        )

    def _emit_verbose(self, message: str) -> None:
        if not self.verbose:
            return
        # Direct console output ensures live visibility even when Scrapy logs are redirected.
        print(f"[verbose][crawl] {message}", flush=True)
        self.logger.info(message)

    def _build_nonverbose_progress_message(self) -> str:
        total_unique = len(self.state.discovered_urls)
        previous_session = min(self.initial_discovered_url_count, total_unique)
        new_this_session = max(0, total_unique - previous_session)
        direct_count = count_urls_by_discovery_sources(self.state, DIRECT_DISCOVERY_SOURCES)
        inference_count = count_urls_by_discovery_sources(self.state, INFERENCE_DISCOVERY_SOURCES)
        return (
            f"Total URLs: {total_unique} - New this session: {new_this_session} - ({direct_count} direct /  {inference_count} guessed)"
        )

    def _emit_nonverbose_progress_if_needed(self, *, force: bool = False) -> None:
        if self.verbose or self.progress is None:
            return

        total_unique = len(self.state.discovered_urls)
        new_this_session = max(0, total_unique - self.initial_discovered_url_count)
        now = time.monotonic()
        enough_time = (now - self._last_nonverbose_progress_at) >= self._nonverbose_progress_interval_seconds
        enough_new_urls = (new_this_session - self._last_nonverbose_new_url_count) >= self._nonverbose_progress_new_url_step
        changed_since_last_emit = total_unique != self._last_nonverbose_total_url_count
        if force and self._has_emitted_nonverbose_progress and not changed_since_last_emit and total_unique > 0:
            return
        if not force and not enough_time and not enough_new_urls:
            return

        self.progress.status(self._build_nonverbose_progress_message())
        self._last_nonverbose_progress_at = now
        self._last_nonverbose_new_url_count = new_this_session
        self._last_nonverbose_total_url_count = total_unique
        self._has_emitted_nonverbose_progress = True

    def _persist_endpoint_record(self, response: scrapy.http.Response, *, role: str, extra: dict[str, Any] | None = None) -> None:
        ep_dir = self._collected["endpoints"]
        req = response.request
        url_key = normalize_url(response.url)
        base = build_collected_endpoint_basename(response.url, role)
        payload: dict[str, Any] = {
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "role": role,
            "url": url_key,
            "request": {
                "method": req.method,
                "url": req.url,
                "headers": req.headers.to_unicode_dict(),
                **encode_body_for_evidence(req.body or b""),
            },
            "response": {
                "status": response.status,
                "url": response.url,
                "headers": response.headers.to_unicode_dict(),
                **encode_body_for_evidence(response.body or b""),
            },
        }
        if extra:
            payload["extra"] = extra
        out_path = ep_dir / f"{base}.json"
        save_json_file(out_path, payload)

    def _append_cookie_record(self, response: scrapy.http.Response, page_url: str) -> None:
        host = (urlparse(page_url).hostname or "unknown").strip().lower()
        host_slug = _slug_file_component(host, "host")
        cookies_path = self._collected["cookies"] / f"set_cookie_{host_slug}.jsonl"
        raw_headers = response.headers.getlist("Set-Cookie") or response.headers.getlist(b"Set-Cookie")
        lines: list[str] = []
        for h in raw_headers:
            if isinstance(h, bytes):
                lines.append(h.decode("latin-1", errors="replace"))
            else:
                lines.append(str(h))
        if not lines:
            return
        append_jsonl_file(
            cookies_path.resolve(),
            {
                "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                "page_url": normalize_url(page_url),
                "response_url": normalize_url(response.url),
                "set_cookie": lines,
            },
        )

    def _save_forms_for_page(self, response: scrapy.http.Response, page_url: str) -> None:
        forms_dir = self._collected["forms"]
        forms = extract_form_payloads_from_response(response, page_url)
        page_label = url_filesystem_label(page_url)[:80]
        used_names: dict[str, int] = {}
        for spec in forms:
            key = spec.get("form_file_key") or "form"
            used_names[key] = used_names.get(key, 0) + 1
            suffix = "" if used_names[key] == 1 else f"_{used_names[key]}"
            action = str(spec.get("action_url") or "")
            action_key = _url_fingerprint_component(action)[:10] if action else "noaction"
            fname = f"{page_label}_form_{key}_{action_key}{suffix}.json"
            path = forms_dir / fname
            save_json_file(path, spec)

    def _save_inline_scripts(self, response: scrapy.http.Response, page_url: str) -> list[tuple[str, set[str]]]:
        scripts_dir = self._collected["scripts"]
        out: list[tuple[str, set[str]]] = []
        idx = 0
        for sel in response.css("script:not([src])"):
            body = "".join(sel.xpath("string()").getall()).strip()
            if len(body) < 2:
                continue
            idx += 1
            digest = hashlib.sha256(body.encode("utf-8", errors="ignore")).hexdigest()[:12]
            sid = (sel.css("::attr(id)").get() or "").strip()
            sname = (sel.css("::attr(name)").get() or "").strip()
            ext = infer_inline_script_extension(sel)
            stem_raw = sid or sname or f"inline_{idx}"
            stem = _slug_file_component(stem_raw, f"inline_{idx}")[:80]
            fname = f"{stem}_{digest}{ext}"
            out_path = scripts_dir / fname
            header = f"// source_page: {page_url}\n"
            out_path.write_text(header + body, encoding="utf-8", errors="replace")
            discovered = extract_urls_from_javascript_for_discovery(body, page_url)
            self._record_js_endpoint_file(page_url, fname, discovered)
            out.append((fname, discovered))
        return out

    def _record_js_endpoint_file(self, parent_url: str, script_name: str, urls: set[str]) -> None:
        js_dir = self._collected["js_endpoints"]
        page_label = url_filesystem_label(parent_url)[:48]
        stem = Path(script_name).stem
        stem_slug = _slug_file_component(stem, "script")[:80]
        key = _url_fingerprint_component(normalize_url(parent_url) + "|" + script_name)[:12]
        path = js_dir / f"{page_label}_jsurls_{stem_slug}_{key}.json"
        save_json_file(
            path,
            {
                "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                "parent_page_url": normalize_url(parent_url),
                "script_storage_name": script_name,
                "urls_extracted": sorted(urls),
            },
        )

    def _yield_discoveries_from_script(
        self,
        script_page_url: str,
        urls: set[str],
        script_label: str,
        *,
        evidence_response: scrapy.http.Response | None = None,
    ):
        for discovered_url in sorted(urls):
            if not is_allowed_domain(discovered_url, self.root_domain):
                continue
            is_new = discovered_url not in self.state.discovered_urls
            self.state.link_graph.setdefault(script_page_url, set()).add(discovered_url)
            existing_record = ensure_inventory_record(self.state, discovered_url)
            if "observed_in_script_file" not in existing_record.discovered_via:
                er = evidence_response or self._make_script_discovery_fake_response(script_page_url, discovered_url)
                evidence_payload = build_scrapy_discovery_evidence(
                    response=er,
                    discovered_url=discovered_url,
                    source_type="observed_in_script_file",
                )
                evidence_file = save_evidence(self.evidence_dir, discovered_url, "observed_in_script_file", evidence_payload)
            else:
                evidence_file = ""
            register_url_discovery(
                state=self.state,
                url=discovered_url,
                source_type="observed_in_script_file",
                discovered_from=normalize_url(script_page_url),
                evidence_file=evidence_file,
            )
            if is_new:
                self._emit_verbose(f"Discovered URL via observed_in_script_file ({script_label}): {discovered_url}")
            self._emit_nonverbose_progress_if_needed()
            if (
                discovered_url not in self.state.visited_urls
                and len(self.state.visited_urls) < self.max_pages
                and should_crawl_url(discovered_url)
            ):
                yield self._build_crawl_request(discovered_url)

    def _make_script_discovery_fake_response(self, script_page_url: str, discovered_url: str) -> scrapy.http.Response:
        from scrapy.http import TextResponse

        return TextResponse(
            url=script_page_url,
            status=200,
            body=json.dumps({"discovered_from_script": True, "target": discovered_url}).encode("utf-8"),
            encoding="utf-8",
        )

    def _maybe_save_artifact_copy(self, response: scrapy.http.Response, logical_url: str) -> None:
        if response.status >= 400:
            return
        norm = normalize_url(logical_url)
        if norm in self.state.artifacts_saved_urls:
            return
        if not looks_like_standard_artifact_url(norm):
            return
        art_dir = self._collected["artifacts"]
        parsed = urlparse(norm)
        tail = unquote((parsed.path or "/").rstrip("/").rsplit("/", 1)[-1] or "artifact")
        stem, ext = split_path_tail_stem_ext(tail)
        stem_slug = _slug_file_component(stem, "artifact")[:100]
        key = _url_fingerprint_component(norm)[:12]
        if not ext:
            ct = (response.headers.get("Content-Type") or b"").decode("latin-1", errors="ignore").lower()
            if "json" in ct:
                ext = ".json"
            elif "xml" in ct:
                ext = ".xml"
            else:
                ext = ".bin"
        out_path = art_dir / f"{stem_slug}_{key}{ext}"
        body = response.body or b""
        out_path.write_bytes(body)
        meta_path = art_dir / f"{stem_slug}_{key}.meta.json"
        save_json_file(
            meta_path.resolve(),
            {
                "url": norm,
                "saved_path": out_path.name,
                "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                "content_type": (response.headers.get("Content-Type") or b"").decode("latin-1", errors="ignore"),
                "status": response.status,
            },
        )
        self.state.artifacts_saved_urls.add(norm)

    def _schedule_script_fetch(self, script_url: str, page_url: str) -> scrapy.Request | None:
        norm = normalize_url(script_url)
        if norm in self.state.scripts_fetched_urls:
            return None
        if self._script_asset_fetch_count >= self._max_script_asset_fetches:
            self.logger.warning("script asset fetch cap reached (%s); skipping %s", self._max_script_asset_fetches, norm)
            return None
        if not is_allowed_domain(norm, self.root_domain):
            return None
        if not looks_like_downloadable_script_url(norm):
            return None
        self._script_asset_fetch_count += 1
        return scrapy.Request(
            norm,
            callback=self.parse,
            errback=self._handle_script_asset_error,
            meta={
                "handle_httpstatus_all": True,
                "nightmare_role": "script_asset",
                "script_src_page": normalize_url(page_url),
            },
        )

    def _handle_script_asset_error(self, failure) -> None:
        self.logger.debug("Script asset request failed: %s", failure.getErrorMessage())

    def _handle_script_asset_response(self, response: scrapy.http.Response):
        logical = normalize_url(response.url)
        parent = str(response.meta.get("script_src_page") or response.request.url)
        self._persist_endpoint_record(
            response,
            role="script_asset",
            extra={"parent_page_url": normalize_url(parent)},
        )
        if response.status >= 400:
            self._maybe_refresh_collected_data_rollup()
            return
        self.state.scripts_fetched_urls.add(logical)
        body = response.body or b""
        scripts_dir = self._collected["scripts"]
        path_tail = (urlparse(logical).path or "/").rstrip("/").rsplit("/", 1)[-1] or "script"
        stem, ext = split_script_stem_and_extension(path_tail)
        stem_slug = _slug_file_component(stem, "script")[:100]
        key = _url_fingerprint_component(logical)[:12]
        filename = f"{stem_slug}_{key}{ext}"
        out_path = scripts_dir / filename
        out_path.write_bytes(body)
        text = body.decode("utf-8", errors="replace")
        discovered = extract_urls_from_javascript_for_discovery(text, logical)
        self._record_js_endpoint_file(parent, out_path.name, discovered)
        self._maybe_refresh_collected_data_rollup()
        yield from self._yield_discoveries_from_script(
            parent, discovered, out_path.name, evidence_response=response
        )

    async def start(self):
        remaining_budget = max(0, self.max_pages - len(self.state.visited_urls))
        self._emit_verbose(
            "Starting crawl from "
            f"{self.start_url} scoped to root domain {self.root_domain} "
            f"(max_pages={self.max_pages}, already_visited={len(self.state.visited_urls)}, "
            f"remaining_budget={remaining_budget}, seeds={len(self.start_urls)})"
        )

        if remaining_budget <= 0:
            self._emit_verbose("Crawl budget already exhausted from resumed session; no new seeds will be scheduled")
            self._emit_nonverbose_progress_if_needed(force=True)
            return

        scheduled = 0
        for seed_url in self.start_urls:
            if scheduled >= remaining_budget:
                break
            if seed_url in self.state.visited_urls:
                continue
            if not is_allowed_domain(seed_url, self.root_domain):
                continue
            scheduled += 1
            wl_guess = seed_url in self.wordlist_path_seeds
            yield self._build_crawl_request(seed_url, wordlist_path_guess=wl_guess)
        self._emit_nonverbose_progress_if_needed(force=True)

    def parse(self, response: scrapy.http.Response):
        if response.meta.get("nightmare_role") == "script_asset":
            yield from self._handle_script_asset_response(response)
            return

        current_url = normalize_url(response.url)

        if current_url in self.state.visited_urls:
            return

        if len(self.state.visited_urls) >= self.max_pages:
            return

        self.state.visited_urls.add(current_url)
        record = ensure_inventory_record(self.state, current_url)
        record.crawl_requested = True
        record.was_crawled = True
        record.crawl_status_code = response.status
        soft_404_detected, soft_404_reason = detect_soft_not_found_response(response.status, response.body)
        record.soft_404_detected = bool(soft_404_detected)
        record.soft_404_reason = soft_404_reason
        if soft_404_detected:
            record.exists_confirmed = False
            record.existence_status_code = response.status
            record.existence_check_method = "GET"
            record.existence_check_note = f"Soft-404 during crawl: {soft_404_reason}"
            record.crawl_note = f"Soft-404 response (HTTP {response.status}): {soft_404_reason}"
        if response.status >= 400:
            record.crawl_note = f"Crawl request returned HTTP {response.status}"

        self._emit_verbose(
            "Crawled "
            f"{current_url} ({len(self.state.visited_urls)}/{self.max_pages} visited, "
            f"{len(self.state.discovered_urls)} discovered, HTTP {response.status})"
        )

        if "crawl_response" not in record.discovered_via:
            evidence_payload = build_scrapy_discovery_evidence(
                response=response,
                discovered_url=current_url,
                source_type="crawl_response",
            )
            evidence_file = save_evidence(self.evidence_dir, current_url, "crawl_response", evidence_payload)
            register_url_discovery(
                state=self.state,
                url=current_url,
                source_type="crawl_response",
                discovered_from=normalize_url(response.request.url),
                evidence_file=evidence_file,
            )

        try:
            self._persist_endpoint_record(response, role="page_visit")
            self._append_cookie_record(response, current_url)
            self._maybe_save_artifact_copy(response, current_url)
        except Exception as coll_exc:
            self.logger.warning("collected_data snapshot failed: %s", coll_exc)

        if 300 <= response.status < 400:
            location_header = (response.headers.get("Location") or b"").decode("latin-1", errors="ignore").strip()
            if location_header:
                redirect_url = normalize_url(urljoin(response.url, location_header))
                if is_allowed_domain(redirect_url, self.root_domain):
                    self.state.link_graph[current_url].add(redirect_url)
                    redirect_record = ensure_inventory_record(self.state, redirect_url)
                    redirect_evidence_payload = build_scrapy_discovery_evidence(
                        response=response,
                        discovered_url=redirect_url,
                        source_type="redirect_location",
                    )
                    redirect_evidence_file = save_evidence(
                        self.evidence_dir,
                        redirect_url,
                        "redirect_location",
                        redirect_evidence_payload,
                    )
                    register_url_discovery(
                        state=self.state,
                        url=redirect_url,
                        source_type="redirect_location",
                        discovered_from=current_url,
                        evidence_file=redirect_evidence_file,
                    )
                    self._emit_verbose(
                        f"Discovered redirect target via HTTP {response.status}: {redirect_url} (from {current_url})"
                    )
                    self._emit_nonverbose_progress_if_needed()
                    if (
                        redirect_url not in self.state.visited_urls
                        and len(self.state.visited_urls) < self.max_pages
                        and should_crawl_url(redirect_url)
                    ):
                        yield self._build_crawl_request(redirect_url)
                    redirect_record.crawl_note = f"Discovered via redirect from {current_url} (HTTP {response.status})"

        if response.status >= 400:
            self.logger.warning("Blocked or error crawl response at %s (HTTP %s)", current_url, response.status)
            self._maybe_refresh_collected_data_rollup()
            self._save_session_state()
            return
        if soft_404_detected:
            self.logger.info("Soft-404 crawl response at %s (HTTP %s): %s", current_url, response.status, soft_404_reason)
            self._maybe_refresh_collected_data_rollup()
            self._save_session_state()
            return

        if not is_markup_response(response):
            self._emit_verbose(f"Skipping link extraction for non-markup response at {current_url}")
            self._maybe_refresh_collected_data_rollup()
            self._save_session_state()
            return

        candidates, lightweight_extraction = extract_discovery_candidates(response)
        if lightweight_extraction and self.progress is not None:
            self.progress.info(
                f"Large HTML ({len(response.body or b'')} bytes) at {current_url}: "
                "using fast link extraction only (anchors and forms); crawl continues."
            )
        source_counts = ", ".join(
            f"{source_type}={len(urls)}" for source_type, urls in sorted(candidates.items())
        ) or "none"
        self._emit_verbose(f"Raw discovery candidates on {current_url}: {source_counts}")

        try:
            self._save_forms_for_page(response, current_url)
            for fname, disc in self._save_inline_scripts(response, current_url):
                yield from self._yield_discoveries_from_script(
                    current_url, disc, fname, evidence_response=response
                )
            for href in response.css("script::attr(src)").getall():
                raw = (href or "").strip()
                if not raw:
                    continue
                script_u = normalize_url(urljoin(response.url, raw))
                req = self._schedule_script_fetch(script_u, current_url)
                if req is not None:
                    yield req
        except Exception as coll_exc:
            self.logger.warning("collected_data HTML enrichment failed: %s", coll_exc)

        allowed_counts: dict[str, int] = defaultdict(int)
        for source_type, discovered_urls in candidates.items():
            for discovered_url in sorted(discovered_urls):
                if not is_allowed_domain(discovered_url, self.root_domain):
                    continue

                is_new_url = discovered_url not in self.state.discovered_urls
                allowed_counts[source_type] += 1
                self.state.link_graph[current_url].add(discovered_url)
                existing_record = ensure_inventory_record(self.state, discovered_url)

                if source_type not in existing_record.discovered_via:
                    evidence_payload = build_scrapy_discovery_evidence(
                        response=response,
                        discovered_url=discovered_url,
                        source_type=source_type,
                    )
                    evidence_file = save_evidence(self.evidence_dir, discovered_url, source_type, evidence_payload)
                else:
                    evidence_file = ""

                register_url_discovery(
                    state=self.state,
                    url=discovered_url,
                    source_type=source_type,
                    discovered_from=current_url,
                    evidence_file=evidence_file,
                )

                if is_new_url:
                    self._emit_verbose(
                        f"Discovered URL via {source_type}: {discovered_url} (from {current_url})"
                    )
                    self._emit_nonverbose_progress_if_needed()

                if (
                    discovered_url not in self.state.visited_urls
                    and len(self.state.visited_urls) < self.max_pages
                    and should_crawl_url(discovered_url)
                ):
                    yield self._build_crawl_request(discovered_url)

        allowed_summary = ", ".join(
            f"{source_type}={count}" for source_type, count in sorted(allowed_counts.items())
        ) or "none"
        self._emit_verbose(f"Allowed internal discovery on {current_url}: {allowed_summary}")
        self._emit_nonverbose_progress_if_needed()

        self._save_session_state()

    def handle_request_failure(self, failure) -> None:
        request = getattr(failure, "request", None)
        if request is None:
            self.logger.warning("Crawl request failed without request context: %s", failure)
            return

        current_url = normalize_url(request.url)
        record = ensure_inventory_record(self.state, current_url)
        record.crawl_requested = True
        response = getattr(getattr(failure, "value", None), "response", None)
        status_code = getattr(response, "status", None)
        if status_code is not None:
            record.crawl_status_code = int(status_code)
            record.crawl_note = f"Crawl request failed before parsing with HTTP {status_code}"
        else:
            record.crawl_note = f"Crawl request failed: {failure.getErrorMessage()}"

        evidence_payload = build_scrapy_request_failure_evidence(
            request=request,
            source_type="crawl_failure",
            note=record.crawl_note or "Crawl request failed",
        )
        evidence_file = save_evidence(self.evidence_dir, current_url, "crawl_failure", evidence_payload)
        register_url_discovery(
            state=self.state,
            url=current_url,
            source_type="crawl_failure",
            discovered_from=normalize_url(request.url),
            evidence_file=evidence_file,
        )

        self.logger.warning("%s (%s)", record.crawl_note, current_url)
        self._save_session_state()

    def closed(self, reason: str) -> None:
        self._emit_nonverbose_progress_if_needed(force=True)
        self._maybe_refresh_collected_data_rollup(force=True)
        self._save_session_state()

    def _save_session_state(self) -> None:
        if self.session_state_path is None:
            return
        save_session_state(
            session_state_path=self.session_state_path,
            start_url=self.start_url,
            root_domain=self.root_domain,
            max_pages=self.max_pages,
            state=self.state,
        )


def build_sitemap(start_url: str, state: CrawlState) -> dict[str, Any]:
    normalized_graph: dict[str, set[str]] = defaultdict(set)
    for source, targets in state.link_graph.items():
        normalized_source = normalize_url_for_sitemap(source)
        for target in targets:
            normalized_target = normalize_url_for_sitemap(target)
            normalized_graph[normalized_source].add(normalized_target)

    normalized_discovered_urls = {normalize_url_for_sitemap(url) for url in state.discovered_urls}
    normalized_discovered_urls.update(normalized_graph.keys())
    for targets in normalized_graph.values():
        normalized_discovered_urls.update(targets)

    inbound_links: dict[str, set[str]] = defaultdict(set)
    for source, targets in normalized_graph.items():
        for target in targets:
            inbound_links[target].add(source)

    page_entries = []
    for url in sorted(normalized_discovered_urls):
        outbounds = sorted(normalized_graph.get(url, set()))
        page_entries.append(
            {
                "url": url,
                "inbound_count": len(inbound_links.get(url, set())),
                "outbound_count": len(outbounds),
                "outbound_internal_links": outbounds,
            }
        )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "start_url": normalize_url_for_sitemap(start_url),
        "pages_crawled": len(state.visited_urls),
        "unique_urls_discovered": len(normalized_discovered_urls),
        "urls": sorted(normalized_discovered_urls),
        "pages": page_entries,
    }


def build_url_inventory(root_domain: str, state: CrawlState) -> dict[str, Any]:
    all_entries = [state.url_inventory[url].to_dict() for url in sorted(state.url_inventory.keys())]
    entries = [
        state.url_inventory[url].to_dict()
        for url in sorted(state.url_inventory.keys())
        if is_effective_existing_record(state.url_inventory.get(url))
    ]
    soft_404_count = sum(
        1
        for url in state.url_inventory.keys()
        if bool(getattr(state.url_inventory.get(url), "soft_404_detected", False))
    )
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root_domain": root_domain,
        "total_urls_raw": len(all_entries),
        "total_urls": len(entries),
        "total_urls_effective": len(entries),
        "soft_404_excluded": soft_404_count,
        "entries_raw": all_entries,
        "entries": entries,
    }


def was_crawl_requested(record: UrlInventoryRecord) -> bool:
    if record.crawl_requested or record.was_crawled or record.crawl_status_code is not None:
        return True
    crawl_note = str(record.crawl_note or "").lower()
    return "crawl request " in crawl_note


def derive_exists_for_requested_url(record: UrlInventoryRecord) -> bool:
    if bool(record.soft_404_detected):
        return False
    if bool(record.exists_confirmed):
        return True

    for raw_status in (record.existence_status_code, record.crawl_status_code):
        try:
            status = int(raw_status) if raw_status is not None else None
        except (TypeError, ValueError):
            status = None
        if status is None:
            continue
        return status not in configured_not_found_statuses()

    return False


def build_requests_inventory(root_domain: str, state: CrawlState) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    direct_plus_inferred = DIRECT_DISCOVERY_SOURCES | INFERENCE_DISCOVERY_SOURCES | {"crawl_failure"}
    inferred_sources = direct_plus_inferred - DIRECT_DISCOVERY_SOURCES - GUESSED_DISCOVERY_SOURCES - {"seed_input"}

    for url in sorted(state.url_inventory.keys()):
        record = state.url_inventory[url]
        if not was_crawl_requested(record):
            continue
        discovered_via = set(record.discovered_via)
        guessed = bool(discovered_via & GUESSED_DISCOVERY_SOURCES)
        found_directly = bool(discovered_via & DIRECT_DISCOVERY_SOURCES)
        inferred = bool(discovered_via & inferred_sources)
        entries.append(
            {
                "url": record.url,
                "found_directly": found_directly,
                "guessed": guessed,
                "inferred": inferred,
                "exists": derive_exists_for_requested_url(record),
                "crawl_status_code": record.crawl_status_code,
                "existence_status_code": record.existence_status_code,
                "soft_404_detected": bool(record.soft_404_detected),
                "discovered_via": sorted(discovered_via),
            }
        )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root_domain": root_domain,
        "total_requested_urls": len(entries),
        "entries": entries,
    }


def _compress_condensed_node(node: CondensedTreeNode, label: str) -> dict[str, Any]:
    parts = [label]
    current = node

    while (
        not current.is_page
        and len(current.query_shapes) == 0
        and len(current.children) == 1
    ):
        next_label = next(iter(current.children.keys()))
        current = current.children[next_label]
        parts.append(next_label)

    path_label = "/".join(part for part in parts if part and part != "/")
    if not path_label:
        path_label = "/"

    serialized_children = [
        _compress_condensed_node(child_node, child_label)
        for child_label, child_node in sorted(current.children.items())
    ]

    payload: dict[str, Any] = {"path": path_label}
    if current.is_page:
        payload["is_page"] = True
    if current.query_shapes:
        payload["query_shapes"] = sorted(current.query_shapes)
    if serialized_children:
        payload["children"] = serialized_children
    return payload


def build_condensed_sitemap(root_domain: str, sitemap: dict[str, Any]) -> dict[str, Any]:
    raw_urls = sitemap.get("urls", [])
    urls = sorted({normalize_url_for_sitemap(url) for url in raw_urls if isinstance(url, str) and url.strip()})
    host_roots: dict[str, CondensedTreeNode] = {}

    for url in urls:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if not host:
            continue

        host_node = host_roots.setdefault(host, CondensedTreeNode())
        path_segments = [normalize_path_segment_for_condensed_tree(seg) for seg in (parsed.path or "/").split("/") if seg]

        current = host_node
        if not path_segments:
            current.is_page = True
        else:
            for segment in path_segments:
                current = current.children.setdefault(segment, CondensedTreeNode())
            current.is_page = True

        if parsed.query:
            query_shape = "&".join(sorted(key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)))
            if query_shape:
                current.query_shapes.add(query_shape)

    hosts_payload = []
    for host in sorted(host_roots.keys()):
        tree_root = host_roots[host]
        children = [
            _compress_condensed_node(child_node, child_label)
            for child_label, child_node in sorted(tree_root.children.items())
        ]
        hosts_payload.append(
            {
                "host": host,
                "is_page": tree_root.is_page,
                "children": children,
            }
        )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root_domain": root_domain,
        "source_sitemap_url_count": len(raw_urls) if isinstance(raw_urls, list) else 0,
        "distinct_url_count": len(urls),
        "host_count": len(hosts_payload),
        "tree": hosts_payload,
    }


def infer_api_endpoints(urls: list[str]) -> list[str]:
    candidates: set[str] = set()
    api_markers = {"api", "graphql", "rpc", "rest", "v1", "v2", "v3"}

    for raw_url in urls:
        parsed = urlparse(raw_url)
        path = parsed.path or "/"
        segments = [segment.lower() for segment in path.split("/") if segment]

        has_api_marker = any(marker in segments for marker in api_markers)
        has_api_prefix = path.lower().startswith("/api/")
        if has_api_marker or has_api_prefix:
            candidates.add(normalize_url_for_sitemap(raw_url))

    return sorted(candidates)


def merge_query_types(types: set[str]) -> str:
    if not types:
        return "string"

    normalized = {value.strip().lower() for value in types if value}
    if not normalized:
        return "string"
    if len(normalized) == 1:
        return next(iter(normalized))

    if "string" in normalized:
        return "string"
    if "float" in normalized and "int" in normalized:
        return "float"

    priority = ["uuid", "datetime", "date", "email", "url", "bool", "float", "int", "hex", "token", "empty"]
    for item in priority:
        if item in normalized:
            return item

    return "string"


def build_endpoint_schema_payload(urls: list[str]) -> dict[str, Any]:
    host_to_path_params: dict[str, dict[str, dict[str, set[str]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    raw_unique = set(condense_urls_for_ai(urls))

    for raw_url in raw_unique:
        parsed = urlparse(raw_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue

        host = parsed.netloc.lower()
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/")
            if not path:
                path = "/"
        _ = host_to_path_params[host][path]

        query_items = parse_qsl(parsed.query, keep_blank_values=True)
        for key, value in query_items:
            if not key:
                continue
            host_to_path_params[host][path][key].add(infer_query_value_type(value))

    host_routes: dict[str, list[str]] = {}
    endpoint_count = 0
    for host in sorted(host_to_path_params.keys()):
        route_rows: list[str] = []
        for path in sorted(host_to_path_params[host].keys()):
            query_schema = host_to_path_params[host][path]
            if not query_schema:
                route_rows.append(path)
                endpoint_count += 1
                continue

            keys = sorted(query_schema.keys())
            query = "&".join(
                f"{key}={{{merge_query_types(query_schema[key])}}}" for key in keys
            )
            route_rows.append(f"{path}?{query}")
            endpoint_count += 1
        host_routes[host] = route_rows

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "unique_url_count": len(raw_unique),
        "endpoint_schema_count": endpoint_count,
        "hosts": host_routes,
    }


def render_endpoint_schema_text(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# nightmare ai data")
    lines.append(f"generated_at_utc: {payload.get('generated_at_utc', '')}")
    lines.append(f"unique_url_count: {payload.get('unique_url_count', 0)}")
    lines.append(f"endpoint_schema_count: {payload.get('endpoint_schema_count', 0)}")
    lines.append("")

    hosts = payload.get("hosts", {})
    if isinstance(hosts, dict):
        for host in sorted(hosts.keys()):
            lines.append(f"[{host}]")
            routes = hosts.get(host, [])
            if isinstance(routes, list):
                for route in routes:
                    lines.append(str(route))
            lines.append("")

    return "\n".join(lines).strip() + "\n"


def compact_endpoint_schema_for_ai(
    payload: dict[str, Any],
    max_total_routes: int,
    *,
    truncated_for_ai: bool,
) -> dict[str, Any]:
    hosts = payload.get("hosts", {})
    if not isinstance(hosts, dict):
        return {"hosts": {}, "unique_url_count": 0, "endpoint_schema_count": 0, "truncated_for_ai": truncated_for_ai}

    remaining = max(0, max_total_routes)
    compact_hosts: dict[str, list[str]] = {}
    endpoint_count = 0

    for host in sorted(hosts.keys()):
        if remaining <= 0:
            break
        routes = hosts.get(host, [])
        if not isinstance(routes, list):
            continue

        def _route_rank(route: str) -> tuple[int, int, str]:
            value = str(route).lower()
            api_bias = 0 if ("/api" in value or "graphql" in value or "rpc" in value) else 1
            return (api_bias, len(value), value)

        ranked = sorted((str(route) for route in routes), key=_route_rank)
        selected = ranked[:remaining]
        if selected:
            compact_hosts[host] = selected
            endpoint_count += len(selected)
            remaining -= len(selected)

    return {
        "generated_at_utc": payload.get("generated_at_utc"),
        "unique_url_count": payload.get("unique_url_count"),
        "endpoint_schema_count": endpoint_count,
        "truncated_for_ai": truncated_for_ai,
        "hosts": compact_hosts,
    }


def infer_endpoint_fuzz_methods(observed_methods: set[str]) -> list[str]:
    ordered = ["GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"]
    normalized_observed = {method.upper() for method in observed_methods if method}
    prioritized = [method for method in ordered if method in normalized_observed]
    for method in ordered:
        if method not in prioritized:
            prioritized.append(method)
    return prioritized


def collect_parameter_observations(
    urls: list[str],
) -> dict[tuple[str, str], dict[str, dict[str, set[str]]]]:
    observations: dict[tuple[str, str], dict[str, dict[str, set[str]]]] = defaultdict(
        lambda: defaultdict(lambda: {"types": set(), "values": set()})
    )
    for raw_url in filter_urls_for_source_of_truth(urls):
        parsed = urlparse(raw_url)
        host = parsed.netloc.lower()
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/") or "/"
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if not key:
                continue
            observations[(host, path)][key]["types"].add(infer_query_value_type(value))
            observations[(host, path)][key]["values"].add(value)
    return observations


def build_source_of_truth_parameter_entries(urls: list[str]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for raw_url in sorted(set(filter_urls_for_source_of_truth(urls))):
        parsed = urlparse(raw_url)
        params = parse_qsl(parsed.query, keep_blank_values=True)
        if not params:
            continue
        parameter_rows = []
        grouped: dict[str, dict[str, set[str]]] = defaultdict(lambda: {"types": set(), "values": set()})
        for key, value in params:
            grouped[key]["types"].add(infer_query_value_type(value))
            grouped[key]["values"].add(value)
        for key in sorted(grouped.keys()):
            observed_values = sorted({value for value in grouped[key]["values"] if value is not None})
            data_type = merge_query_types(grouped[key]["types"])
            parameter_rows.append(
                {
                    "name": key,
                    "observed_data_types": sorted(grouped[key]["types"]),
                    "observed_values": observed_values,
                    "canonical_data_type": data_type,
                }
            )
        entries.append(
            {
                "url": raw_url,
                "host": parsed.netloc.lower(),
                "path": parsed.path or "/",
                "parameters": parameter_rows,
            }
        )
    return entries


def build_source_of_truth_api_endpoint_entries(
    state: CrawlState,
    urls: list[str],
) -> list[dict[str, Any]]:
    parameter_observations = collect_parameter_observations(urls)
    observed_methods_by_route: dict[tuple[str, str], set[str]] = defaultdict(set)
    for raw_url, record in state.url_inventory.items():
        normalized_url = normalize_url(raw_url)
        if not should_include_url_in_source_of_truth(normalized_url):
            continue
        parsed = urlparse(normalized_url)
        host = parsed.netloc.lower()
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/") or "/"
        if record.was_crawled:
            observed_methods_by_route[(host, path)].add("GET")
        if record.existence_check_method:
            observed_methods_by_route[(host, path)].add(str(record.existence_check_method).upper())

    entries: list[dict[str, Any]] = []
    candidate_urls = infer_api_endpoints(filter_urls_for_source_of_truth(urls))

    for endpoint_url in candidate_urls:
        parsed = urlparse(endpoint_url)
        host = parsed.netloc.lower()
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/") or "/"
        observed_methods = set(observed_methods_by_route.get((host, path), set()))

        query_rows = []
        for key in sorted(parameter_observations.get((host, path), {}).keys()):
            observed = parameter_observations[(host, path)][key]
            types = observed["types"]
            data_type = merge_query_types(types)
            query_rows.append(
                {
                    "name": key,
                    "observed_data_types": sorted(types),
                    "observed_values": sorted({value for value in observed["values"] if value is not None}),
                    "canonical_data_type": data_type,
                }
            )

        entries.append(
            {
                "endpoint": endpoint_url,
                "host": host,
                "path": path,
                "observed_methods": sorted(observed_methods),
                "observed_parameters": query_rows,
                "fuzz_methods_to_try": infer_endpoint_fuzz_methods(observed_methods),
            }
        )

    return entries


def merge_source_of_truth_payload(existing: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    merged = {
        "generated_at_utc": existing.get("generated_at_utc") or current.get("generated_at_utc"),
        "updated_at_utc": current.get("updated_at_utc") or current.get("generated_at_utc"),
        "root_domain": current.get("root_domain") or existing.get("root_domain"),
        "observed_run_timestamps_utc": sorted(
            {
                *(existing.get("observed_run_timestamps_utc", []) if isinstance(existing.get("observed_run_timestamps_utc"), list) else []),
                *(current.get("observed_run_timestamps_utc", []) if isinstance(current.get("observed_run_timestamps_utc"), list) else []),
            }
        ),
    }

    merged_unique_urls = sorted(
        {
            *(existing.get("unique_urls", []) if isinstance(existing.get("unique_urls"), list) else []),
            *(current.get("unique_urls", []) if isinstance(current.get("unique_urls"), list) else []),
        }
    )
    merged["unique_urls"] = merged_unique_urls

    def _merge_named_rows(existing_rows: Any, current_rows: Any, key_name: str) -> list[dict[str, Any]]:
        table: dict[str, dict[str, Any]] = {}
        for row in (existing_rows if isinstance(existing_rows, list) else []):
            if isinstance(row, dict) and isinstance(row.get(key_name), str):
                table[row[key_name]] = dict(row)
        for row in (current_rows if isinstance(current_rows, list) else []):
            if not isinstance(row, dict) or not isinstance(row.get(key_name), str):
                continue
            key = row[key_name]
            if key not in table:
                table[key] = dict(row)
                continue
            merged_row = dict(table[key])
            for field, value in row.items():
                if isinstance(value, list) and isinstance(merged_row.get(field), list):
                    merged_row[field] = sorted({*(str(v) for v in merged_row[field]), *(str(v) for v in value)})
                elif isinstance(value, dict) and isinstance(merged_row.get(field), dict):
                    sub = dict(merged_row[field])
                    for subkey, subvalue in value.items():
                        if isinstance(subvalue, list) and isinstance(sub.get(subkey), list):
                            sub[subkey] = sorted({*(str(v) for v in sub[subkey]), *(str(v) for v in subvalue)})
                        else:
                            sub[subkey] = subvalue
                    merged_row[field] = sub
                else:
                    merged_row[field] = value
            table[key] = merged_row
        return [table[key] for key in sorted(table.keys())]

    merged["parameterized_urls"] = _merge_named_rows(
        existing.get("parameterized_urls"),
        current.get("parameterized_urls"),
        "url",
    )
    merged["likely_api_endpoints"] = _merge_named_rows(
        existing.get("likely_api_endpoints"),
        current.get("likely_api_endpoints"),
        "endpoint",
    )
    return merged


def _dedupe_parameter_rows(parameters: Any) -> list[dict[str, Any]]:
    """One row per parameter name; union observed values and types."""
    if not isinstance(parameters, list):
        return []
    by_name: dict[str, dict[str, Any]] = {}
    for row in parameters:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        if not name:
            continue
        types_in = row.get("observed_data_types", [])
        if not isinstance(types_in, list):
            types_in = []
        type_set = {str(t).strip().lower() for t in types_in if str(t).strip()}
        if not type_set and isinstance(row.get("canonical_data_type"), str):
            ct = str(row.get("canonical_data_type", "")).strip().lower()
            if ct:
                type_set.add(ct)
        values_in = row.get("observed_values", [])
        value_set = {str(v) for v in (values_in if isinstance(values_in, list) else []) if v is not None}
        if name not in by_name:
            by_name[name] = {
                "name": name,
                "observed_data_types": sorted(type_set),
                "observed_values": sorted(value_set),
                "canonical_data_type": merge_query_types(set(type_set) if type_set else {"string"}),
            }
            continue
        cur = by_name[name]
        prev_types = set(cur.get("observed_data_types", [])) if isinstance(cur.get("observed_data_types"), list) else set()
        merged_types = {str(t).strip().lower() for t in prev_types if str(t).strip()} | type_set
        cur["observed_data_types"] = sorted(merged_types)
        prev_vals = set(cur.get("observed_values", [])) if isinstance(cur.get("observed_values"), list) else set()
        cur["observed_values"] = sorted({str(v) for v in prev_vals | value_set})
        cur["canonical_data_type"] = merge_query_types(merged_types if merged_types else {"string"})
    return [by_name[key] for key in sorted(by_name.keys())]


def deduplicate_parameter_inventory_entries(entries: Any) -> list[dict[str, Any]]:
    """Unique ``url`` keys (normalized); merged ``parameters`` without duplicate names."""
    if not isinstance(entries, list):
        return []
    by_url: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        raw_url = str(entry.get("url", "")).strip()
        if not raw_url:
            continue
        norm = normalize_url(raw_url)
        parsed = urlparse(norm)
        host = parsed.netloc.lower()
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/") or "/"
        row_params = _dedupe_parameter_rows(entry.get("parameters"))
        if norm not in by_url:
            merged = dict(entry)
            merged["url"] = norm
            merged["host"] = host
            merged["path"] = path
            merged["parameters"] = row_params
            by_url[norm] = merged
            continue
        existing = by_url[norm]
        combined_params = _dedupe_parameter_rows(
            (existing.get("parameters") if isinstance(existing.get("parameters"), list) else [])
            + (entry.get("parameters") if isinstance(entry.get("parameters"), list) else [])
        )
        existing["parameters"] = combined_params
    return [by_url[key] for key in sorted(by_url.keys())]


def build_source_of_truth_payload(root_domain: str, state: CrawlState) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    effective_urls = [
        url
        for url, record in state.url_inventory.items()
        if is_effective_existing_record(record)
    ]
    source_urls = sorted(set(filter_urls_for_source_of_truth(effective_urls)))
    return {
        "generated_at_utc": now,
        "updated_at_utc": now,
        "root_domain": root_domain,
        "observed_run_timestamps_utc": [now],
        "unique_urls": source_urls,
        "parameterized_urls": build_source_of_truth_parameter_entries(source_urls),
        "likely_api_endpoints": build_source_of_truth_api_endpoint_entries(state, source_urls),
    }


def build_parameter_inventory_payload(source_of_truth_payload: dict[str, Any]) -> dict[str, Any]:
    raw_entries = source_of_truth_payload.get("parameterized_urls", [])
    return {
        "generated_at_utc": source_of_truth_payload.get("updated_at_utc") or source_of_truth_payload.get("generated_at_utc"),
        "root_domain": source_of_truth_payload.get("root_domain"),
        "entries": deduplicate_parameter_inventory_entries(raw_entries),
    }


def render_parameter_placeholders_text(source_of_truth_payload: dict[str, Any]) -> str:
    lines: list[str] = []
    parameterized_urls = source_of_truth_payload.get("parameterized_urls", [])
    if not isinstance(parameterized_urls, list):
        return ""

    for entry in parameterized_urls:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url", "")).strip()
        if not url:
            continue
        parsed = urlparse(url)
        parameters = entry.get("parameters", [])
        if not isinstance(parameters, list) or not parameters:
            continue

        type_counters: dict[str, int] = defaultdict(int)
        placeholder_pairs: list[tuple[str, str]] = []
        for parameter in parameters:
            if not isinstance(parameter, dict):
                continue
            name = str(parameter.get("name", "")).strip()
            if not name:
                continue
            data_type = str(parameter.get("canonical_data_type", "string")).strip().lower() or "string"
            type_counters[data_type] += 1
            placeholder_pairs.append((name, f"{{{data_type}{type_counters[data_type]}}}"))

        if not placeholder_pairs:
            continue

        query = urlencode(placeholder_pairs, doseq=True)
        request_string = urlunparse(parsed._replace(query=query))
        lines.append(request_string)

    return "\n".join(lines).strip() + ("\n" if lines else "")


def apply_type_placeholder_defaults(url: str) -> str:
    replacements = {
        "int": "1",
        "float": "1.0",
        "bool": "true",
        "uuid": "00000000-0000-4000-8000-000000000000",
        "date": "2024-01-01",
        "datetime": "2024-01-01T00:00:00Z",
        "email": "test@example.com",
        "url": "https://example.com",
        "hex": "deadbeef",
        "token": "sampletoken",
        "string": "test",
        "empty": "",
    }

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1).strip().lower()
        return replacements.get(key, "test")

    return re.sub(r"\{([A-Za-z0-9_]+)\}", _replace, url)


def build_sitemap_tree(urls: list[str]) -> dict[str, Any]:
    tree: dict[str, Any] = {"children": {}}
    for raw_url in sorted(urls):
        parsed = urlparse(raw_url)
        host = parsed.netloc or "(unknown-host)"
        path = parsed.path or "/"
        segments = [segment for segment in path.split("/") if segment]
        if not segments:
            segments = ["/"]
        if parsed.query:
            segments.append(f"?{parsed.query}")

        host_url = urlunparse((parsed.scheme or "https", parsed.netloc, "/", "", "", ""))
        node = tree["children"].setdefault(host, {"children": {}, "is_leaf": False, "url": host_url})
        current_path = ""
        for segment in segments:
            if segment == "/":
                current_path = "/"
                node_url = host_url
            elif segment.startswith("?"):
                node_url = urlunparse((parsed.scheme or "https", parsed.netloc, current_path or "/", "", segment[1:], ""))
            else:
                if current_path.endswith("/") or not current_path:
                    current_path = f"{current_path}{segment}"
                else:
                    current_path = f"{current_path}/{segment}"
                node_url = urlunparse((parsed.scheme or "https", parsed.netloc, f"/{current_path.strip('/')}", "", "", ""))

            node = node["children"].setdefault(segment, {"children": {}, "is_leaf": False, "url": node_url})
        node["is_leaf"] = True
        node["url"] = raw_url

    return tree


def build_report_url_metadata(state: CrawlState) -> dict[str, dict[str, str]]:
    metadata: dict[str, dict[str, str]] = {}

    for raw_url, record in state.url_inventory.items():
        canonical_url = normalize_url_for_sitemap(raw_url)
        current = metadata.get(canonical_url, {})
        if "live_url" not in current:
            current["live_url"] = raw_url

        if "file_uri" not in current:
            for evidence_file in record.discovery_evidence_files:
                evidence_path = Path(evidence_file)
                if not evidence_path.is_absolute():
                    evidence_path = (BASE_DIR / evidence_path).resolve()
                else:
                    evidence_path = evidence_path.resolve()
                if evidence_path.exists():
                    current["file_uri"] = evidence_path.as_uri()
                    break

        metadata[canonical_url] = current

    return metadata


def render_url_item(display_url: str, live_url: str, file_uri: str | None = None) -> str:
    display_escaped = html.escape(display_url)
    live_escaped = html.escape(live_url, quote=True)
    link_html = f'<a href="{live_escaped}" target="_blank" rel="noopener noreferrer"><code>{display_escaped}</code></a>'

    if file_uri:
        file_escaped = html.escape(file_uri, quote=True)
        link_html += f' <a class="file-link" href="{file_escaped}" target="_blank" rel="noopener noreferrer">(file)</a>'

    return link_html


def render_tree_html(node: dict[str, Any], metadata: dict[str, dict[str, str]]) -> str:
    children = node.get("children", {})
    if not children:
        return ""

    parts = ["<ul>"]
    for label in sorted(children.keys()):
        child = children[label]
        subtree_html = render_tree_html(child, metadata)
        node_url = child.get("url", "")
        node_meta = metadata.get(node_url, {})
        node_live_url = node_meta.get("live_url", node_url)
        node_file_uri = node_meta.get("file_uri")
        label_text = html.escape(label)
        label_link = (
            f'<a href="{html.escape(node_live_url, quote=True)}" target="_blank" rel="noopener noreferrer">{label_text}</a>'
            if node_live_url
            else label_text
        )
        if node_file_uri:
            label_link += (
                f' <a class="file-link" href="{html.escape(node_file_uri, quote=True)}" '
                'target="_blank" rel="noopener noreferrer">(file)</a>'
            )

        if subtree_html:
            parts.append(f"<li><details open><summary>{label_link}</summary>{subtree_html}</details></li>")
        else:
            parts.append(f"<li>{label_link}</li>")
    parts.append("</ul>")
    return "".join(parts)


def generate_standard_html_report(
    root_domain: str,
    sitemap: dict[str, Any],
    unique_urls: list[str],
    api_endpoints: list[str],
    url_metadata: dict[str, dict[str, str]],
) -> str:
    tree = build_sitemap_tree(unique_urls)
    tree_html = render_tree_html(tree, url_metadata)
    url_items_html = "".join(
        "<li>"
        + render_url_item(
            display_url=url,
            live_url=url_metadata.get(url, {}).get("live_url", url),
            file_uri=url_metadata.get(url, {}).get("file_uri"),
        )
        + "</li>"
        for url in unique_urls
    )
    api_items_html = "".join(
        "<li>"
        + render_url_item(
            display_url=url,
            live_url=url_metadata.get(url, {}).get("live_url", url),
            file_uri=url_metadata.get(url, {}).get("file_uri"),
        )
        + "</li>"
        for url in api_endpoints
    )
    if not api_items_html:
        api_items_html = get_string_config_value(
            MESSAGE_TEMPLATES,
            "standard_report_empty_api_item",
            "<li><em>No likely API endpoints inferred from discovered URLs.</em></li>",
        )

    generated_at = html.escape(str(sitemap.get("generated_at_utc", "")))
    pages_crawled = int(sitemap.get("pages_crawled", 0))
    unique_count = len(unique_urls)

    return render_html_template(
        "standard_report.html",
        PAGE_TITLE=html.escape(render_message("standard_report_page_title", root_domain=root_domain)),
        ROOT_DOMAIN=html.escape(root_domain),
        REPORT_HEADING=html.escape(render_message("standard_report_heading", root_domain=root_domain)),
        GENERATED_AT=generated_at,
        PAGES_CRAWLED=pages_crawled,
        UNIQUE_COUNT=unique_count,
        API_ENDPOINT_COUNT=len(api_endpoints),
        TREE_HTML=tree_html,
        URL_ITEMS_HTML=url_items_html,
        API_ITEMS_HTML=api_items_html,
        LABEL_PAGES_CRAWLED=html.escape(get_string_config_value(MESSAGE_TEMPLATES, "label_pages_crawled", "Pages Crawled")),
        LABEL_UNIQUE_URLS=html.escape(
            get_string_config_value(MESSAGE_TEMPLATES, "label_unique_urls", "Unique URLs (Normalized)")
        ),
        LABEL_API_ENDPOINTS=html.escape(
            get_string_config_value(MESSAGE_TEMPLATES, "label_api_endpoints", "Likely API Endpoints")
        ),
        SECTION_TREE_VIEW=html.escape(get_string_config_value(MESSAGE_TEMPLATES, "section_tree_view", "Tree View Sitemap")),
        SECTION_UNIQUE_URLS=html.escape(get_string_config_value(MESSAGE_TEMPLATES, "section_unique_urls", "Unique URLs")),
        SECTION_API_ENDPOINTS=html.escape(
            get_string_config_value(MESSAGE_TEMPLATES, "section_api_endpoints", "Likely API Endpoints")
        ),
        GENERATED_AT_LABEL=html.escape(get_string_config_value(MESSAGE_TEMPLATES, "generated_at_label", "Generated at")),
    )


def extract_html_document(raw_text: str) -> str:
    text = (raw_text or "").strip()
    if not text:
        return ""

    fenced = re.search(r"```html\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        text = fenced.group(1).strip()

    html_open = text.lower().find("<html")
    html_close = text.lower().rfind("</html>")
    if html_open != -1 and html_close != -1 and html_close > html_open:
        return text[html_open : html_close + len("</html>")]

    if "<!doctype html" in text.lower():
        return text

    return render_html_template(
        "fallback_text_report.html",
        FALLBACK_TITLE=html.escape(get_string_config_value(MESSAGE_TEMPLATES, "fallback_html_title", "Nightmare HTML Report")),
        BODY_TEXT=html.escape(text),
    )


def _format_inline_report_text(value: str) -> str:
    escaped = html.escape(value)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    return escaped


def render_text_report_as_html(report_text: str, title: str, subtitle: str = "") -> str:
    lines = [line.rstrip() for line in (report_text or "").splitlines()]
    content: list[str] = []
    in_list = False

    def close_list() -> None:
        nonlocal in_list
        if in_list:
            content.append("</ul>")
            in_list = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            close_list()
            continue

        numbered_heading = re.match(r"^\d+\)\s+(.+)$", stripped)
        markdown_heading = re.match(r"^#{1,6}\s+(.+)$", stripped)
        bullet = re.match(r"^[-*]\s+(.+)$", stripped)

        if numbered_heading:
            close_list()
            content.append(f"<h2>{_format_inline_report_text(numbered_heading.group(1))}</h2>")
            continue
        if markdown_heading:
            close_list()
            content.append(f"<h2>{_format_inline_report_text(markdown_heading.group(1))}</h2>")
            continue
        if bullet:
            if not in_list:
                content.append("<ul>")
                in_list = True
            content.append(f"<li>{_format_inline_report_text(bullet.group(1))}</li>")
            continue

        close_list()
        content.append(f"<p>{_format_inline_report_text(stripped)}</p>")

    close_list()
    if not content:
        content.append(get_string_config_value(MESSAGE_TEMPLATES, "empty_text_report_html", "<p><em>No report content was returned.</em></p>"))

    generated_at = datetime.now(timezone.utc).isoformat()
    subtitle_html = f"<div class='subtitle'>{html.escape(subtitle)}</div>" if subtitle else ""
    body_html = "".join(content)

    return render_html_template(
        "text_report.html",
        TITLE=html.escape(title),
        SUBTITLE_HTML=subtitle_html,
        GENERATED_AT=html.escape(generated_at),
        BODY_HTML=body_html,
        GENERATED_AT_LABEL=html.escape(get_string_config_value(MESSAGE_TEMPLATES, "generated_at_label", "Generated at")),
    )


def ask_openai_for_html_report(
    client: OpenAI,
    model: str,
    root_domain: str,
    sitemap: dict[str, Any],
    endpoint_schema_payload: dict[str, Any],
    unique_urls: list[str],
    api_endpoints: list[str],
    ai_probe_results: list[dict[str, Any]],
    progress: ProgressReporter | None = None,
    request_timeout_seconds: float = DEFAULT_OPENAI_REQUEST_TIMEOUT_SECONDS,
    truncated_for_ai: bool = True,
) -> str:
    last_error: Exception | None = None
    for prompt_budget in OPENAI_PROMPT_BUDGET_STEPS:
        max_urls = max(80, prompt_budget // 320)
        max_pages = max(40, prompt_budget // 640)
        compact_sitemap: dict[str, Any] | None = None
        compact_unique_urls: list[str] = []
        compact_api_endpoints: list[str] = []
        prompt = ""

        while max_urls >= 1 and max_pages >= 1:
            compact_sitemap = compact_sitemap_for_ai(
                sitemap,
                max_urls=max_urls,
                max_pages=max_pages,
                max_outbound_links_per_page=20,
                truncated_for_ai=truncated_for_ai,
            )
            compact_schema = compact_endpoint_schema_for_ai(
                endpoint_schema_payload,
                max_total_routes=max_urls,
                truncated_for_ai=truncated_for_ai,
            )
            compact_unique_urls = compact_list_for_ai(unique_urls, max_urls)
            compact_api_endpoints = compact_list_for_ai(api_endpoints, max(1, max_urls // 2))
            compact_probe_results = ai_probe_results[: max(10, max_urls // 6)]

            prompt = render_prompt(
                "html_report_prompt",
                ROOT_DOMAIN=root_domain,
                SITEMAP_JSON=json.dumps(compact_sitemap, ensure_ascii=False),
                ENDPOINT_SCHEMA_JSON=json.dumps(compact_schema, ensure_ascii=False),
                UNIQUE_URLS_INCLUDED_COUNT=len(compact_unique_urls),
                UNIQUE_URLS_TOTAL_COUNT=len(unique_urls),
                UNIQUE_URLS_JSON=json.dumps(compact_unique_urls, ensure_ascii=False),
                API_ENDPOINTS_INCLUDED_COUNT=len(compact_api_endpoints),
                API_ENDPOINTS_TOTAL_COUNT=len(api_endpoints),
                API_ENDPOINTS_JSON=json.dumps(compact_api_endpoints, ensure_ascii=False),
                PROBE_RESULTS_INCLUDED_COUNT=len(compact_probe_results),
                PROBE_RESULTS_TOTAL_COUNT=len(ai_probe_results),
                PROBE_RESULTS_JSON=json.dumps(compact_probe_results, ensure_ascii=False),
            )

            if len(prompt) <= prompt_budget:
                break
            max_urls = max_urls // 2
            max_pages = max_pages // 2

        if len(prompt) > prompt_budget:
            continue

        try:
            if progress is not None:
                progress.info(
                    "Submitting AI HTML report request with "
                    f"prompt budget {prompt_budget} chars "
                    f"(included_urls={compact_sitemap.get('included_urls_count')}, "
                    f"included_pages={compact_sitemap.get('included_pages_count')})"
                )
            response = create_openai_response_with_backoff(
                client=client,
                model=model,
                prompt=prompt,
                progress=progress,
                request_timeout_seconds=request_timeout_seconds,
            )
            raw_output = response.output_text or ""
            if "<html" in raw_output.lower() or "<!doctype html" in raw_output.lower():
                return extract_html_document(raw_output)
            if progress is not None:
                progress.info("AI returned non-HTML report content; rendering as styled HTML")
            return render_text_report_as_html(
                report_text=raw_output,
                title=f"Nightmare AI Report: {root_domain}",
                subtitle="Rendered from AI textual report output",
            )
        except ContextLengthExceededError as exc:
            last_error = exc
            if progress is not None:
                progress.info(
                    f"OpenAI HTML report exceeded context window at budget {prompt_budget}; retrying smaller payload"
                )
            continue

    raise RuntimeError("Failed to generate AI HTML report within context window") from last_error


def extract_json_object(raw_text: str) -> dict[str, Any]:
    text = (raw_text or "").strip()

    text = re.sub(r"^```(?:json)?", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"```$", "", text).strip()

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    left = text.find("{")
    right = text.rfind("}")
    if left != -1 and right != -1 and right > left:
        try:
            parsed = json.loads(text[left : right + 1])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    return {}


def _is_context_length_error(exc: BadRequestError) -> bool:
    code = str(getattr(exc, "code", "") or "").lower()
    message = str(exc).lower()
    return "context_length_exceeded" in code or "context window" in message or "context_length_exceeded" in message


def prioritize_urls_for_ai(urls: list[str], limit: int) -> list[str]:
    if limit <= 0:
        return []

    unique_urls = sorted(set(filter_urls_for_ai(urls)))
    ranked = sorted(
        unique_urls,
        key=lambda value: (
            1 if not should_crawl_url(value) else 0,
            len(urlparse(value).path or "/"),
            len(value),
            value,
        ),
    )
    return ranked[:limit]


def compact_sitemap_for_ai(
    sitemap: dict[str, Any],
    *,
    max_urls: int,
    max_pages: int,
    max_outbound_links_per_page: int,
    truncated_for_ai: bool,
) -> dict[str, Any]:
    all_urls = sitemap.get("urls", [])
    all_ai_urls = condense_urls_for_ai(all_urls if isinstance(all_urls, list) else [])
    selected_urls = prioritize_urls_for_ai(all_ai_urls, max_urls)
    selected_set = set(selected_urls)

    all_pages = sitemap.get("pages", [])
    compact_pages: list[dict[str, Any]] = []
    if isinstance(all_pages, list):
        for page in all_pages:
            if not isinstance(page, dict):
                continue
            page_url = normalize_url_for_sitemap(str(page.get("url", "")))
            if page_url not in selected_set:
                continue

            outbound_raw = page.get("outbound_internal_links", [])
            outbound_links: list[str] = []
            if isinstance(outbound_raw, list):
                for target in outbound_raw:
                    if not isinstance(target, str):
                        continue
                    normalized_target = normalize_url_for_sitemap(target)
                    if normalized_target in selected_set:
                        outbound_links.append(normalized_target)

            if max_outbound_links_per_page > 0:
                outbound_links = sorted(set(outbound_links))[:max_outbound_links_per_page]
            else:
                outbound_links = []

            compact_pages.append(
                {
                    "url": page_url,
                    "inbound_count": int(page.get("inbound_count", 0)),
                    "outbound_count": int(page.get("outbound_count", len(outbound_links))),
                    "outbound_internal_links": outbound_links,
                }
            )
            if len(compact_pages) >= max_pages:
                break

    return {
        "generated_at_utc": sitemap.get("generated_at_utc"),
        "start_url": sitemap.get("start_url"),
        "pages_crawled": sitemap.get("pages_crawled"),
        "unique_urls_discovered": sitemap.get("unique_urls_discovered"),
        "truncated_for_ai": truncated_for_ai,
        "included_urls_count": len(selected_urls),
        "included_pages_count": len(compact_pages),
        "excluded_urls_count": max(0, len(all_ai_urls) - len(selected_urls)),
        "excluded_pages_count": max(0, len(all_pages) - len(compact_pages)) if isinstance(all_pages, list) else None,
        "urls": selected_urls,
        "pages": compact_pages,
    }


def compact_list_for_ai(values: list[str], limit: int) -> list[str]:
    return prioritize_urls_for_ai(condense_urls_for_ai(values), limit)


def should_run_ai_stage(client: OpenAI | None, interrupted: bool, interrupt_stage: str) -> bool:
    if client is None:
        return False
    return (not interrupted) or interrupt_stage == "crawl"


def create_openai_response_with_backoff(
    client: OpenAI,
    model: str,
    prompt: str,
    progress: ProgressReporter | None = None,
    request_timeout_seconds: float = DEFAULT_OPENAI_REQUEST_TIMEOUT_SECONDS,
    max_attempts: int = 6,
    base_delay_seconds: float = 1.5,
    max_delay_seconds: float = 30.0,
):
    transient_http_statuses = {408, 409, 429, 500, 502, 503, 504}

    try:
        for attempt in range(1, max_attempts + 1):
            try:
                if progress is not None:
                    progress.info(
                        f"OpenAI request attempt {attempt}/{max_attempts} with model {model} "
                        f"(timeout={request_timeout_seconds:.1f}s)"
                    )
                return client.responses.create(model=model, input=prompt, timeout=request_timeout_seconds)
            except BadRequestError as exc:
                if _is_context_length_error(exc):
                    raise ContextLengthExceededError(str(exc)) from exc
                raise
            except RateLimitError:
                if progress is not None:
                    progress.info(f"OpenAI rate limit on attempt {attempt}/{max_attempts}")
                if attempt == max_attempts:
                    raise
            except APITimeoutError:
                if progress is not None:
                    progress.info(f"OpenAI timeout on attempt {attempt}/{max_attempts}")
                if attempt == max_attempts:
                    raise
            except APIError as exc:
                status_code = getattr(exc, "status_code", None)
                if status_code not in transient_http_statuses or attempt == max_attempts:
                    raise
                if progress is not None:
                    progress.info(
                        f"Transient OpenAI API error on attempt {attempt}/{max_attempts} (status={status_code})"
                    )

            sleep_seconds = min(max_delay_seconds, base_delay_seconds * (2 ** (attempt - 1)))
            jitter = 1.0 + (random.random() * 0.25)
            if progress is not None:
                progress.info(f"Waiting about {sleep_seconds * jitter:.2f}s before retrying OpenAI request")
            interruptible_sleep(sleep_seconds * jitter)

        raise RuntimeError("OpenAI response generation failed after retries")
    finally:
        restore_default_interrupt_handlers()


def ask_openai_for_additional_urls(
    client: OpenAI,
    model: str,
    root_domain: str,
    sitemap: dict[str, Any],
    endpoint_schema_payload: dict[str, Any],
    progress: ProgressReporter | None = None,
    request_timeout_seconds: float = DEFAULT_OPENAI_REQUEST_TIMEOUT_SECONDS,
    truncated_for_ai: bool = True,
) -> tuple[list[str], str, str]:
    output_text = ""
    final_prompt = ""
    last_error: Exception | None = None

    for prompt_budget in OPENAI_PROMPT_BUDGET_STEPS:
        max_urls = max(50, prompt_budget // 260)
        max_pages = max(25, prompt_budget // 520)
        prompt = ""
        compact_sitemap: dict[str, Any] | None = None

        while max_urls >= 1 and max_pages >= 1:
            compact_sitemap = compact_sitemap_for_ai(
                sitemap,
                max_urls=max_urls,
                max_pages=max_pages,
                max_outbound_links_per_page=20,
                truncated_for_ai=truncated_for_ai,
            )
            compact_json = json.dumps(compact_sitemap, ensure_ascii=False)
            compact_schema = compact_endpoint_schema_for_ai(
                endpoint_schema_payload,
                max_total_routes=max_urls,
                truncated_for_ai=truncated_for_ai,
            )
            compact_schema_json = json.dumps(compact_schema, ensure_ascii=False)

            prompt = render_prompt(
                "additional_urls_prompt",
                ROOT_DOMAIN=root_domain,
                ENDPOINT_SCHEMA_JSON=compact_schema_json,
                SITEMAP_JSON=compact_json,
            )

            if len(prompt) <= prompt_budget:
                break
            max_urls = max_urls // 2
            max_pages = max_pages // 2

        if len(prompt) > prompt_budget:
            continue

        try:
            if progress is not None:
                progress.info(
                    "Submitting AI additional-URL request with "
                    f"prompt budget {prompt_budget} chars "
                    f"(included_urls={compact_sitemap.get('included_urls_count') if compact_sitemap else 0}, "
                    f"included_pages={compact_sitemap.get('included_pages_count') if compact_sitemap else 0})"
                )
            response = create_openai_response_with_backoff(
                client=client,
                model=model,
                prompt=prompt,
                progress=progress,
                request_timeout_seconds=request_timeout_seconds,
            )
            output_text = response.output_text
            final_prompt = prompt
            break
        except ContextLengthExceededError as exc:
            last_error = exc
            if progress is not None:
                progress.info(
                    f"OpenAI request exceeded context window at budget {prompt_budget}; retrying with smaller payload"
                )
            continue

    if not output_text:
        if progress is not None:
            progress.info(
                "Skipping AI additional URL suggestion: unable to fit request within model context window"
            )
        return [], final_prompt, output_text

    parsed = extract_json_object(output_text)

    candidates = parsed.get("additional_urls", [])
    if not isinstance(candidates, list):
        return [], final_prompt, output_text

    cleaned: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        normalized = normalize_url(candidate)
        if normalized in seen:
            continue
        if not is_allowed_domain(normalized, root_domain):
            continue
        seen.add(normalized)
        cleaned.append(normalized)

    return cleaned, final_prompt, output_text


def ask_openai_for_site_analysis(
    client: OpenAI,
    model: str,
    root_domain: str,
    original_urls: list[str],
    suggested_urls: list[str],
    sitemap: dict[str, Any],
    endpoint_schema_payload: dict[str, Any],
    ai_probe_results: list[dict[str, Any]],
    progress: ProgressReporter | None = None,
    request_timeout_seconds: float = DEFAULT_OPENAI_REQUEST_TIMEOUT_SECONDS,
    truncated_for_ai: bool = True,
) -> str:
    last_error: Exception | None = None
    for prompt_budget in OPENAI_PROMPT_BUDGET_STEPS:
        max_urls = max(80, prompt_budget // 300)
        max_pages = max(40, prompt_budget // 600)
        prompt = ""
        compact_sitemap: dict[str, Any] | None = None
        compact_original: list[str] = []
        compact_suggested: list[str] = []

        while max_urls >= 1 and max_pages >= 1:
            compact_sitemap = compact_sitemap_for_ai(
                sitemap,
                max_urls=max_urls,
                max_pages=max_pages,
                max_outbound_links_per_page=20,
                truncated_for_ai=truncated_for_ai,
            )
            compact_schema = compact_endpoint_schema_for_ai(
                endpoint_schema_payload,
                max_total_routes=max_urls,
                truncated_for_ai=truncated_for_ai,
            )
            compact_original = compact_list_for_ai(original_urls, max_urls)
            compact_suggested = compact_list_for_ai(suggested_urls, max(1, max_urls // 2))
            compact_probe_results = ai_probe_results[: max(10, max_urls // 6)]

            prompt = render_prompt(
                "site_analysis_prompt",
                ROOT_DOMAIN=root_domain,
                ORIGINAL_URLS_INCLUDED_COUNT=len(compact_original),
                ORIGINAL_URLS_TOTAL_COUNT=len(original_urls),
                ORIGINAL_URLS_JSON=json.dumps(compact_original, ensure_ascii=False),
                SUGGESTED_URLS_INCLUDED_COUNT=len(compact_suggested),
                SUGGESTED_URLS_TOTAL_COUNT=len(suggested_urls),
                SUGGESTED_URLS_JSON=json.dumps(compact_suggested, ensure_ascii=False),
                SITEMAP_JSON=json.dumps(compact_sitemap, ensure_ascii=False),
                ENDPOINT_SCHEMA_JSON=json.dumps(compact_schema, ensure_ascii=False),
                PROBE_RESULTS_INCLUDED_COUNT=len(compact_probe_results),
                PROBE_RESULTS_TOTAL_COUNT=len(ai_probe_results),
                PROBE_RESULTS_JSON=json.dumps(compact_probe_results, ensure_ascii=False),
            )

            if len(prompt) <= prompt_budget:
                break
            max_urls = max_urls // 2
            max_pages = max_pages // 2

        if len(prompt) > prompt_budget:
            continue

        try:
            if progress is not None:
                progress.info(
                    "Submitting AI site analysis request with "
                    f"prompt budget {prompt_budget} chars "
                    f"(included_urls={compact_sitemap.get('included_urls_count') if compact_sitemap else 0}, "
                    f"included_pages={compact_sitemap.get('included_pages_count') if compact_sitemap else 0})"
                )
            response = create_openai_response_with_backoff(
                client=client,
                model=model,
                prompt=prompt,
                progress=progress,
                request_timeout_seconds=request_timeout_seconds,
            )
            return response.output_text.strip()
        except ContextLengthExceededError as exc:
            last_error = exc
            if progress is not None:
                progress.info(
                    f"OpenAI site analysis exceeded context window at budget {prompt_budget}; retrying smaller payload"
                )
            continue

    raise RuntimeError("Failed to generate AI site analysis within context window") from last_error


def ask_openai_for_probe_requests(
    client: OpenAI,
    model: str,
    root_domain: str,
    endpoint_schema_payload: dict[str, Any],
    progress: ProgressReporter | None = None,
    request_timeout_seconds: float = DEFAULT_OPENAI_REQUEST_TIMEOUT_SECONDS,
    max_requests: int = 20,
    per_host_max_requests: int = 8,
    truncated_for_ai: bool = True,
) -> tuple[list[dict[str, Any]], str, str]:
    output_text = ""
    final_prompt = ""
    last_error: Exception | None = None

    for prompt_budget in OPENAI_PROMPT_BUDGET_STEPS:
        max_routes = max(80, prompt_budget // 300)
        compact_schema = compact_endpoint_schema_for_ai(
            endpoint_schema_payload,
            max_total_routes=max_routes,
            truncated_for_ai=truncated_for_ai,
        )
        prompt = render_prompt(
            "probe_requests_prompt",
            ROOT_DOMAIN=root_domain,
            MAX_REQUESTS=max_requests,
            PER_HOST_MAX_REQUESTS=per_host_max_requests,
            ENDPOINT_SCHEMA_JSON=json.dumps(compact_schema, ensure_ascii=False),
        )

        if len(prompt) > prompt_budget:
            continue

        try:
            if progress is not None:
                progress.info(
                    "Submitting AI probe-planning request with "
                    f"prompt budget {prompt_budget} chars "
                    f"(routes={compact_schema.get('endpoint_schema_count', 0)})"
                )
            response = create_openai_response_with_backoff(
                client=client,
                model=model,
                prompt=prompt,
                progress=progress,
                request_timeout_seconds=request_timeout_seconds,
            )
            output_text = response.output_text or ""
            final_prompt = prompt
            break
        except ContextLengthExceededError as exc:
            last_error = exc
            if progress is not None:
                progress.info(
                    f"OpenAI probe-planning exceeded context window at budget {prompt_budget}; retrying smaller payload"
                )
            continue

    if not output_text:
        if progress is not None:
            progress.info("Skipping AI probe planning: unable to fit request within model context window")
        return [], final_prompt, output_text

    parsed = extract_json_object(output_text)
    raw_requests = parsed.get("requests", [])
    if not isinstance(raw_requests, list):
        return [], final_prompt, output_text

    cleaned_requests: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    host_counts: dict[str, int] = defaultdict(int)
    for entry in raw_requests:
        if len(cleaned_requests) >= max_requests:
            break

        if isinstance(entry, str):
            candidate_url = apply_type_placeholder_defaults(entry.strip())
            candidate_method = "GET"
            candidate_reason = "AI suggested URL request"
        elif isinstance(entry, dict):
            candidate_url = apply_type_placeholder_defaults(str(entry.get("url", "")).strip())
            candidate_method = str(entry.get("method", "GET")).upper().strip()
            candidate_reason = str(entry.get("reason", "")).strip() or "AI suggested URL request"
        else:
            continue

        if not candidate_url:
            continue
        normalized = normalize_url(candidate_url)
        if not is_allowed_domain(normalized, root_domain):
            continue
        if candidate_method not in {"GET", "HEAD"}:
            candidate_method = "GET"

        key = (candidate_method, normalized)
        if key in seen:
            continue
        host = (urlparse(normalized).hostname or "").lower()
        if host and host_counts[host] >= per_host_max_requests:
            continue
        seen.add(key)
        if host:
            host_counts[host] += 1
        cleaned_requests.append({"url": normalized, "method": candidate_method, "reason": candidate_reason})

    return cleaned_requests, final_prompt, output_text


def execute_ai_probe_request(
    url: str,
    method: str,
    timeout_seconds: float,
    request_throttle: RequestThrottle | None = None,
) -> dict[str, Any]:
    if request_throttle is not None:
        request_throttle.wait()

    request_method = (method or "GET").upper()
    if request_method not in {"GET", "HEAD"}:
        request_method = "GET"

    headers = {
        "User-Agent": "nightmare-ai-probe/1.0",
        "Accept": "*/*",
        "Connection": "close",
    }
    request_payload = {
        "method": request_method,
        "url": url,
        "headers": headers,
        "body_base64": "",
    }
    request = urllib.request.Request(url, method=request_method, headers=headers)

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status_code = int(response.getcode())
            response_body = read_http_body_capped(response)
            preview_text = response_body[:1200].decode("utf-8", errors="replace")
            return {
                "ok": status_code < 400,
                "status_code": status_code,
                "note": "HTTP probe completed",
                "request": request_payload,
                "response": {
                    "status": status_code,
                    "url": response.geturl(),
                    "headers": dict(response.headers.items()),
                    "body_text_preview": preview_text,
                    **encode_body_for_evidence(response_body),
                },
            }
    except urllib.error.HTTPError as error:
        status_code = int(error.code)
        response_body = read_http_body_capped(error) if hasattr(error, "read") else b""
        preview_text = response_body[:1200].decode("utf-8", errors="replace")
        return {
            "ok": False,
            "status_code": status_code,
            "note": "HTTP error response",
            "request": request_payload,
            "response": {
                "status": status_code,
                "url": url,
                "headers": dict(error.headers.items()) if error.headers else {},
                "body_text_preview": preview_text,
                **encode_body_for_evidence(response_body),
            },
        }
    except Exception as error:
        return {
            "ok": False,
            "status_code": None,
            "note": f"Probe failed: {error}",
            "request": request_payload,
            "response": None,
        }


def prioritize_probe_requests(
    requests: list[dict[str, Any]],
    max_total: int,
    max_per_host: int,
) -> list[dict[str, Any]]:
    if max_total <= 0 or max_per_host <= 0:
        return []

    def _rank(entry: dict[str, Any]) -> tuple[int, int, str]:
        url = str(entry.get("url", "")).lower()
        method = str(entry.get("method", "GET")).upper()
        api_bias = 0 if ("/api" in url or "graphql" in url or "rpc" in url) else 1
        method_bias = 0 if method == "GET" else 1
        return (api_bias, method_bias, len(url))

    ranked = sorted(requests, key=_rank)
    selected: list[dict[str, Any]] = []
    host_counts: dict[str, int] = defaultdict(int)

    for item in ranked:
        if len(selected) >= max_total:
            break
        url = normalize_url(str(item.get("url", "")).strip())
        if not url:
            continue
        host = (urlparse(url).hostname or "").lower()
        if host and host_counts[host] >= max_per_host:
            continue
        selected.append(item)
        if host:
            host_counts[host] += 1

    return selected


def probe_url_existence(
    url: str,
    timeout_seconds: float,
    request_throttle: RequestThrottle | None = None,
) -> dict[str, Any]:
    user_agent = "nightmare-url-validator/1.0"
    last_error: str | None = None

    for method in ("HEAD", "GET"):
        if request_throttle is not None:
            request_throttle.wait()

        request_payload = {
            "method": method,
            "url": url,
            "headers": {
                "User-Agent": user_agent,
                "Accept": "*/*",
                "Connection": "close",
            },
            "body_base64": "",
        }
        request = urllib.request.Request(url, method=method, headers=request_payload["headers"])

        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                status_code = int(response.getcode())
                response_body = read_http_body_capped(response)
                if method == "HEAD" and status_code == 200:
                    # HEAD 200 is not sufficient for existence; validate body content with GET.
                    continue
                soft_404_detected, soft_404_reason = detect_soft_not_found_response(status_code, response_body)
                exists_confirmed = status_code not in configured_not_found_statuses()
                if soft_404_detected:
                    exists_confirmed = False
                return {
                    "exists_confirmed": exists_confirmed,
                    "status_code": status_code,
                    "method": method,
                    "note": (
                        f"Soft-404 detected in 200 response: {soft_404_reason}"
                        if soft_404_detected
                        else "Confirmed by direct HTTP probe"
                    ),
                    "soft_404_detected": soft_404_detected,
                    "soft_404_reason": soft_404_reason,
                    "request": request_payload,
                    "response": {
                        "status": status_code,
                        "url": response.geturl(),
                        "headers": dict(response.headers.items()),
                        **encode_body_for_evidence(response_body),
                    },
                }
        except urllib.error.HTTPError as error:
            status_code = int(error.code)
            response_body = read_http_body_capped(error) if hasattr(error, "read") else b""
            if method == "HEAD" and status_code in {405, 501}:
                continue

            soft_404_detected, soft_404_reason = detect_soft_not_found_response(status_code, response_body)
            exists_confirmed = status_code not in configured_not_found_statuses()
            note = "Received HTTP error status during probe"
            if status_code in configured_not_found_statuses():
                note = "URL returned explicit not-found status"
            if soft_404_detected:
                exists_confirmed = False
                note = (
                    f"Soft-404 or block page detected in HTTP error response: {soft_404_reason}"
                    if soft_404_reason
                    else "Soft-404 or block page detected in HTTP error response"
                )

            return {
                "exists_confirmed": exists_confirmed,
                "status_code": status_code,
                "method": method,
                "note": note,
                "soft_404_detected": soft_404_detected,
                "soft_404_reason": soft_404_reason,
                "request": request_payload,
                "response": {
                    "status": status_code,
                    "url": url,
                    "headers": dict(error.headers.items()) if error.headers else {},
                    **encode_body_for_evidence(response_body),
                },
            }
        except urllib.error.URLError as error:
            last_error = str(getattr(error, "reason", error))
        except TimeoutError as error:
            last_error = str(error)

    return {
        "exists_confirmed": False,
        "status_code": None,
        "method": None,
        "note": f"Probe failed: {last_error or 'unknown network error'}",
        "soft_404_detected": False,
        "soft_404_reason": None,
        "request": None,
        "response": None,
    }


def load_path_wordlist_seed_urls(
    normalized_start_url: str,
    root_domain: str,
    wordlist_path: Path,
) -> list[str]:
    """Build absolute same-domain URLs from a path wordlist for the crawl origin."""
    if not wordlist_path.is_file():
        return []
    parsed = urlparse(normalized_start_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return []
    origin_rd = get_root_domain(parsed.hostname)
    if not origin_rd or origin_rd.lower() != str(root_domain).strip().lower():
        return []
    origin = urlunparse((parsed.scheme, parsed.netloc, "/", "", "", ""))
    out: list[str] = []
    try:
        text = wordlist_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    for line in text.splitlines():
        raw = line.split("#", 1)[0].strip()
        if not raw or raw.startswith("#"):
            continue
        if raw.startswith("http://") or raw.startswith("https://"):
            full = normalize_url(raw)
        else:
            path_part = raw if raw.startswith("/") else f"/{raw}"
            full = normalize_url(urljoin(origin, path_part))
        if not is_allowed_domain(full, root_domain):
            continue
        out.append(full)
    return list(dict.fromkeys(out))


def write_wordlist_guessed_paths_index(site_output_dir: Path, state: CrawlState) -> Path | None:
    """Write ``guessed_paths/guessed_paths_index.json`` for wordlist crawl outcomes (hits = not 404/410)."""
    seeds = getattr(state, "wordlist_path_seeds", None) or []
    if not seeds:
        return None
    guessed_dir = site_output_dir / "guessed_paths"
    guessed_dir.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, Any]] = []
    hits = 0
    visited_count = 0
    for url in seeds:
        rec = state.url_inventory.get(url)
        visited = bool(rec and rec.was_crawled)
        if visited:
            visited_count += 1
        code = rec.crawl_status_code if rec else None
        try:
            code_i = int(code) if code is not None else None
        except (TypeError, ValueError):
            code_i = None
        is_hit = (
            visited
            and code_i is not None
            and code_i not in configured_not_found_statuses()
            and not bool(rec and rec.soft_404_detected)
        )
        if is_hit:
            hits += 1
        entries.append({"url": url, "visited": visited, "http_status": code_i, "hit": is_hit})
    payload: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "wordlist_source": str(getattr(state, "wordlist_source", "") or FILE_PATH_WORDLIST_DISCOVERED_FROM),
        "wordlist_paths_total": len(seeds),
        "visited_count": visited_count,
        "hits_count": hits,
        "entries": entries,
    }
    out_path = guessed_dir / "guessed_paths_index.json"
    write_json(out_path, payload)
    return out_path


def effective_crawl_seeds(
    start_urls: list[str],
    state: CrawlState,
    root_domain: str,
    max_pages: int,
    wordlist_seed_urls: set[str] | None = None,
) -> list[str]:
    """URLs the spider would actually schedule in ``start()`` (matches DomainSpider logic)."""
    normalized_seeds = [normalize_url(u) for u in start_urls if isinstance(u, str) and u.strip()]
    remaining_budget = max(0, max_pages - len(state.visited_urls))
    if remaining_budget <= 0:
        return []
    wl = wordlist_seed_urls or set()
    out: list[str] = []
    for seed_url in normalized_seeds:
        if seed_url in state.visited_urls:
            continue
        if not is_allowed_domain(seed_url, root_domain):
            continue
        if seed_url not in wl and not should_crawl_url(seed_url):
            continue
        out.append(seed_url)
        if len(out) >= remaining_budget:
            break
    return out


def crawl_domain(
    start_url: str,
    max_pages: int,
    crawl_delay: float,
    max_delay: float,
    backoff_factor: float,
    recovery_factor: float,
    evidence_dir: str,
    session_state_path: str | None = None,
    resume: bool = False,
    scrapy_log_level: str = "WARNING",
    scrapy_log_file: str | None = None,
    crawl_wordlist_path: Path | None = None,
    verbose: bool = False,
    progress: ProgressReporter | None = None,
) -> tuple[str, CrawlState]:
    start_url = coerce_http_https_start_url(start_url)
    parsed = urlparse(start_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("start_url must be a valid http/https URL")

    if crawl_delay <= 0:
        raise ValueError("crawl_delay must be greater than 0 to enforce throttling")
    if max_delay < crawl_delay:
        raise ValueError("max_delay must be >= crawl_delay")

    root_domain = get_root_domain(parsed.hostname)
    if not root_domain:
        raise ValueError("Unable to determine root domain from URL")

    normalized_start_url = normalize_url(start_url)
    session_path = Path(session_state_path) if session_state_path else None

    state = CrawlState()
    start_urls = [normalized_start_url]

    if resume and session_path is not None and session_path.exists():
        session_payload = load_session_state(session_path)
        saved_start_url = normalize_url(str(session_payload.get("start_url", normalized_start_url)))
        saved_start_parsed = urlparse(saved_start_url)
        saved_start_root_domain = get_root_domain(saved_start_parsed.hostname or "")
        if saved_start_url != normalized_start_url and saved_start_root_domain != root_domain:
            raise ValueError(
                f"Session start URL ({saved_start_url}) does not match requested URL ({normalized_start_url})"
            )
        if saved_start_url != normalized_start_url and progress is not None:
            progress.info(
                "Resuming existing session despite start-URL variant change within the same root domain: "
                f"{saved_start_url} -> {normalized_start_url}"
            )

        saved_root_domain = str(session_payload.get("root_domain", root_domain)).strip().lower()
        if saved_root_domain:
            root_domain = saved_root_domain

        saved_state = session_payload.get("state", {})
        if isinstance(saved_state, dict):
            state = crawl_state_from_dict(saved_state)

        frontier = session_payload.get("frontier", [])
        if isinstance(frontier, list) and frontier:
            start_urls = [
                normalize_url(url)
                for url in frontier
                if isinstance(url, str)
                and url.strip()
                and is_allowed_domain(normalize_url(url), root_domain)
                and should_crawl_url(normalize_url(url))
            ]
            if not start_urls:
                start_urls = build_resume_frontier(state, root_domain)
        else:
            start_urls = build_resume_frontier(state, root_domain)

        if normalized_start_url not in state.discovered_urls:
            register_url_discovery(
                state=state,
                url=normalized_start_url,
                source_type="seed_input",
                discovered_from="user_input",
                evidence_file="",
            )
        if not start_urls and normalized_start_url not in state.visited_urls:
            start_urls = [normalized_start_url]
    else:
        seed_evidence = {
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_type": "seed_input",
            "discovered_url": normalized_start_url,
            "seed_input": {
                "url": normalized_start_url,
            },
            "request": None,
            "response": None,
        }
        seed_evidence_file = save_evidence(Path(evidence_dir), normalized_start_url, "seed_input", seed_evidence)
        register_url_discovery(
            state=state,
            url=normalized_start_url,
            source_type="seed_input",
            discovered_from="user_input",
            evidence_file=seed_evidence_file,
        )

    active_wordlist_path = crawl_wordlist_path or FILE_PATH_WORDLIST_PATH
    state.wordlist_source = format_repo_relative_path(active_wordlist_path)
    wl_from_file = load_path_wordlist_seed_urls(normalized_start_url, root_domain, active_wordlist_path)
    state.wordlist_path_seeds = sorted(set(state.wordlist_path_seeds) | set(wl_from_file))
    if wl_from_file:
        for guessed_url in wl_from_file:
            register_url_discovery(
                state=state,
                url=guessed_url,
                source_type="file_path_wordlist",
                discovered_from=state.wordlist_source,
                evidence_file="",
            )
        seen_seed = set(start_urls)
        for u in wl_from_file:
            if u not in seen_seed:
                start_urls.append(u)
                seen_seed.add(u)
        if progress is not None:
            progress.info(
                f"Appended {len(wl_from_file)} URL(s) from {state.wordlist_source} as crawl seeds "
                f"({len(state.wordlist_path_seeds)} unique paths tracked)"
            )
    elif progress is not None and not active_wordlist_path.is_file():
        progress.info(f"No path wordlist crawl seeds: missing file {active_wordlist_path}")

    wordlist_seed_set = set(state.wordlist_path_seeds)
    unique_seed_urls = {
        normalize_url(u) for u in start_urls if isinstance(u, str) and u.strip() and is_allowed_domain(u, root_domain)
    }
    pending_seed_urls = {
        seed_url
        for seed_url in unique_seed_urls
        if seed_url not in state.visited_urls and (seed_url in wordlist_seed_set or should_crawl_url(seed_url))
    }
    effective_max_pages = max(max_pages, len(state.visited_urls) + len(pending_seed_urls))
    if effective_max_pages != max_pages and progress is not None:
        progress.info(
            "Expanded crawl page budget to include all pending seed URLs: "
            f"configured_max_pages={max_pages}, effective_max_pages={effective_max_pages}, "
            f"pending_seed_urls={len(pending_seed_urls)}, pending_wordlist_paths="
            f"{len([u for u in pending_seed_urls if u in wordlist_seed_set])}"
        )

    if session_path is not None:
        save_session_state(
            session_state_path=session_path,
            start_url=normalized_start_url,
            root_domain=root_domain,
            max_pages=effective_max_pages,
            state=state,
        )

    process_settings = {
        "LOG_LEVEL": scrapy_log_level,
        "DOWNLOAD_DELAY": crawl_delay,
        "AUTOTHROTTLE_START_DELAY": crawl_delay,
        "AUTOTHROTTLE_MAX_DELAY": max_delay,
        "ADAPTIVE_BACKOFF_MIN_DELAY": crawl_delay,
        "ADAPTIVE_BACKOFF_MAX_DELAY": max_delay,
        "ADAPTIVE_BACKOFF_FACTOR": backoff_factor,
        "ADAPTIVE_RECOVERY_FACTOR": recovery_factor,
    }
    if scrapy_log_file:
        process_settings["LOG_FILE"] = scrapy_log_file

    queued = effective_crawl_seeds(
        start_urls, state, root_domain, effective_max_pages, wordlist_seed_urls=wordlist_seed_set
    )
    if not queued and progress is not None:
        session_hint = f" Session file: {session_path.resolve()}" if session_path else ""
        progress.info(
            "No crawl requests will be sent: every seed/ frontier URL is already visited or skipped "
            f"(e.g. static asset extension), or the page budget is exhausted.{session_hint} "
            "For a fresh crawl, run with --no-resume or delete that session file and set "
            '"resume": false in config.'
        )

    process = CrawlerProcess(settings=process_settings)
    process.crawl(
        DomainSpider,
        start_url=normalized_start_url,
        start_urls=start_urls,
        root_domain=root_domain,
        max_pages=effective_max_pages,
        state=state,
        evidence_dir=evidence_dir,
        session_state_path=str(session_path) if session_path is not None else None,
        verbose=verbose,
        progress=progress,
        wordlist_path_seeds=wordlist_seed_set,
    )
    logging.getLogger("nightmare").info(
        "Scrapy reactor starting - Ctrl+C stops the crawl. "
        "The first page can take up to the download timeout (and retries) before any spider log lines appear."
    )
    interrupt_bridge = CrawlInterruptBridge()
    initial_visited_count = len(state.visited_urls)
    interrupt_bridge.install()
    wait_monitor = start_crawl_wait_monitor(
        progress,
        state,
        initial_visited_count,
        interrupt_event=interrupt_bridge.interrupt_event,
    )
    try:
        process.start(stop_after_crawl=True, install_signal_handlers=False)
    finally:
        if wait_monitor is not None:
            wait_monitor[0].set()
            wait_monitor[1].join(timeout=1.0)
        interrupt_bridge.cleanup()
        restore_default_interrupt_handlers()

    if interrupt_bridge.interrupt_event.is_set():
        stop_twisted_threadpool()
        raise KeyboardInterrupt
    _raise_keyboard_interrupt_if_reactor_stopped_by_signal()

    if session_path is not None:
        save_session_state(
            session_state_path=session_path,
            start_url=normalized_start_url,
            root_domain=root_domain,
            max_pages=effective_max_pages,
            state=state,
        )

    return root_domain, state


BATCH_STATE_SCHEMA_VERSION = 1


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(obj, indent=2, ensure_ascii=False) + "\n"
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    # Antivirus/indexers can briefly lock JSON files on Windows right as we rotate state snapshots.
    # Retry a few times instead of failing the entire batch run.
    max_attempts = 8
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                tmp.replace(path)
                return
            except PermissionError:
                if attempt >= max_attempts:
                    raise
                time.sleep(0.05 * attempt)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        SYNCHRONIZE = 0x00100000
        handle = kernel32.OpenProcess(SYNCHRONIZE, False, int(pid))
        if handle:
            kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    else:
        return True


def load_target_file_entries(targets_path: Path) -> list[dict[str, Any]]:
    text = targets_path.read_text(encoding="utf-8-sig")
    entries: list[dict[str, Any]] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        entry_id = hashlib.sha1(f"{line_no}:{stripped}".encode("utf-8")).hexdigest()[:16]
        entries.append({"entry_id": entry_id, "line": line_no, "raw": stripped})
    return entries


def prepare_target_entry_fields(entry: dict[str, Any]) -> None:
    if entry.get("start_url") and entry.get("root_domain"):
        return
    raw = str(entry.get("raw", "")).strip()
    if any(ch in raw for ch in "*?"):
        entry["status"] = "skipped"
        entry["error"] = "wildcard target; specify a concrete hostname/URL for crawling"
        return
    try:
        url = coerce_http_https_start_url(raw)
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("URL must have an http(s) scheme and hostname")
        if any(ch.isspace() for ch in parsed.hostname):
            raise ValueError("hostname contains whitespace (invalid URL)")
        if "*" in (parsed.hostname or "") or "?" in (parsed.hostname or ""):
            raise ValueError("hostname wildcards are not supported")
        root_domain = get_root_domain(parsed.hostname)
        if not root_domain:
            raise ValueError("unable to derive registrable domain")
        entry["start_url"] = url
        entry["root_domain"] = root_domain
    except Exception as exc:
        entry["status"] = "skipped"
        entry["error"] = str(exc)[:800]


def reconcile_batch_state(
    existing: dict[str, Any] | None,
    targets_path: Path,
    target_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    by_id: dict[str, dict[str, Any]] = {}
    if isinstance(existing, dict):
        for item in existing.get("entries", []):
            if isinstance(item, dict) and item.get("entry_id"):
                by_id[str(item["entry_id"])] = item
    merged_entries: list[dict[str, Any]] = []
    for row in target_rows:
        eid = str(row["entry_id"])
        if eid in by_id:
            merged_entries.append(by_id[eid])
            continue
        merged_entries.append(
            {
                "entry_id": eid,
                "line": row["line"],
                "raw": row["raw"],
                "status": "pending",
                "start_url": None,
                "root_domain": None,
                "error": None,
                "started_at_utc": None,
                "completed_at_utc": None,
                "exit_code": None,
                "worker_pid": None,
            }
        )
    return {
        "schema_version": BATCH_STATE_SCHEMA_VERSION,
        "targets_path": str(targets_path.resolve()),
        "targets_file_mtime": targets_path.stat().st_mtime,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "entries": merged_entries,
    }


def recover_stale_running_entries(state: dict[str, Any]) -> None:
    for entry in state.get("entries", []):
        if not isinstance(entry, dict):
            continue
        if str(entry.get("status", "")) == "interrupted":
            entry["status"] = "pending"
            entry["worker_pid"] = None
            entry["started_at_utc"] = None
            entry["completed_at_utc"] = None
            entry["exit_code"] = None
            continue
        if str(entry.get("status", "")) != "running":
            continue
        pid = int(entry.get("worker_pid") or 0)
        if not process_is_running(pid):
            entry["status"] = "pending"
            entry["error"] = (entry.get("error") or "").strip() or "recovered stale run (worker process ended unexpectedly)"
            entry["worker_pid"] = None
            entry["started_at_utc"] = None


def domain_lock_file_path(lock_dir: Path, domain: str) -> Path:
    safe = re.sub(r"[^\w.-]+", "_", domain).strip("._-")[:180] or "domain"
    return lock_dir / f"{safe}.lock"


def _domain_lock_payload_stale(payload: dict[str, Any], lock_path: Path, max_age_seconds: float) -> bool:
    try:
        pid = int(payload.get("pid") or 0)
    except Exception:
        return True
    if pid > 0 and process_is_running(pid):
        return False
    try:
        age = time.time() - lock_path.stat().st_mtime
    except OSError:
        return True
    return age > max_age_seconds


def acquire_domain_lock_file(lock_dir: Path, domain: str, *, max_age_seconds: float = 6 * 3600) -> Path | None:
    lock_dir.mkdir(parents=True, exist_ok=True)
    path = domain_lock_file_path(lock_dir, domain)
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return path
    except FileExistsError:
        try:
            raw = path.read_text(encoding="utf-8").strip()
            payload = json.loads(raw) if raw else {}
            if isinstance(payload, dict) and not _domain_lock_payload_stale(payload, path, max_age_seconds):
                return None
        except Exception:
            pass
        try:
            path.unlink()
        except OSError:
            return None
        return acquire_domain_lock_file(lock_dir, domain, max_age_seconds=max_age_seconds)


def write_domain_lock_pid(lock_path: Path, pid: int) -> None:
    lock_path.write_text(
        json.dumps(
            {
                "pid": int(pid),
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def release_domain_lock_file(lock_path: Path | None) -> None:
    if lock_path is None:
        return
    try:
        lock_path.unlink()
    except OSError:
        pass


def filter_argv_for_batch_child(argv: list[str]) -> list[str]:
    out: list[str] = []
    skip = False
    for arg in argv:
        if skip:
            skip = False
            continue
        if arg in ("--batch-workers", "--targets-file", "--batch-state-dir"):
            skip = True
            continue
        if arg.startswith("--batch-workers=") or arg.startswith("--targets-file=") or arg.startswith("--batch-state-dir="):
            continue
        out.append(arg)
    return out


def run_multi_target_orchestrator(
    args: argparse.Namespace,
    config: dict[str, Any],
    config_path: Path,
) -> None:
    targets_raw = merged_value(args.targets_file, config, "targets_file", "targets.txt")
    targets_path = Path(str(targets_raw)).expanduser()
    if not targets_path.is_absolute():
        targets_path = (BASE_DIR / targets_path).resolve()
    if not targets_path.is_file():
        raise FileNotFoundError(f"Targets file not found: {targets_path}")

    worker_count = int(merged_value(args.batch_workers, config, "batch_workers", 8))
    if worker_count < 1:
        raise ValueError("batch_workers must be at least 1")
    batch_worker_nice = _safe_int_from_any(merged_value(None, config, "batch_worker_nice", 10), 10)
    affinity_cores_raw = merged_value(None, config, "batch_worker_affinity_cores", None)
    affinity_cores = None if affinity_cores_raw in (None, "") else _safe_int_from_any(affinity_cores_raw, 0)
    cpu_count = max(1, int(os.cpu_count() or 1))
    worker_affinity_mask = _compute_worker_affinity_mask(cpu_count, affinity_cores)

    state_dir_raw = merged_value(args.batch_state_dir, config, "batch_state_dir", None)
    if state_dir_raw:
        state_dir = Path(str(state_dir_raw)).expanduser()
        if not state_dir.is_absolute():
            state_dir = (OUTPUT_DIR / state_dir).resolve()
    else:
        state_dir = (OUTPUT_DIR / "batch_state").resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "batch_run_state.json"
    lock_dir = state_dir / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)

    with dev_timed_call("batch.load_target_file_entries"):
        target_rows = load_target_file_entries(targets_path)
    existing_state: dict[str, Any] | None = None
    if state_path.is_file():
        try:
            existing_state = read_json_file(state_path)
        except Exception:
            existing_state = None
    with dev_timed_call("batch.reconcile_batch_state"):
        state = reconcile_batch_state(existing_state, targets_path, target_rows)
    with dev_timed_call("batch.recover_stale_running_entries"):
        recover_stale_running_entries(state)
    for entry in state["entries"]:
        if isinstance(entry, dict) and entry.get("status") == "pending":
            prepare_target_entry_fields(entry)
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    with dev_timed_call("batch.persist_initial_state"):
        _atomic_write_json(state_path, state)

    script_path = Path(__file__).resolve()
    child_suffix = filter_argv_for_batch_child(sys.argv[1:])

    pending_queue: list[dict[str, Any]] = [
        e for e in state["entries"] if isinstance(e, dict) and e.get("status") == "pending" and e.get("start_url")
    ]
    active: dict[str, tuple[subprocess.Popen, Path | None, dict[str, Any]]] = {}
    print(
        f"[batch] targets={targets_path} ({len(target_rows)} non-empty lines), "
        f"workers={worker_count}, state={state_path}",
        flush=True,
    )
    if os.name == "nt":
        print(
            f"[batch] worker scheduling: windows affinity mask=0x{worker_affinity_mask:x} "
            f"(cpu_count={cpu_count}, configured_cores={affinity_cores})",
            flush=True,
        )
    else:
        print(
            f"[batch] worker scheduling: unix nice increment={max(0, batch_worker_nice)}",
            flush=True,
        )

    def persist() -> None:
        state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        try:
            with dev_timed_call("batch.persist_state"):
                _atomic_write_json(state_path, state)
        except OSError as exc:
            print(
                f"[batch] warning: state persist failed for {state_path}: {exc}. Continuing...",
                flush=True,
            )

    def terminate_running_workers() -> None:
        for _eid, (proc, lock_path, _ent) in list(active.items()):
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
            release_domain_lock_file(lock_path)

    def handle_sigint(signum: int, frame: Any) -> None:
        print("\n[batch] interrupt: terminating active workers…", flush=True)
        terminate_running_workers()
        for entry in state.get("entries", []):
            if isinstance(entry, dict) and entry.get("status") == "running":
                entry["status"] = "interrupted"
                entry["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
                entry["error"] = (entry.get("error") or "").strip() or "batch orchestrator interrupted"
        persist()
        force_process_exit(130)

    previous_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, handle_sigint)

    worst_exit = 0
    try:
        while pending_queue or active:
            while len(active) < worker_count and pending_queue:
                entry = pending_queue.pop(0)
                domain = str(entry.get("root_domain") or "")
                lock_path = acquire_domain_lock_file(lock_dir, domain)
                if lock_path is None:
                    print(f"[batch] skip (lock busy): {domain} — {entry.get('start_url')}", flush=True)
                    entry["status"] = "skipped"
                    entry["error"] = "domain lock held by another live process"
                    entry["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
                    persist()
                    continue
                start_url = str(entry.get("start_url") or "")
                cmd = [sys.executable, "-u", str(script_path), start_url] + child_suffix
                entry["status"] = "running"
                entry["started_at_utc"] = datetime.now(timezone.utc).isoformat()
                entry["completed_at_utc"] = None
                entry["exit_code"] = None
                entry["worker_pid"] = None
                entry["error"] = None
                persist()
                print(f"[batch] spawn: {domain} pid-pending cmd={start_url!r}", flush=True)
                child_env = os.environ.copy()
                if os.name == "nt":
                    child_env["NIGHTMARE_WORKER_AFFINITY_MASK"] = str(worker_affinity_mask)
                else:
                    child_env["NIGHTMARE_WORKER_NICE"] = str(max(0, batch_worker_nice))
                spawn_started = time.perf_counter()
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(BASE_DIR),
                    env=child_env,
                )
                _record_dev_timing("batch.worker_spawn", time.perf_counter() - spawn_started)
                entry["worker_pid"] = proc.pid
                write_domain_lock_pid(lock_path, proc.pid)
                active[str(entry["entry_id"])] = (proc, lock_path, entry)
                persist()

            if not active:
                break

            time.sleep(0.25)
            for entry_id, (proc, lock_path, entry) in list(active.items()):
                code = proc.poll()
                if code is None:
                    continue
                release_domain_lock_file(lock_path)
                del active[entry_id]
                entry["exit_code"] = int(code)
                entry["worker_pid"] = None
                entry["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
                if code == 0:
                    entry["status"] = "completed"
                    entry["error"] = None
                else:
                    entry["status"] = "failed"
                    entry["error"] = f"subprocess exited with code {code}"
                    worst_exit = 1
                try:
                    started_iso = str(entry.get("started_at_utc") or "").strip()
                    completed_iso = str(entry.get("completed_at_utc") or "").strip()
                    if started_iso and completed_iso:
                        started_dt = datetime.fromisoformat(started_iso.replace("Z", "+00:00"))
                        completed_dt = datetime.fromisoformat(completed_iso.replace("Z", "+00:00"))
                        _record_dev_timing("batch.worker_runtime", (completed_dt - started_dt).total_seconds())
                except Exception:
                    pass
                print(
                    f"[batch] finished: {entry.get('root_domain')} exit={code} url={entry.get('start_url')!r}",
                    flush=True,
                )
                persist()
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        terminate_running_workers()
        persist()

    summary_counts: dict[str, int] = defaultdict(int)
    for entry in state.get("entries", []):
        if isinstance(entry, dict):
            summary_counts[str(entry.get("status", "unknown"))] += 1
    print(f"[batch] done. State file: {state_path}", flush=True)
    print(f"[batch] summary: {dict(summary_counts)}", flush=True)
    emit_dev_timing_summary(None)
    if worst_exit != 0:
        force_process_exit(1)


def _read_json_dict_safe(path: Path) -> dict[str, Any]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _count_results_json_files(results_dir: Path) -> tuple[int, int]:
    anomalies = 0
    reflections = 0
    if not results_dir.is_dir():
        return anomalies, reflections
    for path in results_dir.iterdir():
        if not path.is_file():
            continue
        lower = path.name.lower()
        if not lower.endswith(".json"):
            continue
        if lower.startswith("anomaly_") or lower.startswith("anomoly_"):
            anomalies += 1
        elif lower.startswith("reflection_"):
            reflections += 1
    return anomalies, reflections


def collect_status_metrics(output_root: Path) -> dict[str, dict[str, Any]]:
    metrics_by_domain: dict[str, dict[str, Any]] = {}
    if not output_root.is_dir():
        return metrics_by_domain

    for domain_dir in sorted(output_root.iterdir(), key=lambda p: p.name.lower()):
        if not domain_dir.is_dir() or domain_dir.name.startswith("."):
            continue
        domain = domain_dir.name.strip().lower()
        if not domain:
            continue

        unique_urls = 0
        parameterized_get_requests = 0
        extractor_matches = 0
        anomalies = 0
        reflections = 0

        inventory_path = domain_dir / f"{domain}_url_inventory.json"
        if not inventory_path.is_file():
            candidates = sorted(
                domain_dir.glob("*_url_inventory.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            inventory_path = candidates[0] if candidates else inventory_path
        if inventory_path.is_file():
            inv = _read_json_dict_safe(inventory_path)
            try:
                unique_urls = int(
                    inv.get("total_urls_effective", inv.get("total_urls", 0)) or 0
                )
            except Exception:
                unique_urls = 0
            if unique_urls <= 0:
                entries = inv.get("entries", [])
                if isinstance(entries, list):
                    unique_urls = len(entries)

        parameters_path = domain_dir / f"{domain}.parameters.json"
        if not parameters_path.is_file():
            candidates = sorted(
                domain_dir.glob("*.parameters.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            parameters_path = candidates[0] if candidates else parameters_path
        if parameters_path.is_file():
            params_payload = _read_json_dict_safe(parameters_path)
            entries = params_payload.get("entries", [])
            if isinstance(entries, list):
                parameterized_get_requests = len(entries)

        fozzy_domain_dir = domain_dir / "fozzy-output" / domain
        fozzy_summary_path = fozzy_domain_dir / f"{domain}.fozzy.summary.json"
        if fozzy_summary_path.is_file():
            fozzy_summary = _read_json_dict_safe(fozzy_summary_path)
            totals = fozzy_summary.get("totals", {})
            if isinstance(totals, dict):
                try:
                    anomalies = int(totals.get("anomalies", 0) or 0)
                except Exception:
                    anomalies = 0
                try:
                    reflections = int(totals.get("reflections", 0) or 0)
                except Exception:
                    reflections = 0
        if anomalies <= 0 and reflections <= 0:
            fallback_results_dir = fozzy_domain_dir / "results"
            anomalies, reflections = _count_results_json_files(fallback_results_dir)

        extractor_summary_path = fozzy_domain_dir / "extractor" / "summary.json"
        if extractor_summary_path.is_file():
            extractor_summary = _read_json_dict_safe(extractor_summary_path)
            try:
                extractor_matches = int(extractor_summary.get("match_count", 0) or 0)
            except Exception:
                extractor_matches = 0
            if extractor_matches <= 0:
                rows = extractor_summary.get("rows", [])
                if isinstance(rows, list):
                    extractor_matches = len(rows)

        spidered = unique_urls > 0 or parameterized_get_requests > 0
        metrics_by_domain[domain] = {
            "domain": domain,
            "spidered": spidered,
            "unique_urls": max(0, int(unique_urls)),
            "parameterized_get_requests": max(0, int(parameterized_get_requests)),
            "extractor_matches": max(0, int(extractor_matches)),
            "anomalies": max(0, int(anomalies)),
            "reflections": max(0, int(reflections)),
        }

    return metrics_by_domain


def resolve_target_domains_for_status(args: argparse.Namespace, config: dict[str, Any]) -> set[str]:
    targets_raw = merged_value(args.targets_file, config, "targets_file", "targets.txt")
    targets_path = Path(str(targets_raw)).expanduser()
    if not targets_path.is_absolute():
        targets_path = (BASE_DIR / targets_path).resolve()
    if not targets_path.is_file():
        return set()

    target_rows = load_target_file_entries(targets_path)
    roots: set[str] = set()
    for row in target_rows:
        entry: dict[str, Any] = {"raw": row.get("raw", "")}
        prepare_target_entry_fields(entry)
        root_domain = str(entry.get("root_domain", "") or "").strip().lower()
        if root_domain:
            roots.add(root_domain)
    return roots


def print_quick_status_report(args: argparse.Namespace, config: dict[str, Any]) -> None:
    metrics_by_domain = collect_status_metrics(OUTPUT_DIR)
    spidered = [row for row in metrics_by_domain.values() if bool(row.get("spidered"))]
    spidered_domains = len(spidered)
    total_unique_urls = sum(int(row.get("unique_urls", 0) or 0) for row in spidered)
    total_parameterized = sum(int(row.get("parameterized_get_requests", 0) or 0) for row in spidered)
    target_domains = resolve_target_domains_for_status(args, config)

    if target_domains:
        spidered_target_count = sum(
            1 for domain in target_domains if bool(metrics_by_domain.get(domain, {}).get("spidered"))
        )
        print(f"Spidered {spidered_target_count:,}/{len(target_domains):,} target domains.")
    else:
        print(f"Spidered {spidered_domains:,} domains.")

    print(f"Requested {total_unique_urls:,} unique URLs from {spidered_domains:,} domains")
    print(f"Discovered {total_parameterized:,} unique get requests with parameters")
    print("Top Domains:")

    ranked = sorted(
        spidered,
        key=lambda row: (
            -int(row.get("unique_urls", 0) or 0),
            -int(row.get("parameterized_get_requests", 0) or 0),
            str(row.get("domain", "")),
        ),
    )
    for row in ranked[:10]:
        domain = str(row.get("domain", "") or "")
        unique_urls = int(row.get("unique_urls", 0) or 0)
        parameterized_get_requests = int(row.get("parameterized_get_requests", 0) or 0)
        extractor_matches = int(row.get("extractor_matches", 0) or 0)
        anomalies = int(row.get("anomalies", 0) or 0)
        reflections = int(row.get("reflections", 0) or 0)
        print(
            f"{domain} - {unique_urls:,} unique URLs - "
            f"{parameterized_get_requests:,} get requests with parameters - "
            f"{extractor_matches:,} extractor matches / "
            f"{anomalies:,} anomalies found / {reflections:,} reflections found"
        )


def parse_args() -> argparse.Namespace:
    ensure_string_resources_loaded()
    parser = argparse.ArgumentParser(
        description=get_string_config_value(HELP_TEXTS, "parser_description")
    )
    parser.add_argument(
        "url",
        nargs="?",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "url_help"),
    )
    parser.add_argument(
        "--config",
        default="nightmare.json",
        help=get_string_config_value(HELP_TEXTS, "config_help"),
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "max_pages_help"),
    )
    parser.add_argument(
        "--model",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "model_help"),
    )
    parser.add_argument(
        "--openai-timeout",
        type=float,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "openai_timeout_help"),
    )
    parser.add_argument(
        "--ai-probe-max-requests",
        type=int,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "ai_probe_max_requests_help"),
    )
    parser.add_argument(
        "--ai-probe-per-host-max",
        type=int,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "ai_probe_per_host_max_help"),
    )
    parser.add_argument(
        "--ai-probe-delay",
        type=float,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "ai_probe_delay_help"),
    )
    parser.add_argument(
        "--sitemap-output",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "sitemap_output_help"),
    )
    parser.add_argument(
        "--condensed-sitemap-output",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "condensed_sitemap_output_help"),
    )
    parser.add_argument(
        "--url-inventory-output",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "url_inventory_output_help"),
    )
    parser.add_argument(
        "--requests-output",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "requests_output_help"),
    )
    parser.add_argument(
        "--evidence-dir",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "evidence_dir_help"),
    )
    parser.add_argument(
        "--crawl-wordlist",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "crawl_wordlist_help"),
    )
    parser.add_argument(
        "--verify-timeout",
        type=float,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "verify_timeout_help"),
    )
    parser.add_argument(
        "--verify-delay",
        type=float,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "verify_delay_help"),
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help=get_string_config_value(HELP_TEXTS, "standalone_help"),
    )
    parser.add_argument(
        "--verify-urls",
        action="store_true",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "verify_urls_help"),
    )
    parser.add_argument(
        "--crawl-delay",
        type=float,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "crawl_delay_help"),
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "max_delay_help"),
    )
    parser.add_argument(
        "--backoff-factor",
        type=float,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "backoff_factor_help"),
    )
    parser.add_argument(
        "--recovery-factor",
        type=float,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "recovery_factor_help"),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "verbose_help"),
    )
    parser.add_argument(
        "--no-ai",
        action="store_true",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "no_ai_help"),
    )
    parser.add_argument(
        "--session-state-output",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "session_state_output_help"),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "resume_help"),
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "no_resume_help"),
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "log_file_help"),
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "log_level_help"),
    )
    parser.add_argument(
        "--console-log-level",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "console_log_level_help"),
    )
    parser.add_argument(
        "--scrapy-log-file",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "scrapy_log_file_help"),
    )
    parser.add_argument(
        "--scrapy-log-level",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "scrapy_log_level_help"),
    )
    parser.add_argument(
        "--html-report",
        action="store_true",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "html_report_help"),
    )
    parser.add_argument(
        "--html-report-output",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "html_report_output_help"),
    )
    parser.add_argument(
        "--ai-data-output",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "ai_data_output_help"),
    )
    parser.add_argument(
        "--truncated-for-ai",
        dest="truncated_for_ai",
        action="store_true",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "truncated_for_ai_help"),
    )
    parser.add_argument(
        "--no-truncated-for-ai",
        dest="truncated_for_ai",
        action="store_false",
        help=get_string_config_value(HELP_TEXTS, "no_truncated_for_ai_help"),
    )
    parser.add_argument(
        "--targets-file",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "targets_file_help"),
    )
    parser.add_argument(
        "--batch-workers",
        type=int,
        default=None,
        help=get_string_config_value(HELP_TEXTS, "batch_workers_help"),
    )
    parser.add_argument(
        "--batch-state-dir",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "batch_state_dir_help"),
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help=get_string_config_value(HELP_TEXTS, "status_help"),
    )
    return parser.parse_args()


def main() -> None:
    restore_default_interrupt_handlers()
    apply_worker_priority_hints_from_env()
    if sys.platform == "win32":
        from scrapy.utils.reactor import set_asyncio_event_loop_policy

        set_asyncio_event_loop_policy()
        force_noninteractive_stdin()
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(line_buffering=True, write_through=True)
        except Exception:
            pass
    if hasattr(sys.stderr, "reconfigure"):
        try:
            sys.stderr.reconfigure(line_buffering=True, write_through=True)
        except Exception:
            pass
    args = parse_args()
    ensure_directory(CONFIG_DIR)
    ensure_directory(OUTPUT_DIR)

    config_path = resolve_config_path(args.config)
    config = read_json_file(config_path)
    dev_timing_enabled = configure_dev_timing(config)
    apply_extension_config(config)
    page_existence_criteria_path_raw = optional_string(
        merged_value(
            None,
            config,
            "page_existence_criteria_config",
            PAGE_EXISTENCE_CRITERIA_CONFIG_PATH.name,
        )
    )
    page_existence_criteria_path = resolve_config_path(
        page_existence_criteria_path_raw or PAGE_EXISTENCE_CRITERIA_CONFIG_PATH.name
    )
    page_existence_criteria_config = read_json_file(page_existence_criteria_path)
    apply_page_existence_criteria_config(page_existence_criteria_config)

    if bool(getattr(args, "status", False)):
        with dev_timed_call("main.print_quick_status_report"):
            print_quick_status_report(args, config)
        emit_dev_timing_summary(None)
        return

    url_arg = optional_string(args.url)
    if not url_arg:
        with dev_timed_call("main.run_multi_target_orchestrator"):
            run_multi_target_orchestrator(args, config, config_path)
        emit_dev_timing_summary(None)
        return

    if getattr(args, "standalone", False):
        if not args.resume:
            args.no_resume = True
        if args.console_log_level is None:
            args.console_log_level = "INFO"
        print(
            "[nightmare] standalone: crawl data and logs are written under output/<domain>/ only; "
            "no coordinator APIs.",
            flush=True,
        )

    max_pages = int(merged_value(args.max_pages, config, "max_pages", 300))
    model = str(merged_value(args.model, config, "model", "gpt-5-mini"))
    openai_timeout = float(
        merged_value(args.openai_timeout, config, "openai_timeout", DEFAULT_OPENAI_REQUEST_TIMEOUT_SECONDS)
    )
    ai_probe_max_requests = int(merged_value(args.ai_probe_max_requests, config, "ai_probe_max_requests", 25))
    ai_probe_per_host_max = int(merged_value(args.ai_probe_per_host_max, config, "ai_probe_per_host_max", 8))
    verify_timeout = float(merged_value(args.verify_timeout, config, "verify_timeout", 12.0))
    crawl_delay = float(merged_value(args.crawl_delay, config, "crawl_delay", 0.25))
    crawl_wordlist_raw = optional_string(
        merged_value(args.crawl_wordlist, config, "crawl_wordlist", FILE_PATH_WORDLIST_DISCOVERED_FROM)
    )
    crawl_wordlist_path = resolve_wordlist_path(crawl_wordlist_raw or FILE_PATH_WORDLIST_DISCOVERED_FROM)
    verify_delay = float(merged_value(args.verify_delay, config, "verify_delay", crawl_delay))
    ai_probe_delay = float(merged_value(args.ai_probe_delay, config, "ai_probe_delay", verify_delay))
    verify_urls = bool(merged_value(args.verify_urls, config, "verify_urls", False))
    max_delay = float(merged_value(args.max_delay, config, "max_delay", 30.0))
    backoff_factor = float(merged_value(args.backoff_factor, config, "backoff_factor", 1.7))
    recovery_factor = float(merged_value(args.recovery_factor, config, "recovery_factor", 0.92))
    verbose = bool(merged_value(args.verbose, config, "verbose", False))
    no_ai = bool(merged_value(args.no_ai, config, "no_ai", False))
    resume = bool(merged_value(args.resume, config, "resume", False))
    if args.no_resume:
        resume = False
    html_report_enabled = bool(merged_value(args.html_report, config, "html_report", False))
    truncated_for_ai = bool(merged_value(args.truncated_for_ai, config, "truncated_for_ai", True))
    app_log_level_name = str(merged_value(args.log_level, config, "log_level", "INFO")).upper()

    if args.console_log_level is not None:
        console_log_level_name = str(args.console_log_level).upper()
    elif verbose:
        console_log_level_name = "INFO"
    else:
        console_log_level_name = str(config.get("console_log_level", "WARNING")).upper()

    if args.scrapy_log_level is not None:
        scrapy_log_level_name = str(args.scrapy_log_level).upper()
    elif verbose:
        scrapy_log_level_name = "INFO"
    else:
        scrapy_log_level_name = str(config.get("scrapy_log_level", "WARNING")).upper()

    if max_pages <= 0:
        raise ValueError("max_pages must be greater than 0")
    if verify_urls and verify_timeout <= 0:
        raise ValueError("verify_timeout must be greater than 0")
    if verify_urls and verify_delay <= 0:
        raise ValueError("verify_delay must be greater than 0 to enforce throttling")
    if not no_ai and openai_timeout <= 0:
        raise ValueError("openai_timeout must be greater than 0")
    if ai_probe_max_requests <= 0:
        raise ValueError("ai_probe_max_requests must be greater than 0")
    if ai_probe_per_host_max <= 0:
        raise ValueError("ai_probe_per_host_max must be greater than 0")
    if ai_probe_delay <= 0:
        raise ValueError("ai_probe_delay must be greater than 0")

    args.url = coerce_http_https_start_url(url_arg)
    parsed_url = urlparse(args.url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
        raise ValueError("url must be a valid http/https URL")
    root_domain = get_root_domain(parsed_url.hostname)
    if not root_domain:
        raise ValueError("Unable to determine root domain from URL")

    default_domain_output_dir = OUTPUT_DIR / root_domain
    default_sitemap_output_path = default_domain_output_dir / f"{root_domain}_sitemap.json"
    default_condensed_sitemap_output_path = default_domain_output_dir / f"{root_domain}_sitemap_condensed.json"
    default_inventory_output_path = default_domain_output_dir / f"{root_domain}_url_inventory.json"
    default_requests_output_path = default_domain_output_dir / "requests.json"
    default_evidence_dir_path = default_domain_output_dir / f"{root_domain}_evidence"
    default_session_state_path = default_domain_output_dir / f"{root_domain}_crawl_session.json"
    default_app_log_file_path = default_domain_output_dir / f"{root_domain}_nightmare.log"
    default_scrapy_log_file_path = default_domain_output_dir / f"{root_domain}_scrapy.log"
    default_html_report_path = default_domain_output_dir / "report.html"
    default_ai_data_output_path = default_domain_output_dir / "ai-data.txt"
    default_source_of_truth_output_path = default_domain_output_dir / f"{root_domain}_source_of_truth.json"
    default_parameters_output_path = default_domain_output_dir / f"{root_domain}.parameters.json"
    default_parameters_text_output_path = default_domain_output_dir / f"{root_domain}.parameters.txt"

    sitemap_output_path = resolve_output_path_with_domain_default(
        cli_value=args.sitemap_output,
        config=config,
        key="sitemap_output",
        default_path=default_sitemap_output_path,
    )
    condensed_sitemap_output_path = resolve_output_path_with_domain_default(
        cli_value=args.condensed_sitemap_output,
        config=config,
        key="condensed_sitemap_output",
        default_path=default_condensed_sitemap_output_path,
    )
    inventory_output_path = resolve_output_path_with_domain_default(
        cli_value=args.url_inventory_output,
        config=config,
        key="url_inventory_output",
        default_path=default_inventory_output_path,
    )
    requests_output_path = resolve_output_path_with_domain_default(
        cli_value=args.requests_output,
        config=config,
        key="requests_output",
        default_path=default_requests_output_path,
    )
    evidence_dir_path = resolve_output_path_with_domain_default(
        cli_value=args.evidence_dir,
        config=config,
        key="evidence_dir",
        default_path=default_evidence_dir_path,
    )
    session_state_path = resolve_output_path_with_domain_default(
        cli_value=args.session_state_output,
        config=config,
        key="session_state_output",
        default_path=default_session_state_path,
    )
    app_log_file_path = resolve_output_path_with_domain_default(
        cli_value=args.log_file,
        config=config,
        key="log_file",
        default_path=default_app_log_file_path,
    )
    scrapy_log_file_path = resolve_output_path_with_domain_default(
        cli_value=args.scrapy_log_file,
        config=config,
        key="scrapy_log_file",
        default_path=default_scrapy_log_file_path,
    )
    html_report_output_path = resolve_output_path_with_domain_default(
        cli_value=args.html_report_output,
        config=config,
        key="html_report_output",
        default_path=default_html_report_path,
    )
    ai_data_output_path = resolve_output_path_with_domain_default(
        cli_value=args.ai_data_output,
        config=config,
        key="ai_data_output",
        default_path=default_ai_data_output_path,
    )
    source_of_truth_output_path = resolve_output_path_with_domain_default(
        cli_value=None,
        config=config,
        key="source_of_truth_output",
        default_path=default_source_of_truth_output_path,
    )
    parameters_output_path = resolve_output_path_with_domain_default(
        cli_value=None,
        config=config,
        key="parameters_output",
        default_path=default_parameters_output_path,
    )
    parameters_text_output_path = resolve_output_path_with_domain_default(
        cli_value=None,
        config=config,
        key="parameters_text_output",
        default_path=default_parameters_text_output_path,
    )

    app_log_level = parse_log_level(app_log_level_name, "log_level")
    console_log_level = parse_log_level(console_log_level_name, "console_log_level")
    parse_log_level(scrapy_log_level_name, "scrapy_log_level")

    app_logger = configure_application_logger(
        log_file=app_log_file_path,
        file_level=app_log_level,
        console_level=console_log_level,
    )
    progress = ProgressReporter(verbose=verbose, logger=app_logger)
    progress.info(f"Using configuration file: {config_path.resolve()}")
    progress.info(f"Using page existence criteria file: {page_existence_criteria_path.resolve()}")
    if dev_timing_enabled:
        progress.info("[dev-perf] development timing enabled")

    client: OpenAI | None = None
    if not no_ai:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY is not set")
        client = OpenAI(api_key=api_key)
        progress.info(f"Initialized OpenAI client for model {model}")
    else:
        progress.info("Running in no-AI mode: OpenAI URL suggestion and analysis are disabled")

    ensure_directory(evidence_dir_path)
    ensure_directory(sitemap_output_path.parent)
    ensure_directory(condensed_sitemap_output_path.parent)
    ensure_directory(inventory_output_path.parent)
    ensure_directory(requests_output_path.parent)
    ensure_directory(session_state_path.parent)
    ensure_directory(scrapy_log_file_path.parent)
    ensure_directory(html_report_output_path.parent)
    ensure_directory(ai_data_output_path.parent)
    ensure_directory(source_of_truth_output_path.parent)
    ensure_directory(parameters_output_path.parent)
    ensure_directory(parameters_text_output_path.parent)
    progress.info(f"Prepared evidence directory at {evidence_dir_path.resolve()}")
    _cd_root = collected_data_root_from_evidence_dir(evidence_dir_path)
    ensure_collected_data_layout(_cd_root)
    progress.info(f"Collected data directory: {_cd_root.resolve()}")
    progress.info(f"Session state file: {session_state_path.resolve()} (resume={resume})")
    progress.info(f"Application log file: {app_log_file_path.resolve()} (level={app_log_level_name})")
    progress.info(f"Scrapy log file: {scrapy_log_file_path.resolve()} (level={scrapy_log_level_name})")
    progress.info(f"HTML report: {html_report_output_path.resolve()} (enabled={html_report_enabled})")
    progress.info(f"AI data text file: {ai_data_output_path.resolve()}")
    progress.info(f"Requests inventory file: {requests_output_path.resolve()}")
    progress.info(f"Source-of-truth file: {source_of_truth_output_path.resolve()}")
    progress.info(f"Parameters JSON file: {parameters_output_path.resolve()}")
    progress.info(f"Parameters text file: {parameters_text_output_path.resolve()}")
    progress.info(f"AI compact payload truncated_for_ai: {truncated_for_ai}")
    progress.info(f"OpenAI timeout: {openai_timeout:.1f}s per request")
    progress.info(
        "AI probe limits: "
        f"max_total={ai_probe_max_requests}, max_per_host={ai_probe_per_host_max}, delay={ai_probe_delay}s"
    )
    progress.info(
        f"Throttle settings: crawl_delay={crawl_delay}s, max_delay={max_delay}s, verify_delay={verify_delay}s, verify_urls={verify_urls}"
    )
    progress.info(f"Crawl path wordlist: {format_repo_relative_path(crawl_wordlist_path)}")

    interrupted = False
    interrupt_stage = ""
    state = CrawlState()

    progress.info(f"Starting crawl for {args.url}")
    try:
        with dev_timed_call("main.crawl_domain", progress):
            root_domain, state = crawl_domain(
                start_url=args.url,
                max_pages=max_pages,
                crawl_delay=crawl_delay,
                max_delay=max_delay,
                backoff_factor=backoff_factor,
                recovery_factor=recovery_factor,
                evidence_dir=str(evidence_dir_path),
                session_state_path=str(session_state_path),
                resume=resume,
                scrapy_log_level=scrapy_log_level_name,
                scrapy_log_file=str(scrapy_log_file_path),
                crawl_wordlist_path=crawl_wordlist_path,
                verbose=verbose,
                progress=progress,
            )
    except KeyboardInterrupt:
        interrupted = True
        interrupt_stage = "crawl"
        progress.info("Interrupt received during crawl; attempting graceful finalization from saved session state")
        if session_state_path.exists():
            try:
                session_payload = load_session_state(session_state_path)
                saved_root_domain = str(session_payload.get("root_domain", root_domain)).strip().lower()
                if saved_root_domain:
                    root_domain = saved_root_domain
                saved_state_payload = session_payload.get("state", {})
                if isinstance(saved_state_payload, dict):
                    state = crawl_state_from_dict(saved_state_payload)
            except Exception as recovery_error:
                progress.info(f"Failed to recover state from session after interrupt: {recovery_error}")

    progress.info(
        f"Crawl completed with {len(state.visited_urls)} visited pages and {len(state.discovered_urls)} discovered URLs"
    )
    if len(state.visited_urls) <= 1:
        seed_record = state.url_inventory.get(normalize_url(args.url))
        if seed_record is not None and seed_record.crawl_note:
            progress.info(
                f"Seed crawl diagnostic for {normalize_url(args.url)}: "
                f"status={seed_record.crawl_status_code}, note={seed_record.crawl_note}"
            )
    with dev_timed_call("main.build_sitemap", progress):
        sitemap = build_sitemap(args.url, state)
    with dev_timed_call("main.write_sitemap_json", progress):
        write_json(sitemap_output_path, sitemap)
    progress.info(f"Wrote sitemap to {sitemap_output_path.resolve()}")
    with dev_timed_call("main.build_condensed_sitemap", progress):
        condensed_sitemap = build_condensed_sitemap(root_domain=root_domain, sitemap=sitemap)
    with dev_timed_call("main.write_condensed_sitemap_json", progress):
        write_json(condensed_sitemap_output_path, condensed_sitemap)
    progress.info(f"Wrote condensed sitemap to {condensed_sitemap_output_path.resolve()}")

    discovered_urls = sorted(state.discovered_urls)
    with dev_timed_call("main.build_endpoint_schema_payload", progress):
        endpoint_schema_payload = build_endpoint_schema_payload(discovered_urls)
    with dev_timed_call("main.render_endpoint_schema_text", progress):
        ai_data_text = render_endpoint_schema_text(endpoint_schema_payload)
    with dev_timed_call("main.write_ai_data_text", progress):
        ai_data_output_path.write_text(ai_data_text, encoding="utf-8")
    progress.info(f"Wrote AI endpoint schema data to {ai_data_output_path.resolve()}")
    additional_urls: list[str] = []
    additional_prompt = ""
    additional_output = ""
    ai_probe_requests: list[dict[str, Any]] = []
    ai_probe_results: list[dict[str, Any]] = []

    if should_run_ai_stage(client, interrupted, interrupt_stage):
        progress.info(f"Requesting AI-suggested additional URLs from existing sitemap ({len(discovered_urls)} URLs)")
        try:
            with dev_timed_call("main.ask_openai_for_additional_urls", progress):
                additional_urls, additional_prompt, additional_output = ask_openai_for_additional_urls(
                    client=client,
                    model=model,
                    root_domain=root_domain,
                    sitemap=sitemap,
                    endpoint_schema_payload=endpoint_schema_payload,
                    progress=progress,
                    request_timeout_seconds=openai_timeout,
                    truncated_for_ai=truncated_for_ai,
                )
            progress.info(f"OpenAI suggested {len(additional_urls)} additional URLs")
        except KeyboardInterrupt:
            interrupted = True
            interrupt_stage = "ai_additional_urls"
            additional_urls = []
            additional_prompt = ""
            additional_output = ""
            progress.info("Interrupt received during AI URL suggestion; proceeding to cleanup/report finalization")
        except Exception as ai_suggest_error:
            additional_urls = []
            additional_prompt = ""
            additional_output = ""
            progress.info(f"AI additional URL suggestion failed and was skipped: {ai_suggest_error}")

    if should_run_ai_stage(client, interrupted, interrupt_stage):
        progress.info("Requesting AI-guided next HTTP requests to try")
        try:
            with dev_timed_call("main.ask_openai_for_probe_requests", progress):
                ai_probe_requests, probe_prompt, probe_output = ask_openai_for_probe_requests(
                    client=client,
                    model=model,
                    root_domain=root_domain,
                    endpoint_schema_payload=endpoint_schema_payload,
                    progress=progress,
                    request_timeout_seconds=openai_timeout,
                    max_requests=ai_probe_max_requests,
                    per_host_max_requests=ai_probe_per_host_max,
                    truncated_for_ai=truncated_for_ai,
                )
            ai_probe_requests = prioritize_probe_requests(
                ai_probe_requests,
                max_total=ai_probe_max_requests,
                max_per_host=ai_probe_per_host_max,
            )
            progress.info(f"AI provided {len(ai_probe_requests)} next requests to try")

            if ai_probe_requests:
                probe_plan_evidence = {
                    "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                    "source_type": "ai_probe_plan",
                    "discovered_url": normalize_url(args.url),
                    "openai_request": {
                        "model": model,
                        "prompt_summary": compact_text_for_evidence(probe_prompt),
                    },
                    "openai_response": {
                        "output_summary": compact_text_for_evidence(probe_output),
                    },
                    "probe_request_count": len(ai_probe_requests),
                }
                save_evidence(evidence_dir_path, args.url, "ai_probe_plan", probe_plan_evidence)

                ai_probe_throttle = RequestThrottle(interval_seconds=ai_probe_delay)
                for index, probe in enumerate(ai_probe_requests, start=1):
                    probe_url = normalize_url(str(probe.get("url", "")))
                    probe_method = str(probe.get("method", "GET")).upper()
                    probe_reason = str(probe.get("reason", ""))
                    progress.info(
                        f"Executing AI probe {index}/{len(ai_probe_requests)}: {probe_method} {probe_url}"
                    )
                    try:
                        with dev_timed_call("main.execute_ai_probe_request", progress):
                            probe_result = execute_ai_probe_request(
                                url=probe_url,
                                method=probe_method,
                                timeout_seconds=verify_timeout,
                                request_throttle=ai_probe_throttle,
                            )
                    except KeyboardInterrupt:
                        interrupted = True
                        interrupt_stage = "ai_probe_execution"
                        progress.info("Interrupt received during AI probe execution; stopping probes and finalizing outputs")
                        break
                    ai_probe_results.append(
                        {
                            "url": probe_url,
                            "method": probe_method,
                            "reason": probe_reason,
                            "status_code": probe_result.get("status_code"),
                            "ok": bool(probe_result.get("ok")),
                            "note": probe_result.get("note"),
                            "response_preview": (
                                probe_result.get("response", {}) or {}
                            ).get("body_text_preview", ""),
                        }
                    )

                    probe_evidence = {
                        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                        "source_type": "ai_probe_response",
                        "discovered_url": probe_url,
                        "probe_request": {
                            "url": probe_url,
                            "method": probe_method,
                            "reason": probe_reason,
                        },
                        "probe_result": probe_result,
                    }
                    probe_evidence_file = save_evidence(evidence_dir_path, probe_url, "ai_probe_response", probe_evidence)
                    register_url_discovery(
                        state=state,
                        url=probe_url,
                        source_type="ai_probe_request",
                        discovered_from="openai_probe_plan",
                        evidence_file=probe_evidence_file,
                    )

                    probe_record = ensure_inventory_record(state, probe_url)
                    probe_record.exists_confirmed = bool(probe_result.get("ok", False))
                    probe_record.existence_status_code = probe_result.get("status_code")
                    probe_record.existence_check_method = probe_method
                    probe_record.existence_check_note = str(probe_result.get("note", ""))
                    if interrupted:
                        break
        except KeyboardInterrupt:
            interrupted = True
            interrupt_stage = "ai_probe_planning"
            ai_probe_requests = []
            ai_probe_results = []
            progress.info("Interrupt received during AI probe planning; proceeding to cleanup/report finalization")
        except Exception as ai_probe_error:
            ai_probe_requests = []
            ai_probe_results = []
            progress.info(f"AI probe planning/execution failed and was skipped: {ai_probe_error}")

    for guessed_url in additional_urls:
        if interrupted:
            break
        progress.info(f"Recording AI-suggested URL evidence for {guessed_url}")
        try:
            guess_evidence = {
                "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_type": "guessed_url",
                "discovered_url": guessed_url,
                "openai_request": {
                    "model": model,
                    "prompt_summary": compact_text_for_evidence(additional_prompt),
                },
                "openai_response": {
                    "output_summary": compact_text_for_evidence(additional_output),
                },
            }
            guess_evidence_file = save_evidence(evidence_dir_path, guessed_url, "guessed_url", guess_evidence)
            register_url_discovery(
                state=state,
                url=guessed_url,
                source_type="guessed_url",
                discovered_from="openai_sitemap_guess",
                evidence_file=guess_evidence_file,
            )
        except KeyboardInterrupt:
            interrupted = True
            interrupt_stage = "evidence_recording"
            progress.info("Interrupt received while recording AI suggestions; continuing with finalization")
            break

    combined_urls = sorted(state.url_inventory.keys())
    if verify_urls and not interrupted:
        progress.info(f"Starting URL existence verification for {len(combined_urls)} URLs")
        verify_throttle = RequestThrottle(interval_seconds=verify_delay)

        for index, url in enumerate(combined_urls, start=1):
            progress.info(f"Verifying URL {index}/{len(combined_urls)}: {url}")
            try:
                with dev_timed_call("main.probe_url_existence", progress):
                    probe_result = probe_url_existence(
                        url=url,
                        timeout_seconds=verify_timeout,
                        request_throttle=verify_throttle,
                    )
            except KeyboardInterrupt:
                interrupted = True
                interrupt_stage = "verify_urls"
                progress.info("Interrupt received during URL verification; stopping verification and finalizing outputs")
                break
            record = ensure_inventory_record(state, url)
            record.exists_confirmed = bool(probe_result["exists_confirmed"])
            record.existence_status_code = probe_result["status_code"]
            record.existence_check_method = probe_result["method"]
            record.existence_check_note = probe_result["note"]
            record.soft_404_detected = bool(probe_result.get("soft_404_detected", False))
            record.soft_404_reason = (
                str(probe_result.get("soft_404_reason", "")).strip() or None
            )
            progress.info(
                "Verification result for "
                f"{url}: exists_confirmed={record.exists_confirmed}, "
                f"status={record.existence_status_code}, method={record.existence_check_method}, "
                f"note={record.existence_check_note}"
            )

            verification_evidence = {
                "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_type": "existence_probe",
                "url": url,
                "probe_result": {
                    "exists_confirmed": probe_result["exists_confirmed"],
                    "status_code": probe_result["status_code"],
                    "method": probe_result["method"],
                    "note": probe_result["note"],
                    "soft_404_detected": bool(probe_result.get("soft_404_detected", False)),
                    "soft_404_reason": probe_result.get("soft_404_reason"),
                },
                "request": probe_result["request"],
                "response": probe_result["response"],
            }
            verification_file = save_evidence(evidence_dir_path, url, "existence_probe", verification_evidence)
            if verification_file not in record.discovery_evidence_files:
                record.discovery_evidence_files.append(verification_file)
            if interrupted:
                break
    else:
        if interrupted:
            progress.info("URL verification skipped due to interrupt")
        else:
            progress.info("URL verification skipped (enable with --verify-urls)")

    with dev_timed_call("main.build_url_inventory", progress):
        inventory = build_url_inventory(root_domain=root_domain, state=state)
    with dev_timed_call("main.write_url_inventory", progress):
        write_json(inventory_output_path, inventory)
    progress.info(f"Wrote URL inventory to {inventory_output_path.resolve()}")
    with dev_timed_call("main.build_requests_inventory", progress):
        requests_inventory = build_requests_inventory(root_domain=root_domain, state=state)
    with dev_timed_call("main.write_requests_inventory", progress):
        write_json(requests_output_path, requests_inventory)
    progress.info(f"Wrote requests inventory to {requests_output_path.resolve()}")
    with dev_timed_call("main.build_source_of_truth_payload", progress):
        source_of_truth_payload = build_source_of_truth_payload(root_domain=root_domain, state=state)
    legacy_source_of_truth_output_path = source_of_truth_output_path.with_suffix(".jsonn")
    if source_of_truth_output_path.exists():
        existing_source_of_truth = read_json_file(source_of_truth_output_path)
    elif legacy_source_of_truth_output_path.exists():
        existing_source_of_truth = read_json_file(legacy_source_of_truth_output_path)
    else:
        existing_source_of_truth = {}
    with dev_timed_call("main.merge_source_of_truth_payload", progress):
        merged_source_of_truth = merge_source_of_truth_payload(existing_source_of_truth, source_of_truth_payload)
    with dev_timed_call("main.write_source_of_truth_json", progress):
        source_of_truth_output_path.write_text(
            json.dumps(to_json_compatible(merged_source_of_truth), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    with dev_timed_call("main.build_parameter_inventory_payload", progress):
        parameters_payload = build_parameter_inventory_payload(merged_source_of_truth)
    with dev_timed_call("main.write_parameters_json", progress):
        parameters_output_path.write_text(
            json.dumps(to_json_compatible(parameters_payload), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    with dev_timed_call("main.write_parameters_text", progress):
        parameters_text_output_path.write_text(
            render_parameter_placeholders_text(merged_source_of_truth),
            encoding="utf-8",
        )
    progress.info(f"Wrote source-of-truth file to {source_of_truth_output_path.resolve()}")
    progress.info(f"Wrote parameters JSON to {parameters_output_path.resolve()}")
    progress.info(f"Wrote parameters text to {parameters_text_output_path.resolve()}")

    analysis = ""
    if should_run_ai_stage(client, interrupted, interrupt_stage):
        if not html_report_enabled:
            progress.info("Requesting final AI site analysis")
            try:
                with dev_timed_call("main.ask_openai_for_site_analysis", progress):
                    analysis = ask_openai_for_site_analysis(
                        client=client,
                        model=model,
                        root_domain=root_domain,
                        original_urls=combined_urls,
                        suggested_urls=additional_urls,
                        sitemap=sitemap,
                        endpoint_schema_payload=endpoint_schema_payload,
                        ai_probe_results=ai_probe_results,
                        progress=progress,
                        request_timeout_seconds=openai_timeout,
                        truncated_for_ai=truncated_for_ai,
                    )
                progress.info("Final AI site analysis received")
            except KeyboardInterrupt:
                interrupted = True
                interrupt_stage = "ai_site_analysis"
                analysis = ""
                progress.info("Interrupt received during AI site analysis; skipping AI analysis and finalizing outputs")
            except Exception as ai_analysis_error:
                analysis = ""
                progress.info(f"AI site analysis failed and was skipped: {ai_analysis_error}")

    report_path: Path | None = None
    report_link: str | None = None
    should_generate_report = html_report_enabled or bool(analysis)

    if should_generate_report and html_report_enabled:
        unique_urls = sorted(sitemap.get("urls", [])) if isinstance(sitemap.get("urls"), list) else []
        api_endpoints = infer_api_endpoints(unique_urls)
        url_metadata = build_report_url_metadata(state)

        if not should_run_ai_stage(client, interrupted, interrupt_stage):
            if interrupted:
                progress.info("Generating standard HTML report after interrupt")
            else:
                progress.info("Generating standard HTML report (--no-ai mode)")
            with dev_timed_call("main.generate_standard_html_report", progress):
                html_report_content = generate_standard_html_report(
                    root_domain=root_domain,
                    sitemap=sitemap,
                    unique_urls=unique_urls,
                    api_endpoints=api_endpoints,
                    url_metadata=url_metadata,
                )
        else:
            progress.info("Requesting AI-generated HTML report")
            try:
                with dev_timed_call("main.ask_openai_for_html_report", progress):
                    html_report_content = ask_openai_for_html_report(
                        client=client,
                        model=model,
                        root_domain=root_domain,
                        sitemap=sitemap,
                        endpoint_schema_payload=endpoint_schema_payload,
                        unique_urls=unique_urls,
                        api_endpoints=api_endpoints,
                        ai_probe_results=ai_probe_results,
                        progress=progress,
                        request_timeout_seconds=openai_timeout,
                        truncated_for_ai=truncated_for_ai,
                    )
            except KeyboardInterrupt:
                interrupted = True
                interrupt_stage = "ai_html_report"
                progress.info("Interrupt received during AI HTML report; falling back to standard report")
                with dev_timed_call("main.generate_standard_html_report", progress):
                    html_report_content = generate_standard_html_report(
                        root_domain=root_domain,
                        sitemap=sitemap,
                        unique_urls=unique_urls,
                        api_endpoints=api_endpoints,
                        url_metadata=url_metadata,
                    )
            except Exception as ai_report_error:
                progress.info(
                    f"AI HTML report generation failed; falling back to standard report: {ai_report_error}"
                )
                with dev_timed_call("main.generate_standard_html_report", progress):
                    html_report_content = generate_standard_html_report(
                        root_domain=root_domain,
                        sitemap=sitemap,
                        unique_urls=unique_urls,
                        api_endpoints=api_endpoints,
                        url_metadata=url_metadata,
                    )

        report_path = html_report_output_path
        with dev_timed_call("main.write_html_report", progress):
            report_path.write_text(html_report_content, encoding="utf-8")
        report_link = report_path.resolve().as_uri()
        progress.info(f"HTML report written to {report_path.resolve()}")

        if not interrupted:
            open_browser_uri_nonblocking(
                report_link,
                progress,
                opened_ok_message="Opened HTML report in default browser",
                opened_false_message="Could not auto-open report in browser (open manually using the printed link)",
            )
    elif should_generate_report and analysis:
        if "<html" in analysis.lower() or "<!doctype html" in analysis.lower():
            progress.info("Saving AI site analysis as returned HTML report")
            html_report_content = extract_html_document(analysis)
        else:
            progress.info("Rendering AI site analysis as styled HTML report")
            with dev_timed_call("main.render_text_report_as_html", progress):
                html_report_content = render_text_report_as_html(
                    report_text=analysis,
                    title=f"Nightmare AI Analysis: {root_domain}",
                    subtitle="Rendered from final AI site analysis output",
                )
        report_path = html_report_output_path
        with dev_timed_call("main.write_html_report", progress):
            report_path.write_text(html_report_content, encoding="utf-8")
        report_link = report_path.resolve().as_uri()
        progress.info(f"HTML analysis report written to {report_path.resolve()}")

        if not interrupted:
            open_browser_uri_nonblocking(
                report_link,
                progress,
                opened_ok_message="Opened HTML analysis report in default browser",
                opened_false_message="Could not auto-open analysis report in browser (open manually using the printed link)",
                failure_message_prefix="Failed to auto-open analysis report",
            )

    print(f"Crawled domain root: {root_domain}")
    print(f"Pages crawled: {len(state.visited_urls)}")
    print(f"Unique URLs discovered: {len(state.discovered_urls)}")
    print(f"AI additional URLs suggested: {len(additional_urls)}")
    print(f"AI probe requests executed: {len(ai_probe_results)}")
    print(f"Sitemap saved to: {sitemap_output_path.resolve()}")
    print(f"Condensed sitemap saved to: {condensed_sitemap_output_path.resolve()}")
    print(f"URL inventory saved to: {inventory_output_path.resolve()}")
    print(f"Requests inventory saved to: {requests_output_path.resolve()}")
    print(f"Evidence directory: {evidence_dir_path.resolve()}")
    print(f"Collected data directory: {collected_data_root_from_evidence_dir(evidence_dir_path).resolve()}")
    print(f"Session state saved to: {session_state_path.resolve()}")
    print(f"Application logs: {app_log_file_path.resolve()}")
    print(f"Scrapy logs: {scrapy_log_file_path.resolve()}")
    print(f"AI data file: {ai_data_output_path.resolve()}")
    print(f"Source-of-truth file: {source_of_truth_output_path.resolve()}")
    print(f"Parameters JSON file: {parameters_output_path.resolve()}")
    print(f"Parameters text file: {parameters_text_output_path.resolve()}")
    if report_path is not None and report_link is not None:
        print(f"HTML report: {report_path.resolve()}")
        print(f"HTML report link: {report_link}")

    if additional_urls:
        print("\nAdditional URLs suggested by AI:")
        for url in additional_urls:
            print(f"- {url}")

    if analysis and report_path is None:
        print("\n=== AI Site Analysis ===\n")
        print(analysis)
    else:
        if no_ai:
            print("\nAI site analysis skipped (--no-ai)")
        elif html_report_enabled:
            print("\nAI site analysis skipped (HTML report mode enabled)")
        elif analysis and report_path is not None:
            print("\nAI site analysis saved to the HTML report shown above.")

    if not verify_urls:
        print("URL verification skipped (--verify-urls not enabled)")

    emit_dev_timing_summary(progress)

    if interrupted:
        progress.info(f"Run interrupted by user during stage: {interrupt_stage or 'unknown'}")
        print(f"Run interrupted by user during stage: {interrupt_stage or 'unknown'}")
        force_process_exit(130)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger = logging.getLogger("nightmare")
        message = "Execution interrupted by user (Ctrl+C). Shutting down gracefully."
        if logger.handlers:
            logger.warning(message)
        else:
            print(message, file=sys.stderr, flush=True)
        force_process_exit(130)
    except Exception as exc:
        logger = logging.getLogger("nightmare")
        if logger.handlers:
            logger.exception("Fatal error during nightmare execution")
        else:
            print(f"Fatal error during nightmare execution: {exc}", file=sys.stderr)
        raise
