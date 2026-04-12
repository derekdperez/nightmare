#!/usr/bin/env python3
"""Domain crawler + AI-assisted sitemap analyzer.

Usage:
    python nightmare.py https://example.com --help
    python nightmare.py --batch-workers 8
      (no URL: read targets from targets_file in config or --targets-file)
"""

from __future__ import annotations

import argparse
import base64
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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import scrapy
from openai import APIError, APITimeoutError, BadRequestError, OpenAI, RateLimitError
from scrapy.crawler import CrawlerProcess


try:
    import tldextract

    _TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=None)
except Exception:  # pragma: no cover - optional dependency fallback
    tldextract = None
    _TLD_EXTRACT = None


NOT_FOUND_STATUSES = {404, 410}
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


def merged_value(cli_value: Any, config: dict[str, Any], key: str, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if key in config:
        return config[key]
    return default


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
    was_crawled: bool = False
    crawl_status_code: int | None = None
    crawl_note: str | None = None
    exists_confirmed: bool = False
    existence_status_code: int | None = None
    existence_check_method: str | None = None
    existence_check_note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "discovered_via": sorted(self.discovered_via),
            "discovered_from": sorted(self.discovered_from),
            "discovery_evidence_files": self.discovery_evidence_files,
            "was_crawled": self.was_crawled,
            "crawl_status_code": self.crawl_status_code,
            "crawl_note": self.crawl_note,
            "exists_confirmed": self.exists_confirmed,
            "existence_status_code": self.existence_status_code,
            "existence_check_method": self.existence_check_method,
            "existence_check_note": self.existence_check_note,
        }


@dataclass
class CrawlState:
    discovered_urls: set[str] = field(default_factory=set)
    visited_urls: set[str] = field(default_factory=set)
    link_graph: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    url_inventory: dict[str, UrlInventoryRecord] = field(default_factory=dict)
    #: Normalized crawl seed URLs built from ``file_path_list.txt`` (for reporting under ``guessed_paths/``).
    wordlist_path_seeds: list[str] = field(default_factory=list)


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
}


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
        "wordlist_path_seeds": list(state.wordlist_path_seeds),
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
                was_crawled=bool(record.get("was_crawled", False)),
                crawl_status_code=record.get("crawl_status_code"),
                crawl_note=str(record.get("crawl_note", "")).strip() or None,
                exists_confirmed=bool(record.get("exists_confirmed", False)),
                existence_status_code=record.get("existence_status_code"),
                existence_check_method=record.get("existence_check_method"),
                existence_check_note=record.get("existence_check_note"),
            )

    for url in state.discovered_urls:
        if url not in state.url_inventory:
            state.url_inventory[url] = UrlInventoryRecord(url=url)

    wordlist_seeds = payload.get("wordlist_path_seeds", [])
    if isinstance(wordlist_seeds, list):
        state.wordlist_path_seeds = [
            normalize_url(u) for u in wordlist_seeds if isinstance(u, str) and u.strip()
        ]

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
        self.initial_discovered_url_count = len(self.state.discovered_urls)
        self._last_nonverbose_progress_at = time.monotonic()
        self._last_nonverbose_new_url_count = 0
        self._last_nonverbose_total_url_count = len(self.state.discovered_urls)
        self._has_emitted_nonverbose_progress = False
        self._nonverbose_progress_interval_seconds = 10.0
        self._nonverbose_progress_new_url_step = 10

    def _build_crawl_request(self, url: str) -> scrapy.Request:
        return scrapy.Request(
            url,
            callback=self.parse,
            errback=self.handle_request_failure,
            meta={"handle_httpstatus_all": True},
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
            yield self._build_crawl_request(seed_url)
        self._emit_nonverbose_progress_if_needed(force=True)

    def parse(self, response: scrapy.http.Response):
        current_url = normalize_url(response.url)

        if current_url in self.state.visited_urls:
            return

        if len(self.state.visited_urls) >= self.max_pages:
            return

        self.state.visited_urls.add(current_url)
        record = ensure_inventory_record(self.state, current_url)
        record.was_crawled = True
        record.crawl_status_code = response.status
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
            self._save_session_state()
            return

        if not is_markup_response(response):
            self._emit_verbose(f"Skipping link extraction for non-markup response at {current_url}")
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
    entries = [state.url_inventory[url].to_dict() for url in sorted(state.url_inventory.keys())]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root_domain": root_domain,
        "total_urls": len(entries),
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
    source_urls = sorted(set(filter_urls_for_source_of_truth(list(state.url_inventory.keys()))))
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
                exists_confirmed = status_code not in NOT_FOUND_STATUSES
                return {
                    "exists_confirmed": exists_confirmed,
                    "status_code": status_code,
                    "method": method,
                    "note": "Confirmed by direct HTTP probe",
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

            exists_confirmed = status_code not in NOT_FOUND_STATUSES
            note = "Received HTTP error status during probe"
            if status_code in NOT_FOUND_STATUSES:
                note = "URL returned explicit not-found status"

            return {
                "exists_confirmed": exists_confirmed,
                "status_code": status_code,
                "method": method,
                "note": note,
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
        "request": None,
        "response": None,
    }


def effective_crawl_seeds(
    start_urls: list[str],
    state: CrawlState,
    root_domain: str,
    max_pages: int,
) -> list[str]:
    """URLs the spider would actually schedule in ``start()`` (matches DomainSpider logic)."""
    normalized_seeds = [normalize_url(u) for u in start_urls if isinstance(u, str) and u.strip()]
    remaining_budget = max(0, max_pages - len(state.visited_urls))
    if remaining_budget <= 0:
        return []
    out: list[str] = []
    for seed_url in normalized_seeds:
        if seed_url in state.visited_urls:
            continue
        if not is_allowed_domain(seed_url, root_domain):
            continue
        if not should_crawl_url(seed_url):
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

    if session_path is not None:
        save_session_state(
            session_state_path=session_path,
            start_url=normalized_start_url,
            root_domain=root_domain,
            max_pages=max_pages,
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

    queued = effective_crawl_seeds(start_urls, state, root_domain, max_pages)
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
        max_pages=max_pages,
        state=state,
        evidence_dir=evidence_dir,
        session_state_path=str(session_path) if session_path is not None else None,
        verbose=verbose,
        progress=progress,
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
            max_pages=max_pages,
            state=state,
        )

    return root_domain, state


BATCH_STATE_SCHEMA_VERSION = 1


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(obj, indent=2, ensure_ascii=False) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


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

    target_rows = load_target_file_entries(targets_path)
    existing_state: dict[str, Any] | None = None
    if state_path.is_file():
        try:
            existing_state = read_json_file(state_path)
        except Exception:
            existing_state = None
    state = reconcile_batch_state(existing_state, targets_path, target_rows)
    recover_stale_running_entries(state)
    for entry in state["entries"]:
        if isinstance(entry, dict) and entry.get("status") == "pending":
            prepare_target_entry_fields(entry)
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
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

    def persist() -> None:
        state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic_write_json(state_path, state)

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
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(BASE_DIR),
                    env=os.environ.copy(),
                )
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
    if worst_exit != 0:
        force_process_exit(1)


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
        "--evidence-dir",
        default=None,
        help=get_string_config_value(HELP_TEXTS, "evidence_dir_help"),
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
    return parser.parse_args()


def main() -> None:
    restore_default_interrupt_handlers()
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
    apply_extension_config(config)

    url_arg = optional_string(args.url)
    if not url_arg:
        run_multi_target_orchestrator(args, config, config_path)
        return

    max_pages = int(merged_value(args.max_pages, config, "max_pages", 300))
    model = str(merged_value(args.model, config, "model", "gpt-5-mini"))
    openai_timeout = float(
        merged_value(args.openai_timeout, config, "openai_timeout", DEFAULT_OPENAI_REQUEST_TIMEOUT_SECONDS)
    )
    ai_probe_max_requests = int(merged_value(args.ai_probe_max_requests, config, "ai_probe_max_requests", 25))
    ai_probe_per_host_max = int(merged_value(args.ai_probe_per_host_max, config, "ai_probe_per_host_max", 8))
    verify_timeout = float(merged_value(args.verify_timeout, config, "verify_timeout", 12.0))
    crawl_delay = float(merged_value(args.crawl_delay, config, "crawl_delay", 0.25))
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
    ensure_directory(session_state_path.parent)
    ensure_directory(scrapy_log_file_path.parent)
    ensure_directory(html_report_output_path.parent)
    ensure_directory(ai_data_output_path.parent)
    ensure_directory(source_of_truth_output_path.parent)
    ensure_directory(parameters_output_path.parent)
    ensure_directory(parameters_text_output_path.parent)
    progress.info(f"Prepared evidence directory at {evidence_dir_path.resolve()}")
    progress.info(f"Session state file: {session_state_path.resolve()} (resume={resume})")
    progress.info(f"Application log file: {app_log_file_path.resolve()} (level={app_log_level_name})")
    progress.info(f"Scrapy log file: {scrapy_log_file_path.resolve()} (level={scrapy_log_level_name})")
    progress.info(f"HTML report: {html_report_output_path.resolve()} (enabled={html_report_enabled})")
    progress.info(f"AI data text file: {ai_data_output_path.resolve()}")
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

    interrupted = False
    interrupt_stage = ""
    state = CrawlState()

    progress.info(f"Starting crawl for {args.url}")
    try:
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
    sitemap = build_sitemap(args.url, state)
    write_json(sitemap_output_path, sitemap)
    progress.info(f"Wrote sitemap to {sitemap_output_path.resolve()}")
    condensed_sitemap = build_condensed_sitemap(root_domain=root_domain, sitemap=sitemap)
    write_json(condensed_sitemap_output_path, condensed_sitemap)
    progress.info(f"Wrote condensed sitemap to {condensed_sitemap_output_path.resolve()}")

    discovered_urls = sorted(state.discovered_urls)
    endpoint_schema_payload = build_endpoint_schema_payload(discovered_urls)
    ai_data_text = render_endpoint_schema_text(endpoint_schema_payload)
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

    inventory = build_url_inventory(root_domain=root_domain, state=state)
    write_json(inventory_output_path, inventory)
    progress.info(f"Wrote URL inventory to {inventory_output_path.resolve()}")
    source_of_truth_payload = build_source_of_truth_payload(root_domain=root_domain, state=state)
    legacy_source_of_truth_output_path = source_of_truth_output_path.with_suffix(".jsonn")
    if source_of_truth_output_path.exists():
        existing_source_of_truth = read_json_file(source_of_truth_output_path)
    elif legacy_source_of_truth_output_path.exists():
        existing_source_of_truth = read_json_file(legacy_source_of_truth_output_path)
    else:
        existing_source_of_truth = {}
    merged_source_of_truth = merge_source_of_truth_payload(existing_source_of_truth, source_of_truth_payload)
    source_of_truth_output_path.write_text(
        json.dumps(to_json_compatible(merged_source_of_truth), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    parameters_payload = build_parameter_inventory_payload(merged_source_of_truth)
    parameters_output_path.write_text(
        json.dumps(to_json_compatible(parameters_payload), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
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
                html_report_content = generate_standard_html_report(
                    root_domain=root_domain,
                    sitemap=sitemap,
                    unique_urls=unique_urls,
                    api_endpoints=api_endpoints,
                    url_metadata=url_metadata,
                )

        report_path = html_report_output_path
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
            html_report_content = render_text_report_as_html(
                report_text=analysis,
                title=f"Nightmare AI Analysis: {root_domain}",
                subtitle="Rendered from final AI site analysis output",
            )
        report_path = html_report_output_path
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
    print(f"Evidence directory: {evidence_dir_path.resolve()}")
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
