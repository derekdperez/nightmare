#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import httpx


DEFAULT_USER_AGENT = "nightmare-httpx/1.0"

_CLIENT_LOCK = threading.Lock()
_CLIENTS: dict[tuple[bool, float, bool, str], httpx.Client] = {}


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "true" if default else "false") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


_SENSITIVE_HEADER_NAMES = {
    "authorization",
    "cookie",
    "proxy-authorization",
    "set-cookie",
    "x-api-key",
    "x-auth-token",
    "x-csrf-token",
}


def _sanitize_headers_for_log(headers: dict[str, str], *, redact_authorization: bool = True) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in dict(headers or {}).items():
        key_text = str(key or "")
        val_text = str(value or "")
        normalized = key_text.strip().lower()
        if redact_authorization and normalized in _SENSITIVE_HEADER_NAMES:
            out[key_text] = "<redacted>"
        else:
            out[key_text] = val_text
    return out


def _truncate_text_for_log(text: str, max_chars: int | None) -> str:
    if max_chars is None:
        return str(text or "")
    safe_max = max(0, int(max_chars))
    value = str(text or "")
    if safe_max <= 0 or len(value) <= safe_max:
        return value
    return value[:safe_max] + f"...<truncated:{len(value) - safe_max} chars>"


def _validate_http_url(url: str) -> str:
    safe_url = str(url or "").strip()
    if not safe_url:
        raise ValueError("url is required")
    try:
        parsed = httpx.URL(safe_url)
    except Exception as exc:
        raise ValueError(f"invalid url: {safe_url!r}") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.host:
        raise ValueError("url must be an absolute http or https URL")
    return safe_url


def _has_header(headers: dict[str, str], header_name: str) -> bool:
    wanted = header_name.lower()
    return any(str(key or "").strip().lower() == wanted for key in headers)


def _client_key(verify: bool, timeout_seconds: float, follow_redirects: bool, user_agent: str) -> tuple[bool, float, bool, str]:
    return (bool(verify), float(timeout_seconds), bool(follow_redirects), str(user_agent or DEFAULT_USER_AGENT))


def get_shared_client(
    *,
    verify: bool = True,
    timeout_seconds: float = 30.0,
    follow_redirects: bool = True,
    user_agent: str = DEFAULT_USER_AGENT,
) -> httpx.Client:
    key = _client_key(verify, timeout_seconds, follow_redirects, user_agent)
    with _CLIENT_LOCK:
        client = _CLIENTS.get(key)
        if client is not None:
            return client
        timeout = httpx.Timeout(timeout_seconds)
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
        client = httpx.Client(
            timeout=timeout,
            follow_redirects=follow_redirects,
            verify=verify,
            headers={"User-Agent": user_agent},
            limits=limits,
        )
        _CLIENTS[key] = client
        return client


def close_shared_clients() -> None:
    with _CLIENT_LOCK:
        clients = list(_CLIENTS.values())
        _CLIENTS.clear()
    for client in clients:
        try:
            client.close()
        except Exception:
            pass


@dataclass
class CappedResponse:
    status_code: int
    url: str
    headers: dict[str, str]
    body: bytes
    elapsed_ms: int


def read_response_body_capped(response: httpx.Response, limit: int) -> bytes:
    if limit <= 0:
        return b""
    chunks: list[bytes] = []
    remaining = int(limit)
    for chunk in response.iter_bytes():
        if not chunk:
            continue
        if len(chunk) <= remaining:
            chunks.append(chunk)
            remaining -= len(chunk)
        else:
            chunks.append(chunk[:remaining])
            remaining = 0
        if remaining <= 0:
            break
    return b"".join(chunks)


def request_capped(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    content: bytes | None = None,
    timeout_seconds: float = 30.0,
    read_limit: int = 4096,
    follow_redirects: bool = True,
    verify: bool = True,
    client: httpx.Client | None = None,
    user_agent: str = DEFAULT_USER_AGENT,
    cookies: Any = None,
) -> CappedResponse:
    safe_url = _validate_http_url(url)
    http = client or get_shared_client(
        verify=verify,
        timeout_seconds=timeout_seconds,
        follow_redirects=follow_redirects,
        user_agent=user_agent,
    )
    request_headers = dict(headers or {})
    if not _has_header(request_headers, "User-Agent"):
        request_headers.setdefault("User-Agent", user_agent)
    t0 = time.perf_counter()
    with http.stream(
        method.upper(),
        safe_url,
        headers=request_headers,
        content=content,
        timeout=timeout_seconds,
        cookies=cookies,
    ) as response:
        body = read_response_body_capped(response, read_limit)
        elapsed_ms = int(round((time.perf_counter() - t0) * 1000.0))
        return CappedResponse(
            status_code=int(response.status_code),
            url=str(response.url),
            headers=dict(response.headers.items()),
            body=body,
            elapsed_ms=elapsed_ms,
        )


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json_payload: dict[str, Any] | None = None,
    timeout_seconds: float = 30.0,
    verify: bool = True,
    follow_redirects: bool = True,
    user_agent: str = DEFAULT_USER_AGENT,
    client: httpx.Client | None = None,
    logger: Any = None,
    log_details: bool | None = None,
    include_payloads: bool | None = None,
    max_logged_body_chars: int | None = None,
    redact_authorization_header: bool = True,
) -> dict[str, Any]:
    safe_url = _validate_http_url(url)
    http = client or get_shared_client(
        verify=verify,
        timeout_seconds=timeout_seconds,
        follow_redirects=follow_redirects,
        user_agent=user_agent,
    )
    request_headers = dict(headers or {})
    if not _has_header(request_headers, "User-Agent"):
        request_headers.setdefault("User-Agent", user_agent)
    should_log = bool(log_details) if log_details is not None else _env_truthy("NIGHTMARE_HTTP_LOG_DETAILS", default=False)
    should_include_payloads = (
        bool(include_payloads)
        if include_payloads is not None
        else _env_truthy("NIGHTMARE_HTTP_LOG_PAYLOADS", default=True)
    )
    started_at = time.perf_counter()
    if logger is not None and should_log:
        req_event: dict[str, Any] = {
            "http_method": method.upper(),
            "http_url": safe_url,
            "timeout_seconds": float(timeout_seconds),
            "verify_tls": bool(verify),
            "request_headers": _sanitize_headers_for_log(
                request_headers,
                redact_authorization=redact_authorization_header,
            ),
        }
        if should_include_payloads:
            req_event["request_json_payload"] = json_payload
        logger.info("http_request_outbound", **req_event)
    try:
        response = http.request(
            method.upper(),
            safe_url,
            headers=request_headers,
            json=json_payload,
            timeout=timeout_seconds,
        )
    except httpx.HTTPError as exc:
        if logger is not None and should_log:
            logger.error(
                "http_request_network_error",
                http_method=method.upper(),
                http_url=safe_url,
                error=str(exc),
            )
        raise RuntimeError(f"Network error {method.upper()} {safe_url}: {exc}") from exc
    elapsed_ms = int(round((time.perf_counter() - started_at) * 1000.0))
    text = response.text
    parsed_any: Any = None
    parse_error = ""
    try:
        parsed_any = json.loads(text or "{}")
    except Exception as exc:
        parse_error = str(exc)
        parsed_any = None
    if logger is not None and should_log:
        resp_event: dict[str, Any] = {
            "http_method": method.upper(),
            "http_url": safe_url,
            "http_status_code": int(response.status_code),
            "elapsed_ms": elapsed_ms,
            "response_headers": _sanitize_headers_for_log(
                dict(response.headers.items()),
                redact_authorization=redact_authorization_header,
            ),
        }
        if should_include_payloads:
            resp_event["response_text"] = _truncate_text_for_log(text, max_logged_body_chars)
            if parsed_any is not None:
                resp_event["response_json_payload"] = parsed_any
            elif parse_error:
                resp_event["response_json_parse_error"] = parse_error
        if response.is_error:
            logger.error("http_response_error", **resp_event)
        else:
            logger.info("http_response_inbound", **resp_event)
    if response.is_error:
        raise RuntimeError(f"HTTP {response.status_code} {method.upper()} {safe_url}: {text[:400]}")
    parsed = parsed_any if parsed_any is not None else {}
    return parsed if isinstance(parsed, dict) else {}
