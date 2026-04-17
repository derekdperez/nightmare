#!/usr/bin/env python3
"""Reusable Fozzy fuzzing core models and request builders."""

from __future__ import annotations

import hashlib
import json
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass, field

from nightmare_shared.value_types import infer_observed_value_type


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
    #: GET, POST, etc. GET may still carry a request body for fuzzing (non-standard but supported).
    http_method: str = "GET"
    observed_param_sets: set[frozenset[str]] = field(default_factory=set)
    params: dict[str, ParameterMeta] = field(default_factory=dict)
    body_params: dict[str, ParameterMeta] = field(default_factory=dict)
    observed_body_param_sets: set[frozenset[str]] = field(default_factory=set)
    #: Query ∪ body keys observed together (drives permutation generation).
    observed_combined_sets: set[frozenset[str]] = field(default_factory=set)
    extra_request_headers: dict[str, str] = field(default_factory=dict)
    body_content_type: str | None = None


def merge_data_type(existing: str, new_type: str, type_priority: list[str] | tuple[str, ...] | None = None) -> str:
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
    priority = list(type_priority or [])
    for candidate in priority:
        if candidate in {existing, new_type}:
            return candidate
    return "string"


def infer_value_type(value: str) -> str:
    return infer_observed_value_type(value)


def generic_value_for(meta: ParameterMeta, default_generic_by_type: dict[str, str] | None = None) -> str:
    table = default_generic_by_type or {}
    return str(table.get(meta.data_type, "test"))


def baseline_seed_value_for(meta: ParameterMeta, default_generic_by_type: dict[str, str] | None = None) -> str:
    observed = sorted({str(value) for value in meta.observed_values if str(value) != ""}, key=lambda item: (len(item), item))
    if observed:
        return observed[0]
    return generic_value_for(meta, default_generic_by_type)


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


def route_group_param_meta(group: RouteGroup, name: str) -> ParameterMeta:
    if name in group.params:
        return group.params[name]
    if name in group.body_params:
        return group.body_params[name]
    raise KeyError(name)


def route_group_all_param_names(group: RouteGroup) -> set[str]:
    return set(group.params.keys()) | set(group.body_params.keys())


def merge_request_headers(base_headers: dict[str, str] | None, extra_headers: dict[str, str] | None) -> dict[str, str]:
    out = {str(k): str(v) for k, v in (base_headers or {}).items() if str(k).strip()}
    for key, value in (extra_headers or {}).items():
        text = str(key).strip()
        if text:
            out[text] = str(value)
    return out


def build_urlencoded_form_body(values: dict[str, str]) -> bytes:
    pairs = [(name, values[name]) for name in sorted(values.keys())]
    return urllib.parse.urlencode(pairs, doseq=True).encode("utf-8")


def build_fuzz_http_request(
    group: RouteGroup,
    values: dict[str, str],
    *,
    base_headers: dict[str, str] | None = None,
) -> tuple[str, str, dict[str, str], bytes]:
    """Return (method, url, headers, body_bytes)."""
    qvals = {k: values[k] for k in group.params if k in values}
    bvals = {k: values[k] for k in group.body_params if k in values}
    url = build_url(group.scheme, group.host, group.path, qvals)
    headers = merge_request_headers(base_headers, group.extra_request_headers)
    body = b""
    if bvals:
        ct = (group.body_content_type or "application/x-www-form-urlencoded").strip().lower()
        if "json" in ct:
            headers.setdefault("Content-Type", group.body_content_type or "application/json")
            body = json.dumps({k: bvals[k] for k in sorted(bvals.keys())}, ensure_ascii=False).encode("utf-8")
        else:
            headers.setdefault("Content-Type", group.body_content_type or "application/x-www-form-urlencoded")
            body = build_urlencoded_form_body(bvals)
    method = (group.http_method or "GET").strip().upper() or "GET"
    return method, url, headers, body


def request_body_fingerprint(body: bytes | None) -> str:
    if not body:
        return ""
    return hashlib.sha256(body).hexdigest()[:16]


def build_placeholder_request_summary(group: RouteGroup, permutation: tuple[str, ...]) -> str:
    """Human-readable plan line including method and body placeholders when applicable."""
    qnames = [n for n in permutation if n in group.params]
    bnames = [n for n in permutation if n in group.body_params]
    q_meta = [group.params[n] for n in sorted(qnames)]
    url_ph = build_placeholder_url(group.scheme, group.host, group.path, q_meta) if q_meta else urllib.parse.urlunparse(
        (group.scheme, group.host, group.path, "", "", "")
    )
    method = (group.http_method or "GET").upper()
    if not bnames:
        return f"{method} {url_ph}"
    b_meta = [route_group_param_meta(group, n) for n in sorted(bnames)]
    counters: dict[str, int] = defaultdict(int)
    body_parts: list[str] = []
    for param in sorted(b_meta, key=lambda item: item.name):
        dtype = param.data_type or "string"
        counters[dtype] += 1
        ph = f"{{{dtype}{counters[dtype]}}}"
        key_enc = urllib.parse.quote_plus(param.name, safe="[]")
        body_parts.append(f"{key_enc}={ph}")
    body_s = "&".join(body_parts)
    return f"{method} {url_ph}  body({group.body_content_type or 'application/x-www-form-urlencoded'}): {body_s}"

