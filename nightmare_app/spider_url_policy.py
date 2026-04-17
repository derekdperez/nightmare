#!/usr/bin/env python3
"""Reusable URL policy helpers for Nightmare spider stages."""

from __future__ import annotations

import re
from collections import defaultdict
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from nightmare_shared.value_types import infer_observed_value_type


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    normalized = parsed._replace(scheme=scheme, netloc=netloc, fragment="", path=path)
    if normalized.path != "/":
        normalized = normalized._replace(path=normalized.path.rstrip("/"))
    return urlunparse(normalized)


def normalize_url_for_sitemap(url: str) -> str:
    normalized = normalize_url(url)
    parsed = urlparse(normalized)
    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    if not query_items:
        return normalized

    key_to_types: dict[str, set[str]] = defaultdict(set)
    for key, value in query_items:
        key_to_types[key].add(infer_observed_value_type(value))

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


def url_path_matches_high_value_suffix(url: str, high_value_path_suffixes: tuple[str, ...]) -> str | None:
    if not high_value_path_suffixes:
        return None
    parsed = urlparse(url)
    path = (parsed.path or "/").lower().rstrip("/") or "/"
    for suffix in high_value_path_suffixes:
        if path == suffix:
            return suffix
        if suffix != "/" and path.endswith(suffix):
            return suffix
    return None


def should_crawl_url(
    url: str,
    *,
    static_asset_extensions: set[str],
    high_value_extensions: set[str],
    high_value_path_suffixes: tuple[str, ...],
) -> bool:
    parsed = urlparse(url)
    path = parsed.path or "/"
    if url_path_matches_high_value_suffix(url, high_value_path_suffixes):
        return True
    if path.endswith("/"):
        return True
    filename = path.rsplit("/", 1)[-1]
    if "." not in filename:
        return True
    extension = f".{filename.rsplit('.', 1)[-1].lower()}"
    if high_value_extensions and extension in high_value_extensions:
        return True
    return extension not in static_asset_extensions


def should_include_url_in_ai_payload(url: str, ai_excluded_url_extensions: set[str]) -> bool:
    parsed = urlparse(url)
    path = parsed.path or "/"
    if path.endswith("/"):
        return True
    filename = path.rsplit("/", 1)[-1]
    if "." not in filename:
        return True
    extension = f".{filename.rsplit('.', 1)[-1].lower()}"
    return extension not in ai_excluded_url_extensions


def filter_urls_for_ai(urls: list[str], ai_excluded_url_extensions: set[str]) -> list[str]:
    return [
        normalize_url(url)
        for url in urls
        if isinstance(url, str) and url.strip() and should_include_url_in_ai_payload(url, ai_excluded_url_extensions)
    ]


def should_include_url_in_source_of_truth(url: str, static_asset_extensions: set[str]) -> bool:
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
    return extension not in static_asset_extensions


def filter_urls_for_source_of_truth(urls: list[str], static_asset_extensions: set[str]) -> list[str]:
    return [
        normalize_url(url)
        for url in urls
        if isinstance(url, str) and url.strip() and should_include_url_in_source_of_truth(url, static_asset_extensions)
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


def condense_urls_for_ai(urls: list[str], ai_excluded_url_extensions: set[str]) -> list[str]:
    condensed: list[str] = []
    seen: set[str] = set()
    for raw_url in sorted(set(filter_urls_for_ai(urls, ai_excluded_url_extensions))):
        summarized = summarize_url_for_ai(raw_url)
        if summarized in seen:
            continue
        seen.add(summarized)
        condensed.append(summarized)
    return condensed

