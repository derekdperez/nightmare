#!/usr/bin/env python3
"""Normalized export generation for crawl/network artifacts."""

from __future__ import annotations

import base64
import gzip
import hashlib
import html
import json
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlparse
from xml.sax.saxutils import escape as xml_escape

from nightmare_app.spider_url_policy import normalize_url, normalize_url_for_sitemap
from nightmare_shared.value_types import infer_observed_value_type

SCHEMA_VERSION = 1
DEFAULT_PARAM_SCAN_MAX_BYTES = 256 * 1024


@dataclass(frozen=True)
class ExportFilterConfig:
    omit_regexes: tuple[str, ...]
    omit_directories: tuple[str, ...]
    omit_keywords: tuple[str, ...]


def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _read_json_gzip_dict(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            parsed = json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        text = path.read_text(encoding="utf-8-sig", errors="replace")
    except OSError:
        return rows
    for line in text.splitlines():
        item = line.strip()
        if not item:
            continue
        try:
            parsed = json.loads(item)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _to_rel(path: Path, root: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(root.resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def _normalize_string_list(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    out: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return tuple(out)


def _build_filter_config(config: dict[str, Any]) -> ExportFilterConfig:
    return ExportFilterConfig(
        omit_regexes=_normalize_string_list(config.get("sitemap_omit_regexes")),
        omit_directories=_normalize_string_list(config.get("sitemap_omit_directories")),
        omit_keywords=_normalize_string_list(config.get("sitemap_omit_keywords")),
    )


def _normalize_directory_prefix(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "/"
    if not text.startswith("/"):
        text = "/" + text
    if text != "/":
        text = text.rstrip("/")
    return text.lower()


def _match_omit_reason(url: str, filters: ExportFilterConfig) -> str | None:
    target = str(url or "")
    target_lower = target.lower()
    parsed = urlparse(target)
    path = (parsed.path or "/").lower()
    if path != "/":
        path = path.rstrip("/") or "/"

    for pattern in filters.omit_regexes:
        try:
            if re.search(pattern, target):
                return f"regex:{pattern}"
        except re.error:
            continue
    for keyword in filters.omit_keywords:
        if keyword.lower() in target_lower:
            return f"keyword:{keyword}"
    for prefix in filters.omit_directories:
        normalized = _normalize_directory_prefix(prefix)
        if path == normalized or path.startswith(f"{normalized}/"):
            return f"directory:{prefix}"
    return None


def _extract_headers(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    return {str(key): str(value) for key, value in raw.items()}


def _decode_body_base64(payload: dict[str, Any]) -> bytes:
    body_b64 = str(payload.get("body_base64", "") or "")
    if not body_b64:
        return b""
    try:
        return base64.b64decode(body_b64)
    except Exception:
        return b""


def _load_body_blob(payload: dict[str, Any], collected_data_root: Path) -> bytes | None:
    blob = payload.get("body_blob")
    if not isinstance(blob, dict):
        return None
    rel = str(blob.get("relative_path", "") or "").strip()
    if not rel:
        return None
    body_path = (collected_data_root / rel).resolve()
    if not body_path.is_file():
        return None
    try:
        if body_path.suffix.lower() == ".gz":
            with gzip.open(body_path, "rb") as handle:
                return handle.read()
        return body_path.read_bytes()
    except OSError:
        return None


def _flatten_json_scalars(value: Any, prefix: str = "") -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key in sorted(value.keys(), key=lambda item: str(item)):
            child = f"{prefix}.{key}" if prefix else str(key)
            out.extend(_flatten_json_scalars(value[key], child))
        return out
    if isinstance(value, list):
        for idx, item in enumerate(value):
            child = f"{prefix}[{idx}]"
            out.extend(_flatten_json_scalars(item, child))
        return out
    if value is None:
        return [(prefix, "")]
    return [(prefix, str(value))]


def _extract_query_parameters(url: str) -> list[dict[str, Any]]:
    parsed = urlparse(url)
    rows: list[dict[str, Any]] = []
    for name, value in parse_qsl(parsed.query, keep_blank_values=True):
        dtype = infer_observed_value_type(value)
        rows.append(
            {
                "name": name,
                "location": "query",
                "observed_data_types": [dtype],
                "observed_values": [value],
                "canonical_data_type": dtype,
            }
        )
    return rows


def _extract_body_parameters(headers: dict[str, str], body: bytes, *, max_scan_bytes: int) -> list[dict[str, Any]]:
    if not body:
        return []
    content_type = ""
    for key, value in headers.items():
        if key.lower() == "content-type":
            content_type = str(value).lower()
            break
    limited = body[: max(1, int(max_scan_bytes))]
    text = limited.decode("utf-8", errors="replace")
    out: list[dict[str, Any]] = []

    if "application/x-www-form-urlencoded" in content_type:
        for name, value in parse_qsl(text, keep_blank_values=True):
            dtype = infer_observed_value_type(value)
            out.append(
                {
                    "name": name,
                    "location": "body_form",
                    "observed_data_types": [dtype],
                    "observed_values": [value],
                    "canonical_data_type": dtype,
                }
            )
        return out

    if "application/json" in content_type:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return out
        for name, value in _flatten_json_scalars(payload):
            if not name:
                continue
            dtype = infer_observed_value_type(value)
            out.append(
                {
                    "name": name,
                    "location": "body_json",
                    "observed_data_types": [dtype],
                    "observed_values": [value],
                    "canonical_data_type": dtype,
                }
            )
    return out


def _dedupe_parameter_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("name", "")).strip()
        location = str(row.get("location", "")).strip() or "query"
        if not name:
            continue
        key = (location, name)
        types_raw = row.get("observed_data_types", [])
        values_raw = row.get("observed_values", [])
        types_set = {str(item).strip().lower() for item in types_raw if str(item).strip()} if isinstance(types_raw, list) else set()
        values_set = {str(item) for item in values_raw if item is not None} if isinstance(values_raw, list) else set()
        if key not in grouped:
            grouped[key] = {
                "name": name,
                "location": location,
                "observed_data_types": sorted(types_set),
                "observed_values": sorted(values_set)[:25],
                "canonical_data_type": infer_observed_value_type(next(iter(values_set), "")),
            }
            continue
        target = grouped[key]
        prev_types = set(target.get("observed_data_types", []))
        prev_values = set(target.get("observed_values", []))
        merged_types = {str(item).strip().lower() for item in prev_types | types_set if str(item).strip()}
        merged_values = {str(item) for item in prev_values | values_set}
        target["observed_data_types"] = sorted(merged_types)
        target["observed_values"] = sorted(merged_values)[:25]
        if target["observed_values"]:
            target["canonical_data_type"] = infer_observed_value_type(str(target["observed_values"][0]))
    return [grouped[key] for key in sorted(grouped.keys())]


def _hash_json(payload: dict[str, Any]) -> str:
    compact = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(compact).hexdigest()


def _write_dedup_json(payload: dict[str, Any], out_dir: Path) -> str:
    digest = _hash_json(payload)
    target = out_dir / f"{digest}.json"
    if not target.exists():
        _write_json(target, payload)
    return target.name


def _iter_endpoint_payloads(collected_data_root: Path) -> list[dict[str, Any]]:
    endpoints_dir = collected_data_root / "endpoints"
    out: list[dict[str, Any]] = []
    if not endpoints_dir.is_dir():
        return out
    for path in sorted(endpoints_dir.glob("*.json")):
        payload = _read_json_dict(path)
        if not payload:
            continue
        payload["_source_path"] = path
        out.append(payload)
    return out


def _iter_evidence_payloads(evidence_dir: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not evidence_dir.is_dir():
        return out
    for path in sorted(evidence_dir.glob("*.json.gz")):
        payload = _read_json_gzip_dict(path)
        if not payload:
            continue
        if not isinstance(payload.get("request"), dict):
            continue
        payload["_source_path"] = path
        out.append(payload)
    return out


def _build_event_record(
    payload: dict[str, Any],
    *,
    source_kind: str,
    source_path: Path,
    domain_output_dir: Path,
    collected_data_root: Path,
    raw_request_dir: Path,
    raw_response_dir: Path,
    max_param_scan_bytes: int,
) -> dict[str, Any] | None:
    request = payload.get("request")
    response = payload.get("response")
    if not isinstance(request, dict):
        return None

    request_headers = _extract_headers(request.get("headers"))
    response_headers = _extract_headers(response.get("headers") if isinstance(response, dict) else {})
    request_url = normalize_url(str(request.get("url", "") or ""))
    if not request_url:
        return None

    req_blob = _load_body_blob(request, collected_data_root)
    req_body = req_blob if req_blob is not None else _decode_body_base64(request)
    rsp_blob = _load_body_blob(response, collected_data_root) if isinstance(response, dict) else None
    rsp_body = rsp_blob if rsp_blob is not None else (_decode_body_base64(response) if isinstance(response, dict) else b"")

    request_object = {
        "method": str(request.get("method", "GET") or "GET").upper(),
        "url": request_url,
        "headers": request_headers,
        "body_size_bytes": len(req_body),
        "body_sha256": hashlib.sha256(req_body).hexdigest() if req_body else "",
        "body_blob": request.get("body_blob") if isinstance(request.get("body_blob"), dict) else None,
    }
    response_object = {
        "status": int(response.get("status", 0) or 0) if isinstance(response, dict) else 0,
        "url": normalize_url(str(response.get("url", request_url) or request_url)) if isinstance(response, dict) else request_url,
        "headers": response_headers,
        "body_size_bytes": len(rsp_body),
        "body_sha256": hashlib.sha256(rsp_body).hexdigest() if rsp_body else "",
        "body_blob": response.get("body_blob") if isinstance(response, dict) else None,
    }

    params = _extract_query_parameters(request_url)
    params.extend(_extract_body_parameters(request_headers, req_body, max_scan_bytes=max_param_scan_bytes))
    params = _dedupe_parameter_rows(params)

    request_file = _write_dedup_json(
        {
            "schema_version": SCHEMA_VERSION,
            "source": source_kind,
            "source_record": _to_rel(source_path, domain_output_dir),
            "request": request_object,
        },
        raw_request_dir,
    )
    response_file = _write_dedup_json(
        {
            "schema_version": SCHEMA_VERSION,
            "source": source_kind,
            "source_record": _to_rel(source_path, domain_output_dir),
            "response": response_object,
        },
        raw_response_dir,
    )
    request_fingerprint = _hash_json(request_object)

    return {
        "event_id": f"{source_kind}:{source_path.stem}",
        "captured_at_utc": str(payload.get("captured_at_utc", "") or ""),
        "source": source_kind,
        "source_record": _to_rel(source_path, domain_output_dir),
        "source_type": str(payload.get("source_type", payload.get("role", "")) or ""),
        "role": str(payload.get("role", "") or ""),
        "request": request_object,
        "response": response_object,
        "parameters": params,
        "request_fingerprint": request_fingerprint,
        "raw_request_link": f"normalized_data/raw_requests/{request_file}",
        "raw_response_link": f"normalized_data/raw_responses/{response_file}",
    }


def _build_requests_payload(
    *,
    root_domain: str,
    generated_at_utc: str,
    events: list[dict[str, Any]],
    legacy_requests_payload: dict[str, Any],
) -> dict[str, Any]:
    by_fingerprint: dict[str, dict[str, Any]] = {}
    for event in events:
        fingerprint = str(event.get("request_fingerprint", "") or "")
        if not fingerprint:
            continue
        request = event.get("request") if isinstance(event.get("request"), dict) else {}
        response = event.get("response") if isinstance(event.get("response"), dict) else {}
        status = int(response.get("status", 0) or 0)
        if fingerprint not in by_fingerprint:
            by_fingerprint[fingerprint] = {
                "request_fingerprint": fingerprint,
                "request_method": str(request.get("method", "") or ""),
                "request_url": str(request.get("url", "") or ""),
                "occurrences": 1,
                "status_codes": sorted({status}),
                "event_ids": [str(event.get("event_id", "") or "")],
            }
            continue
        row = by_fingerprint[fingerprint]
        row["occurrences"] = int(row.get("occurrences", 0)) + 1
        statuses = set(row.get("status_codes", []))
        statuses.add(status)
        row["status_codes"] = sorted(int(code) for code in statuses)
        event_ids = row.get("event_ids", [])
        if isinstance(event_ids, list):
            event_ids.append(str(event.get("event_id", "") or ""))

    legacy_entries = legacy_requests_payload.get("entries", [])
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "root_domain": root_domain,
        "total_requested_urls": int(legacy_requests_payload.get("total_requested_urls", 0) or 0),
        "entries": legacy_entries if isinstance(legacy_entries, list) else [],
        "request_event_count": len(events),
        "unique_request_fingerprint_count": len(by_fingerprint),
        "request_events": events,
        "unique_requests": [by_fingerprint[key] for key in sorted(by_fingerprint.keys())],
    }


def _seed_sitemap_rows(url_inventory_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    entries = url_inventory_payload.get("entries", [])
    if not isinstance(entries, list):
        return out
    for row in entries:
        if not isinstance(row, dict):
            continue
        url = normalize_url(str(row.get("url", "") or ""))
        if not url:
            continue
        parsed = urlparse(url)
        out[url] = {
            "url": url,
            "normalized_sitemap_url": normalize_url_for_sitemap(url),
            "host": parsed.netloc.lower(),
            "path": parsed.path or "/",
            "query": parsed.query,
            "directory": str(Path(parsed.path or "/").parent).replace("\\", "/"),
            "exists_confirmed": bool(row.get("exists_confirmed", False)),
            "existence_status_code": row.get("existence_status_code"),
            "crawl_status_code": row.get("crawl_status_code"),
            "soft_404_detected": bool(row.get("soft_404_detected", False)),
            "discovered_via": sorted(str(item) for item in row.get("discovered_via", []) if str(item).strip())
            if isinstance(row.get("discovered_via"), list)
            else [],
            "request_count": 0,
            "response_status_codes": [],
            "parameters": [],
            "raw_request_links": [],
            "raw_response_links": [],
            "request_event_ids": [],
        }
    return out


def _build_sitemap_payload(
    *,
    root_domain: str,
    generated_at_utc: str,
    url_inventory_payload: dict[str, Any],
    events: list[dict[str, Any]],
    filters: ExportFilterConfig,
) -> dict[str, Any]:
    rows = _seed_sitemap_rows(url_inventory_payload)
    for event in events:
        request = event.get("request") if isinstance(event.get("request"), dict) else {}
        response = event.get("response") if isinstance(event.get("response"), dict) else {}
        url = normalize_url(str(request.get("url", "") or ""))
        if not url:
            continue
        parsed = urlparse(url)
        row = rows.setdefault(
            url,
            {
                "url": url,
                "normalized_sitemap_url": normalize_url_for_sitemap(url),
                "host": parsed.netloc.lower(),
                "path": parsed.path or "/",
                "query": parsed.query,
                "directory": str(Path(parsed.path or "/").parent).replace("\\", "/"),
                "exists_confirmed": False,
                "existence_status_code": None,
                "crawl_status_code": None,
                "soft_404_detected": False,
                "discovered_via": [],
                "request_count": 0,
                "response_status_codes": [],
                "parameters": [],
                "raw_request_links": [],
                "raw_response_links": [],
                "request_event_ids": [],
            },
        )
        row["request_count"] = int(row.get("request_count", 0)) + 1
        statuses = set(row.get("response_status_codes", []))
        statuses.add(int(response.get("status", 0) or 0))
        row["response_status_codes"] = sorted(int(code) for code in statuses)
        row["parameters"] = _dedupe_parameter_rows((row.get("parameters", []) if isinstance(row.get("parameters"), list) else []) + (event.get("parameters", []) if isinstance(event.get("parameters"), list) else []))
        req_links = set(row.get("raw_request_links", []))
        rsp_links = set(row.get("raw_response_links", []))
        req_links.add(str(event.get("raw_request_link", "") or ""))
        rsp_links.add(str(event.get("raw_response_link", "") or ""))
        row["raw_request_links"] = sorted(item for item in req_links if item)
        row["raw_response_links"] = sorted(item for item in rsp_links if item)
        event_ids = set(row.get("request_event_ids", []))
        event_ids.add(str(event.get("event_id", "") or ""))
        row["request_event_ids"] = sorted(item for item in event_ids if item)

    included: list[dict[str, Any]] = []
    omitted: list[dict[str, Any]] = []
    for url in sorted(rows.keys()):
        row = rows[url]
        reason = _match_omit_reason(url, filters)
        if reason:
            omitted.append({"url": url, "omit_reason": reason})
            continue
        row["parameter_count"] = len(row.get("parameters", []))
        included.append(row)

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "root_domain": root_domain,
        "total_urls": len(included),
        "omitted_url_count": len(omitted),
        "filter_config": {
            "omit_regexes": list(filters.omit_regexes),
            "omit_directories": list(filters.omit_directories),
            "omit_keywords": list(filters.omit_keywords),
        },
        "entries": included,
        "omitted_entries": omitted,
    }


def _build_sitemap_xml(sitemap_payload: dict[str, Any]) -> str:
    entries = sitemap_payload.get("entries", [])
    if not isinstance(entries, list):
        entries = []
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<nightmareSitemap>",
        f"  <generatedAtUtc>{xml_escape(str(sitemap_payload.get('generated_at_utc', '') or ''))}</generatedAtUtc>",
        f"  <rootDomain>{xml_escape(str(sitemap_payload.get('root_domain', '') or ''))}</rootDomain>",
        f"  <totalUrls>{int(sitemap_payload.get('total_urls', 0) or 0)}</totalUrls>",
        "  <urls>",
    ]
    for row in entries:
        if not isinstance(row, dict):
            continue
        lines.append("    <urlEntry>")
        lines.append(f"      <url>{xml_escape(str(row.get('url', '') or ''))}</url>")
        lines.append(f"      <host>{xml_escape(str(row.get('host', '') or ''))}</host>")
        lines.append(f"      <path>{xml_escape(str(row.get('path', '') or ''))}</path>")
        lines.append(f"      <requestCount>{int(row.get('request_count', 0) or 0)}</requestCount>")
        lines.append("      <parameters>")
        params = row.get("parameters", [])
        if isinstance(params, list):
            for param in params:
                if not isinstance(param, dict):
                    continue
                lines.append(
                    "        "
                    + (
                        f"<parameter name=\"{xml_escape(str(param.get('name', '') or ''))}\" "
                        f"location=\"{xml_escape(str(param.get('location', '') or ''))}\" "
                        f"canonicalType=\"{xml_escape(str(param.get('canonical_data_type', '') or ''))}\" />"
                    )
                )
        lines.append("      </parameters>")
        lines.append("      <rawRequestLinks>")
        req_links = row.get("raw_request_links", [])
        if isinstance(req_links, list):
            for link in req_links:
                lines.append(f"        <link>{xml_escape(str(link or ''))}</link>")
        lines.append("      </rawRequestLinks>")
        lines.append("      <rawResponseLinks>")
        rsp_links = row.get("raw_response_links", [])
        if isinstance(rsp_links, list):
            for link in rsp_links:
                lines.append(f"        <link>{xml_escape(str(link or ''))}</link>")
        lines.append("      </rawResponseLinks>")
        lines.append("    </urlEntry>")
    lines.extend(["  </urls>", "</nightmareSitemap>", ""])
    return "\n".join(lines)


def _build_sitemap_html(sitemap_payload: dict[str, Any]) -> str:
    entries = sitemap_payload.get("entries", [])
    if not isinstance(entries, list):
        entries = []
    rows_html: list[str] = []
    for row in entries:
        if not isinstance(row, dict):
            continue
        url = html.escape(str(row.get("url", "") or ""))
        host = html.escape(str(row.get("host", "") or ""))
        path = html.escape(str(row.get("path", "") or ""))
        request_count = int(row.get("request_count", 0) or 0)
        statuses = ", ".join(str(code) for code in row.get("response_status_codes", [])) if isinstance(row.get("response_status_codes"), list) else ""
        statuses = html.escape(statuses)
        params = row.get("parameters", [])
        params_html = "<span class='muted'>none</span>"
        if isinstance(params, list) and params:
            params_html = "<br>".join(
                html.escape(
                    f"{str(param.get('location', '') or 'query')}:{str(param.get('name', '') or '')}"
                    f" ({str(param.get('canonical_data_type', '') or 'string')})"
                )
                for param in params
                if isinstance(param, dict)
            )
        request_links = row.get("raw_request_links", [])
        response_links = row.get("raw_response_links", [])
        request_links_html = (
            "<br>".join(
                f"<a href=\"{html.escape(str(link))}\" target=\"_blank\" rel=\"noopener noreferrer\">request</a>"
                for link in request_links
            )
            if isinstance(request_links, list) and request_links
            else "<span class='muted'>none</span>"
        )
        response_links_html = (
            "<br>".join(
                f"<a href=\"{html.escape(str(link))}\" target=\"_blank\" rel=\"noopener noreferrer\">response</a>"
                for link in response_links
            )
            if isinstance(response_links, list) and response_links
            else "<span class='muted'>none</span>"
        )
        rows_html.append(
            "<tr>"
            f"<td>{url}</td>"
            f"<td>{host}</td>"
            f"<td>{path}</td>"
            f"<td data-raw=\"{request_count}\">{request_count}</td>"
            f"<td>{statuses}</td>"
            f"<td>{params_html}</td>"
            f"<td>{request_links_html}</td>"
            f"<td>{response_links_html}</td>"
            "</tr>"
        )
    table_body = "\n".join(rows_html) if rows_html else "<tr><td colspan=\"8\">No rows</td></tr>"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sitemap</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 16px; background: #f8fafc; color: #0f172a; }}
    .toolbar {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }}
    input {{ min-width: 320px; border: 1px solid #94a3b8; border-radius: 6px; padding: 6px 8px; }}
    table {{ width: 100%; border-collapse: collapse; border: 1px solid #cbd5e1; background: #fff; }}
    th, td {{ border: 1px solid #cbd5e1; padding: 6px 8px; text-align: left; font-size: 12px; vertical-align: top; }}
    th {{ background: #e2e8f0; cursor: pointer; user-select: none; }}
    .muted {{ color: #64748b; }}
  </style>
</head>
<body>
  <h1>Sitemap</h1>
  <div class="toolbar">
    <input id="search" placeholder="Search URL, host, path, parameters, status..." />
    <span id="summary" class="muted"></span>
  </div>
  <table id="table">
    <thead>
      <tr>
        <th data-type="text">URL</th>
        <th data-type="text">Host</th>
        <th data-type="text">Path</th>
        <th data-type="number">Requests</th>
        <th data-type="text">Statuses</th>
        <th data-type="text">Parameters</th>
        <th data-type="text">Raw Request</th>
        <th data-type="text">Raw Response</th>
      </tr>
    </thead>
    <tbody>{table_body}</tbody>
  </table>
  <script>
    const table = document.getElementById("table");
    const tbody = table.querySelector("tbody");
    const rows = Array.from(tbody.querySelectorAll("tr"));
    const search = document.getElementById("search");
    const summary = document.getElementById("summary");
    function refreshSummary() {{
      const visible = rows.filter((row) => row.style.display !== "none").length;
      summary.textContent = `Showing ${{visible}} / ${{rows.length}} row(s)`;
    }}
    function applySearch() {{
      const needle = String(search.value || "").trim().toLowerCase();
      rows.forEach((row) => {{
        row.style.display = (!needle || row.textContent.toLowerCase().includes(needle)) ? "" : "none";
      }});
      refreshSummary();
    }}
    search.addEventListener("input", applySearch);
    Array.from(table.querySelectorAll("th")).forEach((th, idx) => {{
      let asc = true;
      th.addEventListener("click", () => {{
        const type = String(th.dataset.type || "text");
        rows.sort((a, b) => {{
          const av = (a.children[idx]?.getAttribute("data-raw") ?? a.children[idx]?.textContent ?? "").trim();
          const bv = (b.children[idx]?.getAttribute("data-raw") ?? b.children[idx]?.textContent ?? "").trim();
          if (type === "number") {{
            return asc ? Number(av || "0") - Number(bv || "0") : Number(bv || "0") - Number(av || "0");
          }}
          return asc ? av.localeCompare(bv) : bv.localeCompare(av);
        }});
        rows.forEach((row) => tbody.appendChild(row));
        asc = !asc;
        applySearch();
      }});
    }});
    refreshSummary();
  </script>
</body>
</html>
"""


def _build_cookies_payload(*, root_domain: str, generated_at_utc: str, collected_data_root: Path) -> dict[str, Any]:
    cookies_dir = collected_data_root / "cookies"
    raw_events: list[dict[str, Any]] = []
    dedupe: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if cookies_dir.is_dir():
        for path in sorted(cookies_dir.glob("*.jsonl")):
            for row in _read_jsonl_rows(path):
                set_cookie_rows = row.get("set_cookie", [])
                if not isinstance(set_cookie_rows, list):
                    set_cookie_rows = []
                page_url = normalize_url(str(row.get("page_url", "") or ""))
                response_url = normalize_url(str(row.get("response_url", "") or ""))
                for raw_cookie in set_cookie_rows:
                    text = str(raw_cookie or "").strip()
                    if not text:
                        continue
                    parsed = SimpleCookie()
                    try:
                        parsed.load(text)
                    except Exception:
                        parsed = SimpleCookie()
                    if not parsed:
                        raw_events.append({"raw": text, "page_url": page_url, "response_url": response_url})
                        continue
                    for name in parsed.keys():
                        morsel = parsed[name]
                        value = str(morsel.value)
                        domain = str(morsel["domain"] or "")
                        cookie_path = str(morsel["path"] or "")
                        key = (name, value, domain, cookie_path)
                        dedupe.setdefault(
                            key,
                            {
                                "name": name,
                                "value": value,
                                "domain": domain,
                                "path": cookie_path,
                                "expires": str(morsel["expires"] or ""),
                                "secure": bool(morsel["secure"]),
                                "httponly": bool(morsel["httponly"]),
                                "samesite": str(morsel["samesite"] or ""),
                                "sources": [],
                            },
                        )
                        dedupe[key]["sources"].append(
                            {"page_url": page_url, "response_url": response_url, "raw": text}
                        )
                        raw_events.append(
                            {"name": name, "value": value, "raw": text, "page_url": page_url, "response_url": response_url}
                        )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "root_domain": root_domain,
        "raw_cookie_event_count": len(raw_events),
        "unique_cookie_count": len(dedupe),
        "raw_cookie_events": raw_events,
        "cookies": [dedupe[key] for key in sorted(dedupe.keys())],
    }


def _script_language(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in {".js", ".mjs", ".cjs", ".jsx"}:
        return "javascript"
    if ext in {".ts", ".tsx"}:
        return "typescript"
    if ext == ".php":
        return "php"
    if ext == ".sql":
        return "sql"
    return "unknown"


def _build_scripts_payload(*, root_domain: str, generated_at_utc: str, collected_data_root: Path) -> dict[str, Any]:
    scripts_dir = collected_data_root / "scripts"
    entries: list[dict[str, Any]] = []
    dedupe: dict[str, dict[str, Any]] = {}
    if scripts_dir.is_dir():
        for script_path in sorted(scripts_dir.rglob("*")):
            if not script_path.is_file() or script_path.name.endswith(".fetch.json"):
                continue
            try:
                body = script_path.read_bytes()
            except OSError:
                continue
            digest = hashlib.sha256(body).hexdigest()
            rel = script_path.relative_to(collected_data_root).as_posix()
            row = {
                "relative_path": rel,
                "size_bytes": len(body),
                "sha256": digest,
                "language": _script_language(script_path),
            }
            entries.append(row)
            if digest not in dedupe:
                dedupe[digest] = {
                    "sha256": digest,
                    "size_bytes": len(body),
                    "language": row["language"],
                    "occurrences": 1,
                    "paths": [rel],
                }
            else:
                dedupe[digest]["occurrences"] = int(dedupe[digest].get("occurrences", 0)) + 1
                dedupe[digest]["paths"].append(rel)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "root_domain": root_domain,
        "script_file_count": len(entries),
        "unique_script_hash_count": len(dedupe),
        "scripts": entries,
        "unique_scripts": [dedupe[key] for key in sorted(dedupe.keys())],
    }


def _build_high_value_payload(
    *,
    root_domain: str,
    generated_at_utc: str,
    domain_output_dir: Path,
    high_value_root: Path,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    if high_value_root.is_dir():
        for meta_path in sorted(high_value_root.rglob("*.json")):
            payload = _read_json_dict(meta_path)
            if not payload:
                continue
            saved_relative = str(payload.get("saved_relative", "") or "")
            entries.append(
                {
                    "metadata_relative_path": _to_rel(meta_path, domain_output_dir),
                    "url": normalize_url(str(payload.get("url", "") or "")),
                    "captured_at_utc": str(payload.get("captured_at_utc", "") or ""),
                    "high_value_extension": str(payload.get("high_value_extension", "") or ""),
                    "high_value_path_suffix": str(payload.get("high_value_path_suffix", "") or ""),
                    "saved_relative": saved_relative,
                    "request": payload.get("request", {}),
                    "response": payload.get("response", {}),
                }
            )
    dedupe: dict[str, dict[str, Any]] = {}
    for row in entries:
        response = row.get("response") if isinstance(row.get("response"), dict) else {}
        key = f"{row.get('url')}|{str(response.get('body_sha256', '') or '')}"
        if key not in dedupe:
            dedupe[key] = {
                "url": row.get("url"),
                "response_body_sha256": str(response.get("body_sha256", "") or ""),
                "occurrences": 1,
                "saved_relative_paths": [row.get("saved_relative")],
            }
        else:
            dedupe[key]["occurrences"] = int(dedupe[key].get("occurrences", 0)) + 1
            dedupe[key]["saved_relative_paths"].append(row.get("saved_relative"))
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "root_domain": root_domain,
        "entry_count": len(entries),
        "unique_entry_count": len(dedupe),
        "entries": entries,
        "unique_entries": [dedupe[key] for key in sorted(dedupe.keys())],
    }


def _mirror_directory_files(source_dir: Path, target_dir: Path, *, skip_suffixes: tuple[str, ...] = ()) -> list[str]:
    copied: list[str] = []
    if not source_dir.is_dir():
        return copied
    for src in sorted(source_dir.rglob("*")):
        if not src.is_file():
            continue
        if skip_suffixes and any(str(src.name).endswith(suf) for suf in skip_suffixes):
            continue
        rel = src.relative_to(source_dir)
        dst = target_dir / rel
        _ensure_directory(dst.parent)
        shutil.copy2(src, dst)
        copied.append(dst.as_posix())
    return copied


def _build_pages_payload(
    *,
    root_domain: str,
    generated_at_utc: str,
    domain_output_dir: Path,
    pages_dir: Path,
    events: list[dict[str, Any]],
    collected_data_root: Path,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    dedupe: dict[str, dict[str, Any]] = {}
    for event in events:
        response = event.get("response") if isinstance(event.get("response"), dict) else {}
        response_url = normalize_url(str(response.get("url", "") or ""))
        if not response_url:
            continue
        body: bytes | None = None
        blob = response.get("body_blob")
        if isinstance(blob, dict):
            rel = str(blob.get("relative_path", "") or "").strip()
            if rel:
                body_path = collected_data_root / rel
                if body_path.is_file():
                    try:
                        if body_path.suffix.lower() == ".gz":
                            with gzip.open(body_path, "rb") as handle:
                                body = handle.read()
                        else:
                            body = body_path.read_bytes()
                    except OSError:
                        body = None
        if body is None:
            body = b""
        parsed = urlparse(response_url)
        host = (parsed.hostname or "host").lower()
        stem = re.sub(r"[^a-zA-Z0-9._-]+", "_", parsed.path.strip("/") or "root").strip("._-") or "root"
        digest = hashlib.sha256(response_url.encode("utf-8")).hexdigest()[:12]
        ext = ""
        path_tail = parsed.path.rsplit("/", 1)[-1]
        if "." in path_tail:
            ext = "." + path_tail.rsplit(".", 1)[-1].lower()
        if not ext:
            ext = ".bin"
        filename = f"{host}_{stem}_{digest}{ext}"
        out_path = pages_dir / filename
        _ensure_directory(out_path.parent)
        out_path.write_bytes(body)
        entry = {
            "event_id": str(event.get("event_id", "") or ""),
            "url": response_url,
            "status": int(response.get("status", 0) or 0),
            "saved_path": _to_rel(out_path, domain_output_dir),
            "size_bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest() if body else "",
            "raw_response_link": str(event.get("raw_response_link", "") or ""),
        }
        entries.append(entry)
        key = f"{entry['url']}|{entry['sha256']}"
        dedupe.setdefault(
            key,
            {"url": entry["url"], "sha256": entry["sha256"], "occurrences": 0, "saved_paths": []},
        )
        dedupe[key]["occurrences"] = int(dedupe[key].get("occurrences", 0)) + 1
        dedupe[key]["saved_paths"].append(entry["saved_path"])
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "root_domain": root_domain,
        "page_count": len(entries),
        "unique_page_count": len(dedupe),
        "pages": entries,
        "unique_pages": [dedupe[key] for key in sorted(dedupe.keys())],
    }


def _build_redirects_payload(*, root_domain: str, generated_at_utc: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    redirects: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int]] = set()
    for event in events:
        response = event.get("response") if isinstance(event.get("response"), dict) else {}
        request = event.get("request") if isinstance(event.get("request"), dict) else {}
        status = int(response.get("status", 0) or 0)
        if status < 300 or status > 399:
            continue
        headers = response.get("headers") if isinstance(response.get("headers"), dict) else {}
        location = ""
        for key, value in headers.items():
            if str(key).lower() == "location":
                location = str(value)
                break
        from_url = normalize_url(str(request.get("url", "") or ""))
        to_url = normalize_url(location) if location else ""
        key = (from_url, to_url, status)
        if key in seen:
            continue
        seen.add(key)
        redirects.append(
            {
                "event_id": str(event.get("event_id", "") or ""),
                "from_url": from_url,
                "to_url": to_url,
                "status_code": status,
                "captured_at_utc": str(event.get("captured_at_utc", "") or ""),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "root_domain": root_domain,
        "redirect_count": len(redirects),
        "redirects": redirects,
    }


def _build_findings_payload(
    *,
    root_domain: str,
    generated_at_utc: str,
    url_inventory_payload: dict[str, Any],
    redirects_payload: dict[str, Any],
    high_value_payload: dict[str, Any],
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()

    def _append_finding(finding_type: str, severity: str, url: str, details: Any) -> None:
        normalized_url = normalize_url(str(url or "")) if url else ""
        normalized_details = str(details or "")
        key = (finding_type, severity, normalized_url, normalized_details)
        if key in seen:
            return
        seen.add(key)
        entries.append(
            {
                "type": finding_type,
                "severity": severity,
                "url": normalized_url,
                "details": normalized_details,
            }
        )

    url_entries = url_inventory_payload.get("entries", [])
    if isinstance(url_entries, list):
        for row in url_entries:
            if not isinstance(row, dict):
                continue
            url = normalize_url(str(row.get("url", "") or ""))
            if not url:
                continue
            if bool(row.get("soft_404_detected", False)):
                _append_finding("soft_404", "medium", url, row.get("soft_404_reason"))
            status = row.get("crawl_status_code")
            try:
                code = int(status) if status is not None else 0
            except (TypeError, ValueError):
                code = 0
            if code >= 500:
                _append_finding("server_error", "high", url, f"HTTP {code}")
    for redirect in redirects_payload.get("redirects", []) if isinstance(redirects_payload.get("redirects"), list) else []:
        if not isinstance(redirect, dict):
            continue
        to_url = str(redirect.get("to_url", "") or "")
        if to_url.startswith("http://"):
            _append_finding("insecure_redirect_target", "medium", str(redirect.get("from_url", "") or ""), to_url)
    if int(high_value_payload.get("entry_count", 0) or 0) > 0:
        _append_finding(
            "high_value_artifacts_detected",
            "info",
            "",
            f"{int(high_value_payload.get('entry_count', 0) or 0)} high-value artifact(s) collected",
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "root_domain": root_domain,
        "finding_count": len(entries),
        "findings": entries,
    }


def write_normalized_exports(
    *,
    root_domain: str,
    domain_output_dir: Path,
    collected_data_root: Path,
    evidence_dir: Path,
    url_inventory_payload: dict[str, Any],
    legacy_requests_payload: dict[str, Any],
    config: dict[str, Any],
    normalized_data_dir: Path,
    requests_output_path: Path,
    sitemap_json_output_path: Path,
    sitemap_xml_output_path: Path,
    sitemap_html_output_path: Path,
    cookies_output_path: Path,
    scripts_output_path: Path,
    high_value_output_path: Path,
    pages_output_path: Path,
    redirects_output_path: Path,
    findings_output_path: Path,
    cookies_dir_output_path: Path,
    scripts_dir_output_path: Path,
    high_value_dir_output_path: Path,
    pages_dir_output_path: Path,
    source_high_value_root: Path,
) -> dict[str, Path]:
    generated_at_utc = datetime.now(timezone.utc).isoformat()
    _ensure_directory(normalized_data_dir)
    raw_request_dir = normalized_data_dir / "raw_requests"
    raw_response_dir = normalized_data_dir / "raw_responses"
    _ensure_directory(raw_request_dir)
    _ensure_directory(raw_response_dir)

    max_param_scan_bytes = int(config.get("normalized_body_parameter_scan_max_bytes", DEFAULT_PARAM_SCAN_MAX_BYTES) or DEFAULT_PARAM_SCAN_MAX_BYTES)
    max_param_scan_bytes = max(1024, max_param_scan_bytes)

    events: list[dict[str, Any]] = []
    for payload in _iter_endpoint_payloads(collected_data_root):
        source_path = payload.get("_source_path")
        if not isinstance(source_path, Path):
            continue
        record = _build_event_record(
            payload,
            source_kind="endpoint",
            source_path=source_path,
            domain_output_dir=domain_output_dir,
            collected_data_root=collected_data_root,
            raw_request_dir=raw_request_dir,
            raw_response_dir=raw_response_dir,
            max_param_scan_bytes=max_param_scan_bytes,
        )
        if record is not None:
            events.append(record)
    for payload in _iter_evidence_payloads(evidence_dir):
        source_path = payload.get("_source_path")
        if not isinstance(source_path, Path):
            continue
        record = _build_event_record(
            payload,
            source_kind="evidence",
            source_path=source_path,
            domain_output_dir=domain_output_dir,
            collected_data_root=collected_data_root,
            raw_request_dir=raw_request_dir,
            raw_response_dir=raw_response_dir,
            max_param_scan_bytes=max_param_scan_bytes,
        )
        if record is not None:
            events.append(record)
    events.sort(key=lambda row: (str(row.get("captured_at_utc", "") or ""), str(row.get("event_id", "") or "")))

    filters = _build_filter_config(config)
    sitemap_payload = _build_sitemap_payload(
        root_domain=root_domain,
        generated_at_utc=generated_at_utc,
        url_inventory_payload=url_inventory_payload,
        events=events,
        filters=filters,
    )
    requests_payload = _build_requests_payload(
        root_domain=root_domain,
        generated_at_utc=generated_at_utc,
        events=events,
        legacy_requests_payload=legacy_requests_payload,
    )
    cookies_payload = _build_cookies_payload(
        root_domain=root_domain,
        generated_at_utc=generated_at_utc,
        collected_data_root=collected_data_root,
    )
    scripts_payload = _build_scripts_payload(
        root_domain=root_domain,
        generated_at_utc=generated_at_utc,
        collected_data_root=collected_data_root,
    )

    high_value_payload = _build_high_value_payload(
        root_domain=root_domain,
        generated_at_utc=generated_at_utc,
        domain_output_dir=domain_output_dir,
        high_value_root=source_high_value_root,
    )
    pages_payload = _build_pages_payload(
        root_domain=root_domain,
        generated_at_utc=generated_at_utc,
        domain_output_dir=domain_output_dir,
        pages_dir=pages_dir_output_path,
        events=events,
        collected_data_root=collected_data_root,
    )
    redirects_payload = _build_redirects_payload(
        root_domain=root_domain,
        generated_at_utc=generated_at_utc,
        events=events,
    )
    findings_payload = _build_findings_payload(
        root_domain=root_domain,
        generated_at_utc=generated_at_utc,
        url_inventory_payload=url_inventory_payload,
        redirects_payload=redirects_payload,
        high_value_payload=high_value_payload,
    )

    _write_json(sitemap_json_output_path, sitemap_payload)
    _write_json(requests_output_path, requests_payload)
    _write_json(cookies_output_path, cookies_payload)
    _write_json(scripts_output_path, scripts_payload)
    _write_json(high_value_output_path, high_value_payload)
    _write_json(pages_output_path, pages_payload)
    _write_json(redirects_output_path, redirects_payload)
    _write_json(findings_output_path, findings_payload)

    _ensure_directory(sitemap_xml_output_path.parent)
    sitemap_xml_output_path.write_text(_build_sitemap_xml(sitemap_payload), encoding="utf-8")
    _ensure_directory(sitemap_html_output_path.parent)
    sitemap_html_output_path.write_text(_build_sitemap_html(sitemap_payload), encoding="utf-8")

    _mirror_directory_files(collected_data_root / "cookies", cookies_dir_output_path)
    _mirror_directory_files(collected_data_root / "scripts", scripts_dir_output_path)
    _mirror_directory_files(source_high_value_root, high_value_dir_output_path)

    return {
        "requests": requests_output_path,
        "sitemap_json": sitemap_json_output_path,
        "sitemap_xml": sitemap_xml_output_path,
        "sitemap_html": sitemap_html_output_path,
        "cookies_json": cookies_output_path,
        "scripts_json": scripts_output_path,
        "high_value_json": high_value_output_path,
        "pages_json": pages_output_path,
        "redirects_json": redirects_output_path,
        "findings_json": findings_output_path,
        "cookies_dir": cookies_dir_output_path,
        "scripts_dir": scripts_dir_output_path,
        "high_value_dir": high_value_dir_output_path,
        "pages_dir": pages_dir_output_path,
        "normalized_data_dir": normalized_data_dir,
    }
