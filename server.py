#!/usr/bin/env python3
"""Small web server for Nightmare/Fozzy reporting and live run overview.

Usage:
    python server.py
    python server.py --host 127.0.0.1 --port 8080 --output-root output
    python server.py --config server.json

The dashboard provides:
- aggregate counts (domains discovered, completed/running/pending/failed),
- per-domain status rows with links to generated HTML reports and JSON artifacts,
- master-report links when available,
- recent log tail snippets.
"""

from __future__ import annotations

import argparse
import html
import json
import mimetypes
import os
import re
import sys
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = BASE_DIR / "output"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "server.json"
MAX_LOG_TAIL_BYTES = 64 * 1024


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
    except Exception:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _resolve_config_path(raw_path: str | None) -> Path:
    value = str(raw_path or "").strip() or "server.json"
    p = Path(value).expanduser()
    if p.is_absolute():
        return p.resolve()
    if p.parts and p.parts[0].lower() == "config":
        return (BASE_DIR / p).resolve()
    return (BASE_DIR / "config" / p).resolve()


def _default_server_config() -> dict[str, Any]:
    return {
        "host": DEFAULT_HOST,
        "port": DEFAULT_PORT,
        "output_root": "output",
    }


def _merged_value(cli_value: Any, cfg: dict[str, Any], key: str, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if key in cfg:
        return cfg[key]
    return default


def _find_all_domains_report_html(output_root: Path) -> Path | None:
    candidate = output_root / "all_domains.results_summary.html"
    if candidate.is_file():
        return candidate
    return None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _collect_master_reports(output_root: Path) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    candidates = [
        output_root / "all_domains.results_summary.html",
        output_root / "all_domains.results_summary.json",
    ]
    for path in candidates:
        if path.is_file():
            reports.append(
                {
                    "name": path.name,
                    "path": str(path.resolve()),
                    "relative": path.resolve().relative_to(BASE_DIR).as_posix()
                    if str(path.resolve()).startswith(str(BASE_DIR.resolve()))
                    else path.resolve().as_posix(),
                }
            )
    return reports


def _to_repo_relative_path(path: Path) -> str | None:
    try:
        return path.resolve().relative_to(BASE_DIR.resolve()).as_posix()
    except ValueError:
        return None


def _discover_fozzy_summary_for_domain(domain_dir: Path, domain: str) -> Path | None:
    direct = domain_dir / "fozzy-output" / domain / f"{domain}.fozzy.summary.json"
    if direct.is_file():
        return direct
    for candidate in domain_dir.glob(f"**/{domain}.fozzy.summary.json"):
        if candidate.is_file():
            return candidate
    return None


def _discover_fozzy_results_html_for_domain(domain_dir: Path, domain: str) -> Path | None:
    direct = domain_dir / "fozzy-output" / domain / "results" / f"{domain}.results_summary.html"
    if direct.is_file():
        return direct
    for candidate in domain_dir.glob("**/*.results_summary.html"):
        if candidate.is_file() and candidate.name.lower() == f"{domain}.results_summary.html":
            return candidate
    return None


def _discover_nightmare_report_html(domain_dir: Path) -> Path | None:
    report = domain_dir / "report.html"
    return report if report.is_file() else None


def _discover_domain_log_files(domain_dir: Path, domain: str) -> list[Path]:
    out: list[Path] = []
    patterns = [
        f"{domain}_nightmare.log",
        f"{domain}_scrapy.log",
        "*.fozzy.log",
        "*.log",
    ]
    seen: set[str] = set()
    for pattern in patterns:
        for path in domain_dir.glob(pattern):
            if not path.is_file():
                continue
            key = str(path.resolve()).lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(path.resolve())
    for path in domain_dir.glob("**/*.log"):
        if not path.is_file():
            continue
        key = str(path.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(path.resolve())
    return sorted(out, key=lambda p: p.name.lower())


def _read_tail_text(path: Path, max_bytes: int = MAX_LOG_TAIL_BYTES) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes), os.SEEK_SET)
            data = handle.read()
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def collect_dashboard_data(output_root: Path) -> dict[str, Any]:
    output_root = output_root.resolve()
    domains: list[dict[str, Any]] = []

    for domain_dir in sorted(output_root.iterdir(), key=lambda p: p.name.lower()) if output_root.is_dir() else []:
        if not domain_dir.is_dir() or domain_dir.name.startswith("."):
            continue
        domain = domain_dir.name.strip().lower()
        if not domain:
            continue

        inv_path = domain_dir / f"{domain}_url_inventory.json"
        req_path = domain_dir / "requests.json"
        params_path = domain_dir / f"{domain}.parameters.json"
        nightmare_report = _discover_nightmare_report_html(domain_dir)
        fozzy_summary = _discover_fozzy_summary_for_domain(domain_dir, domain)
        fozzy_results_html = _discover_fozzy_results_html_for_domain(domain_dir, domain)
        log_files = _discover_domain_log_files(domain_dir, domain)

        inv = _read_json_dict(inv_path) if inv_path.is_file() else {}
        req = _read_json_dict(req_path) if req_path.is_file() else {}
        foz = _read_json_dict(fozzy_summary) if fozzy_summary and fozzy_summary.is_file() else {}

        unique_urls = _safe_int(inv.get("total_urls_effective", inv.get("total_urls", 0)))
        requested_urls = _safe_int(req.get("total_requested_urls", 0))
        interrupted = bool(foz.get("interrupted", False))
        anomaly_count = _safe_int((foz.get("totals") or {}).get("anomalies", 0)) if isinstance(foz.get("totals"), dict) else 0
        reflection_count = _safe_int((foz.get("totals") or {}).get("reflections", 0)) if isinstance(foz.get("totals"), dict) else 0

        status = "discovered"
        if params_path.is_file():
            status = "parameterized"
        if fozzy_summary and fozzy_summary.is_file():
            status = "fozzy_complete" if not interrupted else "fozzy_interrupted"

        domains.append(
            {
                "domain": domain,
                "status": status,
                "unique_urls": unique_urls,
                "requested_urls": requested_urls,
                "anomalies": anomaly_count,
                "reflections": reflection_count,
                "paths": {
                    "domain_dir": str(domain_dir.resolve()),
                    "url_inventory": str(inv_path.resolve()) if inv_path.is_file() else None,
                    "requests": str(req_path.resolve()) if req_path.is_file() else None,
                    "parameters": str(params_path.resolve()) if params_path.is_file() else None,
                    "nightmare_report_html": str(nightmare_report.resolve()) if nightmare_report else None,
                    "fozzy_summary_json": str(fozzy_summary.resolve()) if fozzy_summary else None,
                    "fozzy_results_html": str(fozzy_results_html.resolve()) if fozzy_results_html else None,
                },
                "paths_rel": {
                    "domain_dir": _to_repo_relative_path(domain_dir),
                    "url_inventory": _to_repo_relative_path(inv_path) if inv_path.is_file() else None,
                    "requests": _to_repo_relative_path(req_path) if req_path.is_file() else None,
                    "parameters": _to_repo_relative_path(params_path) if params_path.is_file() else None,
                    "nightmare_report_html": _to_repo_relative_path(nightmare_report) if nightmare_report else None,
                    "fozzy_summary_json": _to_repo_relative_path(fozzy_summary) if fozzy_summary else None,
                    "fozzy_results_html": _to_repo_relative_path(fozzy_results_html) if fozzy_results_html else None,
                },
                "logs": [str(p) for p in log_files],
                "latest_log_tail": _read_tail_text(log_files[-1]) if log_files else "",
            }
        )

    batch_state = _read_json_dict(output_root / "batch_state" / "batch_run_state.json")
    batch_counts: dict[str, int] = {}
    entries = batch_state.get("entries", [])
    if isinstance(entries, list):
        for item in entries:
            if not isinstance(item, dict):
                continue
            st = str(item.get("status", "unknown"))
            batch_counts[st] = batch_counts.get(st, 0) + 1

    totals = {
        "domains": len(domains),
        "fozzy_complete": sum(1 for d in domains if d["status"] == "fozzy_complete"),
        "fozzy_interrupted": sum(1 for d in domains if d["status"] == "fozzy_interrupted"),
        "parameterized": sum(1 for d in domains if d["status"] == "parameterized"),
        "discovered": sum(1 for d in domains if d["status"] == "discovered"),
        "batch_counts": batch_counts,
    }

    return {
        "generated_at_utc": _iso_now(),
        "output_root": str(output_root),
        "totals": totals,
        "master_reports": _collect_master_reports(output_root),
        "domains": domains,
    }


def _normalize_and_validate_relative_path(root: Path, raw_relative: str) -> Path | None:
    cleaned = str(raw_relative or "").strip().replace("\\", "/")
    cleaned = cleaned.lstrip("/")
    candidate = (root / cleaned).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        return None
    return candidate


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "NightmareServer/1.0"

    @property
    def app_root(self) -> Path:
        return self.server.app_root  # type: ignore[attr-defined]

    @property
    def output_root(self) -> Path:
        return self.server.output_root  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        # Keep server output concise.
        sys.stdout.write("[server] " + format % args + "\n")

    def _write_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_text(self, text: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> None:
        body = text.encode("utf-8", errors="replace")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static_file(self, file_path: Path) -> None:
        if not file_path.is_file():
            self._write_text("Not found", status=404)
            return
        content_type, _ = mimetypes.guess_type(str(file_path))
        if not content_type:
            content_type = "application/octet-stream"
        try:
            data = file_path.read_bytes()
        except OSError:
            self._write_text("Read error", status=500)
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            all_domains_html = _find_all_domains_report_html(self.output_root)
            if all_domains_html is not None:
                self._serve_static_file(all_domains_html)
            else:
                self._write_text(self._render_dashboard_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/dashboard":
            self._write_text(self._render_dashboard_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/api/summary":
            payload = collect_dashboard_data(self.output_root)
            self._write_json(payload)
            return
        if path == "/api/log-tail":
            rel = (query.get("path") or [""])[0]
            resolved = _normalize_and_validate_relative_path(self.app_root, unquote(rel))
            if resolved is None or not resolved.is_file():
                self._write_json({"error": "invalid log path"}, status=400)
                return
            self._write_json({"path": str(resolved), "tail": _read_tail_text(resolved)})
            return
        if path.startswith("/files/"):
            rel = unquote(path[len("/files/"):])
            resolved = _normalize_and_validate_relative_path(self.app_root, rel)
            if resolved is None:
                self._write_text("Invalid path", status=400)
                return
            self._serve_static_file(resolved)
            return
        # Allow report-adjacent relative fetches from '/' served report pages, e.g.
        # /all_domains.results_summary.json loaded by all_domains.results_summary.html.
        if path not in {"/favicon.ico"} and not path.startswith("/api/"):
            rel = unquote(path.lstrip("/"))
            resolved = _normalize_and_validate_relative_path(self.output_root, rel)
            if resolved is not None and resolved.is_file():
                self._serve_static_file(resolved)
                return
        self._write_text("Not found", status=404)

    def _render_dashboard_html(self) -> str:
        return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Nightmare Dashboard</title>
  <style>
    body { font-family: Segoe UI, Arial, sans-serif; margin: 16px; background: #f7fafc; color: #0f172a; }
    h1 { margin: 0 0 10px; }
    .meta { color: #475569; margin-bottom: 14px; font-size: 13px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 16px; }
    .card { background: #fff; border: 1px solid #cbd5e1; border-radius: 8px; padding: 10px; }
    .k { font-size: 12px; color: #475569; }
    .v { font-size: 22px; font-weight: 700; margin-top: 4px; }
    table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #cbd5e1; }
    th, td { border: 1px solid #cbd5e1; padding: 6px 8px; font-size: 12px; vertical-align: top; text-align: left; }
    th { background: #e2e8f0; }
    .log { white-space: pre-wrap; word-break: break-word; max-height: 220px; overflow: auto; margin: 0; background: #0b1220; color: #c8d3f5; padding: 8px; border-radius: 6px; }
    .rowlinks a { margin-right: 8px; }
    .muted { color: #64748b; }
    code { background: #e2e8f0; padding: 1px 4px; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>Nightmare Live Dashboard</h1>
  <div class="meta">Auto-refresh every 5s. Reports are served from this app via <code>/files/*</code>.</div>
  <div id="cards" class="cards"></div>
  <h2>Master Reports</h2>
  <div id="masters" class="muted">Loading…</div>
  <h2>Domains</h2>
  <table>
    <thead>
      <tr>
        <th>Domain</th>
        <th>Status</th>
        <th>URLs</th>
        <th>Findings</th>
        <th>Reports</th>
        <th>Latest Log Tail</th>
      </tr>
    </thead>
    <tbody id="rows"><tr><td colspan="6">Loading…</td></tr></tbody>
  </table>
  <script>
    function esc(v) {
      return String(v || "").replace(/[&<>\"']/g, (ch) => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[ch]));
    }
    function fileLink(relPath, label) {
      if (!relPath) return "";
      const encoded = encodeURIComponent(relPath).replaceAll("%2F", "/");
      return `<a href="/files/${encoded}" target="_blank" rel="noopener noreferrer">${esc(label)}</a>`;
    }
    function buildCards(t) {
      const cards = [
        ["Domains", t.domains],
        ["Fozzy Complete", t.fozzy_complete],
        ["Fozzy Interrupted", t.fozzy_interrupted],
        ["Parameterized", t.parameterized],
        ["Discovered Only", t.discovered],
      ];
      return cards.map(([k, v]) => `<div class="card"><div class="k">${esc(k)}</div><div class="v">${esc(v)}</div></div>`).join("");
    }
    function render(data) {
      document.getElementById("cards").innerHTML = buildCards(data.totals || {});
      const masters = (data.master_reports || []).map((m) => fileLink(m.relative, m.name)).join(" | ");
      document.getElementById("masters").innerHTML = masters || "<span class='muted'>No master reports found.</span>";

      const rows = data.domains || [];
      if (!rows.length) {
        document.getElementById("rows").innerHTML = "<tr><td colspan='6'>No domain output folders found.</td></tr>";
        return;
      }
      document.getElementById("rows").innerHTML = rows.map((d) => {
        const p = d.paths_rel || {};
        const links = [
          fileLink(p.nightmare_report_html, "Nightmare HTML"),
          fileLink(p.fozzy_results_html, "Fozzy HTML"),
          fileLink(p.url_inventory, "URL Inventory"),
          fileLink(p.requests, "Requests JSON"),
          fileLink(p.fozzy_summary_json, "Fozzy Summary")
        ].filter(Boolean).join(" ");
        return `<tr>
          <td>${esc(d.domain)}</td>
          <td>${esc(d.status)}</td>
          <td>unique=${esc(d.unique_urls)}<br>requested=${esc(d.requested_urls)}</td>
          <td>anomalies=${esc(d.anomalies)}<br>reflections=${esc(d.reflections)}</td>
          <td class="rowlinks">${links || "<span class='muted'>none</span>"}</td>
          <td><pre class="log">${esc((d.latest_log_tail || "").slice(-4000))}</pre></td>
        </tr>`;
      }).join("");
    }
    async function refresh() {
      try {
        const rsp = await fetch("/api/summary", { cache: "no-store" });
        if (!rsp.ok) return;
        const data = await rsp.json();
        render(data);
      } catch (_) {}
    }
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Nightmare/Fozzy report web dashboard server.")
    p.add_argument(
        "--config",
        default="server.json",
        help="Server config path (default: config/server.json). Relative paths resolve under config/.",
    )
    p.add_argument("--host", default=None, help=f"Bind host (default: {DEFAULT_HOST})")
    p.add_argument("--port", type=int, default=None, help=f"Bind port (default: {DEFAULT_PORT})")
    p.add_argument(
        "--output-root",
        default=None,
        help=f"Output root to summarize (default: {DEFAULT_OUTPUT_ROOT})",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config_path = _resolve_config_path(args.config)
    file_cfg = _read_json_dict(config_path)
    merged_cfg = {**_default_server_config(), **file_cfg}

    output_root_raw = str(_merged_value(args.output_root, merged_cfg, "output_root", "output"))
    output_root = Path(output_root_raw).expanduser()
    if not output_root.is_absolute():
        output_root = (BASE_DIR / output_root).resolve()
    host = str(_merged_value(args.host, merged_cfg, "host", DEFAULT_HOST) or DEFAULT_HOST).strip() or DEFAULT_HOST
    port = int(_merged_value(args.port, merged_cfg, "port", DEFAULT_PORT) or DEFAULT_PORT)
    if port < 1 or port > 65535:
        raise ValueError("port must be in range 1..65535")

    httpd = ThreadingHTTPServer((host, port), DashboardHandler)
    httpd.app_root = BASE_DIR  # type: ignore[attr-defined]
    httpd.output_root = output_root  # type: ignore[attr-defined]

    print(f"[server] starting dashboard on http://{host}:{port}", flush=True)
    print(f"[server] config={config_path}", flush=True)
    print(f"[server] app_root={BASE_DIR}", flush=True)
    print(f"[server] output_root={output_root}", flush=True)
    all_domains_html = _find_all_domains_report_html(output_root)
    if all_domains_html is not None:
        print(f"[server] default route / serving all-domains report: {all_domains_html}", flush=True)
    else:
        print("[server] default route / serving dashboard (all_domains.results_summary.html not found)", flush=True)
    print(f"[server] dashboard route: http://{host}:{port}/dashboard", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[server] interrupt received, shutting down.", flush=True)
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
