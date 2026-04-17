# Conventions

- CLI scripts print explicit human-readable status lines and output artifact paths.
- Config defaults are defined in code and merged with JSON config files (`config/fozzy.json`), then optionally overridden by CLI flags.
- Incremental state is persisted as JSON in scan roots (example: `.fozzy_incremental_state.json`).
- For performance-sensitive recursion in large trees, prefer lower-overhead directory iteration (`os.scandir`) over heavy `Path.rglob` usage.
- For long-running batch orchestration, state persistence failures should be warning-level and non-fatal when possible, so active work is not aborted by transient file locks.
- Extractor reporting rows may include `importance_score` (int) and should preserve it end-to-end: wordlist -> extractor summary rows -> master report table.
- For report table columns that represent numeric risk/priority metrics, mark header `data-type="number"` and store raw numeric value in `data-raw` to keep client-side sort numeric.
- `nightmare.py` crawl path seed source is configurable via `crawl_wordlist` (config) or `--crawl-wordlist` (CLI); relative paths resolve from repo root and bare filenames fall back to `resources/wordlists/`.
- Path-wordlist seed URLs should be registered as discovery source `file_path_wordlist` so they appear in inventory/reporting even before crawl responses are processed.
- In `fozzy.py` incremental mode (no parameters file), `incremental_domain_workers` controls how many domains run concurrently; each child domain process is intentionally constrained to single-worker fuzzing to maintain bounded total concurrency.
- Master report HTML should always include extractor table schema with `Importance score` numeric column (even when there are zero extractor rows) to keep sorting/filter behavior and downstream parsing stable.
- Fozzy report HTML should load detailed discrepancy table rows from companion summary JSON on page load (`summary_json_filename`) instead of embedding all discrepancy row data inline in the HTML.
- `fozzy.py` and `extractor.py` should always write persistent process logs via stdout/stderr tee:
  - `fozzy.py`: mode-specific default log paths (single-domain/incremental/master) with optional `log_file` override.
  - `extractor.py`: default `<scan-root>/extractor.log` with optional `--log-file`.
- Master results payload should include discovered `log_files` so the master HTML report can provide an in-page log viewer.
- Master report sections for domain inventory, per-route inventory, and extractor matches should be rendered client-side from summary JSON data at page load, not inlined into generated HTML rows.
- Project snapshot format convention (`pack.py`/`unpack.py`): store file contents as base64 with explicit metadata in JSON, preserve deterministic path ordering, and exclude `output/` from packed snapshots.
- Transport convention for packed snapshots: wrap the full packed payload as base64 JSON text (`transport_encoding: base64-json`) and decode during unpack; maintain backward compatibility with older raw pack JSON where feasible.
- `nightmare.py` writes a per-domain crawl request inventory artifact at `output/<root-domain>/requests.json` (config/CLI override key: `requests_output` / `--requests-output`), containing requested URLs and classification flags (`found_directly`, `guessed`, `inferred`) plus an `exists` verdict derived from crawl/verification outcomes.
- Fozzy single-domain results HTML must remain usable when opened directly from disk (`file://...`): embed a single-domain payload fallback in the HTML and avoid fetch/XHR attempts under `file:` protocol to prevent browser CORS blocks.
- Fozzy interrupt convention: allow in-flight work to stop cooperatively and still write partial artifacts/summaries, then exit process with code `130` when a user `Ctrl+C` interrupt occurred (single-domain and incremental modes).
- Do not ignore `SIGINT` during Fozzy finalization; keep `Ctrl+C` responsive while writing summaries/master report and treat finalization-stage interrupts as partial-output completion.
- Nightmare page-existence detection is configuration-driven via `config/page_existence_criteria_config.json` (or `page_existence_criteria_config` in `config/nightmare.json`): controls not-found status codes plus soft-404 title/body phrase and regex heuristics with body-size thresholds.
- `extractor.py` supports optional single-domain execution via positional `domain` argument (or `domain` in `config/extractor.json`) and configurable parallel domain processing via `workers` (CLI `--workers` or config; default `4`).
- `server.py` is a stdlib-only dashboard server for live monitoring/report serving:
  - `GET /` renders an auto-refreshing HTML overview.
  - `GET /api/summary` returns JSON status built from `output/` artifacts.
  - `GET /files/<repo-relative-path>` serves generated reports/artifacts safely within repo root.
- `server.py` runtime settings are config-first via `config/server.json` (`host`, `port`, `output_root`) with CLI overrides; route `/` should serve `output/all_domains.results_summary.html` by default when present, while `/dashboard` serves the live monitoring UI.
- `server.py` should send permissive CORS headers for browser-loaded report fetches and must serve report-adjacent files (for example `all_domains.results_summary.json`) from `output_root` so relative fetch/XHR in generated HTML reports succeeds when `/` serves a report file.
- Master report payload convention: when generating `all_domains.results_summary.json`, serialize discrepancies in compact form (trimmed body previews/diff text) to keep browser-side load/parse/render practical.
- Report UI resilience convention: if master summary JSON cannot be loaded at page bootstrap, replace `Loading...` placeholders in master inventory/extractor tables with explicit error rows/notes.
- Summary artifact write convention: write report JSON/HTML via atomic temp-file replacement to avoid leaving truncated or zero-byte files after partial write failures.
- `nightmare.py` batch worker scheduling is OS-specific and config-driven:
  - Windows workers use a reduced affinity mask (default: subset of CPUs; override with `batch_worker_affinity_cores`).
  - macOS/Linux workers apply positive niceness (`batch_worker_nice`, default `10`).
  - Priority hints are passed by orchestrator env vars and applied by child worker at startup.
- Development-only performance instrumentation convention in `nightmare.py`:
  - Enable by setting `environment` to `dev/development/local/test` or `dev_timing_logging=true`.
  - Timed sections are wrapped with `dev_timed_call(...)`; summary emitted via `emit_dev_timing_summary(...)` and logged as `[dev-perf]` lines.
  - `dev_timing_log_each_call=true` optionally logs each timed call; otherwise only aggregate/slowest summaries are logged.
- `server.py` now supports dual listener operation for web-server deployment: `http_port` and `https_port` can run concurrently; HTTPS requires configured `cert_file` and `key_file`.
- Legacy `--port` remains supported as HTTP-only fallback for compatibility.
- Coordinator API convention: when `database_url` is configured, `/api/coord/*` endpoints are active and may be protected by `coordinator_api_token` (Bearer or `X-Coordinator-Token`).
- Postgres is the source of truth for centralized coordinator state (targets/leases/session checkpoints) when coordinator mode is enabled.
- Worker-fleet status convention: central server exposes `GET /api/coord/workers` (token-protected) to return per-worker heartbeat recency, active lease counts, running stage names, and aggregate online/stale counts; `stale_after_seconds` query param tunes online/stale threshold.
- Operator CLI convention: `client.py status` is the preferred central-machine quick check; defaults should resolve coordinator credentials from `deploy/.env`, print coordinator worker status first, then optionally print per-VM worker container state via AWS SSM.
- Fleet rollout convention: `client.py rollout` is the preferred central-machine way to instruct worker VMs to update code and restart worker processing via AWS SSM fanout, with configurable `--branch`, `--repo-dir`, and SSM target selector flags.
- Distributed coordinator architecture convention:
  - Central `server.py` owns Postgres-backed truth for target queue, stage queue, session checkpoints, and replicated artifacts.
  - Worker VMs must claim work via lease-based APIs and maintain heartbeats; lock ownership is `(entry_id, worker_id)` for targets and `(root_domain, stage, worker_id)` for stages.
- Stage pipeline convention:
  - Stage progression is explicit and lock-protected: `nightmare` completion enqueues `fozzy`; `fozzy` completion enqueues `extractor`.
  - Stage workers must not run without successful claim and must mark completion/failure via stage complete endpoint.
- Cross-VM resume convention:
  - Nightmare session snapshots are periodically posted to coordinator while crawl is active.
  - Workers upload/download key artifacts through coordinator artifact APIs so any VM can continue downstream stages.
- Deployment convention:
  - Single image supports roles via `APP_ROLE` (`server` or `coordinator`).
  - Central deployment uses HTTP+HTTPS listeners and Postgres (`deploy/docker-compose.central.yml`); workers use `deploy/docker-compose.worker.yml`.
  - Secrets/tokens are environment-driven (`.env`), never hardcoded in source.
- Page-existence convention: treat Cloudflare block pages as non-existent/invalid when both configured markers are present (`cloudflare_block_title_phrase` + `cloudflare_block_body_phrase`), regardless of HTTP status code class.
- `probe_url_existence()` must apply soft-404/block detection in both successful response path and `HTTPError` path to avoid counting block pages as existing.
- Central bootstrap convention: deploy/bootstrap-central-auto.sh is the quickest setup path on EC2; it should generate secrets/TLS, write deploy/.env, and emit deploy/worker.env.generated for workers.
- Deployment compose convention: pass coordinator/database values as Docker build args so rebuilt images can carry runtime defaults, while still setting them via environment at container start.
- Windows deployment convention: use deploy/bootstrap-windows.ps1 for both coordinator and worker VM bootstrap; central emits deploy/worker.env.generated for worker reuse.
- Deployment resilience convention: shell-based bootstrap scripts should auto-install required Linux packages on apt-based hosts when missing, rather than failing immediately.
- Cross-platform convention: enforce LF for .sh and deployment YAML via .gitattributes to avoid Linux runtime failures after Windows edits/checkouts.
- Linux bootstrap scripts should detect package manager in this order for AWS compatibility: yum, then dnf, then pt-get fallback.
- Compose compatibility convention: bootstrap and worker launch paths must support both docker compose and docker-compose to handle mixed AMI toolchains.
- Master report payload size convention: cap extractor-match rows included in ll_domains.results_summary.json and emit explicit extractor_matches_total/extractor_matches_truncated metadata so the HTML can render quickly and still communicate truncation.
- Module ownership convention for coordinator runtime: any class instantiated in `coordinator_app/runtime.py` (for example `CoordinatorConfig` in `load_config`) must be defined or imported in that module, not only in `coordinator.py`.
- Local deploy convention: `deploy/run-local.ps1` and `deploy/run-local.sh` must preserve existing `POSTGRES_PASSWORD` from `deploy/.env` when present, because `docker-compose.local.yml` persists Postgres state in `postgres_data` and credential rotation without volume reset breaks coordinator startup.
- Local TLS convention: when coordinator server runs with self-signed TLS in Docker (`COORDINATOR_BASE_URL=https://server:443`), worker containers must set `COORDINATOR_INSECURE_TLS=true`; production/real-cert deployments should keep this false.
- Testing convention: prefer fast unit tests that isolate module behavior with mocks/fakes (no live network, no live Postgres/AWS/SSM) and use temp dirs/SQLite for local stateful components.
- Pytest execution convention: keep `tests/conftest.py` repo-root path bootstrap so both `pytest` and `python -m pytest` work without manual `PYTHONPATH` setup.
- Public listener logging convention: in `server.py`, treat client disconnect/abort and malformed TLS-on-HTTP scanner traffic as expected operational noise; suppress traceback-level logs for known disconnect exception classes while preserving real application errors.
- Compose file convention: omit top-level `version` keys in Compose YAML (modern Compose spec) to avoid obsolete/deprecation warnings in Docker Compose v2.
- Modularity convention for coordinator server:
  - Do not define `CoordinatorStore` in `server.py`.
  - Keep database schema/query logic in `server_app/store.py`; `server.py` should import and call it.
  - When adding coordinator DB endpoints, extend `server_app/store.py` first, then wire routes in `server.py`.
- Dashboard/worker page convention:
  - Keep dashboard and worker-control HTML in `templates/*.j2` rendered through `reporting/server_pages.py`; avoid reintroducing large inline HTML render methods in `server.py`.
  - Preserve cross-page operational navigation links (`/dashboard`, `/workers`, `/database`) in template headers/meta bars during UI refactors.
- AWS distributed bootstrap convention:
  - When central TLS is generated via `bootstrap-central-auto.sh` (self-signed), worker env must include `COORDINATOR_INSECURE_TLS=true` unless trusted CA certs are installed.
  - Ensure generated `worker.env.generated` and cloud-init worker `.env` both carry `COORDINATOR_BASE_URL`, `COORDINATOR_API_TOKEN`, and `COORDINATOR_INSECURE_TLS`.
- Server page rendering convention: all operator UI pages (`/dashboard`, `/workers`, `/database`) should render from `templates/*.j2` through `reporting/server_pages.py`; avoid inline HTML page methods in `server.py`.
- Dashboard/worker client URL encoding convention: use `encodeURIComponent(...).replace(/%2F/g, "/")` instead of `replaceAll` for broader JS runtime compatibility.
- Dashboard refresh UX convention: never silently swallow summary-fetch failures; show explicit on-page load failure messaging so operators can distinguish empty data from failed API calls.

- Coordinator DB status endpoint convention: /api/coord/database-status must cap table row payloads (current cap: 20 rows/table), while still reporting true ow_count and explicit truncation metadata (ows_returned, ows_limited, max_rows_per_table).

- Coordinator UI auth UX convention: pages that call /api/coord/* from browser templates should share token persistence via 
ightmare_coord_token cookie (30-day TTL, SameSite=Strict, Path=/, Secure on HTTPS) and auto-hydrate the token input on page load.

- Database-status endpoint convention: avoid full-table/full-cell payloads in operator APIs. Use small preview-select projections, never inline full ytea values, cap row count per table (20), and prefer catalog-estimated row counts for responsiveness on live coordinator databases.

- Refactor convention for large runtime scripts (
ightmare.py, ozzy.py): extract pure/policy logic into dedicated modules (
ightmare_app/*, ozzy_app/*, 
ightmare_shared/*) and keep thin compatibility wrappers in entrypoint scripts to avoid broad call-site churn.
- Reuse convention: value-type inference for URL/form/query analysis should be shared via 
ightmare_shared/value_types.py rather than duplicated in multiple executables.
- Coordinator API auth convention: `_is_coordinator_authorized()` should accept coordinator token from all browser-safe channels used by operator pages:
  - `Authorization: Bearer <token>`
  - `X-Coordinator-Token: <token>`
  - `Cookie: nightmare_coord_token=<token>` (URL-decoded)
- Reason: database/worker pages persist token in cookie and some deployments can lose/strip auth headers; cookie fallback keeps operator APIs usable.

- Crawl export convention:
  - Keep legacy crawl artifacts (`<domain>_sitemap.json`, inventory/source-of-truth files) and also emit normalized replay/export artifacts via `nightmare_app/normalized_exports.py`.
  - Required normalized files (domain output root): `sitemap.json`, `sitemap.xml`, `sitemap.html`, `requests.json`, `cookies.json`, `scripts.json`, `high-value.json`, `pages.json`, `redirects.json`, `findings.json`.
  - Required normalized directories: `cookies/`, `scripts/`, `high-value/`, `pages/`, and `normalized_data/raw_requests|raw_responses`.
  - `high_value` capture source remains under `<output_root>/high_value/<root_domain>`; normalized exports must pass that source root explicitly instead of assuming it lives under the domain folder.

- Coordinator database-status resilience convention:
  - `/api/coord/database-status` must never hard-fail the HTTP connection on query/introspection exceptions.
  - Server route should catch `database_status()` exceptions and return structured JSON error (`500`) so UI receives a normal response body.
  - Store-level table introspection should continue on per-table failure and annotate `table_error` on affected table rows.
