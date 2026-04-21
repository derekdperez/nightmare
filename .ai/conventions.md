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

- Coordinator worker visibility convention:
  - Worker UI discovery should include idle workers via presence heartbeats, not only workers with running/completed target/stage rows.
  - `server_app/store.py` should keep `coordinator_worker_presence` updated during claim/heartbeat/complete code paths.
  - `/api/coord/workers` and `/api/coord/worker-control` routes should catch store exceptions and return JSON error responses instead of breaking the fetch channel.
- Crawl progress observability convention:
  - Per-domain crawl monitoring should be served from coordinator DB via `/api/coord/crawl-progress` and `CoordinatorStore.crawl_progress_snapshot(...)`.
  - Response should include phase + discovered/visited/frontier counts + active workers/stages and summary totals.
  - CLI operator workflow uses `client.py progress` (or `client.py status`) with shared coordinator auth flags.
- Operator UI convention: new operational pages should be template-backed under `templates/*.j2`, exposed via `reporting/server_pages.py`, and routed in `server.py` (no inline HTML in route handlers).
- Crawl monitoring UX convention: use `/api/coord/crawl-progress` as source-of-truth and render per-domain `discovered_urls_count`/`visited_urls_count`/`frontier_count` with lightweight auto-refresh.
- Cross-page ops navigation should keep `/dashboard`, `/workers`, `/database`, and `/crawl-progress` linked in page headers/meta bars.
- Database status convention:
  - Avoid approximate row counts; use exact `COUNT(*)` per table.
  - Preview responses must be bounded to 20 rows/table and should attempt a deterministic recent-first sort (updated/completed/created/id-style columns) when available.
  - Do not embed `%s` tokens in SQL string literals passed through psycopg execute; they are interpreted as placeholders.
- Worker control convention:
  - Worker discovery APIs should be recency-scoped to avoid long-tail stale worker accumulation from historical task rows.
  - Worker status should communicate operational state (`running|paused|stopped|errored|idle`) rather than only online/stale.
  - Worker start/pause/stop commands should be consumed through explicit claim/complete command endpoints and should supersede stale queued commands per worker.
- Dashboard convention:
  - On central coordinator deployments, dashboard domain lists should merge coordinator DB progress data instead of relying only on local output folder discovery.

- Extractor matches UI/API convention:
  - Page rendering remains template-based (`templates/*.j2` via `reporting/server_pages.py`), with route wiring in `server.py`.
  - Domain lists for extractor artifacts should come from store metadata queries first (summary counts), with expensive zip scans only as bounded fallback.
  - Large zip artifacts should be parsed on demand and cached by artifact content hash to prevent repeated unzip/JSON parse work.
  - File download/view endpoints should read directly from DB artifact zip content and avoid writing temporary files to disk.
  - Aggregated filter insights should be computed server-side and returned by API payload (`top_filters`) so UI can render consistent rankings independent of client-side row sorting.
  - For high-row extractor datasets, use server-side paging/filter/sort (`limit`, `offset`, `sort_key`, `sort_dir`, `f_<column>`) with small defaults (250/page) and bounded max page size to keep API responses fast.
  - Extractor UI tables should use fixed-height scrollable containers so the page layout stays stable as row counts grow.
  - Domain dropdown metadata should include both `match_count` and `max_importance_score`; domain ordering should be score-first, then count.
  - UI should provide a quick toggle to hide zero-result domains (`hideZeroDomains`) without requiring a full page reload.

- Fuzzing findings UI/API convention:
  - Fozzy findings should be served from coordinator artifacts (`fozzy_summary_json`) with server-side row flattening for anomaly/reflection entries.
  - Use server-side paging/filter/sort for findings endpoints (`limit`, `offset`, `sort_key`, `sort_dir`, `f_<column>`, global `q`) with default 250 rows/page.
  - File-level detail browsing should read from `fozzy_results_zip` artifact bytes on demand and use in-memory zip index caching for repeated listing/lookups.
  - Keep findings/file tables in fixed-height scroll containers and provide both global and column-specific search/filter controls.

- Fuzzing UI data contract: /api/coord/fuzzing rows should include baseline/fuzz request content, response content, response code, response size, response duration, and diff fields (status_difference, size_difference, duration_difference_ms) so the page can compare pairs without additional client joins.
- Reflection-note convention: when esult_type=reflection, include reflected value text in nomaly_note (format eflection_detected: <value>).


- Web UI preference convention: persist page-specific table settings (hidden columns, column widths) in DB using coordinator API /api/coord/ui-preferences keyed by (page, key) instead of localStorage-only state.

- UI table convention: any scrollable table should use a dedicated .table-wrap container (overflow: auto) and sticky header cells (	h { position: sticky; top: 0; z-index: ... }) so headers remain visible while scrolling.

- Reflection signal convention (Fozzy): reflection detection should ignore configurable low-signal values via eflection_alert_ignore_exact; defaults should include common ambient tokens (0/1, true/false, null-like values) to reduce false positives.

- Fuzzing UI convention: action-oriented grid with per-row View Response and View Diff buttons. Response modal should show headers and body in separate stacked text panes; diff modal should be side-by-side with synchronized scrolling.
- Fuzzing zip-file convention: result_file values may be basename-only while zip entries are nested. File-serving endpoints should support exact-path lookup plus safe unique basename/suffix fallback before returning 404.

- Fozzy response-analysis convention: perform deterministic baseline-vs-fuzz analysis for every live fuzz mutation and emit structured JSONL records (<root_domain>.fozzy.response_analysis.jsonl) with stable fields (request_id, baseline_id, cluster_id, summary, score, findings, header/body diffs, reflection, exceptions, tags).
- Detector convention: keep detectors modular under fozzy_app/response_analysis/detectors and return normalized Finding objects; pipeline orchestrates detector execution, scoring, noise suppression, and clustering (detectors should not directly write files).
- Artifact compatibility convention: anomaly/reflection JSON artifacts should continue to carry baseline/anomaly payloads for legacy consumers, but include enriched response_analysis blocks for new UI/query features.
- Worker Control UI convention: preserve selected worker IDs across refresh cycles, and keep grid operations client-side with global search, per-column filtering, and sortable columns so bulk commands remain usable during frequent auto-refresh.
- Fuzzing response-view convention: header display logic must tolerate mixed/legacy payload shapes (`response_headers`, `headers`, object/list forms) before falling back to names-only rendering.
- Web grid UX convention: use shared template include `templates/_grid_controls.html.j2` for consistent table behavior (column resize, handle double-click auto-fit, per-table column visibility modal, and persisted column prefs).
- For dynamic table pages (rows re-rendered by JS), call returned grid controller `.refresh()` after each render so hidden/width/sort/filter state reapplies to new rows.
- Fuzzing resize convention: resize handles must suppress click bubbling to prevent header-sort race conditions while dragging.
- Operator observability convention:
  - Keep new ops pages template-backed (`templates/*.j2` + `reporting/server_pages.py`) and route them from `server.py`.
  - Protect ops JSON endpoints under `/api/coord/*` with coordinator auth and cookie/Bearer token support.
  - For log viewing, expose source enumeration first (`/api/coord/log-sources`) and fetch tails by stable source ID (`/api/coord/log-tail`) rather than accepting arbitrary file paths from the browser.
- Docker status convention:
  - Prefer `docker-compose` command family in backend status probes; include compose-file scoped status plus raw `docker ps` container inventory.
- Fleet observability convention: Docker/log status APIs should aggregate data from both the central server and worker VMs (via SSM) when AWS credentials and target selectors are configured.
- Remote log source ID convention: `ssm:<instance_id>:docker:<container_name>` is used by `/api/coord/log-sources` and `/api/coord/log-tail`.
- Docker status API convention: support optional expensive log collection (`include_logs`) and bounded tail size (`log_lines`) to keep UI refreshes responsive.
- Fleet command compatibility convention: runtime tooling in server APIs must support both `docker compose` and `docker-compose` because host/AMI environments vary.
- Log viewer API convention:
  - `/api/coord/log-events` is the structured query endpoint for UI search/filter/sort.
  - `/api/coord/log-download` returns a zipped source export.
  - `/api/coord/log-tail` remains for raw quick-tail reads.
- Source ID conventions:
  - Local docker: `docker:<container_name>`
  - Worker VM docker over SSM: `ssm:<instance_id>:docker:<container_name>`
  - EC2 console: `ec2-console:<instance_id>`
- Time normalization convention for operator logs: UI/event payloads emit `event_time_est` using fixed EST (UTC-5) for consistent operator triage.
- Optional dedicated log DB convention:
  - Configure `log_database_url` / `LOG_DATABASE_URL` for structured event persistence in separate Postgres.
  - Keep coordinator DB (`database_url`) and log DB (`log_database_url`) distinct.
- View Logs convention: use explicit client-side request states and timeout-aware fetch wrappers so long backend calls show deterministic UI status (`loading`/`success`/`error`/`timeout`) instead of appearing stalled.
- Grid preference convention update: persist `column_order` alongside `hidden_columns` and `column_widths` in `coordinator_ui_preferences` for grids using `_grid_controls.html.j2` in `columnsOnly` mode.
- Log source discovery convention: avoid unbounded recursive file scans on source listing APIs; use bounded-depth targeted roots and short-lived server-side caching, with optional force-refresh when operators explicitly request a fresh probe.
- Deployment convention: coordinator server startup is now hard-gated on both primary `database_url` and dedicated `log_database_url`; logging DB is no longer optional.
- Database separation convention: `log_database_url` must not resolve to the same DB endpoint/name/user tuple as `database_url`.
- Compose convention: central compose requires `LOG_DATABASE_URL` env var explicitly (fail-fast if missing).
- AWS bootstrap convention: `bootstrap-central-auto.sh` must ensure `LOG_DATABASE_URL` exists before central compose startup. If absent, auto-provision a dedicated log DB VM and write the URL to `deploy/.env`.
- Dedicated log DB provisioning convention: use `deploy/provision-log-db-aws.sh` for EC2 VM creation + cloud-init Postgres bootstrap + `.env` update + central server rebuild.
- Deployment safety convention: central server startup should fail fast when mandatory log DB settings are missing; provisioning scripts should satisfy these requirements automatically.
- Compose operation convention: `docker compose up ... <service>` must use service keys from compose YAML (`server`, `postgres`), not container_name values.
- Worker control convention: Logs column should include direct file links and a one-click per-worker log bundle download action.
- Worker log download API convention: use `/api/coord/worker-log-download?worker_id=...` with coordinator auth and zip response payload.
- Full deploy convention: scripts should include coordinator readiness wait and automatic `register_targets.py` execution against `targets.txt` using values from `deploy/.env`.
- Log source discovery convention: do not restrict remote docker source discovery only to worker compose services; include all EC2 fleet instances matched by log filters and query `docker ps` over SSM.
- View Logs resilience convention: when log DB query returns zero events, fallback to live source reads before returning an empty response.
- Fleet log filter convention: default EC2 log filter values should include coordinator, workers, and dedicated log-db VM name patterns.
- View Logs observability convention: expose per-source diagnostics (health/error/probe preview) via dedicated API and UI panel.
- Log retrieval convention: structured log DB is preferred, but when query total=0, backend must fallback to live source reads to avoid false-empty dashboards.
- Fleet coverage convention: include central coordinator, central DB, and dedicated log DB source entries regardless of dynamic discovery success.
- Deploy script resilience convention: shell bootstrap/wrapper scripts must self-heal executable bits (`chmod +x`) for `deploy/*.sh` before invoking subordinate scripts, because some host checkout/copy paths drop mode bits and cause false `Missing executable` failures.
- Deploy privilege convention: bootstrap scripts should run Python/pip dependency installs as the invoking non-root user (via `$SUDO_USER` when launched with sudo), while elevating only docker/system package-manager operations.
- Deploy dependency convention: central bootstrap must auto-install `aws` CLI (`awscli` package) because auto-provisioning workers/log-db relies on EC2/STS commands.
- Full deploy wrapper convention: any post-bootstrap Python steps (for example `register_targets.py`) must execute as the invoking non-root user when script is run with sudo; otherwise user-site dependencies become invisible to root Python.
- AWS full-deploy convention: treat deploy as reconcile, not always-provision.
  - Central compose should always rebuild/redeploy latest code while preserving existing Postgres volumes by default.
  - Worker fleet handling should be conditional: provision only when no worker VMs exist; otherwise roll existing workers forward via SSM rollout + docker rebuild.
  - Root `full_deploy_command.sh` should delegate to `deploy/full_deploy_command.sh` to avoid duplicate logic drift.
- Bootstrap DB-credential convention: when existing Postgres data/container is detected and `POSTGRES_PASSWORD` is absent from `.env`, bootstrap should attempt best-effort recovery from existing container env before refusing to continue.
- Docker command convention in deploy scripts: use centralized docker access wrapper (sudo-aware) for container/volume introspection so reruns work in both docker-group and sudo-only environments.
- File ownership convention for sudo-launched deploy scripts: any generated/updated repo env files intended for non-root Python tools (`deploy/.env`, `worker.env.generated`) must be chowned back to `$SUDO_USER` after write.
- Env reader convention: `.env` loaders should fail-soft (return empty map) on permission/read errors rather than crashing CLI flows.
- Sudo deploy ownership convention: any deploy-generated files that later need non-root access must be re-owned to invoking user (`$SUDO_USER`) with their primary group after write.
- User-shell integration convention: when scripts update shell startup files, target invoking user's home (`getent passwd $SUDO_USER`) rather than inherited root HOME from sudo context.
- Log DB provisioning convention: before launching a new log DB VM, scripts must attempt to recover `LOG_DATABASE_URL` from existing coordinator container config and must check for existing tagged log-DB instances to avoid duplicate provisioning.
- Rerunnable deploy convention: missing local `.env` values should be reconstructed from live container configuration when possible, rather than interpreted as a request to create new infrastructure.
- Worker rollout credential-sync convention: SSM rollout must rewrite worker `deploy/.env` with the current coordinator URL/token/TLS settings before restarting worker containers to prevent token drift and 401 claim/poll failures.
- Full deploy wrapper convention: pass coordinator URL/token explicitly to `client.py rollout` instead of relying only on local default env discovery.
- Log reader resilience convention: local container log collection should not depend solely on `docker logs`; always include fallback to compose service logs (`docker compose` and `docker-compose`) for known coordinator/DB services.
- Compose spec convention for observability: include `docker-compose.log-store.yml` in compose-spec discovery used by Docker status/log APIs.
- UI time-column convention: render table time fields as fixed EST (`UTC-5`) `HH:MM:SS` with no fractional seconds; prefer dedicated `..._est_time` values from API payloads or deterministic client conversion from UTC timestamps.
- Log-source discovery convention: never add local docker log sources or synthetic compose probes unless local `docker`/`docker-compose` commands are actually available in the runtime.
- EC2 provisioning convention: deploy scripts default to `m7i-flex.large` for worker and dedicated log-DB VM provisioning unless explicitly overridden.
- EC2 storage convention: provisioned worker VMs must launch with at least 50 GB root EBS (`gp3`), and dedicated log DB VMs must launch with at least 100 GB root EBS (`gp3`) using explicit `--block-device-mappings` in provisioning scripts.
- Full deploy readiness convention: after waiting for `/api/coord/database-status`, scripts must fail fast if coordinator API never reaches HTTP 200 and must not continue into `register_targets.py`.
- On readiness failure in full deploy, emit best-effort compose diagnostics (`ps` and recent `server`/`postgres` logs) to surface root-cause quickly.
- Bootstrap package-manager convention: `deploy/bootstrap-central-auto.sh` should detect distro via `/etc/os-release` and prefer `apt-get` on Ubuntu/Debian even if yum/dnf binaries exist.
- Apt compatibility convention: dependency install should gracefully fallback when `docker-compose-plugin` package is unavailable by installing core docker/aws packages and then relying on standalone compose install path.
- Debug default fleet size convention: deploy wrappers should default to 2 worker VMs during active debugging.
- Script permission convention: tracked shell scripts (`*.sh`) must be committed with executable mode (`100755`) so Linux hosts can run them immediately after pull without manual chmod.
- Ubuntu bootstrap dependency convention: when apt repositories do not provide `awscli` or `docker-compose-plugin`, bootstrap should still proceed by installing core docker/curl/openssl/git packages, then installing AWS CLI v2 via the official installer and Compose via existing standalone fallback.
- Ubuntu Python packaging convention: bootstrap pip installs should include `--break-system-packages` when pip supports it, so PEP 668 externally-managed environments do not block user-scoped dependency installation.
- Web page data-loading convention: coordinator UI pages should be cache-first on initial load (bounded `localStorage` TTL), then refresh from live `/api/...` calls and overwrite cache on success.
- Cache resilience convention: if a live refresh fails after cached render, keep cached data visible and show explicit refresh-failed status instead of reverting to empty/error-only content.
- Query-heavy table convention: cache keys for extractor/fuzzing style pages should include active domain/filter/sort/paging parameters to prevent rendering mismatched cached result sets.
- CLI deploy-defaults convention: helper CLIs used in deploy flows should auto-load `deploy/.env` with `override=False` and use `COORDINATOR_BASE_URL` / `COORDINATOR_API_TOKEN` as defaults, while still allowing explicit CLI overrides.
- Worker observability convention: coordinator worker loops should emit structured JSON logs (via project logger) for lifecycle transitions (claim, start, subprocess run, artifact transfer, complete) and should avoid plain `print(...)` diagnostics.
- Error-handling convention for worker runtime: exceptions in background heartbeat/session-upload/poll paths must be logged with error severity and contextual fields; avoid silent `except ...: pass` in operational control paths.
- Coordinator HTTP tracing convention:
  - Use structured request/response events from `http_client.request_json(...)` (method/url/status/elapsed + payloads).
  - Keep tracing configurable by env (`COORDINATOR_HTTP_LOG_DETAILS`, `COORDINATOR_HTTP_LOG_PAYLOADS`, `COORDINATOR_HTTP_LOG_MAX_CHARS`, `COORDINATOR_HTTP_REDACT_AUTH_HEADER`).
  - Suppress default httpx/httpcore info chatter so logs remain queryable and non-duplicative in View Logs.
- Log fidelity convention: server log APIs/downloads must not truncate individual entries or file content by fixed byte caps; truncate/clamp only in the UI layer.
- Tailing convention: when returning tail lines, use line-aware readers that avoid cutting entries mid-line.
- View Logs UI convention: long `description`/`raw_line`/diagnostic cells should use fixed-size scrollable controls, and copy interactions should expose full underlying text.
- Nightmare startup resilience convention: non-critical auxiliary config parse failures (page existence criteria) should degrade to defaults with explicit warnings rather than crash worker runs.
- Worker subprocess convention: `run_subprocess` should persist startup failures into coordinator per-domain log files, and python command invocation should tolerate missing `python` aliases by retrying with `sys.executable`.
- Provisioning convention: `deploy/provision-workers-aws.sh` should default missing required infra/coordinator parameters from `deploy/.env` (and worker env fallback) before treating them as required CLI errors.
- Target registration convention: fleet re-registration from `register_targets.py` is authoritative full-replace, not additive upsert; API payload includes `replace_existing=true` and store truncates `coordinator_targets` before insert.
- Column-configuration convention: all page column modals must use shared `templates/_grid_controls.html.j2` (`NightmareGridControls`) rather than page-local implementations.
- Grid-controls convention update: column reorder controls are enabled for every mode (`full` and `columnsOnly`) unless explicitly disabled via `allowReorder: false`.

- Subprocess failure reporting convention: when a stage subprocess returns non-zero, build the `error` payload from log-tail evidence (not exit code alone) using `summarize_subprocess_failure(stage_name, exit_code, log_path)`.
- Coordinator completion payload convention: `/api/coord/complete` and `/api/coord/stage/complete` should receive concise, actionable error text that includes the concrete exception line when available (for example `NameError: ...`).

- Deploy diagnostic convention: in full-deploy scripts, Docker/Compose diagnostics must detect daemon access mode and fallback to `sudo -n` when invoking-user docker socket access is unavailable (common immediately after docker-group changes on new hosts).
- Script execution convention: do not assume invoking-user context can access Docker daemon even if `docker compose version` succeeds; probe with `docker info` before choosing execution context.

- Server startup convention: every object referenced inside `_prepare_server(...)` must be initialized in `main()` before listener creation.
- Optional service-store convention: feature stores (for example auth0r profile store) should be best-effort initialized with explicit warning and `None` fallback, so unrelated coordinator/dashboard APIs remain available when optional subsystem init fails.

- UI auth token convention: each standalone page script that uses bearer auth must define (or import) `TOKEN_COOKIE_NAME`, `readCookie`, `writeCookie`, and `deleteCookie` helpers before `authHeaders()` references them.
- Auth-header behavior convention: `authHeaders()` should persist non-empty token to cookie and clear cookie when token input is empty, then return `Authorization` header only when token exists.

- UI route parity convention: any navbar-visible page template must have a corresponding explicit GET route in `server.py` (avoid orphaned templates/links).
- Page data contract convention: if a template calls a first-party API path (for example `/api/errors`), that endpoint must be implemented in `server.py` and protected consistently with coordinator auth.
- Error-ingest convention: shell/runtime diagnostics posted to `/api/coord/errors/ingest` should support both single-event and batch-event payloads and return inserted/received counts.

- Discovered-files payload convention: coordinator list APIs should expose `rows` as the canonical collection key, and may include `files` as a backward-compatibility alias during transitions.
- Discovered-file row-shape convention: list rows consumed by UI tables should provide `updated_at_utc` and `content_size_bytes` canonical fields; legacy aliases (`discovered_at_utc`, `file_size`) can be preserved only for compatibility.

- Postgres sort convention for optional ordering modes: avoid `CASE` branches that return different data types (for example text vs timestamp) inside a single expression; instead use separate typed `CASE` expressions in `ORDER BY` or build explicit query variants.
- Preferred SQL ordering convention for mode-dependent sorting: use explicit branch-selected static `ORDER BY` clauses over parameterized `CASE` in `ORDER BY` when key types differ, to eliminate planner/type-resolution ambiguity.

- Extractor UI convention: every visible action button in dashboard templates (for example Pattern Management load/save/add/remove) must have explicit JS handlers bound in the same template script.
- Extractor page UX convention: do not silently swallow domain/match API load failures; surface actionable error text in-page so empty tables are diagnosable.
- Domain visibility convention: avoid default-on "hide zero" filters for primary domain selectors when API data may temporarily lack counts; default should favor visibility over hiding.

- Discovered-target sitemap contract convention: `/api/coord/discovered-target-sitemap` should expose canonical `sitemap.pages` while also including `rows`/`pages` aliases during compatibility transitions; template renderers should tolerate either shape and both legacy/canonical row keys (`parent_count`/`inbound_count`, `parents`/`discovered_from`, `status_code`/`crawl_status_code`).

- URL-existence convention for crawl/source-of-truth outputs:
  - Do not treat generic non-404 statuses as proof of existence.
  - Guess-only discoveries (`file_path_wordlist`, `guessed_url`) require 2xx status to count as existing.
  - Non-guess discoveries require at least 2xx/3xx status to count as existing.
  - Wordlist "hits" reporting should count only true success responses (2xx) and still exclude soft-404.
- Deploy orchestration convention: in central-host scripts, do not assume `COORDINATOR_BASE_URL` is loopback-reachable from that same VM; probe local listener fallbacks (`https://127.0.0.1`, `http://127.0.0.1`, localhost variants) for host-local automation steps.
- Deploy flow convention: keep externally advertised coordinator URL (`COORDINATOR_BASE_URL`) for worker/env distribution, but use an effective locally reachable URL for same-host readiness checks and registration/rollout API calls.
- Startup reliability convention: Postgres connection creation for coordinator/log stores must be bounded with explicit connect timeouts; avoid unbounded startup hangs that prevent listener bind and mask root cause as generic readiness failures.
- Deploy diagnostics convention: readiness failures should print per-URL probe results (public base + localhost variants) and isolate service logs by container/service to make root-cause triage actionable.
