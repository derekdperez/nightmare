# Change Log

## 2026-04-12

- Fixed server-side origin/report fetch issues:
  - `server.py` now adds `Access-Control-Allow-Origin: *` (and related allow headers/methods) on responses plus `OPTIONS` handling.
  - Added output-root static fallback routing for non-API paths so report-relative fetches (for example `/all_domains.results_summary.json`) resolve when `/` serves the all-domains report.
- Why: generated report pages were failing to load companion JSON/log data in browser due origin/fetch path failures.

- Updated `server.py` default serving + configurability:
  - Added server config file `config/server.json` with default `host`, `port`, and `output_root`.
  - Added `--config` support in `server.py`; CLI values still override config.
  - Changed route `/` to serve `all_domains.results_summary.html` by default when available.
  - Added `/dashboard` route for the live monitoring overview UI.
- Why: operators wanted configurable hosting port and immediate landing on the all-domain HTML report.

- Added live dashboard web server (`server.py`) for runtime visibility and report serving:
  - New stdlib threaded HTTP server with auto-refresh dashboard at `/`.
  - Exposes `/api/summary` JSON containing aggregate + per-domain status from output artifacts.
  - Serves generated reports/artifacts via `/files/<repo-relative-path>` with path-safety checks.
  - Dashboard links to Nightmare/Fozzy reports and shows per-domain latest log tail snippets.
- Why: provide a single place to watch ongoing activity and open generated HTML reports while scans/fuzzing are running.

- Added extractor domain-target + worker-config support:
  - `extractor.py` now accepts optional positional `domain` to run extractors for a specific domain output tree.
  - Added parallel processing with configurable worker count (`--workers`), including config-driven default.
  - Added extractor config file loading via `--config` with new default config file `config/extractor.json`.
  - New `config/extractor.json` includes `"workers": 4` default.
- Why: allow focused extractor runs by domain and improve throughput across domains with configurable concurrency.

- Improved post-fuzz finalization behavior in `fozzy.py` to prevent apparent hangs:
  - Added explicit progress lines for finalization stages (artifact write, per-domain summary, master summary generation).
  - Removed temporary `SIGINT` ignore during finalization so `Ctrl+C` remains responsive.
  - Added interrupt handling around master summary generation to skip that step and finish with partial outputs if user interrupts.
- Why: runs could appear stalled after group-level "execution finished" logs, and interrupts were not honored during finalization.

- Added configurable page-existence criteria for Nightmare:
  - New config file: `config/page_existence_criteria_config.json`.
  - `config/nightmare.json` now includes `page_existence_criteria_config` to select criteria file.
  - `nightmare.py` now loads/sanitizes this criteria and applies it in soft-404 detection and not-found status handling.
  - Effective output filtering (`*_url_inventory.json` entries and related downstream artifacts) now excludes URLs with configured not-found status codes, not only soft-404 markers.
- Why: pages like `/.htpasswd` or `/add` may return small branded 200 "Page Not Found" responses and should be treated as non-existent using configurable detection logic.

- Improved Fozzy Ctrl+C shutdown semantics:
  - `run_incremental_domains()` now returns interruption status.
  - `main()` now exits with code `130` after graceful cleanup when interrupted in either incremental or single-domain run paths.
- Why: interrupts were often handled internally with partial-output finalization but process exit status still appeared successful (`0`), which made cancellation handling less predictable.

- Fixed Fozzy single-domain report loading when opened as local files:
  - `render_anomaly_summary_html()` now embeds a single-domain fallback payload directly in the generated HTML.
  - Browser JS now skips fetch/XHR disk reads when the page protocol is `file:` and uses embedded payload data for table bootstrap.
- Why: modern browsers block `file://` fetch/XMLHttpRequest with CORS-origin `null`, which previously caused empty report tables and console errors.

- Added crawl request inventory output to `nightmare.py`:
  - New per-domain artifact: `output/<root-domain>/requests.json` (config key `requests_output`, CLI override `--requests-output`).
  - Includes every crawl-requested URL with booleans for `found_directly`, `guessed`, `inferred`, and `exists`, plus supporting status/source fields.
  - Added `crawl_requested` to URL inventory records so crawl-attempted URLs (including failures) are tracked explicitly and serialized in session state.
  - Added help text key `requests_output_help` in `config/nightmare.help.json`.
- Why: spidering runs now emit a dedicated machine-readable request ledger with discovery classification and existence signal as requested.

- Added `requirements.txt` at repo root for pip installs with core runtime dependencies:
  - `openai`
  - `scrapy`
  - `tldextract`
- Why: provide a standard `pip install -r requirements.txt` setup path.

- Updated `pack.py` / `unpack.py` transport format:
  - `pack.py` now writes a base64-JSON transport envelope (`transport_encoding: base64-json`) so the packed text payload itself is encoded for transport.
  - `unpack.py` now decodes that envelope before restoring files, with backward compatibility for older raw packed JSON format.
- Why: explicit transport-safe encoding was requested in addition to per-file base64 content storage.

- Added repository pack/unpack utilities:
  - New `pack.py`: creates a single `packed.json` containing directory paths, file paths, metadata, and base64 file contents.
  - New `unpack.py`: restores files/folders from `packed.json` with path traversal safety checks.
  - `pack.py` excludes the `output/` directory tree and skips the output `packed.json` file itself.
- Why: provide deterministic project snapshot/restore workflow without relying on external archive formats.

- Optimized `fozzy.py --generate-master-report` HTML generation to avoid large inline table payloads:
  - Removed server-side row inlining for `master_domain_inventory`, `master_per_route_inventory`, and `extractor_matches`.
  - Master report now emits lightweight table shells and populates those sections client-side from loaded summary JSON.
- Why: reduce master-report generation stalls and keep HTML output lightweight and data-driven.

- Added persistent process logging for `fozzy.py` and `extractor.py`:
  - `fozzy.py` now supports config/CLI `log_file` and defaults to mode-specific logs:
    - single-domain run: `<domain-output>/<root_domain>.fozzy.log`
    - incremental run: `<scan-root>/fozzy.incremental.log`
    - master-report generation: `<master-root>/fozzy.master_report.log`
  - `extractor.py` now defaults to `<scan-root>/extractor.log` and supports `--log-file`.
- Why: operators need durable logs instead of console-only output.
- Added master-report log viewer in `all_domains.results_summary.html`:
  - Master payload now includes discovered `log_files`.
  - HTML includes a log selector + viewer that loads selected logs from disk at runtime.
- Why: allow quick troubleshooting without leaving the master report.

- Refactored `fozzy.py` results HTML rendering to avoid inlining large discrepancy datasets:
  - `render_anomaly_summary_html()` now emits a lightweight shell and loads discrepancy rows from the companion `*.results_summary.json` file at page load.
  - Added `summary_json_filename` to summary payloads in both single-domain and master summary writers.
  - Added browser-side fallback messaging when local-file fetch/XHR of JSON is blocked by browser security.
- Why: report table data should be sourced from on-disk files at load time rather than hardcoded into generated HTML.

- Added dedicated incremental domain concurrency control in `fozzy.py`:
  - New config key: `incremental_domain_workers` (added to `config/fozzy.json`).
  - New CLI override: `--incremental-domain-workers`.
  - `run_incremental_domains()` now uses `incremental_domain_workers` instead of reusing `max_background_workers`.
- Why: separate multi-domain scheduling from per-domain fuzz worker settings so both can be tuned independently.
- Updated master HTML extractor section rendering:
  - Extractor table now renders even when there are zero extractor rows.
  - Importance column label/filter updated to explicit `Importance score`.
- Why: ensure generated HTML consistently includes the importance-score column/schema for downstream review and automation.

- Fixed `fozzy.py` incremental multi-domain execution to honor configured worker concurrency:
  - `run_incremental_domains()` now runs domains in parallel via child-process workers instead of a serial `for` loop.
  - Worker count comes from effective `max_background_workers` (including config/CLI overrides).
  - Added failure reporting per domain and graceful interrupt handling for active child workers.
- Why: with no parameters file, Fozzy previously processed one domain at a time regardless of `max_background_workers`.
- Incremental mode now launches each domain run with per-domain worker caps forced to `1` (`--max-background-workers 1`, etc.) while the parent controls overall domain concurrency.
- Why: prevent N x M thread explosion and shared in-process global-config races when running multiple domains concurrently.

- Updated `nightmare.py` spider wordlist seeding to support both default and custom wordlists:
  - Added `--crawl-wordlist` CLI option and `crawl_wordlist` config key.
  - Added `resolve_wordlist_path()` resolution rules (repo-relative + bare filename fallback to `resources/wordlists/`).
  - Generalized seed loading to read from a selected wordlist path and accept absolute URL entries as well as relative paths.
- Why: spidering needed to include all configured path seeds, not just a hardcoded file.
- Updated crawl budget behavior for path-seed runs:
  - `crawl_domain()` now increases effective crawl page budget when needed so all pending seed URLs (including wordlist seeds) are schedulable in the run.
  - Wordlist seeds are now registered in discovery inventory with source `file_path_wordlist`.
  - `guessed_paths_index.json` now records the actual wordlist source used in that run.
- Why: previous `max_pages` caps could silently drop many wordlist seed paths from spider execution and reporting.

- Updated `fozzy.py` incremental scan behavior to reduce "appears hung" startup:
  - Replaced `Path.rglob`-based mtime traversal with `os.scandir` iterative traversal in `folder_tree_max_mtime_ns` for lower overhead on large trees.
  - Added immediate per-domain progress output during incremental dirty-check pass in `run_incremental_domains`.
- Why: default no-argument execution could spend a long time in pre-run change detection with no visible output.
- Hardened `nightmare.py` batch state persistence against transient Windows file-lock failures:
  - `_atomic_write_json` now uses a unique temp filename and retries atomic replace with short backoff before failing.
  - Batch `persist()` now catches write errors and logs a warning instead of terminating the orchestrator.
- Why: observed crash on `[WinError 5] Access is denied` when rotating `batch_run_state.json` during batch completion.
- Added extractor `importance_score` support to reporting pipeline:
  - `extractor.py` now carries `importance_score` from `resources/wordlists/extractor_list.txt` into both match detail files and `extractor/summary.json` rows.
  - Master HTML report table in `fozzy.py` now includes an `Importance` numeric column with sortable header and per-column filter input.
- Why: extractor list now includes score metadata and report needs analyst triage by severity/priority.

- Fixed `fozzy.py` master-report HTML template f-string syntax for Python runtime compatibility:
  - Moved backslash-bearing JSON escaping (`.replace("</", "<\\/")`) out of inline f-string expressions and into precomputed variables.
  - Template now interpolates `summary_json_filename_js` and `master_log_files_js` constants.
- Why: Python raised `SyntaxError: f-string expression part cannot include a backslash` at startup on `python3 fozzy.py`.

- Fixed `UnboundLocalError` in `render_anomaly_summary_html` when generating single-domain summaries:
  - Initialized `master_log_files = payload.get("log_files")` before building JSON-escaped JS constants.
  - Removed later duplicate assignment and reused the initialized variable for master-only log viewer section checks.
- Why: child domain workers crashed during `write_results_summary()` with `local variable 'master_log_files' referenced before assignment`, causing incremental domain failures.

- Added `--status` quick-report mode to `nightmare.py`:
  - New CLI flag `--status` prints aggregate progress without running crawl/batch work.
  - Report fields include spidered target-domain coverage, total unique requested URLs, total unique parameterized GET routes, and top 10 domains by URL volume.
  - Per-domain top-list rows include: unique URLs, parameterized GET count, extractor matches, anomalies, and reflections.
  - Metrics are loaded from on-disk artifacts (`*_url_inventory.json`, `*.parameters.json`, `fozzy.summary.json`, `extractor/summary.json`) with safe fallbacks.
- Why: provide a fast operational snapshot of campaign progress directly from existing output files.

- Added soft-404 filtering for HTTP 200 pages in `nightmare.py` so non-existent pages are not treated as successful:
  - New response heuristics detect likely soft-404 pages using title markers and common not-found phrases on small 200 responses.
  - `probe_url_existence()` now validates HEAD 200 with a follow-up GET and marks soft-404 pages as `exists_confirmed=false`.
  - Crawl parser now marks soft-404 responses and skips link extraction for those pages.
  - URL inventory/source-of-truth output now excludes soft-404 records from effective `entries`/`total_urls` while preserving raw data in `entries_raw` and `total_urls_raw`.
  - Wordlist guessed-path hit logic now treats soft-404 responses as misses.
- Why: 200 status alone is not a reliable existence signal; generic “page not found” templates were inflating discovered/existing counts.

## 2026-04-13

- Fixed all-domains report load reliability in `fozzy.py`:
  - Added compact master-discrepancy serialization (`compact_discrepancies=True`) that trims `baseline/anomaly body_preview` and long unified diff text before writing `all_domains.results_summary.json`.
  - Added explicit master-table fallback rendering when summary payload cannot be loaded, so inventory/extractor tables show a concrete error message instead of staying on `Loading...` indefinitely.
- Why: master summary JSON can grow too large for reliable browser fetch/parse/render, and payload-load failures previously left multiple master tables in a perpetual loading state.

- Hardened report writes against corruption:
  - Added `_atomic_write_text(...)` and switched master/single-domain summary JSON+HTML writes to atomic temp-file replacement.
- Why: interrupted or failed writes (for example, low disk conditions) should not leave zero-byte summary artifacts that break report loading.

- Improved worker scheduling and profiling in `nightmare.py` for high-concurrency runs:
  - Batch workers now receive OS-specific priority hints.
    - Windows: each worker process gets a reduced CPU affinity mask (default: subset of available cores; configurable via `batch_worker_affinity_cores`).
    - macOS/Linux: each worker process applies `os.nice(+10)` by default (configurable via `batch_worker_nice`).
  - Added dev-only timing instrumentation with per-method aggregates and slowest-call output (`[dev-perf]`) for key pipeline methods (crawl, AI calls, probe calls, artifact writes, batch orchestration stages).
  - Added config keys: `environment`, `dev_timing_logging`, `dev_timing_log_each_call`, `batch_worker_nice`, `batch_worker_affinity_cores`.
- Why: lower worker scheduling priority prevents worker saturation from starving interactive/system workloads, and dev-only timing data makes performance hotspots visible for targeted optimization.

- Added self-hosted coordinator foundation in `server.py` with optional Postgres backend (`database_url`):
  - New `/api/coord/*` endpoints for target registration, claim/heartbeat/complete, session save/load, and coordinator state summaries.
  - Added token-based protection (`coordinator_api_token`) for coordinator endpoints.
- Added web-server style dual listeners in `server.py`:
  - Supports HTTP and HTTPS listeners simultaneously via `http_port` and `https_port` (defaults now `80` and `443`).
  - HTTPS listener requires `cert_file` + `key_file`; if absent, HTTPS is disabled with explicit startup warning.
  - Retains backward compatibility with legacy `--port` single-listener mode.
- Added dependency `psycopg[binary]` in `requirements.txt` for Postgres coordinator mode.
- Why: support a central self-hosted coordination server that behaves like a standard web server while persisting shared queue/session state in Postgres for multi-VM execution.

- Implemented centralized distributed orchestration stack for multi-VM scaling:
  - `server.py` now includes Postgres-backed coordinator APIs for target leaseing, stage leaseing, session checkpointing, and artifact storage/replication.
  - Added tables: `coordinator_targets`, `coordinator_sessions`, `coordinator_stage_tasks`, `coordinator_artifacts`.
  - Added endpoints:
    - target queue: `/api/coord/register-targets`, `/api/coord/claim`, `/api/coord/heartbeat`, `/api/coord/complete`
    - stage queue: `/api/coord/stage/enqueue`, `/api/coord/stage/claim`, `/api/coord/stage/heartbeat`, `/api/coord/stage/complete`
    - session/artifacts: `/api/coord/session` (GET/POST), `/api/coord/artifact` (GET/POST), `/api/coord/artifacts`.
- Added `coordinator.py` distributed worker orchestrator:
  - Runs Nightmare/Fozzy/Extractor concurrently with dedicated worker pools (default 2 each).
  - Uses lease heartbeat for target and stage locks.
  - Uploads session checkpoints during active crawl and syncs artifacts to Postgres for cross-VM continuation.
  - Enqueues stage transitions (`nightmare -> fozzy -> extractor`) only after successful completion.
- Added full container/deploy scaffolding for EC2:
  - `Dockerfile`, `docker-entrypoint.sh`, `.dockerignore`
  - `deploy/docker-compose.central.yml` (server + postgres)
  - `deploy/docker-compose.worker.yml` (distributed workers)
  - `deploy/.env.example`, `deploy/bootstrap-central.sh`, `deploy/bootstrap-worker.sh`
  - `DEPLOYMENT.md` with operational runbook.
- Updated dependencies with `psycopg[binary]` for Postgres support.
- Why: enable horizontal VM scale-out with centralized locking/state, resumable failover, and shared artifact/session access across workers.

- Restored/strengthened Cloudflare block detection in `nightmare.py` soft-404 logic:
  - Added explicit Cloudflare invalid-page rule requiring BOTH title phrase (`attention required! | cloudflare`) and blocked-body phrase (`sorry, you have been blocked`).
  - Rule is evaluated before status-specific soft-404 heuristics, so blocked pages with non-200 statuses are still classified non-existent/invalid.
  - `probe_url_existence()` HTTPError path now also runs `detect_soft_not_found_response()` and marks `exists_confirmed=false` when blocked markers are present.
- Updated page existence criteria schema/config with `cloudflare_block_title_phrase` and `cloudflare_block_body_phrase` keys.
- Why: Cloudflare challenge/block pages were being miscounted as existing due to missing checks in HTTPError flow and overwritten title/body marker handling.

## 2026-04-13

- Added automated central bootstrap script: deploy/bootstrap-central-auto.sh.
  - Generates strong random POSTGRES_PASSWORD + COORDINATOR_API_TOKEN.
  - Auto-detects base URL from EC2 metadata (public hostname/IP) with fallback to external IP/hostname.
  - Generates self-signed TLS cert/key under deploy/tls/.
  - Writes deploy/.env and deploy/worker.env.generated.
  - Rebuilds and starts the central Docker stack in one command.
- Why: enable immediate multi-VM bring-up with secure defaults and minimal manual setup steps.

- Updated image/compose secret injection:
  - Dockerfile now accepts build args for COORDINATOR_BASE_URL, COORDINATOR_API_TOKEN, and DATABASE_URL and exports them as image env vars.
  - deploy/docker-compose.central.yml and deploy/docker-compose.worker.yml now pass those build args during image build.
- Why: allow rebuilt images to carry the generated coordinator connection/security values when requested.

- Added Windows-native deployment bootstrap script: deploy/bootstrap-windows.ps1.
  - Supports -Role Central and -Role Worker setup flows.
  - Central flow generates strong credentials, detects coordinator base URL, creates TLS PEM cert/key, writes deploy/.env and deploy/worker.env.generated, and starts central compose stack.
  - Worker flow writes worker .env from passed args (or copied worker.env.generated) and starts worker compose stack.
- Why: enable end-to-end setup on Windows VMs without Bash/WSL.
- Fixed deploy/bootstrap-windows.ps1 script-path resolution bug on Windows PowerShell where $MyInvocation.MyCommand.Path inside function scope can be absent.
- Updated path bootstrap to use $PSScriptRoot with $PSCommandPath fallback.
- Why: script crashed at startup with "property 'Path' cannot be found".
- Enhanced deploy/bootstrap-central-auto.sh for Linux bootstrap reliability:
  - auto-installs missing dependencies on apt-based hosts (docker, docker compose plugin, curl, openssl, git) and enables docker service.
  - adds optional one-command worker auto-provisioning flow (--auto-provision-workers) integrated with new AWS helper script.
- Added deploy/provision-workers-aws.sh to launch and cloud-init bootstrap worker EC2 instances directly from central.
- Added .gitattributes rules to force LF endings for shell scripts and deployment YAML to prevent cross-platform shebang/parse failures.
- Updated Linux bootstrap package-manager support to prefer yum/dnf with pt-get fallback.
  - deploy/bootstrap-central-auto.sh now installs dependencies via yum/dnf first and validates docker compose availability.
  - deploy/provision-workers-aws.sh cloud-init now installs packages via yum/dnf/apt detection instead of apt-only package list.
- Why: support Amazon Linux/RHEL-style EC2 hosts where pt-get is unavailable.
- Hardened bootstrap against Docker Compose version mismatches:
  - deploy/bootstrap-central-auto.sh now resolves compose CLI as docker compose or docker-compose fallback.
  - deploy/provision-workers-aws.sh cloud-init now uses the same fallback before starting worker stack.
- Why: older AMIs often ship standalone docker-compose without compose plugin, causing version/command failures.
- Fixed Amazon Linux 2023 dependency bootstrap conflict (curl vs curl-minimal).
  - deploy/bootstrap-central-auto.sh now installs curl-minimal for yum/dnf hosts.
  - deploy/provision-workers-aws.sh cloud-init also uses curl-minimal on yum/dnf.
- Why: AL2023 ships curl-minimal by default and installing curl caused dependency solver failures.
- Fixed AL2023 compose package availability issue: bootstrap no longer depends on docker-compose-plugin in yum/dnf repos.
  - Central bootstrap now installs standalone docker-compose binary automatically if compose plugin/command is missing.
  - Worker cloud-init uses same fallback.
- Why: many Amazon Linux images do not provide docker-compose-plugin package, which previously caused setup failure.
- Fixed bootstrap failure when Docker is installed but current user lacks socket permissions.
  - deploy/bootstrap-central-auto.sh now detects Docker access mode and falls back to sudo for compose commands automatically.
- Why: Amazon Linux ec2-user often needs re-login after docker group changes; bootstrap should continue in current session.
- Added AWS credential preflight in deploy/provision-workers-aws.sh using ws sts get-caller-identity with actionable remediation guidance.
- Why: central setup could succeed, then worker provisioning failed late with opaque "Unable to locate credentials".
- deploy/provision-workers-aws.sh now auto-loads coordinator URL/token from deploy/.env when flags are omitted.
- Why: operators should not need to manually look up or pass coordinator credentials after central bootstrap.
- Fixed master HTML extractor section reliability by bounding extractor rows included in ll_domains.results_summary.json.
  - ozzy.py now caps master payload extractor rows at MASTER_REPORT_EXTRACTOR_ROWS_MAX (10,000), while tracking extractor_matches_total and extractor_matches_truncated.
  - Master report UI now shows explicit truncation note when full extractor dataset is larger than rendered payload.
- Why: very large per-domain extractor summaries could make master summary payload too large to load/render, causing extractor table to appear stuck/empty.

- Added central worker fleet status endpoint to coordinator server:
  - `server.py` now exposes `GET /api/coord/workers` (token-protected like other `/api/coord/*` endpoints).
  - Endpoint aggregates `coordinator_targets` and `coordinator_stage_tasks` by `worker_id`, returning:
    - worker heartbeat recency (`last_heartbeat_at_utc`, `seconds_since_heartbeat`)
    - online/stale classification using configurable `stale_after_seconds`
    - running/active lease counts and active stage names.
- Why: operators needed a single API to verify all worker VM process identities and health from the central machine.

- Added `client.py` operator CLI for one-command status checks from central:
  - `python client.py status` now performs:
    - coordinator worker status query (`/api/coord/workers`)
    - optional AWS SSM fanout command to each worker VM to report `docker compose ... worker ... ps` container state.
  - Supports environment-first configuration from `deploy/.env` and flags for region/target override (`--aws-region`, `--ssm-target-*`) plus `--skip-ssm`.
- Why: remove repetitive manual AWS/SSM/curl command sequences and provide a fast, consistent operational status check entrypoint.

- Added centralized worker rollout command in `client.py`:
  - New action: `python client.py rollout`.
  - Uses AWS SSM fanout to run on each targeted worker VM:
    - `git fetch --all --prune`
    - `git checkout <branch>`
    - `git pull --ff-only origin <branch>`
    - `docker compose ... up -d --build` for worker stack restart
    - `docker compose ... ps --format json` for post-restart status output.
  - Added rollout flags: `--branch`, `--repo-dir`, `--ssm-target-key`, `--ssm-target-values`.
- Why: operators needed a single central command to update worker VM code and restart processing across the fleet.

- Fixed coordinator worker crash loop on startup (`NameError: CoordinatorConfig`).
  - Moved the shared `CoordinatorConfig` dataclass into `coordinator_app/runtime.py` and imported it in `coordinator.py` so `load_config()` can instantiate it in-module.
  - Restored missing runtime helpers `_read_json_dict` and `_now_iso` used by `SessionUploader`.
- Why: workers were exiting immediately during config load; this unblocks normal boot and prevents the next latent NameError in session upload path.

- Fixed local deploy auth mismatch caused by regenerated secrets with persisted Docker volumes.
  - Updated `deploy/run-local.ps1` and `deploy/run-local.sh` to reuse existing `POSTGRES_PASSWORD` and `COORDINATOR_API_TOKEN` from `deploy/.env` when present.
- Why: repeated local runs were regenerating DB credentials while keeping `postgres_data`, causing server startup failure (`password authentication failed`) and downstream worker `connection refused` loops.

- Fixed local worker TLS failure against self-signed coordinator cert (`CERTIFICATE_VERIFY_FAILED`).
  - Added coordinator `insecure_tls` setting to runtime config model and loader (`COORDINATOR_INSECURE_TLS` env-supported).
  - Updated local/worker compose env wiring and local bootstrap scripts so local runs set `COORDINATOR_INSECURE_TLS=true`.
  - Kept `config/coordinator.json` free of hardcoded `insecure_tls` so deployment-specific env values can control TLS behavior.
- Why: local Docker workers connect to `https://server:443` with a self-signed certificate; strict verify caused perpetual claim failures while server itself was healthy.

- Expanded project-wide unit test coverage with deterministic, fast tests and no external service dependencies.
  - Added `tests/conftest.py` to ensure repo-root imports work under both `pytest` and `python -m pytest`.
  - Added new unit test modules for:
    - shared config utilities and model validation,
    - HTTP client behavior (success, HTTP errors, network failures, capped reads),
    - coordinator runtime helpers (`SessionUploader`, zip/unzip safety, subprocess logging, config loading),
    - HTTP request queue lifecycle (enqueue/claim/retry/dead-letter/success),
    - pack/unpack archive utilities,
    - operator/client + queue CLI helpers + target registration helpers,
    - report rendering and pure helper functions in `server_app/store.py`.
- Why: raise confidence for refactors and runtime bug fixes while keeping CI/local test runs short and infrastructure-independent.

- Hardened central server runtime against public internet scanner disconnect noise.
  - Added `_is_client_disconnect_error(...)` and handler-level suppression for common connection-abort/TLS disconnect exceptions in `server.py` so expected scanner aborts no longer emit full stack traces.
  - Removed obsolete Compose `version` key from `deploy/docker-compose.central.yml` and `deploy/docker-compose.worker.yml` to remove deprecation warning noise.
  - Added unit tests for disconnect classification helper.
- Why: AWS-exposed central nodes were producing noisy traceback logs (`BrokenPipeError`) from malformed/scanner traffic despite healthy service behavior.

## 2026-04-16

- Refactored `server.py` to remove embedded coordinator data-access implementation (`CoordinatorStore`) and import it from `server_app/store.py`.
  - Added `CoordinatorStore.database_status()` to `server_app/store.py` so the `/api/coord/database-status` API remains functional after extraction.
  - Removed duplicate target-normalization/store helpers from `server.py` that only existed to support the in-file store class.
  - Switched dashboard/worker page rendering to template helpers in `reporting/server_pages.py` and removed large inline page-render methods from `server.py`.
  - Added regression coverage in `tests/test_module_decomposition.py` to assert `server.py` uses external store module and no longer defines `class CoordinatorStore`.
- Why: `server.py` had become a large mixed-responsibility file; this extraction enforces module boundaries and reduces maintenance risk for future decomposition work.

- Fixed AWS worker bootstrap/provision TLS wiring for self-signed coordinator certificates:
  - `deploy/bootstrap-central-auto.sh` now persists `COORDINATOR_INSECURE_TLS` into both `deploy/.env` and `deploy/worker.env.generated` (defaulting to `true` for the self-signed bootstrap flow).
  - `deploy/provision-workers-aws.sh` now accepts/loads `COORDINATOR_INSECURE_TLS` and writes it into each worker `deploy/.env` during cloud-init.
- Why: workers could stay idle with pending targets because coordinator claims failed on certificate verification against bootstrap-generated self-signed certs.

- Restored database page discoverability in server UI after template refactor:
  - Added `/database` navigation links back into `templates/server_dashboard.html.j2` and `templates/worker_control.html.j2`.
  - Added template regression tests in `tests/test_refactor_modules.py` to assert database link presence in rendered dashboard/worker pages.
- Why: `/database` route still existed but link/navigation disappeared from the website UI after page-render modularization.

- Converted `/database` to shared template rendering flow:
  - Added `templates/database_status.html.j2`.
  - Added `render_database_html()` in `reporting/server_pages.py`.
  - Updated `server.py` `/database` route to use template renderer and removed inline `_render_database_html`.
- Hardened dashboard/worker UI runtime behavior:
  - Replaced `replaceAll("%2F", "/")` with `replace(/%2F/g, "/")` for broader JS compatibility.
  - Added visible dashboard load-error handling (`showLoadError`) instead of silent refresh failure.
  - Fixed malformed JS in worker logs link rendering that prevented template script execution.
- Extended refactor tests:
  - Added assertions for database template rendering and route wiring expectations.
  - Updated decomposition tests to enforce externalized database page rendering in `server.py`.

- Limited coordinator database-status payload size for stability:
  - server_app/store.py now returns only the first 20 rows per table (max_rows_per_table=20) while still returning full ow_count via COUNT(*).
  - Added ows_returned and ows_limited metadata per table.
  - Updated 	emplates/database_status.html.j2 to display truncation context (showing first N rows) and show the active row-limit card.
  - Added unit coverage in 	ests/test_reporting_and_store_helpers.py to lock row-limit/query behavior.

- Added coordinator token cookie persistence in server UI templates:
  - 	emplates/database_status.html.j2 now reads/writes 
ightmare_coord_token and auto-populates token input on load.
  - 	emplates/worker_control.html.j2 now uses the same cookie behavior for shared auth UX across coordinator pages.
  - Cookie attributes: Path=/, SameSite=Strict, Max-Age (30 days), and Secure when served over HTTPS.
  - Added template regression assertions in 	ests/test_refactor_modules.py for cookie helper presence.

- Hardened /api/coord/database-status to prevent UI hangs on large tables/artifacts:
  - Switched per-table row count from expensive COUNT(*) scans to Postgres catalog estimate (pg_class.reltuples).
  - Kept row cap at 20 and replaced raw SELECT * payloads with preview-safe projections (no full ytea transfer; text/json fields truncated for display).
  - Added response metadata (ow_count_is_estimate, max_text_preview_chars) and UI label for estimated row counts.
  - Updated DB-status unit test fixture to assert new query/model behavior.

- Modularized spider URL policy and fuzzing core logic into reusable modules while preserving existing public function names in main scripts:
  - Added 
ightmare_shared/value_types.py with shared value-type inference (infer_observed_value_type) used by both Nightmare and Fozzy.
  - Added 
ightmare_app/spider_url_policy.py for URL normalization, crawl/AI/source-of-truth filtering, suffix matching, and AI URL condensation helpers.
  - Added ozzy_app/fuzz_core.py for ParameterMeta/RouteGroup models and request-construction helpers.
  - Updated 
ightmare.py and ozzy.py to delegate to these modules via compatibility wrappers, reducing god-file surface without changing call sites.
  - Added module-level regression tests in 	ests/test_modular_spider_fuzz_core.py.
- Fixed coordinator API auth fallback for browser UI pages:
  - `server.py` now accepts `nightmare_coord_token` from `Cookie` header in `_is_coordinator_authorized()` in addition to `Authorization: Bearer` and `X-Coordinator-Token`.
  - Added `_read_cookie()` helper for robust cookie parsing and URL-decoding.
  - Added `tests/test_server_auth_cookie.py` covering bearer, X-token, cookie token, URL-encoded cookie token, and rejection cases.
- Why: operators reported `/database` returning 401 despite valid token usage; cookie fallback hardens auth in environments where auth headers are dropped or not persisted.

- Added normalized crawl export generation into the `nightmare.py` main flow:
  - Integrated `nightmare_app/normalized_exports.write_normalized_exports(...)` after inventory generation.
  - New default artifacts under each domain output now include:
    - `sitemap.json`, `sitemap.xml`, `sitemap.html`
    - `requests.json`, `cookies.json`, `scripts.json`, `high-value.json`, `pages.json`
    - `redirects.json`, `findings.json`
    - mirrored folders: `cookies/`, `scripts/`, `high-value/`, `pages/`
    - `normalized_data/raw_requests/` and `normalized_data/raw_responses/`
  - Fixed normalized export bugs:
    - removed self-copy loop for `pages/` mirroring
    - deduplicated redirect rows and normalized findings rows
    - made high-value source path explicit (`source_high_value_root`) instead of hardcoded wrong root
  - Added `tests/test_normalized_exports_unit.py` to verify `redirects.json` and `findings.json` generation and expected finding types.

- Hardened `/api/coord/database-status` reliability for operator UI:
  - Added server-side exception guard in `server.py` around `CoordinatorStore.database_status()` to return structured HTTP 500 JSON instead of connection resets.
  - Updated `server_app/store.py` `database_status()` to tolerate per-table query failures (records `table_error` for that table and continues).
  - Updated `templates/database_status.html.j2` to render per-table query errors in the page.
  - Added regression test `test_database_status_tolerates_single_table_query_failure` in `tests/test_reporting_and_store_helpers.py`.
- Why: browser-side `Failed to fetch` on the Database page can result from unhandled DB introspection errors; endpoint now degrades gracefully and exposes actionable error details.

- Fixed worker visibility on `/workers` when workers are alive but idle:
  - Added `coordinator_worker_presence` table in `server_app/store.py` schema.
  - Added presence heartbeats (`_touch_worker_presence`) on target/stage claim, heartbeat, and completion paths.
  - Updated `worker_statuses()` and `worker_control_snapshot()` SQL to union worker IDs from target/stage aggregates, queued commands, and worker presence.
  - Added API exception guards in `server.py` for `/api/coord/workers` and `/api/coord/worker-control` to return structured JSON errors instead of connection-reset style failures.
  - Added tests:
    - `test_worker_control_snapshot_includes_presence_only_worker`
    - `test_worker_statuses_includes_presence_only_worker`
- Why: previous worker pages only discovered workers that had target/stage rows or queued commands, so idle polling workers could appear as “no workers discovered”.
- Added coordinator crawl-progress observability path for per-domain crawl monitoring:
  - New store query: `CoordinatorStore.crawl_progress_snapshot(limit=...)` in `server_app/store.py`.
  - New API route: `GET /api/coord/crawl-progress` in `server.py`.
  - New client action: `client.py progress` with `--progress-limit`.
  - `status` action now also prints crawl-progress summary.
- Fixed row-column index mapping bug in crawl-progress result parsing that could misread timestamps/counts and fail at runtime.
- Added tests:
  - `test_client_main_progress_rejects_skip_coordinator`
  - `test_client_main_progress_fetches_crawl_progress`
  - `test_crawl_progress_snapshot_reports_domain_counts`
- Validation: `python -m pytest -q` -> 84 passed.
- Added web app crawl progress report page:
  - New route in `server.py`: `GET /crawl-progress`.
  - New template-backed renderer in `reporting/server_pages.py`: `render_crawl_progress_html()`.
  - New template `templates/crawl_progress.html.j2` with token-cookie auth, auto-refresh (3s), domain filter, limit control, and table showing phase/found/visited/frontier/active-workers/activity age from `/api/coord/crawl-progress`.
- Added cross-page navigation link to Crawl Progress from dashboard/workers/database templates.
- Added/updated tests for template render and server decomposition imports.
- Validation: full suite passes (`python -m pytest -q` => 86 passed).
- Fixed coordinator DB-status table preview regression and tightened DB table introspection behavior:
  - Resolved placeholder mismatch bug caused by `format('<bytea %s bytes>', ...)` by switching to SQL string concatenation for bytea preview.
  - Removed `reltuples` row estimate usage; `database_status()` now executes exact `COUNT(*)` per table.
  - Table row preview now returns only top 20 rows (ordered by most recent timestamp/id-style columns when available) and reports truncation against exact count.
- Worker control hardening and stale-worker cleanup:
  - Worker status/control queries now use recency retention windows so historical worker IDs age out of API responses.
  - Added richer worker status derivation (`running`, `paused`, `stopped`, `errored`, `idle`) and hid stale-only records from worker control snapshots.
  - Added worker command lifecycle APIs in store/server (`claim_worker_command`, `complete_worker_command`) and connected distributed coordinator loops to consume `start/pause/stop` commands.
  - Queue behavior now supersedes older queued commands for a worker.
- Dashboard/crawl progress UX:
  - Dashboard `/api/summary` now merges coordinator crawl-progress data, so domains/URL counts still populate even when local output folders are empty on central nodes.
  - Crawl progress defaults increased to 2000 in API/page/client.
- Validation: `python -m pytest -q` -> 88 passed.

- Added extractor artifact exploration surface to the coordinator web app:
  - New page route: `/extractor-matches`.
  - New template: `templates/extractor_matches.html.j2` with domain dropdown (`domain + match count`), search, sortable columns, and file view/download links.
  - New APIs in `server.py`:
    - `GET /api/coord/extractor-matches/domains`
    - `GET /api/coord/extractor-matches`
    - `GET /api/coord/extractor-matches/download`
    - `GET /api/coord/extractor-matches/file`
  - Added in-memory TTL/LRU-style cache for parsed extractor zip content (`_ExtractorMatchesCache`) to avoid repeated unzip/JSON parse overhead.
  - Added store query `CoordinatorStore.list_extractor_match_domains(...)` to list domains with extractor match artifacts and summary-derived counts.
  - Updated cross-page nav links to include Extractor Matches from dashboard/workers/database pages.
  - Added regression coverage for template/module decomposition and extractor-domain listing behavior.
- Validation: `pytest -q` -> 91 passed.

- Extractor Matches page enhancement:
  - Added top-filter summary on `/extractor-matches` showing top 10 filters by match count.
  - `/api/coord/extractor-matches` now returns `top_filters` computed from the full matched set (after optional search filter, before row-limit truncation).
  - UI renders this in a dedicated panel (`Top Filters (Top 10)`).
  - Added unit coverage for ranking/limit behavior (`_top_extractor_filters`).
- Validation: `pytest -q` -> 92 passed.

- Extractor Matches performance/UX hardening:
  - Switched `/api/coord/extractor-matches` to server-side paging/sort/filter with `limit`, `offset`, `sort_key`, `sort_dir`, and per-column `f_<column>` filters.
  - Default page size lowered to 250 (max 2000 per request) to reduce response size and latency.
  - Added response paging metadata (`offset`, `next_offset`, `prev_offset`, `has_more`, applied sort/filter fields).
  - Added fixed-height scrollable table containers on extractor page so large datasets do not expand page height.
  - Added lazy page loading controls (Prev/Next) and debounced server fetches for global and column filters.
  - Added sortable/filterable/searchable behavior for all extractor-page tables:
    - matches (server-driven global + per-column search/filter/sort),
    - top filters (client-side global + column filter + sort),
    - artifact files (client-side global + column filter + sort).
  - Kept top-filter aggregation server-side and based on full filtered set (pre-pagination).
- Validation: `pytest -q` -> 93 passed.

- Added new coordinator web app page: `/fuzzing` (Fozzy findings explorer).
  - New template: `templates/fuzzing.html.j2`.
  - New renderer: `reporting/server_pages.py::render_fuzzing_html`.
  - New route in `server.py`: `GET /fuzzing`.
  - New APIs:
    - `GET /api/coord/fuzzing/domains`
    - `GET /api/coord/fuzzing`
    - `GET /api/coord/fuzzing/download`
    - `GET /api/coord/fuzzing/file`
  - Implemented server-side paging/filter/sort for fuzzing findings (`limit`, `offset`, `sort_key`, `sort_dir`, `f_<column>`, global `q`) with default 250 rows/page and max 2000.
  - Added cache-backed loading/parsing for `fozzy_summary_json` artifacts and zip-file index caching for `fozzy_results_zip` file browsing.
  - Added coordinator store method `CoordinatorStore.list_fozzy_summary_domains(...)` with parsed totals (`anomalies`, `reflections`, requests, groups) and zip availability metadata.
  - Added fixed-height, scrollable tables and search/filter/sort controls in the UI for findings and zipped result files.
  - Added navigation links to Fuzzing from dashboard/workers/database/crawl-progress/extractor pages.
  - Added tests for new renderer, server module wiring, store domain query parsing, and fuzzing row query behavior.
- Validation: `pytest -q` -> 97 passed.

- Extractor domain dropdown improvements:
  - `/api/coord/extractor-matches/domains` now includes `max_importance_score` per domain.
  - Domain ordering now prioritizes highest `max_importance_score` (desc), then `match_count` (desc), then domain name.
  - Added extractor zip stats scan helper to compute both match count and max importance score when summary/cache data is incomplete.
  - Extractor cache entries now persist `max_importance_score` alongside `match_count`.
  - `templates/extractor_matches.html.j2` now includes `hideZeroDomains` checkbox to hide/show zero-result domains in dropdown without reloading artifacts.
  - Dropdown labels now show both highest score and match count per domain.
- Validation: `pytest -q` -> 98 passed.

## 2026-04-17
- Fuzzing page now exposes full baseline vs fuzz comparison columns (request/response content, status/size/duration, and diff columns) from /api/coord/fuzzing rows.
- Added server-side enrichment that reads per-result JSON payloads from ozzy_results_zip and hydrates baseline/fuzz request/response previews + duration metrics for existing artifacts.
- Reflection findings now include reflected text directly in the note field (eflection_detected: <value>) for both live flattening and new fozzy summary outputs.


- 2026-04-17: Fuzzing table UX upgrade: added horizontal scrolling, per-column resize handles, and column hide/show menu with persisted prefs in Postgres via /api/coord/ui-preferences (GET/POST).

- 2026-04-17: Standardized sticky/frozen headers for scrollable web tables (fuzzing, extractor matches, database, workers, crawl progress, dashboard). Added scroll containers to pages that previously had plain tables so header rows stay visible during scrolling.

- 2026-04-17: Expanded Fozzy eflection_alert_ignore_exact defaults and normalization to suppress low-signal reflection values (e.g., 0/1, true/false, null-like/common tokens). Updated config/fozzy.json defaults to match.

- 2026-04-17: Reworked /fuzzing grid to operator layout (URL, Parameter, Payload, Code, Response/Diff actions, Size Diff, Time Diff, Notes placeholder). Added full-screen response modal (stacked headers/body) and side-by-side diff modal with synchronized scrolling and line-level highlighting.
- 2026-04-17: Fixed /api/coord/fuzzing/file lookup for zip members when rows contain basename-only result_file values. Endpoint now resolves by exact path first, then unique basename/suffix fallback to avoid false 'file not found in zip' failures from UI View Response/View Diff actions.

## 2026-04-17
- Implemented a full baseline-driven response analysis subsystem for Fozzy under fozzy_app/response_analysis:
  - normalizer.py (header/body normalization with deterministic volatile-token masking)
  - feature_extractor.py (metadata/header/body + JSON/HTML/XML/text feature extraction)
  - baseline_manager.py (endpoint/template baseline profiles)
  - diff_engine.py (structured semantic diff + similarity + auth/redirect drift)
  - detectors registry (status/header/semantic/stacktrace/error/reflection/structural-drift)
  - scorer.py, summarizer.py, clusterer.py, pipeline.py
- Wired subsystem into fozzy.py fuzz_group live path so every fuzzed response is analyzed and written to <root_domain>.fozzy.response_analysis.jsonl.
- Updated anomaly/reflection artifact payloads to include response_analysis and added analysis fields to summary entries (score, cluster_id, summary).
- Added run summary totals: analyzed_responses and interesting_responses, plus response_analysis_jsonl path in summary JSON.
- Added deterministic unit tests (tests/test_response_analysis_subsystem.py) covering noise suppression, 500+stacktrace, debug header appearance, security header disappearance, JSON->HTML drift, script-context reflection, redirect/login drift, repeated-cluster behavior, SQL errors, Spring Whitelabel detection.
- Validation: pytest -q tests/test_response_analysis_subsystem.py (10 passed); pytest -q tests/test_modular_spider_fuzz_core.py tests/test_reporting_and_store_helpers.py (29 passed).

## 2026-04-17

- Fixed fuzzing response modal header rendering fallback in `templates/fuzzing.html.j2`:
  - Header formatter now supports multiple payload shapes (`response_headers`, `headers`, object/list tuple forms) and only falls back to header-name-only output when values are truly unavailable.
- Why: View Response modal was showing header names without values for many artifact records.

- Enhanced worker control grid in `templates/worker_control.html.j2`:
  - Added persistent worker row selection across auto-refresh/page reload via cookie-backed selected worker IDs.
  - Added global search + per-column filters.
  - Added sortable columns with explicit sort indicators.
  - Selection state now survives data refresh and is reapplied to visible rows.
- Why: operators were repeatedly losing selections on refresh and could not efficiently locate/sort workers in large fleets.

- Standardized table/grid interaction layer across web pages via shared include `templates/_grid_controls.html.j2`:
  - Added reusable column resizing with per-column resize handles.
  - Added double-click auto-fit behavior on resize handles.
  - Added per-table column visibility modal (`Columns`) with persisted hidden/width preferences (DB-backed when available, localStorage fallback).
  - Added optional local sortable/filterable mode used on dashboard/crawl/database data tables.
- Why: make table UX consistent and satisfy operator requirement for resizable/sortable/filterable/configurable grids across pages.

- Fixed fuzzing table resize reliability in `templates/fuzzing.html.j2`:
  - Resize handle now suppresses click propagation (prevents accidental sort-trigger while resizing).
  - Added double-click auto-fit width for the target column.
  - Normalized sort indicator glyphs to ASCII (`^` / `v`) for cross-platform rendering consistency.
- Why: resize drag area was intermittently acting on wrong behavior and did not support requested auto-fit interaction.

## 2026-04-18

- Added two new operator web pages in template-backed server UI:
  - `/docker-status` shows live container status plus compose status snapshots.
  - `/view-logs` lists available docker/file log sources and loads latest 300 lines on demand.
- Added new coordinator-authenticated APIs in `server.py`:
  - `GET /api/coord/docker-status`
  - `GET /api/coord/log-sources`
  - `GET /api/coord/log-tail?source_id=...&lines=...`
- Navigation links across dashboard/worker/database/crawl/extractor/fuzzing pages now include Docker Status + View Logs.
- Added template render helpers in `reporting/server_pages.py` for the two new pages.
- Added/updated tests to cover template render and routing-module expectations for these new pages.
- Why: provide in-app operational visibility for container health and logs without leaving the coordinator web UI.

## 2026-04-18

- Fixed Docker Status / View Logs data scope: APIs now aggregate central server + worker VM data instead of local-host-only lookups.
- Added AWS SSM-backed worker VM collection in `server.py`:
  - worker container discovery via remote `docker compose ... ps --format json`
  - optional remote log-tail collection per container
  - source IDs for remote logs: `ssm:<instance_id>:docker:<container_name>`
- Updated `/api/coord/docker-status` to support `include_logs` and `log_lines` query controls and to return merged container rows (`all_containers`) across central and worker systems.
- Updated `/api/coord/log-sources` and `/api/coord/log-tail` to include and resolve remote worker docker sources.
- Updated Docker Status page UI to render host origin and per-container console output blocks.
- Why: operators needed fleet-wide visibility, not just the coordinator host's local containers/logs.
- Added `awscli` to `Dockerfile` base image packages so server-container fleet status/log APIs can execute SSM queries in central docker-compose deployments.

## 2026-04-18

- Replaced `command_cheatsheet.txt` with a structured "Useful Commands" runbook covering:
  - AWS cluster launch/provisioning
  - central/worker docker-compose deploy and status
  - AWS EC2/SSM worker visibility
  - central/worker docker logs
  - Postgres access and operational SQL checks
  - watch-style monitor commands (placed at end)
- Why: provide one operator-facing reference for day-to-day deployment and troubleshooting workflows.

## 2026-04-18

- Fixed Docker/compose command compatibility in server fleet observability APIs:
  - Compose status probes now try both `docker compose` and `docker-compose` command styles.
- Expanded log-source discovery and collection:
  - Added worker VM docker log sources via AWS SSM.
  - Added AWS EC2 console output sources (`ec2-console:<instance_id>`).
  - Added fallback central docker sources (`nightmare-coordinator-server`, `nightmare-postgres`) when discovery fails.
- Added new log APIs in `server.py`:
  - `GET /api/coord/log-events` (parsed, searchable, filterable, sortable events)
  - `GET /api/coord/log-download` (zip download for selected source)
  - Refactored `GET /api/coord/log-tail` to use unified source readers.
- Upgraded `templates/view_logs.html.j2` into a full log viewer grid with source selector, search/filter/sort controls, and zip download action.
- Upgraded `templates/docker_status.html.j2` to display worker VM count, AWS fleet section, and per-container console output.
- Added optional dedicated structured logging DB support:
  - New `logging_app/store.py` (`LogStore`) with `application_logs` schema and query/insert methods.
  - `server.py` now supports `log_database_url` (config/CLI/env) and writes server request logs to that store when configured.
  - Added config key `log_database_url` in `config/server.json`.
- Deployment updates:
  - `Dockerfile` now installs `docker.io` so containerized server can run docker CLI probes.
  - `deploy/docker-compose.central.yml` now mounts `/var/run/docker.sock` and passes fleet/log env selectors.
  - Added `deploy/docker-compose.log-store.yml` for a dedicated log Postgres stack.
  - Updated `deploy/full_deploy_command.sh` worker default from 6 to 5.
- Why: fix empty log-source experience and provide fleet-wide centralized log visibility with structured query capability and download/export.
## 2026-04-18

- Improved View Logs UX and persistence:
  - Added explicit loading-state indicator with status transitions (`loading`, `success`, `error`, `timeout`).
  - Added request timeouts for source/event fetches so UI can report timeout vs generic failure.
  - Set log-events grid defaults so the message/description columns are widest and easier to read.
  - Enabled persisted column reordering for column-only grids (including View Logs) and saved `column_order` with widths/hidden columns via `/api/coord/ui-preferences`.
- Improved log source performance:
  - Added short TTL cache for collected view-log sources to avoid repeated expensive discovery on every refresh.
  - Added `force_refresh` support to `/api/coord/log-sources`.
  - Reduced worker source discovery stall risk by using a shorter SSM timeout in source discovery path.
  - Replaced broad recursive file-log discovery over entire app root with bounded-depth targeted directories.
- Why: log source loading and layout usability were bottlenecks for operators; this keeps refreshes responsive and makes logs readable without repeated manual table setup.
## 2026-04-18

- Enforced mandatory dedicated logging/reporting DB for server startup.
  - Server now exits with error unless both `database_url` and `log_database_url` are set.
  - Server now exits with error if `log_database_url` appears to point to the same DB as `database_url`.
  - Log store initialization failure is now fatal instead of silently disabling structured logging.
- Updated central compose env contract so `LOG_DATABASE_URL` is required at deploy time.
- Why: user requirement that logging/reporting always run on a separate secondary database VM with no optional/off mode.
## 2026-04-18

- Added dedicated log DB VM provisioning script: `deploy/provision-log-db-aws.sh`.
  - Provisions a single EC2 VM for Postgres log/reporting storage.
  - Bootstraps Postgres in Docker via cloud-init.
  - Writes `LOG_DATABASE_URL` into `deploy/.env`.
  - Rebuilds central server container by default.
- Updated `deploy/bootstrap-central-auto.sh` to enforce and automate dedicated logging DB setup:
  - Reuses existing `LOG_DATABASE_URL` if present.
  - If missing, auto-provisions log DB VM before central compose startup.
  - Fails early when provisioning parameters are missing.
- Added `deploy/deploy-central-with-logdb.sh` as a one-command wrapper for central + worker + log DB provisioning flow.
- Updated `deploy/.env.example` with `LOG_DATABASE_URL` sample and updated `full_deploy_command.sh` default worker count to 5.
- Why: logging/reporting DB is now a mandatory separate VM and should be automatically provisioned in bootstrap workflows.
- Fixed log DB provision script compose target: use compose service name `server` (not container name `nightmare-coordinator-server`) when rebuilding central stack.
## 2026-04-18

- Added worker log bundle download endpoint: `GET /api/coord/worker-log-download?worker_id=<id>`.
  - Auth required.
  - Resolves worker log links and returns zipped log files (`<worker>.worker.logs.zip`).
  - Applies existing max download byte cap when building bundle.
- Updated Workers Control page Logs column:
  - Added `Download Logs` button per worker row.
  - Button opens worker log bundle endpoint and relies on coordinator token cookie auth.
- Why: operators requested one-click worker log download from the worker control center.
## 2026-04-18

- Updated full deploy scripts (`full_deploy_command.sh` and `deploy/full_deploy_command.sh`) to automatically register targets after central bootstrap.
- New behavior:
  - Source `deploy/.env` for coordinator URL/token.
  - Wait for coordinator readiness via `/api/coord/database-status`.
  - Run `python3 register_targets.py --targets-file targets.txt` automatically.
- Why: eliminate post-deploy manual target registration and ensure queue is populated immediately after deployment.
## 2026-04-18

- Fixed log visibility gaps in View Logs backend.
- Changes in `server.py`:
  - Added remote-EC2 docker container discovery via SSM + `docker ps` (not limited to worker compose services).
  - Expanded default EC2 log filter scope to include log DB VM name pattern (`nightmare-log-db*`).
  - Ensured central must-have docker log sources are always present in source list (`nightmare-coordinator-server`, `nightmare-postgres`, `nightmare-log-postgres`).
  - Enabled docker log reads for remote VM sources (`system=remote_vm`) using existing SSM log readers.
  - Updated `/api/coord/log-events` to fall back to live source parsing when structured log DB query returns zero rows, instead of returning empty immediately.
- Updated `deploy/docker-compose.central.yml` default `COORDINATOR_LOG_EC2_FILTER_VALUES` to include `nightmare-log-db*`.
- Why: operators were seeing empty/no-log behavior despite active fleet services and required visibility for coordinator + both DBs.
## 2026-04-18

- Added View Logs diagnostics subsystem.
- New API endpoint:
  - `GET /api/coord/log-source-diagnostics` (auth required)
  - Probes sources and returns per-source status (`ok/error`), line count, error text, and preview.
- Updated `templates/view_logs.html.j2`:
  - Added `Run Diagnostics` action.
  - Added diagnostics summary + table with per-source error visibility.
  - Added timeout-aware diagnostics status messaging.
- Log source coverage fixes:
  - Added remote EC2 docker discovery via SSM `docker ps` across matched EC2 instances (not just worker compose stack).
  - Included dedicated log DB VM pattern in default EC2 filter values.
  - Ensured central coordinator + central DB + log DB docker sources are always present in source list.
  - Allowed source reader to handle `system=remote_vm` docker sources.
  - `/api/coord/log-events` now falls back to live source reads when log DB query returns zero rows.
- Why: operators need immediate visibility into why a source is empty/failing and must always see coordinator + DB log sources.
## 2026-04-18

- Fixed bootstrap rerun failure on RPM-managed Python hosts (Amazon Linux):
  - `deploy/bootstrap-central-auto.sh` no longer attempts `pip --upgrade pip`.
  - Kept dependency install via pip but with `--disable-pip-version-check` and without self-uninstall path.
- Why: reruns were failing with `Cannot uninstall pip ... RECORD file not found` when pip came from rpm.
## 2026-04-18

- Hardened deploy script executability handling:
  - `deploy/bootstrap-central-auto.sh` now auto-applies `chmod +x` across `deploy/*.sh` at startup.
  - Added `ensure_executable()` guard used for `provision-log-db-aws.sh` and `provision-workers-aws.sh` so missing execute-bit no longer breaks reruns when file exists.
  - Added execute-bit self-heal in both full deploy wrappers (`deploy/full_deploy_command.sh`, `full_deploy_command.sh`) before invoking bootstrap.
- Why: EC2 reruns failed with `Missing executable .../provision-log-db-aws.sh` after checkouts/copies that dropped script mode bits.
## 2026-04-18

- Updated deploy privilege model in `deploy/bootstrap-central-auto.sh`:
  - Added `run_as_python_user()` so all Python/pip installs run as the invoking non-root user (`$SUDO_USER`) when script is started with sudo.
  - Kept docker/package-manager elevation behavior intact (`sudo` only where needed for docker/system operations).
  - Pip install path now uses user context + `--user` when running from sudo to avoid root-owned package installs and permission conflicts.
- Why: full deploy runs started with `sudo` were still executing pip in elevated context, causing failures and ownership issues.
## 2026-04-18

- Fixed deploy bootstrap dependency gap for AWS provisioning:
  - `deploy/bootstrap-central-auto.sh` now treats `aws` CLI as a required dependency in `install_deps_if_missing()`.
  - Added `awscli` package install for yum/dnf/apt flows.
- Why: full deploy path provisions worker/log DB EC2 resources and was failing with `Missing required command: aws` on central VM.
## 2026-04-18

- Fixed post-bootstrap target-registration privilege mismatch in full deploy wrappers:
  - `deploy/full_deploy_command.sh` and `full_deploy_command.sh` now run `register_targets.py` via `run_as_invoking_user`.
  - When invoked with sudo, this forces the Python registration step to run as `$SUDO_USER` instead of root.
- Why: `httpx` was installed in user site-packages, but wrapper executed `register_targets.py` as root and failed with `ModuleNotFoundError: No module named 'httpx'`.
## 2026-04-18

- Made full deploy flow idempotent/re-runnable for AWS fleet updates.
- `deploy/full_deploy_command.sh` now reconciles workers instead of always provisioning new ones:
  - Always updates/rebuilds central stack via `bootstrap-central-auto.sh` without DB reset.
  - Registers targets after coordinator readiness.
  - If no worker VMs exist (`tag:Name=nightmare-worker*`), provisions default worker count.
  - If workers exist, starts stopped workers if needed, then runs `client.py rollout` to rebuild/redeploy worker docker stacks with latest code.
- `full_deploy_command.sh` now delegates to `deploy/full_deploy_command.sh` to keep a single canonical implementation.
- Why: repeated deploy runs were creating drift/failures by re-provisioning every time; expected behavior is reconcile + preserve Postgres data unless explicitly reset.
## 2026-04-18

- Hardened central bootstrap DB credential recovery for reruns:
  - `deploy/bootstrap-central-auto.sh` now attempts to recover `POSTGRES_PASSWORD` from the existing `nightmare-postgres` container config when `.env` is missing the password.
  - Added `run_docker()` wrapper and switched existing-install detection to use docker access mode (direct vs sudo).
  - Existing-install failure message now explicitly notes container-recovery attempt.
- Why: full deploy reruns failed when Postgres data existed but `.env` lost `POSTGRES_PASSWORD`; rerunnable deploy should preserve DB access by recovering existing credentials when possible.
## 2026-04-18

- Fixed sudo-run deploy permission break during worker rollout (`Permission denied: deploy/.env`).
- `deploy/bootstrap-central-auto.sh` now restores ownership of generated files to invoking user (`$SUDO_USER`) after writes:
  - `deploy/.env`
  - `deploy/worker.env.generated`
  - `deploy/coordinator-host-env.sh`
- `deploy/provision-log-db-aws.sh` now restores invoking-user ownership when updating `.env` via `set_env_key`.
- Hardened env-file reads against permission errors:
  - `nightmare_shared/config.py::read_env_file` now returns `{}` on read exceptions.
  - `client.py::_read_env_file` now returns `{}` on read exceptions.
- Why: full deploy can run with sudo for docker/system actions, but post-bootstrap Python rollout executes as non-root user and must be able to read `deploy/.env`.
## 2026-04-18

- Fixed sudo deploy file-permission/ownership drift so non-root operator tools can read generated files.
- `deploy/bootstrap-central-auto.sh` now resolves invoking user identity and re-owns generated artifacts when launched via sudo:
  - `deploy/.env`, `deploy/worker.env.generated`, `deploy/coordinator-host-env.sh`, TLS cert/key, and appended `.bashrc` updates.
  - `.bashrc` update target now uses invoking user home directory instead of root home.
- `deploy/provision-log-db-aws.sh` now restores ownership of `.env` updates using invoking user + primary group (not username-assumed group).
- Why: sudo-run deploys created root-only files that broke non-root rollout/status commands and normal operator workflows.
## 2026-04-18

- Reduced log-viewer noise when coordinator runtime lacks docker CLI binaries.
- `server.py` updates:
  - `_compose_command_prefixes()` no longer injects fake fallback commands when binaries are missing.
  - view-log local docker source discovery now skips local docker sources if neither `docker` nor `docker-compose` is available.
  - compose status/log helpers now return explicit `"docker compose commands are not available in this runtime"` when unavailable.
  - mandatory local docker source injection (`nightmare-coordinator-server`, `nightmare-postgres`, `nightmare-log-postgres`) is now gated on actual local docker/compose command availability.
- Why: prevent repeated `[Errno 2] No such file or directory: 'docker'` synthetic error events in View Logs when running inside minimal app containers.

## 2026-04-18

- Removed noisy unauthorized UI-preference calls on pages without tokens.
- `templates/_grid_controls.html.j2` now calls `/api/coord/ui-preferences` only when an Authorization token is available; otherwise it uses localStorage-only preference load/save.
- Why: dashboard and other tokenless pages were emitting browser-console `401 Unauthorized` for preference fetches.

## 2026-04-18

- Hardened central bootstrap token reuse to prevent worker auth drift on reruns.
- `deploy/bootstrap-central-auto.sh` now attempts to recover `COORDINATOR_API_TOKEN` from the running `nightmare-coordinator-server` container when `.env` is missing/incomplete (and not `--force`).
- Why: this prevents unintended token regeneration that causes workers to loop on `401 Unauthorized` for claim/poll endpoints.

## 2026-04-18

- Standardized website table time rendering toward fixed EST (`UTC-5`) `HH:MM:SS` display for column data.
- `templates/crawl_progress.html.j2`:
  - changed `Last Activity` column label to `Last Activity (EST)`;
  - added deterministic client formatter to render `last_activity_at_utc` as `HH:MM:SS` in fixed UTC-5.
- `templates/view_logs.html.j2` + `server.py`:
  - added diagnostics table `Time (EST)` column;
  - diagnostics rows now include `checked_at_est_time` generated server-side via shared EST time formatter.
- `server.py`:
  - added `_format_est_time(dt)` helper to keep EST time-only formatting consistent with no fractional seconds.
- Why: enforce a single operator-friendly time format in table columns (`HH:MM:SS`, EST/UTC-5) and remove mixed ISO/date-time outputs in grids.

## 2026-04-18

- Prevented unwanted extra log-DB VM provisioning on reruns when infra already exists.
- `deploy/bootstrap-central-auto.sh` now recovers missing `LOG_DATABASE_URL` from existing `nightmare-coordinator-server` container env before deciding to provision log DB.
- `deploy/provision-log-db-aws.sh` now:
  - recovers `LOG_DATABASE_URL` from existing coordinator container env and writes it to `.env` when missing;
  - detects existing log DB instances by tag (`${INSTANCE_NAME_PREFIX}*`) and refuses to provision duplicates when URL is missing.
- Why: reruns were trying to create another VM and failing with `VcpuLimitExceeded` even though fleet was already up.
## 2026-04-18

- Fixed worker 401 unauthorized loops after redeploy by syncing coordinator credentials during rollout.
- `client.py` `_run_ssm_rollout(...)` now accepts and propagates coordinator connection settings:
  - `coordinator_base_url`
  - `api_token`
  - `coordinator_insecure_tls`
- Rollout SSM script now rewrites worker `deploy/.env` with current coordinator URL/token/TLS mode before `docker compose up -d --build`.
- `deploy/full_deploy_command.sh` now invokes `client.py rollout` with explicit coordinator URL/token and `--insecure-tls` when configured.
- Added regression test `test_client_rollout_updates_worker_env_before_compose` to validate worker `.env` rewrite is included in SSM rollout payload.
- Validation: `python -m pytest -q tests/test_client_and_cli_unit.py` -> 12 passed.
- Why: workers kept stale tokens across central token changes and repeatedly received `401 unauthorized` on coordinator endpoints.
## 2026-04-18

- Fixed coordinator/local DB log-source failures when `docker` CLI is unavailable in server runtime (`[Errno 2] No such file or directory: 'docker'`).
- `server.py` local docker log readers now fallback to compose-service logs:
  - Added container->compose service candidate mapping for core services (`nightmare-coordinator-server`, `nightmare-postgres`, `nightmare-log-postgres`).
  - Added compose log execution helper that tries both `docker compose` and `docker-compose`, with/without `--env-file`.
  - Log read paths (`_read_docker_log_tail`, `_read_docker_log_full`) now try `docker logs` first, then compose fallback candidates.
- Expanded compose spec discovery to include `deploy/docker-compose.log-store.yml` so log-store service fallback resolves.
- Validation: `python -m pytest -q tests/test_client_and_cli_unit.py tests/test_reporting_and_store_helpers.py` -> 38 passed.
- Why: View Logs entries for coordinator + DB containers should remain readable even when only compose client is present.

## 2026-04-19

- Updated AWS EC2 provisioning defaults and storage sizing in deploy scripts.
- Instance type default is now `m7i-flex.large` across:
  - `deploy/provision-workers-aws.sh`
  - `deploy/provision-log-db-aws.sh`
  - `deploy/bootstrap-central-auto.sh`
  - `deploy/deploy-central-with-logdb.sh`
  - `deploy/full_deploy_command.sh`
- Added explicit EC2 root EBS sizing at launch time:
  - Workers: 50 GB (`gp3`, delete on termination)
  - Dedicated log DB VM: 100 GB (`gp3`, delete on termination)
- Why: enforce consistent compute class and minimum disk capacity for worker and DB fleet provisioning.
- Hardened `deploy/full_deploy_command.sh` coordinator readiness gate.
  - Added explicit readiness success flag for `/api/coord/database-status` wait loop.
  - Script now exits before target registration if API is still unreachable after timeout.
  - Added best-effort compose diagnostics (`docker compose ps` + `logs --tail 120 server postgres`) on failure.
- Why: previous flow continued to `register_targets.py` even when readiness never succeeded, causing noisy late `ConnectError` crashes.
- Updated `deploy/bootstrap-central-auto.sh` to improve Ubuntu compatibility.
  - Added distro-aware package-manager detection (`detect_package_manager`) using `/etc/os-release`.
  - Ubuntu/Debian now explicitly prefer `apt-get` package flow over yum/dnf probing.
  - Added apt fallback path when `docker-compose-plugin` package is unavailable.
- Reduced default worker VM counts for debugging:
  - `deploy/deploy-central-with-logdb.sh`: `AUTO_PROVISION_WORKERS` default `5 -> 2`.
  - `deploy/full_deploy_command.sh`: `DEFAULT_WORKER_COUNT` default `5 -> 2`.
- Why: ensure bootstrap succeeds on Ubuntu hosts and keep worker fleet smaller while diagnosing deployment issues.
- Set executable Git mode on tracked shell scripts so Linux deploy hosts do not require manual `chmod +x` after pull.
- Updated file modes from `100644` to `100755` for root/deploy helper scripts (for example bootstrap/deploy wrappers, docker entrypoint, diagnostics helpers).
- Why: prevent recurring deploy friction from non-executable scripts after code updates.
- Fixed Ubuntu bootstrap dependency failure in `deploy/bootstrap-central-auto.sh`.
  - Apt fallback no longer requires `awscli` package availability.
  - Added `install_aws_cli_if_missing()` to install AWS CLI v2 from the official AWS installer when `aws` is missing after apt installs.
  - Kept compose fallback behavior so missing `docker-compose-plugin` in apt repos does not block bootstrap.
- Why: Ubuntu hosts with limited/default repos failed bootstrap due unavailable `awscli` and `docker-compose-plugin` packages.
- Fixed Ubuntu PEP 668 bootstrap failure in `deploy/bootstrap-central-auto.sh`.
  - Added conditional pip flag detection and automatic `--break-system-packages` usage for dependency installs when supported.
  - Preserved existing `--user` install behavior and distro-managed pip safety approach.
- Why: Ubuntu 24+ marked Python as externally managed and blocked previous pip install path.

## 2026-04-19

- Added cache-first initial page hydration across all coordinator web UI pages, while preserving live API refresh behavior.
- Updated templates:
  - `templates/server_dashboard.html.j2`
  - `templates/worker_control.html.j2`
  - `templates/database_status.html.j2`
  - `templates/crawl_progress.html.j2`
  - `templates/docker_status.html.j2`
  - `templates/extractor_matches.html.j2`
  - `templates/fuzzing.html.j2`
  - `templates/view_logs.html.j2`
- Behavior changes:
  - Pages now attempt to render recent local cache (`localStorage`, TTL-bounded) before waiting on network calls.
  - Successful live responses overwrite cache and refresh the UI as before.
  - If live fetch fails after cached render, pages retain cached data and surface a refresh-failed message.
  - Extractor and fuzzing table caches are query-scoped (filters/sort/paging/domain) to avoid mismatched stale renders.
- Validation: `python -m pytest -q tests/test_reporting_and_store_helpers.py tests/test_refactor_modules.py tests/test_server_auth_cookie.py tests/test_server_disconnect_handling.py` -> 47 passed.
- Why: improve first-paint responsiveness and keep operator pages useful during transient API latency or errors.

## 2026-04-19

- Updated `register_targets.py` to auto-load coordinator connection defaults from `deploy/.env`.
  - `--server-base-url` now defaults to `COORDINATOR_BASE_URL` (from env / `deploy/.env`).
  - `--api-token` now defaults to `COORDINATOR_API_TOKEN` (from env / `deploy/.env`).
  - Script now raises a clear error when base URL is still missing after env/default resolution.
  - Base URL is normalized via shared config helper for consistency with other CLIs.
- Added tests in `tests/test_client_and_cli_unit.py`:
  - env-default loading for `register_targets.parse_args([])`
  - missing base URL validation path in `register_targets.main()`
- Validation: `python -m pytest -q tests/test_client_and_cli_unit.py` -> 14 passed.
- Why: remove repetitive manual argument passing during deploy workflows and align target-registration behavior with existing deploy env conventions.

## 2026-04-19

- Expanded worker/coordinator observability to emit detailed structured logs (JSON) for both workflow actions and coordinator API traffic.
- Updated files:
  - `coordinator.py`
  - `coordinator_app/runtime.py`
  - `http_client.py`
  - `nightmare_shared/logging_utils.py`
  - `tests/test_http_client_unit.py`
  - `tests/test_runtime_unit.py`
- Worker runtime changes:
  - Replaced worker-loop `print(...)` diagnostics with severity-aware structured logger events (`info`, `warning`, `error`) for claim/start/complete/subprocess/artifact/command lifecycle.
  - Added explicit error logging where exceptions were previously swallowed (`worker command completion`, `fleet settings fetch`, `session restore`, stage enqueue failures, heartbeat tick failures, session upload failures).
- HTTP detail logging changes:
  - `request_json(...)` now supports structured request/response logging with method/url/headers/status/elapsed and payload/body fields.
  - Coordinator client now enables detailed HTTP logs by default via env-driven controls:
    - `COORDINATOR_HTTP_LOG_DETAILS` (default true)
    - `COORDINATOR_HTTP_LOG_PAYLOADS` (default true)
    - `COORDINATOR_HTTP_LOG_MAX_CHARS` (default 0 => no truncation)
    - `COORDINATOR_HTTP_REDACT_AUTH_HEADER` (default false)
  - Added optional auth-header redaction and response-body truncation controls for operational tuning.
- Logging pipeline change:
  - Suppressed default `httpx/httpcore` info chatter in `configure_logging()` so container logs are dominated by project-structured JSON entries instead of duplicate one-line transport summaries.
- Validation:
  - `python -m pytest -q tests/test_http_client_unit.py tests/test_runtime_unit.py tests/test_client_and_cli_unit.py` -> 31 passed.
  - `py_compile` check on changed runtime files passed.
- Why: operator needed substantially richer worker diagnostics (full request/response context + clear worker action state + explicit error severity) and direct compatibility with View Logs ingestion/search.
- Removed server-side byte truncation paths for log viewing/download:
  - Tail readers now collect complete lines from file end without cutting entries mid-line.
  - `/api/coord/log-download` and `/api/coord/worker-log-download` now package full source content.
  - Worker VM docker full-read command no longer uses `head -c`.
- Updated UI truncation behavior to presentation-only:
  - `templates/view_logs.html.j2` clamps long text cells with fixed-height scrollable controls and adds double-click copy for full cell text.
  - `templates/server_dashboard.html.j2` no longer slices log tails in JS.
- Validation: `python -m py_compile server.py` and `python -m pytest -q tests/test_refactor_modules.py tests/test_reporting_and_store_helpers.py tests/test_server_auth_cookie.py tests/test_server_disconnect_handling.py` (47 passed).
- Why: preserve full log fidelity in API/download payloads while preventing oversized UI controls from expanding row height.
- Fixed immediate worker crawl startup failure caused by malformed JSON in `config/page_existence_criteria_config.json` (`"Page Not Found""no such page"` typo).
- Hardened `nightmare.py` page-existence criteria loading:
  - Invalid criteria config now logs a warning and falls back to defaults instead of aborting the run.
  - Applied both in startup config read path and lazy-load helper.
- Hardened subprocess startup diagnostics in `coordinator_app/runtime.py::run_subprocess(...)`:
  - Writes startup failure details into per-domain coordinator logs.
  - Adds automatic fallback from `python`/`python3` command names to `sys.executable` when command lookup fails.
- Validation:
  - `python -m json.tool config/page_existence_criteria_config.json`
  - `python -m py_compile nightmare.py coordinator_app/runtime.py`
  - `python nightmare.py --status --config config/nightmare.json`
  - `python -m pytest -q tests/test_runtime_unit.py tests/test_client_and_cli_unit.py tests/test_reporting_and_store_helpers.py tests/test_refactor_modules.py` -> 62 passed.
- Why: workers were claiming targets then failing before artifacts were produced; malformed criteria config was causing fatal startup exceptions.
- Updated worker provisioning defaults in `deploy/provision-workers-aws.sh`:
  - Script now auto-loads required provisioning params from `deploy/.env`/`deploy/worker.env.generated` when CLI flags are omitted.
  - Added env-backed resolution for `AWS_AMI_ID`, `AWS_SUBNET_ID`, `AWS_SECURITY_GROUP_IDS`, `REPO_URL`, plus related defaults (`AWS_INSTANCE_TYPE`, `AWS_KEY_NAME`, `AWS_IAM_INSTANCE_PROFILE`, `AWS_REGION`, `REPO_BRANCH`, `AUTO_PROVISION_WORKERS`).
  - CLI flags remain highest priority.
- Updated target registration to full-replace semantics:
  - `register_targets.py` now always posts `replace_existing=true`.
  - `/api/coord/register-targets` accepts `replace_existing` and passes through to store.
  - `CoordinatorStore.register_targets(..., replace_existing=True)` truncates `coordinator_targets` before insert.
- Added test coverage for register-target replacement payload in `tests/test_client_and_cli_unit.py`.
- Validation: `python -m py_compile register_targets.py server.py server_app/store.py`; `python -m pytest -q tests/test_client_and_cli_unit.py tests/test_server_auth_cookie.py tests/test_reporting_and_store_helpers.py tests/test_refactor_modules.py` -> 60 passed.
- Unified column configuration UI across web pages by removing custom fuzzing column modal code and reusing shared `templates/_grid_controls.html.j2`.
- Updated shared grid control behavior so column reordering is available in all modes (not only `columnsOnly`).
- `templates/fuzzing.html.j2` now uses `NightmareGridControls.enhance(...)` with shared hide/show + reorder controls and shared preference persistence.
- Updated template assertions in:
  - `tests/test_refactor_modules.py`
  - `tests/test_reporting_and_store_helpers.py`
  to validate shared grid-controls markers instead of legacy `columnToggleBtn`.
- Validation:
  - `python -m pytest -q tests/test_refactor_modules.py tests/test_reporting_and_store_helpers.py` -> 39 passed.
  - `python -m pytest -q tests/test_server_auth_cookie.py tests/test_client_and_cli_unit.py` -> 21 passed.
- Why: ensure one column-config code path and consistent hide/show/reorder behavior across all pages.

## 2026-04-20

- Improved coordinator subprocess failure reporting so non-zero exits include real error details instead of only `"<stage> exit code N"`.
- Added `summarize_subprocess_failure(...)` in `coordinator_app/runtime.py`:
  - reads tail of per-domain subprocess log file,
  - extracts the most relevant error/exception line,
  - returns a concise message like `nightmare exit code 1; NameError: ...`.
- Updated coordinator stage loops in `coordinator.py` (`nightmare`, `fozzy`, `auth0r`, `extractor`) to use the new helper for non-zero subprocess exits.
- Added unit coverage in `tests/test_runtime_unit.py` for:
  - extracting an exception line from log content,
  - fallback behavior when no log file exists.
- Validation: `pytest tests/test_runtime_unit.py -q` -> 11 passed.
- Why: operator logs and `/api/coord/complete` payloads were showing only exit codes, which hid root-cause exceptions that were already present in stage log artifacts.

## 2026-04-20

- Hardened `deploy/full_deploy_command.sh` Docker diagnostics path for fresh hosts where docker-group access is not active in the current shell.
- `run_compose` now executes via detected access mode (`invoking_user`, `current_user`, or `sudo -n`) instead of always forcing invoking-user context.
- Added daemon-access probing (`detect_docker_access_mode`) so readiness-failure diagnostics can still fetch compose `ps` and service logs when direct socket access is denied.
- Updated compose command resolution to check both invoking-user and current-user contexts.
- Why: full deploy readiness failures were masking root cause with Docker socket permission errors during diagnostic logging (`permission denied ... /var/run/docker.sock`).

## 2026-04-21

- Fixed central server startup crash on fresh deploys caused by undefined `auth0r_store` in `server.py` main startup path.
- Root cause:
  - `_prepare_server(...)` always assigned `srv.auth0r_store = auth0r_store`,
  - but `auth0r_store` was never initialized in `main()`, causing `NameError` and container restart loop.
- Change in `server.py`:
  - initialize `auth0r_store: Auth0rProfileStore | None = None` before server creation,
  - best-effort instantiate `Auth0rProfileStore(database_url)` with explicit stderr warning on failure,
  - keep server startup alive even if auth0r store init fails (auth0r APIs return existing 503 unavailable response).
- Validation:
  - `python -m py_compile server.py`
  - `pytest -q tests/test_server_auth_cookie.py tests/test_refactor_modules.py` -> 20 passed.
- Why: unblock central compose/server startup so readiness check can pass and full deploy can proceed.

## 2026-04-21

- Fixed auth0r page load failure (`ReferenceError: readCookie is not defined`) in `templates/auth0r.html.j2`.
- Root cause: page script called `readCookie(TOKEN_COOKIE_NAME)` in `authHeaders()` but did not define `TOKEN_COOKIE_NAME` or cookie helper functions.
- Added missing token-cookie helpers (`readCookie`, `writeCookie`, `deleteCookie`) and `TOKEN_COOKIE_NAME` constant, aligned with other coordinator UI pages.
- Updated `authHeaders()` to persist/remove token cookie consistently and added initial token hydration (`#token` input reads cookie on page load).
- Validation:
  - `pytest -q tests/test_refactor_modules.py` -> 14 passed.
  - `pytest -q tests/test_refactor_modules.py tests/test_reporting_and_store_helpers.py` had 1 pre-existing unrelated failure in discovered-target store SQL expectation.
- Why: auth0r UI could not execute its first API call due to missing shared auth helper definitions.

## 2026-04-21

- Restored Errors page wiring end-to-end.
- Root cause: `templates/errors.html.j2` and navbar link existed, but `server.py` had no `/errors` page route and no `/api/errors` handler, so page load failed/returned not found.
- Added `render_errors_html()` in `reporting/server_pages.py` and imported it into `server.py`.
- Added GET route `/errors` -> `render_errors_html()`.
- Added GET API `/api/errors` (coordinator-auth protected) backed by `LogStore.query_error_events(...)` with filters: `search`, `machine`, `program_name`, `component_name`, `exception_type`, plus `limit/offset/sort_dir`.
- Added POST API `/api/coord/errors/ingest` (coordinator-auth protected) to persist one or many structured error events via `LogStore.insert_events(...)`.
- Validation:
  - `python -m py_compile server.py reporting/server_pages.py`
  - `pytest -q tests/test_server_auth_cookie.py tests/test_refactor_modules.py` -> 20 passed.
- Why: expose the existing errors UI and script-based error ingestion path as first-class server endpoints.

- Fixed discovered files page artifact grid rendering on coordinator UI.
- Root causes:
  - API/template collection key mismatch (`/api/coord/discovered-files` + `/api/coord/high-value-files` returned `rows`, while template consumed `files`).
  - Row field mismatch (`updated_at_utc`/`content_size_bytes` expected by template, but store methods returned `discovered_at_utc`/`file_size` only).
- Changes:
  - `server_app/store.py`: `list_discovered_files` and `list_high_value_files` now emit template-compatible keys (`updated_at_utc`, `captured_at_utc`, `content_size_bytes`) while preserving legacy aliases.
  - `server.py`: both discovered/high-value APIs now return both `rows` and `files` keys for compatibility.
  - `templates/discovered_files.html.j2`: added client-side compatibility helpers (`getRows`, timestamp/size normalization) so the UI tolerates either payload/row shape.
  - `tests/test_reporting_and_store_helpers.py`: added focused regression tests for discovered-files template contract and both store list methods.
- Validation:
  - `python -m py_compile server.py server_app/store.py`
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "render_discovered_files_html or list_discovered_files_returns_template_compatible_keys or list_high_value_files_returns_template_compatible_keys"` -> 3 passed
  - `pytest -q tests/test_refactor_modules.py tests/test_server_auth_cookie.py` -> 20 passed

- Fixed auth0r overview SQL crash that prevented `/auth0r` page load.
- Root cause: `CoordinatorStore.auth0r_overview` used one `CASE` expression in `ORDER BY` mixing `root_domain` (text) and `saved_at_utc` (timestamp), which Postgres rejects with `CASE types timestamp without time zone and text cannot be matched`.
- Changes:
  - `server_app/store.py`: replaced mixed-type `CASE` sort with two type-safe `CASE` expressions (text-only branch + timestamp-only branch) and updated query param binding accordingly.
  - `tests/test_reporting_and_store_helpers.py`: added regression test `test_auth0r_overview_completed_only_uses_type_safe_ordering`.
- Validation:
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "auth0r_overview_completed_only_uses_type_safe_ordering"` -> 1 passed
  - `python -m py_compile server.py server_app/store.py`
  - `pytest -q tests/test_refactor_modules.py tests/test_server_auth_cookie.py` -> 20 passed

- Restored extractor pattern-management UI behavior on `/extractor-matches`.
- Root cause: template rendered Pattern Management controls but had no client-side handlers wired for load/add/save/remove, so buttons were inert and no patterns were shown.
- Additional reliability issue: domain list load failures were silently swallowed in `refreshDomainsAndMatches`, masking API failures and making the page appear empty.
- Changes:
  - `templates/extractor_matches.html.j2`:
    - Added `apiPost` helper.
    - Added full pattern table state helpers (`normalizePatternRow`, `readPatternRowsFromDom`, `renderPatternRows`).
    - Added `loadPatterns()` and `savePatterns()` wired to `/api/coord/extractor-patterns` and `/api/coord/extractor-patterns/save`.
    - Added Add/Remove row interactions and auto-load patterns on page init.
    - Improved domain load error handling (show explicit message instead of silent catch).
    - Changed "Hide 0-result domains" default to unchecked to avoid hiding all domains when match counts are zero/unavailable.
  - Tests:
    - `tests/test_refactor_modules.py`: asserts extractor patterns endpoints and pattern controls are present in rendered template.
    - `tests/test_reporting_and_store_helpers.py`: asserts extractor pattern endpoints are present in extractor page HTML.
- Validation:
  - `pytest -q tests/test_refactor_modules.py` -> 14 passed
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "render_extractor_matches_html_contains_expected_heading"` -> 1 passed
  - `pytest -q tests/test_server_auth_cookie.py` -> 6 passed

- Hardened auth0r overview ordering to remove `CASE`-based mixed-type sort paths entirely.
- Root cause follow-up: even after initial typed-CASE mitigation, some deployed instances still surfaced the old PostgreSQL error path (`CASE types timestamp without time zone and text cannot be matched`) during `/api/coord/auth0r/overview`.
- Changes:
  - `server_app/store.py` `auth0r_overview(...)` now chooses one of two static `ORDER BY` clauses based on `completed_only`:
    - completed-only: `ORDER BY root_domain ASC`
    - default: `ORDER BY COALESCE(saved_at_utc, NOW()) DESC, root_domain ASC`
  - Removed dynamic type-mixing `CASE` ordering and simplified SQL bind params.
  - Updated regression test in `tests/test_reporting_and_store_helpers.py` to assert branch-based ordering and param shape.
- Validation:
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "auth0r_overview_completed_only_uses_type_safe_ordering"` -> 1 passed
  - `python -m py_compile server_app/store.py server.py`
  - `pytest -q tests/test_refactor_modules.py tests/test_server_auth_cookie.py` -> 20 passed

- Fixed discovered-target sitemap contract drift and completed regression validation cleanup.
- Root cause: discovered-target sitemap API/UI contract had diverged (`rows` vs `sitemap.pages`, plus field-name differences), and a follow-up regression test fixture was left with an unreachable SQL branch after a bad merge.
- Changes:
  - `server_app/store.py`: normalized sitemap output from `get_discovered_target_sitemap(...)` to return `{root_domain,start_url,page_count,pages}` with canonical fields and compatibility aliases.
  - `server.py`: `/api/coord/discovered-target-sitemap` now returns `sitemap` plus compatibility aliases (`rows`, `pages`).
  - `templates/discovered_targets.html.j2`: sitemap renderer now accepts either payload shape and legacy/new row keys.
  - `tests/test_reporting_and_store_helpers.py`: repaired `test_list_discovered_target_domains_uses_session_inventory_counts` fake SQL branch so `load_session` queries are matched and fixture shape is valid.
- Validation:
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "list_discovered_target_domains_uses_session_inventory_counts"` -> 1 passed
  - `pytest -q tests/test_reporting_and_store_helpers.py` -> 32 passed
  - `pytest -q tests/test_refactor_modules.py tests/test_server_auth_cookie.py` -> 20 passed

- Tightened URL existence rules to reduce cross-site false positives from wordlist/guess seeds (e.g. `/manager/deploy?path=...`).
- Root cause: `file_path_wordlist` includes `/manager/deploy?path=foo`, and existence logic treated most non-404 statuses as valid (`status not in {404,410}`), so generic 3xx/4xx catch-all responses were promoted as existing URLs.
- Changes:
  - `nightmare.py`:
    - `derive_exists_for_requested_url(...)` now:
      - requires 2xx for guess-only discoveries (`file_path_wordlist`/`guessed_url` without stronger non-guess evidence),
      - requires 2xx/3xx for non-guess URLs,
      - no longer treats arbitrary 4xx/5xx as existing.
    - `probe_url_existence(...)` now marks `exists_confirmed=True` only for HTTP 2xx/3xx and emits explicit note for non-success statuses.
    - `write_wordlist_guessed_paths_index(...)` now counts wordlist "hits" only for 2xx (instead of any non-404/410), excluding soft-404 as before.
- Validation:
  - `python -m py_compile nightmare.py`
  - `pytest -q tests/test_refactor_modules.py tests/test_server_auth_cookie.py tests/test_reporting_and_store_helpers.py` -> 52 passed
- Fixed central deploy readiness/registration flow for EC2 self-reachability edge case.
- Root cause: `deploy/full_deploy_command.sh` required `COORDINATOR_BASE_URL` to be reachable from the same VM; on some EC2 setups (public endpoint hairpin path), self-calls return HTTP `000` even while the container is healthy.
- Changes:
  - Added local reachability probing in `deploy/full_deploy_command.sh` (`COORDINATOR_BASE_URL`, then localhost/127.0.0.1 over HTTPS/HTTP).
  - Readiness now succeeds when the coordinator is reachable on a local listener, not only via the externally advertised base URL.
  - `register_targets.py` and `client.py rollout` calls now use the detected locally reachable coordinator URL for host-local API calls while preserving `COORDINATOR_BASE_URL` for worker provisioning/env generation.
- Validation:
  - Script logic reviewed end-to-end against compose/server startup flow.
  - `bash -n deploy/full_deploy_command.sh` could not be executed in this Windows environment due shell runtime timeout; syntax therefore remains runtime-verified on Linux deploy host.
- Hardened coordinator startup against indefinite DB-connect hangs that looked like API unreachability.
- Root cause refinement: when LOG_DATABASE_URL (or coordinator DB URL) is unreachable at TCP/connect level, psycopg default connect behavior could block server initialization before HTTP listeners bind. Deploy readiness then reports HTTP code `none/000` even though container appears running.
- Changes:
  - `logging_app/store.py`: added bounded DB connect timeout (`LOG_DB_CONNECT_TIMEOUT_SECONDS`, fallback `DB_CONNECT_TIMEOUT_SECONDS`, default 8s).
  - `server_app/store.py`: added bounded DB connect timeout (`COORDINATOR_DB_CONNECT_TIMEOUT_SECONDS`, fallback `DB_CONNECT_TIMEOUT_SECONDS`, default 8s).
  - `deploy/full_deploy_command.sh`: readiness probes now use curl connect/max-time bounds and print per-candidate URL diagnostics; server/postgres logs are emitted separately for clearer failure attribution.
- Validation:
  - `python -m py_compile logging_app/store.py server_app/store.py`
- Fixed bootstrap behavior for deleted log-DB VM with stale `LOG_DATABASE_URL` still present.
- Root cause: `bootstrap-central-auto.sh` reused non-empty `LOG_DATABASE_URL` without reachability validation, so central server repeatedly restarted on log-store connect timeout and readiness stayed at HTTP 000.
- Changes:
  - `deploy/bootstrap-central-auto.sh` now validates `LOG_DATABASE_URL` TCP reachability before reuse.
  - If unreachable, it clears the stale URL, marks re-provision intent, and invokes `provision-log-db-aws.sh` to create a replacement VM.
  - Added `--force-provision` flow in `deploy/provision-log-db-aws.sh` to allow replacement provisioning even when stale URL or legacy tagged instances exist.
- Validation: static checks + source-level verification of new flow; runtime deploy validation pending on EC2.
- Investigated soft-404 false-positive path using Samsung `.bash_history` validation target.
- Findings:
  - Baseline-learning probes for soft-404 profiles were using HEAD-first flow; on domains that return HEAD 403/empty-body, learned fingerprints lacked useful body content and could not match catch-all HTML pages.
  - Heuristic phrase/regex soft-404 detection in `detect_soft_not_found_response(...)` only fired for "small" 200 responses, so large branded soft-404 pages were skipped even when phrases matched.
- Changes:
  - `nightmare.py` `probe_url_existence(...)` now supports `head_first` and automatically uses GET-only when comparing against negative profile (body-required classification).
  - `build_soft_404_negative_profile(...)` now calls probe with `head_first=False` so learned baselines are body-backed GET responses.
  - `detect_soft_not_found_response(...)` now allows phrase/regex matches on large 200 bodies (reason string indicates large-body match).
- Validation:
  - `python -m py_compile nightmare.py`
  - Reproduced baseline learning now capturing GET 404 body for Samsung test host.

## 2026-04-21

- Fixed Auth0r profile-save crash in `auth0r/profile_store.py`:
  - `upsert_profile(...)` now defines and uses `normalized_profile_id` before SQL execution.
- Why: saving a profile from `/auth0r` could raise a runtime `NameError` and surface in browser as a generic `failed to fetch` alert instead of a normal API error payload.

## 2026-04-21

- Updated `templates/auth0r.html.j2` to pre-populate Auth0r forms with concrete defaults on page load.
  - Replay policy textarea now renders full default JSON shape (`read_only_mode`, `verify_state_changes`, method/path allow/deny arrays).
  - Identity JSON fields (`login_extra_fields`, `custom_headers`, `imported_cookies`) now render normalized default JSON values instead of empty placeholders.
  - Added client-side normalization helpers so loading blank/partial records still shows stable expected JSON structures.
- Why: make first-time profile/identity creation self-documenting and reduce malformed JSON edits.

## 2026-04-21

- Added workflow-driven stage scheduling for coordinator workers.
- Changes:
  - `coordinator.py`: added workflow config loading from `workflows/coordinator.workflow.json`, periodic scheduler loop, data-readiness checks, and stage-parameter overrides per tool.
  - `coordinator_app/runtime.py`: added workflow scheduler config fields, `/api/coord/workflow-snapshot` client call, and detailed stage enqueue API helper.
  - `server_app/store.py`: replaced unsafe enqueue semantics with `schedule_stage(...)` decision logic that preserves `running/pending/completed` rows and only retries failed rows when explicitly allowed.
  - `server.py`: added `GET /api/coord/workflow-snapshot` and enhanced `POST /api/coord/stage/enqueue` payload/response contract.
  - `workflows/coordinator.workflow.json`: new default workflow definition using plugin-style metadata (`handler`, `config_schema`) and prerequisites/retry/parameters for `auth0r`, `fozzy`, and `extractor`.
- Why:
  - Removes hard dependency on sequential success chaining and lets stages start whenever required artifacts are present.
  - Prevents accidental task clobber/re-enqueue while still allowing controlled retries for failed stages.
- Validation:
  - `pytest -q tests/test_config_utils.py tests/test_runtime_unit.py tests/test_reporting_and_store_helpers.py` (56 passed).

## 2026-04-22

- Implemented workflow-driven, queue-safe plugin orchestration foundation across coordinator/store/API/runtime.
- `server_app/store.py`:
  - Stage-task schema now includes workflow/progress/checkpoint fields and workflow-scoped primary key.
  - Added workflow-aware claim-next behavior, workflow-aware heartbeat/completion, explicit progress update API method, and manual stage-task reset API method.
  - Workflow snapshot now returns `plugin_tasks` grouped by workflow while preserving legacy `stage_tasks` compatibility for default workflow views.
- `server.py`:
  - Extended stage APIs to accept `workflow_id`, checkpoint/progress payloads, and resume metadata.
  - Added `POST /api/coord/stage/claim-next`, `POST /api/coord/stage/progress`, and `POST /api/coord/stage/reset`.
- `coordinator_app/runtime.py`:
  - Added workflow-aware client methods for claim-next/progress/reset and propagated workflow/progress fields through existing stage methods.
  - Added unified plugin worker config support (`plugin_workers`, `plugin_allowlist`) with compatibility fallback derivation from legacy per-tool worker counts.
- `coordinator.py`:
  - Workflow loader now supports `workflow_id` + plugin entries (`plugins`/`stages`/`steps` compatibility).
  - Scheduler now evaluates plugin preconditions including artifact gates, plugin completion dependencies, and target-status requirements.
  - Added unified plugin worker loop and plugin dispatch execution path (`fozzy`, `extractor`, `auth0r`, plus nightmare artifact-gate plugins).
  - Existing per-tool stage loops retained as compatibility fallback only when unified plugin workers are disabled.
- `workflows/coordinator.workflow.json`:
  - Reworked to workflow v2 shape with explicit `workflow_id`, fine-grained nightmare plugin chain, plugin dependencies/preconditions, and manual-rerun-safe defaults.
- `config/coordinator*.json`:
  - Added workflow scheduler/plugin worker settings and allowlist support.
- Validation:
  - `python -m py_compile coordinator.py coordinator_app/runtime.py nightmare_shared/config.py server.py server_app/store.py`
  - `pytest -q tests/test_runtime_unit.py tests/test_config_utils.py`
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "workflow or stage or crawl_progress"`
  - `pytest -q tests/test_refactor_modules.py tests/test_server_auth_cookie.py`

## 2026-04-22

- Added dedicated workflow operations UI page for monitor + control:
  - New page/template: `templates/workflows.html.j2`.
  - New route: `GET /workflows`.
  - Navbar updated with `Workflows` entry.
  - UI integrates existing workflow APIs: `/api/coord/workflow-snapshot`, `/api/coord/stage/enqueue`, `/api/coord/stage/reset`.
- Added render helper `render_workflows_html()` in `reporting/server_pages.py` and wired import usage in `server.py`.
- Added/updated tests:
  - `tests/test_refactor_modules.py`
  - `tests/test_reporting_and_store_helpers.py`
  - `tests/test_module_decomposition.py`
- Validation:
  - `python -m py_compile reporting/server_pages.py server.py`
  - `pytest -q tests/test_refactor_modules.py tests/test_reporting_and_store_helpers.py tests/test_module_decomposition.py`

## 2026-04-22

- Extended `/workflows` with a second `Timeline` tab for workflow task lifecycle events.
  - Timeline consumes `/api/coord/events` with `event_type=workflow.task.` and client-side filters for domain/workflow/plugin/text.
  - Added quick action to copy current monitor selection into timeline filters (`Use Selected Domain`).
  - Kept monitor + control interactions (`enqueue`, `reset`) in same page and shared refresh loop.
- Validation:
  - `pytest -q tests/test_refactor_modules.py tests/test_reporting_and_store_helpers.py tests/test_module_decomposition.py`

## 2026-04-22

- Fixed coordinator startup crash on legacy databases during workflow schema rollout.
  - Root cause: base `_ensure_schema` DDL referenced `coordinator_stage_tasks.workflow_id` in a bootstrap index before legacy stage-task tables had that column.
  - Change: base DDL now uses legacy-safe stage index shape (`stage, status`) and leaves workflow-aware index creation to migration statements.
  - Change: schema migrations now run as individually committed statements (rollback per statement only), preventing one migration failure from undoing successful prior migrations.
- Added regression guard:
  - `tests/test_reporting_and_store_helpers.py::test_ensure_schema_bootstrap_stage_index_is_legacy_safe`
- Validation:
  - `python -m py_compile server_app/store.py server.py`
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "workflows or ensure_schema_bootstrap_stage_index_is_legacy_safe"`
  - `pytest -q tests/test_module_decomposition.py tests/test_refactor_modules.py`

## 2026-04-22

- Added workflow-driven recon execution support with new plugins in `coordinator.py`:
  - `recon_subdomain_enumeration` (Sublist3r-first enumeration + passive fallback + reachability probes)
  - `recon_spider_source_tags`
  - `recon_spider_script_links`
  - `recon_spider_wordlist`
  - `recon_spider_ai`
  - `recon_extractor_high_value`
- Added resumable local progress-file + completion-flag artifact handling for recon plugins.
- Added reusable Nightmare artifact upload helper and wired recon spider stages to continuously publish Nightmare outputs.
- Added workflow file `workflows/run-recon.workflow.json` with plugin prerequisites and parameters.
- Added opt-in coordinator config `config/coordinator.run-recon.json` for workflow-only recon execution (legacy per-tool loops disabled).
- Added tests in `tests/test_recon_workflow_config.py` to lock expected run-recon plugin order and extractor prerequisites.
- Why: deliver queue-safe, resumable, pluginized recon orchestration where workflow prerequisites are data/flag driven.
- Updated `recon_extractor_high_value` implementation to scan Nightmare output files directly using the high-value rule list, instead of requiring Fozzy artifacts.
- Why: `run-recon` workflow does not run Fozzy, so extractor stage must be independent and still emit extractor-compatible artifacts.

## 2026-04-22

- Added web-driven recon workflow operations in `templates/workflows.html.j2`:
  - New `Run Recon` tab on `/workflows`.
  - Step-by-step plugin editor (enabled/retry/max-attempts/parameters JSON).
  - One-click sequence: save config, enforce workflow-only mode, request worker reload, enqueue starter plugins for all domains.
- Added workflow configuration APIs in `server.py`:
  - `GET /api/coord/workflow-config`
  - `POST /api/coord/workflow-config`
  - `POST /api/coord/workflow/run`
  - `POST /api/coord/workflow/mode`
  - `POST /api/coord/workflow/reload`
- Extended worker-command support for live workflow updates:
  - `server_app/store.py` now accepts `reload` in `queue_worker_command`.
  - `server.py` worker command endpoint now validates `reload`.
  - `coordinator.py` now applies `reload` by re-reading workflow file and rebuilding stage map in-process.
- Added main app navigation links to workflow controls:
  - Navbar now includes direct `Run Recon` link (`/workflows#recon`).
  - Dashboard now includes quick links to workflow monitor/recon controls.
- Validation:
  - `python -m py_compile server.py coordinator.py server_app/store.py`
  - `pytest -q tests/test_recon_workflow_config.py`
  - `pytest -q tests/test_reporting_and_store_helpers.py`

## 2026-04-22

- Refactored major coordinator workflow plugins into a dedicated `plugins/` package:
  - Added plugin class files for `fozzy`, `auth0r`, `extractor`, Nightmare artifact-gate, recon subdomain enumeration, recon high-value extractor, and each recon spider type in `plugins/recon/spider/`.
  - Added shared plugin execution contract in `plugins/base.py` (`PluginExecutionContext`, `CoordinatorPlugin`).
  - Added centralized plugin resolver in `plugins/registry.py`.
- Updated `coordinator.py` plugin dispatch:
  - Replaced hardcoded plugin-name `if/elif` run branch with registry-based execution via `resolve_plugin(...)`.
  - Kept unsupported-plugin failure semantics unchanged.
- Validation:
  - `python -m py_compile coordinator.py plugins/...`
  - `pytest -q tests/test_module_decomposition.py tests/test_refactor_modules.py tests/test_recon_workflow_config.py`

## 2026-04-22

- Recon spider throttle and domain-concurrency hardening:
  - Updated recon spider runtime (`coordinator.py`) to use `spider_throttle_seconds` with fallback to `crawl_delay`, defaulting to `0.5` seconds.
  - Persisted effective spider throttle in recon spider progress payload (`spider_throttle_seconds`).
  - Updated `workflows/run-recon.workflow.json` so all recon spider plugins default to `spider_throttle_seconds: 0.5`.
- Added cross-lane domain locking in `CoordinatorStore` claim paths:
  - `claim_target(...)` now skips domains with active running stage-task leases.
  - `claim_next_stage(...)` now skips domains with active running target leases.
  - Result: no simultaneous target-worker and plugin-worker processing for the same domain.
- Added regression tests:
  - `tests/test_recon_workflow_config.py` checks spider throttle default for all recon spider steps.
  - `tests/test_reporting_and_store_helpers.py` checks source-level domain-lock guards in both claim methods.
- Validation:
  - `python -m py_compile coordinator.py server_app/store.py`
  - `pytest -q tests/test_recon_workflow_config.py tests/test_reporting_and_store_helpers.py -k "recon or claim_target_respects_running_stage_domain_lock or claim_next_stage_respects_running_target_domain_lock or ensure_schema_bootstrap_stage_index_is_legacy_safe"`
  - `pytest -q tests/test_module_decomposition.py tests/test_runtime_unit.py`

## 2026-04-22

- Switched coordinator runtime to plugin-worker-only execution:
  - `coordinator.py::run()` now starts only workflow scheduler thread and generic plugin worker threads.
  - Removed startup paths/fallback behavior that launched legacy dedicated workers (`nightmare`, `fozzy`, `auth0r`, `extractor`).
  - Stage scheduling runtime gate now ignores legacy per-tool enable flags and treats all plugin stages as runtime-eligible.
- Updated startup/logging semantics:
  - Coordinator startup logs now report plugin-worker-only topology and mark legacy worker settings as ignored.
  - CLI description updated to reflect workflow plugin runtime.
- Updated config/loading semantics:
  - `CoordinatorSettings.plugin_workers` default changed to `1`.
  - `load_config(...)` no longer derives plugin worker count from legacy per-tool worker counts; it enforces minimum `1`.
  - Cleaned `config/coordinator.json` and `config/coordinator.run-recon.json` to remove legacy worker knobs.
- Added regression test:
  - `tests/test_runtime_unit.py::test_load_config_uses_plugin_workers_only` ensures plugin worker count is standalone and not legacy-derived.
- Validation:
  - `python -m py_compile coordinator.py coordinator_app/runtime.py nightmare_shared/config.py`
  - `pytest -q tests/test_runtime_unit.py tests/test_config_utils.py tests/test_module_decomposition.py`
  - `pytest -q tests/test_refactor_modules.py tests/test_recon_workflow_config.py`

## 2026-04-23

- Upgraded discovered-target APIs and UI for responsive large-data browsing:
  - `GET /api/coord/discovered-targets` now supports paging/sorting metadata (`offset`, `limit`, `sort_key`, `sort_dir`) and returns pagination fields (`next_offset`, `has_more`, etc.).
  - `GET /api/coord/discovered-target-sitemap` now supports paging/search/subdomain/sort query params, returns subdomain option lists, and lazily enriches only returned rows.
  - Added `GET /api/coord/discovered-target-download` for direct request/response/evidence downloads (`request_json`, `response_json`, `request_body`, `response_body`, `evidence_json`).
- Added missing coordinator-store discovered-target detail path:
  - Implemented `CoordinatorStore.get_discovered_target_response(...)`.
  - Added evidence-file parsing helpers and row enrichment helper (`enrich_discovered_target_sitemap_rows`).
  - Sitemap rows now include subdomain/path/existence summary and response metadata fields expected by UI.
- Rebuilt `templates/discovered_targets.html.j2` into cached, paged, virtualized URL explorer:
  - domain + sitemap paging
  - per-domain URL search/filter/sort with subdomain split
  - lazy refresh with local cache TTLs
  - per-row links for view/request/response/evidence downloads.
- Added non-blocking global network activity indicator in `templates/_navbar.html.j2` by wrapping `window.fetch`, so users always see when actions are in-flight without UI lock.
- Enhanced `templates/discovered_target_response.html.j2` with download link section for request/response/evidence artifacts.
- Validation:
  - `python -m py_compile server.py server_app/store.py`
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "discovered_target or discovered_files or render_discovered_targets_html_contains_expected_heading"`

## 2026-04-23

- Added generalized server-side per-page cache for coordinator list APIs in `server.py`:
  - New `_PageDataCache` with TTL + bounded entries.
  - New API cache metadata envelope (`page_cache`) and cache mode support (`cache_mode=prefer|refresh`).
  - Applied to:
    - `GET /api/coord/crawl-progress`
    - `GET /api/coord/discovered-targets`
    - `GET /api/coord/discovered-target-sitemap`
    - `GET /api/coord/discovered-files`
    - `GET /api/coord/high-value-files`
- Added periodic default-query warming thread in `server.py`:
  - Preloads cache entries for default crawl progress, discovered targets, discovered files, and high-value files.
  - Preloads first-page sitemap cache for a small top-domain sample.
- Updated templates for cache-first, non-blocking refresh behavior:
  - `templates/crawl_progress.html.j2`
  - `templates/discovered_targets.html.j2`
  - `templates/discovered_files.html.j2`
  - Initial load now prefers cached API payloads and follows with background source refresh when applicable.
- Validation:
  - `python -m py_compile server.py`
  - `pytest tests/test_reporting_and_store_helpers.py -k "render_crawl_progress_html or render_discovered_targets_html or render_discovered_files_html"`
  - `pytest tests/test_reporting_and_store_helpers.py -k "list_discovered_target_domains or get_discovered_target_response_and_row_enrichment_include_download_links or list_discovered_files_returns_template_compatible_keys or list_high_value_files_returns_template_compatible_keys or crawl_progress_snapshot_reports_domain_counts"` (existing unrelated failure remains in `test_crawl_progress_snapshot_reports_domain_counts`)

## 2026-04-23

- Fixed discovered URL visibility gaps in discovered-target sitemap generation:
  - `CoordinatorStore.get_discovered_target_sitemap(...)` now derives candidate URLs from both `state.discovered_urls` and `state.url_inventory` keys.
  - Relative URLs are normalized against `start_url` so sitemap rows still render when inventory/link graph stores relative paths.
  - Inbound/outbound link counts now use a normalized link graph, avoiding missed parent/child relationships caused by mixed absolute/relative URL formats.
- Improved crawl progress rendering compatibility:
  - `templates/crawl_progress.html.j2` now renders spider stats from either structured arrays or `{spider_name: count}` maps returned by API payloads.
- Added regression coverage:
  - `tests/test_reporting_and_store_helpers.py::test_get_discovered_target_sitemap_uses_inventory_when_discovered_urls_missing`.
- Validation:
  - `python -m py_compile server_app/store.py server.py`
  - `pytest tests/test_reporting_and_store_helpers.py -k "list_discovered_target_domains_uses_session_inventory_counts or get_discovered_target_sitemap_uses_inventory_when_discovered_urls_missing or get_discovered_target_response_and_row_enrichment_include_download_links"`

## 2026-04-23

- Removed legacy dashboard page route behavior in `server.py`:
  - `GET /dashboard` now returns `404` with guidance to use `/workers` instead of rendering a page alias.
- Why:
  - Keep UI navigation and routes aligned with current page model (Workers + Workflows + HTTP Requests) and avoid exposing deprecated pages.
- Validation:
  - `python -m py_compile server.py`
  - `pytest -q tests/test_refactor_modules.py -k "workflows_template_renders or http_requests_template_renders or server_template_renders or worker_template_renders"`

## 2026-04-23

- Fixed discovered URL visibility regressions on crawl/discovered-target views by hardening coordinator session parsing in `server_app/store.py`.
  - Added payload-shape tolerant extraction for session state (`state`, nested `payload/session/data`, and URL inventory entry lists).
  - Added fallback hydration from `nightmare_url_inventory_json` artifacts when session payloads are missing/partial.
  - Removed strict artifact-fallback cap for crawl/discovered-target domain loaders so URL counts are populated consistently for requested page scope.
- Why:
  - Some coordinator rows/artifacts carry equivalent URL discovery data in different JSON shapes; strict state-only parsing caused `discovered_urls_count` to collapse to zero and sitemap rows to appear empty.

- Added per-worker current-run log surfacing for Worker Control.
  - `coordinator_app/runtime.py::run_subprocess(...)` now supports `mirror_log_path` and mirrors each run segment into a worker-level file.
  - `coordinator.py` now mirrors all stage subprocess output into `output/_worker_logs/<worker_id>.current.log`.
  - `server.py` now infers `current_run_log` from latest structured worker event metadata (`log_path`) with file-link fallback.
  - `templates/worker_control.html.j2` now renders a dedicated current-run log link in each worker row.
- Why:
  - Existing worker log discovery relied on worker-id filename patterns, but active logs were domain/stage named and often not directly linkable per worker.

- Validation:
  - `python -m py_compile server_app/store.py server.py coordinator.py coordinator_app/runtime.py`
  - `pytest -q tests/test_refactor_modules.py -k "worker_template_renders_database_link or crawl_progress_template_renders or discovered_targets_template_renders"`

## 2026-04-23

- Fixed central deploy crash-loop caused by server CLI incompatibility.
  - `server.py` now accepts compose/deploy TLS arguments:
    - `--http-port`
    - `--https-port`
    - `--cert-file`
    - `--key-file`
  - Server startup now selects port precedence as `--port` (legacy) -> `--https-port` -> `--http-port`.
  - When `--https-port` is used, startup validates cert/key presence and file existence before launching uvicorn with TLS.
  - Added env/config fallbacks for TLS path fields (`CERT_FILE` / `KEY_FILE` and `TLS_CERT_FILE` / `TLS_KEY_FILE`).
- Why:
  - `deploy/docker-compose.central.yml` and `deploy/docker-compose.local.yml` pass TLS flags by default; without flag support the server container exits immediately and coordinator readiness checks never succeed.
- Validation:
  - `python -m py_compile server.py`
  - `pytest -q tests/test_runtime_unit.py -k "test_read_json_dict_handles_invalid_content"`

## 2026-04-23

- Fixed coordinator startup crash on central deploy against existing Postgres schema.
  - Root cause: `_ensure_schema` bootstrap DDL created artifact/stage indexes that referenced columns only guaranteed by later `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migrations.
  - On pre-existing `coordinator_artifacts` tables missing `summary_match_count`, bootstrap failed before migrations with `psycopg.errors.UndefinedColumn`.
- Changes in `server_app/store.py`:
  - Bootstrap DDL now keeps legacy-safe stage index shape:
    - `idx_stage_tasks_status_stage` on `(stage, status)` only.
  - Removed non-legacy-safe bootstrap indexes from DDL:
    - `idx_stage_tasks_claim_partial`
    - `idx_stage_tasks_running_domain_lease`
    - `idx_stage_tasks_concurrency`
    - `idx_artifacts_retention`
    - `idx_artifacts_hot_fields`
  - Added post-migration index creation statements (after `ADD COLUMN IF NOT EXISTS` paths):
    - `idx_stage_tasks_workflow_stage_status` on `(workflow_id, stage, status)`
    - `idx_artifacts_retention`
    - `idx_artifacts_hot_fields`
- Added regression guard in `tests/test_reporting_and_store_helpers.py`:
  - `test_ensure_schema_bootstrap_artifact_indexes_are_legacy_safe`
- Validation:
  - `python -m py_compile server_app/store.py tests/test_reporting_and_store_helpers.py`
  - `pytest -q tests/test_runtime_unit.py -k "test_read_json_dict_handles_invalid_content"`
  - Note: full `tests/test_reporting_and_store_helpers.py` import currently fails in this workspace due unrelated `server` export mismatch (`_PageDataCache`), not from this schema patch.

## 2026-04-23

- Restored coordinator API endpoints required by deploy readiness and target bootstrap in `server_app/fastapi_app.py`.
  - Added `GET /api/coord/database-status` (returns `CoordinatorStore.database_status()`).
  - Added `POST /api/coord/register-targets` (accepts `targets` list and optional `replace_existing`, delegates to `CoordinatorStore.register_targets`).
- Why:
  - `deploy/full_deploy_command.sh` readiness loop probes `/api/coord/database-status` and `register_targets.py` posts to `/api/coord/register-targets`.
  - Missing routes caused perpetual readiness `404` and blocked deployment progression.
- Validation:
  - `python -m py_compile server_app/fastapi_app.py server.py`
  - `pytest -q tests/test_runtime_unit.py -k "test_read_json_dict_handles_invalid_content"`

## 2026-04-23

- Restored full web UI server behavior by reverting `server.py` to the last pre-refactor implementation that serves the complete coordinator website + API surface via `DashboardHandler`/`ThreadingHTTPServer`.
  - Root and page routes restored (`/workers`, `/workflows`, `/database`, `/crawl-progress`, `/discovered-targets`, `/discovered-files`, `/http-requests`, `/docker-status`, `/view-logs`, `/errors`, etc.).
  - Coordinator API routes used by deploy/UI restored in same server process (`/api/coord/database-status`, `/api/coord/register-targets`, `/api/coord/worker-control`, `/api/coord/workers`, `/api/coord/http-requests`, `/api/coord/discovered-targets`, `/api/coord/discovered-files`, and related download endpoints).
  - TLS listener/CLI contract retained (`--http-port`, `--https-port`, `--cert-file`, `--key-file`).
- Why:
  - Refactored FastAPI-only entrypoint exposed only a subset of routes and returned a minimal root page, breaking access to the previously functional coordinator website.
- Validation:
  - `python -m py_compile server.py`
  - `pytest -q tests/test_refactor_modules.py -k "server_template_renders or worker_template_renders_database_link or workflows_template_renders or database_template_renders or crawl_progress_template_renders or http_requests_template_renders"`
  - `pytest -q tests/test_server_auth_cookie.py`

## 2026-04-23

- Fixed coordinator responsiveness regression caused by aggressive default page-cache warming.
  - In `server.py`, `_start_default_page_cache_warmer(...)` no longer prewarms `/api/coord/http-requests` data.
  - Why: HTTP-request aggregation reconstructs data across many domains/artifacts and was creating sustained DB load every warm cycle, which could starve other UI/API requests.
- Added cache-backed workflow snapshot reads for Workflow Runs.
  - `GET /api/coord/workflow-snapshot` now supports cache-aware reads via `_resolve_cached_page_payload(...)` with `cache_mode` and short TTL.
  - `templates/workflows.html.j2` now requests snapshot data with `cache_mode=prefer`.
- Improved default homepage behavior for large installations.
  - `GET /` now serves the Workers UI directly (fast control surface).
  - Added `GET /all-domains-report` to explicitly access the generated all-domains HTML report when needed.
- Restored scale reset API parity in FastAPI server mode.
  - `server_app/fastapi_app.py` now supports:
    - status-filtered `POST /api/coord/stage/reset`
    - `POST /api/coord/targets/reset`
    - scoped `POST /api/coord/tasks/reset` (stage_tasks/targets/all)
  - Includes `errored`/`error` => `failed` status normalization for compatibility with UI/operator wording.
- Validation:
  - `python -m py_compile server.py server_app/fastapi_app.py server_app/store.py reset_tasks.py`
  - `pytest -q tests/test_refactor_modules.py -k "workflows_template_renders or worker_template_renders_database_link or database_template_renders or crawl_progress_template_renders or http_requests_template_renders"`
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "render_workflows_html_contains_expected_heading or ensure_schema_bootstrap_stage_index_is_legacy_safe or ensure_schema_bootstrap_artifact_indexes_are_legacy_safe"`

## 2026-04-23

- Fixed empty Events page behavior under token-protected coordinator APIs.
  - `templates/events.html.j2` now includes coordinator token input, cookie hydration/persistence, and sends `Authorization: Bearer ...` on `/api/coord/events` fetches.
  - Root cause: Events UI fetched without auth headers while server endpoint requires coordinator auth.

- Fixed stale/empty worker event metadata in Worker Control.
  - `CoordinatorStore._latest_worker_event_map(...)` now reads from Postgres `coordinator_recent_events` instead of the removed/unused in-memory event stream path.
  - This restores `last_event_emitted` and `last_event_emitted_at_utc` updates per worker.

- Improved worker snapshot live action fields for plugin workflow runtime.
  - `CoordinatorStore.worker_control_snapshot(...)` now includes current running stage metadata per worker:
    - `current_workflow_id`
    - `current_plugin_name`
    - `current_stage_status`
    - `current_stage_activity_at_utc`
    - `current_action` (workflow/plugin multiline text)
  - `current_targets` now includes active stage root-domain context, so worker rows show the domain currently being processed even during stage-task execution.
  - Added `last_seen_time_at_utc` rollup and kept backward-compatible `last_run_time_at_utc` alias.

- Fixed Worker Control UI columns per operator request.
  - Replaced `URLs Scanned (session)` with `Current Action`.
  - Renamed `Last Run Time` to `Last Seen Time` and bound it to `last_seen_time_at_utc`.
  - Kept `Current Targets`, `Last Event Emitted`, and `Last Log Message` wired to live backend values.

- Added scheduler safety-net for domains already mid-workflow.
  - `coordinator.py` workflow scheduler now performs startup + periodic idle full snapshot rescans (`get_workflow_snapshot`) and re-evaluates scheduling on each domain.
  - Root cause addressed: queue-driven scheduler could miss domains that were already ready when scheduler started/restarted, leaving downstream spider tasks unscheduled.

- Improved workflow config normalization compatibility.
  - `_normalize_workflow_entry(...)` now accepts `preconditions` as equivalent to `prerequisites` and merges `inputs.artifacts_all/artifacts_any` into required artifact gates.

- Validation:
  - `python -m py_compile coordinator.py server.py server_app/store.py server_app/fastapi_app.py`
  - `pytest -q tests/test_refactor_modules.py -k "worker_template_renders_database_link or workflows_template_renders or server_template_renders or crawl_progress_template_renders"`
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "render_workflows_html_contains_expected_heading or ensure_schema_bootstrap_stage_index_is_legacy_safe or ensure_schema_bootstrap_artifact_indexes_are_legacy_safe"`

## 2026-04-24

- Fixed recon stage readiness deadlock when no `coordinator_targets` rows exist yet for a domain.
  - Root cause: prerequisite evaluation mapped zero target counts to `unknown`, so stages requiring `target_statuses: [pending, running, completed]` stayed permanently blocked as "Waiting for Prerequisites...".
  - Updated target-status fallback in both readiness evaluators:
    - `server_app/store.py::_stage_prerequisites_satisfied(...)`
    - `coordinator.py::DistributedCoordinator._has_stage_prerequisites(...)`
  - New behavior: when a domain has zero target rows, current target status is treated as `pending` (domain-level workflow default), while `require_target_completed` checks remain strict.
- Added regression tests:
  - `tests/test_stage_prerequisites_target_status.py`
    - scheduler prerequisite evaluator accepts missing-target domains for `pending/running/completed` target-status gates.
    - store prerequisite evaluator mirrors the same behavior.
    - `require_target_completed` still blocks when no completed target exists.

- Fixed workflow definition save failures caused by FK references from `workflow_step_runs` to `workflow_steps`.
  - Root cause: `save_workflow_definition(...)` deleted all `workflow_steps` rows for a definition before re-inserting, which violates `workflow_step_runs_step_definition_id_fkey` when historical runs reference those step IDs.
  - `workflow_app/store.py` now:
    - Adds `workflow_steps.is_archived` (DDL + migration).
    - Replaces delete/reinsert with in-place upsert by stable step `id` (fallback by `step_key`), preserving referenced step IDs.
    - Archives removed steps (`is_archived=true`, `enabled=false`, moved to high ordinals) instead of deleting.
    - Filters archived steps out of definition reads and step counts (`get_workflow_definition`, `list_workflow_definitions`) so removed steps do not reappear in the builder.
    - Adds payload validation for duplicate `step_key` and duplicate `ordinal` to return explicit errors before DB unique-constraint failures.
- Added regression tests:
  - `tests/test_workflow_store.py::test_save_workflow_definition_archives_removed_steps_without_deleting`
  - `tests/test_workflow_store.py::test_get_workflow_definition_excludes_archived_steps`
- Validation:
  - `python -m py_compile workflow_app/store.py tests/test_workflow_store.py`
  - `pytest -q tests/test_workflow_store.py`
  - `pytest -q tests/test_refactor_modules.py -k "workflows_template_renders or server_template_renders"`

## 2026-04-23

- Restored worker-control backward compatibility after worker snapshot/event refactor in `server_app/store.py`.
  - `worker_control_snapshot(...)` now tolerates shorter/legacy SQL row tuples when optional current-stage columns are absent (defensive index access with defaults).
  - `_latest_worker_event_map(...)` now keeps DB-first event lookup but falls back to legacy in-memory `_event_stream.read(...)` when DB lookup is unavailable/empty.
- Why:
  - Unit tests and legacy execution paths can provide reduced worker snapshot row shapes and in-memory event streams; hard assumptions caused crashes or missing last-event metadata.
- Validation:
  - `python -m py_compile server_app/store.py`
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "worker_control_snapshot_includes_presence_only_worker or worker_control_snapshot_includes_latest_worker_event"`
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "render_workflows_html_contains_expected_heading or ensure_schema_bootstrap_stage_index_is_legacy_safe or ensure_schema_bootstrap_artifact_indexes_are_legacy_safe or worker_control_snapshot_includes_presence_only_worker or worker_control_snapshot_includes_latest_worker_event"`

## 2026-04-24

- Unblocked immediate recon bootstrap pickup for domains that have not yet run subdomain enumeration.
  - `server_app/store.py::_stage_prerequisites_satisfied(...)` now treats `recon_subdomain_enumeration` target-status gates as including `failed` when a workflow specifies target statuses.
  - `coordinator.py::DistributedCoordinator._has_stage_prerequisites(...)` mirrors the same rule so scheduler-side enqueue checks and store-side readiness checks stay aligned.
  - `server_app/store.py::try_claim_stage_with_resources(...)` now allows `recon_subdomain_enumeration` claims even when a domain currently has a running `coordinator_targets` lease (other stages still keep the target-running lock).
- Updated recon workflow defaults so first-stage intent is explicit in config:
  - `workflows/run-recon.workflow.json`
  - `workflows/coordinator.workflow.json`
  - `workflows/run-recon.workflow_1.json`
  - Added `"failed"` to `recon_subdomain_enumeration.preconditions.target_statuses`.
- Added regression tests:
  - `tests/test_stage_prerequisites_target_status.py` (failed target status accepted for recon subdomain enumeration in scheduler + store evaluators).
  - `tests/test_reporting_and_store_helpers.py` (claim SQL explicitly preserves target-running lock but exempts `recon_subdomain_enumeration`).
- Validation:
  - `python -m py_compile server_app/store.py coordinator.py tests/test_stage_prerequisites_target_status.py tests/test_reporting_and_store_helpers.py`
  - `pytest -q tests/test_stage_prerequisites_target_status.py tests/test_reporting_and_store_helpers.py -k "prerequisite_check or claim_next_stage_allows_recon_subdomain_enumeration_while_target_running or claim_next_stage_respects_running_target_domain_lock"`

- Added workflow interface extension for config-defined control/results pages.
  - Workflow files can now declare `interfaces` entries (`control`/`results`) with route/template metadata.
  - `server.py` discovers these interface configs and:
    - auto-registers dynamic HTML routes from workflow config at startup/runtime,
    - exposes interface catalog via `GET /api/coord/workflow-interfaces`,
    - refreshes interface catalog after workflow-config saves.
  - Shared navbar (`templates/_navbar.html.j2`) now hydrates workflow interface links dynamically from the interface catalog API.

- Added first recon workflow control/results interfaces.
  - New templates:
    - `templates/workflow_interfaces/recon_control.html.j2`
    - `templates/workflow_interfaces/recon_results.html.j2`
  - `workflows/run-recon.workflow.json` now declares both interface pages (also mirrored in `workflows/coordinator.workflow.json` and `workflows/run-recon.workflow_1.json`).
  - Recon control page supports:
    - searchable/filterable multi-domain target selection + `ALL` target mode,
    - workflow step selection with per-step configure modal (JSON parameters),
    - generate workflow tasks action,
    - clear generated workflow tasks action with running-task warning.
  - Recon results page supports:
    - per-domain recon metrics table,
    - subdomain-count modal drilldown,
    - actions: clear workflow data, clear tasks, open request/response data.

- Added supporting APIs for interface and recon workflow controls.
  - `GET /api/coord/workflow-interface/recon/domains`
  - `GET /api/coord/workflow-interface/recon/results`
  - `GET /api/coord/workflow-interface/recon/subdomains`
  - `POST /api/coord/workflow/clear-generated-tasks`
  - `POST /api/coord/workflow/clear-data`
  - Extended `POST /api/coord/workflow/run` to support:
    - plugin allowlist (`plugins`),
    - per-plugin parameter overrides (`plugin_parameter_overrides`),
    - optional workflow-file persistence + worker reload command queueing.
  - Added `CoordinatorStore` helpers for data cleanup:
    - `delete_artifacts(...)`
    - `delete_sessions(...)`

- UX/supporting updates:
  - `templates/http_requests.html.j2` now respects query params (`root_domain`, `q`) for deep-link filtering.
  - Added tests:
    - `tests/test_workflow_interface_config.py`

- Validation:
  - `python -m py_compile server.py server_app/store.py server_app/fastapi_app.py tests/test_workflow_interface_config.py`
  - `pytest -q tests/test_workflow_interface_config.py tests/test_refactor_modules.py -k "workflow or navbar or http_requests_template_renders"`
  - `pytest -q tests/test_reporting_and_store_helpers.py -k "render_workflows_html_contains_expected_heading or claim_next_stage_respects_running_target_domain_lock"`
