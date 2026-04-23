# Architecture Notes

- `fozzy.py` has two primary entry paths:
  - Single-domain run via explicit `<domain>.parameters.json` argument.
  - Incremental multi-domain run when no parameters file is passed (`run_incremental_domains`).
- Incremental change detection currently depends on scanning each domain folder and comparing a max mtime snapshot with `.fozzy_incremental_state.json`.
- Startup responsiveness in incremental mode is user-critical; no-output periods are interpreted as hangs.
- Technical hotspot: deep recursive filesystem scans over large `output` trees can dominate startup time.
- Batch orchestrator state writes (`output/batch_state/batch_run_state.json`) can hit transient Windows file-lock conflicts during atomic rename.
- `_atomic_write_json` should tolerate short-lived `PermissionError` windows with bounded retry/backoff.
- Crawl seed architecture in `nightmare.py`: URL seeds come from the normalized start URL + optional path wordlist seeds; wordlist source path is persisted in session state for downstream reporting.
- For path-wordlist runs, configured `max_pages` is treated as a floor: effective crawl budget is expanded to include all pending seed URLs so seed coverage is deterministic.
- `fozzy.py` has two concurrency layers now:
  - Incremental `--scan-root` mode: parent scheduler runs multiple domains concurrently via child processes.
    - Concurrency knob: `incremental_domain_workers` (config/CLI).
  - Per-domain fuzzing inside each child: constrained to single-worker in incremental mode to keep global concurrency bounded and avoid shared-global state races.
- Results reporting model in `fozzy.py`: summary JSON remains source-of-truth; generated HTML reads discrepancy rows from sibling summary JSON at runtime (thin-shell HTML), reducing inline payload size and duplication.
- Master-report HTML now follows the same thin-shell model for inventory + extractor tables as well; browser runtime fills these tables from `all_domains.results_summary.json`.
- Observability:
  - `nightmare.py` already emits app + Scrapy logs per domain.
  - `fozzy.py` and `extractor.py` now emit persistent process logs (stdout/stderr tee to file).
  - Master report payload includes discovered log file metadata and the master HTML can load/view selected logs on demand.
- Central coordinator observability boundary:
  - `server.py` now computes worker-fleet status in Postgres (`coordinator_targets` + `coordinator_stage_tasks`) and exposes it through `/api/coord/workers`.
  - Worker liveness is inferred from last heartbeat timestamp relative to a configurable stale threshold (`stale_after_seconds`).
- Operator control/visibility entrypoint:
  - `client.py` acts as a central-machine CLI facade for status checks, combining coordinator worker heartbeat view and optional AWS SSM fanout checks for worker container process state on each VM.
  - `client.py` now also performs centralized worker rollout (`rollout` action): remote git update + compose restart on targeted worker VMs through one SSM command.
- Coordinator runtime boundary:
  - `coordinator_app/runtime.py` must own all runtime config-model symbols returned by `load_config()` (currently `CoordinatorConfig`) plus helper functions referenced by runtime threads (`_read_json_dict`, `_now_iso`).
  - Keeping these symbols in `coordinator.py` can break worker startup when `coordinator.py` is executed as `__main__`, because runtime module globals cannot resolve classes only defined in another module's script scope.
- Local deployment trust boundary:
  - Central server is exposed over HTTPS with a self-signed cert in `deploy/run-local*` flows.
  - Coordinator workers need explicit TLS verification override (`COORDINATOR_INSECURE_TLS=true`) for this local mode; do not rely on hostname heuristics alone.
- Test architecture boundary:
  - Unit tests should validate coordinator/runtime/queue behavior through pure helpers, temp-file state, and mocked HTTP/AWS calls rather than requiring containerized infrastructure.
  - This keeps regressions detectable in sub-10s local runs and avoids flaky integration-coupled failures for routine refactors.
- Internet-facing central server boundary:
  - Exposed 80/443 listeners will receive continuous opportunistic scanner traffic (random PHP probes, malformed protocol bytes, aborted sockets).
  - These events should be handled as transport noise unless they affect coordinator API health; request-handler disconnect exceptions should be absorbed without noisy traceback output.
- Server/store boundary:
  - `server.py` should own HTTP routing/response behavior only.
  - Postgres coordinator data access now lives in `server_app/store.py` (`CoordinatorStore`) and is imported by `server.py`.
  - Coordinator store schema/queries and database introspection (`database_status`) should stay in `server_app/store.py` so future API/UI work can evolve without re-growing server-side god classes.
- Server/UI rendering boundary:
  - Route handlers in `server.py` should delegate dashboard/worker page markup to `reporting/server_pages.py` (template-backed) instead of embedding long inline HTML methods.
- Central/worker trust boundary:
  - `bootstrap-central-auto.sh` defaults to self-signed TLS for coordinator HTTPS.
  - Worker coordinator clients verify TLS by default; deployments using bootstrap self-signed certs must explicitly set `COORDINATOR_INSECURE_TLS=true` on workers or provision trust anchors.
- UI rendering boundary tightened: `server.py` now delegates all HTML page generation (dashboard, worker control, database status) to `reporting/server_pages.py`, keeping route handling separate from template markup.
- Operational visibility hardening: dashboard JS now surfaces API load failures directly in-page to reduce false "empty dashboard" ambiguity during outages or API regressions.

- Runtime boundary cleanup: separated crawl URL policy (
ightmare_app/spider_url_policy.py) and fuzz request/model core (ozzy_app/fuzz_core.py) from the monolithic CLI scripts. Entrypoints now orchestrate config/state while reusable logic lives in importable modules for future service/test reuse.

- Normalized export boundary:
  - `nightmare.py` remains the crawl orchestrator; normalized artifact shaping/writing is delegated to `nightmare_app/normalized_exports.py`.
  - Normalized exports consolidate endpoint snapshots (`collected_data/endpoints/*.json`) and evidence snapshots (`*_evidence/*.json.gz`) into deduplicated raw request/response blobs plus derived inventories (`sitemap`, `requests`, `redirects`, `findings`, etc.).
  - High-value source files are still produced by crawl/high-value capture flow under `<output_root>/high_value/<domain>` and then mirrored into normalized `high-value/` output.

- Database-status fault isolation boundary:
  - `server.py` owns API-level failure translation (exceptions -> HTTP JSON errors).
  - `server_app/store.py` owns table-by-table fault isolation during DB introspection (`table_error` per table) so one problematic relation/type does not break the entire `/api/coord/database-status` payload.

- Worker presence boundary:
  - Worker liveness in UI is no longer inferred only from target/stage leases; `server_app/store.py` now persists lightweight worker presence heartbeats in `coordinator_worker_presence`.
  - Polling claims/heartbeats/completions update this presence table, allowing idle-but-running workers to appear in `/api/coord/workers` and `/api/coord/worker-control`.
- Observability boundary update: crawl-progress aggregation lives in `server_app/store.py` and is exposed by `server.py` as `/api/coord/crawl-progress`; this follows the server/store separation (routing in server, query logic in store).
- Server UI boundary preserved during crawl-progress page addition:
  - Route dispatch remains in `server.py`.
  - HTML rendering remains delegated to `reporting/server_pages.py` templates.
  - Data for crawl page is supplied by existing coordinator API `/api/coord/crawl-progress` (store/query logic remains in `server_app/store.py`).
- Worker-control boundary update:
  - UI command queueing (`/api/coord/workers/command`) now pairs with worker-side command claim/complete APIs (`/api/coord/worker-command/*`) consumed by coordinator worker loops.
  - Presence heartbeats now carry state hints from worker command polling, enabling status reporting beyond lease-derived running tasks.
- Dashboard data boundary update:
  - `collect_dashboard_data()` now supports coordinator-store-backed domain enrichment so operator dashboard remains informative on central-only hosts with sparse local artifacts.

- Extractor artifact browsing boundary:
  - `server_app/store.py` now provides extractor-domain metadata (`list_extractor_match_domains`) while `server.py` owns zip extraction orchestration and HTTP responses.
  - Parsed extractor zip rows/files are cached in-process (`_ExtractorMatchesCache`) keyed by `root_domain + content_sha256` with TTL + bounded domain count.
  - UI page `/extractor-matches` consumes these APIs and can query one domain or all domains without requiring pre-expanded filesystem artifacts.
- Extractor match query shaping (global search, per-column filters, sort, paging) is handled in `server.py` before serialization so clients can request only one page at a time and avoid transferring full result sets.
  - Domain metadata endpoint now also computes highest extractor match importance score per domain, sourced from cache when present and zip-scan fallback when needed.

- Fozzy findings observability boundary:
  - New `/fuzzing` page is template-rendered; findings data is sourced from coordinator DB artifacts (`fozzy_summary_json` and `fozzy_results_zip`).
  - Domain-level summary metadata comes from `server_app/store.py::list_fozzy_summary_domains`.
  - Findings row flattening/paging/filtering/sorting is handled in `server.py` before JSON serialization.
  - Zip file browsing for Fozzy results is handled on-demand from DB artifact bytes (no extraction to disk required), with cached zip file index metadata.

- Fuzzing results enrichment boundary: summary flattening in server.py is now augmented by optional zip-based hydration from ozzy_results_zip (parsed once per domain and cached) so legacy summaries without rich fields can still render detailed baseline-vs-fuzz columns in the web UI.


- Added coordinator_ui_preferences table for durable operator UI state. Server routes own auth + HTTP handling; store layer owns upsert/query of JSON preference payloads by page/key.

- Fozzy response payload now includes esponse_headers (normalized map) in addition to header names, enabling rich modal rendering in coordinator web UI without extra network requests.

- Added modular deterministic fuzz response-analysis subsystem under fozzy_app/response_analysis (normalizer, feature extractor, baseline manager, diff engine, detector registry, scorer, summarizer, clusterer, pipeline).
- Integration boundary: fuzz_group now analyzes each live mutation against its baseline and records one structured analysis document per fuzz response to <domain>.fozzy.response_analysis.jsonl; anomaly/reflection artifacts embed response_analysis for downstream UI/reporting.
- Baseline model boundary: baseline profiles are keyed by method + normalized route pattern + MIME + parameter layout, then updated incrementally during run to keep comparison endpoint-scoped and noise-resistant.
- Ops UI observability boundary extension:
  - Docker/container and log-tail visibility now lives in `server.py` HTTP layer with template pages (`docker_status.html.j2`, `view_logs.html.j2`).
  - Data collection is host-runtime driven (docker CLI + filesystem log discovery) and exposed through coordinator-authenticated read-only APIs.
  - Log-tail endpoint resolves source IDs against enumerated sources each request, reducing arbitrary-path exposure risk.
- Observability boundary expansion:
  - `server.py` now includes an SSM-backed collector path for worker fleet container status/log tails.
  - Fleet status APIs remain coordinator-auth protected and return merged central + worker container inventories.
  - Remote execution uses AWS CLI (`ssm send-command` + `list-command-invocations`) so no new Python dependency was introduced.
- Observability architecture extension:
  - `server.py` now unifies log source abstraction across local docker, worker VM docker (SSM), EC2 console output, and filesystem files.
  - Log sources feed a parser pipeline that derives normalized events (time/severity/description/machine/source) for UI queries.
- Optional persistence boundary:
  - `logging_app/store.py` introduces a dedicated structured-log persistence path separate from coordinator state store.
  - Server can operate with or without log DB; query path falls back to live source reads when log DB is absent.
- Deployment boundary for container introspection:
  - Central server container now needs Docker socket mount + docker CLI to introspect central containers from inside the app container.
- Frontend resilience boundary:
  - UI templates now implement cache-first hydration (`localStorage`) on page entry, with live API fetch as reconciliation layer.
  - Backend API contract remains unchanged; caching is a client-side concern and should not alter route/store boundaries.
- Worker telemetry boundary refinement:
  - Worker execution (`coordinator.py`) now emits structured state/action logs directly in runtime (claim->execute->artifact->complete).
  - Coordinator API transport logging is centralized in `http_client.request_json(...)` and invoked by `CoordinatorClient`, so request/response tracing behavior stays consistent across all coordinator endpoints.
  - Logging backend (`nightmare_shared/logging_utils.py`) suppresses default httpx/httpcore info logs to preserve a single structured telemetry stream for View Logs and downstream parsing.
- Log delivery boundary refinement:
  - Backend log endpoints now prioritize full-fidelity payloads (no byte-slicing in source/download paths).
  - Presentation constraints (row/cell size control) are handled in templates/JS (`view_logs`, dashboard), keeping transport data complete for copy/export workflows.
- Crawl startup fault-tolerance refinement:
  - `nightmare.py` now treats page-existence criteria parse errors as recoverable configuration faults and continues with sanitized defaults.
  - Coordinator subprocess wrapper now records pre-exec failures in log artifacts and performs interpreter fallback for command-name portability.
- Coordinator target ingestion boundary update:
  - `register_targets.py` now requests replace semantics.
  - HTTP layer (`/api/coord/register-targets`) parses `replace_existing` and delegates behavior to store.
  - Store layer owns table replacement (`TRUNCATE coordinator_targets` + insert/upsert path).
- UI controls boundary consolidation:
  - Fuzzing table now delegates column hide/show/reorder and width persistence to shared grid-controls module.
  - Shared grid-controls now exposes consistent reorder behavior regardless of table mode, reducing page-specific divergence and maintenance.

- Worker subprocess error propagation boundary refinement:
  - `coordinator_app/runtime.py` now owns log-tail based subprocess failure summarization (`summarize_subprocess_failure`).
  - `coordinator.py` worker loops consume that helper so completion APIs and worker lifecycle logs carry root-cause detail instead of generic exit codes.

- Deploy/runtime boundary refinement:
  - `deploy/full_deploy_command.sh` now separates compose binary detection from daemon-access detection.
  - Compose diagnostics during coordinator readiness failure can run under invoking user, current user, or sudo-noninteractive mode, reducing false diagnostic failures on newly provisioned EC2 hosts.

- Startup dependency boundary refinement for server bootstrap:
  - Required stores (`CoordinatorStore`, `LogStore`) continue to hard-gate startup.
  - Optional auth0r profile store is now soft-initialized; failure degrades auth0r endpoints to 503 instead of crashing the whole HTTP server process.

- Observability boundary refinement:
  - `/errors` is now a dedicated UI surface rendered from `templates/errors.html.j2`.
  - `/api/errors` serves filtered error events from the structured log DB (`LogStore.query_error_events`), while `/api/coord/errors/ingest` is the write-side ingest endpoint.
  - Read/write responsibilities are split cleanly in `server.py` routing while storage remains in `logging_app/store.py`.

- Discovered-files contract hardening:
  - `server_app/store.py` now emits canonical UI fields for discovered/high-value file listings (`updated_at_utc`/`captured_at_utc`, `content_size_bytes`).
  - `server.py` discovered/high-value endpoints now return both `rows` (canonical) and `files` (compatibility alias) to decouple API evolution from template rollout timing.

- Auth0r overview query robustness:
  - `CoordinatorStore.auth0r_overview` now selects static `ORDER BY` clauses per mode (completed-only vs default recency), removing `CASE`-in-`ORDER BY` entirely from this path.

- Extractor page control-plane robustness:
  - `/extractor-matches` now has complete client-side wiring for pattern configuration read/write against server endpoints.
  - The page now reports domain-list load errors explicitly instead of silently continuing, reducing false "empty page" symptoms when upstream API calls fail.
- Coordinator deploy boundary refinement:
  - `COORDINATOR_BASE_URL` is now treated as the fleet-facing address, not a guaranteed self-call address on the central VM.
  - `deploy/full_deploy_command.sh` now resolves an effective local coordinator URL for control-plane actions executed on the central host (readiness polling, target registration, rollout initiation), reducing false-negative bootstrap failures on EC2 networking topologies that block/flake self-public-endpoint access.
- Coordinator startup sequencing note:
  - HTTP listeners are created only after `CoordinatorStore` and `LogStore` initialization.
  - Any long/blocking DB connect path in those constructors can make the container look healthy while no TCP listener is active.
  - Bounded connect timeouts now enforce fail-fast behavior so deploy diagnostics surface concrete DB connectivity issues instead of opaque API-unreachable symptoms.
- Bootstrap/log-db dependency boundary: central server startup is hard-dependent on dedicated log DB connectivity.
- Startup pipeline now includes pre-compose stale-endpoint detection for `LOG_DATABASE_URL`, preventing repeated coordinator container crash-loop when log DB VM was deleted.
- URL-validation boundary refinement:
  - `probe_url_existence` now separates transport strategy (`head_first`) from classification intent (negative-profile compare), allowing GET-first classification workflows while preserving HEAD-first optimization for generic probes.
  - Soft-404 baseline quality is now less sensitive to server-specific HEAD behavior.

- Coordinator scheduling architecture now has two independent lanes:
  - target-claim lane (Nightmare queue)
  - workflow stage lane (Auth0r/Fozzy/Extractor) driven by artifact readiness from central snapshot API.
- Stage scheduling is now centralized through `CoordinatorStore.schedule_stage(...)` decision logic to keep queue state transitions safe across multiple workers.
- Workflow definition source: `workflows/coordinator.workflow.json` (plugin-style metadata plus prerequisites/retry/parameters).
- Workflow/task orchestration boundary update (2026-04-22):
  - `coordinator_stage_tasks` is now workflow-aware (`workflow_id`) and stores resumable runtime state (`checkpoint_json`, `progress_json`, `progress_artifact_type`, `resume_mode`).
  - Stage claim path now supports "claim next plugin" semantics with optional plugin allowlist and per-domain running-task exclusion, so only one plugin runs at a time for a domain.
  - Coordinator worker model now has a unified plugin worker loop (`_plugin_worker_loop`) that claims generic plugin tasks and dispatches by plugin name.
  - Existing per-tool loops remain as compatibility fallback only when unified plugin worker count is disabled.
- Workflow operations UI boundary update (2026-04-22):
  - Web UI now has a dedicated workflow operations surface at `/workflows`.
  - Page is read/write against coordinator APIs (`workflow-snapshot`, `stage/enqueue`, `stage/reset`) and does not duplicate scheduler logic in the frontend.
  - Route rendering stays centralized through `reporting/server_pages.py` helpers, matching existing server page decomposition.
  - `/workflows` now includes a separate timeline pane fed by `/api/coord/events` (`workflow.task.*`) so task lifecycle observability is colocated with workflow controls.
- Workflow control-plane extension (2026-04-22):
  - Workflow definition lifecycle is now server-managed through explicit APIs (`/api/coord/workflow-config`, `/workflow/run`, `/workflow/mode`, `/workflow/reload`) instead of file-only/manual operations.
  - Recon workflow UI (`/workflows#recon`) edits plugin-level runtime parameters directly and can trigger full-domain workflow bootstrap.
  - Worker command plane now supports a config reload verb (`reload`) so scheduler/plugin workers can refresh workflow maps without process restart.
- Schema bootstrap/migration boundary refinement (2026-04-22):
  - Base schema bootstrap SQL must remain backward-compatible with prior table shapes.
  - Workflow-aware index/constraint upgrades for `coordinator_stage_tasks` are migration responsibilities, not base bootstrap assumptions.
  - Migration statements now commit independently to avoid full rollback of already-applied schema upgrades when a later migration statement fails.

- Recon workflow architecture extension (run-recon):
  - Recon tasks are implemented as first-class coordinator plugins and executed by existing plugin workers, not a separate engine.
  - Plugin progress durability uses two layers:
    - stage-level DB checkpoint/progress (`coordinator_stage_tasks`)
    - plugin-owned progress files uploaded as artifacts (`<plugin>_progress_json`) for cross-worker resume.
  - Completion gating uses explicit completion-flag artifacts (`<plugin>_complete_flag`) consumed by workflow prerequisites.
  - Spider stages reuse `nightmare.py --resume` over enumerated subdomain seeds and publish canonical Nightmare artifacts after each successful subdomain run.
  - High-value extraction stage reuses extractor runtime with workflow parameterized wordlist (`resources/wordlists/high_value_extractor_list.txt`).
- Recon extractor stage is now independent of Fozzy artifacts: it scans Nightmare domain output files directly with high-value regex rules and publishes both recon-specific and generic extractor artifacts.
- Plugin package boundary refinement (2026-04-22):
  - Coordinator workflow plugins are now represented as concrete classes in `plugins/` (`plugins/recon/spider/*` for spider variants).
  - `coordinator.py` plugin execution now resolves handlers through `plugins/registry.py` and executes through a shared `PluginExecutionContext`.
  - Plugin selection logic is centralized in registry mapping/prefix rules instead of duplicated switch branches in coordinator runtime flow.
- Request concurrency boundary refinement (2026-04-22):
  - Store claim logic now prevents cross-lane concurrent work on the same `root_domain`:
    - target claims skip domains with active running stage-task leases.
    - stage claims skip domains with active running target leases.
  - This ensures only one worker lane can issue requests for a domain at a time.
- Recon spider runtime default:
  - Spider throttle defaults to `0.5s` (`spider_throttle_seconds`, with `crawl_delay` backward-compatible alias).
