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
