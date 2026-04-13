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
