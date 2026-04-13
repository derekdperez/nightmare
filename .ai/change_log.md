# Change Log

## 2026-04-12

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
