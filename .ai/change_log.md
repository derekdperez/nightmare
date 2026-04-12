# Change Log

## 2026-04-12

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
