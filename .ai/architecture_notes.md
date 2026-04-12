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
  - Per-domain fuzzing inside each child: constrained to single-worker in incremental mode to keep global concurrency bounded and avoid shared-global state races.
