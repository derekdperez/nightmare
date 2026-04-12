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
- In `fozzy.py` incremental mode (no parameters file), `max_background_workers` controls how many domains run concurrently; each child domain process is intentionally constrained to single-worker fuzzing to maintain bounded total concurrency.
