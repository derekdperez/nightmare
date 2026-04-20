# TODO Improvements

- Consider adding a lightweight incremental fingerprint strategy that avoids full recursive scans of every domain folder on every run.
- Consider a startup summary timer (e.g., elapsed time for dirty-check stage) to aid diagnostics on large datasets.
- Consider surfacing quick-fuzz-list path validation earlier with a clearer remediation hint in CLI output.
- Continue god-file decomposition in staged slices:
  - `nightmare.py`: split CLI/config parsing, crawl orchestration, and output artifact writers into dedicated modules.
  - `fozzy.py`: split report rendering payload builders from fuzz execution control flow.
  - `extractor.py`: split matcher engine, summary aggregation, and I/O/reporting boundaries.
- Implement worker command consumption loop for queued control commands (`start`/`pause`/`stop`) from `coordinator_worker_commands`; commands are currently enqueued by server APIs/UI but not applied by `coordinator.py`.
- Add pagination + server-side source selection caching for `/api/coord/log-events` when querying `source_id=__all__` on very large fleets.
- Add explicit UI diagnostics panel on View Logs for SSM/EC2 permission failures and target-filter mismatches.
- Add optional asynchronous ingestion worker for log DB writes to decouple source polling latency from API response times.

- Fix `nightmare.py` undefined variable path (`verify_timeout` in `crawl_domain`) and add a regression test so coordinator runs stop failing with repeated `NameError`.
