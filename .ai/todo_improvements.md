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
- Integrate `nightmare_shared/page_classification.py` into crawl/probe existence decisions so catch-all redirect/login/block pages are baseline-classified instead of relying mostly on status-code heuristics.
- Add guarded exception handling around Auth0r POST handlers in `server.py` (`/api/coord/auth0r/profile/save`, `/identity/save`, etc.) so runtime store errors return structured JSON 500s instead of browser-level fetch failures.
- Add dedicated integration tests for new workflow plugin APIs/endpoints (`stage/claim-next`, `stage/progress`, `stage/reset`) including workflow_id-scoped behavior.
- Add crash-handoff resume tests that verify checkpoint/progress continuity across worker interruption and re-claim for each legacy plugin adapter (`fozzy`, `extractor`, `auth0r`).

- Add targeted coordinator unit tests for recon plugin runtime methods (subdomain probe resume, per-subdomain spider resume, completion-flag artifact emission).
- Add deploy bootstrap check/install for Sublist3r so active enumeration path is deterministic across fresh workers.
- Add API-level tests for new workflow control endpoints (`workflow-config`, `workflow/run`, `workflow/mode`, `workflow/reload`) including malformed parameter JSON and workflow-not-found cases.
- Add optimistic concurrency/versioning for workflow file saves to prevent accidental overwrite when multiple operators edit `/workflows#recon` simultaneously.
