# Open Questions

## 2026-04-24

- Recon Control task generation still intermittently diverges from builder-triggered workflow runs in some environments. Need a direct compare of:
  - exact request payloads,
  - resulting `coordinator_stage_tasks` row inserts,
  - immediate post-insert reset/delete events.

- Some deployments report many idle workers while queued tasks are not claimed. Need a quick diagnostic endpoint for:
  - per-status stage counts,
  - claim candidate count,
  - current worker state summary,
  - plugin allowlist visibility.
