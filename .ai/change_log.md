# AI Change Log

## 2026-04-24

- Reduced primary navigation to operational pages and workflow-generated pages only.
- Removed legacy workflow monitor panels (enqueue/reset/timeline) and replaced plugin task actions with `Delete`, `Pause`, and `Run`.
- Added backend task-control support:
  - `POST /api/coord/stage/control` (server + fastapi),
  - `CoordinatorStore.control_stage_task(...)`,
  - `paused` status filter support in reset/status parsing,
  - force-run prerequisite override in claim path.
- Expanded recon results interface into consolidated output workspace:
  - recon summary table/actions,
  - crawl progress table,
  - discovered target domains + sitemap table,
  - discovered files + high-value file tables,
  - extractor domain/match table + zip download.
- Updated recon control domain API to include all discovered domains (not only snapshot domains) while retaining workflow task counters.
