# AI Change Log

## 2026-04-24

- Navigation and page model simplified:
  removed legacy pages and kept workers/workflows/workflow-generated pages/workflow builder/database/docker status/view logs.

- Workflow monitor/task controls reworked:
  task actions are now `Delete`, `Pause`, `Run` with backend support in `/api/coord/stage/control`.

- Recon workflow interfaces expanded:
  recon control/results pages now host consolidated recon operations and output views.

- Workflow run enqueue path hardened:
  added persistence verification diagnostics and retry logic when scheduled rows are not observed immediately.

- Force-run claim path fixed:
  tasks flagged with `force_run_override` can bypass running-target gate during candidate selection.

- `.ai` memory set refreshed:
  outdated conventions/architecture notes replaced with current architecture and workflow/runtime behavior.
