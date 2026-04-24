# Conventions

## Code Ownership Boundaries

- HTTP routing/page serving:
  `server.py`.
- Coordinator persistence, queue operations, readiness/claim logic:
  `server_app/store.py`.
- Distributed worker runtime loops:
  `coordinator.py`.
- Workflow JSON definitions:
  `workflows/*.workflow.json`.
- Workflow interface pages:
  `templates/workflow_interfaces/*.j2`.

## Workflow and Task Conventions

- Use normalized lowercase identifiers for:
  `workflow_id`, `root_domain`, `stage/plugin_name`.
- Stage task statuses in queue logic:
  `pending`, `ready`, `running`, `paused`, `completed`, `failed`.
- `pending` means blocked on prerequisites; `ready` is claimable.
- Operator `Run` means:
  force ready + one-shot prerequisite override for next claim.

## API Surface Conventions

- `/api/coord/*`:
  coordinator queue, worker control, artifacts, workflow interface data.
- `/api/workflow-definitions*` and `/api/workflow-runs*`:
  workflow builder/run APIs (served by `server.py`).
- When adding operator features, wire server route + template behavior together in the same change.

## UI Conventions

- Keep removed pages removed; do not reintroduce legacy nav targets.
- Current canonical pages are:
  workers, workflows, workflow-generated pages, workflow builder, database, docker status, view logs.
- Recon-specific operational data belongs in recon results interface, not separate legacy pages.
