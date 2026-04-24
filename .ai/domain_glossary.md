# Domain Glossary

- `workflow`:
  ordered plugin-stage definition, currently file-backed in `workflows/*.workflow.json`.

- `workflow definition`:
  DB-backed builder entity (`workflow_definitions`) used by Workflow Builder and workflow runs API.

- `workflow run`:
  execution instance from builder APIs (`workflow_runs` + `workflow_step_runs`), bridged to coordinator stage tasks.

- `stage task`:
  queue row in `coordinator_stage_tasks` for one `(workflow_id, root_domain, stage)` execution unit.

- `artifact`:
  persisted output payload in `coordinator_artifacts` (progress, summaries, zips, session data).

- `workflow interface`:
  auto-discovered UI page declared under `interfaces` in workflow JSON (control/results route + template).

- `force run override`:
  one-shot checkpoint flag used by task `Run` control to bypass normal prerequisite gating on next claim.
