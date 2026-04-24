# Architecture Notes

## Active Design

- `server.py` is the main production web/API runtime.
  - Serves core pages (`/workers`, `/workflows`, `/workflow-definitions`, `/database`, `/docker-status`, `/view-logs`).
  - Serves workflow-generated UI routes (for example `/workflow-interfaces/run-recon/control` and `/workflow-interfaces/run-recon/results`).
  - Exposes both:
    - workflow-builder APIs (`/api/workflow-definitions*`, `/api/workflow-runs*`)
    - coordinator APIs (`/api/coord/*`).

- `server_app/fastapi_app.py` provides a coordinator API surface used by FastAPI deployment paths.
  - Not full feature parity with all `server.py` web/builder routes.
  - Includes stage lifecycle and `/api/coord/workflow/run`.

- `coordinator.py` runtime is plugin-worker centric.
  - Starts plugin workers (`plugin_workers`) and optional scheduler loop.
  - Legacy dedicated worker loops are intentionally disabled in `run()`.

## Queue and Claim Rules

- Stage task identity:
  `workflow_id + root_domain + stage`.
- Claim path:
  `CoordinatorStore.try_claim_stage_with_resources()`.
- Readiness transition:
  `refresh_stage_task_readiness()` re-evaluates pending/ready tasks based on workflow preconditions.
- Manual task controls:
  - `Delete`: remove stage task row.
  - `Pause`: set `paused`, clear lease/worker.
  - `Run`: set `ready` + one-shot `force_run_override`.
- Force-run behavior:
  claim candidate selection allows `force_run_override` tasks to bypass normal running-target gating.

## Recon Workflow Surfaces

- Recon control page:
  task generation + per-step parameter overrides + task clearing.
- Recon results page:
  recon rollup + crawl progress + discovered targets/files + extractor visibility.
