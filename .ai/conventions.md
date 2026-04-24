# Conventions

## Architecture Boundaries

- `server.py` is the production coordinator UI/API runtime.
  - It serves operator HTML pages.
  - It exposes `/api/coord/*` coordinator APIs.
  - It should remain compatible with deploy flags: `--http-port`, `--https-port`, `--cert-file`, `--key-file`, and legacy `--port`.

- `server.py` should own HTTP routing and response behavior only.
  - Coordinator DB access belongs in `server_app/store.py`.
  - Template-backed page rendering belongs in `reporting/server_pages.py`.
  - Operator UI templates live under `templates/*.j2`.

- Do not reintroduce large inline HTML render methods in `server.py`.

- `coordinator.py` and `coordinator_app/runtime.py` own runtime orchestration.
  - Any class/function instantiated by `coordinator_app/runtime.py` must be defined or imported in that module.
  - Avoid relying on symbols that only exist in `coordinator.py` script scope.

- Shared crawler/fuzzer/extractor policy logic should live in importable modules:
  - `nightmare_app/*`
  - `fozzy_app/*`
  - `nightmare_shared/*`
  - plugin modules under `plugins/`

## Runtime Model

- Central host responsibilities:
  - FastAPI/web control plane
  - coordinator APIs
  - workflow scheduling
  - worker command/control
  - coordinator PostgreSQL access
  - dedicated log DB access
  - artifact/report browsing

- Worker VM responsibilities:
  - run coordinator in worker mode
  - claim plugin tasks through coordinator APIs
  - execute plugin handlers
  - persist progress/checkpoints
  - upload artifacts
  - send heartbeats and consume commands

- Runtime execution is plugin-worker-only.
  - Start generic plugin workers plus optional workflow scheduler.
  - Do not start legacy dedicated `nightmare`, `fozzy`, `extractor`, or `auth0r` worker loops.
  - `plugin_workers` is authoritative and should default to at least `1` for worker profiles.

## Workflow Scheduling

- Workflow tasks are represented in `coordinator_stage_tasks`.

- Plugin task identity is scoped by:

  ```text
  workflow_id + root_domain + plugin_name