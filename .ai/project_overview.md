# Project Overview

## Purpose

Nightmare is a coordinator-driven recon and workflow orchestration platform with:
- a web control plane,
- a shared task queue,
- distributed workers that claim and run workflow stage tasks.

## Major Components

- `server.py`:
  primary HTTP server for operator pages and APIs.
- `server_app/store.py`:
  PostgreSQL persistence and queue/task lifecycle logic.
- `coordinator.py`:
  distributed worker runtime (plugin workers + optional scheduler loop).
- `workflows/*.workflow.json`:
  file-based workflow definitions and interface metadata.
- `templates/workflow_interfaces/*`:
  workflow-specific control/results pages (currently recon control/results).

## Runtime Model

- Stage task source of truth: `coordinator_stage_tasks`.
- Artifacts source of truth: `coordinator_artifacts`.
- Workers claim ready tasks via `/api/coord/stage/claim-next`.
- Workflow interfaces are auto-discovered from workflow JSON and injected into nav on server startup.
