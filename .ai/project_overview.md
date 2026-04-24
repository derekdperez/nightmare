# Project Overview

- Purpose: coordinator-driven recon and workflow orchestration with operational web UI.
- Major components:
  - `server.py` coordinator UI/API runtime
  - `server_app/store.py` persistence, scheduler, task lifecycle
  - `workflow` JSON definitions + workflow interface templates
- Runtime model:
  - central coordinator persists tasks/artifacts/sessions and workers claim runnable tasks from shared queue.
