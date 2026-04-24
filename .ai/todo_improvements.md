# TODO Improvements

## 2026-04-24

- Add enqueue/claim diagnostics API:
  - scoped counts by `workflow_id`, `root_domain`, `stage`, `status`,
  - recent matching event log slice (`workflow.task.enqueued`, `workflow.task.reset`, `workflow.task.control`, `workflow.task.claimed`).

- Add worker-claim health panel in Workflows UI:
  - ready-task count,
  - blocked-task count,
  - idle/running worker counts,
  - plugin allowlist currently applied by runtime workers.

- Normalize Recon Control generation path to use the same backend initiation flow as working builder launch paths where possible.
