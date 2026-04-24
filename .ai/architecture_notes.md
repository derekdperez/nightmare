# Architecture Notes

## 2026-04-24

- Workflow task control now supports explicit operator actions from the monitor:
  - `Delete`: removes the stage task row.
  - `Pause`: sets stage task to `paused` and clears active lease ownership.
  - `Run`: sets stage task to `ready` and marks one-shot prerequisite bypass via checkpoint override.
- Claim path consumes `force_run_override` and bypasses readiness gating for that claim, then clears override keys on transition to `running`.
- Recon output surfaces were consolidated into workflow interface page `recon_results` (crawl/discovery/files/extractor views).
- Primary navigation trimmed to operational pages and workflow-generated interfaces only.
