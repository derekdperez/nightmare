# Conventions

- Event contracts: use record types in `NightmareV2.Contracts.Events`; include envelope metadata fields for every new event.
- Event publication:
  - Prefer preserving inbound `CorrelationId`.
  - Set `CausationId` from parent event `EventId` when available.
  - Set `Producer` to the emitting service (`command-center`, `gatekeeper`, `worker-*`).
- Queue state values currently remain string constants (`HttpRequestQueueState` static class). Transition to enum-backed state machine is planned.
- Maintenance/diagnostics safety:
  - Endpoint enable flags alone are insufficient.
  - When enabled, corresponding API key must be configured; otherwise endpoint reports misconfiguration.
