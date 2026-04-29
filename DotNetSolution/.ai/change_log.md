# AI Change Log

## 2026-04-29

- Standardized event envelope fields across contract events (`EventId`, `CausationId`, `SchemaVersion`, `Producer`; `OccurredAtUtc` exposed consistently).
  - Why: establish traceability and idempotency foundation for reliability redesign.
- Updated all known event emitters to populate envelope metadata and causation chains.
  - Why: avoid empty metadata and enable immediate ops debugging value.
- Hardened diagnostics/maintenance endpoint behavior so enabled mode requires configured API keys.
  - Why: prevent accidental exposure in production-like environments.
- Added reliability baseline endpoints:
  - `/api/ops/reliability-slo`
  - `/api/workers/capabilities`
  - `/api/workers/health`
  - Why: provide measurable reliability and worker-operational signals before deeper refactors.
- Removed silent event-loss behavior in `EfAssetPersistence` by adding publish retries and terminal failure.
  - Why: eliminate unobservable publish drops for scanner-trigger events.
- Validation: `dotnet build NightmareV2.slnx -c Release` succeeded.
