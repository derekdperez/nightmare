# Architecture Notes

- Core pattern: Raw `AssetDiscovered` events are gatekept, canonicalized, deduped, persisted, then republished as Indexed events.
- HTTP probing architecture currently relies on a durable Postgres queue drained by `HttpRequestQueueWorker` in spider service.
- Event contracts now carry envelope metadata (`EventId`, `CorrelationId`, `CausationId`, `OccurredAtUtc`, `SchemaVersion`, `Producer`) for tracing and idempotency groundwork.
- Reliability gap reduced: `ScannableContentAvailable` publishing in `EfAssetPersistence` now retries and fails loudly after retry exhaustion instead of silent swallow.
- Ops baseline expanded with explicit reliability/worker introspection endpoints:
  - `/api/ops/reliability-slo`
  - `/api/workers/capabilities`
  - `/api/workers/health`
- Security hardening added for diagnostics and maintenance endpoints: enabled mode now requires explicit API key configuration.
