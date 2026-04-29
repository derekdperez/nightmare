# Domain Glossary

- Target: root domain registered for reconnaissance.
- Raw asset: unverified candidate discovery (`AssetAdmissionStage.Raw`).
- Indexed asset: gatekeeper-approved asset (`AssetAdmissionStage.Indexed`).
- Lifecycle status:
  - `Queued`: awaiting HTTP verification.
  - `Confirmed`: HTTP success and not soft-404.
  - `NonExistent`: failed/non-success or soft-404 response.
- HTTP request queue: durable Postgres-backed work queue for URL/domain fetches.
- High-value finding: regex/path-based security-relevant signal extracted from confirmed responses.
- Event envelope:
  - `EventId`: unique identifier for emitted event.
  - `CorrelationId`: trace chain across workflow.
  - `CausationId`: parent event that caused current emission.
