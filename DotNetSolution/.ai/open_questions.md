# Open Questions

- Should event publication failures in persistence paths trigger immediate transaction rollback (strict consistency) or durable outbox buffering (eventual consistency)?
- Should diagnostics/maintenance endpoints move to full auth middleware/policy instead of API-key header checks?
- Should `ScannableContentAvailable` keep `StoredAtUtc` long-term or migrate fully to `OccurredAtUtc` naming?
- What is the desired health threshold per worker class (current implementation uses generic recent-consume heuristics)?
- When migrating to SQS/SNS, do we require dual-write transport period or cutover by service group?
