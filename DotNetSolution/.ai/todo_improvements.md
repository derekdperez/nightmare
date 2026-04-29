# TODO Improvements

- Implement transactional outbox/inbox tables and dispatcher workers for all critical event emissions.
- Replace `HttpRequestQueueState` string constants with enum + validated state machine transitions.
- Break `CommandCenter/Program.cs` into feature endpoint modules by bounded context.
- Add OpenTelemetry traces/metrics exporters and remove hot-path DB bus journaling dependency.
- Replace enum and portscan stubs with real implementations behind adapter interfaces and targeted tests.
- Introduce EF migrations and remove `EnsureCreated` + raw patch drift.
- Add integration tests with Testcontainers/LocalStack for queue, broker outage, and duplicate delivery scenarios.
