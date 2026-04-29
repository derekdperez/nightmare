namespace NightmareV2.Contracts.Events;

/// <summary>Emitted when a finding is classified as critical (regex importance or sensitive path confirmation).</summary>
public sealed record CriticalHighValueFindingAlert(
    Guid FindingId,
    Guid TargetId,
    Guid? SourceAssetId,
    string PatternName,
    string SourceUrl,
    string Severity,
    DateTimeOffset OccurredAtUtc,
    Guid CorrelationId,
    Guid EventId = default,
    Guid CausationId = default,
    string SchemaVersion = "1",
    string Producer = "nightmare-v2") : IEventEnvelope;
