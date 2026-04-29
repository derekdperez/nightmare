namespace NightmareV2.Contracts.Events;

/// <summary>
/// Standard event envelope metadata used for correlation, tracing, and schema evolution.
/// </summary>
public interface IEventEnvelope
{
    Guid EventId { get; }
    Guid CorrelationId { get; }
    Guid CausationId { get; }
    DateTimeOffset OccurredAtUtc { get; }
    string SchemaVersion { get; }
    string Producer { get; }
}
