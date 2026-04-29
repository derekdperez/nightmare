namespace NightmareV2.Domain.Entities;

public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public string MessageType { get; set; } = "";
    public string PayloadJson { get; set; } = "";
    public Guid EventId { get; set; }
    public Guid CorrelationId { get; set; }
    public Guid CausationId { get; set; }
    public DateTimeOffset OccurredAtUtc { get; set; }
    public string Producer { get; set; } = "";
    public string State { get; set; } = OutboxMessageState.Pending;
    public int AttemptCount { get; set; }
    public DateTimeOffset CreatedAtUtc { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset UpdatedAtUtc { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset NextAttemptAtUtc { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? DispatchedAtUtc { get; set; }
    public string? LastError { get; set; }
    public string? LockedBy { get; set; }
    public DateTimeOffset? LockedUntilUtc { get; set; }
}
