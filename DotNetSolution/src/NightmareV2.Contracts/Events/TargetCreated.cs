namespace NightmareV2.Contracts.Events;

/// <summary>
/// User (or automation) registered a new in-scope target root (design §5 step 1).
/// </summary>
public record TargetCreated(
    Guid TargetId,
    string RootDomain,
    int GlobalMaxDepth,
    DateTimeOffset OccurredAt,
    Guid CorrelationId);
