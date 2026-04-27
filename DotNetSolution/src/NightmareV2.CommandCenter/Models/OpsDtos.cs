namespace NightmareV2.CommandCenter.Models;

public sealed record BusJournalRowDto(
    long Id,
    string Direction,
    string MessageType,
    string? ConsumerType,
    string PayloadJson,
    DateTimeOffset OccurredAtUtc);

public sealed record AssetGridRowDto(
    Guid Id,
    Guid TargetId,
    string Kind,
    string CanonicalKey,
    string RawValue,
    int Depth,
    string DiscoveredBy,
    DateTimeOffset DiscoveredAtUtc,
    string LifecycleStatus);

public sealed record WorkerToggleRowDto(string WorkerKey, bool IsEnabled, DateTimeOffset UpdatedAtUtc);

public sealed record WorkerPatchRequest(bool Enabled);
