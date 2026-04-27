namespace NightmareV2.CommandCenter.Models;

public sealed record BusJournalRowDto(
    long Id,
    string Direction,
    string MessageType,
    string PayloadJson,
    DateTimeOffset OccurredAtUtc,
    string? ConsumerType);

public sealed record AssetGridRowDto(
    Guid Id,
    Guid TargetId,
    string Kind,
    string LifecycleStatus,
    string CanonicalKey,
    string RawValue,
    int Depth,
    string DiscoveredBy,
    DateTimeOffset DiscoveredAtUtc,
    bool HasDetails);

public sealed record WorkerSwitchDto(string WorkerKey, bool IsEnabled, DateTimeOffset UpdatedAtUtc);

public sealed record WorkerPatchRequest(bool Enabled);
