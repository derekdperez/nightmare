namespace NightmareV2.CommandCenter.Models;

public sealed record BusJournalRowDto(
    long Id,
    string Direction,
    string MessageType,
    string PayloadJson,
    DateTimeOffset OccurredAtUtc,
    string? ConsumerType,
    string HostName);

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

public sealed record WorkerSwitchDto(string WorkerKey, bool IsEnabled, DateTimeOffset UpdatedAtUtc);

public sealed record WorkerPatchRequest(bool Enabled);

/// <summary>Per process/container: last completed consume from the shared bus journal.</summary>
public sealed record WorkerInstanceActivityDto(
    string HostName,
    string WorkerKind,
    string ConsumerShortName,
    bool? ToggleEnabled,
    DateTimeOffset LastCompletedAtUtc,
    string LastMessageType,
    string LastPayloadPreview,
    string ActivityLabel);

/// <summary>Roll-up by logical worker (matches <c>worker_switches.worker_key</c>).</summary>
public sealed record WorkerKindSummaryDto(
    string WorkerKey,
    bool ToggleEnabled,
    int InstanceCount,
    DateTimeOffset? LastActivityUtc,
    string RollupActivityLabel);

public sealed record WorkerActivitySnapshotDto(
    IReadOnlyList<WorkerKindSummaryDto> Summaries,
    IReadOnlyList<WorkerInstanceActivityDto> Instances);
