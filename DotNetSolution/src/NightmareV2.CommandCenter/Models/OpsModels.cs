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
    string DiscoveryContext,
    DateTimeOffset DiscoveredAtUtc,
    string LifecycleStatus);

public sealed record WorkerSwitchDto(string WorkerKey, bool IsEnabled, DateTimeOffset UpdatedAtUtc);

public sealed record WorkerCapabilityDto(
    string WorkerKey,
    string DisplayName,
    string Version,
    bool CanConsumeBusEvents,
    bool CanEmitAssets,
    bool CanPerformNetworkIOMainPath,
    bool IsStubbed);

public sealed record WorkerHealthDto(
    string WorkerKey,
    bool ToggleEnabled,
    DateTimeOffset? LastConsumeUtc,
    long BusConsumesLast1Hour,
    long BusConsumesLast24Hours,
    bool Healthy,
    string Reason);

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

public sealed record AssetCountByDomainDto(string RootDomain, long Count);

public sealed record DiscoveredByCountDto(string DiscoveredBy, long Count);

public sealed record AssetOpsSummaryDto(
    long TotalAssets,
    long TotalTargets,
    long AssetsLast1Hour,
    long AssetsLast24Hours,
    DateTimeOffset? LastAssetDiscoveredUtc,
    long AssetsInDiscoveredState,
    long AssetsInQueuedState,
    long AssetsConfirmedState,
    long FetchableAssetsStillDiscovered,
    long SubdomainsTotal,
    long DomainsTotal,
    long IpAddressesTotal,
    long UrlsTotal,
    long UrlsConfirmed,
    long HttpPipelineAssetsTotal,
    long HttpPipelineAssetsConfirmed,
    long HttpSnapshotsSaved,
    long OpenPortsTotal,
    long HighValueFindingsTotal,
    IReadOnlyList<AssetCountByDomainDto> TopDomainsByAssetCount,
    IReadOnlyList<DiscoveredByCountDto> AssetsByDiscoveredBy);

public sealed record BusTrafficSummaryDto(
    long PublishesLast1Hour,
    long PublishesLast24Hours,
    long ConsumesLast1Hour,
    long ConsumesLast24Hours);

public sealed record RabbitQueueBriefDto(
    string Name,
    int Messages,
    int MessagesReady,
    int MessagesUnacknowledged,
    int Consumers,
    string? LikelyWorkerKey);

public sealed record WorkerDetailStatsDto(
    string WorkerKey,
    long BusConsumesLast1Hour,
    long BusConsumesLast24Hours,
    DateTimeOffset? LastConsumeUtc,
    long DbAssetsAttributedLast1Hour,
    long DbAssetsAttributedLast24Hours,
    long RabbitMessagesReady,
    long RabbitMessagesUnacked,
    IReadOnlyList<string> MatchedRabbitQueueNames);

/// <summary>Aggregated Ops dashboard payload (polled every few seconds).</summary>
public sealed record HighValueFindingRowDto(
    Guid Id,
    Guid TargetId,
    Guid? SourceAssetId,
    string FindingType,
    string Severity,
    string PatternName,
    string? Category,
    string? MatchedText,
    string SourceUrl,
    string WorkerName,
    int? ImportanceScore,
    DateTimeOffset DiscoveredAtUtc,
    string? TargetRootDomain);

public sealed record OpsSnapshotDto(
    IReadOnlyList<WorkerSwitchDto> Workers,
    WorkerActivitySnapshotDto WorkerActivity,
    AssetOpsSummaryDto Assets,
    BusTrafficSummaryDto BusTraffic,
    IReadOnlyList<WorkerDetailStatsDto> WorkerMetrics,
    IReadOnlyList<RabbitQueueBriefDto> RabbitQueues,
    bool RabbitManagementAvailable);

public sealed record OpsOverviewDto(
    long TotalTargets,
    long TotalAssetsConfirmed,
    long TotalUrlAssets,
    long UrlsFromFetchedPages,
    long UrlsFromScripts,
    long UrlsGuessedWithWordlist,
    string? TopDomainByAssets,
    long TopDomainAssetCount,
    long DomainsWith10OrMoreAssets,
    long DomainsWith10OrFewerAssets);


public sealed record HttpRequestQueueSettingsDto(
    bool Enabled,
    int GlobalRequestsPerMinute,
    int PerDomainRequestsPerMinute,
    int MaxConcurrency,
    int RequestTimeoutSeconds,
    DateTimeOffset UpdatedAtUtc);

public sealed record HttpRequestQueueSettingsPatch(
    bool Enabled,
    int GlobalRequestsPerMinute,
    int PerDomainRequestsPerMinute,
    int MaxConcurrency,
    int RequestTimeoutSeconds);

public sealed record HttpRequestQueueRowDto(
    Guid Id,
    Guid AssetId,
    Guid TargetId,
    string AssetKind,
    string Method,
    string RequestUrl,
    string DomainKey,
    string State,
    int AttemptCount,
    int MaxAttempts,
    int Priority,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc,
    DateTimeOffset NextAttemptAtUtc,
    DateTimeOffset? StartedAtUtc,
    DateTimeOffset? CompletedAtUtc,
    int? LastHttpStatus,
    string? LastError,
    long? DurationMs,
    string? ResponseContentType,
    long? ResponseContentLength,
    string? FinalUrl);


public sealed record HttpRequestQueueMetricsDto(
    long QueuedCount,
    long ReadyRetryCount,
    long ScheduledRetryCount,
    long InFlightCount,
    long FailedCount,
    long CompletedLastHourCount,
    long BacklogCount,
    DateTimeOffset? OldestQueuedAtUtc,
    long? OldestQueuedAgeSeconds);

public sealed record ReliabilitySloSnapshotDto(
    DateTimeOffset AtUtc,
    long EventPublishesLast1Hour,
    long EventConsumesLast1Hour,
    decimal EventProcessingSuccessRate1Hour,
    long QueueBacklogCount,
    long? QueueBacklogAgeSeconds,
    long QueueCompletedLastHour,
    long WorkerErrorsLast1Hour,
    bool ApiReady);
