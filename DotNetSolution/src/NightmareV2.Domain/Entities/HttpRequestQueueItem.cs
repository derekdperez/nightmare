using NightmareV2.Contracts;

namespace NightmareV2.Domain.Entities;

public sealed class HttpRequestQueueItem
{
    public Guid Id { get; set; }
    public Guid AssetId { get; set; }
    public StoredAsset? Asset { get; set; }
    public Guid TargetId { get; set; }
    public AssetKind AssetKind { get; set; }
    public string Method { get; set; } = "GET";
    public string RequestUrl { get; set; } = "";
    public string DomainKey { get; set; } = "";
    public string State { get; set; } = HttpRequestQueueState.Queued;
    public int Priority { get; set; }
    public int AttemptCount { get; set; }
    public int MaxAttempts { get; set; } = 3;
    public DateTimeOffset CreatedAtUtc { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset UpdatedAtUtc { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset NextAttemptAtUtc { get; set; } = DateTimeOffset.UtcNow;
    public string? LockedBy { get; set; }
    public DateTimeOffset? LockedUntilUtc { get; set; }
    public DateTimeOffset? StartedAtUtc { get; set; }
    public DateTimeOffset? CompletedAtUtc { get; set; }
    public long? DurationMs { get; set; }
    public int? LastHttpStatus { get; set; }
    public string? LastError { get; set; }
    public string? RequestHeadersJson { get; set; }
    public string? RequestBody { get; set; }
    public string? ResponseHeadersJson { get; set; }
    public string? ResponseBody { get; set; }
    public string? ResponseContentType { get; set; }
    public long? ResponseContentLength { get; set; }
    public string? FinalUrl { get; set; }
}
