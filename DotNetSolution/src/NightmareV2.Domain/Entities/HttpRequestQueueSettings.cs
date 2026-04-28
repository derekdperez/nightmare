namespace NightmareV2.Domain.Entities;

public sealed class HttpRequestQueueSettings
{
    public int Id { get; set; } = 1;
    public bool Enabled { get; set; } = true;
    public int GlobalRequestsPerMinute { get; set; } = 120;
    public int PerDomainRequestsPerMinute { get; set; } = 6;
    public int MaxConcurrency { get; set; } = 8;
    public int RequestTimeoutSeconds { get; set; } = 30;
    public DateTimeOffset UpdatedAtUtc { get; set; } = DateTimeOffset.UtcNow;
}
