namespace NightmareV2.Domain.Entities;

public sealed class BusJournalEntry
{
    public long Id { get; set; }
    public string Direction { get; set; } = "";
    public string MessageType { get; set; } = "";
    public string? ConsumerType { get; set; }
    public string PayloadJson { get; set; } = "";
    public DateTimeOffset OccurredAtUtc { get; set; }
    /// <summary>Process/container host name (e.g. Docker hostname) for correlating journal rows with a worker instance.</summary>
    public string HostName { get; set; } = "";
}
