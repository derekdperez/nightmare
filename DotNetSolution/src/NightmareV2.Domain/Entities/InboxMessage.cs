namespace NightmareV2.Domain.Entities;

public sealed class InboxMessage
{
    public Guid Id { get; set; }
    public Guid EventId { get; set; }
    public string Consumer { get; set; } = "";
    public DateTimeOffset ProcessedAtUtc { get; set; } = DateTimeOffset.UtcNow;
}
