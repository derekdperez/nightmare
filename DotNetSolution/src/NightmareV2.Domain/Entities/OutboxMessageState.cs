namespace NightmareV2.Domain.Entities;

public static class OutboxMessageState
{
    public const string Pending = "Pending";
    public const string InFlight = "InFlight";
    public const string Succeeded = "Succeeded";
    public const string Failed = "Failed";
    public const string DeadLetter = "DeadLetter";
}
