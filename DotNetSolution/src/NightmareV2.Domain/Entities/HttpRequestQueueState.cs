namespace NightmareV2.Domain.Entities;

public static class HttpRequestQueueState
{
    public const string Queued = "Queued";
    public const string InFlight = "InFlight";
    public const string Succeeded = "Succeeded";
    public const string Retry = "Retry";
    public const string Failed = "Failed";
}
