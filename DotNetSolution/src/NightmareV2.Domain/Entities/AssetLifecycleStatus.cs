namespace NightmareV2.Domain.Entities;

public static class AssetLifecycleStatus
{
    public const string Discovered = "Discovered";
    /// <summary>URL queued for probe (e.g. high-value path wordlist); not yet confirmed by HTTP.</summary>
    public const string Queued = "Queued";
    public const string Confirmed = "Confirmed";
}
