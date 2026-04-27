namespace NightmareV2.Contracts;

/// <summary>
/// <see cref="Raw"/> — ingress to Gatekeeper (normalize, dedupe, persist).
/// <see cref="Indexed"/> — persisted canonical asset; workers (spider, etc.) react.
/// </summary>
public enum AssetAdmissionStage
{
    Raw = 0,
    Indexed = 1,
}
