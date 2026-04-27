namespace NightmareV2.Contracts.Events;

/// <summary>
/// Gatekeeper approved an IP (or host resolving to IP) for targeted port discovery (design §4.3).
/// </summary>
public record PortScanRequested(
    Guid TargetId,
    string TargetRootDomain,
    int GlobalMaxDepth,
    int Depth,
    string HostOrIp,
    Guid AssetId,
    Guid CorrelationId);
