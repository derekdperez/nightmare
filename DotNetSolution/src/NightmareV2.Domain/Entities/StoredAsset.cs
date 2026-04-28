using NightmareV2.Contracts;

namespace NightmareV2.Domain.Entities;

public class StoredAsset
{
    public Guid Id { get; set; }
    public Guid TargetId { get; set; }
    public ReconTarget? Target { get; set; }
    public AssetKind Kind { get; set; }
    /// <summary>Normalized identity key (URL without fragment, lowercased host, etc.).</summary>
    public string CanonicalKey { get; set; } = "";
    public string RawValue { get; set; } = "";
    public int Depth { get; set; }
    public string DiscoveredBy { get; set; } = "";
    /// <summary>Human-readable description of how the asset was found (parent page, wordlist category, etc.).</summary>
    public string DiscoveryContext { get; set; } = "";
    public DateTimeOffset DiscoveredAtUtc { get; set; }

    /// <summary><see cref="AssetLifecycleStatus"/> values.</summary>
    public string LifecycleStatus { get; set; } = AssetLifecycleStatus.Queued;

    /// <summary>Type-specific payload (URL fetch request/response, timings, etc.).</summary>
    public string? TypeDetailsJson { get; set; }
}
