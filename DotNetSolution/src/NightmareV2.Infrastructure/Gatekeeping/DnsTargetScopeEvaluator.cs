using NightmareV2.Application.Gatekeeping;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Infrastructure.Gatekeeping;

/// <summary>
/// Minimal scope model: hosts must be the target root or a subdomain of it (design §4.2 step 3).
/// </summary>
public sealed class DnsTargetScopeEvaluator : ITargetScopeEvaluator
{
    public bool IsInScope(AssetDiscovered message, CanonicalAsset canonical)
    {
        var root = message.TargetRootDomain.Trim().TrimEnd('.').ToLowerInvariant();
        if (canonical.Kind is AssetKind.Url or AssetKind.ApiEndpoint or AssetKind.JavaScriptFile or AssetKind.MarkdownBody)
        {
            if (!Uri.TryCreate(canonical.NormalizedDisplay, UriKind.Absolute, out var uri))
                return false;
            return HostAllowed(uri.Host, root);
        }

        if (canonical.Kind is AssetKind.Subdomain or AssetKind.Domain)
        {
            var host = canonical.NormalizedDisplay.Trim().TrimEnd('.').ToLowerInvariant();
            return HostAllowed(host, root);
        }

        return true;
    }

    private static bool HostAllowed(string host, string root)
    {
        host = host.TrimEnd('.').ToLowerInvariant();
        root = root.TrimEnd('.').ToLowerInvariant();
        return host == root || host.EndsWith("." + root, StringComparison.Ordinal);
    }
}
