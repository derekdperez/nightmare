using System.Net;
using NightmareV2.Application.Workers;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Infrastructure.Workers;

public sealed class DefaultEnumerationService : IEnumerationService
{
    private static readonly string[] CandidatePrefixes =
    [
        "www",
        "api",
        "dev",
        "staging",
        "admin",
        "app",
        "portal",
        "cdn",
        "m",
        "beta",
    ];

    public async Task<IReadOnlyList<string>> DiscoverSubdomainsAsync(
        TargetCreated target,
        CancellationToken cancellationToken = default)
    {
        var discovered = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var prefix in CandidatePrefixes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var candidate = $"{prefix}.{target.RootDomain}".Trim().TrimEnd('.');
            try
            {
                var addrs = await Dns.GetHostAddressesAsync(candidate, cancellationToken).ConfigureAwait(false);
                if (addrs.Length > 0)
                    discovered.Add(candidate);
            }
            catch
            {
                // Resolution failures are expected for many candidates.
            }
        }

        return discovered.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList();
    }
}
