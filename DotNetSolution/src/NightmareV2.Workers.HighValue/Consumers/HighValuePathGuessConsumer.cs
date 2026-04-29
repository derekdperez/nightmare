using MassTransit;
using NightmareV2.Application.Events;
using Microsoft.Extensions.Logging;
using NightmareV2.Application.Workers;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Workers.HighValue.Consumers;

/// <summary>
/// Publishes Raw URL <see cref="AssetDiscovered"/> events for high-value paths per indexed host asset (subdomain/domain).
/// HTTP probing is delegated to the existing Spider + Gatekeeper pipeline.
/// </summary>
public sealed class HighValuePathGuessConsumer(
    IWorkerToggleReader toggles,
    IInboxDeduplicator inbox,
    IEventOutbox outbox,
    IConfiguration configuration,
    HighValueWordlistBootstrap wordlists,
    ILogger<HighValuePathGuessConsumer> logger) : IConsumer<AssetDiscovered>
{
    public async Task Consume(ConsumeContext<AssetDiscovered> context)
    {
        if (!await inbox.TryBeginProcessingAsync(context.Message, nameof(HighValuePathGuessConsumer), context.CancellationToken).ConfigureAwait(false))
            return;

        var ct = context.CancellationToken;
        if (!await toggles.IsWorkerEnabledAsync(WorkerKeys.HighValuePaths, ct).ConfigureAwait(false))
            return;

        var m = context.Message;
        if (m.AdmissionStage != AssetAdmissionStage.Indexed)
            return;
        if (m.Kind is not (AssetKind.Subdomain or AssetKind.Domain))
            return;

        var max = configuration.GetValue("HighValuePaths:MaxProbesPerHost", 600);
        var host = m.RawValue.Trim().TrimEnd('/');
        if (host.Length == 0 || host.Contains(' ', StringComparison.Ordinal) || host.Contains("..", StringComparison.Ordinal))
            return;
        var causation = m.EventId == Guid.Empty ? m.CorrelationId : m.EventId;

        var baseUrl = host.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
                      || host.StartsWith("https://", StringComparison.OrdinalIgnoreCase)
            ? host.TrimEnd('/')
            : "https://" + host.TrimEnd('/');

        var published = 0;
        foreach (var (category, lines) in wordlists.Categories)
        {
            var by = $"hvpath:{category}";
            if (by.Length > 128)
                by = by[..128];

            foreach (var line in lines)
            {
                if (published >= max)
                    return;

                var path = line.StartsWith('/') ? line : "/" + line;
                var combined = baseUrl + path;
                if (!Uri.TryCreate(combined, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
                    continue;

                var url = uri.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped);
                var ctx = TruncateDiscoveryContext($"High-value paths: category \"{category}\" from host {baseUrl} → {url}");
                await outbox.EnqueueAsync(
                        new AssetDiscovered(
                            m.TargetId,
                            m.TargetRootDomain,
                            m.GlobalMaxDepth,
                            m.Depth + 1,
                            AssetKind.Url,
                            url,
                            by,
                            DateTimeOffset.UtcNow,
                            m.CorrelationId,
                            AssetAdmissionStage.Raw,
                            null,
                            ctx,
                            EventId: NewId.NextGuid(),
                            CausationId: causation,
                            Producer: "worker-highvalue-paths"),
                        ct)
                    .ConfigureAwait(false);
                published++;
            }
        }

        if (published > 0)
            logger.LogInformation("HighValuePaths: queued {Count} URL probes for host {Host}", published, host);
    }

    private static string TruncateDiscoveryContext(string s, int maxChars = 512) =>
        s.Length <= maxChars ? s : s[..(maxChars - 1)] + "…";
}
