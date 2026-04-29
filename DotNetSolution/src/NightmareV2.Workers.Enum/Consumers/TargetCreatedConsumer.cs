using MassTransit;
using NightmareV2.Application.Events;
using NightmareV2.Application.Workers;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Workers.Enum.Consumers;

/// <summary>
/// Passive discovery entry point. Stub emits subdomains as Raw assets for Gatekeeper.
/// </summary>
public sealed class TargetCreatedConsumer(ILogger<TargetCreatedConsumer> logger, IWorkerToggleReader toggles, IEventOutbox outbox)
    : IConsumer<TargetCreated>
{
    public async Task Consume(ConsumeContext<TargetCreated> context)
    {
        if (!await toggles.IsWorkerEnabledAsync(WorkerKeys.Enumeration, context.CancellationToken).ConfigureAwait(false))
        {
            logger.LogDebug("Enumeration disabled; skipping target {TargetId}", context.Message.TargetId);
            return;
        }

        var m = context.Message;
        logger.LogInformation("Enumeration starting for {Domain} target {TargetId}", m.RootDomain, m.TargetId);

        var correlation = m.CorrelationId == Guid.Empty ? NewId.NextGuid() : m.CorrelationId;
        var causation = m.EventId == Guid.Empty ? correlation : m.EventId;
        var subs = new[] { $"www.{m.RootDomain}", $"api.{m.RootDomain}", $"dev.{m.RootDomain}" };

        foreach (var sub in subs)
        {
            await outbox.EnqueueAsync(
                    new AssetDiscovered(
                        m.TargetId,
                        m.RootDomain,
                        m.GlobalMaxDepth,
                        Depth: 1,
                        Kind: AssetKind.Subdomain,
                        RawValue: sub,
                        DiscoveredBy: "enum-worker-stub",
                        OccurredAt: DateTimeOffset.UtcNow,
                        CorrelationId: correlation,
                        AdmissionStage: AssetAdmissionStage.Raw,
                        AssetId: null,
                        DiscoveryContext: "Enumeration stub: suggested subdomain emitted when target was created",
                        EventId: NewId.NextGuid(),
                        CausationId: causation,
                        Producer: "worker-enum"),
                    context.CancellationToken)
                .ConfigureAwait(false);
        }
    }
}
