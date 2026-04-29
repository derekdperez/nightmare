using MassTransit;
using NightmareV2.Application.Events;
using Microsoft.Extensions.Logging;
using NightmareV2.Application.Workers;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Application.Gatekeeping;

/// <summary>
/// Asset Gatekeeper: Raw admissions are normalized, deduped, persisted, then re-published as Indexed for workers.
/// </summary>
public sealed class GatekeeperOrchestrator(
    IAssetCanonicalizer canonicalizer,
    IAssetDeduplicator deduplicator,
    ITargetScopeEvaluator scope,
    IAssetPersistence persistence,
    IEventOutbox outbox,
    IWorkerToggleReader workerToggles,
    ILogger<GatekeeperOrchestrator> logger)
{
    public async Task ProcessAsync(AssetDiscovered message, CancellationToken cancellationToken = default)
    {
        if (message.AdmissionStage != AssetAdmissionStage.Raw)
            return;

        if (!await workerToggles.IsWorkerEnabledAsync(WorkerKeys.Gatekeeper, cancellationToken).ConfigureAwait(false))
        {
            logger.LogDebug("Gatekeeper disabled; skipping Raw {Raw}", message.RawValue);
            return;
        }

        if (message.Depth > message.GlobalMaxDepth)
        {
            logger.LogDebug("Dropping {Raw} depth {Depth} > max {Max}", message.RawValue, message.Depth, message.GlobalMaxDepth);
            return;
        }

        var canonical = canonicalizer.Canonicalize(message);
        if (!await deduplicator.TryReserveAsync(message.TargetId, canonical.CanonicalKey, cancellationToken).ConfigureAwait(false))
        {
            logger.LogDebug("Dedupe hit for {Key}", canonical.CanonicalKey);
            return;
        }

        try
        {
            if (!scope.IsInScope(message, canonical))
            {
                logger.LogInformation("Out of scope: {Key}", canonical.CanonicalKey);
                await deduplicator.ReleaseAsync(message.TargetId, canonical.CanonicalKey, cancellationToken).ConfigureAwait(false);
                return;
            }

            var (assetId, inserted) = await persistence.PersistNewAssetAsync(message, canonical, cancellationToken).ConfigureAwait(false);
            if (!inserted)
            {
                await deduplicator.ReleaseAsync(message.TargetId, canonical.CanonicalKey, cancellationToken).ConfigureAwait(false);
                return;
            }

            await PublishIndexedAsync(message, canonical, assetId, cancellationToken).ConfigureAwait(false);

            if (message.Kind == AssetKind.IpAddress
                && await workerToggles.IsWorkerEnabledAsync(WorkerKeys.PortScan, cancellationToken).ConfigureAwait(false))
            {
                var causation = message.EventId == Guid.Empty ? message.CorrelationId : message.EventId;
                await outbox.EnqueueAsync(
                        new PortScanRequested(
                            message.TargetId,
                            message.TargetRootDomain,
                            message.GlobalMaxDepth,
                            message.Depth,
                            canonical.NormalizedDisplay,
                            assetId,
                            message.CorrelationId,
                            EventId: NewId.NextGuid(),
                            CausationId: causation,
                            OccurredAtUtc: DateTimeOffset.UtcNow,
                            Producer: "gatekeeper"),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch
        {
            await deduplicator.ReleaseAsync(message.TargetId, canonical.CanonicalKey, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    private Task PublishIndexedAsync(
        AssetDiscovered message,
        CanonicalAsset canonical,
        Guid assetId,
        CancellationToken cancellationToken)
    {
        var rawForWorkers = canonical.NormalizedDisplay;
        if (message.Kind is AssetKind.Subdomain or AssetKind.Domain)
            rawForWorkers = canonical.NormalizedDisplay.Trim().TrimEnd('/');
        var causation = message.EventId == Guid.Empty ? message.CorrelationId : message.EventId;

        return outbox.EnqueueAsync(
            new AssetDiscovered(
                message.TargetId,
                message.TargetRootDomain,
                message.GlobalMaxDepth,
                message.Depth,
                message.Kind,
                rawForWorkers,
                "gatekeeper",
                DateTimeOffset.UtcNow,
                message.CorrelationId,
                AssetAdmissionStage.Indexed,
                assetId,
                message.DiscoveryContext,
                EventId: NewId.NextGuid(),
                CausationId: causation,
                Producer: "gatekeeper"),
            cancellationToken);
    }
}
