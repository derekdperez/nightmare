using System.Text.Json;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using NightmareV2.Application.Assets;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure.Data;
using Npgsql;

namespace NightmareV2.Infrastructure.Gatekeeping;

public sealed class EfAssetPersistence(
    NightmareDbContext db,
    IPublishEndpoint publish,
    ILogger<EfAssetPersistence> logger) : IAssetPersistence
{
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = false };

    /// <summary>DiscoveredBy prefix for high-value path wordlist URL probes (hvpath consumer).</summary>
    private const string HighValuePathWordlistPrefix = "hvpath:";

    public async Task<(Guid AssetId, bool Inserted)> PersistNewAssetAsync(
        AssetDiscovered message,
        CanonicalAsset canonical,
        CancellationToken cancellationToken = default)
    {
        var targetExists = await db.Targets.AsNoTracking()
            .AnyAsync(t => t.Id == message.TargetId, cancellationToken)
            .ConfigureAwait(false);
        if (!targetExists)
        {
            logger.LogDebug("Skip asset persist: target {TargetId} not in recon_targets (stale bus message).", message.TargetId);
            return (Guid.Empty, false);
        }

        var existingId = await db.Assets.AsNoTracking()
            .Where(a => a.TargetId == message.TargetId && a.CanonicalKey == canonical.CanonicalKey)
            .Select(a => (Guid?)a.Id)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
        if (existingId is { } existing)
            return (existing, false);

        var entity = new StoredAsset
        {
            Id = Guid.NewGuid(),
            TargetId = message.TargetId,
            Kind = message.Kind,
            CanonicalKey = canonical.CanonicalKey,
            RawValue = message.RawValue,
            Depth = message.Depth,
            DiscoveredBy = message.DiscoveredBy,
            DiscoveryContext = message.DiscoveryContext ?? "",
            DiscoveredAtUtc = message.OccurredAt,
            LifecycleStatus = ResolveInitialLifecycleStatus(message),
        };

        db.Assets.Add(entity);
        try
        {
            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException pg
            && pg.SqlState == PostgresErrorCodes.ForeignKeyViolation
            && pg.ConstraintName?.Contains("recon_targets", StringComparison.OrdinalIgnoreCase) == true)
        {
            db.Entry(entity).State = EntityState.Detached;
            logger.LogDebug(ex, "Skip asset persist: FK to recon_targets for target {TargetId} (likely deleted during insert).", message.TargetId);
            return (Guid.Empty, false);
        }

        return (entity.Id, true);
    }

    private static string ResolveInitialLifecycleStatus(AssetDiscovered message)
    {
        if (message.Kind == AssetKind.Url
            && message.DiscoveredBy.StartsWith(HighValuePathWordlistPrefix, StringComparison.OrdinalIgnoreCase))
            return AssetLifecycleStatus.Queued;
        return AssetLifecycleStatus.Discovered;
    }

    public async Task ConfirmUrlAssetAsync(
        Guid assetId,
        UrlFetchSnapshot snapshot,
        Guid correlationId,
        CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(snapshot, JsonOpts);
        var isHttpSuccess = snapshot.StatusCode is >= 200 and < 300;
        var isSoft404 = isHttpSuccess && UrlFetchClassifier.LooksLikeSoft404(snapshot);

        if (isSoft404)
        {
            logger.LogDebug(
                "URL asset {AssetId} returned HTTP {StatusCode} but response body looks like a 404/not-found error; leaving unconfirmed.",
                assetId,
                snapshot.StatusCode);
        }

        var isConfirmedResponse = isHttpSuccess && !isSoft404;
        if (isConfirmedResponse)
        {
            await db.Assets
                .Where(a => a.Id == assetId)
                .ExecuteUpdateAsync(
                    s => s
                        .SetProperty(a => a.LifecycleStatus, AssetLifecycleStatus.Confirmed)
                        .SetProperty(a => a.TypeDetailsJson, json),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await db.Assets
                .Where(a => a.Id == assetId)
                .ExecuteUpdateAsync(s => s.SetProperty(a => a.TypeDetailsJson, json), cancellationToken)
                .ConfigureAwait(false);
        }

        var meta = await db.Assets.AsNoTracking()
            .Where(a => a.Id == assetId)
            .Select(a => new { a.TargetId, a.RawValue })
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
        if (meta is null)
            return;

        if (!isConfirmedResponse)
            return;

        try
        {
            await publish.Publish(
                    new ScannableContentAvailable(
                        assetId,
                        meta.TargetId,
                        meta.RawValue ?? "",
                        correlationId == Guid.Empty ? NewId.NextGuid() : correlationId,
                        DateTimeOffset.UtcNow,
                        ScannableContentSource.UrlHttpResponse),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            // Bus publish must not fail persistence; workers can still use stored JSON if needed.
        }
    }
}
