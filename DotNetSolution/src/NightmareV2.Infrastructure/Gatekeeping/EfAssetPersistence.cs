using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using NightmareV2.Application.Assets;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Contracts.Events;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.Infrastructure.Gatekeeping;

public sealed class EfAssetPersistence(NightmareDbContext db) : IAssetPersistence
{
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = false };

    public async Task<(Guid AssetId, bool Inserted)> PersistNewAssetAsync(
        AssetDiscovered message,
        CanonicalAsset canonical,
        CancellationToken cancellationToken = default)
    {
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
            DiscoveredAtUtc = message.OccurredAt,
            LifecycleStatus = AssetLifecycleStatus.Discovered,
        };

        db.Assets.Add(entity);
        await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        return (entity.Id, true);
    }

    public async Task ConfirmUrlAssetAsync(Guid assetId, UrlFetchSnapshot snapshot, CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(snapshot, JsonOpts);
        await db.Assets
            .Where(a => a.Id == assetId)
            .ExecuteUpdateAsync(
                s => s
                    .SetProperty(a => a.LifecycleStatus, AssetLifecycleStatus.Confirmed)
                    .SetProperty(a => a.TypeDetailsJson, json),
                cancellationToken)
            .ConfigureAwait(false);
    }
}
