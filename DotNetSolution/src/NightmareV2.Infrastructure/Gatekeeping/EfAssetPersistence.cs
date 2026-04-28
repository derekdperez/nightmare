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

        EnqueueHttpRequestIfNeeded(entity);

        try
        {
            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException pg
            && pg.SqlState == PostgresErrorCodes.UniqueViolation
            && pg.ConstraintName?.Contains("http_request_queue", StringComparison.OrdinalIgnoreCase) == true)
        {
            logger.LogDebug(ex, "HTTP request queue row already exists for asset {AssetId}.", entity.Id);
        }

        return (entity.Id, true);
    }

    private static string ResolveInitialLifecycleStatus(AssetDiscovered message)
    {
        return AssetLifecycleStatus.Queued;
    }

    private void EnqueueHttpRequestIfNeeded(StoredAsset asset)
    {
        if (!ShouldRequest(asset.Kind))
            return;

        if (!TryResolveRequestUrl(asset, out var requestUrl, out var domainKey))
            return;

        db.HttpRequestQueue.Add(
            new HttpRequestQueueItem
            {
                Id = Guid.NewGuid(),
                AssetId = asset.Id,
                TargetId = asset.TargetId,
                AssetKind = asset.Kind,
                Method = "GET",
                RequestUrl = requestUrl,
                DomainKey = domainKey,
                State = HttpRequestQueueState.Queued,
                Priority = ResolvePriority(asset.Kind),
                CreatedAtUtc = DateTimeOffset.UtcNow,
                UpdatedAtUtc = DateTimeOffset.UtcNow,
                NextAttemptAtUtc = DateTimeOffset.UtcNow,
            });
    }

    private static bool ShouldRequest(AssetKind kind) =>
        kind is AssetKind.Url or AssetKind.ApiEndpoint or AssetKind.JavaScriptFile or AssetKind.MarkdownBody
            or AssetKind.Subdomain or AssetKind.Domain;

    private static int ResolvePriority(AssetKind kind) =>
        kind is AssetKind.Subdomain or AssetKind.Domain ? 10 : 0;

    private static bool TryResolveRequestUrl(StoredAsset asset, out string requestUrl, out string domainKey)
    {
        requestUrl = "";
        domainKey = "";

        if (asset.Kind is AssetKind.Subdomain or AssetKind.Domain)
        {
            var host = asset.RawValue.Trim().TrimEnd('/');
            if (host.Length == 0)
                return false;
            if (!Uri.TryCreate($"https://{host}/", UriKind.Absolute, out var domainUri))
                return false;
            requestUrl = domainUri.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped);
            domainKey = domainUri.IdnHost.ToLowerInvariant();
            return true;
        }

        var raw = asset.RawValue.Trim();
        if (!Uri.TryCreate(raw, UriKind.Absolute, out var uri)
            && !Uri.TryCreate("https://" + raw, UriKind.Absolute, out uri))
        {
            return false;
        }

        if (uri.Scheme is not ("http" or "https") || string.IsNullOrWhiteSpace(uri.Host))
            return false;

        requestUrl = uri.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped);
        domainKey = uri.IdnHost.ToLowerInvariant();
        return true;
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
                .ExecuteUpdateAsync(
                    s => s
                        .SetProperty(a => a.LifecycleStatus, AssetLifecycleStatus.NonExistent)
                        .SetProperty(a => a.TypeDetailsJson, json),
                    cancellationToken)
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
