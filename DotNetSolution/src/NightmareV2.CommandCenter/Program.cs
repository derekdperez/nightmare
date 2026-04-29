using System.Net.Http;
using System.Text.Json;
using MassTransit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using NightmareV2.Application.Assets;
using NightmareV2.Application.Events;
using NightmareV2.Application.FileStore;
using NightmareV2.Application.HighValue;
using NightmareV2.Application.Workers;
using NightmareV2.CommandCenter;
using NightmareV2.CommandCenter.Components;
using NightmareV2.CommandCenter.DataMaintenance;
using NightmareV2.CommandCenter.Diagnostics;
using NightmareV2.CommandCenter.Hubs;
using NightmareV2.CommandCenter.Models;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure;
using NightmareV2.Infrastructure.Data;
using NightmareV2.Infrastructure.Messaging;

var builder = WebApplication.CreateBuilder(args);

OpsSnapshotBuilder.RegisterHttpClient(builder);

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddScoped(sp =>
{
    var nav = sp.GetRequiredService<NavigationManager>();
    return new HttpClient { BaseAddress = new Uri(nav.BaseUri) };
});

builder.Services.AddNightmareInfrastructure(builder.Configuration);
builder.Services.AddSignalR();
builder.Services.AddNightmareRabbitMq(builder.Configuration, _ => { });

var app = builder.Build();

var skipStartupDatabase = app.Configuration.GetValue("Nightmare:SkipStartupDatabase", false)
    || string.Equals(
        Environment.GetEnvironmentVariable("NIGHTMARE_SKIP_STARTUP_DATABASE"),
        "1",
        StringComparison.OrdinalIgnoreCase);

await InitializeStartupDatabasesAsync(app, skipStartupDatabase).ConfigureAwait(false);

var listenPlainHttp = app.Configuration.GetValue("Nightmare:ListenPlainHttp", false);

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    if (!listenPlainHttp)
        app.UseHsts();
}

app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
if (!listenPlainHttp)
    app.UseHttpsRedirection();
app.UseAntiforgery();

DiagnosticsEndpoints.Map(app);
DataMaintenanceEndpoints.Map(app);

app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapHub<DiscoveryHub>("/hubs/discovery");

app.MapGet(
        "/api/targets",
        async (NightmareDbContext db, CancellationToken ct) =>
        {
            var rows = await db.Targets.AsNoTracking()
                .OrderByDescending(t => t.CreatedAtUtc)
                .Select(t => new TargetSummary(t.Id, t.RootDomain, t.GlobalMaxDepth, t.CreatedAtUtc))
                .Take(5000)
                .ToListAsync(ct)
                .ConfigureAwait(false);
            return Results.Ok(rows);
        })
    .WithName("ListTargets");

app.MapPost(
        "/api/targets",
        async (
            CreateTargetRequest dto,
            NightmareDbContext db,
            IEventOutbox outbox,
            IHubContext<DiscoveryHub> hub,
            CancellationToken ct) =>
        {
            if (!TargetRootNormalization.TryNormalize(dto.RootDomain, out var root))
                return Results.BadRequest("root domain required");

            var target = new ReconTarget
            {
                Id = Guid.NewGuid(),
                RootDomain = root,
                GlobalMaxDepth = dto.GlobalMaxDepth > 0 ? dto.GlobalMaxDepth : 12,
                CreatedAtUtc = DateTimeOffset.UtcNow,
            };

            db.Targets.Add(target);
            await db.SaveChangesAsync(ct).ConfigureAwait(false);

            var correlation = NewId.NextGuid();
            var eventId = NewId.NextGuid();
            await outbox.EnqueueAsync(
                    new TargetCreated(
                        target.Id,
                        target.RootDomain,
                        target.GlobalMaxDepth,
                        target.CreatedAtUtc,
                        correlation,
                        EventId: eventId,
                        CausationId: correlation,
                        Producer: "command-center"),
                    ct)
                .ConfigureAwait(false);

            await hub.Clients.All.SendAsync("TargetQueued", target.Id, target.RootDomain, cancellationToken: ct)
                .ConfigureAwait(false);

            return Results.Created($"/api/targets/{target.Id}", new TargetSummary(target.Id, target.RootDomain, target.GlobalMaxDepth, target.CreatedAtUtc));
        })
    .WithName("CreateTarget");

app.MapPut(
        "/api/targets/{id:guid}",
        async (Guid id, UpdateTargetRequest dto, NightmareDbContext db, CancellationToken ct) =>
        {
            if (!TargetRootNormalization.TryNormalize(dto.RootDomain, out var root))
                return Results.BadRequest("root domain required");

            var depth = dto.GlobalMaxDepth > 0 ? dto.GlobalMaxDepth : 12;
            var target = await db.Targets.FirstOrDefaultAsync(t => t.Id == id, ct).ConfigureAwait(false);
            if (target is null)
                return Results.NotFound();

            if (!string.Equals(target.RootDomain, root, StringComparison.Ordinal))
            {
                var taken = await db.Targets.AnyAsync(t => t.RootDomain == root && t.Id != id, ct).ConfigureAwait(false);
                if (taken)
                    return Results.Conflict("root domain already in use");
            }

            target.RootDomain = root;
            target.GlobalMaxDepth = depth;
            await db.SaveChangesAsync(ct).ConfigureAwait(false);
            return Results.Ok(new TargetSummary(target.Id, target.RootDomain, target.GlobalMaxDepth, target.CreatedAtUtc));
        })
    .WithName("UpdateTarget");

app.MapDelete(
        "/api/targets/{id:guid}",
        async (Guid id, NightmareDbContext db, CancellationToken ct) =>
        {
            var target = await db.Targets.FirstOrDefaultAsync(t => t.Id == id, ct).ConfigureAwait(false);
            if (target is null)
                return Results.NotFound();
            db.Targets.Remove(target);
            await db.SaveChangesAsync(ct).ConfigureAwait(false);
            return Results.NoContent();
        })
    .WithName("DeleteTarget");

app.MapPost(
        "/api/targets/bulk",
        async (HttpRequest httpRequest, NightmareDbContext db, IEventOutbox outbox, IHubContext<DiscoveryHub> hub, CancellationToken ct) =>
        {
            const int maxLines = 50_000;
            var rawLines = new List<string>();
            var globalDepth = 12;
            var contentType = httpRequest.ContentType ?? "";

            if (contentType.Contains("multipart/form-data", StringComparison.OrdinalIgnoreCase))
            {
                var form = await httpRequest.ReadFormAsync(ct).ConfigureAwait(false);
                if (form.TryGetValue("globalMaxDepth", out var depthVals) && int.TryParse(depthVals.ToString(), out var parsedDepth) && parsedDepth > 0)
                    globalDepth = parsedDepth;
                var file = form.Files.GetFile("file");
                if (file is null || file.Length == 0)
                    return Results.BadRequest("multipart field \"file\" is required");
                await using var stream = file.OpenReadStream();
                using var reader = new StreamReader(stream);
                var text = await reader.ReadToEndAsync(ct).ConfigureAwait(false);
                rawLines.AddRange(TargetRootNormalization.SplitLines(text));
            }
            else
            {
                var dto = await httpRequest.ReadFromJsonAsync<BulkImportRequest>(cancellationToken: ct).ConfigureAwait(false);
                if (dto is null)
                    return Results.BadRequest("expected JSON body or multipart/form-data with field \"file\"");
                globalDepth = dto.GlobalMaxDepth > 0 ? dto.GlobalMaxDepth : 12;
                if (dto.Domains is not null)
                    rawLines.AddRange(dto.Domains);
            }

            if (rawLines.Count > maxLines)
                return Results.BadRequest($"maximum {maxLines} lines per import");

            var firstOrder = new List<string>();
            var batchSeen = new HashSet<string>(StringComparer.Ordinal);
            var skippedEmpty = 0;
            var skippedDupBatch = 0;
            var skippedInvalid = 0;
            foreach (var line in rawLines)
            {
                var trimmed = line.Trim();
                if (trimmed.Length == 0)
                {
                    skippedEmpty++;
                    continue;
                }

                if (!TargetRootNormalization.TryNormalize(trimmed, out var n))
                {
                    skippedInvalid++;
                    continue;
                }

                if (!batchSeen.Add(n))
                {
                    skippedDupBatch++;
                    continue;
                }

                firstOrder.Add(n);
            }

            if (firstOrder.Count == 0)
            {
                return Results.Ok(
                    new BulkImportResult(
                        0,
                        0,
                        skippedInvalid + skippedEmpty,
                        skippedDupBatch));
            }

            var existing = await db.Targets.AsNoTracking()
                .Where(t => firstOrder.Contains(t.RootDomain))
                .Select(t => t.RootDomain)
                .ToListAsync(ct)
                .ConfigureAwait(false);
            var existingSet = existing.ToHashSet(StringComparer.Ordinal);

            var skippedExist = 0;
            var newTargets = new List<ReconTarget>();
            foreach (var n in firstOrder)
            {
                if (existingSet.Contains(n))
                {
                    skippedExist++;
                    continue;
                }

                existingSet.Add(n);
                var target = new ReconTarget
                {
                    Id = Guid.NewGuid(),
                    RootDomain = n,
                    GlobalMaxDepth = globalDepth,
                    CreatedAtUtc = DateTimeOffset.UtcNow,
                };
                newTargets.Add(target);
                db.Targets.Add(target);
            }

            await db.SaveChangesAsync(ct).ConfigureAwait(false);

            foreach (var target in newTargets)
            {
                var correlation = NewId.NextGuid();
                var eventId = NewId.NextGuid();
                await outbox.EnqueueAsync(
                        new TargetCreated(
                            target.Id,
                            target.RootDomain,
                            target.GlobalMaxDepth,
                            target.CreatedAtUtc,
                            correlation,
                            EventId: eventId,
                            CausationId: correlation,
                            Producer: "command-center"),
                        ct)
                    .ConfigureAwait(false);
                await hub.Clients.All.SendAsync("TargetQueued", target.Id, target.RootDomain, cancellationToken: ct)
                    .ConfigureAwait(false);
            }

            return Results.Ok(
                new BulkImportResult(
                    newTargets.Count,
                    skippedExist,
                    skippedInvalid + skippedEmpty,
                    skippedDupBatch));
        })
    .WithName("BulkImportTargets");


app.MapGet(
        "/api/http-request-queue/settings",
        async (NightmareDbContext db, CancellationToken ct) =>
        {
            var row = await db.HttpRequestQueueSettings.AsNoTracking()
                .FirstOrDefaultAsync(s => s.Id == 1, ct)
                .ConfigureAwait(false)
                ?? new HttpRequestQueueSettings();

            return Results.Ok(
                new HttpRequestQueueSettingsDto(
                    row.Enabled,
                    row.GlobalRequestsPerMinute,
                    row.PerDomainRequestsPerMinute,
                    row.MaxConcurrency,
                    row.RequestTimeoutSeconds,
                    row.UpdatedAtUtc));
        })
    .WithName("GetHttpRequestQueueSettings");

app.MapPut(
        "/api/http-request-queue/settings",
        async (HttpRequestQueueSettingsPatch body, NightmareDbContext db, CancellationToken ct) =>
        {
            var row = await db.HttpRequestQueueSettings.FirstOrDefaultAsync(s => s.Id == 1, ct).ConfigureAwait(false);
            if (row is null)
            {
                row = new HttpRequestQueueSettings { Id = 1 };
                db.HttpRequestQueueSettings.Add(row);
            }

            row.Enabled = body.Enabled;
            row.GlobalRequestsPerMinute = Math.Clamp(body.GlobalRequestsPerMinute, 1, 100_000);
            row.PerDomainRequestsPerMinute = Math.Clamp(body.PerDomainRequestsPerMinute, 1, 10_000);
            row.MaxConcurrency = Math.Clamp(body.MaxConcurrency, 1, 1_000);
            row.RequestTimeoutSeconds = Math.Clamp(body.RequestTimeoutSeconds, 5, 300);
            row.UpdatedAtUtc = DateTimeOffset.UtcNow;

            await db.SaveChangesAsync(ct).ConfigureAwait(false);
            return Results.NoContent();
        })
    .WithName("UpdateHttpRequestQueueSettings");

app.MapGet(
        "/api/http-request-queue",
        async (NightmareDbContext db, Guid? targetId, int? take, CancellationToken ct) =>
        {
            var limit = Math.Clamp(take ?? 800, 1, 5000);
            var q = db.HttpRequestQueue.AsNoTracking().OrderByDescending(r => r.CreatedAtUtc).AsQueryable();
            if (targetId is { } tid)
                q = q.Where(r => r.TargetId == tid);

            var rows = await q.Take(limit)
                .Select(r => new HttpRequestQueueRowDto(
                    r.Id,
                    r.AssetId,
                    r.TargetId,
                    r.AssetKind.ToString(),
                    r.Method,
                    r.RequestUrl,
                    r.DomainKey,
                    r.State,
                    r.AttemptCount,
                    r.MaxAttempts,
                    r.Priority,
                    r.CreatedAtUtc,
                    r.UpdatedAtUtc,
                    r.NextAttemptAtUtc,
                    r.StartedAtUtc,
                    r.CompletedAtUtc,
                    r.LastHttpStatus,
                    r.LastError,
                    r.DurationMs,
                    r.ResponseContentType,
                    r.ResponseContentLength,
                    r.FinalUrl))
                .ToListAsync(ct)
                .ConfigureAwait(false);

            return Results.Ok(rows);
        })
    .WithName("ListHttpRequestQueue");

app.MapGet(
        "/api/http-request-queue/metrics",
        async (NightmareDbContext db, CancellationToken ct) =>
        {
            var now = DateTimeOffset.UtcNow;
            var oneHourAgo = now.AddHours(-1);

            var queued = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.Queued, ct)
                .ConfigureAwait(false);
            var retry = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.Retry && q.NextAttemptAtUtc <= now, ct)
                .ConfigureAwait(false);
            var scheduledRetry = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.Retry && q.NextAttemptAtUtc > now, ct)
                .ConfigureAwait(false);
            var inFlight = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.InFlight, ct)
                .ConfigureAwait(false);
            var failed = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.Failed, ct)
                .ConfigureAwait(false);
            var completedLastHour = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.Succeeded && q.CompletedAtUtc >= oneHourAgo, ct)
                .ConfigureAwait(false);
            var oldestQueuedAt = await db.HttpRequestQueue.AsNoTracking()
                .Where(q => q.State == HttpRequestQueueState.Queued
                    || (q.State == HttpRequestQueueState.Retry && q.NextAttemptAtUtc <= now))
                .OrderBy(q => q.CreatedAtUtc)
                .Select(q => (DateTimeOffset?)q.CreatedAtUtc)
                .FirstOrDefaultAsync(ct)
                .ConfigureAwait(false);

            return Results.Ok(
                new HttpRequestQueueMetricsDto(
                    queued,
                    retry,
                    scheduledRetry,
                    inFlight,
                    failed,
                    completedLastHour,
                    queued + retry,
                    oldestQueuedAt,
                    oldestQueuedAt is null ? null : (long)(now - oldestQueuedAt.Value).TotalSeconds));
        })
    .WithName("GetHttpRequestQueueMetrics");

app.MapGet(
        "/api/bus/live",
        async (NightmareDbContext db, int? minutes, int? take, CancellationToken ct) =>
        {
            var window = TimeSpan.FromMinutes(Math.Clamp(minutes ?? 3, 1, 60));
            var limit = Math.Clamp(take ?? 150, 1, 500);
            var since = DateTimeOffset.UtcNow - window;
            var rows = await db.BusJournal.AsNoTracking()
                .Where(e => e.Direction == "Publish" && e.OccurredAtUtc >= since)
                .OrderByDescending(e => e.OccurredAtUtc)
                .Take(limit)
                .Select(e => new BusJournalRowDto(e.Id, e.Direction, e.MessageType, e.PayloadJson, e.OccurredAtUtc, e.ConsumerType, e.HostName))
                .ToListAsync(ct)
                .ConfigureAwait(false);
            return Results.Ok(rows);
        })
    .WithName("BusLive");

app.MapGet(
        "/api/bus/history",
        async (NightmareDbContext db, int? take, CancellationToken ct) =>
        {
            var limit = Math.Clamp(take ?? 400, 1, 2000);
            var rows = await db.BusJournal.AsNoTracking()
                .OrderByDescending(e => e.Id)
                .Take(limit)
                .Select(e => new BusJournalRowDto(e.Id, e.Direction, e.MessageType, e.PayloadJson, e.OccurredAtUtc, e.ConsumerType, e.HostName))
                .ToListAsync(ct)
                .ConfigureAwait(false);
            return Results.Ok(rows);
        })
    .WithName("BusHistory");

app.MapGet(
        "/api/assets",
        async (NightmareDbContext db, Guid? targetId, int? take, CancellationToken ct) =>
        {
            var limit = Math.Clamp(take ?? 500, 1, 5000);
            var q = db.Assets.AsNoTracking().OrderByDescending(a => a.DiscoveredAtUtc).AsQueryable();
            if (targetId is { } tid)
                q = q.Where(a => a.TargetId == tid);
            var rows = await q.Take(limit)
                .Select(a => new AssetGridRowDto(
                    a.Id,
                    a.TargetId,
                    a.Kind.ToString(),
                    a.CanonicalKey,
                    a.RawValue,
                    a.Depth,
                    a.DiscoveredBy,
                    a.DiscoveryContext,
                    a.DiscoveredAtUtc,
                    a.LifecycleStatus))
                .ToListAsync(ct)
                .ConfigureAwait(false);
            return Results.Ok(rows);
        })
    .WithName("ListAssets");

const long fileStoreMaxUploadBytes = 50L * 1024 * 1024;

app.MapPost(
        "/api/filestore",
        async (HttpRequest req, IFileStore store, CancellationToken ct) =>
        {
            if (!req.HasFormContentType)
                return Results.BadRequest("multipart/form-data with field \"file\" is required");
            var form = await req.ReadFormAsync(ct).ConfigureAwait(false);
            var file = form.Files.GetFile("file");
            if (file is null || file.Length == 0)
                return Results.BadRequest("multipart field \"file\" is required");
            if (file.Length > fileStoreMaxUploadBytes)
                return Results.BadRequest($"file exceeds maximum size ({fileStoreMaxUploadBytes} bytes)");
            var logical = form["logicalName"].ToString();
            if (string.IsNullOrWhiteSpace(logical))
                logical = file.FileName;
            await using var uploadStream = file.OpenReadStream();
            var created = await store.StoreAsync(uploadStream, file.ContentType, logical, ct).ConfigureAwait(false);
            return Results.Created($"/api/filestore/{created.Id}", created);
        })
    .WithName("UploadFileBlob")
    .DisableAntiforgery();

app.MapGet(
        "/api/filestore/{id:guid}/info",
        async (Guid id, IFileStore store, CancellationToken ct) =>
        {
            var meta = await store.GetDescriptorAsync(id, ct).ConfigureAwait(false);
            return meta is null ? Results.NotFound() : Results.Ok(meta);
        })
    .WithName("GetFileBlobInfo");

app.MapGet(
        "/api/filestore/{id:guid}",
        async (Guid id, IFileStore store, CancellationToken ct) =>
        {
            var meta = await store.GetDescriptorAsync(id, ct).ConfigureAwait(false);
            if (meta is null)
                return Results.NotFound();
            var stream = await store.OpenReadAsync(id, ct).ConfigureAwait(false);
            if (stream is null)
                return Results.NotFound();
            return Results.File(
                stream,
                meta.ContentType ?? "application/octet-stream",
                fileDownloadName: meta.LogicalName ?? $"{meta.Id:N}");
        })
    .WithName("DownloadFileBlob");

app.MapDelete(
        "/api/filestore/{id:guid}",
        async (Guid id, IFileStore store, CancellationToken ct) =>
        {
            var meta = await store.GetDescriptorAsync(id, ct).ConfigureAwait(false);
            if (meta is null)
                return Results.NotFound();
            await store.DeleteAsync(id, ct).ConfigureAwait(false);
            return Results.NoContent();
        })
    .WithName("DeleteFileBlob");

app.MapGet(
        "/api/high-value-findings",
        async (NightmareDbContext db, bool? criticalOnly, int? take, CancellationToken ct) =>
        {
            var limit = Math.Clamp(take ?? 500, 1, 5000);
            var fetchCount = Math.Clamp(limit * 6, limit, 30000);
            var q = db.HighValueFindings.AsNoTracking()
                .Where(f => f.SourceAssetId != null)
                .Join(
                    db.Assets.AsNoTracking(),
                    f => f.SourceAssetId!.Value,
                    a => a.Id,
                    (f, a) => new { f, a })
                .Join(
                    db.Targets.AsNoTracking(),
                    x => x.f.TargetId,
                    t => t.Id,
                    (x, t) => new { x.f, x.a, t.RootDomain });
            if (criticalOnly == true)
                q = q.Where(x => x.f.Severity == "Critical");
            var rows = await q
                .OrderByDescending(x => x.f.DiscoveredAtUtc)
                .Take(fetchCount)
                .Select(x => new
                {
                    Row = new HighValueFindingRowDto(
                        x.f.Id,
                        x.f.TargetId,
                        x.f.SourceAssetId,
                        x.f.FindingType,
                        x.f.Severity,
                        x.f.PatternName,
                        x.f.Category,
                        x.f.MatchedText,
                        x.f.SourceUrl,
                        x.f.WorkerName,
                        x.f.ImportanceScore,
                        x.f.DiscoveredAtUtc,
                        x.RootDomain),
                    x.a.LifecycleStatus,
                    x.a.DiscoveredBy,
                    x.a.TypeDetailsJson,
                })
                .ToListAsync(ct)
                .ConfigureAwait(false);
            var confirmedRows = rows
                .Where(x => string.Equals(x.LifecycleStatus, AssetLifecycleStatus.Confirmed, StringComparison.Ordinal))
                .ToList();

            var allowedHighValuePaths = LoadHighValuePathSet();

            var filtered = confirmedRows
                .Select(x => new
                {
                    x.Row,
                    x.DiscoveredBy,
                    SnapshotOk = TryParseSnapshot(x.TypeDetailsJson, out var snap),
                    Snapshot = snap,
                })
                .Where(x => x.SnapshotOk)
                .Where(x => !LooksLikeSoft404(x.Snapshot))
                .Where(x => FindingSourceIsAllowed(x.Row, x.DiscoveredBy, x.Snapshot, allowedHighValuePaths))
                .Select(x => x.Row)
                .Take(limit)
                .ToList();
            return Results.Ok(filtered);
        })
    .WithName("ListHighValueFindings");

static bool FindingSourceIsAllowed(
    HighValueFindingRowDto row,
    string discoveredBy,
    UrlFetchSnapshot snap,
    IReadOnlySet<string> allowedHighValuePaths)
{
    // Regex findings can be raised from any confirmed URL response. The previous page query
    // incorrectly limited every high-value finding to hvpath:* assets, which hid confirmed
    // assets that matched the high-value regex scanner.
    if (!discoveredBy.StartsWith("hvpath:", StringComparison.OrdinalIgnoreCase))
        return true;

    return HighValuePathRedirectIsAllowed(row, snap, allowedHighValuePaths);
}

static bool HighValuePathRedirectIsAllowed(
    HighValueFindingRowDto row,
    UrlFetchSnapshot snap,
    IReadOnlySet<string> allowedHighValuePaths)
{
    var source = NormalizeUrlForCompare(row.SourceUrl);
    if (source is null)
        return false;

    // Older confirmed snapshots may not include FinalUrl. Treat them as displayable after the
    // confirmed-status and soft-404 checks above; otherwise historical high-value assets vanish.
    var final = NormalizeUrlForCompare(snap.FinalUrl);
    if (final is null)
        return true;

    var redirected = !string.Equals(source, final, StringComparison.OrdinalIgnoreCase);
    if (!redirected)
        return true;
    if (!Uri.TryCreate(final, UriKind.Absolute, out var finalUri))
        return false;
    return allowedHighValuePaths.Contains(NormalizeWordlistPath(finalUri.AbsolutePath));
}

static string? NormalizeUrlForCompare(string? url)
{
    if (string.IsNullOrWhiteSpace(url))
        return null;
    if (!Uri.TryCreate(url.Trim(), UriKind.Absolute, out var u))
        return null;
    if (u.Scheme is not ("http" or "https"))
        return null;
    var canonical = u.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped);
    return canonical.TrimEnd('/');
}

static IReadOnlySet<string> LoadHighValuePathSet()
{
    var dir = Path.Combine(AppContext.BaseDirectory, "Resources", "Wordlists", "high_value");
    var list = HighValueWordlistCatalog.LoadFromDirectory(dir);
    var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    foreach (var (_, lines) in list)
    {
        foreach (var line in lines)
            set.Add(NormalizeWordlistPath(line));
    }

    return set;
}

static string NormalizeWordlistPath(string path)
{
    var p = path.Trim();
    if (p.Length == 0)
        return "/";
    var q = p.IndexOfAny(['?', '#']);
    if (q >= 0)
        p = p[..q];
    if (!p.StartsWith('/'))
        p = "/" + p;
    return p.TrimEnd('/');
}

static bool TryParseSnapshot(string? typeDetailsJson, out UrlFetchSnapshot snapshot)
{
    snapshot = default!;
    if (string.IsNullOrWhiteSpace(typeDetailsJson))
        return false;
    try
    {
        snapshot = JsonSerializer.Deserialize<UrlFetchSnapshot>(typeDetailsJson)!;
        return snapshot is not null;
    }
    catch
    {
        return false;
    }
}

static bool LooksLikeSoft404(UrlFetchSnapshot? snap)
{
    if (snap is null)
        return true;
    if (snap.StatusCode is 404 or 410)
        return true;
    if (snap.StatusCode < 200 || snap.StatusCode >= 300)
        return true;

    var body = snap.ResponseBody;
    if (string.IsNullOrWhiteSpace(body))
        return false;

    var contentType = snap.ContentType ?? "";
    var textLike = contentType.Contains("text/html", StringComparison.OrdinalIgnoreCase)
        || contentType.Contains("text/plain", StringComparison.OrdinalIgnoreCase)
        || contentType.Contains("application/xhtml+xml", StringComparison.OrdinalIgnoreCase);
    if (!textLike)
        return false;

    var normalized = body.ToLowerInvariant();
    return normalized.Contains("404 not found", StringComparison.Ordinal)
        || normalized.Contains("page not found", StringComparison.Ordinal)
        || normalized.Contains("doesn't exist", StringComparison.Ordinal)
        || normalized.Contains("cannot be found", StringComparison.Ordinal)
        || normalized.Contains("the page you are looking for", StringComparison.Ordinal);
}

app.MapGet(
        "/api/workers",
        async (NightmareDbContext db, CancellationToken ct) =>
        {
            var rows = await db.WorkerSwitches.AsNoTracking()
                .OrderBy(w => w.WorkerKey)
                .Select(w => new WorkerSwitchDto(w.WorkerKey, w.IsEnabled, w.UpdatedAtUtc))
                .ToListAsync(ct)
                .ConfigureAwait(false);
            return Results.Ok(rows);
        })
    .WithName("ListWorkers");

app.MapGet(
        "/api/workers/capabilities",
        () =>
        {
            var rows = new[]
            {
                new WorkerCapabilityDto(WorkerKeys.Gatekeeper, "Gatekeeper", "v1", true, true, false, false),
                new WorkerCapabilityDto(WorkerKeys.Spider, "Spider HTTP Queue", "v1", false, true, true, false),
                new WorkerCapabilityDto(WorkerKeys.Enumeration, "Enumeration", "v1", true, true, false, true),
                new WorkerCapabilityDto(WorkerKeys.PortScan, "Port Scan", "v1", true, false, true, true),
                new WorkerCapabilityDto(WorkerKeys.HighValueRegex, "High Value Regex", "v1", true, false, false, false),
                new WorkerCapabilityDto(WorkerKeys.HighValuePaths, "High Value Paths", "v1", true, true, false, false),
            };
            return Results.Ok(rows);
        })
    .WithName("WorkerCapabilities");

app.MapGet(
        "/api/workers/health",
        async (NightmareDbContext db, CancellationToken ct) =>
        {
            var now = DateTimeOffset.UtcNow;
            var since1 = now.AddHours(-1);
            var since24 = now.AddHours(-24);

            var toggles = await db.WorkerSwitches.AsNoTracking()
                .ToDictionaryAsync(w => w.WorkerKey, w => w.IsEnabled, ct)
                .ConfigureAwait(false);

            var consumeRows = await db.BusJournal.AsNoTracking()
                .Where(e => e.Direction == "Consume" && e.ConsumerType != null && e.OccurredAtUtc >= since24)
                .Select(e => new { e.ConsumerType, e.OccurredAtUtc })
                .ToListAsync(ct)
                .ConfigureAwait(false);

            var byKind = consumeRows
                .Select(r => new { Kind = WorkerConsumerKindResolver.KindFromConsumerType(r.ConsumerType), r.OccurredAtUtc })
                .Where(r => !string.IsNullOrWhiteSpace(r.Kind))
                .GroupBy(r => r.Kind!)
                .ToDictionary(
                    g => g.Key,
                    g => new
                    {
                        Last = g.Max(x => x.OccurredAtUtc),
                        Last1h = g.LongCount(x => x.OccurredAtUtc >= since1),
                        Last24h = g.LongCount(),
                    },
                    StringComparer.Ordinal);

            var keys = new[]
            {
                WorkerKeys.Gatekeeper,
                WorkerKeys.Spider,
                WorkerKeys.Enumeration,
                WorkerKeys.PortScan,
                WorkerKeys.HighValueRegex,
                WorkerKeys.HighValuePaths,
            };

            var rows = keys.Select(
                    key =>
                    {
                        var enabled = toggles.GetValueOrDefault(key, true);
                        var has = byKind.TryGetValue(key, out var stats);
                        var last = has ? stats!.Last : (DateTimeOffset?)null;
                        var c1 = has ? stats!.Last1h : 0;
                        var c24 = has ? stats!.Last24h : 0;
                        var healthy = !enabled || c1 > 0 || (last is not null && (now - last.Value) <= TimeSpan.FromMinutes(15));
                        var reason = !enabled
                            ? "worker toggle is disabled"
                            : healthy
                                ? "worker consumed events recently"
                                : "worker has no recent consume activity";
                        return new WorkerHealthDto(key, enabled, last, c1, c24, healthy, reason);
                    })
                .ToList();

            return Results.Ok(rows);
        })
    .WithName("WorkerHealth");

app.MapGet(
        "/api/workers/activity",
        async (NightmareDbContext db, CancellationToken ct) =>
        {
            var snap = await WorkerActivityQuery.BuildSnapshotAsync(db, ct).ConfigureAwait(false);
            return Results.Ok(snap);
        })
    .WithName("WorkerActivity");

app.MapGet(
        "/api/ops/snapshot",
        async (NightmareDbContext db, IHttpClientFactory httpFactory, IConfiguration configuration, CancellationToken ct) =>
        {
            var snap = await OpsSnapshotBuilder.BuildAsync(db, httpFactory, configuration, ct).ConfigureAwait(false);
            return Results.Ok(snap);
        })
    .WithName("OpsSnapshot");

app.MapGet(
        "/api/ops/overview",
        async (NightmareDbContext db, CancellationToken ct) =>
        {
            var totalTargets = await db.Targets.AsNoTracking().LongCountAsync(ct).ConfigureAwait(false);
            var totalAssetsConfirmed = await db.Assets.AsNoTracking()
                .LongCountAsync(a => a.LifecycleStatus == AssetLifecycleStatus.Confirmed, ct)
                .ConfigureAwait(false);
            var totalUrls = await db.Assets.AsNoTracking()
                .LongCountAsync(a => a.Kind == AssetKind.Url, ct)
                .ConfigureAwait(false);

            var urlsFromFetchedPages = await db.Assets.AsNoTracking()
                .LongCountAsync(
                    a => a.Kind == AssetKind.Url
                        && a.DiscoveredBy == "spider-worker"
                        && EF.Functions.Like(a.DiscoveryContext, "Spider: link extracted from fetched page %"),
                    ct)
                .ConfigureAwait(false);

            var urlsFromScripts = await db.Assets.AsNoTracking()
                .LongCountAsync(
                    a => a.Kind == AssetKind.Url
                        && a.DiscoveredBy == "spider-worker"
                        && (EF.Functions.ILike(a.DiscoveryContext, "%.js%")
                            || EF.Functions.ILike(a.DiscoveryContext, "%javascript%")),
                    ct)
                .ConfigureAwait(false);

            var urlsGuessedWithWordlist = await db.Assets.AsNoTracking()
                .LongCountAsync(
                    a => a.Kind == AssetKind.Url
                        && EF.Functions.ILike(a.DiscoveredBy, "hvpath:%"),
                    ct)
                .ConfigureAwait(false);

            var domainCounts = await db.Assets.AsNoTracking()
                .Join(db.Targets.AsNoTracking(), a => a.TargetId, t => t.Id, (_, t) => t.RootDomain)
                .GroupBy(d => d)
                .Select(g => new { RootDomain = g.Key, Count = g.LongCount() })
                .ToListAsync(ct)
                .ConfigureAwait(false);

            var top = domainCounts
                .OrderByDescending(x => x.Count)
                .ThenBy(x => x.RootDomain, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault();
            var domains10OrMore = domainCounts.LongCount(x => x.Count >= 10);
            var domains10OrFewer = domainCounts.LongCount(x => x.Count <= 10);

            return Results.Ok(
                new OpsOverviewDto(
                    totalTargets,
                    totalAssetsConfirmed,
                    totalUrls,
                    urlsFromFetchedPages,
                    urlsFromScripts,
                    urlsGuessedWithWordlist,
                    top?.RootDomain,
                    top?.Count ?? 0,
                    domains10OrMore,
                    domains10OrFewer));
        })
    .WithName("OpsOverview");

app.MapGet(
        "/api/ops/reliability-slo",
        async (NightmareDbContext db, CancellationToken ct) =>
        {
            var now = DateTimeOffset.UtcNow;
            var since = now.AddHours(-1);

            var publishes = await db.BusJournal.AsNoTracking()
                .LongCountAsync(e => e.Direction == "Publish" && e.OccurredAtUtc >= since, ct)
                .ConfigureAwait(false);
            var consumes = await db.BusJournal.AsNoTracking()
                .LongCountAsync(e => e.Direction == "Consume" && e.OccurredAtUtc >= since, ct)
                .ConfigureAwait(false);
            var successRate = publishes <= 0 ? 1m : Math.Min(1m, consumes / (decimal)publishes);

            var queued = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.Queued, ct)
                .ConfigureAwait(false);
            var readyRetry = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.Retry && q.NextAttemptAtUtc <= now, ct)
                .ConfigureAwait(false);
            var backlog = queued + readyRetry;
            var completed = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.Succeeded && q.CompletedAtUtc >= since, ct)
                .ConfigureAwait(false);
            var failedLastHour = await db.HttpRequestQueue.AsNoTracking()
                .LongCountAsync(q => q.State == HttpRequestQueueState.Failed && q.UpdatedAtUtc >= since, ct)
                .ConfigureAwait(false);
            var oldestQueuedAt = await db.HttpRequestQueue.AsNoTracking()
                .Where(q => q.State == HttpRequestQueueState.Queued
                    || (q.State == HttpRequestQueueState.Retry && q.NextAttemptAtUtc <= now))
                .OrderBy(q => q.CreatedAtUtc)
                .Select(q => (DateTimeOffset?)q.CreatedAtUtc)
                .FirstOrDefaultAsync(ct)
                .ConfigureAwait(false);

            var apiReady = await db.Database.CanConnectAsync(ct).ConfigureAwait(false);
            return Results.Ok(
                new ReliabilitySloSnapshotDto(
                    now,
                    publishes,
                    consumes,
                    successRate,
                    backlog,
                    oldestQueuedAt is null ? null : (long)(now - oldestQueuedAt.Value).TotalSeconds,
                    completed,
                    failedLastHour,
                    apiReady));
        })
    .WithName("ReliabilitySloSnapshot");

app.MapPut(
        "/api/workers/{key}",
        async (string key, WorkerPatchRequest body, NightmareDbContext db, CancellationToken ct) =>
        {
            var row = await db.WorkerSwitches.FirstOrDefaultAsync(w => w.WorkerKey == key, ct).ConfigureAwait(false);
            if (row is null)
                return Results.NotFound();
            row.IsEnabled = body.Enabled;
            row.UpdatedAtUtc = DateTimeOffset.UtcNow;
            await db.SaveChangesAsync(ct).ConfigureAwait(false);
            return Results.NoContent();
        })
    .WithName("PatchWorker");


static async Task InitializeStartupDatabasesAsync(WebApplication app, bool skipStartupDatabase)
{
    var startupLog = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Startup");
    if (skipStartupDatabase)
    {
        startupLog.LogWarning(
            "Startup database EnsureCreated skipped (Nightmare:SkipStartupDatabase or NIGHTMARE_SKIP_STARTUP_DATABASE=1). "
            + "APIs that need Postgres will still fail until a database is reachable.");
        return;
    }

    var continueOnFailure = app.Configuration.GetValue("Nightmare:ContinueOnStartupDatabaseFailure", true);
    var retryDelays = new[]
    {
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(15),
    };

    for (var attempt = 1; attempt <= retryDelays.Length + 1; attempt++)
    {
        try
        {
            using var scope = app.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<NightmareDbContext>();
            await db.Database.EnsureCreatedAsync(app.Lifetime.ApplicationStopping).ConfigureAwait(false);
            await NightmareDbSchemaPatches.ApplyAfterEnsureCreatedAsync(db, app.Lifetime.ApplicationStopping).ConfigureAwait(false);
            await NightmareDbSeeder.SeedWorkerSwitchesAsync(db, app.Lifetime.ApplicationStopping).ConfigureAwait(false);

            try
            {
                var fileStoreFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<FileStoreDbContext>>();
                await using var fs = await fileStoreFactory.CreateDbContextAsync(app.Lifetime.ApplicationStopping).ConfigureAwait(false);
                await fs.Database.EnsureCreatedAsync(app.Lifetime.ApplicationStopping).ConfigureAwait(false);
            }
            catch (Exception ex) when (!app.Lifetime.ApplicationStopping.IsCancellationRequested)
            {
                startupLog.LogWarning(ex, "File store database unavailable; create database nightmare_v2_files or set ConnectionStrings:FileStore.");
            }

            startupLog.LogInformation("Startup database initialization completed.");
            return;
        }
        catch (Exception ex) when (attempt <= retryDelays.Length && !app.Lifetime.ApplicationStopping.IsCancellationRequested)
        {
            startupLog.LogWarning(
                ex,
                "Startup database initialization failed on attempt {Attempt}; retrying.",
                attempt);
            await Task.Delay(retryDelays[attempt - 1], app.Lifetime.ApplicationStopping).ConfigureAwait(false);
        }
        catch (Exception ex) when (!app.Lifetime.ApplicationStopping.IsCancellationRequested)
        {
            if (!continueOnFailure)
                throw;

            startupLog.LogError(
                ex,
                "Startup database initialization failed after retries. Command Center will continue to serve /health and diagnostics, but database-backed APIs will fail until Postgres/schema is fixed.");
            return;
        }
    }
}

app.Run();
