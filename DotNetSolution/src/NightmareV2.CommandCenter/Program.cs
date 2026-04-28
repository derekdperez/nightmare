using System.Net.Http;
using MassTransit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using NightmareV2.Application.FileStore;
using NightmareV2.CommandCenter;
using NightmareV2.CommandCenter.Components;
using NightmareV2.CommandCenter.Hubs;
using NightmareV2.CommandCenter.Models;
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

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<NightmareDbContext>();
    await db.Database.EnsureCreatedAsync().ConfigureAwait(false);
    await NightmareDbSchemaPatches.ApplyAfterEnsureCreatedAsync(db).ConfigureAwait(false);
    await NightmareDbSeeder.SeedWorkerSwitchesAsync(db).ConfigureAwait(false);

    try
    {
        var fileStoreFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<FileStoreDbContext>>();
        await using var fs = await fileStoreFactory.CreateDbContextAsync().ConfigureAwait(false);
        await fs.Database.EnsureCreatedAsync().ConfigureAwait(false);
    }
    catch (Exception ex)
    {
        var log = scope.ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("Startup");
        log.LogWarning(ex, "File store database unavailable; create database nightmare_v2_files or set ConnectionStrings:FileStore.");
    }
}

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
            IPublishEndpoint bus,
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
            await bus.Publish(
                    new TargetCreated(target.Id, target.RootDomain, target.GlobalMaxDepth, target.CreatedAtUtc, correlation),
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
        async (HttpRequest httpRequest, NightmareDbContext db, IPublishEndpoint bus, IHubContext<DiscoveryHub> hub, CancellationToken ct) =>
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
                await bus.Publish(
                        new TargetCreated(target.Id, target.RootDomain, target.GlobalMaxDepth, target.CreatedAtUtc, correlation),
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
            var q = db.HighValueFindings.AsNoTracking()
                .Join(
                    db.Targets.AsNoTracking(),
                    f => f.TargetId,
                    t => t.Id,
                    (f, t) => new { f, t.RootDomain });
            if (criticalOnly == true)
                q = q.Where(x => x.f.Severity == "Critical");
            var rows = await q.OrderByDescending(x => x.f.DiscoveredAtUtc)
                .Take(limit)
                .Select(x => new HighValueFindingRowDto(
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
                    x.RootDomain))
                .ToListAsync(ct)
                .ConfigureAwait(false);
            return Results.Ok(rows);
        })
    .WithName("ListHighValueFindings");

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

app.Run();
