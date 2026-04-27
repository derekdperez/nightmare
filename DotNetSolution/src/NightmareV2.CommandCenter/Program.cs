using MassTransit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
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
}

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
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
            var root = dto.RootDomain.Trim().TrimEnd('.').ToLowerInvariant();
            if (string.IsNullOrWhiteSpace(root))
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
