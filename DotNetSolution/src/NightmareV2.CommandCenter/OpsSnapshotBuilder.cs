using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using NightmareV2.Application.Workers;
using NightmareV2.CommandCenter.Models;
using NightmareV2.Contracts;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.CommandCenter;

internal static class OpsSnapshotBuilder
{
    private const string HttpClientName = "ops-rabbit";

    private static readonly JsonSerializerOptions RabbitJson = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private static readonly (string Key, string ConsumerSubstring)[] WorkerConsumeMarkers =
    [
        (WorkerKeys.Gatekeeper, "Gatekeeper.Consumers.AssetDiscoveredConsumer"),
        (WorkerKeys.Spider, "SpiderAssetDiscoveredConsumer"),
        (WorkerKeys.Enumeration, "Workers.Enum.Consumers.TargetCreatedConsumer"),
        (WorkerKeys.PortScan, "PortScanRequestedConsumer"),
        (WorkerKeys.HighValueRegex, "Workers.HighValue.Consumers.HighValueRegexConsumer"),
        (WorkerKeys.HighValuePaths, "Workers.HighValue.Consumers.HighValuePathGuessConsumer"),
    ];

    public static void RegisterHttpClient(WebApplicationBuilder builder)
    {
        builder.Services.AddHttpClient(HttpClientName, client => client.Timeout = TimeSpan.FromSeconds(12));
    }

    public static async Task<OpsSnapshotDto> BuildAsync(
        NightmareDbContext db,
        IHttpClientFactory httpFactory,
        IConfiguration configuration,
        CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var h1 = now.AddHours(-1);
        var h24 = now.AddHours(-24);

        // Same NightmareDbContext must not run concurrent async queries (EF Core is not thread-safe).
        var rabbitTask = TryLoadRabbitQueuesAsync(httpFactory, configuration, cancellationToken);

        var workers = await db.WorkerSwitches.AsNoTracking()
            .OrderBy(w => w.WorkerKey)
            .Select(w => new WorkerSwitchDto(w.WorkerKey, w.IsEnabled, w.UpdatedAtUtc))
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);
        var activity = await WorkerActivityQuery.BuildSnapshotAsync(db, cancellationToken).ConfigureAwait(false);
        var assets = await LoadAssetSummaryAsync(db, h1, h24, cancellationToken).ConfigureAwait(false);
        var busTraffic = await LoadBusTrafficAsync(db, h1, h24, cancellationToken).ConfigureAwait(false);

        var (queues, rabbitOk) = await rabbitTask.ConfigureAwait(false);

        var rabbitByWorker = AggregateRabbitByWorker(queues);

        var order = new[]
        {
            WorkerKeys.Gatekeeper,
            WorkerKeys.Spider,
            WorkerKeys.Enumeration,
            WorkerKeys.PortScan,
            WorkerKeys.HighValueRegex,
            WorkerKeys.HighValuePaths,
        };
        var metrics = new List<WorkerDetailStatsDto>(order.Length);
        foreach (var key in order)
        {
            metrics.Add(
                await BuildOneWorkerDetailAsync(db, key, h1, h24, rabbitByWorker, cancellationToken)
                    .ConfigureAwait(false));
        }

        return new OpsSnapshotDto(
            workers,
            activity,
            assets,
            busTraffic,
            metrics,
            queues,
            rabbitOk);
    }

    private static async Task<AssetOpsSummaryDto> LoadAssetSummaryAsync(
        NightmareDbContext db,
        DateTimeOffset h1,
        DateTimeOffset h24,
        CancellationToken ct)
    {
        // Single DbContext: run queries sequentially (EF Core is not safe for concurrent ops on one context).
        var assets = db.Assets.AsNoTracking();
        var totalAssets = await assets.LongCountAsync(ct).ConfigureAwait(false);
        var totalTargets = await db.Targets.AsNoTracking().LongCountAsync(ct).ConfigureAwait(false);
        var assets1h = await assets.LongCountAsync(a => a.DiscoveredAtUtc >= h1, ct).ConfigureAwait(false);
        var assets24h = await assets.LongCountAsync(a => a.DiscoveredAtUtc >= h24, ct).ConfigureAwait(false);
        var lastAssetUtc = await assets
            .OrderByDescending(a => a.DiscoveredAtUtc)
            .Select(a => (DateTimeOffset?)a.DiscoveredAtUtc)
            .FirstOrDefaultAsync(ct)
            .ConfigureAwait(false);

        var discoveredCount = await assets.LongCountAsync(a => a.LifecycleStatus == "Discovered", ct)
            .ConfigureAwait(false);
        var queuedCount = await assets.LongCountAsync(a => a.LifecycleStatus == AssetLifecycleStatus.Queued, ct)
            .ConfigureAwait(false);
        var confirmedCount = await assets.LongCountAsync(a => a.LifecycleStatus == AssetLifecycleStatus.Confirmed, ct)
            .ConfigureAwait(false);

        var fetchableDiscovered = await assets.LongCountAsync(
                a => (a.LifecycleStatus == "Discovered"
                        || a.LifecycleStatus == AssetLifecycleStatus.Queued)
                    && (a.Kind == AssetKind.Url
                        || a.Kind == AssetKind.ApiEndpoint
                        || a.Kind == AssetKind.JavaScriptFile
                        || a.Kind == AssetKind.MarkdownBody
                        || a.Kind == AssetKind.Subdomain
                        || a.Kind == AssetKind.Domain),
                ct)
            .ConfigureAwait(false);

        var subdomainsTotal = await assets.LongCountAsync(a => a.Kind == AssetKind.Subdomain, ct).ConfigureAwait(false);
        var domainsTotal = await assets.LongCountAsync(a => a.Kind == AssetKind.Domain, ct).ConfigureAwait(false);
        var ipAddressesTotal = await assets.LongCountAsync(a => a.Kind == AssetKind.IpAddress, ct).ConfigureAwait(false);
        var urlsTotal = await assets.LongCountAsync(a => a.Kind == AssetKind.Url, ct).ConfigureAwait(false);
        var urlsConfirmed = await assets.LongCountAsync(
                a => a.Kind == AssetKind.Url && a.LifecycleStatus == AssetLifecycleStatus.Confirmed,
                ct)
            .ConfigureAwait(false);
        var httpPipelineTotal = await assets.LongCountAsync(
                a => a.Kind == AssetKind.Url
                    || a.Kind == AssetKind.ApiEndpoint
                    || a.Kind == AssetKind.JavaScriptFile
                    || a.Kind == AssetKind.MarkdownBody,
                ct)
            .ConfigureAwait(false);
        var httpPipelineConfirmed = await assets.LongCountAsync(
                a => (a.Kind == AssetKind.Url
                        || a.Kind == AssetKind.ApiEndpoint
                        || a.Kind == AssetKind.JavaScriptFile
                        || a.Kind == AssetKind.MarkdownBody)
                    && a.LifecycleStatus == AssetLifecycleStatus.Confirmed,
                ct)
            .ConfigureAwait(false);
        var httpSnapshotsSaved = await assets.LongCountAsync(
                a => a.TypeDetailsJson != null && a.TypeDetailsJson != "",
                ct)
            .ConfigureAwait(false);
        var openPortsTotal = await assets.LongCountAsync(a => a.Kind == AssetKind.OpenPort, ct).ConfigureAwait(false);
        var highValueFindingsTotal = await db.HighValueFindings.AsNoTracking().LongCountAsync(ct).ConfigureAwait(false);

        var topDomains = await assets
            .Join(
                db.Targets.AsNoTracking(),
                a => a.TargetId,
                t => t.Id,
                (a, t) => t.RootDomain)
            .GroupBy(d => d)
            .Select(g => new AssetCountByDomainDto(g.Key, g.LongCount()))
            .OrderByDescending(x => x.Count)
            .Take(25)
            .ToListAsync(ct)
            .ConfigureAwait(false);

        var byDiscoveredBy = await assets
            .GroupBy(a => a.DiscoveredBy)
            .Select(g => new DiscoveredByCountDto(g.Key, g.LongCount()))
            .OrderByDescending(x => x.Count)
            .ToListAsync(ct)
            .ConfigureAwait(false);

        return new AssetOpsSummaryDto(
            totalAssets,
            totalTargets,
            assets1h,
            assets24h,
            lastAssetUtc,
            discoveredCount,
            queuedCount,
            confirmedCount,
            fetchableDiscovered,
            subdomainsTotal,
            domainsTotal,
            ipAddressesTotal,
            urlsTotal,
            urlsConfirmed,
            httpPipelineTotal,
            httpPipelineConfirmed,
            httpSnapshotsSaved,
            openPortsTotal,
            highValueFindingsTotal,
            topDomains,
            byDiscoveredBy);
    }

    private static async Task<BusTrafficSummaryDto> LoadBusTrafficAsync(
        NightmareDbContext db,
        DateTimeOffset h1,
        DateTimeOffset h24,
        CancellationToken ct)
    {
        var p1 = await db.BusJournal.AsNoTracking()
            .LongCountAsync(e => e.Direction == "Publish" && e.OccurredAtUtc >= h1, ct)
            .ConfigureAwait(false);
        var p24 = await db.BusJournal.AsNoTracking()
            .LongCountAsync(e => e.Direction == "Publish" && e.OccurredAtUtc >= h24, ct)
            .ConfigureAwait(false);
        var c1 = await db.BusJournal.AsNoTracking()
            .LongCountAsync(e => e.Direction == "Consume" && e.OccurredAtUtc >= h1, ct)
            .ConfigureAwait(false);
        var c24 = await db.BusJournal.AsNoTracking()
            .LongCountAsync(e => e.Direction == "Consume" && e.OccurredAtUtc >= h24, ct)
            .ConfigureAwait(false);
        return new BusTrafficSummaryDto(p1, p24, c1, c24);
    }

    private static async Task<WorkerDetailStatsDto> BuildOneWorkerDetailAsync(
        NightmareDbContext db,
        string workerKey,
        DateTimeOffset h1,
        DateTimeOffset h24,
        Dictionary<string, RabbitAgg> rabbitByWorker,
        CancellationToken ct)
    {
        var marker = WorkerConsumeMarkers.First(m => m.Key == workerKey).ConsumerSubstring;

        var c1 = await db.BusJournal.AsNoTracking()
            .LongCountAsync(
                e => e.Direction == "Consume" && e.ConsumerType != null && e.ConsumerType.Contains(marker)
                    && e.OccurredAtUtc >= h1,
                ct)
            .ConfigureAwait(false);
        var c24 = await db.BusJournal.AsNoTracking()
            .LongCountAsync(
                e => e.Direction == "Consume" && e.ConsumerType != null && e.ConsumerType.Contains(marker)
                    && e.OccurredAtUtc >= h24,
                ct)
            .ConfigureAwait(false);
        var last = await db.BusJournal.AsNoTracking()
            .Where(e => e.Direction == "Consume" && e.ConsumerType != null && e.ConsumerType.Contains(marker))
            .OrderByDescending(e => e.Id)
            .Select(e => (DateTimeOffset?)e.OccurredAtUtc)
            .FirstOrDefaultAsync(ct)
            .ConfigureAwait(false);
        var (a1, a24) = await LoadAttributedAssetsAsync(db, workerKey, h1, h24, ct).ConfigureAwait(false);

        if (!rabbitByWorker.TryGetValue(workerKey, out var rb))
            rb = new RabbitAgg();

        return new WorkerDetailStatsDto(
            workerKey,
            c1,
            c24,
            last,
            a1,
            a24,
            rb.Ready,
            rb.Unacked,
            rb.Names);
    }

    private static async Task<(long H1, long H24)> LoadAttributedAssetsAsync(
        NightmareDbContext db,
        string workerKey,
        DateTimeOffset h1,
        DateTimeOffset h24,
        CancellationToken ct)
    {
        var by = DiscoveredByForWorker(workerKey);
        if (by is null)
            return (0, 0);

        var a1 = await db.Assets.AsNoTracking()
            .LongCountAsync(a => a.DiscoveredBy == by && a.DiscoveredAtUtc >= h1, ct)
            .ConfigureAwait(false);
        var a24 = await db.Assets.AsNoTracking()
            .LongCountAsync(a => a.DiscoveredBy == by && a.DiscoveredAtUtc >= h24, ct)
            .ConfigureAwait(false);
        return (a1, a24);
    }

    private static string? DiscoveredByForWorker(string workerKey) =>
        workerKey switch
        {
            WorkerKeys.Gatekeeper => "gatekeeper",
            WorkerKeys.Spider => "spider-worker",
            WorkerKeys.Enumeration => "enum-worker-stub",
            _ => null,
        };

    private sealed class RabbitAgg
    {
        public long Ready;
        public long Unacked;
        public readonly List<string> Names = [];
    }

    private static Dictionary<string, RabbitAgg> AggregateRabbitByWorker(IReadOnlyList<RabbitQueueBriefDto> queues)
    {
        var d = new Dictionary<string, RabbitAgg>(StringComparer.Ordinal);
        foreach (var q in queues)
        {
            if (q.LikelyWorkerKey is not { } key)
                continue;
            if (!d.TryGetValue(key, out var agg))
            {
                agg = new RabbitAgg();
                d[key] = agg;
            }

            agg.Ready += q.MessagesReady;
            agg.Unacked += q.MessagesUnacknowledged;
            agg.Names.Add(q.Name);
        }

        foreach (var agg in d.Values)
            agg.Names.Sort(StringComparer.OrdinalIgnoreCase);
        return d;
    }

    private static async Task<(List<RabbitQueueBriefDto> Queues, bool Ok)> TryLoadRabbitQueuesAsync(
        IHttpClientFactory httpFactory,
        IConfiguration configuration,
        CancellationToken ct)
    {
        var baseUrl = configuration["RabbitMq:ManagementUrl"]?.Trim();
        if (string.IsNullOrEmpty(baseUrl))
            return ([], false);

        var vhost = configuration["RabbitMq:VirtualHost"];
        if (string.IsNullOrEmpty(vhost))
            vhost = "/";
        var vhostSeg = Uri.EscapeDataString(vhost);
        var url = $"{baseUrl.TrimEnd('/')}/api/queues/{vhostSeg}";

        var user = configuration["RabbitMq:Username"] ?? "guest";
        var pass = configuration["RabbitMq:Password"] ?? "guest";

        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Get, url);
            var token = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{user}:{pass}"));
            req.Headers.Authorization = new AuthenticationHeaderValue("Basic", token);

            var client = httpFactory.CreateClient(HttpClientName);
            using var resp = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct)
                .ConfigureAwait(false);
            if (!resp.IsSuccessStatusCode)
                return ([], false);

            var json = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            var rows = JsonSerializer.Deserialize<List<RabbitMgmtQueueRow>>(json, RabbitJson) ?? [];

            var list = new List<RabbitQueueBriefDto>(rows.Count);
            foreach (var r in rows)
            {
                var name = r.Name ?? "";
                if (name.StartsWith("amq.", StringComparison.OrdinalIgnoreCase))
                    continue;
                list.Add(
                    new RabbitQueueBriefDto(
                        name,
                        r.Messages,
                        r.MessagesReady,
                        r.MessagesUnacknowledged,
                        r.Consumers,
                        GuessWorkerFromQueueName(name)));
            }

            list.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase));
            return (list, true);
        }
        catch
        {
            return ([], false);
        }
    }

    private static string? GuessWorkerFromQueueName(string queueName)
    {
        var n = queueName.ToLowerInvariant();
        if (n.Contains("spider"))
            return WorkerKeys.Spider;
        if (n.Contains("gatekeeper"))
            return WorkerKeys.Gatekeeper;
        if (n.Contains("port-scan") || n.Contains("portscan"))
            return WorkerKeys.PortScan;
        if (n.Contains("target-created") || n.Contains("target_created")
            || (n.Contains("enum") && (n.Contains("target") || n.Contains("worker"))))
            return WorkerKeys.Enumeration;
        if (n.Contains("highvaluepath") || n.Contains("high-value-path") || n.Contains("hvpath"))
            return WorkerKeys.HighValuePaths;
        if (n.Contains("highvalue") || n.Contains("high-value") || n.Contains("high_value") || n.Contains("scannable"))
            return WorkerKeys.HighValueRegex;
        return null;
    }

    private sealed class RabbitMgmtQueueRow
    {
        public string? Name { get; set; }
        public int Messages { get; set; }

        [JsonPropertyName("messages_ready")]
        public int MessagesReady { get; set; }

        [JsonPropertyName("messages_unacknowledged")]
        public int MessagesUnacknowledged { get; set; }

        public int Consumers { get; set; }
    }
}
