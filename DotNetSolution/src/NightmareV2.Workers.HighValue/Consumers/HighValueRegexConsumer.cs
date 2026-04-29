using System.Net.Http.Json;
using System.Text.Json;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using NightmareV2.Application.Assets;
using NightmareV2.Application.Events;
using NightmareV2.Application.HighValue;
using NightmareV2.Application.Workers;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.Workers.HighValue.Consumers;

public sealed class HighValueRegexConsumer(
    NightmareDbContext db,
    IWorkerToggleReader toggles,
    IInboxDeduplicator inbox,
    IHighValueFindingWriter writer,
    IEventOutbox outbox,
    IConfiguration configuration,
    HighValueRegexMatcher matcher,
    IHttpClientFactory httpFactory,
    ILogger<HighValueRegexConsumer> logger) : IConsumer<ScannableContentAvailable>
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public async Task Consume(ConsumeContext<ScannableContentAvailable> context)
    {
        if (!await inbox.TryBeginProcessingAsync(context.Message, nameof(HighValueRegexConsumer), context.CancellationToken).ConfigureAwait(false))
            return;

        var ct = context.CancellationToken;
        if (!await toggles.IsWorkerEnabledAsync(WorkerKeys.HighValueRegex, ct).ConfigureAwait(false))
            return;

        var m = context.Message;
        if (m.Source != ScannableContentSource.UrlHttpResponse)
            return;

        var asset = await db.Assets.AsNoTracking()
            .Where(a => a.Id == m.AssetId)
            .Select(a => new { a.TypeDetailsJson, a.DiscoveredBy })
            .FirstOrDefaultAsync(ct)
            .ConfigureAwait(false);
        if (asset?.TypeDetailsJson is not { } json || string.IsNullOrWhiteSpace(json))
            return;

        UrlFetchSnapshot? snap;
        try
        {
            snap = JsonSerializer.Deserialize<UrlFetchSnapshot>(json, JsonOpts);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "HighValueRegex: could not deserialize snapshot for asset {AssetId}", m.AssetId);
            return;
        }

        if (snap is null)
            return;

        foreach (var hit in matcher.ScanUrlHttpExchange(m.SourceUrl, snap))
        {
            var severity = hit.ImportanceScore >= 10 ? "Critical" : hit.ImportanceScore >= 8 ? "High" : "Medium";
            var id = await writer.InsertFindingAsync(
                    new HighValueFindingInput(
                        m.TargetId,
                        m.AssetId,
                        "Regex",
                        severity,
                        hit.PatternName,
                        hit.Scope,
                        hit.MatchedSnippet,
                        m.SourceUrl,
                        WorkerKeys.HighValueRegex,
                        hit.ImportanceScore),
                    ct)
                .ConfigureAwait(false);
            if (severity == "Critical")
                await RaiseCriticalAsync(id, m, hit.PatternName, ct).ConfigureAwait(false);
        }

        if (snap.StatusCode is >= 200 and < 300
            && asset.DiscoveredBy.Contains("hvpath:critical", StringComparison.OrdinalIgnoreCase))
        {
            var id = await writer.InsertFindingAsync(
                    new HighValueFindingInput(
                        m.TargetId,
                        m.AssetId,
                        "SensitivePath",
                        "Critical",
                        "critical_path_http_2xx",
                        "critical",
                        $"HTTP {snap.StatusCode}",
                        m.SourceUrl,
                        WorkerKeys.HighValueRegex,
                        10),
                    ct)
                .ConfigureAwait(false);
            await RaiseCriticalAsync(id, m, "critical_path_http_2xx", ct).ConfigureAwait(false);
        }
    }

    private async Task RaiseCriticalAsync(
        Guid findingId,
        ScannableContentAvailable m,
        string patternName,
        CancellationToken ct)
    {
        await outbox.EnqueueAsync(
                new CriticalHighValueFindingAlert(
                    findingId,
                    m.TargetId,
                    m.AssetId,
                    patternName,
                    m.SourceUrl,
                    "Critical",
                    DateTimeOffset.UtcNow,
                    m.CorrelationId,
                    EventId: NewId.NextGuid(),
                    CausationId: m.EventId == Guid.Empty ? m.CorrelationId : m.EventId,
                    Producer: "worker-highvalue-regex"),
                ct)
            .ConfigureAwait(false);

        var webhookUrl = configuration["HighValue:CriticalWebhookUrl"]?.Trim();
        if (string.IsNullOrEmpty(webhookUrl))
            return;

        try
        {
            var client = httpFactory.CreateClient();
            var payload = new
            {
                findingId,
                m.TargetId,
                m.AssetId,
                patternName,
                m.SourceUrl,
                severity = "Critical",
                atUtc = DateTimeOffset.UtcNow,
            };
            using var resp = await client.PostAsJsonAsync(webhookUrl, payload, ct).ConfigureAwait(false);
            if (!resp.IsSuccessStatusCode)
                logger.LogWarning("Critical webhook returned {Status} for {Url}", (int)resp.StatusCode, webhookUrl);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Critical webhook POST failed for {Url}", webhookUrl);
        }
    }
}
