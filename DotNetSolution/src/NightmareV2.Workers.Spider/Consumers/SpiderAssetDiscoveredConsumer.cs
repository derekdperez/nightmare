using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using MassTransit;
using Microsoft.Extensions.Logging;
using NightmareV2.Application.Assets;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Application.Workers;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;
using Polly;
using Polly.Extensions.Http;

namespace NightmareV2.Workers.Spider.Consumers;

/// <summary>
/// Subscribes to indexed assets, confirms them over HTTP, harvests links, and publishes Raw discoveries back to Gatekeeper.
/// </summary>
public sealed class SpiderAssetDiscoveredConsumer(
    IHttpClientFactory httpFactory,
    IAssetPersistence persistence,
    IWorkerToggleReader workerToggles,
    ILogger<SpiderAssetDiscoveredConsumer> logger) : IConsumer<AssetDiscovered>
{
    private const int MaxLinksPerAsset = 500;
    private const int MaxBodyCaptureChars = 200_000;

    public async Task Consume(ConsumeContext<AssetDiscovered> context)
    {
        if (!await workerToggles.IsWorkerEnabledAsync(WorkerKeys.Spider, context.CancellationToken).ConfigureAwait(false))
        {
            logger.LogDebug("Spider disabled; skipping indexed asset {AssetId}", context.Message.AssetId);
            return;
        }

        var m = context.Message;
        if (m.AdmissionStage != AssetAdmissionStage.Indexed || m.AssetId is not { } assetId)
            return;

        if (!ShouldFetch(m.Kind))
            return;

        if (!TryResolveFetchUri(m, out var fetchUri))
        {
            logger.LogWarning("Spider could not resolve URL for {Kind} {Raw}", m.Kind, m.RawValue);
            return;
        }

        var http = httpFactory.CreateClient("spider");
        var sw = Stopwatch.StartNew();
        using var request = new HttpRequestMessage(HttpMethod.Get, fetchUri);
        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, context.CancellationToken)
            .ConfigureAwait(false);
        sw.Stop();

        var reqHeaders = HeadersToDict(request.Headers);
        var respHeaders = HeadersToDict(response.Headers);
        if (response.Content.Headers is { } ch)
        {
            foreach (var h in ch)
                respHeaders[h.Key] = string.Join(", ", h.Value);
        }

        var body = await response.Content.ReadAsStringAsync(context.CancellationToken).ConfigureAwait(false);
        var truncatedBody = body.Length > MaxBodyCaptureChars ? body[..MaxBodyCaptureChars] : body;
        var contentType = response.Content.Headers.ContentType?.ToString();

        var snapshot = new UrlFetchSnapshot(
            request.Method.Method,
            reqHeaders,
            null,
            (int)response.StatusCode,
            respHeaders,
            truncatedBody,
            response.Content.Headers.ContentLength,
            sw.Elapsed.TotalMilliseconds,
            contentType,
            DateTimeOffset.UtcNow,
            response.RequestMessage?.RequestUri?.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped));

        var isSoft404 = UrlFetchClassifier.LooksLikeSoft404(snapshot);

        await persistence.ConfirmUrlAssetAsync(assetId, snapshot, m.CorrelationId, context.CancellationToken)
            .ConfigureAwait(false);

        if (!response.IsSuccessStatusCode || isSoft404)
            return;

        var ct = contentType ?? "";
        var parentPage = fetchUri.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped);
        var spiderContext = TruncateDiscoveryContext($"Spider: link extracted from fetched page {parentPage}");
        foreach (var link in LinkHarvest.Extract(body, ct, fetchUri).Take(MaxLinksPerAsset))
        {
            var kind = LinkHarvest.GuessKindForUrl(link);
            await context.Publish(
                    new AssetDiscovered(
                        m.TargetId,
                        m.TargetRootDomain,
                        m.GlobalMaxDepth,
                        m.Depth + 1,
                        kind,
                        link,
                        "spider-worker",
                        DateTimeOffset.UtcNow,
                        m.CorrelationId,
                        AssetAdmissionStage.Raw,
                        null,
                        spiderContext),
                    context.CancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static string TruncateDiscoveryContext(string s, int maxChars = 512) =>
        s.Length <= maxChars ? s : s[..(maxChars - 1)] + "…";

    private static bool ShouldFetch(AssetKind kind) =>
        kind is AssetKind.Url or AssetKind.ApiEndpoint or AssetKind.JavaScriptFile or AssetKind.MarkdownBody
            or AssetKind.Subdomain or AssetKind.Domain;

    private static bool TryResolveFetchUri(AssetDiscovered m, out Uri uri)
    {
        uri = default!;
        if (m.Kind is AssetKind.Subdomain or AssetKind.Domain)
        {
            var host = m.RawValue.Trim().TrimEnd('/');
            if (host.Length == 0)
                return false;
            return Uri.TryCreate($"https://{host}/", UriKind.Absolute, out uri);
        }

        var raw = m.RawValue.Trim();
        if (Uri.TryCreate(raw, UriKind.Absolute, out uri))
            return uri.Scheme is "http" or "https";
        return Uri.TryCreate("https://" + raw, UriKind.Absolute, out uri);
    }

    private static Dictionary<string, string> HeadersToDict(HttpHeaders headers)
    {
        var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var h in headers)
            d[h.Key] = string.Join(", ", h.Value);
        return d;
    }

    public static IAsyncPolicy<HttpResponseMessage> RetryPolicy() =>
        HttpPolicyExtensions
            .HandleTransientHttpError()
            .OrResult(r => r.StatusCode == HttpStatusCode.TooManyRequests)
            .WaitAndRetryAsync(3, attempt => TimeSpan.FromMilliseconds(200 * attempt));
}
