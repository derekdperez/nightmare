using System.Data.Common;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using NightmareV2.Application.Assets;
using NightmareV2.Application.Events;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Application.Workers;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.Workers.Spider;

public sealed class HttpRequestQueueWorker(
    IDbContextFactory<NightmareDbContext> dbFactory,
    IHttpClientFactory httpFactory,
    IServiceScopeFactory scopeFactory,
    IEventOutbox outbox,
    IWorkerToggleReader workerToggles,
    ILogger<HttpRequestQueueWorker> logger) : BackgroundService
{
    private const int MaxLinksPerAsset = 500;
    private const int MaxBodyCaptureChars = 200_000;
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };
    private readonly string _workerId = $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid():N}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("HTTP request queue worker {WorkerId} starting.", _workerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!await workerToggles.IsWorkerEnabledAsync(WorkerKeys.Spider, stoppingToken).ConfigureAwait(false))
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await ReapExpiredLocksAsync(stoppingToken).ConfigureAwait(false);
                var lease = await TryLeaseNextAsync(stoppingToken).ConfigureAwait(false);
                if (lease is null)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(500), stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await ProcessLeaseAsync(lease, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "HTTP request queue worker loop fault.");
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private async Task ReapExpiredLocksAsync(CancellationToken ct)
    {
        await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;

        await db.HttpRequestQueue
            .Where(q => q.State == HttpRequestQueueState.InFlight
                && q.LockedUntilUtc < now
                && q.AttemptCount < q.MaxAttempts)
            .ExecuteUpdateAsync(
                s => s
                    .SetProperty(q => q.State, HttpRequestQueueState.Retry)
                    .SetProperty(q => q.UpdatedAtUtc, now)
                    .SetProperty(q => q.NextAttemptAtUtc, now)
                    .SetProperty(q => q.LockedBy, (string?)null)
                    .SetProperty(q => q.LockedUntilUtc, (DateTimeOffset?)null)
                    .SetProperty(q => q.LastError, "Worker lock expired before completion; request will be retried."),
                ct)
            .ConfigureAwait(false);

        await db.HttpRequestQueue
            .Where(q => q.State == HttpRequestQueueState.InFlight
                && q.LockedUntilUtc < now
                && q.AttemptCount >= q.MaxAttempts)
            .ExecuteUpdateAsync(
                s => s
                    .SetProperty(q => q.State, HttpRequestQueueState.Failed)
                    .SetProperty(q => q.UpdatedAtUtc, now)
                    .SetProperty(q => q.CompletedAtUtc, now)
                    .SetProperty(q => q.LockedBy, (string?)null)
                    .SetProperty(q => q.LockedUntilUtc, (DateTimeOffset?)null)
                    .SetProperty(q => q.LastError, "Worker lock expired after the final allowed attempt."),
                ct)
            .ConfigureAwait(false);
    }

    private async Task<HttpRequestQueueItem?> TryLeaseNextAsync(CancellationToken ct)
    {
        await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;
        var oneMinuteAgo = now.AddMinutes(-1);
        var lockUntil = now.AddMinutes(5);

        var conn = db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct).ConfigureAwait(false);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          WITH candidate AS (
                              SELECT q.*
                              FROM http_request_queue q
                              CROSS JOIN http_request_queue_settings s
                              WHERE s.id = 1
                                AND s.enabled = true
                                AND (
                                    (q.state IN ('Queued', 'Retry') AND q.next_attempt_at_utc <= @now)
                                    OR (q.state = 'InFlight' AND q.locked_until_utc < @now)
                                )
                                AND q.attempt_count < q.max_attempts
                                AND (
                                    SELECT COUNT(*)
                                    FROM http_request_queue running
                                    WHERE running.state = 'InFlight'
                                      AND running.locked_until_utc > @now
                                ) < s.max_concurrency
                                AND (
                                    SELECT COUNT(*)
                                    FROM http_request_queue recent_global
                                    WHERE recent_global.started_at_utc IS NOT NULL
                                      AND recent_global.started_at_utc >= @one_minute_ago
                                ) < s.global_requests_per_minute
                                AND (
                                    SELECT COUNT(*)
                                    FROM http_request_queue recent_domain
                                    WHERE recent_domain.domain_key = q.domain_key
                                      AND recent_domain.started_at_utc IS NOT NULL
                                      AND recent_domain.started_at_utc >= @one_minute_ago
                                ) < s.per_domain_requests_per_minute
                              ORDER BY q.priority DESC, q.next_attempt_at_utc ASC, q.created_at_utc ASC
                              FOR UPDATE SKIP LOCKED
                              LIMIT 1
                          )
                          UPDATE http_request_queue q
                          SET state = 'InFlight',
                              locked_by = @worker_id,
                              locked_until_utc = @lock_until,
                              started_at_utc = @now,
                              updated_at_utc = @now,
                              attempt_count = q.attempt_count + 1,
                              last_error = NULL
                          FROM candidate
                          WHERE q.id = candidate.id
                          RETURNING q.*;
                          """;
        AddParameter(cmd, "now", now);
        AddParameter(cmd, "one_minute_ago", oneMinuteAgo);
        AddParameter(cmd, "worker_id", _workerId);
        AddParameter(cmd, "lock_until", lockUntil);

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return MapQueueItem(reader);
    }

    private static void AddParameter(DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }

    private static HttpRequestQueueItem MapQueueItem(DbDataReader reader) =>
        new()
        {
            Id = reader.GetGuid(reader.GetOrdinal("id")),
            AssetId = reader.GetGuid(reader.GetOrdinal("asset_id")),
            TargetId = reader.GetGuid(reader.GetOrdinal("target_id")),
            AssetKind = (AssetKind)reader.GetInt32(reader.GetOrdinal("asset_kind")),
            Method = reader.GetString(reader.GetOrdinal("method")),
            RequestUrl = reader.GetString(reader.GetOrdinal("request_url")),
            DomainKey = reader.GetString(reader.GetOrdinal("domain_key")),
            State = reader.GetString(reader.GetOrdinal("state")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            AttemptCount = reader.GetInt32(reader.GetOrdinal("attempt_count")),
            MaxAttempts = reader.GetInt32(reader.GetOrdinal("max_attempts")),
            CreatedAtUtc = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("created_at_utc")),
            UpdatedAtUtc = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("updated_at_utc")),
            NextAttemptAtUtc = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("next_attempt_at_utc")),
            LockedBy = ReadNullableString(reader, "locked_by"),
            LockedUntilUtc = ReadNullableDateTimeOffset(reader, "locked_until_utc"),
            StartedAtUtc = ReadNullableDateTimeOffset(reader, "started_at_utc"),
            CompletedAtUtc = ReadNullableDateTimeOffset(reader, "completed_at_utc"),
            DurationMs = ReadNullableInt64(reader, "duration_ms"),
            LastHttpStatus = ReadNullableInt32(reader, "last_http_status"),
            LastError = ReadNullableString(reader, "last_error"),
            RequestHeadersJson = ReadNullableString(reader, "request_headers_json"),
            RequestBody = ReadNullableString(reader, "request_body"),
            ResponseHeadersJson = ReadNullableString(reader, "response_headers_json"),
            ResponseBody = ReadNullableString(reader, "response_body"),
            ResponseContentType = ReadNullableString(reader, "response_content_type"),
            ResponseContentLength = ReadNullableInt64(reader, "response_content_length"),
            FinalUrl = ReadNullableString(reader, "final_url"),
        };

    private static string? ReadNullableString(DbDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private static int? ReadNullableInt32(DbDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);
    }

    private static long? ReadNullableInt64(DbDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal) ? null : reader.GetInt64(ordinal);
    }

    private static DateTimeOffset? ReadNullableDateTimeOffset(DbDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<DateTimeOffset>(ordinal);
    }

    private async Task ProcessLeaseAsync(HttpRequestQueueItem item, CancellationToken ct)
    {
        HttpRequestQueueSettings settings;
        StoredAsset? asset;
        await using (var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false))
        {
            settings = await db.HttpRequestQueueSettings.AsNoTracking()
                .FirstOrDefaultAsync(s => s.Id == 1, ct)
                .ConfigureAwait(false)
                ?? new HttpRequestQueueSettings();

            asset = await db.Assets.AsNoTracking()
                .Include(a => a.Target)
                .FirstOrDefaultAsync(a => a.Id == item.AssetId, ct)
                .ConfigureAwait(false);
        }

        if (asset is null)
        {
            await MarkFailedAsync(item.Id, "Asset no longer exists.", terminal: true, ct).ConfigureAwait(false);
            return;
        }

        var timeout = TimeSpan.FromSeconds(Math.Clamp(settings.RequestTimeoutSeconds, 5, 300));
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);

        try
        {
            var snapshot = await SendAsync(item, timeoutCts.Token).ConfigureAwait(false);
            if (ShouldQueueRetry((HttpStatusCode)snapshot.StatusCode) && item.AttemptCount < item.MaxAttempts)
            {
                await SaveRetryResponseAsync(item, snapshot, $"Transient HTTP {snapshot.StatusCode}; retry scheduled.", ct)
                    .ConfigureAwait(false);
                return;
            }

            var terminalState = ShouldQueueRetry((HttpStatusCode)snapshot.StatusCode)
                ? HttpRequestQueueState.Failed
                : HttpRequestQueueState.Succeeded;
            await SaveResponseAsync(item.Id, snapshot, terminalState, null, terminal: true, ct).ConfigureAwait(false);

            using (var scope = scopeFactory.CreateScope())
            {
                var persistence = scope.ServiceProvider.GetRequiredService<IAssetPersistence>();
                await persistence.ConfirmUrlAssetAsync(item.AssetId, snapshot, Guid.Empty, ct).ConfigureAwait(false);
            }

            if (snapshot.StatusCode is >= 200 and < 300 && !UrlFetchClassifier.LooksLikeSoft404(snapshot))
                await HarvestLinksAsync(asset, snapshot, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            await RetryOrFailAsync(item, "HTTP request timed out.", ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsHttpTransient(ex))
        {
            await RetryOrFailAsync(item, ex.Message, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "HTTP request queue item {QueueItemId} failed with an unexpected worker error.", item.Id);
            await RetryOrFailAsync(item, ex.Message, ct).ConfigureAwait(false);
        }
    }

    private async Task<UrlFetchSnapshot> SendAsync(HttpRequestQueueItem item, CancellationToken ct)
    {
        var http = httpFactory.CreateClient("spider");
        var sw = Stopwatch.StartNew();

        using var request = new HttpRequestMessage(new HttpMethod(item.Method), item.RequestUrl);
        request.Headers.UserAgent.ParseAdd("NightmareV2/1.0");

        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct)
            .ConfigureAwait(false);
        sw.Stop();

        var reqHeaders = HeadersToDict(request.Headers);
        var respHeaders = HeadersToDict(response.Headers);
        foreach (var h in response.Content.Headers)
            respHeaders[h.Key] = string.Join(", ", h.Value);

        var truncatedBody = await BoundedHttpContentReader.ReadAsStringAsync(response.Content, MaxBodyCaptureChars, ct)
            .ConfigureAwait(false);
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

        return snapshot;
    }

    private async Task SaveResponseAsync(
        Guid queueItemId,
        UrlFetchSnapshot snapshot,
        string state,
        string? error,
        bool terminal,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var item = await db.HttpRequestQueue.FirstOrDefaultAsync(q => q.Id == queueItemId, ct).ConfigureAwait(false);
        if (item is null)
            return;

        item.State = state;
        item.UpdatedAtUtc = now;
        item.CompletedAtUtc = terminal ? now : item.CompletedAtUtc;
        item.LockedBy = null;
        item.LockedUntilUtc = null;
        item.DurationMs = (long?)snapshot.DurationMs;
        item.LastHttpStatus = snapshot.StatusCode;
        item.LastError = error;
        item.RequestHeadersJson = JsonSerializer.Serialize(snapshot.RequestHeaders, JsonOptions);
        item.RequestBody = snapshot.RequestBody;
        item.ResponseHeadersJson = JsonSerializer.Serialize(snapshot.ResponseHeaders, JsonOptions);
        item.ResponseBody = snapshot.ResponseBody;
        item.ResponseContentType = snapshot.ContentType;
        item.ResponseContentLength = snapshot.ResponseSizeBytes;
        item.FinalUrl = snapshot.FinalUrl;

        await db.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    private async Task SaveRetryResponseAsync(HttpRequestQueueItem item, UrlFetchSnapshot snapshot, string error, CancellationToken ct)
    {
        var delay = TimeSpan.FromSeconds(Math.Min(300, Math.Pow(2, Math.Max(0, item.AttemptCount)) * 5));
        var now = DateTimeOffset.UtcNow;
        await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var row = await db.HttpRequestQueue.FirstOrDefaultAsync(q => q.Id == item.Id, ct).ConfigureAwait(false);
        if (row is null)
            return;

        row.State = HttpRequestQueueState.Retry;
        row.UpdatedAtUtc = now;
        row.NextAttemptAtUtc = now + delay;
        row.LockedBy = null;
        row.LockedUntilUtc = null;
        row.DurationMs = (long?)snapshot.DurationMs;
        row.LastHttpStatus = snapshot.StatusCode;
        row.LastError = Truncate(error, 2048);
        row.RequestHeadersJson = JsonSerializer.Serialize(snapshot.RequestHeaders, JsonOptions);
        row.RequestBody = snapshot.RequestBody;
        row.ResponseHeadersJson = JsonSerializer.Serialize(snapshot.ResponseHeaders, JsonOptions);
        row.ResponseBody = snapshot.ResponseBody;
        row.ResponseContentType = snapshot.ContentType;
        row.ResponseContentLength = snapshot.ResponseSizeBytes;
        row.FinalUrl = snapshot.FinalUrl;

        await db.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    private async Task RetryOrFailAsync(HttpRequestQueueItem item, string error, CancellationToken ct)
    {
        var terminal = item.AttemptCount >= item.MaxAttempts;
        if (terminal)
        {
            await MarkFailedAsync(item.Id, error, terminal: true, ct).ConfigureAwait(false);

            using var scope = scopeFactory.CreateScope();
            var persistence = scope.ServiceProvider.GetRequiredService<IAssetPersistence>();
            var snapshot = new UrlFetchSnapshot(
                item.Method,
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                null,
                0,
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                error,
                null,
                0,
                "text/plain",
                DateTimeOffset.UtcNow,
                item.RequestUrl);
            await persistence.ConfirmUrlAssetAsync(item.AssetId, snapshot, Guid.Empty, ct).ConfigureAwait(false);
            return;
        }

        var delay = TimeSpan.FromSeconds(Math.Min(300, Math.Pow(2, Math.Max(0, item.AttemptCount)) * 5));
        await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
        await db.HttpRequestQueue
            .Where(q => q.Id == item.Id)
            .ExecuteUpdateAsync(
                s => s
                    .SetProperty(q => q.State, HttpRequestQueueState.Retry)
                    .SetProperty(q => q.UpdatedAtUtc, DateTimeOffset.UtcNow)
                    .SetProperty(q => q.NextAttemptAtUtc, DateTimeOffset.UtcNow + delay)
                    .SetProperty(q => q.LockedBy, (string?)null)
                    .SetProperty(q => q.LockedUntilUtc, (DateTimeOffset?)null)
                    .SetProperty(q => q.LastError, Truncate(error, 2048)),
                ct)
            .ConfigureAwait(false);
    }

    private async Task MarkFailedAsync(Guid queueItemId, string error, bool terminal, CancellationToken ct)
    {
        await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
        await db.HttpRequestQueue
            .Where(q => q.Id == queueItemId)
            .ExecuteUpdateAsync(
                s => s
                    .SetProperty(q => q.State, terminal ? HttpRequestQueueState.Failed : HttpRequestQueueState.Retry)
                    .SetProperty(q => q.UpdatedAtUtc, DateTimeOffset.UtcNow)
                    .SetProperty(q => q.CompletedAtUtc, terminal ? DateTimeOffset.UtcNow : (DateTimeOffset?)null)
                    .SetProperty(q => q.LockedBy, (string?)null)
                    .SetProperty(q => q.LockedUntilUtc, (DateTimeOffset?)null)
                    .SetProperty(q => q.LastError, Truncate(error, 2048)),
                ct)
            .ConfigureAwait(false);
    }

    private async Task HarvestLinksAsync(StoredAsset asset, UrlFetchSnapshot snapshot, CancellationToken ct)
    {
        var body = snapshot.ResponseBody ?? "";
        var contentType = snapshot.ContentType ?? "";
        var baseUrl = snapshot.FinalUrl ?? asset.RawValue;
        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var baseUri))
            return;

        var parentPage = baseUri.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped);
        var spiderContext = TruncateDiscoveryContext($"Spider: link extracted from fetched page {parentPage}");
        var correlation = NewId.NextGuid();
        foreach (var link in LinkHarvest.Extract(body, contentType, baseUri).Take(MaxLinksPerAsset))
        {
            var kind = LinkHarvest.GuessKindForUrl(link);
            await outbox.EnqueueAsync(
                    new AssetDiscovered(
                        asset.TargetId,
                        asset.Target?.RootDomain ?? "",
                        asset.Target?.GlobalMaxDepth ?? asset.Depth + 10,
                        asset.Depth + 1,
                        kind,
                        link,
                        "spider-worker",
                        DateTimeOffset.UtcNow,
                        correlation,
                        AssetAdmissionStage.Raw,
                        null,
                        spiderContext,
                        EventId: NewId.NextGuid(),
                        CausationId: correlation,
                        Producer: "worker-spider"),
                    ct)
                .ConfigureAwait(false);
        }
    }

    private static bool ShouldQueueRetry(HttpStatusCode statusCode) =>
        statusCode is HttpStatusCode.RequestTimeout
            or HttpStatusCode.TooManyRequests
            or HttpStatusCode.InternalServerError
            or HttpStatusCode.BadGateway
            or HttpStatusCode.ServiceUnavailable
            or HttpStatusCode.GatewayTimeout;

    private static bool IsHttpTransient(Exception ex) =>
        ex is HttpRequestException or IOException or TaskCanceledException or OperationCanceledException;

    private static Dictionary<string, string> HeadersToDict(HttpHeaders headers)
    {
        var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var h in headers)
            d[h.Key] = string.Join(", ", h.Value);
        return d;
    }

    private static string Truncate(string s, int maxChars) =>
        s.Length <= maxChars ? s : s[..maxChars];

    private static string TruncateDiscoveryContext(string s, int maxChars = 512) =>
        s.Length <= maxChars ? s : s[..(maxChars - 1)] + "…";
}
