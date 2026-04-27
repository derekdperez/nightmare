using Microsoft.EntityFrameworkCore;
using NightmareV2.Application.Workers;
using NightmareV2.CommandCenter.Models;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.CommandCenter;

internal static class WorkerActivityQuery
{
    private static readonly TimeSpan Lookback = TimeSpan.FromHours(24);
    private static readonly TimeSpan HotWindow = TimeSpan.FromSeconds(25);
    private static readonly TimeSpan RecentWindow = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan IdleWindow = TimeSpan.FromHours(1);

    public static async Task<WorkerActivitySnapshotDto> BuildSnapshotAsync(NightmareDbContext db, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var since = now - Lookback;

        var rows = await db.BusJournal.AsNoTracking()
            .Where(e => e.Direction == "Consume" && e.ConsumerType != null && e.OccurredAtUtc >= since)
            .OrderByDescending(e => e.Id)
            .Take(15_000)
            .Select(e => new { e.HostName, e.ConsumerType, e.MessageType, e.PayloadJson, e.OccurredAtUtc })
            .ToListAsync(ct)
            .ConfigureAwait(false);

        var latestByKey = new Dictionary<(string Host, string Consumer), (string MessageType, string Payload, DateTimeOffset At)>();
        foreach (var r in rows)
        {
            var host = string.IsNullOrWhiteSpace(r.HostName) ? "(no host)" : r.HostName.Trim();
            var key = (host, r.ConsumerType!);
            if (latestByKey.ContainsKey(key))
                continue;
            latestByKey[key] = (r.MessageType, r.PayloadJson, r.OccurredAtUtc);
        }

        var toggles = await db.WorkerSwitches.AsNoTracking()
            .ToDictionaryAsync(w => w.WorkerKey, w => w.IsEnabled, ct)
            .ConfigureAwait(false);

        var instances = new List<WorkerInstanceActivityDto>(latestByKey.Count);
        foreach (var kv in latestByKey)
        {
            var (host, consumer) = kv.Key;
            var data = kv.Value;
            var kind = WorkerConsumerKindResolver.KindFromConsumerType(consumer) ?? "Other";
            bool? toggle = kind == "Other"
                ? null
                : (toggles.TryGetValue(kind, out var en) ? en : true);
            instances.Add(
                new WorkerInstanceActivityDto(
                    host,
                    kind,
                    ShortConsumerName(consumer),
                    toggle,
                    data.At,
                    data.MessageType,
                    TruncatePreview(data.Payload),
                    ActivityLabel(data.At, now)));
        }

        instances.Sort(CompareInstances);

        var summaries = new List<WorkerKindSummaryDto>();
        foreach (var key in toggles.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            var matching = instances.Where(i => i.WorkerKind == key).ToList();
            var last = matching.Count == 0 ? (DateTimeOffset?)null : matching.Max(i => i.LastCompletedAtUtc);
            var label = last is { } t ? ActivityLabel(t, now) : "No journal data (24h)";
            summaries.Add(
                new WorkerKindSummaryDto(
                    key,
                    toggles[key],
                    matching.Count,
                    last,
                    label));
        }

        var orphanKinds = instances
            .Select(i => i.WorkerKind)
            .Where(k => k != "Other" && !toggles.ContainsKey(k))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(k => k, StringComparer.Ordinal);
        foreach (var k in orphanKinds)
        {
            var matching = instances.Where(i => i.WorkerKind == k).ToList();
            var last = matching.Max(i => i.LastCompletedAtUtc);
            summaries.Add(
                new WorkerKindSummaryDto(k, true, matching.Count, last, ActivityLabel(last, now)));
        }

        if (instances.Any(i => i.WorkerKind == "Other"))
        {
            var o = instances.Where(i => i.WorkerKind == "Other").ToList();
            var last = o.Max(i => i.LastCompletedAtUtc);
            summaries.Add(new WorkerKindSummaryDto("Other", true, o.Count, last, ActivityLabel(last, now)));
        }

        return new WorkerActivitySnapshotDto(summaries, instances);
    }

    private static int CompareInstances(WorkerInstanceActivityDto a, WorkerInstanceActivityDto b)
    {
        var c = string.Compare(a.WorkerKind, b.WorkerKind, StringComparison.Ordinal);
        if (c != 0)
            return c;
        c = string.Compare(a.HostName, b.HostName, StringComparison.Ordinal);
        if (c != 0)
            return c;
        return string.Compare(a.ConsumerShortName, b.ConsumerShortName, StringComparison.Ordinal);
    }

    private static string ActivityLabel(DateTimeOffset lastCompleted, DateTimeOffset now)
    {
        var ago = now - lastCompleted;
        if (ago <= HotWindow)
            return "Hot (just finished a message)";
        if (ago <= RecentWindow)
            return "Recently active";
        if (ago <= IdleWindow)
            return "Idle";
        return "Stale / likely offline";
    }

    private static string ShortConsumerName(string fullName)
    {
        var i = fullName.LastIndexOf('.');
        return i >= 0 && i < fullName.Length - 1 ? fullName[(i + 1)..] : fullName;
    }

    private static string TruncatePreview(string json)
    {
        var s = json.ReplaceLineEndings(" ").Trim();
        const int max = 140;
        return s.Length <= max ? s : s[..max] + "…";
    }
}
