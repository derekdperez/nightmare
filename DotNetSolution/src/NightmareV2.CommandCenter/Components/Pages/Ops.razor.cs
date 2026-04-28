using System.Globalization;
using System.Linq;
using Microsoft.AspNetCore.Components.QuickGrid;
using NightmareV2.CommandCenter.Models;

namespace NightmareV2.CommandCenter.Components.Pages;

public partial class Ops
{
    private static readonly GridSort<AssetCountByDomainDto> SortTopDomainByRoot =
        GridSort<AssetCountByDomainDto>.ByAscending(static r => r.RootDomain);

    private static readonly GridSort<DiscoveredByCountDto> SortDiscoveredByPipeline =
        GridSort<DiscoveredByCountDto>.ByAscending(static r => r.DiscoveredBy);

    private static readonly GridSort<WorkerDetailStatsDto> SortWorkerBus1h =
        GridSort<WorkerDetailStatsDto>.ByDescending(static m => m.BusConsumesLast1Hour);

    private static readonly GridSort<WorkerDetailStatsDto> SortWorkerLastConsume =
        GridSort<WorkerDetailStatsDto>.ByDescending(static m => m.LastConsumeUtc);

    private static readonly GridSort<WorkerDetailStatsDto> SortWorkerDbAssets1h =
        GridSort<WorkerDetailStatsDto>.ByDescending(static m => m.DbAssetsAttributedLast1Hour);

    private static readonly GridSort<WorkerDetailStatsDto> SortWorkerRabbitReady =
        GridSort<WorkerDetailStatsDto>.ByDescending(static m => m.RabbitMessagesReady);

    private static readonly GridSort<RabbitQueueBriefDto> SortRabbitQueueName =
        GridSort<RabbitQueueBriefDto>.ByAscending(static q => q.Name);

    private static readonly GridSort<WorkerKindSummaryDto> SortWorkerSummaryToggle =
        GridSort<WorkerKindSummaryDto>.ByAscending(static s => s.ToggleEnabled);

    private static readonly GridSort<WorkerKindSummaryDto> SortWorkerSummaryLastActivity =
        GridSort<WorkerKindSummaryDto>.ByDescending(static s => s.LastActivityUtc);

    private static readonly GridSort<WorkerInstanceActivityDto> SortInstanceToggle =
        GridSort<WorkerInstanceActivityDto>.ByAscending(static i => i.ToggleEnabled);

    private static readonly GridSort<WorkerInstanceActivityDto> SortInstancePayload =
        GridSort<WorkerInstanceActivityDto>.ByAscending(static i => i.LastPayloadPreview);

    private static readonly GridSort<WorkerSwitchDto> SortWorkerSwitchEnabled =
        GridSort<WorkerSwitchDto>.ByAscending(static w => w.IsEnabled);

    private static readonly GridSort<AssetGridRowDto> SortAssetDiscoveryContext =
        GridSort<AssetGridRowDto>.ByAscending(static a => a.DiscoveryContext);

    private static string InstanceRowKey(WorkerInstanceActivityDto i) =>
        string.Concat(
            i.HostName,
            "|",
            i.ConsumerShortName,
            "|",
            i.LastCompletedAtUtc.Ticks.ToString(CultureInfo.InvariantCulture),
            "|",
            i.LastMessageType);

    private sealed record TrafficRow(string Label, long LastHour, long Last24Hours);

    private string _filterTopDomains = "";
    private string _filterDiscoveredBy = "";
    private string _filterWorkerMetrics = "";
    private string _filterRabbitQueues = "";
    private string _filterWorkerSummaries = "";
    private string _filterWorkerInstances = "";
    private string _filterWorkers = "";
    private string _filterAssets = "";
    private string _filterLiveBus = "";
    private string _filterHistoryBus = "";

    private IQueryable<TrafficRow> BusTrafficRows =>
        _snapshot is null
            ? Enumerable.Empty<TrafficRow>().AsQueryable()
            : new[]
            {
                new TrafficRow("Publishes", _snapshot.BusTraffic.PublishesLast1Hour, _snapshot.BusTraffic.PublishesLast24Hours),
                new TrafficRow("Consumes", _snapshot.BusTraffic.ConsumesLast1Hour, _snapshot.BusTraffic.ConsumesLast24Hours),
            }.AsQueryable();

    private IQueryable<AssetCountByDomainDto> FilteredTopDomains =>
        _snapshot is null
            ? Enumerable.Empty<AssetCountByDomainDto>().AsQueryable()
            : _snapshot.Assets.TopDomainsByAssetCount.AsQueryable().Where(r => Matches(r.RootDomain, _filterTopDomains));

    private IQueryable<DiscoveredByCountDto> FilteredDiscoveredBy =>
        _snapshot is null
            ? Enumerable.Empty<DiscoveredByCountDto>().AsQueryable()
            : _snapshot.Assets.AssetsByDiscoveredBy.AsQueryable().Where(r => Matches(r.DiscoveredBy, _filterDiscoveredBy));

    private IQueryable<WorkerDetailStatsDto> FilteredWorkerMetrics =>
        _snapshot is null
            ? Enumerable.Empty<WorkerDetailStatsDto>().AsQueryable()
            : _snapshot.WorkerMetrics.AsQueryable().Where(m =>
                Matches(m.WorkerKey, _filterWorkerMetrics)
                || (m.LastConsumeUtc != null
                    && Matches(m.LastConsumeUtc.GetValueOrDefault().UtcDateTime.ToString("O", CultureInfo.InvariantCulture), _filterWorkerMetrics))
                || Matches(string.Join(' ', m.MatchedRabbitQueueNames), _filterWorkerMetrics));

    private IQueryable<RabbitQueueBriefDto> FilteredRabbitQueues =>
        _snapshot is null
            ? Enumerable.Empty<RabbitQueueBriefDto>().AsQueryable()
            : _snapshot.RabbitQueues.AsQueryable().Where(q =>
                Matches(q.Name, _filterRabbitQueues)
                || Matches(q.LikelyWorkerKey, _filterRabbitQueues));

    private IQueryable<WorkerKindSummaryDto> FilteredWorkerSummaries =>
        _snapshot is null
            ? Enumerable.Empty<WorkerKindSummaryDto>().AsQueryable()
            : _snapshot.WorkerActivity.Summaries.AsQueryable().Where(s =>
                Matches(s.WorkerKey, _filterWorkerSummaries)
                || Matches(s.RollupActivityLabel, _filterWorkerSummaries));

    private IQueryable<WorkerInstanceActivityDto> FilteredWorkerInstances =>
        _snapshot is null
            ? Enumerable.Empty<WorkerInstanceActivityDto>().AsQueryable()
            : _snapshot.WorkerActivity.Instances.AsQueryable().Where(i =>
                Matches(i.HostName, _filterWorkerInstances)
                || Matches(i.WorkerKind, _filterWorkerInstances)
                || Matches(i.ConsumerShortName, _filterWorkerInstances)
                || Matches(i.ActivityLabel, _filterWorkerInstances)
                || Matches(i.LastMessageType, _filterWorkerInstances)
                || Matches(i.LastPayloadPreview, _filterWorkerInstances));

    private IQueryable<WorkerSwitchDto> FilteredWorkers =>
        _snapshot is null
            ? Enumerable.Empty<WorkerSwitchDto>().AsQueryable()
            : _snapshot.Workers.AsQueryable().Where(w => Matches(w.WorkerKey, _filterWorkers));

    private IQueryable<AssetGridRowDto> FilteredAssets =>
        _assets.AsQueryable().Where(a =>
            Matches(a.Kind, _filterAssets)
            || Matches(a.LifecycleStatus, _filterAssets)
            || Matches(a.RawValue, _filterAssets)
            || Matches(a.DiscoveredBy, _filterAssets)
            || Matches(a.DiscoveryContext, _filterAssets)
            || Matches(a.CanonicalKey, _filterAssets));

    private IQueryable<BusJournalRowDto> FilteredLiveBus =>
        _liveBus.AsQueryable().Where(e =>
            Matches(e.MessageType, _filterLiveBus)
            || Matches(e.HostName, _filterLiveBus)
            || Matches(e.PayloadJson, _filterLiveBus));

    private IQueryable<BusJournalRowDto> FilteredHistoryBus =>
        _historyBus.AsQueryable().Where(e =>
            Matches(e.Direction, _filterHistoryBus)
            || Matches(e.MessageType, _filterHistoryBus)
            || Matches(e.ConsumerType, _filterHistoryBus)
            || Matches(e.HostName, _filterHistoryBus)
            || Matches(e.PayloadJson, _filterHistoryBus)
            || e.Id.ToString(CultureInfo.InvariantCulture).Contains(_filterHistoryBus, StringComparison.OrdinalIgnoreCase));

    private static bool Matches(string? value, string filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
            return true;
        return value?.Contains(filter, StringComparison.OrdinalIgnoreCase) ?? false;
    }
}
