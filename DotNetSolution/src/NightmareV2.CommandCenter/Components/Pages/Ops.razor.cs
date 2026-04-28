using System;
using System.Linq;
using Microsoft.AspNetCore.Components.QuickGrid;
using NightmareV2.CommandCenter.Components.DataGrid;
using NightmareV2.CommandCenter.Models;

namespace NightmareV2.CommandCenter.Components.Pages;

public partial class Ops
{
    private static readonly GridSort<AssetGridRowDto> SortAssetDiscoveryContext =
        GridSort<AssetGridRowDto>.ByAscending(static a => a.DiscoveryContext);

    private IQueryable<AssetGridRowDto> RequestQueueRows =>
        _assets.AsQueryable()
            .Where(a => string.Equals(a.LifecycleStatus, "Queued", StringComparison.Ordinal))
            .OrderByDescending(a => a.DiscoveredAtUtc);

    private IQueryable<AssetGridRowDto> FilteredAssets =>
        _assets.AsQueryable().Where(a =>
            (GridTextFilter.Matches(a.Kind, _filterAssets)
             || GridTextFilter.Matches(a.LifecycleStatus, _filterAssets)
             || GridTextFilter.Matches(a.RawValue, _filterAssets)
             || GridTextFilter.Matches(a.DiscoveredBy, _filterAssets)
             || GridTextFilter.Matches(a.DiscoveryContext, _filterAssets)
             || GridTextFilter.Matches(a.CanonicalKey, _filterAssets))
            && GridTextFilter.Matches(a.Kind, _filterKindCol)
            && GridTextFilter.Matches(a.LifecycleStatus, _filterStatusCol)
            && GridTextFilter.Matches(a.RawValue, _filterRawCol)
            && GridTextFilter.Matches(a.DiscoveredBy, _filterPipelineCol)
            && GridTextFilter.Matches(a.DiscoveryContext, _filterHowFoundCol));

    private IQueryable<AssetGridRowDto> FilteredRequestQueue =>
        RequestQueueRows.Where(a =>
            (GridTextFilter.Matches(a.Kind, _filterQueueSearch)
             || GridTextFilter.Matches(a.RawValue, _filterQueueSearch)
             || GridTextFilter.Matches(a.DiscoveredBy, _filterQueueSearch)
             || GridTextFilter.Matches(a.DiscoveredAtUtc.ToString("u"), _filterQueueSearch))
            && GridTextFilter.Matches(a.Kind, _filterQueueKindCol)
            && GridTextFilter.Matches(a.RawValue, _filterQueueRawCol)
            && GridTextFilter.Matches(a.DiscoveredBy, _filterQueuePipelineCol));

    private static bool IsUrlOrSubdomain(string kind) =>
        string.Equals(kind, "Url", StringComparison.OrdinalIgnoreCase)
        || string.Equals(kind, "Subdomain", StringComparison.OrdinalIgnoreCase);

    private static string? ToLiveHref(AssetGridRowDto row)
    {
        var raw = row.RawValue?.Trim();
        if (string.IsNullOrWhiteSpace(raw) || !IsUrlOrSubdomain(row.Kind))
            return null;

        if (Uri.TryCreate(raw, UriKind.Absolute, out var absolute) && absolute.Scheme is "http" or "https")
            return absolute.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped);

        if (string.Equals(row.Kind, "Subdomain", StringComparison.OrdinalIgnoreCase))
        {
            var host = raw.Trim().TrimEnd('/');
            if (host.Length == 0)
                return null;
            return $"https://{host}";
        }

        return null;
    }
}
