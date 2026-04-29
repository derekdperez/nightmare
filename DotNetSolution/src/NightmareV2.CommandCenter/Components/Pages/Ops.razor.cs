using System;
using System.Globalization;
using System.Linq;
using Microsoft.AspNetCore.Components.QuickGrid;
using NightmareV2.CommandCenter.Components.DataGrid;
using NightmareV2.CommandCenter.Models;

namespace NightmareV2.CommandCenter.Components.Pages;

public partial class Ops
{
    private static readonly GridSort<AssetGridRowDto> SortAssetDiscoveryContext =
        GridSort<AssetGridRowDto>.ByAscending(static a => a.DiscoveryContext);

    private IQueryable<HttpRequestQueueRowDto> RequestQueueRows =>
        _requestQueue.AsQueryable()
            .OrderByDescending(q => q.CreatedAtUtc);

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

    private IQueryable<HttpRequestQueueRowDto> FilteredRequestQueue =>
        RequestQueueRows.Where(q =>
            (GridTextFilter.Matches(q.AssetKind, _filterQueueSearch)
             || GridTextFilter.Matches(q.RequestUrl, _filterQueueSearch)
             || GridTextFilter.Matches(q.DomainKey, _filterQueueSearch)
             || GridTextFilter.Matches(q.State, _filterQueueSearch)
             || GridTextFilter.Matches(
                 q.LastHttpStatus != null
                     ? q.LastHttpStatus.Value.ToString(CultureInfo.InvariantCulture)
                     : string.Empty,
                 _filterQueueSearch)
             || GridTextFilter.Matches(q.LastError, _filterQueueSearch))
            && GridTextFilter.Matches(q.AssetKind, _filterQueueKindCol)
            && GridTextFilter.Matches(q.RequestUrl, _filterQueueRawCol)
            && GridTextFilter.Matches(q.State, _filterQueuePipelineCol));

    private static bool IsUrlOrSubdomain(string kind) =>
        string.Equals(kind, "Url", StringComparison.OrdinalIgnoreCase)
        || string.Equals(kind, "Subdomain", StringComparison.OrdinalIgnoreCase);

    private static string? ToLiveHref(HttpRequestQueueRowDto row)
    {
        var raw = row.FinalUrl ?? row.RequestUrl;
        if (string.IsNullOrWhiteSpace(raw))
            return null;
        if (Uri.TryCreate(raw.Trim(), UriKind.Absolute, out var absolute) && absolute.Scheme is "http" or "https")
            return absolute.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped);
        return null;
    }

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
