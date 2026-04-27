using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Infrastructure.Gatekeeping;

public sealed class DefaultAssetCanonicalizer : IAssetCanonicalizer
{
    public CanonicalAsset Canonicalize(AssetDiscovered message)
    {
        var raw = message.RawValue.Trim();
        return message.Kind switch
        {
            AssetKind.Url or AssetKind.ApiEndpoint or AssetKind.JavaScriptFile or AssetKind.MarkdownBody => CanonicalizeUrl(raw, message.Kind),
            AssetKind.Subdomain or AssetKind.Domain => CanonicalizeHost(raw, message.Kind),
            AssetKind.IpAddress => new CanonicalAsset(AssetKind.IpAddress, raw.ToLowerInvariant(), raw),
            _ => new CanonicalAsset(message.Kind, StableHash(raw), raw),
        };
    }

    private static CanonicalAsset CanonicalizeHost(string host, AssetKind kind)
    {
        var h = host.Trim().TrimEnd('.').ToLowerInvariant();
        var idn = new IdnMapping();
        try
        {
            h = idn.GetAscii(h);
        }
        catch (ArgumentException)
        {
            // keep unicode form if punycode fails
        }

        return new CanonicalAsset(kind, $"host:{h}", h);
    }

    private static CanonicalAsset CanonicalizeUrl(string raw, AssetKind kind)
    {
        if (!Uri.TryCreate(raw, UriKind.Absolute, out var uri))
        {
            if (!Uri.TryCreate("https://" + raw.TrimStart('/'), UriKind.Absolute, out uri))
                return new CanonicalAsset(kind, StableHash(raw), raw);
        }

        var builder = new UriBuilder(uri) { Fragment = null };
        if (string.IsNullOrEmpty(builder.Path))
            builder.Path = "/";

        var normalized = builder.Uri.ToString();
        var key = $"url:{normalized.ToLowerInvariant()}";
        return new CanonicalAsset(kind, key, normalized);
    }

    private static string StableHash(string value)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(bytes, 0, 16);
    }
}
