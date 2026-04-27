using System.Text.RegularExpressions;
using AngleSharp.Html.Dom;
using AngleSharp.Html.Parser;
using Markdig;
using NightmareV2.Contracts;
using Markdig.Syntax;
using Markdig.Syntax.Inlines;

namespace NightmareV2.Workers.Spider;

internal static class LinkHarvest
{
    private static readonly Regex UrlInText = new(
        @"https?://[^\s""'<>()]+",
        RegexOptions.Compiled | RegexOptions.IgnoreCase,
        TimeSpan.FromSeconds(2));

    private static readonly Regex SrcHref = new(
        @"(?:src|href)\s*=\s*[""']([^""']+)[""']",
        RegexOptions.Compiled | RegexOptions.IgnoreCase,
        TimeSpan.FromSeconds(2));

    public static IEnumerable<string> Extract(string text, string contentType, Uri baseUri)
    {
        var ct = contentType.ToLowerInvariant();
        if (ct.Contains("html", StringComparison.Ordinal) || LooksLikeHtml(text))
            return ExtractFromHtml(text, baseUri);

        if (ct.Contains("markdown", StringComparison.Ordinal) || baseUri.AbsolutePath.EndsWith(".md", StringComparison.OrdinalIgnoreCase))
            return ExtractFromMarkdown(text, baseUri);

        if (ct.Contains("javascript", StringComparison.Ordinal) || baseUri.AbsolutePath.EndsWith(".js", StringComparison.OrdinalIgnoreCase))
            return ExtractFromScript(text, baseUri);

        return ExtractFromPlain(text, baseUri);
    }

    private static bool LooksLikeHtml(string text) =>
        text.Contains('<', StringComparison.Ordinal) && text.Contains('>', StringComparison.Ordinal);

    private static IEnumerable<string> ExtractFromHtml(string html, Uri baseUri)
    {
        var parser = new HtmlParser();
        var doc = parser.ParseDocument(html);
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var el in doc.QuerySelectorAll(
                     "a[href], link[href], area[href], script[src], img[src], iframe[src], embed[src], object[data], source[src], video[src], audio[src], form[action]"))
        {
            var href = el.GetAttribute("href") ?? el.GetAttribute("src") ?? el.GetAttribute("data") ?? el.GetAttribute("action");
            if (string.IsNullOrWhiteSpace(href))
                continue;
            if (TryResolve(baseUri, href, out var abs))
                set.Add(abs);
        }

        foreach (var el in doc.QuerySelectorAll("[srcset]"))
        {
            var srcset = el.GetAttribute("srcset");
            if (string.IsNullOrWhiteSpace(srcset))
                continue;
            foreach (var part in srcset.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var url = part.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries)[0];
                if (TryResolve(baseUri, url, out var abs))
                    set.Add(abs);
            }
        }

        if (doc is IHtmlDocument htmlDoc)
        {
            foreach (var script in htmlDoc.Scripts)
            {
                if (!string.IsNullOrEmpty(script.Source) && TryResolve(baseUri, script.Source, out var srcAbs))
                    set.Add(srcAbs);
                else if (!string.IsNullOrEmpty(script.Text))
                {
                    foreach (var m in UrlInText.Matches(script.Text).Cast<Match>())
                    {
                        if (TryResolve(baseUri, m.Value, out var abs))
                            set.Add(abs);
                    }
                }
            }
        }

        foreach (var m in UrlInText.Matches(html).Cast<Match>())
        {
            if (TryResolve(baseUri, m.Value, out var abs))
                set.Add(abs);
        }

        return set;
    }

    private static IEnumerable<string> ExtractFromMarkdown(string markdown, Uri baseUri)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var doc = Markdown.Parse(markdown);
        foreach (var inline in doc.Descendants().OfType<LinkInline>())
        {
            if (!string.IsNullOrWhiteSpace(inline.Url) && TryResolve(baseUri, inline.Url, out var abs))
                set.Add(abs);
        }

        foreach (var inline in doc.Descendants().OfType<AutolinkInline>())
        {
            if (TryResolve(baseUri, inline.Url, out var abs))
                set.Add(abs);
        }

        foreach (var m in UrlInText.Matches(markdown).Cast<Match>())
        {
            if (TryResolve(baseUri, m.Value, out var abs))
                set.Add(abs);
        }

        return set;
    }

    private static IEnumerable<string> ExtractFromScript(string script, Uri baseUri)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var m in UrlInText.Matches(script).Cast<Match>())
        {
            if (TryResolve(baseUri, m.Value, out var abs))
                set.Add(abs);
        }

        foreach (var m in SrcHref.Matches(script).Cast<Match>())
        {
            if (TryResolve(baseUri, m.Groups[1].Value, out var abs))
                set.Add(abs);
        }

        return set;
    }

    private static IEnumerable<string> ExtractFromPlain(string text, Uri baseUri)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var m in UrlInText.Matches(text).Cast<Match>())
        {
            if (TryResolve(baseUri, m.Value, out var abs))
                set.Add(abs);
        }

        return set;
    }

    private static bool TryResolve(Uri baseUri, string raw, out string absolute)
    {
        absolute = "";
        var trimmed = raw.Trim();
        if (trimmed.StartsWith("//", StringComparison.Ordinal))
            trimmed = baseUri.Scheme + ":" + trimmed;
        if (Uri.TryCreate(trimmed, UriKind.Absolute, out var uri))
        {
            absolute = uri.ToString();
            return true;
        }

        if (Uri.TryCreate(baseUri, trimmed, out uri))
        {
            absolute = uri.ToString();
            return true;
        }

        return false;
    }

    public static AssetKind GuessKindForUrl(string url)
    {
        if (url.EndsWith(".md", StringComparison.OrdinalIgnoreCase))
            return AssetKind.MarkdownBody;
        if (url.EndsWith(".js", StringComparison.OrdinalIgnoreCase) || url.Contains("/js/", StringComparison.OrdinalIgnoreCase))
            return AssetKind.JavaScriptFile;
        return AssetKind.Url;
    }
}
