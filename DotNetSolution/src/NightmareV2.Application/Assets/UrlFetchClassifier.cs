using System.Text.Json;
using System.Text.RegularExpressions;

namespace NightmareV2.Application.Assets;

public static class UrlFetchClassifier
{
    private static readonly Regex Structured404FieldRegex = new(
        @"""(?:error_code|error|status_code|status|code|http_status|statusCode)""\s*:\s*""?404""?",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex NotFoundErrorFieldRegex = new(
        @"""(?:error_type|type|error_message|errorMessage|message|detail|title)""\s*:\s*""[^""]*(?:route[_\s-]*not[_\s-]*found|not[_\s-]*found|404)[^""]*""",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static bool LooksLikeSoft404(UrlFetchSnapshot snapshot)
    {
        var body = snapshot.ResponseBody;
        if (string.IsNullOrWhiteSpace(body))
            return false;

        // Fast path for common soft-404 JSON shapes, including:
        // {"error_code":404,...}, {"error_code":"404",...}, {"error":404,...}
        if (Structured404FieldRegex.IsMatch(body) || NotFoundErrorFieldRegex.IsMatch(body))
            return true;

        if (!LooksLikeJson(snapshot.ContentType, body))
            return false;

        try
        {
            using var doc = JsonDocument.Parse(body);
            return JsonElementContains404Signal(doc.RootElement);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool LooksLikeJson(string? contentType, string body)
    {
        if (contentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
            return true;

        var trimmed = body.AsSpan().TrimStart();
        return trimmed.Length > 0 && (trimmed[0] == '{' || trimmed[0] == '[');
    }

    private static bool JsonElementContains404Signal(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    if (Is404ErrorProperty(property))
                        return true;

                    if (JsonElementContains404Signal(property.Value))
                        return true;
                }

                return false;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    if (JsonElementContains404Signal(item))
                        return true;
                }

                return false;

            default:
                return false;
        }
    }

    private static bool Is404ErrorProperty(JsonProperty property)
    {
        var name = property.Name;
        if (IsErrorStatusField(name) && Is404Value(property.Value))
            return true;

        if (IsErrorTextField(name) && ContainsNotFoundSignal(property.Value))
            return true;

        return false;
    }

    private static bool IsErrorStatusField(string name) =>
        name.Equals("error_code", StringComparison.OrdinalIgnoreCase)
        || name.Equals("error", StringComparison.OrdinalIgnoreCase)
        || name.Equals("status_code", StringComparison.OrdinalIgnoreCase)
        || name.Equals("status", StringComparison.OrdinalIgnoreCase)
        || name.Equals("code", StringComparison.OrdinalIgnoreCase)
        || name.Equals("http_status", StringComparison.OrdinalIgnoreCase)
        || name.Equals("statusCode", StringComparison.OrdinalIgnoreCase);

    private static bool IsErrorTextField(string name) =>
        name.Equals("error_type", StringComparison.OrdinalIgnoreCase)
        || name.Equals("type", StringComparison.OrdinalIgnoreCase)
        || name.Equals("error_message", StringComparison.OrdinalIgnoreCase)
        || name.Equals("errorMessage", StringComparison.OrdinalIgnoreCase)
        || name.Equals("message", StringComparison.OrdinalIgnoreCase)
        || name.Equals("detail", StringComparison.OrdinalIgnoreCase)
        || name.Equals("title", StringComparison.OrdinalIgnoreCase);

    private static bool Is404Value(JsonElement value) =>
        value.ValueKind switch
        {
            JsonValueKind.Number => value.TryGetInt32(out var code) && code == 404,
            JsonValueKind.String => string.Equals(value.GetString()?.Trim(), "404", StringComparison.OrdinalIgnoreCase),
            _ => false
        };

    private static bool ContainsNotFoundSignal(JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.String)
            return false;

        var s = value.GetString();
        if (string.IsNullOrWhiteSpace(s))
            return false;

        return s.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || s.Contains("not_found", StringComparison.OrdinalIgnoreCase)
            || s.Contains("route not found", StringComparison.OrdinalIgnoreCase)
            || string.Equals(s.Trim(), "404", StringComparison.OrdinalIgnoreCase);
    }
}
