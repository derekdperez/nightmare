namespace NightmareV2.Application.Assets;

/// <summary>Captured HTTP exchange when a URL-class asset is confirmed.</summary>
public sealed record UrlFetchSnapshot(
    string RequestMethod,
    Dictionary<string, string> RequestHeaders,
    string? RequestBody,
    int StatusCode,
    Dictionary<string, string> ResponseHeaders,
    string? ResponseBody,
    long? ResponseSizeBytes,
    double DurationMs,
    string? ContentType,
    DateTimeOffset CompletedAtUtc);
