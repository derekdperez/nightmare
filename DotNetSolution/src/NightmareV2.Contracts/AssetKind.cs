namespace NightmareV2.Contracts;

/// <summary>
/// Polymorphic asset classification (design §6).
/// </summary>
public enum AssetKind
{
    Domain = 0,
    Subdomain = 1,
    IpAddress = 2,
    CidrBlock = 3,
    Asn = 4,
    Url = 10,
    ApiEndpoint = 11,
    JavaScriptFile = 12,
    Parameter = 13,
    OpenPort = 20,
    TlsCertificate = 21,
    Secret = 30,
    CloudBucket = 31,
    Email = 32,
    /// <summary>Markdown page or .md resource discovered via URL pipeline.</summary>
    MarkdownBody = 33,
}
