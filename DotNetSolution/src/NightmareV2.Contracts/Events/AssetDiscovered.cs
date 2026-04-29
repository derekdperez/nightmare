namespace NightmareV2.Contracts.Events;

/// <summary>
/// Candidate or indexed asset on the bus. Raw admissions are processed by Gatekeeper; Indexed admissions are consumed by workers (spider, etc.).
/// </summary>
public record AssetDiscovered(
    Guid TargetId,
    string TargetRootDomain,
    int GlobalMaxDepth,
    int Depth,
    AssetKind Kind,
    string RawValue,
    string DiscoveredBy,
    DateTimeOffset OccurredAt,
    Guid CorrelationId,
    AssetAdmissionStage AdmissionStage,
    /// <summary>Set when <see cref="AdmissionStage"/> is <see cref="AssetAdmissionStage.Indexed"/>.</summary>
    Guid? AssetId,
    /// <summary>Human-readable provenance (parent URL, wordlist, etc.). Kept short for DB and bus payloads.</summary>
    string DiscoveryContext = "",
    Guid EventId = default,
    Guid CausationId = default,
    string SchemaVersion = "1",
    string Producer = "nightmare-v2") : IEventEnvelope
{
    public DateTimeOffset OccurredAtUtc => OccurredAt;
}
