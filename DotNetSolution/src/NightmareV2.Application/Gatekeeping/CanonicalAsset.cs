using NightmareV2.Contracts;

namespace NightmareV2.Application.Gatekeeping;

public sealed record CanonicalAsset(AssetKind Kind, string CanonicalKey, string NormalizedDisplay);
