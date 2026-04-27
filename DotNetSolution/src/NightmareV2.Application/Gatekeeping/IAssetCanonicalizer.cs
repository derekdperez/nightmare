using NightmareV2.Contracts.Events;

namespace NightmareV2.Application.Gatekeeping;

public interface IAssetCanonicalizer
{
    CanonicalAsset Canonicalize(AssetDiscovered message);
}
