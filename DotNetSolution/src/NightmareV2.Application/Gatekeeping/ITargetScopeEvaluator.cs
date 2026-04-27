using NightmareV2.Contracts.Events;

namespace NightmareV2.Application.Gatekeeping;

public interface ITargetScopeEvaluator
{
    bool IsInScope(AssetDiscovered message, CanonicalAsset canonical);
}
