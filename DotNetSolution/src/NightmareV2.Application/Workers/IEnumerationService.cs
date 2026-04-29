using NightmareV2.Contracts.Events;

namespace NightmareV2.Application.Workers;

public interface IEnumerationService
{
    Task<IReadOnlyList<string>> DiscoverSubdomainsAsync(TargetCreated target, CancellationToken cancellationToken = default);
}
