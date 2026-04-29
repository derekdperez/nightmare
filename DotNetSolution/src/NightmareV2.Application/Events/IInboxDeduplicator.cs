using NightmareV2.Contracts.Events;

namespace NightmareV2.Application.Events;

public interface IInboxDeduplicator
{
    Task<bool> TryBeginProcessingAsync(
        IEventEnvelope envelope,
        string consumer,
        CancellationToken cancellationToken = default);
}
