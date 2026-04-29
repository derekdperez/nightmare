using NightmareV2.Contracts.Events;

namespace NightmareV2.Application.Events;

public interface IEventOutbox
{
    Task EnqueueAsync<TEvent>(TEvent message, CancellationToken cancellationToken = default)
        where TEvent : class, IEventEnvelope;
}
