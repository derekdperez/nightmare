using System.Text.Json;
using NightmareV2.Application.Events;
using NightmareV2.Contracts.Events;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.Infrastructure.Messaging;

public sealed class EfEventOutbox(NightmareDbContext db) : IEventOutbox
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    public async Task EnqueueAsync<TEvent>(TEvent message, CancellationToken cancellationToken = default)
        where TEvent : class, IEventEnvelope
    {
        var resolvedEventId = message.EventId == Guid.Empty ? Guid.NewGuid() : message.EventId;
        var resolvedCorrelation = message.CorrelationId == Guid.Empty ? Guid.NewGuid() : message.CorrelationId;
        var resolvedCausation = message.CausationId == Guid.Empty ? resolvedCorrelation : message.CausationId;
        var resolvedOccurredAt = message.OccurredAtUtc == default ? DateTimeOffset.UtcNow : message.OccurredAtUtc;

        db.OutboxMessages.Add(
            new OutboxMessage
            {
                Id = Guid.NewGuid(),
                MessageType = message.GetType().AssemblyQualifiedName ?? message.GetType().FullName ?? message.GetType().Name,
                PayloadJson = JsonSerializer.Serialize(message, message.GetType(), JsonOptions),
                EventId = resolvedEventId,
                CorrelationId = resolvedCorrelation,
                CausationId = resolvedCausation,
                OccurredAtUtc = resolvedOccurredAt,
                Producer = string.IsNullOrWhiteSpace(message.Producer) ? "nightmare-v2" : message.Producer,
                State = OutboxMessageState.Pending,
                AttemptCount = 0,
                CreatedAtUtc = DateTimeOffset.UtcNow,
                UpdatedAtUtc = DateTimeOffset.UtcNow,
                NextAttemptAtUtc = DateTimeOffset.UtcNow,
            });

        await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
    }
}
