using Microsoft.EntityFrameworkCore;
using NightmareV2.Application.Events;
using NightmareV2.Contracts.Events;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure.Data;
using Npgsql;

namespace NightmareV2.Infrastructure.Messaging;

public sealed class EfInboxDeduplicator(NightmareDbContext db, ILogger<EfInboxDeduplicator> logger) : IInboxDeduplicator
{
    public async Task<bool> TryBeginProcessingAsync(
        IEventEnvelope envelope,
        string consumer,
        CancellationToken cancellationToken = default)
    {
        if (envelope.EventId == Guid.Empty)
            return true;

        db.InboxMessages.Add(
            new InboxMessage
            {
                Id = Guid.NewGuid(),
                EventId = envelope.EventId,
                Consumer = consumer,
                ProcessedAtUtc = DateTimeOffset.UtcNow,
            });

        try
        {
            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException pg
            && pg.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            logger.LogDebug(
                "Skipping duplicate inbox event {EventId} for consumer {Consumer}.",
                envelope.EventId,
                consumer);
            return false;
        }
    }
}
