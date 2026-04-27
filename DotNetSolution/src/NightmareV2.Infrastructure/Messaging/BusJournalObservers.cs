using System.Text.Json;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.Infrastructure.Messaging;

/// <summary>
/// MassTransit publish/consume observers run highly concurrently. Writing each event to Postgres with a
/// scoped <see cref="NightmareDbContext"/> in parallel caused Npgsql protocol errors (e.g. BindComplete vs
/// ReadyForQuery) and disposed-context faults. All journal writes are serialized on one gate and use a
/// factory-created context per write.
/// </summary>
public sealed class BusJournalPublishObserver(
    IDbContextFactory<NightmareDbContext> dbFactory,
    ILogger<BusJournalPublishObserver> logger) : IPublishObserver
{
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = false };

    public Task PrePublish<T>(PublishContext<T> context)
        where T : class =>
        Task.CompletedTask;

    public Task PostPublish<T>(PublishContext<T> context)
        where T : class =>
        WriteAsync("Publish", typeof(T).Name, Json(context.Message), context.CancellationToken);

    public Task PublishFault<T>(PublishContext<T> context, Exception exception)
        where T : class =>
        Task.CompletedTask;

    private async Task WriteAsync(string direction, string messageType, string payloadJson, CancellationToken ct)
    {
        await BusJournalWriteGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            db.BusJournal.Add(
                new BusJournalEntry
                {
                    Direction = direction,
                    MessageType = messageType,
                    ConsumerType = null,
                    PayloadJson = Truncate(payloadJson, 24_000),
                    OccurredAtUtc = DateTimeOffset.UtcNow,
                    HostName = Environment.MachineName,
                });
            await db.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Bus journal skipped for {Direction} {MessageType}", direction, messageType);
        }
        finally
        {
            BusJournalWriteGate.Release();
        }
    }

    private static string Json<T>(T message) where T : class =>
        JsonSerializer.Serialize(message, message.GetType(), JsonOpts);

    private static string Truncate(string s, int max) =>
        s.Length <= max ? s : s[..max] + "…";
}

public sealed class BusJournalConsumeObserver(
    IDbContextFactory<NightmareDbContext> dbFactory,
    ILogger<BusJournalConsumeObserver> logger) : IConsumeObserver
{
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = false };

    public Task ConsumeFault<T>(ConsumeContext<T> context, Exception exception)
        where T : class =>
        Task.CompletedTask;

    public Task ConsumeFault(ConsumeContext context, Exception exception) => Task.CompletedTask;

    public Task PostConsume<T>(ConsumeContext<T> context)
        where T : class =>
        WriteAsync("Consume", typeof(T).Name, context.Message!, typeof(T).FullName, context.CancellationToken);

    public Task PostConsume(ConsumeContext context) => Task.CompletedTask;

    public Task PreConsume<T>(ConsumeContext<T> context)
        where T : class =>
        Task.CompletedTask;

    public Task PreConsume(ConsumeContext context) => Task.CompletedTask;

    private async Task WriteAsync(string direction, string messageType, object message, string? consumerType, CancellationToken ct)
    {
        await BusJournalWriteGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            db.BusJournal.Add(
                new BusJournalEntry
                {
                    Direction = direction,
                    MessageType = messageType,
                    ConsumerType = consumerType,
                    PayloadJson = Truncate(Json(message), 24_000),
                    OccurredAtUtc = DateTimeOffset.UtcNow,
                    HostName = Environment.MachineName,
                });
            await db.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Bus journal skipped for {Direction} {MessageType}", direction, messageType);
        }
        finally
        {
            BusJournalWriteGate.Release();
        }
    }

    private static string Json(object message) =>
        JsonSerializer.Serialize(message, message.GetType(), JsonOpts);

    private static string Truncate(string s, int max) =>
        s.Length <= max ? s : s[..max] + "…";
}

/// <summary>One in-flight journal insert per process so Npgsql never interleaves commands on shared pool state.</summary>
internal static class BusJournalWriteGate
{
    internal static readonly SemaphoreSlim Semaphore = new(1, 1);

    internal static Task WaitAsync(CancellationToken ct) => Semaphore.WaitAsync(ct);

    internal static void Release() => Semaphore.Release();
}
