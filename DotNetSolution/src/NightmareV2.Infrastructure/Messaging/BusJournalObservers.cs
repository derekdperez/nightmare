using System.Text.Json;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.Infrastructure.Messaging;

public sealed class BusJournalPublishObserver(IServiceScopeFactory scopeFactory) : IPublishObserver
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

    private Task WriteAsync(string direction, string messageType, string payloadJson, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<NightmareDbContext>();
        db.BusJournal.Add(
            new BusJournalEntry
            {
                Direction = direction,
                MessageType = messageType,
                ConsumerType = null,
                PayloadJson = Truncate(payloadJson, 24_000),
                OccurredAtUtc = DateTimeOffset.UtcNow,
            });
        return db.SaveChangesAsync(ct);
    }

    private static string Json<T>(T message) where T : class =>
        JsonSerializer.Serialize(message, message.GetType(), JsonOpts);

    private static string Truncate(string s, int max) =>
        s.Length <= max ? s : s[..max] + "…";
}

public sealed class BusJournalConsumeObserver(IServiceScopeFactory scopeFactory) : IConsumeObserver
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

    private Task WriteAsync(string direction, string messageType, object message, string? consumerType, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<NightmareDbContext>();
        db.BusJournal.Add(
            new BusJournalEntry
            {
                Direction = direction,
                MessageType = messageType,
                ConsumerType = consumerType,
                PayloadJson = Truncate(Json(message), 24_000),
                OccurredAtUtc = DateTimeOffset.UtcNow,
            });
        return db.SaveChangesAsync(ct);
    }

    private static string Json(object message) =>
        JsonSerializer.Serialize(message, message.GetType(), JsonOpts);

    private static string Truncate(string s, int max) =>
        s.Length <= max ? s : s[..max] + "…";
}
