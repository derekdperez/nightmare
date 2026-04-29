using System.Text.Json;
using MassTransit;

namespace NightmareV2.Infrastructure.Messaging;

public sealed class BusJournalPublishObserver(BusJournalBuffer buffer) : IPublishObserver
{
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = false };

    public Task PrePublish<T>(PublishContext<T> context)
        where T : class =>
        Task.CompletedTask;

    public Task PostPublish<T>(PublishContext<T> context)
        where T : class
    {
        buffer.TryEnqueue(
            direction: "Publish",
            messageType: typeof(T).Name,
            payloadJson: JsonSerializer.Serialize(context.Message, context.Message!.GetType(), JsonOpts),
            consumerType: null);
        return Task.CompletedTask;
    }

    public Task PublishFault<T>(PublishContext<T> context, Exception exception)
        where T : class =>
        Task.CompletedTask;
}

public sealed class BusJournalConsumeObserver(BusJournalBuffer buffer) : IConsumeObserver
{
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = false };

    public Task ConsumeFault<T>(ConsumeContext<T> context, Exception exception)
        where T : class =>
        Task.CompletedTask;

    public Task ConsumeFault(ConsumeContext context, Exception exception) => Task.CompletedTask;

    public Task PostConsume<T>(ConsumeContext<T> context)
        where T : class
    {
        buffer.TryEnqueue(
            direction: "Consume",
            messageType: typeof(T).Name,
            payloadJson: JsonSerializer.Serialize(context.Message!, context.Message!.GetType(), JsonOpts),
            consumerType: ResolveConsumerClrName(context));
        return Task.CompletedTask;
    }

    public Task PostConsume(ConsumeContext context) => Task.CompletedTask;

    public Task PreConsume<T>(ConsumeContext<T> context)
        where T : class =>
        Task.CompletedTask;

    public Task PreConsume(ConsumeContext context) => Task.CompletedTask;

    private static string? ResolveConsumerClrName<T>(ConsumeContext<T> context)
        where T : class
    {
        foreach (var arg in context.GetType().GetGenericArguments())
        {
            if (typeof(IConsumer<T>).IsAssignableFrom(arg))
                return arg.FullName;
        }

        return typeof(T).FullName;
    }
}
