using MassTransit;
using NightmareV2.Application.Events;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Gatekeeper.Consumers;

public sealed class AssetDiscoveredConsumer(GatekeeperOrchestrator orchestrator, IInboxDeduplicator inbox) : IConsumer<AssetDiscovered>
{
    public async Task Consume(ConsumeContext<AssetDiscovered> context)
    {
        if (!await inbox.TryBeginProcessingAsync(context.Message, nameof(AssetDiscoveredConsumer), context.CancellationToken).ConfigureAwait(false))
            return;

        await orchestrator.ProcessAsync(context.Message, context.CancellationToken).ConfigureAwait(false);
    }
}
