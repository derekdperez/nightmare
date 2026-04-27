using MassTransit;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Gatekeeper.Consumers;

public sealed class AssetDiscoveredConsumer(GatekeeperOrchestrator orchestrator) : IConsumer<AssetDiscovered>
{
    public Task Consume(ConsumeContext<AssetDiscovered> context) =>
        orchestrator.ProcessAsync(context.Message, context.CancellationToken);
}
