using MassTransit;
using Microsoft.Extensions.Logging;
using NightmareV2.Application.Workers;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Workers.PortScan.Consumers;

public sealed class PortScanRequestedConsumer(IWorkerToggleReader toggles, ILogger<PortScanRequestedConsumer> logger)
    : IConsumer<PortScanRequested>
{
    public async Task Consume(ConsumeContext<PortScanRequested> context)
    {
        if (!await toggles.IsWorkerEnabledAsync("PortScan", context.CancellationToken).ConfigureAwait(false))
            return;

        var m = context.Message;
        logger.LogInformation("Port scan placeholder for {Host} asset {AssetId}", m.HostOrIp, m.AssetId);
    }
}
