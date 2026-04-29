using MassTransit;
using NightmareV2.Application.Events;
using Microsoft.Extensions.Logging;
using NightmareV2.Application.Workers;
using NightmareV2.Contracts;
using NightmareV2.Contracts.Events;

namespace NightmareV2.Workers.PortScan.Consumers;

public sealed class PortScanRequestedConsumer(
    IWorkerToggleReader toggles,
    IPortScanService portScan,
    IEventOutbox outbox,
    IConfiguration configuration,
    ILogger<PortScanRequestedConsumer> logger)
    : IConsumer<PortScanRequested>
{
    private static readonly int[] DefaultPorts =
    [
        21, 22, 25, 53, 80, 110, 143, 443, 445, 465, 587, 631, 993, 995, 1433, 1521, 2375, 2376, 3000, 3306, 3389, 4000, 5000, 5432, 5672, 6379, 8080, 8443, 9200, 27017
    ];

    public async Task Consume(ConsumeContext<PortScanRequested> context)
    {
        if (!await toggles.IsWorkerEnabledAsync(WorkerKeys.PortScan, context.CancellationToken).ConfigureAwait(false))
            return;

        var m = context.Message;
        var ports = ParsePorts(configuration["PortScan:Ports"]);
        var timeoutMs = Math.Clamp(configuration.GetValue("PortScan:TimeoutMs", 700), 100, 5000);
        var maxConcurrency = Math.Clamp(configuration.GetValue("PortScan:MaxConcurrency", 32), 1, 256);
        var open = await portScan.ScanOpenTcpPortsAsync(
                m.HostOrIp,
                ports,
                TimeSpan.FromMilliseconds(timeoutMs),
                maxConcurrency,
                context.CancellationToken)
            .ConfigureAwait(false);

        logger.LogInformation("Port scan completed for {Host}; open ports: {Count}", m.HostOrIp, open.Count);
        if (open.Count == 0)
            return;

        var causation = m.EventId == Guid.Empty ? m.CorrelationId : m.EventId;
        foreach (var port in open)
        {
            await outbox.EnqueueAsync(
                    new AssetDiscovered(
                        m.TargetId,
                        m.TargetRootDomain,
                        m.GlobalMaxDepth,
                        m.Depth + 1,
                        AssetKind.OpenPort,
                        $"{m.HostOrIp}:{port}",
                        "worker-portscan",
                        DateTimeOffset.UtcNow,
                        m.CorrelationId,
                        AssetAdmissionStage.Raw,
                        null,
                        $"Port scan found open TCP port {port} on host {m.HostOrIp}.",
                        EventId: NewId.NextGuid(),
                        CausationId: causation,
                        Producer: "worker-portscan"),
                    context.CancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static IReadOnlyList<int> ParsePorts(string? csv)
    {
        if (string.IsNullOrWhiteSpace(csv))
            return DefaultPorts;
        var parsed = csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(s => int.TryParse(s, out var n) ? n : -1)
            .Where(n => n > 0 && n <= 65535)
            .Distinct()
            .OrderBy(n => n)
            .ToArray();
        return parsed.Length > 0 ? parsed : DefaultPorts;
    }
}
