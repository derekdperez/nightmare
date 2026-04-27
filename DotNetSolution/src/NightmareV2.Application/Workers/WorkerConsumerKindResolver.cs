namespace NightmareV2.Application.Workers;

/// <summary>Maps MassTransit consumer CLR type names to operator <see cref="WorkerKeys"/>.</summary>
public static class WorkerConsumerKindResolver
{
    public static string? KindFromConsumerType(string? consumerTypeFullName)
    {
        if (string.IsNullOrWhiteSpace(consumerTypeFullName))
            return null;

        if (consumerTypeFullName.Contains("Gatekeeper.Consumers.AssetDiscoveredConsumer", StringComparison.Ordinal))
            return WorkerKeys.Gatekeeper;
        if (consumerTypeFullName.Contains("Workers.Spider.Consumers.SpiderAssetDiscoveredConsumer", StringComparison.Ordinal))
            return WorkerKeys.Spider;
        if (consumerTypeFullName.Contains("Workers.Enum.Consumers.TargetCreatedConsumer", StringComparison.Ordinal))
            return WorkerKeys.Enumeration;
        if (consumerTypeFullName.Contains("Workers.PortScan.Consumers.PortScanRequestedConsumer", StringComparison.Ordinal))
            return WorkerKeys.PortScan;
        return null;
    }
}
