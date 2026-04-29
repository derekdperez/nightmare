namespace NightmareV2.Application.Workers;

public interface IPortScanService
{
    Task<IReadOnlyList<int>> ScanOpenTcpPortsAsync(
        string hostOrIp,
        IReadOnlyList<int> ports,
        TimeSpan perPortTimeout,
        int maxConcurrency,
        CancellationToken cancellationToken = default);
}
