using System.Net.Sockets;
using NightmareV2.Application.Workers;

namespace NightmareV2.Infrastructure.Workers;

public sealed class DefaultPortScanService : IPortScanService
{
    public async Task<IReadOnlyList<int>> ScanOpenTcpPortsAsync(
        string hostOrIp,
        IReadOnlyList<int> ports,
        TimeSpan perPortTimeout,
        int maxConcurrency,
        CancellationToken cancellationToken = default)
    {
        var open = new List<int>();
        var semaphore = new SemaphoreSlim(Math.Clamp(maxConcurrency, 1, 256));
        var gate = new object();
        var tasks = ports.Select(async port =>
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (await IsOpenAsync(hostOrIp, port, perPortTimeout, cancellationToken).ConfigureAwait(false))
                {
                    lock (gate)
                    {
                        open.Add(port);
                    }
                }
            }
            finally
            {
                semaphore.Release();
            }
        }).ToArray();

        await Task.WhenAll(tasks).ConfigureAwait(false);
        open.Sort();
        return open;
    }

    private static async Task<bool> IsOpenAsync(string host, int port, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var client = new TcpClient();
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);
        try
        {
            await client.ConnectAsync(host, port, timeoutCts.Token).ConfigureAwait(false);
            return client.Connected;
        }
        catch
        {
            return false;
        }
    }
}
