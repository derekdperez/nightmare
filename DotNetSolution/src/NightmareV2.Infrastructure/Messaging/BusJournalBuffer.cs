using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NightmareV2.Domain.Entities;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.Infrastructure.Messaging;

public sealed class BusJournalBuffer(
    IDbContextFactory<NightmareDbContext> dbFactory,
    IConfiguration configuration,
    ILogger<BusJournalBuffer> logger) : BackgroundService
{
    private readonly Channel<BusJournalEntry> _channel = Channel.CreateBounded<BusJournalEntry>(
        new BoundedChannelOptions(5000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });
    private readonly bool _enabled = configuration.GetValue("Nightmare:BusJournal:Enabled", true);
    private readonly int _batchSize = Math.Clamp(configuration.GetValue("Nightmare:BusJournal:BatchSize", 100), 10, 500);
    private readonly TimeSpan _flushInterval = TimeSpan.FromMilliseconds(
        Math.Clamp(configuration.GetValue("Nightmare:BusJournal:FlushIntervalMs", 250), 50, 2000));

    public void TryEnqueue(string direction, string messageType, string payloadJson, string? consumerType)
    {
        if (!_enabled)
            return;

        var entry = new BusJournalEntry
        {
            Direction = direction,
            MessageType = Truncate(messageType, 256),
            ConsumerType = consumerType is null ? null : Truncate(consumerType, 512),
            PayloadJson = Truncate(payloadJson, 24_000),
            OccurredAtUtc = DateTimeOffset.UtcNow,
            HostName = Environment.MachineName,
        };

        if (!_channel.Writer.TryWrite(entry))
            logger.LogWarning("Bus journal channel is full; oldest entries are being dropped.");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_enabled)
            return;

        var batch = new List<BusJournalEntry>(_batchSize);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                while (batch.Count < _batchSize && _channel.Reader.TryRead(out var entry))
                    batch.Add(entry);

                if (batch.Count == 0)
                {
                    await Task.Delay(_flushInterval, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await FlushBatchAsync(batch, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Bus journal batch flush failed.");
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken).ConfigureAwait(false);
            }
        }

        if (batch.Count > 0)
            await FlushBatchAsync(batch, CancellationToken.None).ConfigureAwait(false);
    }

    private async Task FlushBatchAsync(List<BusJournalEntry> batch, CancellationToken ct)
    {
        if (batch.Count == 0)
            return;

        await using var db = await dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
        db.BusJournal.AddRange(batch);
        await db.SaveChangesAsync(ct).ConfigureAwait(false);
        batch.Clear();
    }

    private static string Truncate(string value, int maxChars) =>
        value.Length <= maxChars ? value : value[..maxChars];
}
