using Microsoft.EntityFrameworkCore;
using NightmareV2.Application.Workers;
using NightmareV2.Domain.Entities;

namespace NightmareV2.Infrastructure.Data;

public static class NightmareDbSeeder
{
    public static async Task SeedWorkerSwitchesAsync(NightmareDbContext db, CancellationToken cancellationToken = default)
    {
        if (await db.WorkerSwitches.AnyAsync(cancellationToken).ConfigureAwait(false))
            return;

        var now = DateTimeOffset.UtcNow;
        db.WorkerSwitches.AddRange(
            new WorkerSwitch { WorkerKey = WorkerKeys.Spider, IsEnabled = true, UpdatedAtUtc = now },
            new WorkerSwitch { WorkerKey = WorkerKeys.Enumeration, IsEnabled = true, UpdatedAtUtc = now },
            new WorkerSwitch { WorkerKey = WorkerKeys.PortScan, IsEnabled = true, UpdatedAtUtc = now });
        await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
    }
}
