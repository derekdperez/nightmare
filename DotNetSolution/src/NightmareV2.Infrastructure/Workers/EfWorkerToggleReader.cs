using Microsoft.EntityFrameworkCore;
using NightmareV2.Application.Workers;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.Infrastructure.Workers;

public sealed class EfWorkerToggleReader(NightmareDbContext db) : IWorkerToggleReader
{
    public async Task<bool> IsWorkerEnabledAsync(string workerKey, CancellationToken cancellationToken = default)
    {
        var row = await db.WorkerSwitches.AsNoTracking()
            .Where(w => w.WorkerKey == workerKey)
            .Select(w => (bool?)w.IsEnabled)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
        return row ?? true;
    }
}
