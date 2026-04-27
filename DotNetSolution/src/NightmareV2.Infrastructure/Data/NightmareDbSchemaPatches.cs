using Microsoft.EntityFrameworkCore;

namespace NightmareV2.Infrastructure.Data;

/// <summary>
/// EF <c>EnsureCreated</c> does not add new columns to existing databases; run these patches after it on upgrade.
/// </summary>
public static class NightmareDbSchemaPatches
{
    public static async Task ApplyAfterEnsureCreatedAsync(NightmareDbContext db, CancellationToken cancellationToken = default)
    {
        await db.Database.ExecuteSqlRawAsync(
                """
                ALTER TABLE bus_journal ADD COLUMN IF NOT EXISTS host_name character varying(256) NOT NULL DEFAULT '';
                """,
                cancellationToken)
            .ConfigureAwait(false);
    }
}
