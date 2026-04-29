using System.Diagnostics;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using NightmareV2.Infrastructure.Data;

namespace NightmareV2.CommandCenter.Diagnostics;

public static class DiagnosticsEndpoints
{
    public static void Map(WebApplication app)
    {
        app.MapGet(
                "/health",
                () => Results.Ok(new { status = "live", at = DateTimeOffset.UtcNow }))
            .WithName("HealthLive")
            .AllowAnonymous();

        app.MapGet(
                "/health/ready",
                async (NightmareDbContext db, CancellationToken ct) =>
                {
                    try
                    {
                        if (!await db.Database.CanConnectAsync(ct).ConfigureAwait(false))
                            return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
                        return Results.Ok(new { status = "ready", postgres = "ok", at = DateTimeOffset.UtcNow });
                    }
                    catch (Exception)
                    {
                        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
                    }
                })
            .WithName("HealthReady")
            .AllowAnonymous();

        app.MapGet(
                "/api/diagnostics/self",
                async (
                    HttpContext http,
                    IConfiguration config,
                    NightmareDbContext db,
                    IDbContextFactory<FileStoreDbContext> fileStoreFactory,
                    CancellationToken ct) =>
                {
                    if (!config.GetValue("Nightmare:Diagnostics:Enabled", false))
                        return Results.NotFound();

                    var requiredKey = config["Nightmare:Diagnostics:ApiKey"]?.Trim();
                    if (string.IsNullOrWhiteSpace(requiredKey))
                    {
                        return Results.Problem(
                            title: "Diagnostics endpoint misconfigured",
                            detail: "Nightmare:Diagnostics:Enabled=true requires Nightmare:Diagnostics:ApiKey to be configured.",
                            statusCode: StatusCodes.Status503ServiceUnavailable);
                    }

                    if (!string.Equals(
                            http.Request.Headers["X-Nightmare-Diagnostics-Key"].ToString(),
                            requiredKey,
                            StringComparison.Ordinal))
                    {
                        return Results.Unauthorized();
                    }

                    var postgres = "unknown";
                    try
                    {
                        postgres = await db.Database.CanConnectAsync(ct).ConfigureAwait(false) ? "ok" : "fail";
                    }
                    catch
                    {
                        postgres = "fail";
                    }

                    var fileStore = "unknown";
                    try
                    {
                        await using var fs = await fileStoreFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
                        fileStore = await fs.Database.CanConnectAsync(ct).ConfigureAwait(false) ? "ok" : "fail";
                    }
                    catch
                    {
                        fileStore = "fail";
                    }

                    var asm = typeof(DiagnosticsEndpoints).Assembly;
                    var informational =
                        asm.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
                        ?? asm.GetName().Version?.ToString()
                        ?? "?";
                    var proc = Process.GetCurrentProcess();
                    var uptime = DateTimeOffset.UtcNow - proc.StartTime.ToUniversalTime();

                    return Results.Json(
                        new
                        {
                            at = DateTimeOffset.UtcNow,
                            environment = app.Environment.EnvironmentName,
                            machine = Environment.MachineName,
                            uptimeSeconds = (int)Math.Floor(uptime.TotalSeconds),
                            buildStamp = Environment.GetEnvironmentVariable("NIGHTMARE_BUILD_STAMP") ?? "(not set)",
                            assemblyInformationalVersion = informational,
                            postgres,
                            fileStore,
                            rabbitHost = config["RabbitMq:Host"],
                            redisConfigured = !string.IsNullOrWhiteSpace(config.GetConnectionString("Redis")),
                        });
                })
            .WithName("DiagnosticsSelf")
            .AllowAnonymous();
    }
}
