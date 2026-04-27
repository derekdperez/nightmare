using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Gatekeeper.Consumers;
using NightmareV2.Infrastructure;
using NightmareV2.Infrastructure.Data;
using NightmareV2.Infrastructure.Messaging;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddNightmareInfrastructure(builder.Configuration);
builder.Services.AddScoped<GatekeeperOrchestrator>();
builder.Services.AddNightmareRabbitMq(builder.Configuration, x => x.AddConsumer<AssetDiscoveredConsumer>());

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<NightmareDbContext>();
    await db.Database.EnsureCreatedAsync().ConfigureAwait(false);
    await NightmareDbSchemaPatches.ApplyAfterEnsureCreatedAsync(db).ConfigureAwait(false);
    await NightmareDbSeeder.SeedWorkerSwitchesAsync(db).ConfigureAwait(false);
}

await host.RunAsync().ConfigureAwait(false);
