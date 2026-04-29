using NightmareV2.Infrastructure;
using NightmareV2.Infrastructure.Data;
using NightmareV2.Infrastructure.Messaging;
using NightmareV2.Workers.PortScan.Consumers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddNightmareInfrastructure(builder.Configuration);
builder.Services.AddNightmareRabbitMq(builder.Configuration, x => x.AddConsumer<PortScanRequestedConsumer>());

var host = builder.Build();
var startupLog = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Startup");
await StartupDatabaseBootstrap.InitializeAsync(
        host.Services,
        host.Services.GetRequiredService<IConfiguration>(),
        startupLog,
        includeFileStore: false,
        host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping)
    .ConfigureAwait(false);

await host.RunAsync().ConfigureAwait(false);
