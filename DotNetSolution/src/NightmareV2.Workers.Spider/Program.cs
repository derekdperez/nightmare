using System.Net.Http;
using System.Net.Security;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using NightmareV2.Infrastructure;
using NightmareV2.Infrastructure.Data;
using NightmareV2.Infrastructure.Messaging;
using NightmareV2.Workers.Spider;
using NightmareV2.Workers.Spider.Consumers;

var builder = Host.CreateApplicationBuilder(args);

var allowInsecureSpiderSsl = builder.Configuration.GetValue("Spider:Http:AllowInsecureSsl", false);
if (allowInsecureSpiderSsl)
{
    Console.WriteLine("Spider: Spider:Http:AllowInsecureSsl=true — TLS server certificate validation is disabled for HTTP fetches.");
}

builder.Services.AddHttpClient("spider")
    .ConfigurePrimaryHttpMessageHandler(() =>
    {
        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        };
        if (allowInsecureSpiderSsl)
        {
            handler.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
        }

        return handler;
    })
    .AddPolicyHandler(SpiderAssetDiscoveredConsumer.RetryPolicy());

builder.Services.AddNightmareInfrastructure(builder.Configuration);
builder.Services.AddHostedService<HttpRequestQueueWorker>();
builder.Services.AddNightmareRabbitMq(builder.Configuration, _ => { });

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<NightmareDbContext>();
    await db.Database.EnsureCreatedAsync().ConfigureAwait(false);
    await NightmareDbSchemaPatches.ApplyAfterEnsureCreatedAsync(db).ConfigureAwait(false);
    await NightmareDbSeeder.SeedWorkerSwitchesAsync(db).ConfigureAwait(false);
}

await host.RunAsync().ConfigureAwait(false);
