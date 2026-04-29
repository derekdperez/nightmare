using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Application.Workers;
using NightmareV2.Infrastructure.Data;
using NightmareV2.Application.FileStore;
using NightmareV2.Application.Events;
using NightmareV2.Application.HighValue;
using NightmareV2.Infrastructure.FileStore;
using NightmareV2.Infrastructure.Gatekeeping;
using NightmareV2.Infrastructure.HighValue;
using NightmareV2.Infrastructure.Messaging;
using NightmareV2.Infrastructure.Workers;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;

namespace NightmareV2.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddNightmareInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        var redisConn = configuration.GetConnectionString("Redis") ?? "localhost:6379";
        var pgConn = configuration.GetConnectionString("Postgres")
                     ?? "Host=localhost;Port=5432;Database=nightmare_v2;Username=nightmare;Password=nightmare";
        var fileStoreConn = configuration.GetConnectionString("FileStore")
            ?? configuration.GetConnectionString("Postgres")?.Replace(
                "Database=nightmare_v2",
                "Database=nightmare_v2_files",
                StringComparison.OrdinalIgnoreCase)
            ?? "Host=localhost;Port=5432;Database=nightmare_v2_files;Username=nightmare;Password=nightmare";

        services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConn));

        void ConfigureNpgsql(DbContextOptionsBuilder options) => options.UseNpgsql(pgConn);

        services.AddDbContext<NightmareDbContext>(ConfigureNpgsql);
        services.AddDbContextFactory<NightmareDbContext>(ConfigureNpgsql);

        void ConfigureFileStore(DbContextOptionsBuilder options) => options.UseNpgsql(fileStoreConn);
        services.AddDbContextFactory<FileStoreDbContext>(ConfigureFileStore);
        services.AddSingleton<IFileStore, EfFileStore>();

        services.AddSingleton<IAssetCanonicalizer, DefaultAssetCanonicalizer>();
        services.AddSingleton<IAssetDeduplicator, RedisAssetDeduplicator>();
        services.AddSingleton<ITargetScopeEvaluator, DnsTargetScopeEvaluator>();
        services.AddScoped<IAssetPersistence, EfAssetPersistence>();
        services.AddScoped<IHighValueFindingWriter, EfHighValueFindingWriter>();
        services.AddScoped<IWorkerToggleReader, EfWorkerToggleReader>();
        services.AddSingleton<IHttpRequestQueueStateMachine, DefaultHttpRequestQueueStateMachine>();
        services.AddSingleton<IEnumerationService, DefaultEnumerationService>();
        services.AddSingleton<IPortScanService, DefaultPortScanService>();
        services.AddScoped<IEventOutbox, EfEventOutbox>();
        services.AddScoped<IInboxDeduplicator, EfInboxDeduplicator>();
        services.AddHostedService<OutboxDispatcherWorker>();
        services.AddSingleton<BusJournalBuffer>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<BusJournalBuffer>());
        services.AddSingleton<BusJournalPublishObserver>();
        services.AddSingleton<BusJournalConsumeObserver>();

        return services;
    }
}
