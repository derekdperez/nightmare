using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NightmareV2.Application.Gatekeeping;
using NightmareV2.Application.Workers;
using NightmareV2.Infrastructure.Data;
using NightmareV2.Infrastructure.Gatekeeping;
using NightmareV2.Infrastructure.Messaging;
using NightmareV2.Infrastructure.Workers;
using StackExchange.Redis;

namespace NightmareV2.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddNightmareInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        var redisConn = configuration.GetConnectionString("Redis") ?? "localhost:6379";
        var pgConn = configuration.GetConnectionString("Postgres")
                     ?? "Host=localhost;Port=5432;Database=nightmare_v2;Username=nightmare;Password=nightmare";

        services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConn));

        void ConfigureNpgsql(DbContextOptionsBuilder options) => options.UseNpgsql(pgConn);

        services.AddDbContext<NightmareDbContext>(ConfigureNpgsql);
        services.AddDbContextFactory<NightmareDbContext>(ConfigureNpgsql);

        services.AddSingleton<IAssetCanonicalizer, DefaultAssetCanonicalizer>();
        services.AddSingleton<IAssetDeduplicator, RedisAssetDeduplicator>();
        services.AddSingleton<ITargetScopeEvaluator, DnsTargetScopeEvaluator>();
        services.AddScoped<IAssetPersistence, EfAssetPersistence>();
        services.AddScoped<IWorkerToggleReader, EfWorkerToggleReader>();
        services.AddSingleton<BusJournalPublishObserver>();
        services.AddSingleton<BusJournalConsumeObserver>();

        return services;
    }
}
