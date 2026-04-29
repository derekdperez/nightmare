using MassTransit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace NightmareV2.Infrastructure.Messaging;

/// <summary>
/// Dev: RabbitMQ. Production: swap for MassTransit.AmazonSQS (design §3).
/// </summary>
public static class MassTransitRabbitExtensions
{
    public static IServiceCollection AddNightmareRabbitMq(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<IBusRegistrationConfigurator> configureConsumers)
    {
        var host = configuration["RabbitMq:Host"] ?? "localhost";
        var user = configuration["RabbitMq:Username"] ?? "guest";
        var pass = configuration["RabbitMq:Password"] ?? "guest";
        var vhost = string.IsNullOrWhiteSpace(configuration["RabbitMq:VirtualHost"])
            ? "/"
            : configuration["RabbitMq:VirtualHost"]!;

        services.TryAddSingleton<BusJournalPublishObserver>();
        services.TryAddSingleton<BusJournalConsumeObserver>();

        services.Configure<MassTransitHostOptions>(options =>
        {
            options.WaitUntilStarted = false;
            options.StartTimeout = TimeSpan.FromSeconds(
                GetClampedSeconds(configuration, "RabbitMq:StartTimeoutSeconds", 15, 1, 120));
            options.StopTimeout = TimeSpan.FromSeconds(
                GetClampedSeconds(configuration, "RabbitMq:StopTimeoutSeconds", 30, 1, 120));
        });

        services.AddMassTransit(x =>
        {
            configureConsumers(x);
            x.SetKebabCaseEndpointNameFormatter();
            x.UsingRabbitMq((context, cfg) =>
            {
                cfg.Host(host, vhost, h =>
                {
                    h.Username(user);
                    h.Password(pass);
                });
                cfg.ConnectPublishObserver(context.GetRequiredService<BusJournalPublishObserver>());
                cfg.ConnectConsumeObserver(context.GetRequiredService<BusJournalConsumeObserver>());
                cfg.ConfigureEndpoints(context);
            });
        });

        return services;
    }

    private static int GetClampedSeconds(
        IConfiguration configuration,
        string key,
        int defaultValue,
        int minValue,
        int maxValue)
    {
        var configuredValue = configuration[key];
        if (!int.TryParse(configuredValue, out var parsedValue))
        {
            parsedValue = defaultValue;
        }

        return Math.Clamp(parsedValue, minValue, maxValue);
    }
}
