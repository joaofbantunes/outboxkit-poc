using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using YakShaveFx.OutboxKit.Core.Polling;

namespace YakShaveFx.OutboxKit.Core;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddOutboxKit(
        this IServiceCollection services,
        Action<IOutboxKitConfigurator> configure,
        string configurationSection = "Outbox")
    {
        services.AddSingleton<OutboxSettings>(s =>
        {
            var config = s.GetRequiredService<IConfiguration>();
            return config.GetSection(configurationSection).Get<OutboxSettings>() ?? new OutboxSettings();
        });
        services.AddSingleton<PollingSettings>(s => s.GetRequiredService<OutboxSettings>().Polling);
        var configurator = new OutboxKitConfigurator();
        configure(configurator);
        services.AddSingleton<ITargetProducerProvider>(
            s => new DefaultTargetProducerProvider(s, configurator.TargetProducers));

        if (configurator.PollingConfigurators.Count > 0)
        {
            AddOutboxKitPolling(services, configurator);
        }

        return services;
    }

    private static void AddOutboxKitPolling(IServiceCollection services, OutboxKitConfigurator configurator)
    {
        services.AddSingleton<Producer>();

        if (configurator.PollingConfigurators.Count == 1)
        {
            services.AddSingleton<Listener>();
            services.AddSingleton<IOutboxListener>(s => s.GetRequiredService<Listener>());
            services.AddSingleton<IOutboxTrigger>(s => s.GetRequiredService<Listener>());
            services.AddSingleton<IKeyedOutboxListener>(s => s.GetRequiredService<Listener>());
            services.AddSingleton<IKeyedOutboxTrigger>(s => s.GetRequiredService<Listener>());
        }
        else
        {
            services.AddSingleton<KeyedListener>(s =>
            {
                var keys = configurator.PollingConfigurators.Keys;
                return new KeyedListener(keys);
            });
            services.AddSingleton<IKeyedOutboxListener>(s => s.GetRequiredService<KeyedListener>());
            services.AddSingleton<IKeyedOutboxTrigger>(s => s.GetRequiredService<KeyedListener>());
        }

        foreach (var (key, pollingConfigurator) in configurator.PollingConfigurators)
        {
            services.AddHostedService(s => new PollingBackgroundService(
                key,
                s.GetRequiredService<PollingSettings>(),
                s.GetRequiredService<IKeyedOutboxListener>(),
                s.GetRequiredService<Producer>(),
                s.GetRequiredService<TimeProvider>(),
                s.GetRequiredService<ILogger<PollingBackgroundService>>()));

            pollingConfigurator.Configure(key, services);
        }
    }
}

public interface IOutboxKitConfigurator
{
    IOutboxKitConfigurator WithTargetProducer<TTargetProducer>(string target)
        where TTargetProducer : class, ITargetProducer;

    // IOutboxKitConfigurator WithPolling<TPollingOutboxKitConfigurator>(Action<TPollingOutboxKitConfigurator> configure) 
    //     where TPollingOutboxKitConfigurator : IPollingOutboxKitConfigurator, new();
    //
    // IOutboxKitConfigurator WithPolling<TPollingOutboxKitConfigurator>(
    //     string key,
    //     Action<TPollingOutboxKitConfigurator> configure)
    //     where TPollingOutboxKitConfigurator : IPollingOutboxKitConfigurator, new();

    IOutboxKitConfigurator WithPolling<TPollingOutboxKitConfigurator>(TPollingOutboxKitConfigurator configurator)
        where TPollingOutboxKitConfigurator : IPollingOutboxKitConfigurator, new();

    IOutboxKitConfigurator WithPolling<TPollingOutboxKitConfigurator>(
        string key,
        TPollingOutboxKitConfigurator configurator)
        where TPollingOutboxKitConfigurator : IPollingOutboxKitConfigurator, new();
}

public interface IPollingOutboxKitConfigurator
{
    void Configure(string key, IServiceCollection services);
}

internal sealed class OutboxKitConfigurator : IOutboxKitConfigurator
{
    private readonly Dictionary<string, Type> _targetProducers = new();
    private readonly Dictionary<string, IPollingOutboxKitConfigurator> _pollingConfigurators = new();

    public IReadOnlyDictionary<string, Type> TargetProducers => _targetProducers;

    public IReadOnlyDictionary<string, IPollingOutboxKitConfigurator> PollingConfigurators => _pollingConfigurators;

    public IOutboxKitConfigurator WithTargetProducer<TTargetProducer>(string target)
        where TTargetProducer : class, ITargetProducer
    {
        _targetProducers.Add(target, typeof(TTargetProducer));
        return this;
    }

    public IOutboxKitConfigurator WithPolling<TPollingOutboxKitConfigurator>(
        TPollingOutboxKitConfigurator configurator)
        where TPollingOutboxKitConfigurator : IPollingOutboxKitConfigurator, new()
        => WithPolling("default", configurator);

    public IOutboxKitConfigurator WithPolling<TPollingOutboxKitConfigurator>(
        string key,
        TPollingOutboxKitConfigurator configurator)
        where TPollingOutboxKitConfigurator : IPollingOutboxKitConfigurator, new()
    {
        _pollingConfigurators.Add(key, configurator);
        return this;
    }
}