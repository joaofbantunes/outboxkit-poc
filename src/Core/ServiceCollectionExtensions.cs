using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using YakShaveFx.OutboxKit.Core.Polling;

namespace YakShaveFx.OutboxKit.Core;

public static class ServiceCollectionExtensions
{
    // TODO: support multiple databases (particularly for multi tenant scenarios, but probably also useful for other unexpected scenarios)
    // TODO: follow an OpenTelemetry approach to configuring this, returning a custom builder instead of the IServiceCollection
    public static IServiceCollection AddOutboxKit(
        this IServiceCollection services,
        Action<IOutboxKitConfigurator> configure,
        string configurationSection = "Outbox")
    {
        services.AddHostedService<PollingBackgroundService>();
        services.AddSingleton<Listener>();
        services.AddSingleton<IOutboxListener>(s => s.GetRequiredService<Listener>());
        services.AddSingleton<IOutboxTrigger>(s => s.GetRequiredService<Listener>());
        services.AddSingleton<Producer>();
        services.AddSingleton<OutboxSettings>(s =>
        {
            var config = s.GetRequiredService<IConfiguration>();
            return config.GetSection(configurationSection).Get<OutboxSettings>() ?? new OutboxSettings();
        });
        services.AddSingleton<PollingSettings>(s => s.GetRequiredService<OutboxSettings>().Polling);
        var configurator = new OutboxKitConfigurator();
        configure(configurator);
        services.AddSingleton<ITargetProducerProvider>(s =>
            new DefaultTargetProducerProvider(s, configurator.TargetProducers));
        return services;
    }

    public static IServiceCollection AddOutboxBatchFetcher<TOutboxBatchFetcher>(
        this IServiceCollection services)
        where TOutboxBatchFetcher : class, IOutboxBatchFetcher
    {
        services.AddSingleton<IOutboxBatchFetcher, TOutboxBatchFetcher>();
        return services;
    }
}

public interface IOutboxKitConfigurator
{
    IOutboxKitConfigurator WithTargetProducer<TTargetProducer>(string target)
        where TTargetProducer : class, ITargetProducer;
}

internal sealed class OutboxKitConfigurator : IOutboxKitConfigurator
{
    private readonly Dictionary<string, Type> _targetProducers = new();

    public IReadOnlyDictionary<string, Type> TargetProducers => _targetProducers;

    public IOutboxKitConfigurator WithTargetProducer<TTargetProducer>(string target)
        where TTargetProducer : class, ITargetProducer
    {
        _targetProducers[target] = typeof(TTargetProducer);
        return this;
    }
}