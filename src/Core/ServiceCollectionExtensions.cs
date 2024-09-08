using System.Collections.Frozen;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace YakShaveFx.OutboxKit.Core;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddOutboxKit(
        this IServiceCollection services,
        Action<IOutboxKitConfigurator> configure,
        string configurationSection = "Outbox")
    {
        services.AddHostedService<PollingBackgroundService>();
        services.AddSingleton<Listener>();
        services.AddSingleton<IOutboxListener>(s => s.GetRequiredService<Listener>());
        services.AddSingleton<IOutboxTrigger>(s => s.GetRequiredService<Listener>());
        services.AddSingleton<IOutboxProducer, Producer>();
        services.AddSingleton<OutboxSettings>(s =>
        {
            var config = s.GetRequiredService<IConfiguration>();
            return config.GetSection(configurationSection).Get<OutboxSettings>() ?? new OutboxSettings();
        });
        var configurator = new OutboxKitConfigurator(services);
        configure(configurator);
        configurator.Build();
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

internal sealed class OutboxKitConfigurator(IServiceCollection services) : IOutboxKitConfigurator
{
    private readonly Dictionary<string, Type> _targetProducers = new();

    public IOutboxKitConfigurator WithTargetProducer<TTargetProducer>(string target)
        where TTargetProducer : class, ITargetProducer
    {
        _targetProducers[target] = typeof(TTargetProducer);
        return this;
    }

    public void Build()
    {
        services.AddSingleton<ITargetProducerProvider>(s => new TargetProducerProvider(s, _targetProducers));
    }
}

internal sealed class TargetProducerProvider : ITargetProducerProvider
{
    private readonly FrozenDictionary<string, ITargetProducer> _targetProducers;

    public TargetProducerProvider(IServiceProvider serviceProvider, IReadOnlyDictionary<string, Type> targetProducers)
    {
        _targetProducers = targetProducers.ToDictionary(
                kvp => kvp.Key,
                kvp => (ITargetProducer)serviceProvider.GetRequiredService(kvp.Value))
            .ToFrozenDictionary();
    }

    public ITargetProducer Get(string target)
    {
        if (!_targetProducers.TryGetValue(target, out var producer))
        {
            throw new InvalidOperationException($"No producer registered for target '{target}'");
        }

        return producer;
    }
}