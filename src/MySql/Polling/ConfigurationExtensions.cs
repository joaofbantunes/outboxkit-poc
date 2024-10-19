using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.Polling;

namespace YakShaveFx.OutboxKit.MySql.Polling;

public static class OutboxKitConfiguratorExtensions
{
    public static IOutboxKitConfigurator WithMySqlPolling(
        this IOutboxKitConfigurator configurator,
        Action<IMySqlPollingOutboxKitConfigurator> configure)
    {
        var pollingConfigurator = new PollingOutboxKitConfigurator();
        configure(pollingConfigurator);
        configurator.WithPolling(pollingConfigurator);
        return configurator;
    }

    public static IOutboxKitConfigurator WithMySqlPolling(
        this IOutboxKitConfigurator configurator, 
        string key,
        Action<IMySqlPollingOutboxKitConfigurator> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, nameof(key));
        var pollingConfigurator = new PollingOutboxKitConfigurator();
        configure(pollingConfigurator);
        configurator.WithPolling(key, pollingConfigurator);
        return configurator;
    }
}

public interface IMySqlPollingOutboxKitConfigurator
{
    IMySqlPollingOutboxKitConfigurator WithConnectionString(string connectionString);
}

internal sealed class PollingOutboxKitConfigurator : IPollingOutboxKitConfigurator, IMySqlPollingOutboxKitConfigurator
{
    private string? _connectionString;

    public IMySqlPollingOutboxKitConfigurator WithConnectionString(string connectionString)
    {
        _connectionString = connectionString;
        return this;
    }
    
    public void Configure(string key, IServiceCollection services)
    {
        if (_connectionString is null)
        {
            throw new InvalidOperationException($"Connection string must be set for MySql polling with key \"{key}\"");
        }

        services
            .AddKeyedMySqlDataSource(key, _connectionString)
            .AddKeyedSingleton<IOutboxBatchFetcher>(key, (s, _) => new OutboxBatchFetcher(s, key));
    }
}