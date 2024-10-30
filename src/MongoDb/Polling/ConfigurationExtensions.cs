using System.Linq.Expressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.Polling;
using YakShaveFx.OutboxKit.MongoDb.Synchronization;

namespace YakShaveFx.OutboxKit.MongoDb.Polling;

public static class OutboxKitPollingConfiguratorExtensions
{
    public static IOutboxKitConfigurator WithMongoDbPolling(
        this IOutboxKitConfigurator configurator,
        Action<IMongoDbPollingOutboxKitConfigurator> configure)
    {
        var pollingConfigurator = new PollingOutboxKitConfigurator();
        configure(pollingConfigurator);
        configurator.WithPolling(pollingConfigurator);
        return configurator;
    }

    public static IOutboxKitConfigurator WithMongoDbPolling(
        this IOutboxKitConfigurator configurator,
        string key,
        Action<IMongoDbPollingOutboxKitConfigurator> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, nameof(key));
        var pollingConfigurator = new PollingOutboxKitConfigurator();
        configure(pollingConfigurator);
        configurator.WithPolling(key, pollingConfigurator);
        return configurator;
    }
}

public interface IMongoDbPollingOutboxKitConfigurator
{
    IMongoDbPollingOutboxKitConfigurator WithConnectionString(string connectionString);
    IMongoDbPollingOutboxKitConfigurator WithDatabaseName(string databaseName);
    IMongoDbPollingOutboxKitConfigurator WithPollingInterval(TimeSpan pollingInterval);
    IMongoDbPollingOutboxKitConfigurator WithBatchSize(int batchSize);
}

internal sealed class PollingOutboxKitConfigurator : IPollingOutboxKitConfigurator, IMongoDbPollingOutboxKitConfigurator
{
    private string? _connectionString;
    private string? _databaseName;
    private CorePollingSettings _corePollingSettings = new();
    private MongoDbPollingSettings _mongoDbPollingSettings = new();


    public IMongoDbPollingOutboxKitConfigurator WithConnectionString(string connectionString)
    {
        _connectionString = connectionString;
        return this;
    }

    public IMongoDbPollingOutboxKitConfigurator WithDatabaseName(string databaseName)
    {
        _databaseName = databaseName;
        return this;
    }

    public IMongoDbPollingOutboxKitConfigurator WithPollingInterval(TimeSpan pollingInterval)
    {
        if (pollingInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(pollingInterval), pollingInterval,
                "Polling interval must be greater than zero");
        }

        _corePollingSettings = _corePollingSettings with { PollingInterval = pollingInterval };
        return this;
    }

    public IMongoDbPollingOutboxKitConfigurator WithBatchSize(int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be greater than zero");
        }

        _mongoDbPollingSettings = _mongoDbPollingSettings with { BatchSize = batchSize };
        return this;
    }

    public void ConfigureServices(string key, IServiceCollection services)
    {
        if (string.IsNullOrWhiteSpace(_connectionString))
        {
            throw new InvalidOperationException(
                $"Connection string must be set for MongoDB polling with key \"{key}\"");
        }

        if (string.IsNullOrWhiteSpace(_databaseName))
        {
            throw new InvalidOperationException($"Database name must be set for MongoDB polling with key \"{key}\"");
        }

        var client = new MongoClient(_connectionString);

        services
            .AddKeyedSingleton(key, client)
            .AddKeyedSingleton(key, client.GetDatabase(_databaseName))
            .AddKeyedSingleton(key, new DistributedLockSettings { ChangeStreamsEnabled = false })
            .AddKeyedSingleton(key, (s, _) => new DistributedLockThingy(
                s.GetRequiredKeyedService<DistributedLockSettings>(key),
                s.GetRequiredKeyedService<IMongoDatabase>(key),
                s.GetRequiredService<TimeProvider>(),
                s.GetRequiredService<ILogger<DistributedLockThingy>>()))
            .AddKeyedSingleton<IOutboxBatchFetcher>(
                key,
                (s, _) => new OutboxBatchFetcher(
                    key,
                    _mongoDbPollingSettings,
                    s.GetRequiredKeyedService<IMongoDatabase>(key),
                    s.GetRequiredKeyedService<DistributedLockThingy>(key)));
    }

    public CorePollingSettings GetCoreSettings() => _corePollingSettings;
}

internal sealed record MongoDbPollingSettings
{
    public int BatchSize { get; init; } = 100;
}