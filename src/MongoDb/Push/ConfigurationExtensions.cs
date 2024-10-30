using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.MongoDb.Synchronization;

namespace YakShaveFx.OutboxKit.MongoDb.Push;

public static class OutboxKitPushConfiguratorExtensions
{
    public static IOutboxKitConfigurator WithMongoDbPush(
        this IOutboxKitConfigurator configurator,
        Action<IMongoDbPushOutboxKitConfigurator> configure)
    {
        var pushConfigurator = new PushOutboxKitConfigurator();
        configure(pushConfigurator);
        configurator.WithPush(pushConfigurator);
        return configurator;
    }

    public static IOutboxKitConfigurator WithMongoDbPush(
        this IOutboxKitConfigurator configurator,
        string key,
        Action<IMongoDbPushOutboxKitConfigurator> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, nameof(key));
        var pushConfigurator = new PushOutboxKitConfigurator();
        configure(pushConfigurator);
        configurator.WithPush(key, pushConfigurator);
        return configurator;
    }
}

public interface IMongoDbPushOutboxKitConfigurator
{
    IMongoDbPushOutboxKitConfigurator WithConnectionString(string connectionString);

    IMongoDbPushOutboxKitConfigurator WithDatabaseName(string databaseName);

    IMongoDbPushOutboxKitConfigurator WithBatchSize(int batchSize);
}

internal sealed class PushOutboxKitConfigurator : IPushOutboxKitConfigurator, IMongoDbPushOutboxKitConfigurator
{
    private string? _connectionString;
    private string? _databaseName;
    private MongoDbPushSettings _mongoPushSettings = new();

    public IMongoDbPushOutboxKitConfigurator WithConnectionString(string connectionString)
    {
        _connectionString = connectionString;
        return this;
    }

    public IMongoDbPushOutboxKitConfigurator WithDatabaseName(string databaseName)
    {
        _databaseName = databaseName;
        return this;
    }

    public IMongoDbPushOutboxKitConfigurator WithBatchSize(int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be greater than zero");
        }

        _mongoPushSettings = _mongoPushSettings with { BatchSize = batchSize };
        return this;
    }

    public void ConfigureServices(string key, IServiceCollection services)
    {
        if (_connectionString is null)
        {
            throw new InvalidOperationException($"Connection string must be set for MySql polling with key \"{key}\"");
        }

        var client = new MongoClient(_connectionString);

        services.AddSingleton<IHostedService>(
            s => new PushBackgroundService(key, s, s.GetRequiredService<ILogger<PushBackgroundService>>()));
        services.AddKeyedSingleton(key, client);
        services.AddKeyedSingleton(key, client.GetDatabase(_databaseName));
        services.AddKeyedSingleton(
            key,
            (s, _) => new Producer(
                key,
                s,
                _mongoPushSettings,
                s.GetRequiredService<TimeProvider>(),
                s.GetRequiredService<ILogger<Producer>>()));
        services.AddKeyedSingleton(key, (s, _) => new DistributedLockThingy(
            s.GetRequiredKeyedService<DistributedLockSettings>(key),
            s.GetRequiredKeyedService<IMongoDatabase>(key),
            s.GetRequiredService<TimeProvider>(),
            s.GetRequiredService<ILogger<DistributedLockThingy>>()));
        services.AddKeyedSingleton(key, new DistributedLockSettings { ChangeStreamsEnabled = true });

        services.AddKeyedSingleton(key, _mongoPushSettings);
    }
}

internal sealed record MongoDbPushSettings
{
    public int BatchSize { get; init; } = 100;
}