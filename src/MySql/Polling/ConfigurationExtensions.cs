using System.Linq.Expressions;
using System.Reflection;
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

    IMySqlPollingOutboxKitConfigurator WithTable(Action<IMySqlPollingOutboxTableConfigurator> configure);
}

public interface IMySqlPollingOutboxTableConfigurator
{
    IMySqlPollingOutboxTableConfigurator WithTableName(string tableName);

    IMySqlPollingOutboxTableConfigurator WithColumnName<TProperty>(
        Expression<Func<Message, TProperty>> propertyExpression, string columnName);
}

internal sealed class PollingOutboxKitConfigurator : IPollingOutboxKitConfigurator, IMySqlPollingOutboxKitConfigurator
{
    private readonly MySqlPollingOutboxTableConfigurator _tableConfigurator = new();
    private string? _connectionString;


    public IMySqlPollingOutboxKitConfigurator WithConnectionString(string connectionString)
    {
        _connectionString = connectionString;
        return this;
    }

    public IMySqlPollingOutboxKitConfigurator WithTable(Action<IMySqlPollingOutboxTableConfigurator> configure)
    {
        configure(_tableConfigurator);
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
            .AddKeyedSingleton<IOutboxBatchFetcher>(
                key,
                (s, _) => new OutboxBatchFetcher(s, key, _tableConfigurator.BuildConfiguration()));
    }
}

internal sealed class MySqlPollingOutboxTableConfigurator : IMySqlPollingOutboxTableConfigurator
{
    private string _tableName = "outbox_messages";
    private readonly Dictionary<string, string> _columnNameMappings = new()
    {
        [nameof(Message.Id)] = "id",
        [nameof(Message.Target)] = "target",
        [nameof(Message.Type)] = "type",
        [nameof(Message.Payload)] = "payload",
        [nameof(Message.CreatedAt)] = "created_at",
        [nameof(Message.ObservabilityContext)] = "observability_context"
    };

    public TableConfiguration BuildConfiguration() => new(_tableName, _columnNameMappings);

    public IMySqlPollingOutboxTableConfigurator WithTableName(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));
        _tableName = tableName;
        return this;
    }

    public IMySqlPollingOutboxTableConfigurator WithColumnName<TProperty>(
        Expression<Func<Message, TProperty>> propertyExpression,
        string columnName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(columnName, nameof(columnName));
        if (propertyExpression.Body is MemberExpression { Member: PropertyInfo propertyInfo })
        {
            _columnNameMappings[propertyInfo.Name] = columnName;
        }
        else
        {
            throw new ArgumentException(
                "Property expression must be a member expression selecting a property",
                nameof(propertyExpression));
        }

        return this;
    }
}

internal sealed record TableConfiguration(string TableName, IReadOnlyDictionary<string, string> ColumnNameMappings);