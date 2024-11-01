using Dapper;
using MySqlConnector;
using Testcontainers.MySql;
using Xunit.Abstractions;
using Xunit.Sdk;
using System.Text.Json;

namespace YakShaveFx.OutboxKit.MySql.Tests;

// in xunit 3, we'll be able to use assembly fixtures to share the container across all tests
// until then, we'll have to use a collection fixture (though this means the tests don't run in parallel)

[CollectionDefinition(Name)]
public sealed class MySqlCollection : ICollectionFixture<MySqlFixture>
{
    public const string Name = "MySQL collection";

    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}

public sealed class MySqlFixture(IMessageSink diagnosticMessageSink) : IAsyncLifetime
{
    private readonly MySqlContainer _container = new MySqlBuilder()
        .WithImage("mysql:8.0")
        .WithUsername("root")
        .WithPassword("root")
        .Build();

    public async Task<DatabaseContext> SetupDatabaseContextAsync()
    {
        var connectionString = _container.GetConnectionString();
        var databaseName = $"test_{Guid.NewGuid():N}";
        await using var c = new MySqlConnection(connectionString);
        await c.OpenAsync();
        await using var createDbCommand = new MySqlCommand($"CREATE DATABASE {databaseName};", c);
        await createDbCommand.ExecuteNonQueryAsync();
        return new DatabaseContext(connectionString, databaseName, diagnosticMessageSink);
    }

    public Task InitializeAsync() => _container.StartAsync();

    public async Task DisposeAsync() => await _container.DisposeAsync();

    public sealed class DatabaseContext(
        string originalConnectionString,
        string databaseName,
        IMessageSink diagnosticMessageSink) : IDatabaseContext
    {
        public MySqlDataSource DataSource { get; } = new(new MySqlConnectionStringBuilder(originalConnectionString)
        {
            Database = databaseName,
        }.ConnectionString);

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            try
            {
                await using var c = new MySqlConnection(originalConnectionString);
                await c.OpenAsync();
                var command = new MySqlCommand($"DROP DATABASE {databaseName};", c);
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                diagnosticMessageSink.OnMessage(
                    new DiagnosticMessage($"Failed to drop database {databaseName}: {e.Message}"));
            }
        }
    }
}

public interface IDatabaseContext : IAsyncDisposable
{
    MySqlDataSource DataSource { get; }
}

public static class CenasExtensions
{
    public static async Task SetupDatabaseWithDefaultSettingsAsync(this MySqlDataSource dataSource, int seedCount = 10)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new MySqlCommand(
            // lang=mysql
            """
            create table if not exists outbox_messages
            (
                id                    bigint auto_increment primary key,
                target                varchar(128) not null,
                type                  varchar(128) not null,
                payload               longblob     not null,
                created_at            datetime(6)  not null,
                observability_context longblob     null
            );
            """, connection);
        await command.ExecuteNonQueryAsync();
        
        await SeedAsync(dataSource, seedCount);
    }

    private static async Task SeedAsync(MySqlDataSource dataSource, int seedCount)
    {
        if(seedCount == 0) return;
        
        var messages = Enumerable.Range(0, seedCount).Select(i => new Message(
            0,
            "some-target",
            "some-type",
            JsonSerializer.SerializeToUtf8Bytes($"payload{i}"),
            DateTime.UtcNow,
            null
        ));
        await using var connection = await dataSource.OpenConnectionAsync();
        await connection.ExecuteAsync(
            // lang=mysql
            "INSERT INTO outbox_messages (target, type, payload, created_at, observability_context) VALUES (@Target, @Type, @Payload, @CreatedAt, @ObservabilityContext);",
            messages);
    }
}