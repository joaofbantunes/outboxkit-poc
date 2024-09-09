using System.Data;
using Dapper;
using MySqlConnector;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.Polling;

namespace DapperMySqlSample;

public record OutboxMessage(
    long Id,
    string Target,
    string Type,
    string Payload,
    string? ObservabilityContext,
    DateTime? ProducedAt) : IMessage;

internal sealed class FakeTargetProducer(ILogger<FakeTargetProducer> logger) : ITargetProducer
{
    public Task<ProduceResult> ProduceAsync(IEnumerable<IMessage> messages, CancellationToken ct)
    {
        var x = messages.ToList();
        logger.LogInformation("Producing {Count} messages", x.Count);
        return Task.FromResult(new ProduceResult(x));
    }
}

internal sealed class OutboxBatchFetcher(MySqlDataSource dataSource, TimeProvider timeProvider) : IOutboxBatchFetcher
{
    private const int BatchSize = 100;

    public async Task<IOutboxBatchContext> FetchAndHoldAsync(CancellationToken ct)
    {
        var connection = await dataSource.OpenConnectionAsync(ct);
        try
        {
            await connection.OpenAsync(ct);
            var tx = await connection.BeginTransactionAsync(IsolationLevel.Serializable, ct);
            var messages = await FetchMessagesAsync(connection, BatchSize, ct);

            if (messages.Length == 0)
            {
                await tx.RollbackAsync(ct);
                await connection.DisposeAsync();
                return EmptyBatchContext.Instance;
            }

            var now = timeProvider.GetUtcNow().DateTime;
            await SetProducedAtAsync(connection, messages.Select(m => m.Id), now, ct);

            return new BatchContext(messages, connection, tx);
        }
        catch (Exception)
        {
            await connection.DisposeAsync();
            throw;
        }

        static async Task<OutboxMessage[]> FetchMessagesAsync(MySqlConnection connection, int size,
            CancellationToken ct)
        {
            var command = new CommandDefinition(
                "SELECT Id, Target, Type, Payload, ObservabilityContext, ProducedAt FROM outbox_messages LIMIT @Size",
                new { Size = size },
                cancellationToken: ct);
            return (await connection.QueryAsync<OutboxMessage>(command)).ToArray();
        }

        static async Task SetProducedAtAsync(MySqlConnection connection, IEnumerable<long> ids, DateTime now,
            CancellationToken ct)
        {
            var command = new CommandDefinition(
                "UPDATE outbox_messages SET ProducedAt = @Now WHERE Id IN @Ids",
                new { Now = now, Ids = ids },
                cancellationToken: ct);
            await connection.ExecuteAsync(command);
        }
    }

    public async Task<bool> IsNewAvailableAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var command = new CommandDefinition(
            "SELECT COUNT(*) > 0 FROM outbox_messages WHERE ProducedAt IS NULL LIMIT 1",
            cancellationToken: ct);
        return await connection.ExecuteScalarAsync<bool>(command);
    }

    private class BatchContext(
        IReadOnlyCollection<IMessage> messages,
        MySqlConnection connection,
        MySqlTransaction tx) : IOutboxBatchContext
    {
        public IReadOnlyCollection<IMessage> Messages => messages;

        public async Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct)
        {
            if (ok.Count == messages.Count)
            {
                await tx.CommitAsync(ct);
            }
            else if (ok.Count == 0)
            {
                await tx.RollbackAsync(ct);
            }
            else
            {
                var ids = messages.Except(ok).Cast<OutboxMessage>().Select(m => m.Id);
                await connection.ExecuteAsync(
                    "UPDATE outbox_messages SET ProducedAt = NULL WHERE Id IN @Ids",
                    new { Ids = ids });
                await tx.CommitAsync(ct);
            }
        }

        public ValueTask DisposeAsync() => connection.DisposeAsync();
    }

    private sealed class EmptyBatchContext : IOutboxBatchContext
    {
        public static EmptyBatchContext Instance { get; } = new();
        public IReadOnlyCollection<IMessage> Messages => Array.Empty<IMessage>();
        public Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}