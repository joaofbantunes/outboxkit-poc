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
            var tx = await connection.BeginTransactionAsync(ct);
            var messages = await FetchMessagesAsync(connection, BatchSize + 1, ct);

            if (messages.Length == 0)
            {
                await tx.RollbackAsync(ct);
                await connection.DisposeAsync();
                return EmptyBatchContext.Instance;
            }

            var hasNext = messages.Length > BatchSize;
            if (hasNext)
            {
                messages = messages.AsSpan(0, BatchSize).ToArray();
            }

            return new BatchContext(timeProvider, messages, hasNext, connection, tx);
        }
        catch (Exception)
        {
            await connection.DisposeAsync();
            throw;
        }

        static async Task<OutboxMessage[]> FetchMessagesAsync(
            MySqlConnection connection,
            int size,
            CancellationToken ct)
        {
            var command = new CommandDefinition(
                "SELECT Id, Target, Type, Payload, ObservabilityContext, ProducedAt FROM outbox_messages LIMIT @Size FOR UPDATE",
                new { Size = size },
                cancellationToken: ct);
            return (await connection.QueryAsync<OutboxMessage>(command)).ToArray();
        }
    }

    private class BatchContext(
        TimeProvider timeProvider,
        IReadOnlyCollection<IMessage> messages,
        bool hasNext,
        MySqlConnection connection,
        MySqlTransaction tx) : IOutboxBatchContext
    {
        public IReadOnlyCollection<IMessage> Messages => messages;

        public bool HasNext => hasNext;

        public async Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct)
        {
            if (ok.Count > 0)
            {
                await connection.ExecuteAsync(
                    "UPDATE outbox_messages SET ProducedAt = @Now WHERE Id IN @Ids",
                    new
                    {
                        Ids = ok.Cast<OutboxMessage>().Select(m => m.Id),
                        Now = timeProvider.GetUtcNow().DateTime
                    });
                await tx.CommitAsync(ct);
            }
            else
            {
                await tx.RollbackAsync(ct);
            }
        }

        public ValueTask DisposeAsync() => connection.DisposeAsync();
    }
}