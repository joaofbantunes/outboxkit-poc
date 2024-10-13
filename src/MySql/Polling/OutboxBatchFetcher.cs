using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.Polling;

namespace YakShaveFx.OutboxKit.MySql.Polling;

// TODO: make outbox_messages table name, schema and columns configurable

internal sealed class OutboxBatchFetcher(IServiceProvider services) : IOutboxBatchFetcher
{
    private const int BatchSize = 100;

    // TODO: the key should be configurable, to support multi-tenancy?
    private readonly MySqlDataSource _dataSource = services.GetRequiredKeyedService<MySqlDataSource>("outboxkit");

    public async Task<IOutboxBatchContext> FetchAndHoldAsync(CancellationToken ct)
    {
        var connection = await _dataSource.OpenConnectionAsync(ct);
        try
        {
            var tx = await connection.BeginTransactionAsync(ct);
            var messages = await FetchMessagesAsync(connection, tx, BatchSize + 1, ct);

            if (messages.Count == 0)
            {
                await tx.RollbackAsync(ct);
                await connection.DisposeAsync();
                return EmptyBatchContext.Instance;
            }
            
            var hasNext = messages.Count > BatchSize;
            if (hasNext)
            {
                messages.RemoveAt(BatchSize);
            }

            return new BatchContext(messages, hasNext, connection, tx);
        }
        catch (Exception)
        {
            await connection.DisposeAsync();
            throw;
        }

        static async Task<List<Message>> FetchMessagesAsync(MySqlConnection connection,
            MySqlTransaction tx,
            int size,
            CancellationToken ct)
        {
            await using var command = new MySqlCommand(
                // lang=mysql
                "SELECT id, target, type, payload, created_at, observability_context FROM outbox_messages LIMIT @size FOR UPDATE",
                connection,
                tx);
            
            command.Parameters.AddWithValue("size", size);

            await using var reader = await command.ExecuteReaderAsync(ct);
            
            if (!reader.HasRows) return [];

            var messages = new List<Message>(size);
            while (await reader.ReadAsync(ct))
            {
                messages.Add(new Message(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetFieldValue<byte[]>(3),
                    reader.GetDateTime(4),
                    reader.IsDBNull(5) ? null : reader.GetFieldValue<byte[]>(5)));
            }

            return messages;
        }
    }

    private class BatchContext(
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
                var idParams = string.Join(", ",Enumerable.Range(0, ok.Count).Select(i => $"@id{i}"));
                var command = new MySqlCommand(
                    // lang=mysql
                    $"DELETE FROM outbox_messages WHERE id IN ({idParams});",
                    connection,
                    tx);

                var i = 0;
                foreach (var m in ok.Cast<Message>())
                {
                    command.Parameters.AddWithValue($"id{i}", m.Id);
                    i++;
                }
                
                var deleted = await command.ExecuteNonQueryAsync(ct);
                
                if (deleted != ok.Count)
                {
                    // think if this is the best way to handle this (considering this shouldn't happen, probably it's good enough)
                    await tx.RollbackAsync(ct);
                    throw new InvalidOperationException("Failed to delete messages");
                }
                
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