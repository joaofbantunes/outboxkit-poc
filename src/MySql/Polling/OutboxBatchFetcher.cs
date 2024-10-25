using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.Polling;

namespace YakShaveFx.OutboxKit.MySql.Polling;

// ReSharper disable once ClassNeverInstantiated.Global - automagically instantiated by DI
internal sealed class OutboxBatchFetcher(
    MySqlPollingSettings pollingSettings,
    TableConfiguration tableConfig,
    MySqlDataSource dataSource)
    : IOutboxBatchFetcher
{
    private readonly int _batchSize = pollingSettings.BatchSize;

    private readonly string _selectQuery = $"""
                                            SELECT
                                                {tableConfig.ColumnNameMappings[nameof(Message.Id)]},
                                                {tableConfig.ColumnNameMappings[nameof(Message.Target)]},
                                                {tableConfig.ColumnNameMappings[nameof(Message.Type)]},
                                                {tableConfig.ColumnNameMappings[nameof(Message.Payload)]},
                                                {tableConfig.ColumnNameMappings[nameof(Message.CreatedAt)]},
                                                {tableConfig.ColumnNameMappings[nameof(Message.ObservabilityContext)]}
                                            FROM {tableConfig.TableName} LIMIT @size FOR UPDATE;
                                            """;

    private readonly string _deleteQuery = $"DELETE FROM {tableConfig.TableName} WHERE id IN ({{0}});";

    private readonly string _hasNextQuery = $"SELECT EXISTS(SELECT 1 FROM {tableConfig.TableName} LIMIT 1);";

    public async Task<IOutboxBatchContext> FetchAndHoldAsync(CancellationToken ct)
    {
        var connection = await dataSource.OpenConnectionAsync(ct);
        try
        {
            var tx = await connection.BeginTransactionAsync(ct);
            var messages = await FetchMessagesAsync(connection, tx, _batchSize, ct);

            if (messages.Count == 0)
            {
                await tx.RollbackAsync(ct);
                await connection.DisposeAsync();
                return EmptyBatchContext.Instance;
            }
            
            return new BatchContext(messages, connection, tx, _deleteQuery, _hasNextQuery);
        }
        catch (Exception)
        {
            await connection.DisposeAsync();
            throw;
        }
    }

// 3572
    private async Task<List<Message>> FetchMessagesAsync(
        MySqlConnection connection,
        MySqlTransaction tx,
        int size,
        CancellationToken ct)
    {
        await using var command = new MySqlCommand(
            _selectQuery,
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

    private class BatchContext(
        IReadOnlyCollection<IMessage> messages,
        MySqlConnection connection,
        MySqlTransaction tx,
        string deleteQuery,
        string hasNextQuery) : IOutboxBatchContext
    {
        public IReadOnlyCollection<IMessage> Messages => messages;

        public async Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct)
        {
            if (ok.Count > 0)
            {
                var idParams = string.Join(", ", Enumerable.Range(0, ok.Count).Select(i => $"@id{i}"));
                var command = new MySqlCommand(
                    string.Format(deleteQuery, idParams),
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

        public async Task<bool> HasNextAsync(CancellationToken ct)
        {
            var command = new MySqlCommand(hasNextQuery, connection);
            var result = await command.ExecuteScalarAsync(ct);
            return result switch
            {
                bool b => b,
                int i => i == 1,
                long l => l == 1,
                _ => false
            };
        }

        public ValueTask DisposeAsync() => connection.DisposeAsync();
    }
}