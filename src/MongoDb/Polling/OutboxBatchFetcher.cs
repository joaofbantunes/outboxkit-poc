using MongoDB.Driver;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.Polling;
using YakShaveFx.OutboxKit.MongoDb.Synchronization;

namespace YakShaveFx.OutboxKit.MongoDb.Polling;

// ReSharper disable once ClassNeverInstantiated.Global - automagically instantiated by DI
internal sealed class OutboxBatchFetcher(
    MongoDbPollingSettings pollingSettings,
    IMongoDatabase database,
    DistributedLockThingy lockThingy)
    : IOutboxBatchFetcher
{
    private readonly int _batchSize = pollingSettings.BatchSize;

    private readonly IMongoCollection<Message> _collection =
        database.GetCollection<Message>("outbox_messages"); // TODO: make name configurable

    public async Task<IOutboxBatchContext> FetchAndHoldAsync(CancellationToken ct)
    {
        var @lock = await lockThingy.TryAcquireAsync(_ => Task.CompletedTask, ct);
        // if we can't acquire, we can assume that another instance is already processing the outbox, so we can just return
        if (@lock is null) return EmptyBatchContext.Instance;
        try
        {
            var messages = await FetchMessagesAsync(ct);

            if (messages.Count == 0)
            {
                await @lock.DisposeAsync();
                return EmptyBatchContext.Instance;
            }

            return new BatchContext(messages, _collection, @lock);
        }
        catch (Exception)
        {
            await @lock.DisposeAsync();
            throw;
        }
    }

    private Task<List<Message>> FetchMessagesAsync(CancellationToken ct)
        => _collection.Find(static _ => true).SortBy(static m => m.Id).Limit(_batchSize).ToListAsync(ct);

    private class BatchContext(
        IReadOnlyCollection<IMessage> messages,
        IMongoCollection<Message> collection,
        IDistributedLock @lock) : IOutboxBatchContext
    {
        public IReadOnlyCollection<IMessage> Messages => messages;

        public async Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct)
        {
            if (ok.Count > 0)
            {
                var ids = ok.Cast<Message>().Select(m => m.Id);
                await collection.DeleteManyAsync(m => ids.Contains(m.Id), ct);
            }
        }

        public Task<bool> HasNextAsync(CancellationToken ct) => collection.Find(static _ => true).Limit(1).AnyAsync(ct);

        public ValueTask DisposeAsync() => @lock.DisposeAsync();
    }
}