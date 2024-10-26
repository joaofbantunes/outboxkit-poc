using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.MongoDb.OpenTelemetry;
using WatchPipelineDefinition =
    MongoDB.Driver.PipelineDefinition<
        MongoDB.Driver.ChangeStreamDocument<YakShaveFx.OutboxKit.MongoDb.Message>,
        MongoDB.Driver.ChangeStreamDocument<YakShaveFx.OutboxKit.MongoDb.Message>>;

namespace YakShaveFx.OutboxKit.MongoDb.Push;

// TODO can some of this be moved into core?
// doesn't look like it, but it's worth checking

internal sealed partial class Producer(
    string key,
    IServiceProvider services,
    MongoDbPushSettings settings,
    TimeProvider timeProvider,
    ILogger<Producer> logger)
{
    private static readonly WatchPipelineDefinition WatchPipelineDefinition
        = PipelineDefinitionBuilder
            .For<ChangeStreamDocument<Message>>()
            .Match(d => d.OperationType == ChangeStreamOperationType.Insert);

    private static readonly TimeSpan MaxWaitTime = TimeSpan.FromMinutes(5); // TODO: make configurable?

    private readonly int _batchSize = settings.BatchSize;
    private readonly IMongoCollection<Message> _collection = GetCollection(key, services);

    private DateTimeOffset _lastFetch;

    public async Task WatchAndProduceAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await ProducePendingAsync(ct);

            await WaitForMessagesAsync(ct);
        }
    }

    private async Task ProducePendingAsync(CancellationToken ct)
    {
        while (await HasNextAsync(ct))
        {
            using var batchActivity = StartActivity("produce outbox message batch", key);
            using var scope = services.CreateScope();
            var targetProducerProvider = scope.ServiceProvider.GetRequiredService<ITargetProducerProvider>();

            var messages = await FetchBatchAsync(ct);

            batchActivity?.SetTag(ActivityConstants.OutboxBatchSizeTag, messages.Count);

            if (messages.Count == 0) return;

            var ok = new List<Message>(messages.Count);

            try
            {
                var messagesByTarget = messages.GroupBy(m => m.Target);

                foreach (var targetMessages in messagesByTarget)
                {
                    var targetProducer = targetProducerProvider.Get(targetMessages.Key);
                    var result = await targetProducer.ProduceAsync(targetMessages, ct);
                    ok.AddRange(result.Ok.Cast<Message>());
                }
            }
            finally
            {
                // try to ack any messages already produced
                // not passing the actual cancellation token to try to complete the batch even if the application is shutting down
                await AckAsync(ok, CancellationToken.None);
            }
        }
    }


    // we're not actually publishing things as they come in,
    // as it doesn't feel like the more reliable thing.
    // so we're using the change stream just to trigger fetching
    private async Task WaitForMessagesAsync(CancellationToken ct)
    {
        // note that, because the timestamp resolution is in seconds,
        // we might get the change stream triggered multiple times
        // by the same messages until we get to the next second
        // (or we could add a Task.Delay before returning from this method, but it's a bit meh 😅)
        var waitFrom = _lastFetch;

        LogWaiting(logger, key, waitFrom);

        using var cursor = await _collection.WatchAsync(
            WatchPipelineDefinition,
            new ChangeStreamOptions
            {
                BatchSize = 1,
                MaxAwaitTime = MaxWaitTime,
                StartAtOperationTime = new BsonTimestamp(
                    // 🧨casting to int because that's what the constructor expects,
                    // but it'll cause problems in 2038 (probably this lib will be long gone by then. so 🤷‍♂️)
                    // maybe there's a smarter way to do this, but didn't figure it out yet
                    (int)waitFrom.ToUnixTimeSeconds(),
                    1)
            },
            ct);

        /*
         * As we're setting a large(ish) MaxAwaitTime, we can just return when it elapses,
         * so it'll trigger the next iteration of the background service, catching any,
         * unlikely but not impossible, undetected new messages (due to possible race conditions)
         * and ending up here again to wait for the next batch.
         *
         * One important thing  to note is that it seems like the current implementation
         * always returns immediately on the first MoveNextAsync call, even if there are no messages.
         * That's the reason the following code is more complex than just awaiting on a MoveNextAsync call and returning.
         */

        _ = await cursor.MoveNextAsync(ct);
        if (cursor.Current.Any()) return;
        _ = await cursor.MoveNextAsync(ct);
    }


    private Task<bool> HasNextAsync(CancellationToken ct)
    {
        _lastFetch = timeProvider.GetUtcNow();
        return _collection.Find(static _ => true).Limit(1).AnyAsync(ct);
    }

    private Task<List<Message>> FetchBatchAsync(CancellationToken ct)
    {
        _lastFetch = timeProvider.GetUtcNow();
        return _collection.Find(static _ => true).SortBy(static m => m.Id).Limit(_batchSize).ToListAsync(ct);
    }

    private async Task AckAsync(
        IEnumerable<Message> messages,
        CancellationToken ct)
    {
        // TODO: check if not all messages were deleted?
        var ids = messages.Select(m => m.Id);
        await _collection.DeleteManyAsync(m => ids.Contains(m.Id), ct);
    }

    private static IMongoCollection<Message> GetCollection(string key, IServiceProvider services)
    {
        var database = services.GetRequiredKeyedService<IMongoDatabase>(key);
        return database.GetCollection<Message>("outbox_messages"); // TODO: make name configurable
    }

    private static Activity? StartActivity(string activityName, string key)
    {
        if (!ActivityHelpers.ActivitySource.HasListeners())
        {
            return null;
        }

        // ReSharper disable once ExplicitCallerInfoArgument
        return ActivityHelpers.ActivitySource.StartActivity(
            name: activityName,
            kind: ActivityKind.Internal,
            tags: [new(ActivityConstants.OutboxKeyTag, key)]);
    }

    [LoggerMessage(LogLevel.Debug,
        Message = "Waiting for messages for outbox with key \"{key}\", starting from {waitFrom:O}")]
    private static partial void LogWaiting(ILogger logger, string key, DateTimeOffset waitFrom);
}