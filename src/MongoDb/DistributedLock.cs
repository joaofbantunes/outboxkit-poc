using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace YakShaveFx.OutboxKit.MongoDb;

internal interface IDistributedLock : IAsyncDisposable;

internal sealed record DistributedLockDocument
{
    public required string Id { get; init; }
    public required string AcquiredBy { get; init; }
    public required long ExpiresAt { get; init; }
}

internal sealed partial class DistributedLockThingy(string key, IServiceProvider services)
{
    private readonly IMongoCollection<DistributedLockDocument> _collection = GetCollection(key, services);

    public async Task<IDistributedLock> AcquireAsync(
        Func<DistributedLock, Task> onLockLost,
        CancellationToken ct)
    {
        var @lock = new DistributedLock(
            key,
            onLockLost,
            services.GetRequiredService<TimeProvider>(),
            _collection,
            services.GetRequiredService<ILogger<DistributedLock>>());

        await @lock.AcquireAsync(ct);
        return @lock;
    }

    private static IMongoCollection<DistributedLockDocument> GetCollection(string key, IServiceProvider services)
    {
        var database = services.GetRequiredKeyedService<IMongoDatabase>(key);
        return database.GetCollection<DistributedLockDocument>("outbox_locks"); // TODO: make name configurable
    }

    internal sealed partial class DistributedLock(
        string key,
        Func<DistributedLock, Task> onLockLost,
        TimeProvider timeProvider,
        IMongoCollection<DistributedLockDocument> collection,
        ILogger<DistributedLock> logger)
        : IDistributedLock
    {
        private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(5); // TODO: make configurable?
        private static readonly TimeSpan KeepAliveInterval = LockDuration / 2; // TODO: make configurable?

        private static readonly TimeSpan
            MaxLockAcquirePollInterval = TimeSpan.FromMinutes(5); // TODO: make configurable?

        // TODO: make configurable?
        private readonly DistributedLockDocument _baseDocument = new()
        {
            Id = "outbox_lock",
            AcquiredBy = Environment.MachineName,
            ExpiresAt = 0
        };


        public async Task AcquireAsync(CancellationToken ct)
        {
            if (await TryAcquireAsync(ct))
            {
                OnAcquired(ct);
                return;
            }

            await KeepTryingToAcquireAsync(ct);
            OnAcquired(ct);
        }

        public async ValueTask DisposeAsync()
        {
            // try to release the lock, so others can acquire it before expiration
            try
            {
                var result = await collection.DeleteOneAsync(
                    Builders<DistributedLockDocument>.Filter.And(
                        Builders<DistributedLockDocument>.Filter.Eq(d => d.Id, _baseDocument.Id),
                        Builders<DistributedLockDocument>.Filter.Eq(d => d.AcquiredBy, _baseDocument.AcquiredBy)));

                if (result.DeletedCount == 1)
                {
                    LogReleased(logger, key);
                }
            }
            catch (Exception ex)
            {
                logger.LogInformation(ex, "Failed to release lock for key {key}", key);
            }
        }

        private async Task<bool> TryAcquireAsync(CancellationToken ct)
        {
            try
            {
                _ = await collection.ReplaceOneAsync(
                    GetUpsertFilter(),
                    _baseDocument with { ExpiresAt = GetExpiresAt() },
                    new ReplaceOptions { IsUpsert = true },
                    ct);

                // _ = await collection.UpdateOneAsync(
                //     GetUpsertFilter(),
                //     Builders<DistributedLockDocument>.Update
                //         .Set(d => d.Id, _baseDocument.Id)
                //         .Set(d => d.AcquiredBy, _baseDocument.AcquiredBy)
                //         .Set(d => d.ExpiresAt, GetExpiresAt()),
                //     new UpdateOptions { IsUpsert = true },
                //     ct);

                return true;
            }
            // TODO: would be nice to be able to do this without exceptions, not sure it's possible though, needs investigation
            catch (MongoWriteException mwex) when (mwex.WriteError.Category == ServerErrorCategory.DuplicateKey)
            {
                return false;
            }
        }

        private FilterDefinition<DistributedLockDocument> GetUpsertFilter()
            => Builders<DistributedLockDocument>.Filter.Or(
                Builders<DistributedLockDocument>.Filter.And(
                    Builders<DistributedLockDocument>.Filter.Eq(d => d.Id, _baseDocument.Id),
                    Builders<DistributedLockDocument>.Filter.Eq(d => d.AcquiredBy, _baseDocument.AcquiredBy)),
                Builders<DistributedLockDocument>.Filter.And(
                    Builders<DistributedLockDocument>.Filter.Eq(d => d.Id, _baseDocument.Id),
                    Builders<DistributedLockDocument>.Filter.Lt(d => d.ExpiresAt, GetNow())));

        private async Task KeepTryingToAcquireAsync(CancellationToken ct)
        {
            // TODO: make change stream optional, so the lock can also be used in a polling approach

            while (!ct.IsCancellationRequested)
            {
                using var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var linkedCt = linkedTokenSource.Token;

                var changeStreamTask = Task.Run(async () =>
                {
                    using var cursor = await collection.WatchAsync(
                        PipelineDefinitionBuilder
                            .For<ChangeStreamDocument<DistributedLockDocument>>()
                            .Match(d =>
                                d.OperationType == ChangeStreamOperationType.Delete
                                && d.DocumentKey["_id"] == _baseDocument.Id),
                        new ChangeStreamOptions
                        {
                            BatchSize = 1,
                            MaxAwaitTime = TimeSpan.FromMinutes(5)
                        },
                        linkedCt);

                    while (!linkedCt.IsCancellationRequested && await cursor.MoveNextAsync(ct))
                    {
                        foreach (var _ in cursor.Current)
                        {
                            if (await TryAcquireAsync(linkedCt)) return true;
                        }
                    }

                    return false;
                }, linkedCt);

                var pollingTask = Task.Run(async () =>
                {
                    while (!linkedCt.IsCancellationRequested)
                    {
                        var current = await collection
                            .Find(Builders<DistributedLockDocument>.Filter.Eq(d => d.Id, _baseDocument.Id))
                            .FirstOrDefaultAsync(linkedCt);
                        var remaining = GetRemainingTime(current?.ExpiresAt ?? 0);
                        if (remaining > TimeSpan.Zero)
                        {
                            await Task.Delay(remaining, timeProvider, linkedCt);
                        }

                        if (await TryAcquireAsync(linkedCt)) return true;
                    }

                    return false;
                }, linkedCt);

                // TODO: check if there are some exceptions worth handling here

                var result = await Task.WhenAny(changeStreamTask, pollingTask);
                await linkedTokenSource.CancelAsync();
                if (result.Result) return;
            }
        }


        private void KickoffKeepAlive(CancellationToken ct)
        {
            // TODO: also add change stream subscription (optional) so we can react to lock loss faster
            _ = Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    var delayTask = Task.Delay(KeepAliveInterval, timeProvider, linkedTokenSource.Token);
                    var detectLockLossTask = DetectLockLossAsync(linkedTokenSource.Token);
                    await Task.WhenAny(delayTask, detectLockLossTask);

                    await linkedTokenSource.CancelAsync();

                    if (!ct.IsCancellationRequested && detectLockLossTask.IsCompletedSuccessfully)
                    {
                        LogPotentiallyLost(logger, key);
                    }

                    if (!await TryAcquireAsync(ct))
                    {
                        LogLost(logger, key);
                        await onLockLost(this);
                        return;
                    }

                    if (detectLockLossTask.IsCompletedSuccessfully)
                    {
                        LogReacquired(logger, key);
                    }
                }
            }, ct);
        }

        private async Task DetectLockLossAsync(CancellationToken ct)
        {
            using var cursor = await collection.WatchAsync(
                PipelineDefinitionBuilder
                    .For<ChangeStreamDocument<DistributedLockDocument>>()
                    .Match(d => d.DocumentKey["_id"] == _baseDocument.Id),
                new ChangeStreamOptions
                {
                    BatchSize = 1,
                    MaxAwaitTime = TimeSpan.FromMinutes(5)
                },
                ct);

            while (!ct.IsCancellationRequested && await cursor.MoveNextAsync(ct))
            {
                var value = cursor.Current.FirstOrDefault();
                if (value != null)
                {
                    return;
                }
            }
        }

        private void OnAcquired(CancellationToken ct)
        {
            LogAcquired(logger, key);
            KickoffKeepAlive(ct);
        }


        private long GetNow() => timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

        private long GetExpiresAt() => timeProvider.GetUtcNow().Add(LockDuration).ToUnixTimeMilliseconds();

        private TimeSpan GetRemainingTime(long expiresAt)
        {
            var remaining = expiresAt - timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            return remaining > 0
                ? TimeSpan.FromMilliseconds(Math.Min(remaining, MaxLockAcquirePollInterval.TotalMilliseconds))
                : TimeSpan.Zero;
        }

        [LoggerMessage(LogLevel.Debug, Message = "Producer lock acquired for outbox with key \"{key}\"")]
        private static partial void LogAcquired(ILogger logger, string key);

        [LoggerMessage(LogLevel.Debug, Message = "Producer lock released for outbox with key \"{key}\"")]
        private static partial void LogReleased(ILogger logger, string key);

        [LoggerMessage(LogLevel.Debug,
            Message = "Producer lock potentially lost for outbox with key \"{key}\", trying to reacquire")]
        private static partial void LogPotentiallyLost(ILogger logger, string key);
        
        [LoggerMessage(LogLevel.Debug, Message = "Producer lock reacquired for outbox with key \"{key}\"")]
        private static partial void LogReacquired(ILogger logger, string key);

        [LoggerMessage(LogLevel.Debug, Message = "Producer lock lost for outbox with key \"{key}\"")]
        private static partial void LogLost(ILogger logger, string key);
    }
}