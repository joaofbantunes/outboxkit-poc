using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace YakShaveFx.OutboxKit.MongoDb.Synchronization;

internal sealed partial class DistributedLockThingy(string key, IServiceProvider services)
{
    private readonly IMongoCollection<DistributedLockDocument> _collection = GetCollection(key, services);
    private readonly DistributedLockSettings _settings = services.GetRequiredKeyedService<DistributedLockSettings>(key);

    public async Task<IDistributedLock> AcquireAsync(
        Func<DistributedLock, Task> onLockLost,
        CancellationToken ct)
    {
        var @lock = new DistributedLock(
            key,
            _settings.ChangeStreamsEnabled,
            onLockLost,
            services.GetRequiredService<TimeProvider>(),
            _collection,
            services.GetRequiredService<ILogger<DistributedLock>>());

        await @lock.AcquireAsync(ct);
        return @lock;
    }

    public async Task<IDistributedLock?> TryAcquireAsync(
        Func<DistributedLock, Task> onLockLost,
        CancellationToken ct)
    {
        var @lock = new DistributedLock(
            key,
            _settings.ChangeStreamsEnabled,
            onLockLost,
            services.GetRequiredService<TimeProvider>(),
            _collection,
            services.GetRequiredService<ILogger<DistributedLock>>());

        if (await @lock.TryAcquireAsync(ct))
        {
            return @lock;
        }

        await @lock.DisposeAsync();
        return null;
    }

    private static IMongoCollection<DistributedLockDocument> GetCollection(string key, IServiceProvider services)
    {
        var database = services.GetRequiredKeyedService<IMongoDatabase>(key);
        return database.GetCollection<DistributedLockDocument>("outbox_locks"); // TODO: make name configurable
    }

    internal sealed partial class DistributedLock(
        string key,
        bool changeStreamsEnabled,
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

        private bool _skipRelease;

        public async Task AcquireAsync(CancellationToken ct)
        {
            if (await InnerTryAcquireAsync(ct))
            {
                OnAcquired(ct);
                return;
            }

            await KeepTryingToAcquireAsync(ct);
            OnAcquired(ct);
        }

        public async Task<bool> TryAcquireAsync(CancellationToken ct)
        {
            var acquired = await InnerTryAcquireAsync(ct);
            if (acquired) OnAcquired(ct);
            _skipRelease = !acquired;
            return acquired;
        }

        public async ValueTask DisposeAsync()
        {
            // try to release the lock, so others can acquire it before expiration
            
            if (_skipRelease) return;
            
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

        private async Task<bool> InnerTryAcquireAsync(CancellationToken ct)
        {
            try
            {
                _ = await collection.ReplaceOneAsync(
                    GetUpsertFilter(),
                    _baseDocument with { ExpiresAt = GetExpiresAt() },
                    new ReplaceOptions { IsUpsert = true },
                    ct);

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
            if (!changeStreamsEnabled)
            {
                await PollAndKeepTryingToAcquireAsync(ct);
                return;
            }

            using var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var linkedCt = linkedTokenSource.Token;

            var pollingTask = PollAndKeepTryingToAcquireAsync(linkedCt);
            var changeStreamTask = WatchAndKeepTryingToAcquireAsync(linkedCt);

            // TODO: check if there are some exceptions worth handling here
            await Task.WhenAny(changeStreamTask, pollingTask);
            await linkedTokenSource.CancelAsync();
        }

        private async Task PollAndKeepTryingToAcquireAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                var current = await collection
                    .Find(Builders<DistributedLockDocument>.Filter.Eq(d => d.Id, _baseDocument.Id))
                    .FirstOrDefaultAsync(ct);
                var remaining = GetRemainingTime(current?.ExpiresAt ?? 0);
                if (remaining > TimeSpan.Zero)
                {
                    await Task.Delay(remaining, timeProvider, ct);
                }

                if (await InnerTryAcquireAsync(ct)) return;
            }
        }

        private async Task WatchAndKeepTryingToAcquireAsync(CancellationToken ct)
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
                ct);

            while (!ct.IsCancellationRequested && await cursor.MoveNextAsync(ct))
            {
                foreach (var _ in cursor.Current)
                {
                    if (await InnerTryAcquireAsync(ct)) return;
                }
            }
        }

        private void KickoffKeepAlive(CancellationToken ct)
        {
            _ = Task.Run(async () =>
            {
                if (!changeStreamsEnabled)
                {
                    while (!ct.IsCancellationRequested)
                    {
                        await Task.Delay(KeepAliveInterval, timeProvider, ct);
                        if (!await InnerTryAcquireAsync(ct))
                        {
                            OnLost();
                            return;
                        }

                        LogExtended(logger, key);
                    }
                }
                else
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        var delayTask = Task.Delay(KeepAliveInterval, timeProvider, linkedTokenSource.Token);
                        var watchLockLossTask = WatchForPotentialLockLossAsync(linkedTokenSource.Token);
                        await Task.WhenAny(delayTask, watchLockLossTask);

                        await linkedTokenSource.CancelAsync();

                        if (!await InnerTryAcquireAsync(ct))
                        {
                            OnLost();
                            return;
                        }

                        if (watchLockLossTask.IsCompletedSuccessfully) LogReacquired(logger, key);
                        else LogExtended(logger, key);
                    }
                }
            }, ct);
        }

        private async Task WatchForPotentialLockLossAsync(CancellationToken ct)
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
                if (cursor.Current.Any())
                {
                    LogPotentiallyLost(logger, key);
                    return;
                }
            }
        }

        private void OnAcquired(CancellationToken ct)
        {
            LogAcquired(logger, key);
            KickoffKeepAlive(ct);
        }

        private void OnLost()
        {
            LogLost(logger, key);
            _ = Task.Run(() => onLockLost(this));
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

        [LoggerMessage(LogLevel.Debug, Message = "Producer lock ownership extended for outbox with key \"{key}\"")]
        private static partial void LogExtended(ILogger logger, string key);

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