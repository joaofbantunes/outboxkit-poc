using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace YakShaveFx.OutboxKit.MongoDb.Push;

internal sealed partial class PushBackgroundService(
    string key,
    IServiceProvider services,
    ILogger<PushBackgroundService> logger) : BackgroundService
{
    private readonly Producer _producer = services.GetRequiredKeyedService<Producer>(key);
    private readonly Synchronization.DistributedLockThingy _lockThingy = services.GetRequiredKeyedService<Synchronization.DistributedLockThingy>(key);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogStarting(logger, key);

        await Task.Yield(); // just to let the startup continue, without waiting on the outbox

        while (!stoppingToken.IsCancellationRequested)
        {
            using var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            try
            {
                var ct = linkedTokenSource.Token;
                await using var @lock = await _lockThingy.AcquireAsync(
                    // if we unexpectedly lose the lock, we need to stop producing, to avoid duplicates as much as possible
                    _ => linkedTokenSource.CancelAsync(),
                    stoppingToken);

                await _producer.WatchAndProduceAsync(ct);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // expected when the service is stopping, let it stop gracefully
            }
            catch (OperationCanceledException) when (
                !stoppingToken.IsCancellationRequested
                && linkedTokenSource.IsCancellationRequested)
            {
                LogLockLost(logger, key);
            }
            catch (Exception ex)
            {
                // we don't want the background service to stop while the application continues, so catching and logging
                LogUnexpectedError(logger, key, ex);
            }
        }

        LogStopping(logger, key);
    }


    [LoggerMessage(LogLevel.Debug, Message = "Starting outbox push background service for key \"{key}\"")]
    private static partial void LogStarting(ILogger logger, string key);

    [LoggerMessage(LogLevel.Debug, Message = "Shutting down outbox push background service for key \"{key}\"")]
    private static partial void LogStopping(ILogger logger, string key);

    [LoggerMessage(LogLevel.Warning, Message = "Lock over outbox with key \"{key}\" unexpectedly lost")]
    private static partial void LogLockLost(ILogger logger, string key);

    [LoggerMessage(
        LogLevel.Error,
        Message = "Unexpected error during execution of outbox for key \"{key}\"")]
    private static partial void LogUnexpectedError(ILogger logger, string key, Exception ex);
}