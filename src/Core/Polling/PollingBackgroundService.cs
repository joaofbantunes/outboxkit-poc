using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace YakShaveFx.OutboxKit.Core.Polling;

internal sealed class PollingBackgroundService(
    string key,
    PollingSettings settings,
    IKeyedOutboxListener listener,
    Producer producer,
    TimeProvider timeProvider,
    ILogger<PollingBackgroundService> logger) : BackgroundService
{
    private readonly TimeSpan _pollingInterval = settings.PollingInterval;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield(); // just to let the startup continue, without waiting on the outbox

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                try
                {
                    await producer.ProducePendingAsync(key, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // expected when the service is stopping, let it stop gracefully
                    continue;
                }
                catch (Exception ex)
                {
                    // We don't want the background service to stop while the application continues, so catching and logging.
                    // Could eventually be improved by checking the reason for the exception and acting accordingly.
                    logger.LogError(ex, "Unexpected error while producing pending outbox messages");
                }

                // to avoid letting the delays running in the background, wasting resources
                // we create a linked token, to cancel them
                using var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

                var listenerTask = listener.WaitForMessagesAsync(key, linkedTokenSource.Token);
                var delayTask = Task.Delay(_pollingInterval, timeProvider, linkedTokenSource.Token);

                // wait for whatever occurs first:
                // - being notified of new messages added to the outbox
                // - poll the outbox every x amount of time, for example, in cases where another instance of the service persisted
                //   something but didn't produce it, or some error occurred when producing and there are pending messages
                await Task.WhenAny(
                    listenerTask,
                    delayTask);

                await linkedTokenSource.CancelAsync();
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // expected when the service is stopping, let it stop gracefully
            }
        }
    }
}