using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using YakShaveFx.OutboxKit.Core.OpenTelemetry;

namespace YakShaveFx.OutboxKit.Core.Polling;

internal interface IProducer
{
    Task ProducePendingAsync(string key, CancellationToken ct);
}

internal sealed class Producer(IServiceScopeFactory serviceScopeFactory) : IProducer
{
    public async Task ProducePendingAsync(string key, CancellationToken ct)
    {
        // Invokes ProduceBatchAsync while batches are being produce, to exhaust all pending messages.

        // ReSharper disable once EmptyEmbeddedStatement - the logic is part of the method invoked in the condition 
        while (!ct.IsCancellationRequested && await ProduceBatchAsync(key, ct)) ;
    }

    // returns true if there is a new batch to produce, false otherwise
    private async Task<bool> ProduceBatchAsync(string key, CancellationToken ct)
    {
        using var activity = StartActivity("produce outbox message batch", key);
        using var scope = serviceScopeFactory.CreateScope();
        var batchFetcher = scope.ServiceProvider.GetRequiredKeyedService<IOutboxBatchFetcher>(key);

        await using var batchContext = await batchFetcher.FetchAndHoldAsync(ct);

        var messages = batchContext.Messages;
        activity?.SetTag(ActivityConstants.OutboxBatchSizeTag, messages.Count);

        // if we got not messages, there either aren't messages available or are being produced concurrently
        // in either case, we can break the loop
        if (messages.Count <= 0) return false;

        var result = await ProduceBatchAsync(scope, messages, ct);

        BatchProduced(scope, key, messages.Count == result.Ok.Count);
        
        // messages already produced, try to ack them
        // not passing the actual cancellation token to try to complete the batch even if the application is shutting down
        await batchContext.CompleteAsync(result.Ok, CancellationToken.None);

        return await batchContext.HasNextAsync(ct);
    }

    private static Task<BatchProduceResult> ProduceBatchAsync(
        IServiceScope scope,
        IReadOnlyCollection<IMessage> messages,
        CancellationToken ct)
    {
        var batchProducer = scope.ServiceProvider.GetRequiredService<IBatchProducerProvider>().Get();
        return batchProducer.ProduceAsync(messages, ct);
    }

    private static void BatchProduced(IServiceScope scope, string key, bool allMessagesProduced)
    {
        var metrics = scope.ServiceProvider.GetRequiredService<ProducerMetrics>();
        metrics.BatchProduced(key, allMessagesProduced);
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
}