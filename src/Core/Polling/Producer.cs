using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace YakShaveFx.OutboxKit.Core.Polling;

internal sealed class Producer(IServiceScopeFactory serviceScopeFactory)
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
        using var activity = StartActivity("produce outbox message batch");
        using var scope = serviceScopeFactory.CreateScope();
        var batchFetcher = scope.ServiceProvider.GetRequiredKeyedService<IOutboxBatchFetcher>(key);
        var targetProducerProvider = scope.ServiceProvider.GetRequiredService<ITargetProducerProvider>();

        await using var batchContext = await batchFetcher.FetchAndHoldAsync(ct);

        var ok = new List<IMessage>(batchContext.Messages.Count);

        try
        {
            var messages = batchContext.Messages;

            // if we got not messages, there either aren't messages available or are being produced concurrently
            // in either case, we can break the loop
            if (messages.Count <= 0) return false;

            var messagesByTarget = messages.GroupBy(m => m.Target);

            foreach (var targetMessages in messagesByTarget)
            {
                var targetProducer = targetProducerProvider.Get(targetMessages.Key);
                var result = await targetProducer.ProduceAsync(targetMessages, ct);
                ok.AddRange(result.Ok);
            }

            // messages already produced, try to ack them
            // not passing the actual cancellation token to try to complete the batch even if the application is shutting down
            await batchContext.CompleteAsync(ok, CancellationToken.None);

            return batchContext.HasNext;
        }
        catch (Exception)
        {
            // try to ack any messages already produced
            // not passing the actual cancellation token to try to complete the batch even if the application is shutting down
            await batchContext.CompleteAsync(ok, CancellationToken.None);
            throw;
        }
    }

    private static Activity? StartActivity(string activityName)
    {
        if (!Observability.ActivitySource.HasListeners())
        {
            return null;
        }

        // ReSharper disable once ExplicitCallerInfoArgument
        return Observability.ActivitySource.StartActivity(name: activityName, kind: ActivityKind.Internal);
    }
}