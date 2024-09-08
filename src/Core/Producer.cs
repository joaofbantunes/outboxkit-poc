// --------------------------------------------------------------------------------------------------------------------
// <copyright company="Openvia">
//     Copyright (c) Openvia. All rights reserved.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

using System.Data;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace YakShaveFx.OutboxKit.Core;

public interface IOutboxBatchFetcher
{
    Task<IOutboxBatchContext> FetchAndHoldAsync(int size, CancellationToken ct);
    Task<bool> IsNewAvailableAsync(CancellationToken ct);
}

public interface IOutboxBatchContext
{
    /// <summary>
    /// The batch messages.
    /// </summary>
    IReadOnlyCollection<IMessage> Messages { get; }
    
    /// <summary>
    /// <para>Completes the batch.</para>
    /// <para>Messages that are not included in the <paramref name="ok"/> should be assumed to not have been produced, so should be kept in the outbox for posterior retry.</para>
    /// </summary>
    /// <param name="ok">The messages that were successfully produced.</param>
    /// <param name="ct">The async cancellation token.</param>
    /// <returns>The task representing the asynchronous operation</returns>
    Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct);
}

internal sealed class Producer(
    OutboxSettings settings,
    IServiceScopeFactory serviceScopeFactory,
    ILogger<Producer> logger) : IOutboxProducer
{
    private readonly int _maxBatchSize = settings.MaxBatchSize;

    public async Task ProducePendingAsync(CancellationToken ct)
    {
        // Invokes ProduceBatchAsync while batches are being produce, to exhaust all pending messages.

        // ReSharper disable once EmptyEmbeddedStatement - the logic is part of the method invoked in the condition 
        while (!ct.IsCancellationRequested && await ProduceBatchAsync(ct));
    }

    // returns true if there is a new batch to produce, false otherwise
    private async Task<bool> ProduceBatchAsync(CancellationToken ct)
    {
        using var activity = StartActivity("produce outbox message batch");
        using var scope = serviceScopeFactory.CreateScope();
        var batchFetcher = scope.ServiceProvider.GetRequiredService<IOutboxBatchFetcher>();
        var targetProducerProvider = scope.ServiceProvider.GetRequiredService<ITargetProducerProvider>();

        var batchContext = await batchFetcher.FetchAndHoldAsync(_maxBatchSize, ct);
        
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

            return await batchFetcher.IsNewAvailableAsync(ct);
        }
        catch (Exception)
        {
            // try to ack any messages already produced
            // not passing the actual cancellation token to try to complete the batch even if the application is shutting down
            await batchContext.CompleteAsync(ok,  CancellationToken.None);
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