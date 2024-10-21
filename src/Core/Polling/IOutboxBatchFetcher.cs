namespace YakShaveFx.OutboxKit.Core.Polling;

public interface IOutboxBatchFetcher
{
    Task<IOutboxBatchContext> FetchAndHoldAsync(CancellationToken ct);
}

public interface IOutboxBatchContext : IAsyncDisposable
{
    /// <summary>
    /// The batch messages.
    /// </summary>
    IReadOnlyCollection<IMessage> Messages { get; }

    /// <summary>
    /// Indicates if there are more messages to fetch.
    /// </summary>
    bool HasNext { get; }

    /// <summary>
    /// <para>Completes the batch.</para>
    /// <para>Messages that are not included in the <paramref name="ok"/> should be assumed to not have been produced, so should be kept in the outbox for posterior retry.</para>
    /// </summary>
    /// <param name="ok">The messages that were successfully produced.</param>
    /// <param name="ct">The async cancellation token.</param>
    /// <returns>The task representing the asynchronous operation</returns>
    Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct);
}

public sealed class EmptyBatchContext : IOutboxBatchContext
{
    private EmptyBatchContext()
    {
    }

    public static EmptyBatchContext Instance { get; } = new();
    public IReadOnlyCollection<IMessage> Messages => Array.Empty<IMessage>();
    public bool HasNext => false;
    public Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}