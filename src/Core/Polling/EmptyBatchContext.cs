namespace YakShaveFx.OutboxKit.Core.Polling;

public sealed class EmptyBatchContext : IOutboxBatchContext
{
    public static EmptyBatchContext Instance { get; } = new();
    public IReadOnlyCollection<IMessage> Messages => Array.Empty<IMessage>();
    public bool HasNext => false;
    public Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}