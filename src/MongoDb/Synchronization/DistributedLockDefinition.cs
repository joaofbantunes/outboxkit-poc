namespace YakShaveFx.OutboxKit.MongoDb.Synchronization;

internal sealed class DistributedLockDefinition
{
    public required string Id { get; init; }
    public required string Owner { get; init; }
    public TimeSpan Duration { get; init; } = TimeSpan.FromMinutes(5);
    public Func<Task> OnLockLost { get; init; } = () => Task.CompletedTask;
}