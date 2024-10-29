namespace YakShaveFx.OutboxKit.MongoDb.Synchronization;

internal sealed class DistributedLockSettings
{
    public string CollectionName { get; init; } = "outbox_locks"; 
    public bool ChangeStreamsEnabled { get; init; } = true;
}