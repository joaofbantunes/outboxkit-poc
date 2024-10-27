namespace YakShaveFx.OutboxKit.MongoDb.Synchronization;

internal sealed class DistributedLockSettings
{
    public bool ChangeStreamsEnabled { get; init; } = true;
}