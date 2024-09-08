namespace YakShaveFx.OutboxKit.Core.Polling;

public interface IOutboxListener
{
    Task WaitForMessagesAsync(CancellationToken ct);
}