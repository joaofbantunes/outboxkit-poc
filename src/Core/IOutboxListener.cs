namespace YakShaveFx.OutboxKit.Core;

public interface IOutboxListener
{
    Task WaitForMessagesAsync(CancellationToken ct);
}