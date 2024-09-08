namespace YakShaveFx.OutboxKit.Core.Polling;

public interface IOutboxProducer
{
    Task ProducePendingAsync(CancellationToken ct);
}