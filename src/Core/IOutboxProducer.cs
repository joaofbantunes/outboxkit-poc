namespace YakShaveFx.OutboxKit.Core;

public interface IOutboxProducer
{
    Task ProducePendingAsync(CancellationToken ct);
}