namespace YakShaveFx.OutboxKit.Core;

public interface ITargetProducerProvider
{
    ITargetProducer Get(string target);
}

public interface ITargetProducer
{
    Task<ProduceResult> ProduceAsync(IEnumerable<IMessage> messages, CancellationToken ct);
}

public sealed record ProduceResult(IReadOnlyCollection<IMessage> Ok);