using System.Collections.Frozen;
using Microsoft.Extensions.DependencyInjection;

namespace YakShaveFx.OutboxKit.Core;

public sealed class DefaultTargetProducerProvider : ITargetProducerProvider
{
    private readonly FrozenDictionary<string, ITargetProducer> _targetProducers;

    public DefaultTargetProducerProvider(IServiceProvider serviceProvider, IReadOnlyDictionary<string, Type> targetProducers)
    {
        _targetProducers = targetProducers.ToDictionary(
                kvp => kvp.Key,
                kvp => (ITargetProducer)serviceProvider.GetRequiredService(kvp.Value))
            .ToFrozenDictionary();
    }

    public ITargetProducer Get(string target)
    {
        if (!_targetProducers.TryGetValue(target, out var producer))
        {
            throw new InvalidOperationException($"No producer registered for target '{target}'");
        }

        return producer;
    }
}