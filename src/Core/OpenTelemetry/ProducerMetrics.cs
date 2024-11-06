using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace YakShaveFx.OutboxKit.Core.OpenTelemetry;

internal sealed class ProducerMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _producedBatchesCounter;

    public ProducerMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(MetricShared.MeterName);
        
        _producedBatchesCounter = _meter.CreateCounter<long>(
            "outbox.produced_batches",
            unit: "{batch}",
            description: "The number of batches produced");
    }
    
    public void BatchProduced(string key, bool allMessagesProduced)
    {
        if (_producedBatchesCounter.Enabled)
        {
            var tags = new TagList
            {
                { "key", key },
                { "all_messages_produced", allMessagesProduced }
            };
            _producedBatchesCounter.Add(1, tags);
        }
    }

    public void Dispose() => _meter.Dispose();
}