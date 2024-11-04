using System.Text;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.OpenTelemetry;

namespace MySqlEfPollingSample;

internal sealed class FakeBatchProducer(ILogger<FakeBatchProducer> logger) : IBatchProducer
{
    public Task<ProduceResult> ProduceAsync(IReadOnlyCollection<IMessage> messages, CancellationToken ct)
    {
        var x = messages.Cast<OutboxMessage>().ToList();
        logger.LogInformation("Producing {Count} messages", x.Count);
        foreach (var message in x)
        {
            using var activity = ObservabilityContextHelpers.StartActivityFromObservabilityContext(message.ObservabilityContext);
            
            logger.LogInformation(
                """id {Id}, type {Type}, payload "{Payload}", created_at {CreatedAt}, observability_context {ObservabilityContext}""",
                message.Id, 
                message.Type,
                Encoding.UTF8.GetString(message.Payload),
                message.CreatedAt,
                message.ObservabilityContext is null ? "null" : $"{message.ObservabilityContext.Length} bytes");
        }

        return Task.FromResult(new ProduceResult{Ok = x});
    }
}