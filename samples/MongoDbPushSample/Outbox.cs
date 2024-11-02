using System.Text;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.OpenTelemetry;
using YakShaveFx.OutboxKit.MongoDb;

namespace MongoDbPushSample;

internal sealed class FakeTargetProducer(ILogger<FakeTargetProducer> logger) : ITargetProducer
{
    public Task<ProduceResult> ProduceAsync(IEnumerable<IMessage> messages, CancellationToken ct)
    {
        var x = messages.Cast<Message>().ToList();
        logger.LogInformation("Producing {Count} messages", x.Count);
        foreach (var message in x)
        {
            using var activity = ObservabilityContextHelpers.StartActivityFromObservabilityContext(message.ObservabilityContext);
            
            logger.LogInformation(
                """id {Id}, target {Target}, type {Type}, payload "{Payload}", created_at {CreatedAt}, observability_context {ObservabilityContext}""",
                message.Id, 
                message.Target,
                message.Type,
                Encoding.UTF8.GetString(message.Payload),
                message.CreatedAt,
                message.ObservabilityContext is null ? "null" : $"{message.ObservabilityContext.Length} bytes");
        }

        return Task.FromResult(new ProduceResult { Ok = x });
    }
}