using System.Diagnostics;
using System.Text;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.OpenTelemetry;
using YakShaveFx.OutboxKit.MongoDb;

namespace MongoDbPushSample;

internal sealed class FakeBatchProducer(ILogger<FakeBatchProducer> logger) : IBatchProducer
{
    public static ActivitySource ActivitySource { get; } = new(typeof(FakeBatchProducer).Assembly.GetName().Name!);
    
    public Task<BatchProduceResult> ProduceAsync(IReadOnlyCollection<IMessage> messages, CancellationToken ct)
    {
        var x = messages.Cast<Message>().ToList();
        logger.LogInformation("Producing {Count} messages", x.Count);
        foreach (var message in x)
        {
            using var activity = StartActivityFromObservabilityContext(message.ObservabilityContext);
            
            logger.LogInformation(
                """id {Id}, type {Type}, payload "{Payload}", created_at {CreatedAt}, observability_context {ObservabilityContext}""",
                message.Id, 
                message.Type,
                Encoding.UTF8.GetString(message.Payload),
                message.CreatedAt,
                message.ObservabilityContext is null ? "null" : $"{message.ObservabilityContext.Length} bytes");
        }

        return Task.FromResult(new BatchProduceResult { Ok = x });
    }
    
    private static Activity? StartActivityFromObservabilityContext(byte[]? observabilityContext)
    {
        var parentContext = ObservabilityContextHelpers.ExtractParentContext(observabilityContext);

        return ActivitySource.StartActivity(
            "produce message",
            ActivityKind.Producer,
            parentContext: parentContext?.ActivityContext ?? default);
    }
}