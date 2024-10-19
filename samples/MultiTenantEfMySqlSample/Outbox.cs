using System.Text;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.MySql;

namespace MultiTenantEfMySqlSample;

internal sealed class FakeTargetProducer(ILogger<FakeTargetProducer> logger) : ITargetProducer
{
    public Task<ProduceResult> ProduceAsync(IEnumerable<IMessage> messages, CancellationToken ct)
    {
        var x = messages.Cast<Message>().ToList();
        logger.LogInformation("Producing {Count} messages", x.Count);
        foreach (var message in x)
        {
            logger.LogInformation(
                """id {Id}, target {Target}, type {Type}, payload "{Payload}", created_at {CreatedAt}, observability_context {ObservabilityContext}""",
                message.Id, 
                message.Target,
                message.Type,
                Encoding.UTF8.GetString(message.Payload),
                message.CreatedAt,
                message.ObservabilityContext);
        }

        return Task.FromResult(new ProduceResult(x));
    }
}