using System.Collections.Frozen;
using YakShaveFx.OutboxKit.MySql.Polling;

namespace YakShaveFx.OutboxKit.MySql.Tests.Polling;

internal static class Defaults
{
    internal static readonly MySqlPollingSettings DefaultPollingSettings = new()
    {
        BatchSize = 5
    };

    internal static readonly TableConfiguration DefaultTableConfig = new(
        "outbox_messages",
        new Dictionary<string, string>()
        {
            [nameof(Message.Id)] = "id",
            [nameof(Message.Target)] = "target",
            [nameof(Message.Type)] = "type",
            [nameof(Message.Payload)] = "payload",
            [nameof(Message.CreatedAt)] = "created_at",
            [nameof(Message.ObservabilityContext)] = "observability_context"
        }.ToFrozenDictionary());
}