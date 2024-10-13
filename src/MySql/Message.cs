using YakShaveFx.OutboxKit.Core;

namespace YakShaveFx.OutboxKit.MySql;

public sealed record Message(
    long Id,
    string Target,
    string Type,
    byte[] Payload,
    DateTime CreatedAt,
    byte[]? ObservabilityContext) : IMessage;