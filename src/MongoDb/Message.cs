using MongoDB.Bson;
using YakShaveFx.OutboxKit.Core;

namespace YakShaveFx.OutboxKit.MongoDb;

public sealed record Message(
    ObjectId Id,
    string Type,
    byte[] Payload,
    DateTime CreatedAt,
    byte[]? ObservabilityContext) : IMessage;