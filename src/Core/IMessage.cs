namespace YakShaveFx.OutboxKit.Core;

public interface IMessage
{
    string Target { get; }
    string Type { get; }
    string Payload { get; } // TODO: store bytes instead of string?
    string? ObservabilityContext { get; } // TODO: store bytes instead of string?
}