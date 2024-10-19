namespace YakShaveFx.OutboxKit.Core;

public interface IMessage
{
    string Target { get; }
}