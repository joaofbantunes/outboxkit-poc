namespace YakShaveFx.OutboxKit.Core;

public interface IOutboxTrigger
{
    void OnNewMessages();
}