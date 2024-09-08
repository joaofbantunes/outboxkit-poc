namespace YakShaveFx.OutboxKit.Core.Polling;

public interface IOutboxTrigger
{
    void OnNewMessages();
}