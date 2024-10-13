using Nito.AsyncEx;

namespace YakShaveFx.OutboxKit.Core.Polling;

internal sealed class Listener : IOutboxListener, IOutboxTrigger
{
    private readonly AsyncAutoResetEvent _autoResetEvent = new();

    public void OnNewMessages() => _autoResetEvent.Set();

    public Task WaitForMessagesAsync(CancellationToken ct) => _autoResetEvent.WaitAsync(ct);
}