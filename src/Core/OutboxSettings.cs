using YakShaveFx.OutboxKit.Core.Polling;

namespace YakShaveFx.OutboxKit.Core;

public sealed class OutboxSettings
{
    public PollingSettings Polling { get; init; } = new();
}