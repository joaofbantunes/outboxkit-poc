using System.Diagnostics;

namespace YakShaveFx.OutboxKit.Core.OpenTelemetry;

internal static class ActivityHelpers
{
    private static string ActivitySourceName { get; } = typeof(ActivityHelpers).Assembly.GetName().Name!;

    internal static readonly ActivitySource ActivitySource = new(
        ActivitySourceName,
        typeof(ActivityHelpers).Assembly.GetName().Version!.ToString());
}

internal static class ActivityConstants
{
    // TODO: review these tags
    public const string OutboxKeyTag = "outbox.key";
    public const string OutboxBatchSizeTag = "outbox.batch.size";
}

