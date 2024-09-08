using System.Diagnostics;

namespace YakShaveFx.OutboxKit.Core;

internal static class Observability
{
    private static string ActivitySourceName { get; } = typeof(Observability).Assembly.GetName().Name!;

    internal static readonly ActivitySource ActivitySource = new(
        ActivitySourceName,
        typeof(Observability).Assembly.GetName().Version!.ToString());
}