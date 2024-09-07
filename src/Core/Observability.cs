using System.Diagnostics;
using System.Reflection;

namespace YakShaveFx.OutboxKit.Core;

internal static class Observability
{
    internal static string ActivitySourceName { get; } = typeof(Observability).Assembly.GetName().Name!;

    internal static readonly ActivitySource ActivitySource = new(
        ActivitySourceName,
        typeof(Observability).Assembly.GetName().Version!.ToString());
}