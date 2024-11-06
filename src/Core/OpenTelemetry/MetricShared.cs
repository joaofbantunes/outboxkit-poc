namespace YakShaveFx.OutboxKit.Core.OpenTelemetry;

internal static class MetricShared
{
    public static string MeterName { get; } = typeof(MetricShared).Assembly.GetName().Name!; 
}