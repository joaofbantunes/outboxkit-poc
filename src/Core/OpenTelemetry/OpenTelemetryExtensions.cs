using OpenTelemetry.Trace;

namespace YakShaveFx.OutboxKit.Core.OpenTelemetry;

public static class OutboxKitInstrumentationTracerProviderBuilderExtensions
{
    public static TracerProviderBuilder AddOutboxKitInstrumentation(this TracerProviderBuilder builder)
    {
        builder.AddSource(ActivityHelpers.ActivitySource.Name);
        return builder;
    }
}

public static class OutboxKitInstrumentationMeterProviderBuilderExtensions
{
    // TODO: implement
}