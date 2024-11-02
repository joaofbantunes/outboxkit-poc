using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace YakShaveFx.OutboxKit.Core.OpenTelemetry;

public static class ObservabilityContextHelpers
{
    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    public static byte[]? GetCurrentObservabilityContext()
    {
        var activity = Activity.Current;

        if (activity is null)
        {
            return null;
        }

        var extractedContext = new List<ContextEntry>(1);

        Propagator.Inject(
            new PropagationContext(activity.Context, Baggage.Current),
            extractedContext,
            InjectEntry);

        return JsonSerializer.SerializeToUtf8Bytes(extractedContext, SourceGenerationContext.Default.ListContextEntry);

        static void InjectEntry(List<ContextEntry> context, string key, string value)
            => context.Add(new(key, value));
    }

    public static PropagationContext? ExtractParentContext(byte[]? observabilityContext)
    {
        if (observabilityContext is not { Length: > 0 })
        {
            return null;
        }

        var deserializedContext = JsonSerializer.Deserialize<List<ContextEntry>>(observabilityContext)!;

        return Propagator.Extract(
            default,
            deserializedContext,
            ExtractEntry);

        static IEnumerable<string> ExtractEntry(List<ContextEntry> context, string key)
        {
            foreach (var entry in context)
            {
                if (entry.Key == key)
                {
                    yield return entry.Value;
                }
            }
        }
    }

    public static Activity? StartActivityFromObservabilityContext(byte[]? observabilityContext)
    {
        if (!ActivityHelpers.ActivitySource.HasListeners())
        {
            return null;
        }

        var parentContext = ExtractParentContext(observabilityContext);

        if (!parentContext.HasValue)
        {
            return null;
        }

        // we're going to use a custom parent context to look like we're part of the same request flow
        // as when the message was created but, to not lose completely the context of the outbox background job,
        // we're linking it
        var links = Activity.Current is { } currentActivity
            ? new[] { new ActivityLink(currentActivity.Context) }
            : default;

        return ActivityHelpers.ActivitySource.StartActivity(
            "outbox message produce",
            ActivityKind.Internal,
            parentContext.Value.ActivityContext,
            links: links);
    }
}

internal record struct ContextEntry(string Key, string Value);

[JsonSerializable(typeof(List<ContextEntry>))]
internal partial class SourceGenerationContext : JsonSerializerContext;