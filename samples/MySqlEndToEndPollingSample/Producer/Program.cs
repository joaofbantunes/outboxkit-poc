using System.Text;
using System.Text.Json;
using Bogus;
using MySqlConnector;
using MySqlEndToEndPollingSample.Producer;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.OpenTelemetry;
using YakShaveFx.OutboxKit.MySql;
using YakShaveFx.OutboxKit.MySql.Polling;
using Dapper;
using RabbitMQ.Client;
using YakShaveFx.OutboxKit.Core.Polling;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Default")!;
var rabbitMqSettings = builder.Configuration.GetRequiredSection("RabbitMq").Get<RabbitMqSettings>()!;
builder.Services
    .AddMySqlDataSource(connectionString)
    .AddSingleton(rabbitMqSettings)
    .AddSingleton<IConnection>(_ =>
    {
        var factory = new ConnectionFactory { HostName = rabbitMqSettings.Host, Port = rabbitMqSettings.Port };
        return factory.CreateConnection();
    })
    .AddSingleton<RabbitMqProducer>()
    .AddOutboxKit(kit =>
        kit
            .WithBatchProducer<RabbitMqProducer>()
            .WithMySqlPolling(p => p.WithConnectionString(connectionString)))
    .AddSingleton(new Faker())
    .AddSingleton(TimeProvider.System)
    .AddHostedService<DbSetupHostedService>();

builder.Services
    .AddSingleton<RabbitMqProducerMetrics>()
    .AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(
        serviceName: "MySqlEndToEndPollingSample.Producer",
        serviceVersion: typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown",
        serviceInstanceId: Environment.MachineName))
    .WithTracing(b => b
        .AddAspNetCoreInstrumentation()
        .AddOutboxKitInstrumentation()
        .AddSource("MySqlConnector")
        .AddSource(RabbitMqProducerActivitySource.ActivitySourceName)
        .AddOtlpExporter(o =>
            o.Endpoint = new Uri(builder.Configuration["OpenTelemetrySettings:Endpoint"] ?? "http://localhost:4317")))
    .WithMetrics(b => b
        .AddAspNetCoreInstrumentation()
        .AddMeter("MySqlConnector")
        .AddOutboxKitInstrumentation()
        .AddMeter(RabbitMqProducerMetrics.MeterName)
        .AddOtlpExporter(o =>
            o.Endpoint = new Uri(builder.Configuration["OpenTelemetrySettings:Endpoint"] ?? "http://localhost:4317")));

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.MapPost("/produce-something", async (
    Faker faker,
    TimeProvider timeProvider,
    MySqlDataSource dataSource,
    IOutboxTrigger trigger) =>
{
    var @event = new SampleEvent
    {
        Id = Guid.NewGuid(),
        Verb = faker.Hacker.Verb()
    };

    var outboxMessage = new Message(
        default,
        nameof(SampleEvent),
        JsonSerializer.SerializeToUtf8Bytes(@event),
        timeProvider.GetUtcNow().DateTime,
        ObservabilityContextHelpers.GetCurrentObservabilityContext());

    await using var connection = await dataSource.OpenConnectionAsync();
    await connection.ExecuteAsync(
        // lang=mysql
        "INSERT INTO outbox_messages (type, payload, created_at, observability_context) VALUES (@Type, @Payload, @CreatedAt, @ObservabilityContext)",
        outboxMessage);
    trigger.OnNewMessages();
});

app.Run();