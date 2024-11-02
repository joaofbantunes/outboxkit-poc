using System.Text;
using Bogus;
using Microsoft.EntityFrameworkCore;
using MySqlEfPollingSample;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.OpenTelemetry;
using YakShaveFx.OutboxKit.MySql.Polling;

const string connectionString =
    "server=localhost;port=3306;database=outboxkit_ef_mysql_sample;user=root;password=root;";

var builder = WebApplication.CreateBuilder(args);
var mySqlVersion = new MySqlServerVersion(new Version(8, 0));

builder.Services
    .AddDbContext<SampleContext>((s, options) =>
    {
        options.UseMySql(connectionString, mySqlVersion);
        options.AddInterceptors(s.GetRequiredService<OutboxInterceptor>());
    })
    .AddHostedService<DbSetupHostedService>()
    .AddScoped<OutboxInterceptor>()
    .AddSingleton<FakeTargetProducer>()
    .AddOutboxKit(kit =>
        kit
            .WithTargetProducer<FakeTargetProducer>("fake")
            .WithMySqlPolling(p =>
                p
                    .WithConnectionString(connectionString)
                    // this is optional, only needed if the default table name and column names don't match
                    .WithTable(t => t
                        .WithTableName("OutboxMessages")
                        .WithColumnName(m => m.Id, nameof(OutboxMessage.Id))
                        .WithColumnName(m => m.Target, nameof(OutboxMessage.Target))
                        .WithColumnName(m => m.Type, nameof(OutboxMessage.Type))
                        .WithColumnName(m => m.Payload, nameof(OutboxMessage.Payload))
                        .WithColumnName(m => m.CreatedAt, nameof(OutboxMessage.CreatedAt))
                        .WithColumnName(m => m.ObservabilityContext, nameof(OutboxMessage.ObservabilityContext)))))
    .AddSingleton(new Faker())
    .AddSingleton(TimeProvider.System);

builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("MySqlEfPollingSample"))
    .WithTracing(b => b
        .AddAspNetCoreInstrumentation()
        .AddOutboxKitInstrumentation()
        .AddSource("MySqlConnector")
        .AddOtlpExporter())
    .WithMetrics(b => b
        .AddAspNetCoreInstrumentation()
        .AddMeter("MySqlConnector")
        // TODO: add OutboxKit instrumentation
        .AddOtlpExporter());

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.MapPost("/publish/{count}", async (int count, Faker faker, SampleContext db) =>
{
    var messages = Enumerable.Range(0, count)
        .Select(_ => new OutboxMessage
        {
            Target = "fake",
            Type = "sample",
            Payload = Encoding.UTF8.GetBytes(faker.Hacker.Verb()),
            CreatedAt = DateTime.UtcNow,
            ObservabilityContext = ObservabilityContextHelpers.GetCurrentObservabilityContext()
        });

    await db.OutboxMessages.AddRangeAsync(messages);
    await db.SaveChangesAsync();
});

app.Run();