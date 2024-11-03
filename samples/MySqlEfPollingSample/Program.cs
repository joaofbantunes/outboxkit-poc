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
                    // this is optional, only needed if not using the default table structure
                    .WithTable(t => t
                        .WithName("OutboxMessages")
                        .WithColumns(["Id", "Target", "Type", "Payload", "CreatedAt", "ObservabilityContext"])
                        .WithIdColumn("Id")
                        .WithOrderByColumn("Id")
                        .WithIdGetter(m => ((OutboxMessage)m).Id)
                        .WithMessageFactory(r => new OutboxMessage
                        {
                            Id = r.GetInt64(0),
                            Target = r.GetString(1),
                            Type = r.GetString(2),
                            Payload = r.GetFieldValue<byte[]>(3),
                            CreatedAt = r.GetDateTime(4),
                            ObservabilityContext = r.IsDBNull(5) ? null : r.GetFieldValue<byte[]>(5)
                        }))))
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