using System.Text;
using Bogus;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDbPollingSample;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.OpenTelemetry;
using YakShaveFx.OutboxKit.Core.Polling;
using YakShaveFx.OutboxKit.MongoDb;
using YakShaveFx.OutboxKit.MongoDb.Polling;

const string connectionString = "mongodb://localhost:27017?replicaSet=rs0";
const string databaseName = "outboxkit_mongo_polling_sample";

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddSingleton<FakeTargetProducer>()
    .AddSingleton(new MongoClient(connectionString))
    .AddSingleton(s => s.GetRequiredService<MongoClient>().GetDatabase(databaseName))
    .AddSingleton(s => s.GetRequiredService<IMongoDatabase>().GetCollection<Message>("outbox_messages"))
    .AddOutboxKit(kit =>
        kit
            .WithTargetProducer<FakeTargetProducer>("fake")
            .WithMongoDbPolling(p =>
                p
                    .WithConnectionString(connectionString)
                    .WithDatabaseName(databaseName)))
    .AddSingleton(new Faker())
    .AddSingleton(TimeProvider.System);

builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("MongoDbPushSample"))
    .WithTracing(b => b
        .AddAspNetCoreInstrumentation()
        .AddOutboxKitInstrumentation()
        .AddOtlpExporter())
    .WithMetrics(b => b
        .AddAspNetCoreInstrumentation()
        // TODO: add OutboxKit instrumentation
        .AddOtlpExporter());

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.MapPost(
    "/publish/{count}",
    async (int count, Faker faker, IMongoCollection<Message> collection, IOutboxTrigger trigger) =>
    {
        var messages = Enumerable.Range(0, count)
            .Select(_ => new Message
            (
                Id: ObjectId.Empty,
                Target: "fake",
                Type: "sample",
                Payload: Encoding.UTF8.GetBytes(faker.Hacker.Verb()),
                CreatedAt: DateTime.UtcNow,
                ObservabilityContext: ObservabilityContextHelpers.GetCurrentObservabilityContext()
            ));

        await collection.InsertManyAsync(messages);
        trigger.OnNewMessages();
    });

app.Run();