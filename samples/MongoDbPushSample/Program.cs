using System.Text;
using Bogus;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDbPushSample;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.OpenTelemetry;
using YakShaveFx.OutboxKit.MongoDb;
using YakShaveFx.OutboxKit.MongoDb.Push;

const string connectionString = "mongodb://localhost:27017?replicaSet=rs0";
const string databaseName = "outboxkit_mongo_push_sample";

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddSingleton<FakeBatchProducer>()
    .AddSingleton(new MongoClient(connectionString))
    .AddSingleton(s => s.GetRequiredService<MongoClient>().GetDatabase(databaseName))
    .AddSingleton(s => s.GetRequiredService<IMongoDatabase>().GetCollection<Message>("outbox_messages"))
    .AddOutboxKit(kit =>
        kit
            .WithBatchProducer<FakeBatchProducer>()
            .WithMongoDbPush(p =>
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
        .AddSource(FakeBatchProducer.ActivitySource.Name)
        .AddOtlpExporter())
    .WithMetrics(b => b
        .AddAspNetCoreInstrumentation()
        .AddOutboxKitInstrumentation()
        .AddOtlpExporter());

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.MapPost("/publish/{count}", async (int count, Faker faker, IMongoCollection<Message> collection) =>
{
    var messages = Enumerable.Range(0, count)
        .Select(_ => new Message
        (
            Id: ObjectId.Empty,
            Type: "sample",
            Payload: Encoding.UTF8.GetBytes(faker.Hacker.Verb()),
            CreatedAt: DateTime.UtcNow,
            ObservabilityContext: ObservabilityContextHelpers.GetCurrentObservabilityContext()
        ));

    await collection.InsertManyAsync(messages);
});

app.Run();