using DapperMySqlSample;
using YakShaveFx.OutboxKit.Core;
using YakShaveFx.OutboxKit.Core.Polling;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddSingleton<FakeTargetProducer>()
    .AddOutboxBatchFetcher<OutboxBatchFetcher>()
    .AddOutboxKit(kit => kit.WithTargetProducer<FakeTargetProducer>("fake"));

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.Run();

