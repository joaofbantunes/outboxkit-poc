using System.Data;
using Dapper;
using MySqlConnector;
using YakShaveFx.OutboxKit.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddSingleton<FakeTargetProducer>()
    .AddOutboxBatchFetcher<OutboxBatchFetcher>()
    .AddOutboxKit(kit =>
        kit.WithTargetProducer<FakeTargetProducer>("fake"));

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.Run();

