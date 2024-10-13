using System.Text;
using Bogus;
using EfMySqlSample;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using YakShaveFx.OutboxKit.Core;
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
    // needed by outboxkit.mysql, using keyed only to avoid clashing with Pomelo
    // probably interesting to move this into the outboxkit.mysql package
    .AddKeyedMySqlDataSource("outboxkit", connectionString) 
    .AddSingleton<FakeTargetProducer>()
    .AddMySqlOutboxBatchFetcher()
    .AddOutboxKit(kit => kit.WithTargetProducer<FakeTargetProducer>("fake"))
    .AddSingleton(new Faker())
    .AddSingleton(TimeProvider.System);

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
            ObservabilityContext = null // TODO
        });

    await db.OutboxMessages.AddRangeAsync(messages);
    await db.SaveChangesAsync();
});

app.Run();