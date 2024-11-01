using FluentAssertions;
using YakShaveFx.OutboxKit.MySql.Polling;
using static YakShaveFx.OutboxKit.MySql.Tests.Polling.Defaults;

namespace YakShaveFx.OutboxKit.MySql.Tests.Polling;

[Collection(MySqlCollection.Name)]
public class OutboxBatchFetcherTests(MySqlFixture mySqlFixture)
{
    [Fact]
    public async Task WhenTheOutboxIsPolledConcurrentlyThenTheSecondGetsBlocked()
    {
        await using var databaseContext = await mySqlFixture.DbInitializer.WithDefaultSchema().WithSeed().InitAsync();
        await using var connection = await databaseContext.DataSource.OpenConnectionAsync();

        var sut1 = new OutboxBatchFetcher(DefaultPollingSettings, DefaultTableConfig, databaseContext.DataSource);
        var sut2 = new OutboxBatchFetcher(DefaultPollingSettings, DefaultTableConfig, databaseContext.DataSource);

        // start fetching from the outbox concurrently
        // - first delay is to ensure the first query is executed before the second one
        // - second delay is to give the second query time to block
        // (if there's a better way to test this, I'm all ears 😅)
        var batch1Task = sut1.FetchAndHoldAsync(CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(1));
        var batch2Task = sut2.FetchAndHoldAsync(CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(1));

        batch1Task.Should().BeEquivalentTo(new
        {
            IsCompleted = true,
            IsCompletedSuccessfully = true,
        });
        batch2Task.Should().BeEquivalentTo(new
        {
            IsCompleted = false,
            IsCompletedSuccessfully = false,
        });
    }

    [Fact]
    public async Task WhenTheOutboxIsPolledConcurrentlyTheSecondIsUnblockedByTheFirstCompletingObtainingTheNextBatch()
    {
        await using var databaseContext = await mySqlFixture.DbInitializer.WithDefaultSchema().WithSeed().InitAsync();
        await using var connection = await databaseContext.DataSource.OpenConnectionAsync();

        var sut1 = new OutboxBatchFetcher(DefaultPollingSettings, DefaultTableConfig, databaseContext.DataSource);
        var sut2 = new OutboxBatchFetcher(DefaultPollingSettings, DefaultTableConfig, databaseContext.DataSource);

        var batch1Task = sut1.FetchAndHoldAsync(CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(1));
        var batch2Task = sut2.FetchAndHoldAsync(CancellationToken.None);

        await using var batch1 = await batch1Task;
        await batch1.CompleteAsync(batch1.Messages, CancellationToken.None);
        batch1.Messages.Cast<Message>().Should().AllSatisfy(m => m.Id.Should().BeInRange(1, 5));
        
        await using var batch2 = await batch2Task;
        await batch2.CompleteAsync(batch2.Messages, CancellationToken.None);
        batch2.Messages.Cast<Message>().Should().AllSatisfy(m => m.Id.Should().BeInRange(6, 10));
    }
}