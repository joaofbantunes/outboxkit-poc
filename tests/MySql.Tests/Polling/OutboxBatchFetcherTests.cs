using Dapper;
using FluentAssertions;

namespace YakShaveFx.OutboxKit.MySql.Tests.Polling;

[Collection(MySqlCollection.Name)]
public class OutboxBatchFetcherTests(MySqlFixture mySqlFixture)
{
    [Fact]
    public async Task Test1()
    {
        await using var databaseContext = await mySqlFixture.SetupDatabaseContextAsync();
        await databaseContext.DataSource.SetupDatabaseWithDefaultSettingsAsync();
        await using var connection = await databaseContext.DataSource.OpenConnectionAsync();
        var exists = await connection.QueryFirstAsync<bool>(
            // lang=mysql
            "SELECT EXISTS(SELECT 1 FROM outbox_messages);");
        exists.Should().BeFalse();
    }
}