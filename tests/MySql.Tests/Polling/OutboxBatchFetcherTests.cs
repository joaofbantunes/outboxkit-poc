using MySqlConnector;
using YakShaveFx.OutboxKit.MySql.Polling;

namespace YakShaveFx.OutboxKit.MySql.Tests.Polling;

[Collection(MySqlCollection.Name)]
public class OutboxBatchFetcherTests(MySqlFixture mySqlFixture)
{
    [Fact]
    public async Task Test1()
    {
        await using var databaseContext = await mySqlFixture.SetupDatabaseContextAsync();
        await databaseContext.DataSource.SetupDatabaseWithDefaultSettingsAsync();
        
    }
}