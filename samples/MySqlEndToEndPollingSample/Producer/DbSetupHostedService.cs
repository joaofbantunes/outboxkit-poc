using MySqlConnector;

internal sealed class DbSetupHostedService(MySqlDataSource dataSource) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(stoppingToken);
        await using var command = new MySqlCommand(
            // lang=mysql
            $"""
             create table if not exists outbox_messages
             (
                 id                    bigint auto_increment primary key,
                 target                varchar(128) not null,
                 type                  varchar(128) not null,
                 payload               longblob     not null,
                 created_at            datetime(6)  not null,
                 observability_context longblob     null
             );
             """, connection);
        await command.ExecuteNonQueryAsync(stoppingToken);
    }
}