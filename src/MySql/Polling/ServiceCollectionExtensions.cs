using Microsoft.Extensions.DependencyInjection;
using YakShaveFx.OutboxKit.Core;

namespace YakShaveFx.OutboxKit.MySql.Polling;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMySqlOutboxBatchFetcher(this IServiceCollection services) 
        => services.AddOutboxBatchFetcher<OutboxBatchFetcher>();
}