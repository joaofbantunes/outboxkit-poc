using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using YakShaveFx.OutboxKit.Core.Polling;

namespace EfMySqlSample;

public sealed class SampleContext(DbContextOptions<SampleContext> options) : DbContext(options)
{
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ApplyConfigurationsFromAssembly(GetType().Assembly);
}

public sealed class OutboxMessage
{
    public long Id { get; init; }
    public required string Target { get; init; }
    public required string Type { get; init; }
    public required byte[] Payload { get; init; }
    public DateTime CreatedAt { get; init; }
    public byte[]? ObservabilityContext { get; init; }
}

public sealed class OutboxMessageConfiguration : IEntityTypeConfiguration<OutboxMessage>
{
    public void Configure(EntityTypeBuilder<OutboxMessage> builder)
    {
        builder.ToTable("outbox_messages");
        builder.HasKey(e => e.Id);
        builder.Property(e => e.Id).HasColumnName("id");
        builder.Property(e => e.Target).HasColumnName("target").HasMaxLength(128);
        builder.Property(e => e.Type).HasColumnName("type").HasMaxLength(128);
        builder.Property(e => e.Payload).HasColumnName("payload");
        builder.Property(e => e.CreatedAt).HasColumnName("created_at");
        builder.Property(e => e.ObservabilityContext).HasColumnName("observability_context");
    }
}

public sealed class DbSetupHostedService(IServiceProvider serviceProvider) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var scope = serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<SampleContext>();
        await context.Database.EnsureCreatedAsync(cancellationToken);
    }

    // no-op
    public Task StopAsync(CancellationToken cancellationToken)
        => Task.CompletedTask;
}

public sealed class OutboxInterceptor(IOutboxTrigger trigger) : SaveChangesInterceptor
{
    private bool _hasOutboxMessages;

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = new())
    {
        if (eventData.Context is SampleContext db && db.ChangeTracker.Entries<OutboxMessage>().Any())
        {
            _hasOutboxMessages = true;
        }

        return base.SavingChangesAsync(eventData, result, cancellationToken);
    }

    public override async ValueTask<int> SavedChangesAsync(
        SaveChangesCompletedEventData eventData,
        int result,
        CancellationToken cancellationToken = new())
    {
        if (_hasOutboxMessages)
        {
            trigger.OnNewMessages();
        }

        return await base.SavedChangesAsync(eventData, result, cancellationToken);
    }
}