using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using YakShaveFx.OutboxKit.Core.Polling;

namespace YakShaveFx.OutboxKit.Core.Tests.Polling;

public class ProducerTests
{
    private const string Key = "key";

    [Fact]
    public async Task WhenBatchIsEmptyThenProducerIsNotInvoked()
    {
        var (producerSpy, producerProviderStub) = CreateProducer();
        var services = CreateServices(
            new OutboxBatchFetcherStub([new OutboxBatchContextStub([], false)]),
            producerProviderStub);
        var sut = new Producer(services.GetRequiredService<IServiceScopeFactory>());

        await sut.ProducePendingAsync(Key, CancellationToken.None);

        await producerSpy
            .DidNotReceive()
            .ProduceAsync(Arg.Any<IReadOnlyCollection<IMessage>>(), Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task WhileThereAreAvailableBatchesProducerIsInvoked(int numberOfBatches)
    {
        var (producerSpy, producerProviderStub) = CreateProducer();
        var services = CreateServices(
            new OutboxBatchFetcherStub(CreateBatchContexts(numberOfBatches)),
            producerProviderStub);
        var sut = new Producer(services.GetRequiredService<IServiceScopeFactory>());

        await sut.ProducePendingAsync(Key, CancellationToken.None);

        await producerSpy
            .Received(numberOfBatches)
            .ProduceAsync(Arg.Any<IReadOnlyCollection<IMessage>>(), Arg.Any<CancellationToken>());
    }

    private static OutboxBatchContextStub[] CreateBatchContexts(int numberOfBatches)
        => Enumerable.Range(0, numberOfBatches)
            .Select(i => new OutboxBatchContextStub([new MessageStub()], i + 1 < numberOfBatches))
            .ToArray();

    private static (IBatchProducer, IBatchProducerProvider) CreateProducer()
    {
        var producerSpy = Substitute.For<IBatchProducer>();
        producerSpy
            .ProduceAsync(default!, default)
            .ReturnsForAnyArgs(args =>
                Task.FromResult(new ProduceResult { Ok = (IReadOnlyCollection<IMessage>)args[0] }));
        var producerProviderStub = Substitute.For<IBatchProducerProvider>();
        producerProviderStub.Get().Returns(producerSpy);
        return (producerSpy, producerProviderStub);
    }

    private static IServiceProvider CreateServices(
        IOutboxBatchFetcher batchFetcher,
        IBatchProducerProvider producerProvider)
        => new ServiceCollection()
            .AddKeyedSingleton(Key, batchFetcher)
            .AddSingleton(producerProvider)
            .BuildServiceProvider();
}

public sealed class MessageStub : IMessage;

public sealed class OutboxBatchFetcherStub(OutboxBatchContextStub[] contexts) : IOutboxBatchFetcher
{
    private int _index = 0;

    public Task<IOutboxBatchContext> FetchAndHoldAsync(CancellationToken ct)
    {
        if (_index >= contexts.Length)
        {
            return Task.FromResult<IOutboxBatchContext>(EmptyBatchContext.Instance);
        }

        return Task.FromResult<IOutboxBatchContext>(contexts[_index++]);
    }
}

public sealed class OutboxBatchContextStub(IReadOnlyCollection<IMessage> messages, bool hasNext) : IOutboxBatchContext
{
    public IReadOnlyCollection<IMessage> Messages => messages;
    public Task CompleteAsync(IReadOnlyCollection<IMessage> ok, CancellationToken ct) => Task.CompletedTask;
    public Task<bool> HasNextAsync(CancellationToken ct) => Task.FromResult(hasNext);
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}