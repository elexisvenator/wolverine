using IntegrationTests;
using JasperFx;
using JasperFx.CodeGeneration;
using JasperFx.Core.Reflection;
using JasperFx.Events.Projections;
using JasperFx.Resources;
using Marten;
using JasperFx.Events;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Wolverine;
using Wolverine.ComplianceTests;
using Wolverine.Marten;
using Wolverine.Persistence;
using Wolverine.Runtime;
using Wolverine.Tracking;

namespace MartenTests.AggregateHandlerWorkflow;

/// <summary>
/// Marten brackets an Async aggregate fetch in a snapshot transaction of its own, and only accepts that before
/// everything else in a batch. Wolverine enlists those fetches first, whatever order the handler declares its
/// parameters in. An exclusive fetch cannot share their batch, because the bracket's <c>end</c> would release its
/// row lock, so it gets a second batch after the first.
/// </summary>
public class async_aggregate_load_in_a_batch : PostgresqlContext, IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private IHost _host = null!;
    private IDocumentStore _store = null!;

    public async_aggregate_load_in_a_batch(ITestOutputHelper output)
    {
        _output = output;
    }

    public async ValueTask InitializeAsync()
    {
        _host = await WolverineHost.ForAsync(opts =>
        {
            opts.Services.AddMarten(m =>
                {
                    m.Connection(Servers.PostgresConnectionString);
                    m.DatabaseSchemaName = "async_in_batch";
                    m.Projections.Snapshot<NkProduct>(SnapshotLifecycle.Inline);
                    m.Projections.Snapshot<NkProductStock>(SnapshotLifecycle.Async);
                    m.Projections.Snapshot<AsyncTally>(SnapshotLifecycle.Async);
                })
                .UseLightweightSessions()
                .IntegrateWithWolverine();

            opts.Discovery.DisableConventionalDiscovery()
                .IncludeType(typeof(AsyncInBatchHandler));
            opts.Durability.Mode = DurabilityMode.Solo;
            opts.Services.AddResourceSetupOnStartup();
            opts.CodeGeneration.TypeLoadMode = TypeLoadMode.Auto;
        });

        _store = _host.Services.GetRequiredService<IDocumentStore>();
        await _store.Advanced.Clean.DeleteAllEventDataAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    private string sourceFor<T>()
    {
        _host.GetRuntime().Handlers.HandlerFor<T>();
        var code = _host.GetRuntime().Handlers.ChainFor<T>()!.SourceCode!;
        _output.WriteLine(code);
        return code;
    }

    private async Task<Guid> startTally()
    {
        var tallyId = Guid.NewGuid();
        await using var session = _store.LightweightSession();
        session.Events.StartStream<AsyncTally>(tallyId, new TallyBumped());
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return tallyId;
    }

    private async Task<Guid> listProduct(NkSku sku, int stock)
    {
        var streamId = Guid.NewGuid();
        await using var session = _store.LightweightSession();
        session.Events.StartStream<NkProduct>(streamId, new NkProductListed(sku, "Widget"), new NkProductRestocked(stock));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return streamId;
    }

    private async Task<int> tallyCount(Guid tallyId)
    {
        await using var query = _store.LightweightSession();
        return (await query.Events.FetchLatest<AsyncTally>(tallyId, TestContext.Current.CancellationToken))!.Count;
    }

    private static int positionOf(string code, string text)
    {
        var index = code.IndexOf(text, StringComparison.Ordinal);
        index.ShouldBeGreaterThanOrEqualTo(0, $"'{text}' is not in the generated code");
        return index;
    }

    [Fact]
    public async Task an_async_aggregate_by_stream_id_after_another_batched_load()
    {
        var tallyId = await startTally();

        // Declared second, enlisted first
        var code = sourceFor<BumpAsyncTally>();
        positionOf(code, $"FetchForWriting<{typeof(AsyncTally).FullNameInCode()}>(")
            .ShouldBeLessThan(positionOf(code, $"Query<{typeof(NkProduct).FullNameInCode()}>()"));

        await _host.InvokeMessageAndWaitAsync(new BumpAsyncTally(tallyId));

        (await tallyCount(tallyId)).ShouldBe(2);
    }

    [Fact]
    public async Task an_async_aggregate_by_natural_key_after_another_batched_load()
    {
        var sku = new NkSku("ASYNC-IN-BATCH");
        var streamId = await listProduct(sku, 1);

        await _host.InvokeMessageAndWaitAsync(new RestockNkProductStockAfterListing(sku, 2));

        await using var query = _store.QuerySession();
        (await query.LoadAsync<NkProduct>(streamId, TestContext.Current.CancellationToken))!.Stock.ShouldBe(3);
    }

    [Fact]
    public async Task an_async_read_after_another_batched_load()
    {
        var tallyId = await startTally();

        AsyncInBatchHandler.LastTally = null;
        await _host.InvokeMessageAndWaitAsync(new ViewAsyncTally(tallyId));

        AsyncInBatchHandler.LastTally!.Count.ShouldBe(1);
    }

    [Fact]
    public async Task an_exclusive_load_gets_a_second_batch_after_the_async_one()
    {
        var tallyId = await startTally();
        var sku = new NkSku("ASYNC-AND-EXCLUSIVE");
        var productId = await listProduct(sku, 1);

        var code = sourceFor<BumpTallyAndRestockExclusively>();

        // The async fetch and the other load share the first batch; the exclusive fetch has the second
        code.ShouldContain("lockingBatchedQuery");
        var firstExecute = positionOf(code, "await batchedQuery.Execute(");
        positionOf(code, $"batchedQuery.Events.FetchForWriting<{typeof(AsyncTally).FullNameInCode()}>(")
            .ShouldBeLessThan(firstExecute);
        positionOf(code, $"batchedQuery.Query<{typeof(NkProduct).FullNameInCode()}>()").ShouldBeLessThan(firstExecute);
        positionOf(code, "lockingBatchedQuery.Events.FetchForExclusiveWriting<")
            .ShouldBeGreaterThan(firstExecute);

        await _host.InvokeMessageAndWaitAsync(new BumpTallyAndRestockExclusively(tallyId, sku, 4));

        (await tallyCount(tallyId)).ShouldBe(2);
        await using var query = _store.QuerySession();
        (await query.LoadAsync<NkProduct>(productId, TestContext.Current.CancellationToken))!.Stock.ShouldBe(5);
    }

    [Fact]
    public async Task an_exclusive_load_of_an_async_aggregate_stays_in_one_batch()
    {
        // It never brackets: it already owns the session's transaction, so it counts as exclusive
        var tallyId = await startTally();

        var code = sourceFor<BumpAsyncTallyExclusively>();
        code.ShouldNotContain("lockingBatchedQuery");
        code.ShouldContain("batchedQuery.Events.FetchForExclusiveWriting<");

        await _host.InvokeMessageAndWaitAsync(new BumpAsyncTallyExclusively(tallyId));

        (await tallyCount(tallyId)).ShouldBe(2);
    }
}

public record TallyBumped;

public class AsyncTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }

    public static AsyncTally Create(TallyBumped e) => new() { Count = 1 };

    public void Apply(TallyBumped e) => Count++;
}

public record BumpAsyncTally(Guid AsyncTallyId);

public record BumpAsyncTallyExclusively(Guid AsyncTallyId);

public record ViewAsyncTally(Guid AsyncTallyId);

public record RestockNkProductStockAfterListing(NkSku Sku, int Quantity);

public record BumpTallyAndRestockExclusively(Guid AsyncTallyId, NkSku Sku, int Quantity);

public static class AsyncInBatchHandler
{
    public static AsyncTally? LastTally;

    // [All] is declared first, the Async aggregate load second
    public static TallyBumped Handle(BumpAsyncTally command, [All] IReadOnlyList<NkProduct> products,
        [WriteAggregate] AsyncTally tally) => new();

    public static NkProductRestocked Handle(RestockNkProductStockAfterListing command,
        [All] IReadOnlyList<NkProduct> products, [WriteAggregate] NkProductStock stock) => new(command.Quantity);

    public static void Handle(ViewAsyncTally query, [All] IReadOnlyList<NkProduct> products,
        [ReadAggregate] AsyncTally tally) => LastTally = tally;

    public static TallyBumped Handle(BumpAsyncTallyExclusively command, [All] IReadOnlyList<NkProduct> products,
        [WriteAggregate(LoadStyle = Wolverine.Marten.ConcurrencyStyle.Exclusive)] AsyncTally tally) => new();

    // The exclusive load is declared first, the Async one last
    public static void Handle(BumpTallyAndRestockExclusively command,
        [WriteAggregate<NkSku>(LoadStyle = Wolverine.Marten.ConcurrencyStyle.Exclusive)] IEventStream<NkProduct> product,
        [All] IReadOnlyList<NkProduct> products,
        [WriteAggregate] IEventStream<AsyncTally> tally)
    {
        product.AppendOne(new NkProductRestocked(command.Quantity));
        tally.AppendOne(new TallyBumped());
    }
}
