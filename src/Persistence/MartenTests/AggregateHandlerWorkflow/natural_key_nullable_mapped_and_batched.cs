using IntegrationTests;
using JasperFx;
using JasperFx.CodeGeneration;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using JasperFx.Resources;
using Marten;
using Marten.Events;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Wolverine;
using Wolverine.ComplianceTests;
using Wolverine.Marten;
using Wolverine.Persistence.EventSourcing;
using Wolverine.Runtime;
using Wolverine.Tracking;

namespace MartenTests.AggregateHandlerWorkflow;

/// <summary>
/// The aggregate handler workflow resolving a natural key from a <c>TKey?</c> command member, through a
/// mapped key on an async read model, under an exclusive load, and inside a batched query.
/// </summary>
public class natural_key_nullable_mapped_and_batched : PostgresqlContext, IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private IHost _host = null!;
    private IDocumentStore _store = null!;

    public natural_key_nullable_mapped_and_batched(ITestOutputHelper output)
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
                    m.DatabaseSchemaName = "nk_nullable_mapped";
                    m.Projections.Snapshot<NkProduct>(SnapshotLifecycle.Inline);
                    m.Projections.Snapshot<NkProductStock>(SnapshotLifecycle.Async);
                })
                .UseLightweightSessions()
                .IntegrateWithWolverine();

            opts.Discovery.DisableConventionalDiscovery()
                .IncludeType(typeof(NkProductHandler));
            opts.Durability.Mode = DurabilityMode.Solo;

            opts.Services.AddResourceSetupOnStartup();
            opts.CodeGeneration.TypeLoadMode = TypeLoadMode.Auto;
        });

        _store = _host.Services.GetRequiredService<IDocumentStore>();

        // A natural key identifies one stream, so a rerun against the same database would claim the same
        // keys again. DeleteAllEventDataAsync cascades into the lookup tables.
        await _store.Advanced.Clean.DeleteAllEventDataAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    private async Task<Guid> listProduct(NkSku sku, int stock = 0)
    {
        var streamId = Guid.NewGuid();
        await using var session = _store.LightweightSession();
        session.Events.StartStream<NkProduct>(streamId, new NkProductListed(sku, "Widget"),
            new NkProductRestocked(stock));
        await session.SaveChangesAsync();
        return streamId;
    }

    private async Task<NkProduct?> loadProduct(Guid streamId)
    {
        await using var query = _store.QuerySession();
        return await query.LoadAsync<NkProduct>(streamId);
    }

    private string sourceFor<T>()
    {
        _host.GetRuntime().Handlers.HandlerFor<T>();
        var chain = _host.GetRuntime().Handlers.ChainFor<T>();
        chain.ShouldNotBeNull();
        chain.SourceCode.ShouldNotBeNull();
        _output.WriteLine(chain.SourceCode);
        return chain.SourceCode;
    }

    [Fact]
    public async Task a_nullable_command_member_resolves_the_natural_key()
    {
        var sku = new NkSku("NULLABLE-1");
        var streamId = await listProduct(sku);

        await _host.InvokeMessageAndWaitAsync(new RestockNkProduct(sku, 4));

        (await loadProduct(streamId))!.Stock.ShouldBe(4);
    }

    [Fact]
    public async Task a_mapped_natural_key_loads_an_async_read_model()
    {
        var sku = new NkSku("MAPPED-1");
        var streamId = await listProduct(sku, 2);

        await _host.InvokeMessageAndWaitAsync(new RestockNkProductStock(sku, 3));

        // The handler appended through the read model's stream, which is the owner's stream too
        (await loadProduct(streamId))!.Stock.ShouldBe(5);
        NkProductHandler.LastStockSeen.ShouldBe(2);
    }

    [Fact]
    public async Task an_exclusive_natural_key_load_locks_the_stream()
    {
        var sku = new NkSku("EXCLUSIVE-1");
        var streamId = await listProduct(sku);

        sourceFor<LockAndRestockNkProduct>().ShouldContain("FetchForExclusiveWriting<");

        await _host.InvokeMessageAndWaitAsync(new LockAndRestockNkProduct(sku, 7));
        (await loadProduct(streamId))!.Stock.ShouldBe(7);
    }

    [Fact]
    public async Task a_natural_key_load_joins_the_batched_query()
    {
        var sku = new NkSku("BATCHED-1");
        var streamId = await listProduct(sku, 1);
        await listProduct(new NkSku("BATCHED-OTHER"), 1);

        var code = sourceFor<AuditNkProduct>();
        code.ShouldContain("CreateBatchQuery");
        code.ShouldContain($"batchedQuery.Events.FetchForWriting<{typeof(NkProduct).FullNameInCode()}, {typeof(NkSku).FullNameInCode()}>");

        await _host.InvokeMessageAndWaitAsync(new AuditNkProduct(sku));

        (await loadProduct(streamId))!.Stock.ShouldBe(2);
        NkProductHandler.LastProductCount.ShouldBeGreaterThanOrEqualTo(2);
    }
}

/// <summary>
/// A command that carries both an owned and a mapped key of the aggregate cannot say which one it means.
/// </summary>
public class natural_key_ambiguous_command : PostgresqlContext
{
    [Fact]
    public async Task a_command_matching_two_natural_keys_is_refused()
    {
        using var host = await WolverineHost.ForAsync(opts =>
        {
            opts.Services.AddMarten(m =>
                {
                    m.Connection(Servers.PostgresConnectionString);
                    m.DatabaseSchemaName = "nk_ambiguous";
                    m.Projections.Snapshot<NkProduct>(SnapshotLifecycle.Inline);
                    m.Projections.Snapshot<NkShelf>(SnapshotLifecycle.Inline);
                })
                .UseLightweightSessions()
                .IntegrateWithWolverine();

            opts.Discovery.DisableConventionalDiscovery()
                .IncludeType(typeof(NkShelfHandler));
            opts.Durability.Mode = DurabilityMode.Solo;
            opts.Services.AddResourceSetupOnStartup();
            opts.CodeGeneration.TypeLoadMode = TypeLoadMode.Auto;
        });

        var ex = Should.Throw<Exception>(() => host.GetRuntime().Handlers.HandlerFor<StockNkShelf>());

        var message = ex.ToString();
        message.ShouldContain("more than one natural key");
        message.ShouldContain(nameof(StockNkShelf.Sku));
        message.ShouldContain(nameof(StockNkShelf.Code));
    }
}

public readonly record struct NkSku(string Value);

public record NkShelfCode(string Value);

public record NkProductListed(NkSku Sku, string Name);

public record NkProductRestocked(int Quantity);

public record NkShelfPlaced(NkShelfCode Code, NkSku Sku);

public class NkProduct
{
    public Guid Id { get; set; }

    [NaturalKey]
    public NkSku Sku { get; set; }

    public string Name { get; set; } = string.Empty;
    public int Stock { get; set; }

    [NaturalKeySource]
    public static NkSku KeyFor(NkProductListed e) => e.Sku;

    public void Apply(NkProductListed e)
    {
        Sku = e.Sku;
        Name = e.Name;
    }

    public void Apply(NkProductRestocked e) => Stock += e.Quantity;
}

#region sample_wolverine_marten_mapped_natural_key
// An async read model of the product stream, reachable by the product's key
public class NkProductStock
{
    public Guid Id { get; set; }

    [MappedNaturalKey<NkProduct>]
    public NkSku Sku { get; set; }

    public int Stock { get; set; }

    public static NkProductStock Create(NkProductListed e) => new() { Sku = e.Sku };

    public void Apply(NkProductRestocked e) => Stock += e.Quantity;
}

#endregion

// Owns its code and maps the product's key
public class NkShelf
{
    public Guid Id { get; set; }

    [NaturalKey]
    public NkShelfCode Code { get; set; } = null!;

    [MappedNaturalKey<NkProduct>]
    public NkSku Sku { get; set; }

    [NaturalKeySource]
    public static NkShelfCode KeyFor(NkShelfPlaced e) => e.Code;

    public void Apply(NkShelfPlaced e)
    {
        Code = e.Code;
        Sku = e.Sku;
    }
}

#region sample_wolverine_marten_nullable_natural_key_command
// NkSku is a struct, and the command may carry it as NkSku?
public record RestockNkProduct(NkSku? Sku, int Quantity);
#endregion

public record RestockNkProductStock(NkSku Sku, int Quantity);

public record LockAndRestockNkProduct(NkSku Sku, int Quantity);

public record AuditNkProduct(NkSku Sku);

public record StockNkShelf(NkSku Sku, NkShelfCode Code);

public static class NkProductHandler
{
    public static int LastStockSeen;
    public static int LastProductCount;

    public static NkProductRestocked Handle(RestockNkProduct command, [WriteAggregate] NkProduct product)
        => new(command.Quantity);

    #region sample_wolverine_marten_mapped_natural_key_handler
    // Loaded through the product's lookup, at its current state, before the daemon has caught up
    public static NkProductRestocked Handle(RestockNkProductStock command, [WriteAggregate] NkProductStock stock)
    {
        LastStockSeen = stock.Stock;
        return new NkProductRestocked(command.Quantity);
    }
    #endregion

    public static NkProductRestocked Handle(LockAndRestockNkProduct command,
        [WriteAggregate(LoadStyle = Wolverine.Marten.ConcurrencyStyle.Exclusive)] NkProduct product)
        => new(command.Quantity);

    public static NkProductRestocked Handle(AuditNkProduct command, [WriteAggregate] NkProduct product,
        [Wolverine.Persistence.All] IReadOnlyList<NkProduct> all)
    {
        LastProductCount = all.Count;
        return new NkProductRestocked(1);
    }
}

public static class NkShelfHandler
{
    public static NkShelfPlaced Handle(StockNkShelf command, [WriteAggregate] NkShelf shelf)
        => new(command.Code, command.Sku);
}
