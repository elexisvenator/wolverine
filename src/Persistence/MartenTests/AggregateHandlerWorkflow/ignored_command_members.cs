using IntegrationTests;
using JasperFx;
using JasperFx.CodeGeneration;
using JasperFx.Core;
using JasperFx.Events.Projections;
using JasperFx.Resources;
using Marten;
using Marten.Events.CodeGeneration;
using Marten.Events.Projections;
using MartenTests.AggregateHandlerWorkflow.LoneId4883;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Wolverine;
using Wolverine.Marten;
using Wolverine.Tracking;

namespace MartenTests.AggregateHandlerWorkflow;

/// <summary>
/// A command member marked [JasperFxIgnore], or its Marten subclass [MartenIgnore], is never matched by
/// convention: not as the stream id or the expected version. An explicit name still selects it.
/// </summary>
public class ignored_command_members : PostgresqlContext, IAsyncLifetime
{
    private IHost _host = null!;
    private IDocumentStore _store = null!;

    public async ValueTask InitializeAsync()
    {
        _host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Discovery.DisableConventionalDiscovery()
                    .IncludeType(typeof(IgnoredMemberHandler))
                    .IncludeType(typeof(IgnoredMemberAggregateHandler));
                opts.Durability.Mode = DurabilityMode.Solo;
                opts.Services.AddMarten(m =>
                    {
                        m.Connection(Servers.PostgresConnectionString);
                        m.DatabaseSchemaName = "ignored_members";
                        m.Projections.Snapshot<IgnoredMemberCounter>(SnapshotLifecycle.Inline);
                        m.DisableNpgsqlLogging = true;
                    })
                    .UseLightweightSessions()
                    .IntegrateWithWolverine();

                opts.Services.AddResourceSetupOnStartup();
                opts.CodeGeneration.TypeLoadMode = TypeLoadMode.Auto;
            }).StartAsync();

        _store = _host.Services.GetRequiredService<IDocumentStore>();
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    // A stream at version 1
    private async Task<Guid> givenCounter()
    {
        await using var session = _store.LightweightSession();
        var id = session.Events.StartStream<IgnoredMemberCounter>(new IgnoredMemberCounterStarted()).Id;
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return id;
    }

    private async Task<int> countOf(Guid id)
    {
        await using var session = _store.LightweightSession();
        return (await session.LoadAsync<IgnoredMemberCounter>(id, TestContext.Current.CancellationToken))!.Count;
    }

    [Fact]
    public async Task an_ignored_version_is_not_checked()
    {
        var id = await givenCounter();

        await _host.InvokeMessageAndWaitAsync(new BumpWithIgnoredVersion(id, 99));

        (await countOf(id)).ShouldBe(1);
    }

    [Fact]
    public async Task a_version_without_the_attribute_is_still_checked()
    {
        var id = await givenCounter();

        await Should.ThrowAsync<ConcurrencyException>(() =>
            _host.InvokeMessageAndWaitAsync(new BumpWithVersion(id, 99)));

        (await countOf(id)).ShouldBe(0);
    }

    [Fact]
    public async Task an_explicit_version_source_uses_an_ignored_member()
    {
        var id = await givenCounter();

        await Should.ThrowAsync<ConcurrencyException>(() =>
            _host.InvokeMessageAndWaitAsync(new BumpWithExplicitIgnoredVersion(id, 99)));

        (await countOf(id)).ShouldBe(0);
    }

    // In each command below the ignored member is the one the convention would take first, and it names a
    // stream that doesn't exist

    [Fact]
    public async Task an_ignored_id_is_not_the_stream_id_for_write_aggregate()
    {
        var id = await givenCounter();

        await _host.InvokeMessageAndWaitAsync(new BumpWithIgnoredAggregateId(Guid.NewGuid(), id));

        (await countOf(id)).ShouldBe(1);
    }

    [Fact]
    public async Task an_ignored_id_is_not_the_stream_id_for_read_aggregate()
    {
        var id = await givenCounter();
        await _host.InvokeMessageAndWaitAsync(new BumpWithIgnoredVersion(id, 0));

        IgnoredMemberHandler.LastCount = -1;
        await _host.InvokeMessageAndWaitAsync(new ReadWithIgnoredAggregateId(Guid.NewGuid(), id));

        IgnoredMemberHandler.LastCount.ShouldBe(1);
    }

    [Fact]
    public async Task an_ignored_id_is_not_the_stream_id_for_aggregate_handler()
    {
        var id = await givenCounter();

        await _host.InvokeMessageAndWaitAsync(new BumpViaAggregateHandler(Guid.NewGuid(), id));

        (await countOf(id)).ShouldBe(1);
    }

    [Fact]
    public async Task an_ignored_strong_typed_id_is_not_a_rival_to_the_other()
    {
        var raw = Guid.NewGuid();
        await using (var session = _store.LightweightSession())
        {
            session.Events.StartStream<StrongKettle>(raw, new KettleFilled(2));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var id = new KettleId(raw);
        var other = new KettleId(Guid.NewGuid());

        // Two KettleId members, one ignored, so the other is the single strong typed id member
        await _host.InvokeMessageAndWaitAsync(new BoilStrongWithIgnoredRival(id, other));
        await _host.InvokeMessageAndWaitAsync(new BoilStrongViaAggregateHandler(id, other));

        IgnoredMemberHandler.LastCount = -1;
        await _host.InvokeMessageAndWaitAsync(new CountStrongWithIgnoredRival(id, other));
        IgnoredMemberHandler.LastCount.ShouldBe(2);
    }
}

public class IgnoredMemberCounter
{
    public Guid Id { get; set; }

    public int Count { get; set; }

    public static IgnoredMemberCounter Create(IgnoredMemberCounterStarted e) => new();

    public void Apply(IgnoredMemberCounterBumped e) => Count++;
}

public record IgnoredMemberCounterStarted;
public record IgnoredMemberCounterBumped;

#region sample_wolverine_marten_ignored_command_members
// Version is not the stream's expected version, so it is not checked
public record BumpWithIgnoredVersion(Guid IgnoredMemberCounterId, [property: MartenIgnore] long Version);

// IgnoredMemberCounterId is the caller's own reference, so the stream id is Id
public record BumpWithIgnoredAggregateId([property: JasperFxIgnore] Guid IgnoredMemberCounterId, Guid Id);
#endregion

public record BumpWithVersion(Guid IgnoredMemberCounterId, long Version);
public record BumpWithExplicitIgnoredVersion(Guid IgnoredMemberCounterId, [property: JasperFxIgnore] int Version);
public record ReadWithIgnoredAggregateId([property: JasperFxIgnore] Guid IgnoredMemberCounterId, Guid Id);
public record BumpViaAggregateHandler([property: JasperFxIgnore] Guid Id, Guid IgnoredMemberCounterId);

public record BoilStrongWithIgnoredRival(KettleId Kettle, [property: JasperFxIgnore] KettleId PreviousKettle);
public record CountStrongWithIgnoredRival(KettleId Kettle, [property: JasperFxIgnore] KettleId PreviousKettle);
public record BoilStrongViaAggregateHandler(KettleId Kettle, [property: JasperFxIgnore] KettleId PreviousKettle);

public static class IgnoredMemberHandler
{
    public static int LastCount;

    public static IgnoredMemberCounterBumped Handle(BumpWithIgnoredVersion command,
        [WriteAggregate] IgnoredMemberCounter counter) => new();

    public static IgnoredMemberCounterBumped Handle(BumpWithVersion command,
        [WriteAggregate] IgnoredMemberCounter counter) => new();

    public static IgnoredMemberCounterBumped Handle(BumpWithExplicitIgnoredVersion command,
        [WriteAggregate(VersionSource = nameof(BumpWithExplicitIgnoredVersion.Version))]
        IgnoredMemberCounter counter) => new();

    public static IgnoredMemberCounterBumped Handle(BumpWithIgnoredAggregateId command,
        [WriteAggregate] IgnoredMemberCounter counter) => new();

    public static void Handle(ReadWithIgnoredAggregateId command, [ReadAggregate] IgnoredMemberCounter counter)
        => LastCount = counter.Count;

    public static KettleBoiled Handle(BoilStrongWithIgnoredRival command, [WriteAggregate] StrongKettle kettle)
        => new();

    public static void Handle(CountStrongWithIgnoredRival query, [ReadAggregate] StrongKettle kettle)
        => LastCount = kettle.Boils;
}

[AggregateHandler]
public static class IgnoredMemberAggregateHandler
{
    public static IgnoredMemberCounterBumped Handle(BumpViaAggregateHandler command, IgnoredMemberCounter counter)
        => new();

    public static KettleBoiled Handle(BoilStrongViaAggregateHandler command, StrongKettle kettle) => new();
}
