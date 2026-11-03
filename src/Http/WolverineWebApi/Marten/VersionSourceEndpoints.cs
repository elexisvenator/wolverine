using JasperFx.Core;
using Wolverine.Http;
using Wolverine.Http.Marten;
using Wolverine.Marten;
using WolverineWebApi.Marten;

namespace WolverineWebApi.Marten;

public record ShipOrderWithExpectedVersion(Guid OrderId, long ExpectedVersion);

// Version is not the expected version of the stream, so it is never taken as one
public record ShipOrderWithIgnoredVersion(Guid OrderId, [property: JasperFxIgnore] long Version);

public static class VersionSourceEndpoints
{
    // Route argument version with custom name
    [WolverinePost("/orders/{orderId}/ship-with-expected-version/{expectedVersion}")]
    [EmptyResponse]
    public static OrderShipped ShipWithRouteVersion(
        ShipOrderWithExpectedVersion command,
        [Aggregate(VersionSource = "expectedVersion")] Order order)
    {
        return new OrderShipped();
    }

    // Request body member version with custom name
    [WolverinePost("/orders/ship-with-body-version")]
    [EmptyResponse]
    public static OrderShipped ShipWithBodyVersion(
        ShipOrderWithExpectedVersion command,
        [Aggregate(VersionSource = nameof(ShipOrderWithExpectedVersion.ExpectedVersion))] Order order)
    {
        return new OrderShipped();
    }

    // A Version member marked [JasperFxIgnore] is not the expected version
    [WolverinePost("/orders/ship-with-ignored-version")]
    [EmptyResponse]
    public static OrderShipped ShipWithIgnoredVersion(
        ShipOrderWithIgnoredVersion command,
        [Aggregate] Order order)
    {
        return new OrderShipped();
    }
}
