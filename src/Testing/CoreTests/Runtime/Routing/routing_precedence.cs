using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TestingSupport;
using Wolverine.Attributes;
using Wolverine.Configuration;
using Wolverine.Runtime;
using Wolverine.Runtime.Routing;
using Wolverine.Transports.Tcp;
using Xunit;
using IMessagePublisher = Wolverine.IMessagePublisher;

namespace CoreTests.Runtime.Routing;

public class routing_precedence
{

    
    [Fact]
    public async Task local_routing_is_applied_automatically()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine().StartAsync();

        var bus = host.Services.GetRequiredService<IMessagePublisher>();
        bus.PreviewSubscriptions(new BlueMessage())
            .Single().Destination.ShouldBe(new Uri("local://blue"));
    }
    
    
    [Fact]
    public async Task respect_local_queue()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine().StartAsync();

        var bus = host.Services.GetRequiredService<IMessagePublisher>();
        bus.PreviewSubscriptions(new GreenMessage())
            .Single().Destination.ShouldBe(new Uri("local://seagreen"));
        
        bus.PreviewSubscriptions(new DarkGreenMessage())
            .Single().Destination.ShouldBe(new Uri("local://seagreen"));
    }
        
    [Fact]
    public async Task explicit_routing_to_local_wins()
    {
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.PublishMessage<BlueMessage>().ToLocalQueue("purple");
            }).StartAsync();

        var bus = host.Services.GetRequiredService<IMessagePublisher>();
        bus.PreviewSubscriptions(new BlueMessage())
            .Single().Destination.ShouldBe(new Uri("local://purple"));
    }
    
    [Fact]
    public async Task explicit_routing_to_elsewhere_wins()
    {
        var port = PortFinder.GetAvailablePort();
        
        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.PublishMessage<BlueMessage>().ToPort(port);
            }).StartAsync();

        var bus = host.Services.GetRequiredService<IMessagePublisher>();
        bus.PreviewSubscriptions(new BlueMessage())
            .Single().Destination.ShouldBe(new Uri("tcp://localhost:" + port));
    }

    [Fact]
    public async Task local_takes_precedence_on_other_routers()
    {
        var port = PortFinder.GetAvailablePort();
        var convention = new FakeRoutingConvention
        {
            Senders =
            {
                [typeof(BlueMessage)] = port
            }
        };

        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.RouteWith(convention);
            }).StartAsync();

        var bus = host.Services.GetRequiredService<IMessagePublisher>();
        bus.PreviewSubscriptions(new BlueMessage())
            .Single().Destination.ShouldBe(new Uri("local://blue"));
    }

    [Fact]
    public async Task fall_through_to_other_rules_if_no_local()
    {
        var port = PortFinder.GetAvailablePort();
        var convention = new FakeRoutingConvention
        {
            Senders =
            {
                [typeof(RedMessage)] = port
            }
        };

        using var host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.RouteWith(convention);
            }).StartAsync();

        var bus = host.Services.GetRequiredService<IMessagePublisher>();
        bus.PreviewSubscriptions(new RedMessage())
            .Single().Destination.ShouldBe(new Uri("tcp://localhost:" + port));
    }
}

public class ColorsMessageHandler
{
    public void Handle(BlueMessage message){}
    public void Handle(GreenMessage message){}
    public void Handle(DarkGreenMessage message){}
}

[MessageIdentity("blue")]
public record BlueMessage;
public record RedMessage;

[LocalQueue("seagreen")]
public record GreenMessage;

[LocalQueue("seagreen")]
public record DarkGreenMessage;

public class FakeRoutingConvention : IMessageRoutingConvention
{
    public Dictionary<Type, int> Senders { get; } = new();
    

    public void DiscoverListeners(IWolverineRuntime runtime, IReadOnlyList<Type> handledMessageTypes)
    {
        // Nothing
    }

    public IEnumerable<Endpoint> DiscoverSenders(Type messageType, IWolverineRuntime runtime)
    {
        if (Senders.TryGetValue(messageType, out var port))
        {
            var endpoint = runtime.Endpoints.GetOrBuildSendingAgent(new Uri("tcp://localhost:" + port)).Endpoint;
            yield return endpoint;
        }
    }
}