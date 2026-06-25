namespace NServiceBus.TransportTests.Simulation;

using System.Threading.Tasks;
using NServiceBus.Routing;
using NServiceBus.Transport;
using NUnit.Framework;
using static NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_multicast_dispatching_under_send_simulation_rejects_after_first_subscriber
{
    [Test]
    public async Task Should_deliver_partially_and_surface_simulation_failure()
    {
        await using var limiter = new CountingRateLimiter(permitCount: 1);
        await using var broker = new NonDurableBroker(new NonDurableBrokerOptions
        {
            Send = { Mode = NonDurableSimulationMode.Reject, RateLimiter = limiter }
        });

        broker.Subscribe("sub-a", typeof(PublishedEvent).FullName!);
        broker.Subscribe("sub-b", typeof(PublishedEvent).FullName!);

        var dispatcher = await CreateDispatcher(broker);
        var message = new OutgoingMessage("msg-1", [], new byte[] { 1 });
        var operation = new TransportOperation(message, new MulticastAddressTag(typeof(PublishedEvent)));

        var publishTask = dispatcher.Dispatch(new TransportOperations(operation), new TransportTransaction());

        Assert.ThrowsAsync<NonDurableSimulationException>(async () => await publishTask);

        // Delivery is partial: the single shared permit let exactly one subscriber through before the rejection.
        Assert.That(broker.GetOrCreateQueue("sub-a").Count + broker.GetOrCreateQueue("sub-b").Count, Is.EqualTo(1));
    }
}

sealed class PublishedEvent : IEvent;