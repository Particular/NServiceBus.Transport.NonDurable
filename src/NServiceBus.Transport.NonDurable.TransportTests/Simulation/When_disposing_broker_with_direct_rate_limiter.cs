namespace NServiceBus.TransportTests.Simulation;

using System.Threading.Tasks;
using NUnit.Framework;

[TestFixture]
public class When_disposing_broker_with_direct_rate_limiter
{
    [Test]
    public async Task Should_not_dispose_direct_limiters()
    {
        await using var limiter = new DisposableRateLimiter();
        var broker = new NonDurableBroker(new NonDurableBrokerOptions
        {
            Send =
            {
                RateLimiter = limiter
            }
        });

        var dispatcher = await NonDurableBrokerSimulationTestHelper.CreateDispatcher(broker);
        await NonDurableBrokerSimulationTestHelper.Dispatch(dispatcher, "msg-1", "queue");

        await broker.DisposeAsync();

        Assert.That(limiter.Disposed, Is.False);
    }
}
