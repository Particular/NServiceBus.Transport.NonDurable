namespace NServiceBus.TransportTests;

using System.Threading.Tasks;
using NUnit.Framework;
using static NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_simulating_send_with_direct_rate_limiter
{
    [Test]
    public async Task Should_use_configured_limiter()
    {
        await using var limiter = new CountingRateLimiter(permitCount: 1);
        await using var broker = new NonDurableBroker(new NonDurableBrokerOptions
        {
            Send =
            {
                Mode = NonDurableSimulationMode.Reject,
                RateLimiter = limiter
            }
        });

        var dispatcher = await CreateDispatcher(broker);
        await Dispatch(dispatcher, "msg-1", "queue");

        _ = Assert.ThrowsAsync<NonDurableSimulationException>(async () => await Dispatch(dispatcher, "msg-2", "queue"));
        Assert.That(limiter.AttemptAcquireCalls, Is.EqualTo(2));
    }
}
