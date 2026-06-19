namespace NServiceBus.TransportTests.Simulation;

using System;
using System.Threading.RateLimiting;
using System.Threading.Tasks;
using NUnit.Framework;

[TestFixture]
public class When_built_in_fixed_window_limiter_rejects
{
    [Test]
    public async Task Should_surface_retry_after_metadata_in_the_simulation_exception()
    {
        await using var limiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            PermitLimit = 1,
            Window = TimeSpan.FromSeconds(30),
            QueueLimit = 0,
            AutoReplenishment = true
        });

        await using var broker = new NonDurableBroker(new NonDurableBrokerOptions
        {
            Send =
            {
                Mode = NonDurableSimulationMode.Reject,
                RateLimiter = limiter
            }
        });

        var dispatcher = await NonDurableBrokerSimulationTestHelper.CreateDispatcher(broker);
        await NonDurableBrokerSimulationTestHelper.Dispatch(dispatcher, "msg-1", "queue");

        var exception = Assert.ThrowsAsync<NonDurableSimulationException>(async () => await NonDurableBrokerSimulationTestHelper.Dispatch(dispatcher, "msg-2", "queue"));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception!.RetryAfter, Is.GreaterThan(TimeSpan.Zero));
            Assert.That(exception.RetryAfter, Is.LessThanOrEqualTo(TimeSpan.FromSeconds(30)));
        }
    }
}
