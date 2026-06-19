namespace NServiceBus.TransportTests.Simulation;

using System;
using System.Threading;
using NUnit.Framework;
using static NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_simulating_send_reject
{
    [Test]
    public void Should_throw_immediately()
    {
        Assert.DoesNotThrowAsync(async () =>
        {
            await using var broker = new NonDurableBroker(new NonDurableBrokerOptions
            {
                Send = { Mode = NonDurableSimulationMode.Reject, RateLimit = new NonDurableRateLimitOptions { PermitLimit = 1, Window = TimeSpan.FromMinutes(1) } }
            });

            var dispatcher = await CreateDispatcher(broker, CancellationToken.None);
            await Dispatch(dispatcher, "msg-1", "queue", CancellationToken.None);
        });

        Assert.ThrowsAsync<NonDurableSimulationException>(async () =>
        {
            await using var broker = new NonDurableBroker(new NonDurableBrokerOptions
            {
                Send = { Mode = NonDurableSimulationMode.Reject, RateLimit = new NonDurableRateLimitOptions { PermitLimit = 1, Window = TimeSpan.FromMinutes(1) } }
            });

            var dispatcher = await CreateDispatcher(broker, CancellationToken.None);
            await Dispatch(dispatcher, "msg-1", "queue", CancellationToken.None);
            await Dispatch(dispatcher, "msg-2", "queue", CancellationToken.None);
        });
    }
}
