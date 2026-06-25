namespace NServiceBus.TransportTests.Simulation;

using System;
using System.Threading;
using System.Threading.RateLimiting;
using System.Threading.Tasks;
using NUnit.Framework;

[TestFixture]
public class When_disposing_broker_with_rate_limiter_factory
{
    [Test]
    public async Task Should_dispose_factory_created_limiters()
    {
        var limiter = new DisposableRateLimiter();
        var broker = new NonDurableBroker(new NonDurableBrokerOptions
        {
            Send =
            {
                RateLimiterFactory = _ => limiter
            }
        });

        var dispatcher = await NonDurableBrokerSimulationTestHelper.CreateDispatcher(broker);
        await NonDurableBrokerSimulationTestHelper.Dispatch(dispatcher, "msg-1", "queue");

        await broker.DisposeAsync();

        Assert.That(limiter.Disposed, Is.True);
    }
}

sealed class DisposableRateLimiter : RateLimiter
{
    public bool Disposed { get; private set; }

    public override TimeSpan? IdleDuration => null;

    public override RateLimiterStatistics GetStatistics() => null;

    protected override RateLimitLease AttemptAcquireCore(int permitCount) => FixedRateLimitLease.Granted;

    protected override ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult<RateLimitLease>(FixedRateLimitLease.Granted);

    protected override void Dispose(bool disposing) => Disposed = true;
}
