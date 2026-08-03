namespace NServiceBus.TransportTests.Simulation;

using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using static NonDurableBrokerSimulationTestHelper;

// Regression guard for the synchronous tight loop in the delayed-delivery simulation rejection
// path: a rejected delayed message with no RetryAfter (treated as TimeSpan.Zero) re-schedules
// due immediately, so the delayed pump must force-yield rather than spin synchronously, and must
// still make progress once the limiter starts granting.
[TestFixture]
public class When_delayed_delivery_rejects_without_retry_after
{
    [Test]
    public async Task Should_yield_while_rejecting_and_deliver_once_limiter_grants()
    {
        await using var limiter = new ManualGrantRateLimiter();
        await using var broker = new NonDurableBroker(new NonDurableBrokerOptions
        {
            DelayedDelivery =
            {
                Mode = NonDurableSimulationMode.Reject,
                RateLimiter = limiter
            }
        });

        broker.EnqueueDelayed(CreateEnvelope("msg-1", "queue", 1), DateTimeOffset.UtcNow);
        await broker.StartPump();

        var queue = broker.GetOrCreateQueue("queue");

        // While the limiter rejects with no RetryAfter, the pump must keep retrying without
        // delivering the message to the target queue.
        await AsyncSpinWait.Until(() => limiter.Attempts >= 3, maxIterations: 100);
        Assert.That(queue.Count, Is.EqualTo(0));

        // Once the limiter starts granting, the pump must make progress and deliver the message.
        limiter.StartGranting();
        await AsyncSpinWait.Until(() => queue.Count == 1, maxIterations: 100);
        Assert.That(queue.Count, Is.EqualTo(1));

        // Dequeue and dispose the delivered envelope so its pooled buffer is returned (the test
        // must not retain a rented buffer).
        var delivered = await queue.Dequeue(CancellationToken.None);
        Assert.That(delivered.MessageId, Is.EqualTo("msg-1"));
        delivered.Dispose();
    }
}
