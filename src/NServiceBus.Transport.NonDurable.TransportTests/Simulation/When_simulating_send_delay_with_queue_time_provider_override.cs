namespace NServiceBus.TransportTests;

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using NUnit.Framework;
using static NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_simulating_send_delay_with_queue_time_provider_override
{
    [Test]
    public async Task Should_use_queue_operation_time_provider_over_broker_time_provider()
    {
        var brokerTime = new FakeTimeProvider(new DateTimeOffset(2026, 03, 28, 12, 0, 0, TimeSpan.Zero));
        var queueTime = new FakeTimeProvider(new DateTimeOffset(2026, 03, 28, 12, 0, 0, TimeSpan.Zero));
        var options = new NonDurableBrokerOptions
        {
            TimeProvider = brokerTime,
            Send = { RateLimit = new NonDurableRateLimitOptions { PermitLimit = 1, Window = TimeSpan.FromSeconds(5) } }
        };
        options.ForQueue("queue").Send.TimeProvider = queueTime;

        await using var broker = new NonDurableBroker(options);
        var dispatcher = await CreateDispatcher(broker);

        await Dispatch(dispatcher, "msg-1", "queue");
        var secondDispatch = Dispatch(dispatcher, "msg-2", "queue");

        brokerTime.Advance(TimeSpan.FromSeconds(5));
        await AsyncSpinWait.Until(() => secondDispatch.IsCompleted, maxIterations: 20);
        Assert.That(secondDispatch.IsCompleted, Is.False);

        queueTime.Advance(TimeSpan.FromSeconds(5));
        await AsyncSpinWait.Until(() => secondDispatch.IsCompleted, maxIterations: 100);
        Assert.That(secondDispatch.IsCompleted, Is.True);
    }
}
