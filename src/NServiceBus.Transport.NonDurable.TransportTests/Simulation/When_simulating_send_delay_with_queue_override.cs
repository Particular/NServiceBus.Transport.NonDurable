namespace NServiceBus.TransportTests.Simulation;

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using NUnit.Framework;
using static NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_simulating_send_delay_with_queue_override
{
    [Test]
    public async Task Should_use_queue_operation_settings_over_broker_defaults()
    {
        var simulatedTime = new FakeTimeProvider(new DateTimeOffset(2026, 03, 28, 12, 0, 0, TimeSpan.Zero));
        var options = new NonDurableBrokerOptions
        {
            TimeProvider = simulatedTime,
            Send = { RateLimit = new NonDurableRateLimitOptions { PermitLimit = 1, Window = TimeSpan.FromSeconds(30) } }
        };
        options.ForQueue("queue").Send.RateLimit = new NonDurableRateLimitOptions { PermitLimit = 2, Window = TimeSpan.FromSeconds(30) };

        await using var broker = new NonDurableBroker(options);
        var dispatcher = await CreateDispatcher(broker);

        await Dispatch(dispatcher, "msg-1", "queue");
        await Dispatch(dispatcher, "msg-2", "queue");
        var thirdDispatch = Dispatch(dispatcher, "msg-3", "queue");

        await AsyncSpinWait.Until(() => broker.GetOrCreateQueue("queue").Count == 2, maxIterations: 20);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(thirdDispatch.IsCompleted, Is.False);
            Assert.That(broker.GetOrCreateQueue("queue").Count, Is.EqualTo(2));
        }

        simulatedTime.Advance(TimeSpan.FromSeconds(30));
        await AsyncSpinWait.Until(() => thirdDispatch.IsCompleted, maxIterations: 100);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(thirdDispatch.IsCompleted, Is.True);
            Assert.That(broker.GetOrCreateQueue("queue").Count, Is.EqualTo(3));
        }
    }
}
