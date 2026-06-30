#nullable enable

namespace NServiceBus.TransportTests.OpenTelemetry;

using System.Diagnostics;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NServiceBus.Transport.NonDurable.Tests;
using NUnit.Framework;
using static Simulation.NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_no_activity_listener_is_registered
{
    [Test]
    public async Task Should_dispatch_without_affecting_behavior()
    {
        // No TestingActivityListener is created, so ActivitySource.HasListeners() is false and
        // the transport takes the zero-allocation fast path. This asserts the listener-free path
        // does not change dispatch behavior or leave diagnostic state behind.
        await using var broker = new NonDurableBroker();
        var dispatcher = await CreateDispatcher(broker);

        await Dispatch(dispatcher, "msg-1", "queue");

        using (Assert.EnterMultipleScope())
        {
            Assert.That(broker.GetOrCreateQueue("queue").TryPeek(out var envelope), Is.True);
            Assert.That(envelope, Is.Not.Null);
            // No trace context should be injected when there are no listeners.
            Assert.That(envelope!.Headers.ContainsKey(Headers.DiagnosticsTraceParent), Is.False);
        }

        Assert.That(Activity.Current, Is.Null, "no ambient activity should be left after dispatch");
    }
}