#nullable enable

namespace NServiceBus.TransportTests.OpenTelemetry;

using System.Threading.Tasks;
using NUnit.Framework;
using static Simulation.NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_no_transport_listener_is_registered
{
    [Test]
    public async Task Should_not_change_headers_when_no_transport_listener_is_registered()
    {
        await using var broker = new NonDurableBroker();
        var dispatcher = await CreateDispatcher(broker);

        await Dispatch(dispatcher, "msg-3", "queue");

        Assert.That(broker.GetOrCreateQueue("queue").TryPeek(out var envelope), Is.True);
        Assert.That(envelope!.Headers.ContainsKey(Headers.DiagnosticsTraceParent), Is.False);
    }
}
