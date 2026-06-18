#nullable enable

namespace NServiceBus.TransportTests;

using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using static NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_emitting_send_span
{
    [Test]
    public async Task Should_create_send_span_and_propagate_its_context()
    {
        await using var broker = new NonDurableBroker();
        using var listener = new TestingActivityListener(NonDurableTransportTracing.ActivitySourceName);
        var dispatcher = await CreateDispatcher(broker);

        await Dispatch(dispatcher, "msg-1", "queue");

        Assert.That(broker.GetOrCreateQueue("queue").TryPeek(out var envelope), Is.True);

        var sendActivity = listener.CompletedFrom(NonDurableTransportTracing.ActivitySourceName).Single(activity => activity.OperationName == NonDurableTransportTracing.SendActivityName);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(sendActivity.DisplayName, Is.EqualTo("send"));
            Assert.That(sendActivity.Status, Is.EqualTo(ActivityStatusCode.Ok));
            Assert.That(sendActivity.GetTagItem("messaging.system"), Is.EqualTo("nondurable"));
            Assert.That(sendActivity.GetTagItem("messaging.destination.name"), Is.EqualTo("queue"));
            Assert.That(sendActivity.GetTagItem("messaging.operation.name"), Is.EqualTo("send"));
            Assert.That(sendActivity.GetTagItem("messaging.operation.type"), Is.EqualTo("send"));
            Assert.That(sendActivity.GetTagItem("messaging.message.id"), Is.EqualTo(envelope!.MessageId));
            Assert.That(sendActivity.Events.Any(e => e.Name == "nondurable.enqueued"), Is.True);
            Assert.That(envelope.Headers[Headers.DiagnosticsTraceParent], Is.EqualTo(sendActivity.Id));
        }
    }
}
