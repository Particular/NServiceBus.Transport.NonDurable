#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class When_dispatching_local_address_detection_restricted_to_endpoint_owned_receive_addresses
{
    [Test]
    public async Task Run()
    {
        await using var broker = new NonDurableBroker();
        var dispatcher = await CreateDispatcher(broker, ["input", "input-instance-a"]);

        var task = dispatcher.Dispatch(new TransportOperations(CreateUnicast("error")), new TransportTransaction());

        await task;

        Assert.That(task.IsCompletedSuccessfully, Is.True);
        Assert.That(GetInlineState(await broker.GetOrCreateQueue("error").Dequeue()), Is.Null);
    }
}
