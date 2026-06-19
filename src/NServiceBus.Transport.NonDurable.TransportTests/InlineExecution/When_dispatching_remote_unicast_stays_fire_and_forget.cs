#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class When_dispatching_remote_unicast_stays_fire_and_forget
{
    [Test]
    public async Task Run()
    {
        await using var broker = new NonDurableBroker();
        var dispatcher = await CreateDispatcher(broker, ["input"]);

        var task = dispatcher.Dispatch(new TransportOperations(CreateUnicast("remote")), new TransportTransaction());

        await task;

        Assert.That(task.IsCompletedSuccessfully, Is.True);
        Assert.That(broker.GetOrCreateQueue("remote").Count, Is.EqualTo(1));
        Assert.That(GetInlineState(await broker.GetOrCreateQueue("remote").Dequeue()), Is.Null);
    }
}
