#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class When_dispatching_immediate_local_unicast_root_send_creates_scope
{
    [Test]
    public async Task Run()
    {
        await using var broker = new NonDurableBroker();
        var dispatcher = await CreateDispatcher(broker, ["input"]);

        var task = dispatcher.Dispatch(new TransportOperations(CreateUnicast("input")), new TransportTransaction());
        var envelope = await broker.GetOrCreateQueue("input").Dequeue();
        var inlineState = GetInlineState(envelope);
        var scope = GetInlineScope(inlineState!);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(task.IsCompleted, Is.False);
            Assert.That(inlineState, Is.Not.Null);
            Assert.That(GetIsRootDispatch(inlineState!), Is.True);
            Assert.That(GetDepth(inlineState!), Is.EqualTo(0));
            Assert.That(task, Is.SameAs(GetCompletion(scope)));
        }
    }
}
