#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Linq;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class When_dispatching_user_requested_delayed_local_dispatch_from_receive_pipeline_is_not_inline
{
    [Test]
    public async Task Run()
    {
        await using var broker = new NonDurableBroker();
        var dispatcher = await CreateDispatcher(broker, ["input"]);
        var transaction = new TransportTransaction();
        var (committable, receiveTransaction) = CreateReceiveTransaction();
        var scope = CreateScope();

        transaction.Set(CreateReceivePipelineMarker());
        AttachReceiveTransaction(transaction, (committable, receiveTransaction));
        AttachInlineScope(transaction, scope);

        var task = dispatcher.Dispatch(new TransportOperations(CreateUnicast("input", DispatchConsistency.Default, TimeSpan.FromMinutes(1))), transaction);

        await task;

        var pending = GetPendingEnvelopes(receiveTransaction);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(task.IsCompletedSuccessfully, Is.True);
            Assert.That(task, Is.Not.SameAs(GetCompletion(scope)));
            Assert.That(pending, Has.Count.EqualTo(1));
            Assert.That(GetInlineState(pending.Single()), Is.Null);
        }

        committable.Dispose();
    }
}
