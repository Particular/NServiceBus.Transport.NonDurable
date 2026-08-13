#nullable enable

namespace NServiceBus.TransportTests.OpenTelemetry;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Routing;
using NServiceBus.Transport;
using NServiceBus.Transport.NonDurable.Tests;
using NUnit.Framework;

[TestFixture]
public class When_chaining_send_process_spans_in_inline_execution
{
    [Test]
    public async Task Should_parent_process_spans_to_send_spans_across_inline_hops()
    {
        const int hops = 3;
        await using var broker = new NonDurableBroker();
        using var listener = new TestingActivityListener(NonDurableTransportTracing.ActivitySourceName);
        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var dispatcher = infrastructure.Dispatcher;
        var receiver = infrastructure.Receivers["receiver-0"];
        var handledCount = 0;
        var allHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            async (context, cancellationToken) =>
            {
                var hop = Interlocked.Increment(ref handledCount);
                if (hop < hops)
                {
                    var message = new OutgoingMessage($"msg-{hop + 1}", [], new byte[] { 1 });
                    await dispatcher.Dispatch(
                        new TransportOperations(new TransportOperation(message, new UnicastAddressTag("input"))),
                        context.TransportTransaction,
                        cancellationToken);
                }
                else
                {
                    allHandled.TrySetResult();
                }
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await receiver.StartReceive();
        await dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("input")),
            new TransportTransaction()).WaitAsync(TimeSpan.FromSeconds(5));
        await allHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await receiver.StopReceive();

        var transportActivities = listener.CompletedFrom(NonDurableTransportTracing.ActivitySourceName);
        var sendActivities = transportActivities.Where(activity => activity.OperationName == NonDurableTransportTracing.SendActivityName).ToList();
        var processActivities = transportActivities.Where(activity => activity.OperationName == NonDurableTransportTracing.ProcessActivityName).ToList();

        using (Assert.EnterMultipleScope())
        {
            Assert.That(processActivities, Has.Count.EqualTo(hops));
            Assert.That(sendActivities, Has.Count.EqualTo(hops));

            // Inline execution processes each hop synchronously within the send, so every process
            // span is parented to the send span that produced the message.
            foreach (var process in processActivities)
            {
                Assert.That(process.ParentId, Is.Not.Null, "process span should be parented to a send span");
                Assert.That(sendActivities.Any(send => send.Id == process.ParentId), Is.True, "process span should be parented to the send span that produced the message");
                Assert.That(process.Links, Is.Empty, "parented process spans carry no links");
            }

            // The chain is parent-child end to end: only the root send span has no parent, and
            // every nested send span is parented to a process span.
            Assert.That(sendActivities.Count(send => send.ParentId is null), Is.EqualTo(1), "only the root send span is a root");
            foreach (var send in sendActivities.Where(send => send.ParentId is not null))
            {
                Assert.That(processActivities.Any(process => process.Id == send.ParentId), Is.True, "each nested send span should be parented to a process span");
            }
        }
    }
}
